// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>
#include <cstdlib>
#include <cstring>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <set>
#include <seastar/core/app-template.hh>
#include <seastar/core/signal.hh>
#include <string>
#include <string_view>
#include <vector>
#include <sys/stat.h>
#include <sstream>

#include "common/Formatter.h"
#include "common/hobject.h"
#include "crimson/common/errorator.h"
#include "crimson/osd/stop_signal.h"
#include "crimson/osd/osd_meta.h"

#include "objectstore_tool.h"
#include "crimson/common/config_proxy.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"
#include "seastar/util/closeable.hh"
#include "seastar/util/log.hh"
#include "crimson/os/seastore/segment_manager.h"
#include "include/expected.hpp"
#include "osd/osd_types.h"
#include "json_spirit/json_spirit_reader.h"

namespace bpo = boost::program_options;
using crimson::common::sharded_conf;
using crimson::common::local_conf;

using namespace crimson::tools::kvstore;

// Helper function to convert binary data to readable format
std::string cleanbin(const std::string& str) {
  // Check if string contains non-printable characters
  for (char c : str) {
    if (std::iscntrl(c)) {
      // Convert to base64
      ceph::bufferlist bl;
      bl.append(str);
      ceph::bufferlist b64;
      bl.encode_base64(b64);
      return "Base64:" + std::string(b64.c_str(), b64.length());
    }
  }
  return str;
}

std::string cleanbin_tmp(std::string_view str_view) {
    std::string str(str_view);
    return cleanbin(str);
}

static bool outistty = false;

enum class operation_type_t {
  // PG-level operations (--op)
  LIST_PGS,
  LIST_OBJECTS,
  INFO,

  // Object-level operations (positional arguments)
  GET_BYTES,
  SET_BYTES,
  GET_ATTR,
  SET_ATTR,
  RM_ATTR,
  LIST_ATTRS,
  GET_OMAP,
  SET_OMAP,
  RM_OMAP,
  LIST_OMAP,
  REMOVE,
  REMOVEALL,
  DUMP,
  SET_SIZE,
  CLEAR_DATA_DIGEST,
};

std::string to_string(operation_type_t op) {
  switch (op) {
    case operation_type_t::LIST_PGS: return "list-pgs";
    case operation_type_t::LIST_OBJECTS: return "list-objects";
    case operation_type_t::INFO: return "info";
    case operation_type_t::GET_BYTES: return "get-bytes";
    case operation_type_t::SET_BYTES: return "set-bytes";
    case operation_type_t::GET_ATTR: return "get-attr";
    case operation_type_t::SET_ATTR: return "set-attr";
    case operation_type_t::RM_ATTR: return "rm-attr";
    case operation_type_t::LIST_ATTRS: return "list-attrs";
    case operation_type_t::GET_OMAP: return "get-omap";
    case operation_type_t::SET_OMAP: return "set-omap";
    case operation_type_t::RM_OMAP: return "rm-omap";
    case operation_type_t::LIST_OMAP: return "list-omap";
    case operation_type_t::REMOVE: return "remove";
    case operation_type_t::REMOVEALL: return "removeall";
    case operation_type_t::DUMP: return "dump";
    case operation_type_t::SET_SIZE: return "set-size";
    case operation_type_t::CLEAR_DATA_DIGEST: return "clear-data-digest";
    default: return "unknown";
  }
}

tl::expected<operation_type_t, std::string> parse_pg_operation(const std::string& op_str) {
  if (op_str == "list-pgs") return operation_type_t::LIST_PGS;
  if (op_str == "list") return operation_type_t::LIST_OBJECTS;
  if (op_str == "info") return operation_type_t::INFO;
  return tl::unexpected("Unsupported PG operation: " + op_str);
}

tl::expected<operation_type_t, std::string> parse_object_operation(const std::string& objcmd) {
  if (objcmd == "get-bytes") return operation_type_t::GET_BYTES;
  if (objcmd == "set-bytes") return operation_type_t::SET_BYTES;
  if (objcmd == "get-attr") return operation_type_t::GET_ATTR;
  if (objcmd == "set-attr") return operation_type_t::SET_ATTR;
  if (objcmd == "rm-attr") return operation_type_t::RM_ATTR;
  if (objcmd == "list-attrs") return operation_type_t::LIST_ATTRS;
  if (objcmd == "get-omap") return operation_type_t::GET_OMAP;
  if (objcmd == "set-omap") return operation_type_t::SET_OMAP;
  if (objcmd == "rm-omap") return operation_type_t::RM_OMAP;
  if (objcmd == "list-omap") return operation_type_t::LIST_OMAP;
  if (objcmd == "remove") return operation_type_t::REMOVE;
  if (objcmd == "removeall") return operation_type_t::REMOVEALL;
  if (objcmd == "dump") return operation_type_t::DUMP;
  if (objcmd == "set-size") return operation_type_t::SET_SIZE;
  if (objcmd == "clear-data-digest") return operation_type_t::CLEAR_DATA_DIGEST;
  return tl::unexpected("Unknown object command: " + objcmd);
}

struct operation_params_t {
  operation_type_t op;
  std::optional<pg_t> pgid;
  std::optional<std::string> object;
  std::optional<std::string> key;
  std::optional<std::string> omap_start;
  std::optional<std::string> file;
  std::optional<uint64_t> size;
  bool force = false;
};

struct objectstore_config_t {
  // Basic parameters
  std::string data_path;
  std::string device_type;
  std::string type;
  std::string format;
  std::string pgid_str;
  std::string op;
  std::string file;
  std::string namespace_;

  // Positional arguments
  std::string object;
  std::string objcmd;
  std::string arg1;
  std::string arg2;

  // Flags
  bool debug = false;
  bool force = false;
  bool tty = false;

  // Internal state
  std::optional<operation_params_t> operation;
  coll_t coll;
  spg_t pgid;
  ghobject_t ghobj;

  void populate_options(bpo::options_description &desc) {
    desc.add_options()
      ("help", "produce help message")
      ("type", bpo::value<std::string>(&type)->default_value("seastore"),
       "store type, default: seastore")
      ("data-path", bpo::value<std::string>(&data_path),
       "path to object store, mandatory")
      ("device-type", bpo::value<std::string>(&device_type)->default_value("SSD"),
       "path to object store, defualt: SSD")
      ("pgid", bpo::value<std::string>(&pgid_str),
       "PG id, mandatory for info operation")
      ("op", bpo::value<std::string>(&op),
       "Arg is one of [info, list, list-pgs]")
      ("file", bpo::value<std::string>(&file),
       "path of file to read from or write to")
      ("format", bpo::value<std::string>(&format)->default_value("json-pretty"),
       "Output format which may be json, json-pretty, xml, xml-pretty. defualt: json pretty")
      ("debug", bpo::bool_switch(&debug),
       "Set seastar logger level to debug. default: error)")
      ("force", bpo::bool_switch(&force),
       "Ignore some types of errors and proceed with operation - USE WITH CAUTION")
      ("tty", bpo::bool_switch(&tty),
       "Treat stdout as a tty (no binary data)")
      ("namespace", bpo::value<std::string>(&namespace_),
       "Specify namespace when searching for objects")
      ;
  }
};

tl::expected<pg_t, std::string> parse_pgid(const std::string& pgid_str) {
  pg_t pgid;
  if (!pgid.parse(pgid_str.c_str())) {
    return tl::unexpected("Invalid pgid: " + pgid_str);
  }
  return pgid;
}

std::string get_pgid_str_from_coll(const coll_t& coll) {
  std::string coll_str = fmt::format("{}", coll);
  auto pos = coll_str.find('_');
  if (pos != std::string::npos) {
    return coll_str.substr(0, pos);
  }
  return coll_str;
}

class ObjectAction {
public:
  virtual ~ObjectAction() = default;
  virtual seastar::future<> call(
    const coll_t& coll,
    seastar::shard_id shard_id,
    const ghobject_t& ghobj) = 0;
};

struct FoundObject {
  coll_t coll;
  ghobject_t ghobj;
  seastar::shard_id shard_id;
};

class LookupGhobject : public ObjectAction {
public:
  explicit LookupGhobject(
    std::string_view name,
    std::optional<std::string_view> nspace = std::nullopt)
    : name(name), nspace(nspace) {}

  seastar::future<> call(
    const coll_t& coll,
    seastar::shard_id shard_id,
    const ghobject_t& ghobj) override {
    if ((name.empty() || ghobj.hobj.oid.name == name) &&
        (!nspace.has_value() || ghobj.hobj.nspace == *nspace)) {
      objects.push_back({coll, ghobj, shard_id});
    }
    return seastar::now();
  }

  std::vector<FoundObject> objects;
private:
  std::string_view name;
  std::optional<std::string_view> nspace;
};

class PrintObjectAction : public ObjectAction {
public:
  seastar::future<> call(
    const coll_t& coll,
    seastar::shard_id shard_id,
    const ghobject_t& ghobj) override {
    std::stringstream ss;
    std::unique_ptr<Formatter> obj_formatter(Formatter::create("json"));
    obj_formatter->dump_object("obj", ghobj);
    obj_formatter->flush(ss);
    std::string obj_json = ss.str();
    if (!obj_json.empty() && obj_json.back() == '\n') {
      obj_json.pop_back();
    }
    fmt::println(std::cout, R"(["{}",{}])""", get_pgid_str_from_coll(coll), obj_json);
    return seastar::now();
  }
};

static seastar::future<int> action_on_all_objects(
  StoreTool& st,
  ObjectAction& action,
  std::optional<pg_t> pgid_filter = std::nullopt)
{
  auto pgs = co_await st.list_pgs();

  for (const auto& [coll, shard_id] : pgs) {
    if (pgid_filter.has_value()) {
      spg_t cand_pgid;
      if (!coll.is_pg(&cand_pgid)) {
        continue;
      }
      if (cand_pgid.pgid != *pgid_filter) {
        continue;
      }
    }

    st.set_shard_id(shard_id);
    ghobject_t next;
    do {
      auto [objs, next_obj] = co_await st.list_objects(coll, next);
      next = next_obj;
      for (const auto& obj : objs) {
        co_await action.call(coll, shard_id, obj);
      }
    } while (next != ghobject_t::get_max());
  }
  co_return 0;
}

static seastar::future<bool> find_shard_for_object(
  StoreTool& st,
  objectstore_config_t& config,
  const std::string& object_type)
{
  auto pgs = co_await st.list_pgs();
  auto it = std::find_if(pgs.begin(), pgs.end(),
                         [&config](const auto& pg) { return pg.first == config.coll; });
  if (it == pgs.end()) {
    fmt::println(std::cerr, "PG '{}' not found for {} object", config.coll, object_type);
    co_return false;
  }
  st.set_shard_id(it->second);
  co_return true;
}

static seastar::future<bool> resolve_operation_parameters(
  StoreTool& st,
  objectstore_config_t& config)
{
  auto& op = config.operation.value();

  // 1. Resolve PGID if present (from --pgid)
  if (op.pgid.has_value()) {
    config.pgid = spg_t(*op.pgid);
    config.coll = coll_t(config.pgid);
  }

  // 2. Handle empty object case
  if (!op.object.has_value() || op.object->empty()) {
    if (op.op == operation_type_t::LIST_OBJECTS) {
      co_return true;  // No object resolution needed for list operation
    }
    if (!op.pgid.has_value()) {
      fmt::println(std::cerr, "PG ID is required for pgmeta operations");
      co_return false;
    }
    fmt::println(std::cout, "object name is empty, use pgmeta oid");
    config.ghobj = config.pgid.make_pgmeta_oid();

    // Find shard for pgmeta object
    co_return co_await find_shard_for_object(st, config, "pgmeta");
  } else {
    // 3. Handle non-empty object specification
    ghobject_t parsed_obj;
    bool is_json = false;
    try {
      json_spirit::Value json_val;
      if (json_spirit::read(*op.object, json_val)) {
        parsed_obj.decode(json_val);
        is_json = true;
      }
    } catch (...) {
      is_json = false;
    }

    if (is_json) {
      // JSON object specification
      config.ghobj = parsed_obj;
      if (!op.pgid.has_value()) {
        fmt::println(std::cerr, "PG ID is required when object is specified as JSON");
        co_return false;
      }

      // Find shard for JSON object
      co_return co_await find_shard_for_object(st, config, "JSON");
    } else {
      // Object name lookup
      LookupGhobject lookup(*op.object, config.namespace_);
      co_await action_on_all_objects(st, lookup, op.pgid);

      if (lookup.objects.empty()) {
        fmt::println(std::cerr, "Object '{}' not found. If this object is in a non-default namespace, please specify it with --namespace.", *op.object);
        co_return false;
      }
      if (lookup.objects.size() > 1) {
        fmt::println(std::cerr, "Found {} objects with name '{}', please specify pgid or use JSON format",
                      lookup.objects.size(), *op.object);
        co_return false;
      }

      // Name lookup successful - object and shard are already resolved
      const auto& found = lookup.objects.front();
      config.ghobj = found.ghobj;
      config.coll = found.coll;
      spg_t found_spg;
      if (found.coll.is_pg(&found_spg)) {
        config.pgid = found_spg;
        op.pgid = {found_spg.pgid};
      }
      st.set_shard_id(found.shard_id);
      co_return true;  // Name lookup provides complete resolution
    }
  }
}

void print_usage(const bpo::options_description& desc) {
  std::cout << std::endl;
  std::cout << desc << std::endl;
  std::cout << std::endl;
  std::cout << "Positional syntax:" << std::endl;
  std::cout << std::endl;
  std::cout << "crimson-objectstore-tool ... <object> (get|set)-bytes [file]" << std::endl;
  std::cout << "crimson-objectstore-tool ... <object> set-(attr|omap) <key> [file]" << std::endl;
  std::cout << "crimson-objectstore-tool ... <object> (get|rm)-(attr|omap) <key>" << std::endl;
  std::cout << "crimson-objectstore-tool ... <object> list-attrs" << std::endl;
  std::cout << "crimson-objectstore-tool ... <object> list-omap" << std::endl;
  std::cout << "crimson-objectstore-tool ... <object> remove|removeall" << std::endl;
  std::cout << "crimson-objectstore-tool ... <object> dump" << std::endl;
  std::cout << "crimson-objectstore-tool ... <object> set-size" << std::endl;
  std::cout << "crimson-objectstore-tool ... <object> clear-data-digest" << std::endl;
  std::cout << std::endl;
  std::cout << "<object> can be a JSON object description as displayed" << std::endl;
  std::cout << "by --op list." << std::endl;
  std::cout << "<object> can be an object name which will be looked up in all" << std::endl;
  std::cout << "the OSD's PGs." << std::endl;
  std::cout << "<object> can be the empty string ('') which with a provided pgid " << std::endl;
  std::cout << "specifies the pgmeta object" << std::endl;
  std::cout << std::endl;
  std::cout << "The optional [file] argument will read stdin or write stdout" << std::endl;
  std::cout << "if not specified or if '-' specified." << std::endl;
}

class SeastoreMetaReader {
private:
  std::string m_data_path;
  std::string m_device_type;

  size_t get_filesystem_block_size(const std::string& path) {
    struct stat st;
    if (stat(path.c_str(), &st) == 0) {
      return st.st_blksize;
    }
    return 4096;
  }

public:
  explicit SeastoreMetaReader(const std::string& path, const std::string& device_type) :
    m_data_path(path), m_device_type(device_type) {}

  tl::expected<crimson::os::seastore::block_sm_superblock_t, std::string> load_seastore_superblock() {
    try {
      std::string block_path = m_data_path + "/block";

      size_t block_size = get_filesystem_block_size(block_path);

      std::ifstream file(block_path, std::ios::binary);
      if (!file.is_open()) {
        return tl::unexpected("Could not open block file: " + block_path);
      }

      std::vector<char> buf(block_size);
      file.read(buf.data(), block_size);

      if (!file.good() && !file.eof()) {
        return tl::unexpected("Could not read superblock from " + block_path);
      }

      bufferlist bl;
      bl.append(buf.data(), block_size);

      auto bliter = bl.cbegin();

      // fmt::println(std::cout, "Device type: {}", m_device_type);

      if (m_device_type != "RANDOM_BLOCK_SSD") {
        // TODO this Signature is only applicable for segment devices(SSD/HDD) not
        // for other two devices like ZBD/RANDOM_BLOCK_SSD
        constexpr const char SEASTORE_SUPERBLOCK_SIGN[] = "seastore block device\n";
        constexpr std::size_t SEASTORE_SUPERBLOCK_SIGN_LEN = sizeof(SEASTORE_SUPERBLOCK_SIGN) - 1;

        // Validate the magic prefix
        std::string sb_magic;
        bliter.copy(SEASTORE_SUPERBLOCK_SIGN_LEN, sb_magic);
        if (sb_magic != SEASTORE_SUPERBLOCK_SIGN) {
          return tl::unexpected("invalid superblock signature " + block_path);
        }
      }

      crimson::os::seastore::block_sm_superblock_t superblock;
      decode(superblock, bliter);

      ceph_assert(ceph::encoded_sizeof<crimson::os::seastore::block_sm_superblock_t>(superblock) <
                  block_size);

      return superblock;

    } catch (const std::exception& e) {
      return tl::unexpected("Could not read seastore superblock: " + std::string(e.what()));
    } catch (...) {
      return tl::unexpected("Could not read seastore superblock: unknown error");
    }
  }

  tl::expected<unsigned int, std::string> get_shard_count() {
    auto superblock_result = load_seastore_superblock();
    if (!superblock_result) {
      return tl::unexpected(superblock_result.error());
    }

    // fmt::println(std::cout, "Read shard count from storage: {}", superblock_result->shard_num);
    return superblock_result->shard_num;
  }
};

static tl::expected<unsigned int, std::string>
read_shard_count_from_storage(const std::string& data_path,
                              const std::string& type,
                              const std::string& device_type)
{
  if (type != "seastore") {
    return tl::unexpected("Store type not supported for shard count reading");
  }

  SeastoreMetaReader meta_reader(data_path, device_type);
  return meta_reader.get_shard_count();
}

static tl::expected<std::vector<std::string>, std::string>
get_seastar_args_from_storage(const objectstore_config_t& config)
{
  auto shard_count_result = read_shard_count_from_storage(config.data_path,
                                                          config.type,
                                                          config.device_type);

  if (!shard_count_result) {
    return tl::unexpected(shard_count_result.error());
  }

  // seastore case with valid shard count
  std::vector<std::string> seastar_args;
  seastar_args.emplace_back("--smp");
  seastar_args.emplace_back(std::to_string(*shard_count_result));
  seastar_args.emplace_back("--thread-affinity");
  seastar_args.emplace_back("0");

  // fmt::println(std::cout, "Using shard configuration from storage: --smp {}", *shard_count_result);
  return seastar_args;
}

seastar::future<int> write_output(
  const std::optional<std::string>& file_path,
  const std::string& data)
{
  if (file_path.has_value() && *file_path != "-") {
    std::ofstream outfile(file_path.value(), std::ios::binary);
    if (!outfile) {
      fmt::println(std::cerr, "Failed to open output file '{}'", *file_path);
      return seastar::make_ready_future<int>(EXIT_FAILURE);
    }
    outfile.write(data.data(), data.size());
  } else {
    std::cout.write(data.data(), data.size());
  }
  return seastar::make_ready_future<int>(EXIT_SUCCESS);
}

tl::expected<std::string, int> read_input(
  const std::optional<std::string>& file_path)
{
  std::string input_data;
  if (file_path.has_value() && *file_path != "-") {
    std::ifstream infile(file_path.value(), std::ios::binary);
    if (!infile.is_open()) {
      fmt::println(std::cerr, "failed to open input-file '{}'", file_path.value());
      return tl::unexpected(EXIT_FAILURE);
    }
    std::stringstream buffer;
    buffer << infile.rdbuf();
    input_data = buffer.str();
  } else {
    if (isatty(STDIN_FILENO) && (!file_path.has_value() || *file_path != "-")) {
      fmt::println(std::cerr, "stdin is a tty and no file specified");
      return tl::unexpected(EXIT_FAILURE);
    }
    std::stringstream buffer;
    buffer << std::cin.rdbuf();
    input_data = buffer.str();
  }
  return input_data;
}

seastar::future<int> run_tool(StoreTool& st, objectstore_config_t& config) {
  std::unique_ptr<Formatter> formatter(
    Formatter::create(config.format));

  if (!config.operation.has_value()) {
    fmt::println(std::cerr, "No operation specified");
    co_return EXIT_FAILURE;
  }

  // Resolve object and pgid before executing operations
  if (config.operation->op != operation_type_t::LIST_PGS) {
    bool resolved = co_await resolve_operation_parameters(st, config);
    if (!resolved) {
      co_return EXIT_FAILURE;
    }
  }

  const auto& op = config.operation.value();

  switch (op.op) {
    case operation_type_t::LIST_PGS: {
      auto pgs = co_await st.list_pgs();
      std::set<std::string> pgid_strs;
      for (const auto& pg : pgs) {
        pgid_strs.insert(get_pgid_str_from_coll(pg.first));
      }
      for (const auto& pgid_str : pgid_strs) {
        fmt::println(std::cout, "{}", pgid_str);
      }
      break;
    }

    case operation_type_t::INFO: {
      auto info = co_await st.get_pg_info(config.coll);
      formatter->open_object_section("info");
      info.dump(formatter.get());
      formatter->close_section();
      formatter->flush(std::cout);
      break;
    }

    case operation_type_t::LIST_OBJECTS: {
      PrintObjectAction print_action;
      co_await action_on_all_objects(st, print_action, op.pgid);
      break;
    }

    case operation_type_t::LIST_OMAP: {

      std::function<ObjectStore::omap_iter_ret_t(std::string_view, std::string_view)> callback =
        [] (std::string_view key, std::string_view value) {
        if (outistty) {
          std::string cleaned_key = cleanbin_tmp(key);
          fmt::println(std::cout, "{}", cleaned_key);
        } else {
          fmt::println(std::cout, "{}", key);
        }
        return ObjectStore::omap_iter_ret_t::NEXT;
      };

      co_await st.omap_iterate(config.coll, config.ghobj, op.omap_start, callback);

      break;
    }

    case operation_type_t::SET_OMAP: {
      if (!op.key.has_value()) {
        fmt::println(std::cerr, "key is required for set-omap");
        co_return EXIT_FAILURE;
      }

      auto omap_value_result = read_input(op.file);
      if (!omap_value_result) {
        co_return omap_value_result.error();
      }
      
      bool success = co_await st.set_omap(
        config.coll, config.ghobj,
        op.key.value(), *omap_value_result);
      if (success) {
        fmt::println(std::cout, "set omap success: key={}, value size={}",
          op.key.value(), omap_value_result->size());
      } else {
        fmt::println(std::cerr, "set omap failed");
        co_return EXIT_FAILURE;
      }
      break;
    }

    case operation_type_t::GET_OMAP: {
      if (!op.key.has_value()) {
        fmt::println(std::cerr, "key is required for get-omap");
        co_return EXIT_FAILURE;
      }
      std::string omap_value = co_await st.get_omap(
        config.coll, config.ghobj, op.key.value());
      co_return co_await write_output(op.file, omap_value);
    }

    case operation_type_t::RM_OMAP: {
      if (!op.key.has_value()) {
        fmt::println(std::cerr, "omap-key is required for rm-omap");
        co_return EXIT_FAILURE;
      }
      bool success = co_await st.remove_omap(
        config.coll, config.ghobj, op.key.value());
      if (success) {
        fmt::println(std::cout, "remove omap success: key={}",
          op.key.value());
      } else {
        fmt::println(std::cerr, "remove omap failed");
        co_return EXIT_FAILURE;
      }
      break;
    }

    case operation_type_t::LIST_ATTRS: {
      auto attrs_result = co_await st.get_attrs(config.coll, config.ghobj);
      if (!attrs_result) {
        fmt::println(std::cerr, "Error listing attributes: {}", attrs_result.error());
        co_return EXIT_FAILURE;
      }
      const auto& attrs = *attrs_result;

      for (const auto& attr : attrs) {
        std::string key = attr.first;
        if (outistty) {
          key = cleanbin(key);
        }
        fmt::println(std::cout, "{}", key);
      }
      break;
    }

    case operation_type_t::SET_ATTR: {
      if (!op.key.has_value()) {
        fmt::println(std::cerr, "key is required for set-attr");
        co_return EXIT_FAILURE;
      }
      auto attr_value_result = read_input(op.file);
      if (!attr_value_result) {
        co_return attr_value_result.error();
      }
      bool success = co_await st.set_attr(
        config.coll, config.ghobj,
        op.key.value(), *attr_value_result);
      if (success) {
        fmt::println(std::cout, "set attr success: key={}, value size={}",
          op.key.value(), attr_value_result->size());
      } else {
        fmt::println(std::cerr, "set attr failed");
        co_return EXIT_FAILURE;
      }
      break;
    }

    case operation_type_t::GET_ATTR: {
      if (!op.key.has_value()) {
        fmt::println(std::cerr, "key is required for get-attr");
        co_return EXIT_FAILURE;
      }
      auto attr_result = co_await st.get_attr(
        config.coll, config.ghobj, op.key.value());

      if (!attr_result) {
        fmt::println(std::cerr, "Error reading attribute '{}': {}",
                     op.key.value(), attr_result.error());
        co_return EXIT_FAILURE;
      }

      std::string output_value = *attr_result;
      if (outistty) {
        output_value = cleanbin(output_value);
        if (!output_value.empty() && output_value.back() != '\n') {
          output_value.push_back('\n');
        }
      }
      co_return co_await write_output(op.file, output_value);
    }

    case operation_type_t::RM_ATTR: {
      if (!op.key.has_value()) {
        fmt::println(std::cerr, "key is required for rm-attr");
        co_return EXIT_FAILURE;
      }
      bool success = co_await st.remove_attr(
        config.coll, config.ghobj, op.key.value());
      if (success) {
        fmt::println(std::cout, "remove attr success: key={}",
          op.key.value());
      } else {
        fmt::println(std::cerr, "remove attr failed");
        co_return EXIT_FAILURE;
      }
      break;
    }

    case operation_type_t::GET_BYTES: {
      std::string object_data = co_await st.get_bytes(config.coll, config.ghobj);
      co_return co_await write_output(op.file, object_data);
    }

    case operation_type_t::SET_BYTES: {
      auto object_data_result = read_input(op.file);
      if (!object_data_result) {
        co_return object_data_result.error();
      }

      bool success = co_await st.set_bytes(config.coll, config.ghobj, *object_data_result);
      if (success) {
        fmt::println(std::cout, "set bytes success: data size={}", object_data_result->size());
      } else {
        fmt::println(std::cerr, "set bytes failed");
        co_return EXIT_FAILURE;
      }
      break;
    }

    case operation_type_t::REMOVE: {
      bool success = co_await st.remove_object(config.coll, config.ghobj, false, op.force);
      if (success) {
        fmt::println(std::cout, "remove object success");
      } else {
        fmt::println(std::cerr, "remove object failed");
        co_return EXIT_FAILURE;
      }
      break;
    }

    case operation_type_t::REMOVEALL: {
      bool success = co_await st.remove_object(config.coll, config.ghobj, true, op.force);
      if (success) {
        fmt::println(std::cout, "remove object and clones success");
      } else {
        fmt::println(std::cerr, "remove object and clones failed");
        co_return EXIT_FAILURE;
      }
      break;
    }

    case operation_type_t::DUMP: {
      std::string dump_info = co_await st.dump_object_info(config.coll, config.ghobj);
      co_return co_await write_output(op.file, dump_info);
    }

    case operation_type_t::SET_SIZE: {
      if (!op.size.has_value()) {
        fmt::println(std::cerr, "size is required for set-size");
        co_return EXIT_FAILURE;
      }
      bool success = co_await st.set_object_size(config.coll, config.ghobj, op.size.value());
      if (success) {
        fmt::println(std::cout, "set object size success: size={}", op.size.value());
      } else {
        fmt::println(std::cerr, "set object size failed");
        co_return EXIT_FAILURE;
      }
      break;
    }

    case operation_type_t::CLEAR_DATA_DIGEST: {
      bool success = co_await st.clear_data_digest(config.coll, config.ghobj);
      if (success) {
        fmt::println(std::cout, "clear data digest success");
      } else {
        fmt::println(std::cerr, "clear data digest failed");
        co_return EXIT_FAILURE;
      }
      break;
    }

    default:
      fmt::println(std::cerr, "Operation {} not implemented yet", to_string(op.op));
      co_return EXIT_FAILURE;
  }

  std::cout.flush();
  co_return EXIT_SUCCESS;
}

int main(int argc, const char* argv[])
{
  bpo::options_description desc("Allowed options");
  objectstore_config_t config;
  config.populate_options(desc);

  bpo::options_description positional("Positional options");
  positional.add_options()
    ("object", bpo::value<std::string>(&config.object), 
     "'' for pgmeta_oid, object name or ghobject in json")
    ("objcmd", bpo::value<std::string>(&config.objcmd), 
     "command [(get|set)-bytes, (get|set|rm)-(attr|omap), list-attrs, list-omap, remove, removeall, dump, set-size, clear-data-digest]")
    ("arg1", bpo::value<std::string>(&config.arg1), "arg1 based on cmd")
    ("arg2", bpo::value<std::string>(&config.arg2), "arg2 based on cmd")
    ;

  bpo::options_description all;
  all.add(desc).add(positional);

  bpo::positional_options_description pd;
  pd.add("object", 1).add("objcmd", 1).add("arg1", 1).add("arg2", 1);

  bpo::variables_map vm;
  std::vector<std::string> ceph_option_strings;
  
  try {
    auto parsed = bpo::command_line_parser(argc, argv)
      .options(all)
      .allow_unregistered()
      .positional(pd)
      .run();
    bpo::store(parsed, vm);
    bpo::notify(vm);
    ceph_option_strings = bpo::collect_unrecognized(parsed.options, bpo::include_positional);
  } catch (const bpo::error& e) {
    fmt::println(std::cerr, "Error: {}", e.what());
    print_usage(desc);
    return EXIT_FAILURE;
  }

  if (vm.count("help")) {
    print_usage(desc);
    return EXIT_SUCCESS;
  }

  outistty = isatty(STDOUT_FILENO) || config.tty;

  // Parse operation type
  if (vm.count("objcmd")) {
    // Object-level operation
    auto op_result = parse_object_operation(config.objcmd);
    if (!op_result) {
      fmt::println(std::cerr, "Invalid object command: {}", op_result.error());
      print_usage(desc);
      return EXIT_FAILURE;
    }

    std::optional<pg_t> pgid;
    if (vm.count("pgid")) {
      auto pgid_result = parse_pgid(config.pgid_str);
      if (!pgid_result) {
        fmt::println(std::cerr, "Invalid pgid: {}", pgid_result.error());
        return EXIT_FAILURE;
      }
      pgid = *pgid_result;
    }

    std::optional<std::string> object_str;
    if (vm.count("object")) {
      object_str = config.object;
    }

    operation_params_t params;
    params.op = *op_result;
    params.pgid = pgid;
    params.object = object_str;
    params.force = config.force;

    if (vm.count("file")) {
        params.file = config.file;
    }

    switch (*op_result) {
      case operation_type_t::GET_OMAP:
      case operation_type_t::RM_OMAP:
      case operation_type_t::GET_ATTR:
      case operation_type_t::RM_ATTR:
        if (!vm.count("arg1")) {
          fmt::println(std::cerr, "Missing key for command {}", config.objcmd);
          return EXIT_FAILURE;
        }
        params.key = config.arg1;
        break;

      case operation_type_t::SET_OMAP:
      case operation_type_t::SET_ATTR:
        if (!vm.count("arg1")) {
          fmt::println(std::cerr, "Missing key for command {}", config.objcmd);
          return EXIT_FAILURE;
        }
        params.key = config.arg1;
        if (vm.count("arg2")) {
          if (params.file.has_value()) {
              fmt::println(std::cerr, "File specified both with --file and positionally, which is ambiguous");
              return EXIT_FAILURE;
          }
          params.file = config.arg2;
        }
        break;

      case operation_type_t::SET_SIZE:
        if (!vm.count("arg1")) {
          fmt::println(std::cerr, "Missing size for command {}", config.objcmd);
          return EXIT_FAILURE;
        }
        try {
          params.size = std::stoull(config.arg1);
        } catch (const std::exception& e) {
          fmt::println(std::cerr, "Invalid size '{}': {}", config.arg1, e.what());
          return EXIT_FAILURE;
        }
        break;

      default:
        // A simple fallback for commands that might take a file argument
        if (vm.count("arg1")) {
            if (params.file.has_value()) {
              fmt::println(std::cerr, "File specified both with --file and positionally, which is ambiguous");
              return EXIT_FAILURE;
            }
          params.file = config.arg1;
        }
        break;
    }
    config.operation = params;
  } else if (vm.count("op")) {
    // PG-level operation
    auto op_result = parse_pg_operation(config.op);
    if (!op_result) {
      fmt::println(std::cerr, "Invalid operation: {}", op_result.error());
      print_usage(desc);
      return EXIT_FAILURE;
    }

    std::optional<pg_t> pgid;
    if (vm.count("pgid")) {
      auto pgid_result = parse_pgid(config.pgid_str);
      if (!pgid_result) {
        fmt::println(std::cerr, "Invalid pgid: {}", pgid_result.error());
        return EXIT_FAILURE;
      }
      pgid = *pgid_result;
    }

    config.operation = operation_params_t{
      *op_result,
      pgid,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      config.force
    };
  } else {
    fmt::println(std::cerr, "Must provide --op or object command...");
    print_usage(desc);
    return EXIT_FAILURE;
  }

  // Validate required parameters
  if (!vm.count("data-path")) {
    fmt::println(std::cerr, "Must provide --data-path");
    print_usage(desc);
    return EXIT_FAILURE;
  }

  auto seastar_args_result = get_seastar_args_from_storage(config);
  if (!seastar_args_result) {
    fmt::println(std::cerr, "{}", seastar_args_result.error());
    return EXIT_FAILURE;
  }
  auto& seastar_args = *seastar_args_result;

  seastar::app_template::seastar_options app_cfg;
  app_cfg.name = "crimson-objectstore-tool";
  app_cfg.auto_handle_sigint_sigterm = true;
  // Only show "Reactor stalled for" above 200ms
  app_cfg.reactor_opts.blocked_reactor_notify_ms.set_default_value(200);
  seastar::app_template app(std::move(app_cfg));

  std::vector<char*> seastar_argv;
  seastar_argv.push_back(const_cast<char*>(argv[0]));
  for (auto& arg : seastar_args) {
    seastar_argv.push_back(const_cast<char*>(arg.c_str()));
  }

  try {
    return app.run(
      seastar_argv.size(),
      seastar_argv.data(),
      [&] {
        return seastar::async([&] {
          try {
            sharded_conf().start(EntityName{}, std::string_view{"ceph"}).get();
            auto stop_conf = seastar::deferred_stop(sharded_conf());
            local_conf().start().get();
            seastar_apps_lib::stop_signal should_stop;
            if (config.debug) {
              seastar::global_logger_registry().set_all_loggers_level(
                seastar::log_level::debug
              );
            } else {
              seastar::global_logger_registry().set_all_loggers_level(
                seastar::log_level::error
              );
            }
            auto store = crimson::os::FuturizedStore::create(
              config.type,
              config.data_path,
              local_conf().get_config_values());
            store->start().get();
            store->mount().handle_error(
              crimson::stateful_ec::assert_failure(fmt::format(
                "error mounting object store in {}",
                config.data_path
              ).c_str())
            ).get();
            StoreTool st(std::move(store));
            auto stop_st = seastar::deferred_stop(st);
            int ret = run_tool(st, config).get();
            return ret;
          } catch (...) {
            fmt::println(std::cerr, "startup failed: {}", std::current_exception());
            return EXIT_FAILURE;
          }
        });
      }
    );
  } catch (...) {
    fmt::println(std::cerr, "FATAL: Exception during startup, aborting: {}",
               std::current_exception());
    return EXIT_FAILURE;
  }
}
