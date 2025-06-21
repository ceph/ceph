// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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
#include "osd/osd_types.h"
#include "crimson/common/config_proxy.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"
#include "seastar/util/closeable.hh"
#include "seastar/util/log.hh"
#include "crimson/os/seastore/segment_manager.h"

namespace bpo = boost::program_options;
using crimson::common::sharded_conf;
using crimson::common::local_conf;

using namespace crimson::tools::kvstore;

struct objectstore_config_t {
  std::string data_path;
  std::string store_type;
  std::string pg;
  std::string object;
  std::string omap_start;
  std::string omap_key;
  std::string omap_value;
  std::string format;
  coll_t coll;
  spg_t pgid;
  unsigned int shard_id = 0;
  ghobject_t ghobj;
  std::optional<std::string> start;
  bool debug = false;
  bool list_pgs = false;
  bool list_objects = false;
  bool list_omap = false;
  bool get_omap = false;
  bool set_omap = false;
  bool remove_omap = false;

  void populate_options(bpo::options_description &desc) {
    desc.add_options()
      ("data-path",
       bpo::value<std::string>(&data_path)->required(),
       "osd data path (required)")
      ("store-type",
       bpo::value<std::string>(&store_type)->required(),
       "store type, e.g seastore (required)")
      ("debug",
       bpo::bool_switch(&debug),
       "set logger level as debug")
      ("list-pgs",
       bpo::bool_switch(&list_pgs),
       "list all pgs")
      ("list-objects",
       bpo::bool_switch(&list_objects),
       "list all objects in a pg")
      ("list-omap",
       bpo::bool_switch(&list_omap),
       "list all omap keys in an object")
      ("get-omap",
       bpo::bool_switch(&get_omap),
       "get specific omap value by key")
      ("set-omap",
       bpo::bool_switch(&set_omap),
       "set omap key-value for an object")
      ("remove-omap",
       bpo::bool_switch(&remove_omap),
       "remove omap key from an object")
      ("pg",
       bpo::value<std::string>(&pg)->default_value(""),
       "pg name")
      ("object",
       bpo::value<std::string>(&object)->default_value(""),
       "Use the object names output by list-objects command, enclosed in double quotes")
      ("omap-start",
       bpo::value<std::string>(&omap_start)->default_value(""),
       "list omap start at START")
      ("omap-key",
       bpo::value<std::string>(&omap_key)->default_value(""),
       "omap key for set operation")
      ("omap-value",
       bpo::value<std::string>(&omap_value)->default_value(""),
       "omap value for set operation")
      ("format",
       bpo::value<std::string>(&format)->default_value("json-pretty"),
       "Output format which may be json, json-pretty, xml, xml-pretty");
  }
};

class SeastoreMetaReader {
private:
  std::string m_data_path;

  size_t get_filesystem_block_size(const std::string& path) {
    struct stat st;
    if (stat(path.c_str(), &st) == 0) {
      return st.st_blksize;
    }
    return 4096;
  }

public:
  explicit SeastoreMetaReader(const std::string& path) : m_data_path(path) {}

  std::optional<crimson::os::seastore::block_sm_superblock_t> load_seastore_superblock() {
    try {
      std::string block_path = m_data_path + "/block";

      size_t block_size = get_filesystem_block_size(block_path);

      std::ifstream file(block_path, std::ios::binary);
      if (!file.is_open()) {
        logger.error("Could not open block file: {}", block_path);
        return std::nullopt;
      }

      std::vector<char> buf(block_size);
      file.read(buf.data(), block_size);

      if (!file.good() && !file.eof()) {
        logger.error("Could not read superblock from {}", block_path);
        return std::nullopt;
      }

      bufferlist bl;
      bl.append(buf.data(), block_size);

      crimson::os::seastore::block_sm_superblock_t superblock;
      auto bliter = bl.cbegin();
      decode(superblock, bliter);

      ceph_assert(ceph::encoded_sizeof<crimson::os::seastore::block_sm_superblock_t>(superblock) <
                  block_size);

      return superblock;

    } catch (const std::exception& e) {
      logger.error("Warning: Could not read seastore superblock: {}", e.what());
      return std::nullopt;
    } catch (...) {
      logger.error("Warning: Could not read seastore superblock, unknown error");
      return std::nullopt;
    }
  }

  std::optional<unsigned int> get_shard_count() {
    auto superblock = load_seastore_superblock();
    if (superblock.has_value()) {
      logger.info("Read shard count from storage: {}", superblock->shard_num);
      return superblock->shard_num;
    }
    return std::nullopt;
  }
};

static std::optional<unsigned int>
read_shard_count_from_storage(const std::string& data_path,
                              const std::string& store_type)
{
  if (store_type != "seastore") {
    return std::nullopt;
  }

  SeastoreMetaReader meta_reader(data_path);
  return meta_reader.get_shard_count();
}

static std::optional<std::vector<std::string>>
get_seastar_args_from_storage(const objectstore_config_t& config)
{
  auto shard_count = read_shard_count_from_storage(config.data_path,
                                                  config.store_type);

  if (!shard_count.has_value()) {
    if (config.store_type == "seastore") {
      logger.error("Failed to read shard configuration from storage superblock");
      logger.error("This indicates the storage is corrupted or unreadable.");
      return std::nullopt;
    } else {
      logger.info("Using default seastar configuration for {} store type", config.store_type);
      return std::vector<std::string>{};
    }
  }

  // seastore case with valid shard count
  std::vector<std::string> seastar_args;
  seastar_args.emplace_back("--smp");
  seastar_args.emplace_back(std::to_string(shard_count.value()));
  seastar_args.emplace_back("--thread-affinity");
  seastar_args.emplace_back("0");

  logger.info("Using shard configuration from storage: --smp {}", shard_count.value());
  return seastar_args;
}

seastar::future<int> run_tool(StoreTool& st, objectstore_config_t& config) {
  std::unique_ptr<Formatter> formatter(
    Formatter::create(config.format));
  if (config.list_objects ||
    config.list_omap ||
    config.get_omap ||
    config.set_omap ||
    config.remove_omap) {
    if (std::strcmp(config.pg.c_str(), "meta") == 0) {
      config.coll = coll_t::meta();
    } else {
      if (!config.pgid.parse(config.pg.c_str())) {
        logger.error("Invalid pgid '{}' specified", config.pg);
        co_return EXIT_FAILURE;
      }
      config.coll = coll_t(config.pgid);
    }
    auto pgs = co_await st.list_pgs();
    auto it = std::find_if(pgs.begin(), pgs.end(),
      [&config](const auto& pg) { return pg.first == config.coll; });
    if (it == pgs.end()) {
      logger.error("PG '{}' not found", config.pg);
      co_return EXIT_FAILURE;
    }
    config.shard_id = it->second;

    if (config.list_omap ||
        config.get_omap ||
        config.set_omap ||
        config.remove_omap) {
      if (config.object.empty()) {
        logger.info("object name is empty, use pgmeta oid");
        config.ghobj = config.pgid.make_pgmeta_oid();
      } else if (config.object == ghobject_t::SNAPMAPPER_OID) {
        config.ghobj = config.pgid.make_snapmapper_oid();
      } else {
        if (!config.ghobj.parse(config.object)) {
          logger.error("Invalid object name '{}'", config.object);
          co_return EXIT_FAILURE;
        }
      }

      if (config.list_omap &&
          !config.omap_start.empty()) {
        config.start = config.omap_start;
      }
    }
  }

  if (config.list_pgs) {
    auto pgs = co_await st.list_pgs();
    logger.info("list-pgs lens: {}", pgs.size());
    for (auto pg : pgs) {
      fmt::print(std::cout, "pg: {}, shard id: {}\n", pg.first, pg.second);
    }
  } else if (config.list_objects) {
    ghobject_t next;
    do {
      auto objs = co_await st.list_objects(config.coll, config.shard_id, next);
      next = std::get<1>(objs);
      for (auto obj : std::get<0>(objs)) {
        formatter->open_object_section("objects");
        formatter->dump_string("name", fmt::format("{}", obj));
        formatter->close_section();
      }
    } while (next != ghobject_t::get_max());
    formatter->flush(std::cout);
  } else if (config.list_omap) {
    try {
      FuturizedStore::Shard::omap_values_t omaps =
        co_await st.omap_get_values(config.coll, config.shard_id, config.ghobj, config.start);
      if (omaps.empty()) {
        if (config.format == "json" ||
            config.format == "json-pretty") {
          formatter->open_array_section("omap_keys");
          formatter->close_section();
          formatter->flush(std::cout);
        } else {
          logger.info("No omap keys found");
        }
      } else {
        if (config.format == "json" ||
            config.format == "json-pretty") {
          formatter->open_array_section("omap_keys");
          for (const auto& omap : omaps) {
            formatter->dump_string("key", omap.first);
          }
          formatter->close_section();
          formatter->flush(std::cout);
        } else {
          for (const auto& omap : omaps) {
            fmt::print(std::cout, "{}", omap.first);
          }
        }
      }
    } catch (const std::exception& e) {
      logger.error("Error reading omap values: {}", e.what());
      logger.error("This may indicate storage corruption or version mismatch");
      co_return EXIT_FAILURE;
    }
  } else if (config.set_omap) {
    if (config.omap_key.empty()) {
      logger.error("omap-key is required for set-omap");
      co_return EXIT_FAILURE;
    }
    if (config.omap_value.empty()) {
      logger.error("omap-value is required for set-omap");
      co_return EXIT_FAILURE;
    }
    bool success = co_await st.set_omap(
      config.coll, config.shard_id, config.ghobj,
      config.omap_key, config.omap_value);
    if (success) {
      fmt::print(std::cout, "set omap success: key={}, value={}\n",
        config.omap_key,
        config.omap_value);
    } else {
      logger.error("set omap failed");
      co_return EXIT_FAILURE;
    }
  } else if (config.get_omap) {
    if (config.omap_key.empty()) {
      logger.error("omap-key is required for get-omap");
      co_return EXIT_FAILURE;
    }
    try {
      std::string omap_value = co_await st.get_omap(
        config.coll, config.shard_id, config.ghobj, config.omap_key);
      if (!omap_value.empty()) {
        fmt::print(std::cout, "{}", omap_value);
      } else {
        logger.error("get omap failed");
        co_return EXIT_FAILURE;
      }
    } catch (const std::exception& e) {
      logger.error("Error reading omap value: {}", e.what());
      logger.error("This may indicate storage corruption or version mismatch");
      co_return EXIT_FAILURE;
    }
  } else if (config.remove_omap) {
    if (config.omap_key.empty()) {
      logger.error("omap-key is required for remove-omap");
      co_return EXIT_FAILURE;
    }
    try {
      bool success = co_await st.remove_omap(
        config.coll, config.shard_id, config.ghobj, config.omap_key);
      if (success) {
        fmt::print(std::cout, "remove omap success: key={}\n",
          config.omap_key);
      } else {
        logger.error("remove omap failed");
        co_return EXIT_FAILURE;
      }
    } catch (const std::exception& e) {
      logger.error("Error removing omap value: {}", e.what());
      logger.error("This may indicate storage corruption or version mismatch");
      co_return EXIT_FAILURE;
    }
  } else {
    logger.error("Invalid command");
  }
  co_return EXIT_SUCCESS;
}

int main(int argc, const char* argv[])
{
  bpo::options_description desc{"ObjectStore Tool Options"};

  objectstore_config_t config;
  config.populate_options(desc);

  desc.add_options()
    ("help,h", "produce help message");

  bpo::variables_map vm;
  std::vector<std::string> unrecognized_options;
  try {
    auto parsed = bpo::command_line_parser(argc, argv)
      .options(desc)
      .allow_unregistered()
      .run();
    bpo::store(parsed, vm);

    if (vm.count("help")) {
      std::stringstream ss;
      ss << desc;
      fmt::print(std::cout, "{}", ss.str());
      return 0;
    }

    bpo::notify(vm);
    unrecognized_options =
      bpo::collect_unrecognized(parsed.options, bpo::include_positional);
  } catch (const bpo::error& e) {
    std::stringstream ss;
    ss << desc;
    logger.error("Error: {}\n{}", e.what(), ss.str());
    return 1;
  }

  auto seastar_args_result = get_seastar_args_from_storage(config);
  if (!seastar_args_result.has_value()) {
    return EXIT_FAILURE;
  }
  auto& seastar_args = seastar_args_result.value();

  seastar::app_template::config app_cfg;
  app_cfg.name = "crimson-objectstore-tool";
  app_cfg.auto_handle_sigint_sigterm = true;
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
              seastar::log_level::warn
            );
          }
          logger.set_ostream_enabled(true);

          auto store = crimson::os::FuturizedStore::create(
            config.store_type,
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
          logger.error("startup failed: {}", std::current_exception());
          return EXIT_FAILURE;
        }
        });
      }
    );
  } catch (...) {
    logger.error("FATAL: Exception during startup, aborting: {}",
               std::current_exception());
    return EXIT_FAILURE;
  }
}
