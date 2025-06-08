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
#include <ostream>
#include <seastar/core/app-template.hh>
#include <seastar/core/signal.hh>
#include <string>
#include <string_view>
#include <vector>
#include <sys/stat.h>

#include "common/Formatter.h"
#include "common/hobject.h"
#include "crimson/common/errorator.h"
#include "crimson/osd/stop_signal.h"
#include "crimson/osd/osd_meta.h"

#include "objectstore_tool.h"
#include "osd/osd_types.h"
#include "seastar/core/thread.hh"
#include "crimson/common/config_proxy.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"
#include "seastar/util/closeable.hh"
#include "seastar/util/log.hh"
#include "crimson/os/seastore/segment_manager.h"

namespace bpo = boost::program_options;
using crimson::common::sharded_conf;
using crimson::common::local_conf;

struct objectstore_config_t {
  std::string data_path;
  std::string store_type;
  std::string pg;
  unsigned int shard_id = 0;
  std::string object;
  std::string omap_start;
  std::string omap_key;
  std::string omap_value;
  std::string object_namespace;
  std::string format = "json-pretty";
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
      ("shard-id",
       bpo::value<unsigned int>(&shard_id)->default_value(0),
       "which cpu core")
      ("object",
       bpo::value<std::string>(&object)->default_value(""),
       "object name")
      ("omap-start",
       bpo::value<std::string>(&omap_start)->default_value(""),
       "list omap start at START")
      ("omap-key",
       bpo::value<std::string>(&omap_key)->default_value(""),
       "omap key for set operation")
      ("omap-value",
       bpo::value<std::string>(&omap_value)->default_value(""),
       "omap value for set operation")
      ("namespace",
       bpo::value<std::string>(&object_namespace)->default_value(""),
       "object namespace (optional, default: empty)")
      ("format",
       bpo::value<std::string>(&format)->default_value("json-pretty"),
       "output format: plain|json|json-pretty (default: json-pretty)");
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
        fmt::print(std::cerr, "Warning: Could not open block file: {}\n",
                   block_path);
        return std::nullopt;
      }

      std::vector<char> buf(block_size);
      file.read(buf.data(), block_size);

      if (!file.good() && !file.eof()) {
        fmt::print(std::cerr, "Warning: Could not read superblock from {}\n",
                   block_path);
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
      fmt::print(std::cerr,
                 "Warning: Could not read seastore superblock: {}\n", e.what());
      return std::nullopt;
    } catch (...) {
      fmt::print(std::cerr,
                 "Warning: Could not read seastore superblock, unknown error\n");
      return std::nullopt;
    }
  }

  std::optional<unsigned int> get_shard_count() {
    auto superblock = load_seastore_superblock();
    if (superblock.has_value()) {
      fmt::print(std::cerr, "Read shard count from storage: {}\n",
                 superblock->shard_num);
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
      fmt::print(std::cerr,
                 "Error: Failed to read shard configuration from storage superblock\n");
      fmt::print(std::cerr,
                 "This indicates the storage is corrupted or unreadable.\n");
      return std::nullopt;
    } else {
      fmt::print(std::cerr,
                 "Using default seastar configuration for {} store type\n",
                 config.store_type);
      return std::vector<std::string>{};
    }
  }

  // seastore case with valid shard count
  std::vector<std::string> seastar_args;
  seastar_args.emplace_back("--smp");
  seastar_args.emplace_back(std::to_string(shard_count.value()));
  seastar_args.emplace_back("--thread-affinity");
  seastar_args.emplace_back("0");

  fmt::print(std::cerr, "Using shard configuration from storage: --smp {}\n",
             shard_count.value());
  return seastar_args;
}

int main(int argc, const char* argv[])
{
  bpo::options_description desc{"ObjectStore Tool Options"};

  objectstore_config_t objectstore_config;
  objectstore_config.populate_options(desc);

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
      std::cout << desc << std::endl;
      return 0;
    }

    bpo::notify(vm);
    unrecognized_options =
      bpo::collect_unrecognized(parsed.options, bpo::include_positional);
  } catch(const bpo::error& e) {
    std::cerr << "error: " << e.what() << std::endl;
    std::cerr << desc << std::endl;
    return 1;
  }

  auto seastar_args_result = get_seastar_args_from_storage(objectstore_config);
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
      [&objectstore_config] {
      return seastar::async([&objectstore_config] {
        try {
          std::unique_ptr<Formatter> formatter(
            Formatter::create(objectstore_config.format));
          coll_t coll;
          spg_t pgid;
          ghobject_t ghobj;
          std::optional<std::string> start;

          sharded_conf().start(EntityName{}, std::string_view{"ceph"}).get();
          auto stop_conf = seastar::deferred_stop(sharded_conf());
          local_conf().start().get();
          fmt::print(std::cerr, "data-path: {}, store-type: {}\n",
                     objectstore_config.data_path, objectstore_config.store_type);
          seastar_apps_lib::stop_signal should_stop;

          if (objectstore_config.debug) {
            seastar::global_logger_registry().set_all_loggers_level(
              seastar::log_level::debug
            );
          } else {
            seastar::global_logger_registry().set_all_loggers_level(
              seastar::log_level::warn
            );
          }

          auto store = crimson::os::FuturizedStore::create(
            objectstore_config.store_type,
            objectstore_config.data_path,
            local_conf().get_config_values());
          store->start().get();
          store->mount().handle_error(
            crimson::stateful_ec::assert_failure(fmt::format(
              "error mounting object store in {}",
              objectstore_config.data_path
            ).c_str())
          ).get();
          StoreTool st(std::move(store));
          auto stop_st = seastar::deferred_stop(st);

          if (objectstore_config.list_objects ||
              objectstore_config.list_omap ||
              objectstore_config.get_omap ||
              objectstore_config.set_omap ||
              objectstore_config.remove_omap) {
            if (std::strcmp(objectstore_config.pg.c_str(), "meta") == 0) {
              coll = coll_t::meta();
            } else {
              if (!pgid.parse(objectstore_config.pg.c_str())) {
                fmt::print(std::cerr, "Invalid pgid '{}' specified\n",
                           objectstore_config.pg);
                return EXIT_FAILURE;
              }
              coll = coll_t(pgid);
            }
            fmt::print(std::cout, "pg: {}, pgidstr: {}\n",
                       coll.c_str(), objectstore_config.pg);

            if (objectstore_config.list_omap ||
                objectstore_config.get_omap ||
                objectstore_config.set_omap ||
                objectstore_config.remove_omap) {
              if (objectstore_config.object.empty()) {
                fmt::print(std::cerr, "object name is empty, use pgmeta oid\n");
                ghobj = pgid.make_pgmeta_oid();
              } else if (objectstore_config.object == ghobject_t::SNAPMAPPER_OID) {
                ghobj = pgid.make_snapmapper_oid();
              } else {
                if (!ghobj.parse(objectstore_config.object)) {
                  ghobj = ghobject_t(
                    hobject_t(object_t(objectstore_config.object), "",
                              CEPH_NOSNAP, pgid.ps(), pgid.pool(),
                              objectstore_config.object_namespace),
                    ghobject_t::NO_GEN, shard_id_t(objectstore_config.shard_id));
                  fmt::print(std::cerr,
                             "Treating '{}' as simple object name in namespace '{}'\n",
                             objectstore_config.object,
                             objectstore_config.object_namespace);
                }
              }

              if (objectstore_config.list_omap &&
                  !objectstore_config.omap_start.empty()) {
                start = objectstore_config.omap_start;
              }
            }
          }

          if (objectstore_config.list_pgs) {
            auto pgs = st.list_pgs().get();
            fmt::print(std::cout, "list-pgs lens: {}\n", pgs.size());
            for (auto pg : pgs) {
              std::cout << "pg: " << pg.first
                        << ", shard id: " << pg.second << std::endl;
            }
          } else if (objectstore_config.list_objects) {
            ghobject_t next;
            do {
              auto objs = st.list_objects(coll, objectstore_config.shard_id, next).get();
              next = std::get<1>(objs);
              for (auto obj : std::get<0>(objs)) {
                formatter->open_object_section("objects");
                formatter->dump_object("name", obj);
                formatter->close_section();
              }
            } while (next != ghobject_t::get_max());
            formatter->flush(std::cout);
          } else if (objectstore_config.list_omap) {
            try {
              FuturizedStore::Shard::omap_values_t omaps =
                st.omap_get_values(coll, objectstore_config.shard_id, ghobj, start).get();
              if (omaps.empty()) {
                if (objectstore_config.format == "json" ||
                    objectstore_config.format == "json-pretty") {
                  formatter->open_array_section("omap_keys");
                  formatter->close_section();
                  formatter->flush(std::cout);
                } else {
                  fmt::print(std::cout, "No omap keys found\n");
                }
              } else {
                if (objectstore_config.format == "json" ||
                    objectstore_config.format == "json-pretty") {
                  formatter->open_array_section("omap_keys");
                  for (const auto& omap : omaps) {
                    formatter->dump_string("key", omap.first);
                  }
                  formatter->close_section();
                  formatter->flush(std::cout);
                } else {
                  for (const auto& omap : omaps) {
                    std::cout << omap.first << std::endl;
                  }
                }
              }
            } catch (const std::exception& e) {
              fmt::print(std::cerr,
                         "Error reading omap values: {}\n", e.what());
              fmt::print(std::cerr,
                         "This may indicate storage corruption or version mismatch\n");
              return EXIT_FAILURE;
            }
          } else if (objectstore_config.set_omap) {
            fmt::print(std::cerr, "set omap\n");
            if (objectstore_config.omap_key.empty()) {
              fmt::print(std::cerr, "omap-key is required for set-omap\n");
              return EXIT_FAILURE;
            }
            if (objectstore_config.omap_value.empty()) {
              fmt::print(std::cerr, "omap-value is required for set-omap\n");
              return EXIT_FAILURE;
            }
            bool success = st.set_omap(coll, objectstore_config.shard_id, ghobj,
                                       objectstore_config.omap_key,
                                       objectstore_config.omap_value).get();
            if (success) {
              fmt::print(std::cout,
                         "set omap success: key={}, value={}, namespace={}\n",
                         objectstore_config.omap_key,
                         objectstore_config.omap_value,
                         objectstore_config.object_namespace);
            } else {
              fmt::print(std::cerr, "set omap failed\n");
              return EXIT_FAILURE;
            }
          } else if (objectstore_config.get_omap) {
            fmt::print(std::cerr, "get omap\n");
            if (objectstore_config.omap_key.empty()) {
              fmt::print(std::cerr, "omap-key is required for get-omap\n");
              return EXIT_FAILURE;
            }
            try {
              std::string omap_value = st.get_omap(coll, objectstore_config.shard_id,
                                                   ghobj, objectstore_config.omap_key).get();
              if (!omap_value.empty()) {
                std::cout << omap_value << std::endl;
              } else {
                fmt::print(std::cerr, "get omap failed\n");
                return EXIT_FAILURE;
              }
            } catch (const std::exception& e) {
              fmt::print(std::cerr,
                         "Error reading omap value: {}\n", e.what());
              fmt::print(std::cerr,
                         "This may indicate storage corruption or version mismatch\n");
              return EXIT_FAILURE;
            }
          } else if (objectstore_config.remove_omap) {
            fmt::print(std::cerr, "remove omap\n");
            if (objectstore_config.omap_key.empty()) {
              fmt::print(std::cerr, "omap-key is required for remove-omap\n");
              return EXIT_FAILURE;
            }
            try {
              bool success = st.remove_omap(coll, objectstore_config.shard_id, ghobj,
                                          objectstore_config.omap_key).get();
              if (success) {
                fmt::print(std::cout,
                          "remove omap success: key={}, namespace={}\n",
                          objectstore_config.omap_key,
                          objectstore_config.object_namespace);
              } else {
                fmt::print(std::cerr, "remove omap failed\n");
                return EXIT_FAILURE;
              }
            } catch (const std::exception& e) {
              fmt::print(std::cerr,
                         "Error removing omap value: {}\n", e.what());
              fmt::print(std::cerr,
                         "This may indicate storage corruption or version mismatch\n");
              return EXIT_FAILURE;
            }
          } else {
            fmt::print(std::cerr, "Invalid command\n");
          }
          return EXIT_SUCCESS;
        } catch (...) {
          fmt::print(std::cerr, "startup failed: {}", std::current_exception());
          return EXIT_FAILURE;
        }
      });
      }
    );
  } catch (...) {
    fmt::print(std::cerr, "FATAL: Exception during startup, aborting: {}\n",
               std::current_exception());
    return EXIT_FAILURE;
  }
}