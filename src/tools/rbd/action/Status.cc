// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/errno.h"
#include "common/Formatter.h"
#include "json_spirit/json_spirit.h"
#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/rbd_types.h"
#include "include/stringify.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace status {

namespace at = argument_types;
namespace po = boost::program_options;

namespace {

const std::string IMAGE_CACHE_STATE = ".librbd/image_cache_state";

} // anonymous namespace

static int do_show_status(librados::IoCtx& io_ctx, const std::string &image_name,
                          librbd::Image &image, Formatter *f)
{
  int r;
  std::list<librbd::image_watcher_t> watchers;

  r = image.list_watchers(watchers);
  if (r < 0)
    return r;

  uint64_t features;
  r = image.features(&features);
  if (r < 0) {
    return r;
  }

  librbd::image_migration_status_t migration_status;
  std::string source_spec;
  std::string source_pool_name;
  std::string dest_pool_name;
  std::string migration_state;
  if ((features & RBD_FEATURE_MIGRATING) != 0) {
    r = librbd::RBD().migration_status(io_ctx, image_name.c_str(),
                                       &migration_status,
                                       sizeof(migration_status));
    if (r < 0) {
      std::cerr << "rbd: getting migration status failed: " << cpp_strerror(r)
                << std::endl;
      // not fatal
    } else {
      if (migration_status.source_pool_id >= 0) {
        librados::IoCtx src_io_ctx;
        r = librados::Rados(io_ctx).ioctx_create2(migration_status.source_pool_id, src_io_ctx);
        if (r < 0) {
          source_pool_name = stringify(migration_status.source_pool_id);
        } else {
          source_pool_name = src_io_ctx.get_pool_name();
        }
      } else {
        r = image.get_migration_source_spec(&source_spec);
        if (r < 0) {
          std::cerr << "rbd: getting migration source spec failed: "
                    << cpp_strerror(r) << std::endl;
        }
      }

      librados::IoCtx dst_io_ctx;
      r = librados::Rados(io_ctx).ioctx_create2(migration_status.dest_pool_id, dst_io_ctx);
      if (r < 0) {
        dest_pool_name = stringify(migration_status.dest_pool_id);
      } else {
        dest_pool_name = dst_io_ctx.get_pool_name();
      }

      switch (migration_status.state) {
      case RBD_IMAGE_MIGRATION_STATE_ERROR:
        migration_state = "error";
        break;
      case RBD_IMAGE_MIGRATION_STATE_PREPARING:
        migration_state = "preparing";
        break;
      case RBD_IMAGE_MIGRATION_STATE_PREPARED:
        migration_state = "prepared";
        break;
      case RBD_IMAGE_MIGRATION_STATE_EXECUTING:
        migration_state = "executing";
        break;
      case RBD_IMAGE_MIGRATION_STATE_EXECUTED:
        migration_state = "executed";
        break;
      case RBD_IMAGE_MIGRATION_STATE_ABORTING:
        migration_state = "aborting";
        break;
      default:
        migration_state = "unknown";
      }
    }
  }

  struct {
    // decoded
    std::string host;
    std::string path;
    uint64_t size;
    std::string mode;
    std::string stats_timestamp;
    bool present;
    bool empty;
    bool clean;
    uint64_t allocated_bytes;
    uint64_t cached_bytes;
    uint64_t dirty_bytes;
    uint64_t free_bytes;
    uint64_t hits_full;
    uint64_t hits_partial;
    uint64_t misses;
    uint64_t hit_bytes;
    uint64_t miss_bytes;

    // calculated
    uint64_t total_read_ops;
    uint64_t total_read_bytes;
    int hits_full_percent;
    int hits_partial_percent;
    int hit_bytes_percent;
  } cache_state;
  std::string cache_str;
  if (features & RBD_FEATURE_DIRTY_CACHE) {
    r = image.metadata_get(IMAGE_CACHE_STATE, &cache_str);
    if (r < 0) {
      std::cerr << "rbd: getting persistent cache state failed: " << cpp_strerror(r)
                << std::endl;
      // not fatal
    }
    json_spirit::mValue json_root;
    if (!json_spirit::read(cache_str.c_str(), json_root)) {
      std::cerr << "rbd: parsing persistent cache state failed" << std::endl;
      cache_str.clear();
    } else {
      try {
        auto& o = json_root.get_obj();
        cache_state.host = o["host"].get_str();
        cache_state.path = o["path"].get_str();
        cache_state.size = o["size"].get_uint64();
        cache_state.mode = o["mode"].get_str();
        time_t stats_timestamp_sec = o["stats_timestamp"].get_uint64();
        cache_state.stats_timestamp = ctime(&stats_timestamp_sec);
        cache_state.stats_timestamp.pop_back();
        cache_state.present = o["present"].get_bool();
        cache_state.empty = o["empty"].get_bool();
        cache_state.clean = o["clean"].get_bool();
        cache_state.allocated_bytes = o["allocated_bytes"].get_uint64();
        cache_state.cached_bytes = o["cached_bytes"].get_uint64();
        cache_state.dirty_bytes = o["dirty_bytes"].get_uint64();
        cache_state.free_bytes = o["free_bytes"].get_uint64();
        cache_state.hits_full = o["hits_full"].get_uint64();
        cache_state.hits_partial = o["hits_partial"].get_uint64();
        cache_state.misses = o["misses"].get_uint64();
        cache_state.hit_bytes = o["hit_bytes"].get_uint64();
        cache_state.miss_bytes = o["miss_bytes"].get_uint64();
      } catch (std::runtime_error &e) {
        std::cerr << "rbd: parsing persistent cache state failed: " << e.what()
                  << std::endl;
        cache_str.clear();
      }
      cache_state.total_read_ops = cache_state.hits_full +
          cache_state.hits_partial + cache_state.misses;
      cache_state.total_read_bytes = cache_state.hit_bytes +
          cache_state.miss_bytes;
      cache_state.hits_full_percent = utils::get_percentage(
          cache_state.hits_full, cache_state.total_read_ops);
      cache_state.hits_partial_percent = utils::get_percentage(
          cache_state.hits_partial, cache_state.total_read_ops);
      cache_state.hit_bytes_percent = utils::get_percentage(
          cache_state.hit_bytes, cache_state.total_read_bytes);
    }
  }

  if (f)
    f->open_object_section("status");

  if (f) {
    f->open_array_section("watchers");
    for (auto &watcher : watchers) {
      f->open_object_section("watcher");
      f->dump_string("address", watcher.addr);
      f->dump_unsigned("client", watcher.id);
      f->dump_unsigned("cookie", watcher.cookie);
      f->close_section();
    }
    f->close_section(); // watchers
    if (!migration_state.empty()) {
      f->open_object_section("migration");
      if (!source_spec.empty()) {
        f->dump_string("source_spec", source_spec);
      } else {
        f->dump_string("source_pool_name", source_pool_name);
        f->dump_string("source_pool_namespace",
                       migration_status.source_pool_namespace);
        f->dump_string("source_image_name", migration_status.source_image_name);
        f->dump_string("source_image_id", migration_status.source_image_id);
      }
      f->dump_string("dest_pool_name", dest_pool_name);
      f->dump_string("dest_pool_namespace",
                     migration_status.dest_pool_namespace);
      f->dump_string("dest_image_name", migration_status.dest_image_name);
      f->dump_string("dest_image_id", migration_status.dest_image_id);
      f->dump_string("state", migration_state);
      f->dump_string("state_description", migration_status.state_description);
      f->close_section(); // migration
    }
    if (!cache_str.empty()) {
      f->open_object_section("persistent_cache");
      f->dump_string("host", cache_state.host);
      f->dump_string("path", cache_state.path);
      f->dump_unsigned("size", cache_state.size);
      f->dump_string("mode", cache_state.mode);
      f->dump_string("stats_timestamp", cache_state.stats_timestamp);
      f->dump_bool("present", cache_state.present);
      f->dump_bool("empty", cache_state.empty);
      f->dump_bool("clean", cache_state.clean);
      f->dump_unsigned("allocated_bytes", cache_state.allocated_bytes);
      f->dump_unsigned("cached_bytes", cache_state.cached_bytes);
      f->dump_unsigned("dirty_bytes", cache_state.dirty_bytes);
      f->dump_unsigned("free_bytes", cache_state.free_bytes);
      f->dump_unsigned("hits_full", cache_state.hits_full);
      f->dump_int("hits_full_percent", cache_state.hits_full_percent);
      f->dump_unsigned("hits_partial", cache_state.hits_partial);
      f->dump_int("hits_partial_percent", cache_state.hits_partial_percent);
      f->dump_unsigned("misses", cache_state.misses);
      f->dump_unsigned("hit_bytes", cache_state.hit_bytes);
      f->dump_int("hit_bytes_percent", cache_state.hit_bytes_percent);
      f->dump_unsigned("miss_bytes", cache_state.miss_bytes);
      f->close_section(); // persistent_cache
    }
  } else {
    if (watchers.size()) {
      std::cout << "Watchers:" << std::endl;
      for (auto &watcher : watchers) {
        std::cout << "\twatcher=" << watcher.addr << " client." << watcher.id
                  << " cookie=" << watcher.cookie << std::endl;
      }
    } else {
      std::cout << "Watchers: none" << std::endl;
    }
    if (!migration_state.empty()) {
      if (!migration_status.source_pool_namespace.empty()) {
        source_pool_name += ("/" + migration_status.source_pool_namespace);
      }
      if (!migration_status.dest_pool_namespace.empty()) {
        dest_pool_name += ("/" + migration_status.dest_pool_namespace);
      }

      std::cout << "Migration:" << std::endl;
      std::cout << "\tsource: ";
      if (!source_spec.empty()) {
        std::cout << source_spec;
      } else {
        std::cout << source_pool_name << "/"
                  << migration_status.source_image_name;
        if (!migration_status.source_image_id.empty()) {
          std::cout << " (" << migration_status.source_image_id <<  ")";
        }
      }
      std::cout << std::endl;
      std::cout << "\tdestination: " << dest_pool_name << "/"
                << migration_status.dest_image_name << " ("
                << migration_status.dest_image_id <<  ")" << std::endl;
      std::cout << "\tstate: " << migration_state;
      if (!migration_status.state_description.empty()) {
        std::cout << " (" << migration_status.state_description <<  ")";
      }
      std::cout << std::endl;
    }
    if (!cache_str.empty()) {
      std::cout << "Persistent cache state:" << std::endl;
      std::cout << "\thost: " << cache_state.host << std::endl;
      std::cout << "\tpath: " << cache_state.path << std::endl;
      std::cout << "\tsize: " << byte_u_t(cache_state.size) << std::endl;
      std::cout << "\tmode: " << cache_state.mode << std::endl;
      std::cout << "\tstats_timestamp: " << cache_state.stats_timestamp
                << std::endl;
      std::cout << "\tpresent: " << (cache_state.present ? "true" : "false")
                << "\tempty: " << (cache_state.empty ? "true" : "false")
                << "\tclean: " << (cache_state.clean ? "true" : "false")
                << std::endl;
      std::cout << "\tallocated: " << byte_u_t(cache_state.allocated_bytes)
                << std::endl;
      std::cout << "\tcached: " << byte_u_t(cache_state.cached_bytes)
                << std::endl;
      std::cout << "\tdirty: " << byte_u_t(cache_state.dirty_bytes) << std::endl;
      std::cout << "\tfree: " << byte_u_t(cache_state.free_bytes) << std::endl;
      std::cout << "\thits_full: " << cache_state.hits_full << " / "
                << cache_state.hits_full_percent << "%" << std::endl;
      std::cout << "\thits_partial: " << cache_state.hits_partial << " / "
                << cache_state.hits_partial_percent << "%" << std::endl;
      std::cout << "\tmisses: " << cache_state.misses << std::endl;
      std::cout << "\thit_bytes: " << byte_u_t(cache_state.hit_bytes) << " / "
                << cache_state.hit_bytes_percent << "%" << std::endl;
      std::cout << "\tmiss_bytes: " << byte_u_t(cache_state.miss_bytes)
                << std::endl;
    }
  }

  if (f) {
    f->close_section(); // status
    f->flush(std::cout);
  }

  return 0;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_format_options(options);
}

int execute(const po::variables_map &vm,
            const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &namespace_name,
    &image_name, &snap_name, true, utils::SNAPSHOT_PRESENCE_NONE,
    utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  at::Format::Formatter formatter;
  r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, namespace_name, image_name, "", "",
                                 true, &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = do_show_status(io_ctx, image_name, image, formatter.get());
  if (r < 0) {
    std::cerr << "rbd: show status failed: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

Shell::Action action(
  {"status"}, {}, "Show the status of this image.", "", &get_arguments,
  &execute);

} // namespace status
} // namespace action
} // namespace rbd
