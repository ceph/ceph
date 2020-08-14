// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_json.h"
#include "common/errno.h"
#include "common/Formatter.h"
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

struct ImageCacheState {
  bool present;
  bool clean;
  int size;
  std::string host;
  std::string path;
};

bool image_cache_parse(const std::string& s, ImageCacheState &cache_state) {
  JSONParser p;
  JSONFormattable f;
  bool success = p.parse(s.c_str(), s.size());
  if (success) {
    decode_json_obj(f, &p);
    if ((success = f.exists("present"))) {
      cache_state.present = (bool)f["present"];
    }
    if (success && (success = f.exists("clean"))) {
      cache_state.present = (bool)f["clean"];
    }
    if (success && (success = f.exists("rwl_size"))) {
      cache_state.size = (int)f["rwl_size"];
    }
    if (success && (success = f.exists("rwl_host"))) {
      cache_state.host = (std::string)f["rwl_host"];
    }
    if (success && (success = f.exists("rwl_path"))) {
      cache_state.path = (std::string)f["rwl_path"];
    }
  }
  return success;
}

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
      librados::IoCtx src_io_ctx;
      r = librados::Rados(io_ctx).ioctx_create2(migration_status.source_pool_id, src_io_ctx);
      if (r < 0) {
        source_pool_name = stringify(migration_status.source_pool_id);
      } else {
        source_pool_name = src_io_ctx.get_pool_name();
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
      default:
        migration_state = "unknown";
      }
    }
  }

  ImageCacheState cache_state;
  if (features & RBD_FEATURE_DIRTY_CACHE) {
    std::string image_cache_str;
    r = image.metadata_get(IMAGE_CACHE_STATE, &image_cache_str);
    if (r < 0) {
      std::cerr << "rbd: getting image cache status failed: " << cpp_strerror(r)
                << std::endl;
    } else {
      r = image_cache_parse(image_cache_str, cache_state);
      if (r < 0) {
        std::cerr << "rbd: image cache metadata is corrupted: " << cpp_strerror(r)
                  << std::endl;
      }
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
      f->dump_string("source_pool_name", source_pool_name);
      f->dump_string("source_pool_namespace",
                     migration_status.source_pool_namespace);
      f->dump_string("source_image_name", migration_status.source_image_name);
      f->dump_string("source_image_id", migration_status.source_image_id);
      f->dump_string("dest_pool_name", dest_pool_name);
      f->dump_string("dest_pool_namespace",
                     migration_status.dest_pool_namespace);
      f->dump_string("dest_image_name", migration_status.dest_image_name);
      f->dump_string("dest_image_id", migration_status.dest_image_id);
      f->dump_string("state", migration_state);
      f->dump_string("state_description", migration_status.state_description);
      f->close_section(); // migration
    }
    if (cache_state.present) {
        f->open_object_section("image_cache_state");
        f->dump_bool("clean", cache_state.clean);
        f->dump_int("size", cache_state.size);
        f->dump_string("host", cache_state.host);
        f->dump_string("path", cache_state.path);
	f->close_section(); // image_cache_state
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
      std::cout << "\tsource: " << source_pool_name << "/"
              << migration_status.source_image_name;
      if (!migration_status.source_image_id.empty()) {
        std::cout << " (" << migration_status.source_image_id <<  ")";
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

    if (cache_state.present) {
        std::cout << "image cache state:" << std::endl;
        std::cout << "\tclean: " << (cache_state.clean ? "true" : "false")
                  << "  size: " << byte_u_t(cache_state.size)
                  << "  host: " << cache_state.host
                  << "  path: "  << cache_state.path << std::endl;
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
