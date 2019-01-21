// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace pool {

namespace at = argument_types;
namespace po = boost::program_options;

void get_arguments_init(po::options_description *positional,
                        po::options_description *options) {
  at::add_pool_options(positional, options, false);
  options->add_options()
      ("force", po::bool_switch(),
       "force initialize pool for RBD use if registered by another application");
}

int execute_init(const po::variables_map &vm,
                 const std::vector<std::string> &ceph_global_init_args) {
  std::string pool_name;
  size_t arg_index = 0;
  int r = utils::get_pool_and_namespace_names(vm, true, false, &pool_name,
                                              nullptr, &arg_index);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, "", &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.pool_init(io_ctx, vm["force"].as<bool>());
  if (r == -EOPNOTSUPP) {
    std::cerr << "rbd: luminous or later release required." << std::endl;
  } else if (r == -EPERM) {
    std::cerr << "rbd: pool already registered to a different application."
              << std::endl;
  } else if (r < 0) {
    std::cerr << "rbd: error registered application: " << cpp_strerror(r)
              << std::endl;
  }

  return 0;
}

void get_arguments_stats(po::options_description *positional,
                         po::options_description *options) {
  at::add_pool_options(positional, options, true);
  at::add_format_options(options);
}

int execute_stats(const po::variables_map &vm,
                  const std::vector<std::string> &ceph_global_init_args) {
  std::string pool_name;
  std::string namespace_name;
  size_t arg_index = 0;
  int r = utils::get_pool_and_namespace_names(vm, true, false, &pool_name,
                                              &namespace_name, &arg_index);
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
  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  uint64_t image_count;
  uint64_t provisioned_bytes;
  uint64_t snap_count;
  uint64_t trash_count;
  uint64_t trash_provisioned_bytes;
  uint64_t trash_snap_count;

  librbd::PoolStats pool_stats;
  pool_stats.add(RBD_POOL_STAT_OPTION_IMAGES, &image_count);
  pool_stats.add(RBD_POOL_STAT_OPTION_IMAGE_MAX_PROVISIONED_BYTES,
                 &provisioned_bytes);
  pool_stats.add(RBD_POOL_STAT_OPTION_IMAGE_SNAPSHOTS, &snap_count);
  pool_stats.add(RBD_POOL_STAT_OPTION_TRASH_IMAGES, &trash_count);
  pool_stats.add(RBD_POOL_STAT_OPTION_TRASH_MAX_PROVISIONED_BYTES,
                 &trash_provisioned_bytes);
  pool_stats.add(RBD_POOL_STAT_OPTION_TRASH_SNAPSHOTS, &trash_snap_count);

  r = rbd.pool_stats_get(io_ctx, &pool_stats);
  if (r < 0) {
    std::cerr << "rbd: failed to query pool stats: " << cpp_strerror(r)
              << std::endl;
    return r;
  }

  if (formatter) {
    formatter->open_object_section("stats");
    formatter->open_object_section("images");
    formatter->dump_unsigned("count", image_count);
    formatter->dump_unsigned("provisioned_bytes", provisioned_bytes);
    formatter->dump_unsigned("snap_count", snap_count);
    formatter->close_section();
    formatter->open_object_section("trash");
    formatter->dump_unsigned("count", trash_count);
    formatter->dump_unsigned("provisioned_bytes", trash_provisioned_bytes);
    formatter->dump_unsigned("snap_count", trash_snap_count);
    formatter->close_section();
    formatter->close_section();
    formatter->flush(std::cout);
  } else {
    std::cout << "Total Images: " << image_count;
    if (trash_count > 0) {
      std::cout << " (" << trash_count << " in trash)";
    }
    std::cout << std::endl;

    std::cout << "Total Snapshots: " << snap_count;
    if (trash_count > 0) {
      std::cout << " (" << trash_snap_count << " in trash)";
    }
    std::cout << std::endl;

    std::cout << "Provisioned Size: " << byte_u_t(provisioned_bytes);
    if (trash_count > 0) {
      std::cout << " (" << byte_u_t(trash_provisioned_bytes) << " in trash)";
    }
    std::cout << std::endl;
  }

  return 0;
}

Shell::Action init_action(
  {"pool", "init"}, {}, "Initialize pool for use by RBD.", "",
  &get_arguments_init, &execute_init);
Shell::Action stat_action(
  {"pool", "stats"}, {}, "Display pool statistics.",
  "Note: legacy v1 images are not included in stats",
  &get_arguments_stats, &execute_stats);

} // namespace pool
} // namespace action
} // namespace rbd
