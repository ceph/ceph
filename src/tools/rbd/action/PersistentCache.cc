// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/types.h"
#include "include/rbd_types.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace persistent_cache {

namespace at = argument_types;
namespace po = boost::program_options;

void get_arguments_invalidate(po::options_description *positional,
                              po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_image_id_option(options);
}

int execute_invalidate(const po::variables_map &vm,
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

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, namespace_name, image_name, "", "",
                                 false, &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = image.invalidate_cache();
  if (r < 0) {
    std::cerr << "rbd: invalidating persistent cache failed: "
              << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

void get_arguments_flush(po::options_description *positional,
                         po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_image_id_option(options);
}

int execute_flush(const po::variables_map &vm,
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

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, namespace_name, image_name, "", "",
                                 false, &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  uint64_t features;
  r = image.features(&features);
  if (r < 0) {
    return r;
  }

  if (features & RBD_FEATURE_DIRTY_CACHE) {
    r = image.flush();
    if (r < 0) {
      std::cerr << "rbd: flushing persistent cache failed: "
                << cpp_strerror(r) << std::endl;
      return r;
    }
  } else {
    std::cout << "rbd: persistent cache is clean or disabled" << std::endl;
  }

  return 0;
}

Shell::Action action_invalidate(
  {"persistent-cache", "invalidate"}, {},
  "Invalidate (discard) existing / dirty persistent cache.", "",
  &get_arguments_invalidate, &execute_invalidate);
Shell::Action action_flush(
  {"persistent-cache", "flush"}, {}, "Flush persistent cache.", "",
  &get_arguments_flush, &execute_flush);

} // namespace persistent_cache
} // namespace action
} // namespace rbd
