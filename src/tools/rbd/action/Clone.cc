// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/errno.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace clone {

namespace at = argument_types;
namespace po = boost::program_options;

int do_clone(librbd::RBD &rbd, librados::IoCtx &p_ioctx,
             const char *p_name, const char *p_snapname,
             librados::IoCtx &c_ioctx, const char *c_name,
             librbd::ImageOptions& opts) {
  uint64_t features;
  int r = opts.get(RBD_IMAGE_OPTION_FEATURES, &features);
  assert(r == 0);

  if ((features & RBD_FEATURE_LAYERING) != RBD_FEATURE_LAYERING) {
    return -EINVAL;
  }

  return rbd.clone3(p_ioctx, p_name, p_snapname, c_ioctx, c_name, opts);
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_snap_spec_options(positional, options, at::ARGUMENT_MODIFIER_SOURCE);
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_DEST);
  at::add_create_image_options(options, false);
}

int execute(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_SOURCE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_REQUIRED, utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  std::string dst_pool_name;
  std::string dst_image_name;
  std::string dst_snap_name;
  r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_DEST, &arg_index, &dst_pool_name, &dst_image_name,
    &dst_snap_name, utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_FULL);
  if (r < 0) {
    return r;
  }

  librbd::ImageOptions opts;
  r = utils::get_image_options(vm, false, &opts);
  if (r < 0) {
    return r;
  }
  opts.set(RBD_IMAGE_OPTION_FORMAT, static_cast<uint64_t>(2));

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librados::IoCtx dst_io_ctx;
  r = utils::init_io_ctx(rados, dst_pool_name, &dst_io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = do_clone(rbd, io_ctx, image_name.c_str(), snap_name.c_str(), dst_io_ctx,
               dst_image_name.c_str(), opts);
  if (r < 0) {
    std::cerr << "rbd: clone error: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

Shell::Action action(
  {"clone"}, {}, "Clone a snapshot into a COW child image.",
  at::get_long_features_help(), &get_arguments, &execute);

} // namespace clone
} // namespace action
} // namespace rbd
