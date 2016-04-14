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
namespace rename {

namespace at = argument_types;
namespace po = boost::program_options;

static int do_rename(librbd::RBD &rbd, librados::IoCtx& io_ctx,
                     const char *imgname, const char *destname)
{
  int r = rbd.rename(io_ctx, imgname, destname);
  if (r < 0)
    return r;
  return 0;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_SOURCE);
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_DEST);
}

int execute(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_SOURCE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_NONE);
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

  if (pool_name != dst_pool_name) {
    std::cerr << "rbd: mv/rename across pools not supported" << std::endl
              << "source pool: " << pool_name<< " dest pool: " << dst_pool_name
              << std::endl;
    return -EINVAL;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = do_rename(rbd, io_ctx, image_name.c_str(), dst_image_name.c_str());
  if (r < 0) {
    std::cerr << "rbd: rename error: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

Shell::Action action(
  {"rename"}, {"mv"}, "Rename image within pool.", "", &get_arguments,
  &execute);

} // namespace list
} // namespace action
} // namespace rbd
