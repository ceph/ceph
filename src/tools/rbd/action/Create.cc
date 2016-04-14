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
namespace create {

namespace at = argument_types;
namespace po = boost::program_options;

static int do_create(librbd::RBD &rbd, librados::IoCtx& io_ctx,
                     const char *imgname, uint64_t size,
		     librbd::ImageOptions& opts) {
  int r;
  uint64_t format;
  r = opts.get(RBD_IMAGE_OPTION_FORMAT, &format);
  assert(r == 0);
  if (format == 1) {
    uint64_t order;
    r = opts.get(RBD_IMAGE_OPTION_ORDER, &order);
    assert(r == 0);
    int order_ = order;
    r = rbd.create(io_ctx, imgname, size, &order_);
  } else {
    r = rbd.create4(io_ctx, imgname, size, opts);
  }
  if (r < 0) {
    return r;
  }
  return 0;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_create_image_options(options, true);
  at::add_size_option(options);
}

int execute(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_FULL);
  if (r < 0) {
    return r;
  }

  librbd::ImageOptions opts;
  r = utils::get_image_options(vm, true, &opts);
  if (r < 0) {
    return r;
  }

  uint64_t size;
  r = utils::get_image_size(vm, &size);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = do_create(rbd, io_ctx, image_name.c_str(), size, opts);
  if (r < 0) {
    std::cerr << "rbd: create error: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

Shell::Action action(
  {"create"}, {}, "Create an empty image.", at::get_long_features_help(),
  &get_arguments, &execute);

} // namespace create
} // namespace action
} // namespace rbd
