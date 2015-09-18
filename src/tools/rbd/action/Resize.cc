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
namespace resize {

namespace at = argument_types;
namespace po = boost::program_options;

static int do_resize(librbd::Image& image, uint64_t size, bool no_progress)
{
  utils::ProgressContext pc("Resizing image", no_progress);
  int r = image.resize_with_progress(size, pc);
  if (r < 0) {
    pc.fail();
    return r;
  }
  pc.finish();
  return 0;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_size_option(options);
  options->add_options()
    ("allow-shrink", po::bool_switch(), "permit shrinking");
  at::add_no_progress_option(options);
}

int execute(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_NONE);
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
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, snap_name, false,
                                 &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  librbd::image_info_t info;
  r = image.stat(info, sizeof(info));
  if (r < 0) {
    std::cerr << "rbd: resize error: " << cpp_strerror(r) << std::endl;
    return r;
  }

  if (info.size > size && !vm["allow-shrink"].as<bool>()) {
    std::cerr << "rbd: shrinking an image is only allowed with the "
              << "--allow-shrink flag" << std::endl;
    return -EINVAL;
  }

  r = do_resize(image, size, vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    std::cerr << "rbd: resize error: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

Shell::SwitchArguments switched_arguments({"allow-shrink"});
Shell::Action action(
  {"resize"}, {}, "Resize (expand or shrink) image.", "", &get_arguments,
  &execute);

} // namespace list
} // namespace action
} // namespace rbd
