// -*- mode:C++; tab-width:8; c-basic-offsset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace children {

namespace at = argument_types;
namespace po = boost::program_options;

int do_list_children(librbd::Image &image, Formatter *f)
{
  std::set<std::pair<std::string, std::string> > children;
  int r;

  r = image.list_children(&children);
  if (r < 0)
    return r;

  if (f)
    f->open_array_section("children");

  for (auto &child_it : children) {
    if (f) {
      f->open_object_section("child");
      f->dump_string("pool", child_it.first);
      f->dump_string("image", child_it.second);
      f->close_section();
    } else {
      std::cout << child_it.first << "/" << child_it.second << std::endl;
    }
  }

  if (f) {
    f->close_section();
    f->flush(std::cout);
  }

  return 0;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_snap_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_format_options(options);
}

int execute(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_REQUIRED, utils::SPEC_VALIDATION_NONE);
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
  r = utils::init_and_open_image(pool_name, image_name, snap_name, true,
                                 &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = do_list_children(image, formatter.get());
  if (r < 0) {
    std::cerr << "rbd: listing children failed: " << cpp_strerror(r)
              << std::endl;
    return r;
  }
  return 0;
}

Shell::Action action(
  {"children"}, {}, "Display children of snapshot.", "", &get_arguments,
  &execute);

} // namespace children
} // namespace action
} // namespace rbd
