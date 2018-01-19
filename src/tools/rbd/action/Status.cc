// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/rbd_types.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace status {

namespace at = argument_types;
namespace po = boost::program_options;

static int do_show_status(librbd::Image &image, Formatter *f)
{
  int r;
  std::list<librbd::image_watcher_t> watchers;

  r = image.list_watchers(watchers);
  if (r < 0)
    return r;

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
    f->close_section();
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
  }
  if (f) {
    f->close_section();
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
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_NONE);
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
  r = utils::init_and_open_image(pool_name, image_name, "", "", true, &rados,
                                 &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = do_show_status(image, formatter.get());
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
