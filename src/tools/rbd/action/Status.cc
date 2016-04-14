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

static int do_show_status(librados::IoCtx &io_ctx, librbd::Image &image,
                          const char *imgname, Formatter *f)
{
  librbd::image_info_t info;
  uint8_t old_format;
  int r;
  std::string header_oid;
  std::list<obj_watch_t> watchers;

  r = image.old_format(&old_format);
  if (r < 0)
    return r;

  if (old_format) {
    header_oid = imgname;
    header_oid += RBD_SUFFIX;
  } else {
    r = image.stat(info, sizeof(info));
    if (r < 0)
      return r;

    char prefix[RBD_MAX_BLOCK_NAME_SIZE + 1];
    strncpy(prefix, info.block_name_prefix, RBD_MAX_BLOCK_NAME_SIZE);
    prefix[RBD_MAX_BLOCK_NAME_SIZE] = '\0';

    header_oid = RBD_HEADER_PREFIX;
    header_oid.append(prefix + strlen(RBD_DATA_PREFIX));
  }

  r = io_ctx.list_watchers(header_oid, &watchers);
  if (r < 0)
    return r;

  if (f)
    f->open_object_section("status");

  if (f) {
    f->open_object_section("watchers");
    for (std::list<obj_watch_t>::iterator i = watchers.begin(); i != watchers.end(); ++i) {
      f->open_object_section("watcher");
      f->dump_string("address", i->addr);
      f->dump_unsigned("client", i->watcher_id);
      f->dump_unsigned("cookie", i->cookie);
      f->close_section();
    }
    f->close_section();
  } else {
    if (watchers.size()) {
      std::cout << "Watchers:" << std::endl;
      for (std::list<obj_watch_t>::iterator i = watchers.begin();
           i != watchers.end(); ++i) {
        std::cout << "\twatcher=" << i->addr << " client." << i->watcher_id
                  << " cookie=" << i->cookie << std::endl;
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

int execute(const po::variables_map &vm) {
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
  r = utils::init_and_open_image(pool_name, image_name, "", true, &rados,
                                 &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = do_show_status(io_ctx, image, image_name.c_str(), formatter.get());
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
