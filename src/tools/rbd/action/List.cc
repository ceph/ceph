// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/stringify.h"
#include "include/types.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/TextTable.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace list {

namespace at = argument_types;
namespace po = boost::program_options;

int do_list(librbd::RBD &rbd, librados::IoCtx& io_ctx, bool lflag,
                   Formatter *f) {
  std::vector<std::string> names;
  int r = rbd.list(io_ctx, names);
  if (r == -ENOENT)
    r = 0;
  if (r < 0)
    return r;

  if (!lflag) {
    if (f)
      f->open_array_section("images");
    for (std::vector<std::string>::const_iterator i = names.begin();
       i != names.end(); ++i) {
       if (f)
         f->dump_string("name", *i);
       else
         std::cout << *i << std::endl;
    }
    if (f) {
      f->close_section();
      f->flush(std::cout);
    }
    return 0;
  }

  TextTable tbl;

  if (f) {
    f->open_array_section("images");
  } else {
    tbl.define_column("NAME", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("SIZE", TextTable::RIGHT, TextTable::RIGHT);
    tbl.define_column("PARENT", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("FMT", TextTable::RIGHT, TextTable::RIGHT);
    tbl.define_column("PROT", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("LOCK", TextTable::LEFT, TextTable::LEFT);
  }

  std::string pool, image, snap, parent;

  for (std::vector<std::string>::const_iterator i = names.begin();
       i != names.end(); ++i) {
    librbd::image_info_t info;
    librbd::Image im;

    r = rbd.open_read_only(io_ctx, im, i->c_str(), NULL);
    // image might disappear between rbd.list() and rbd.open(); ignore
    // that, warn about other possible errors (EPERM, say, for opening
    // an old-format image, because you need execute permission for the
    // class method)
    if (r < 0) {
      if (r != -ENOENT) {
        std::cerr << "rbd: error opening " << *i << ": " << cpp_strerror(r)
                  << std::endl;
      }
      // in any event, continue to next image
      continue;
    }

    // handle second-nth trips through loop
    parent.clear();
    r = im.parent_info(&pool, &image, &snap);
    if (r < 0 && r != -ENOENT)
      goto out;
    bool has_parent = false;
    if (r != -ENOENT) {
      parent = pool + "/" + image + "@" + snap;
      has_parent = true;
    }

    if (im.stat(info, sizeof(info)) < 0) {
      r = -EINVAL;
      goto out;
    }

    uint8_t old_format;
    im.old_format(&old_format);

    std::list<librbd::locker_t> lockers;
    bool exclusive;
    r = im.list_lockers(&lockers, &exclusive, NULL);
    if (r < 0)
      goto out;
    std::string lockstr;
    if (!lockers.empty()) {
      lockstr = (exclusive) ? "excl" : "shr";
    }

    if (f) {
      f->open_object_section("image");
      f->dump_string("image", *i);
      f->dump_unsigned("size", info.size);
      if (has_parent) {
        f->open_object_section("parent");
        f->dump_string("pool", pool);
        f->dump_string("image", image);
        f->dump_string("snapshot", snap);
        f->close_section();
      }
      f->dump_int("format", old_format ? 1 : 2);
      if (!lockers.empty())
        f->dump_string("lock_type", exclusive ? "exclusive" : "shared");
      f->close_section();
    } else {
      tbl << *i
          << stringify(si_t(info.size))
          << parent
          << ((old_format) ? '1' : '2')
          << ""                         // protect doesn't apply to images
          << lockstr
          << TextTable::endrow;
    }

    std::vector<librbd::snap_info_t> snaplist;
    if (im.snap_list(snaplist) >= 0 && !snaplist.empty()) {
      for (std::vector<librbd::snap_info_t>::iterator s = snaplist.begin();
           s != snaplist.end(); ++s) {
        bool is_protected;
        bool has_parent = false;
        parent.clear();
        im.snap_set(s->name.c_str());
        r = im.snap_is_protected(s->name.c_str(), &is_protected);
        if (r < 0)
          goto out;
        if (im.parent_info(&pool, &image, &snap) >= 0) {
          parent = pool + "/" + image + "@" + snap;
          has_parent = true;
        }
        if (f) {
          f->open_object_section("snapshot");
          f->dump_string("image", *i);
          f->dump_string("snapshot", s->name);
          f->dump_unsigned("size", s->size);
          if (has_parent) {
            f->open_object_section("parent");
            f->dump_string("pool", pool);
            f->dump_string("image", image);
            f->dump_string("snapshot", snap);
            f->close_section();
          }
          f->dump_int("format", old_format ? 1 : 2);
          f->dump_string("protected", is_protected ? "true" : "false");
          f->close_section();
        } else {
          tbl << *i + "@" + s->name
              << stringify(si_t(s->size))
              << parent
              << ((old_format) ? '1' : '2')
              << (is_protected ? "yes" : "")
              << ""                     // locks don't apply to snaps
              << TextTable::endrow;
        }
      }
    }
  }

out:
  if (f) {
    f->close_section();
    f->flush(std::cout);
  } else if (!names.empty()) {
    std::cout << tbl;
  }

  return r < 0 ? r : 0;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  options->add_options()
    ("long,l", po::bool_switch(), "long listing format");
  at::add_pool_options(positional, options);
  at::add_format_options(options);
}

int execute(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  at::Format::Formatter formatter;
  int r = utils::get_formatter(vm, &formatter);
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
  r = do_list(rbd, io_ctx, vm["long"].as<bool>(), formatter.get());
  if (r < 0) {
    std::cerr << "rbd: list: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

Shell::SwitchArguments switched_arguments({"long", "l"});
Shell::Action action(
  {"list"}, {"ls"}, "List rbd images.", "", &get_arguments, &execute);

} // namespace list
} // namespace action
} // namespace rbd
