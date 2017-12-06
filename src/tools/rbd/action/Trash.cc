// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "common/Formatter.h"
#include "common/TextTable.h"
#include "common/Clock.h"
#include <iostream>
#include <sstream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace trash {

namespace at = argument_types;
namespace po = boost::program_options;


void get_move_arguments(po::options_description *positional,
                        po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  options->add_options()
    (at::DELAY.c_str(), po::value<uint64_t>(),
     "time delay in seconds until effectively remove the image");
}

int execute_move(const po::variables_map &vm) {
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

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  uint64_t delay = 0;
  if (vm.find(at::DELAY) != vm.end()) {
    delay = vm[at::DELAY].as<uint64_t>();
  }

  librbd::RBD rbd;
  r = rbd.trash_move(io_ctx, image_name.c_str(), delay);
  if (r < 0) {
    std::cerr << "rbd: deferred delete error: " << cpp_strerror(r)
              << std::endl;
  }

  return r;

}

void get_remove_arguments(po::options_description *positional,
                          po::options_description *options) {
  positional->add_options()
    (at::IMAGE_ID.c_str(), "image id\n(example: [<pool-name>/]<image-id>)");
  at::add_pool_option(options, at::ARGUMENT_MODIFIER_NONE);
  at::add_image_id_option(options);

  at::add_no_progress_option(options);
  options->add_options()
      ("force", po::bool_switch(), "force remove of non-expired delayed images");
}

int execute_remove(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_id;
  int r = utils::get_pool_image_id(vm, &arg_index, &pool_name, &image_id);
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

  utils::ProgressContext pc("Removing image", vm[at::NO_PROGRESS].as<bool>());
  r = rbd.trash_remove_with_progress(io_ctx, image_id.c_str(),
                                     vm["force"].as<bool>(), pc);
  if (r < 0) {
    if (r == -ENOTEMPTY) {
      std::cerr << "rbd: image has snapshots - these must be deleted"
                << " with 'rbd snap purge' before the image can be removed."
                << std::endl;
    } else if (r == -EBUSY) {
      std::cerr << "rbd: error: image still has watchers"
                << std::endl
                << "This means the image is still open or the client using "
                << "it crashed. Try again after closing/unmapping it or "
                << "waiting 30s for the crashed client to timeout."
                << std::endl;
    } else if (r == -EMLINK) {
      std::cerr << std::endl
		<< "Remove the image from the consistency group and try again."
		<< std::endl;
    } else if (r == -EPERM) {
      std::cerr << std::endl
                << "Deferment time has not expired, please use --force if you "
                << "really want to remove the image"
                << std::endl;
    } else {
      std::cerr << "rbd: remove error: " << cpp_strerror(r) << std::endl;
    }
    pc.fail();
    return r;
  }

  pc.finish();

  return r;
}

std::string delete_status(time_t deferment_end_time) {
  time_t now = ceph_clock_gettime();

  std::string time_str = ctime(&deferment_end_time);
  time_str = time_str.substr(0, time_str.length() - 1);

  std::stringstream ss;
  if (now < deferment_end_time) {
    ss << "protected until " << time_str;
  }

  return ss.str();
}

int do_list(librbd::RBD &rbd, librados::IoCtx& io_ctx, bool long_flag,
            bool all_flag, Formatter *f) {
  std::vector<librbd::trash_image_info_t> trash_entries;
  int r = rbd.trash_list(io_ctx, trash_entries);
  if (r < 0) {
    return r;
  }

  if (!long_flag) {
    if (f) {
      f->open_array_section("trash");
    }
    for (const auto& entry : trash_entries) {
      if (!all_flag &&
          entry.source == RBD_TRASH_IMAGE_SOURCE_MIRRORING) {
        continue;
      }
       if (f) {
         f->dump_string("id", entry.id);
         f->dump_string("name", entry.name);
       } else {
         std::cout << entry.id << " " << entry.name << std::endl;
       }
    }
    if (f) {
      f->close_section();
      f->flush(std::cout);
    }
    return 0;
  }

  TextTable tbl;

  if (f) {
    f->open_array_section("trash");
  } else {
    tbl.define_column("ID", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("NAME", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("SOURCE", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("DELETED_AT", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("STATUS", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("PARENT", TextTable::LEFT, TextTable::LEFT);
  }

  for (const auto& entry : trash_entries) {
    if (!all_flag &&
        entry.source == RBD_TRASH_IMAGE_SOURCE_MIRRORING) {
      continue;
    }
    librbd::Image im;

    r = rbd.open_by_id_read_only(io_ctx, im, entry.id.c_str(), NULL);
    // image might disappear between rbd.list() and rbd.open(); ignore
    // that, warn about other possible errors (EPERM, say, for opening
    // an old-format image, because you need execute permission for the
    // class method)
    if (r < 0) {
      if (r != -ENOENT) {
        std::cerr << "rbd: error opening " << entry.id << ": "
                  << cpp_strerror(r) << std::endl;
      }
      // in any event, continue to next image
      continue;
    }

    std::string del_source;
    switch (entry.source) {
      case RBD_TRASH_IMAGE_SOURCE_USER:
        del_source = "USER";
        break;
      case RBD_TRASH_IMAGE_SOURCE_MIRRORING:
        del_source = "MIRRORING";
        break;
    }

    std::string time_str = ctime(&entry.deletion_time);
    time_str = time_str.substr(0, time_str.length() - 1);

    std::string pool, image, snap, parent;
    r = im.parent_info(&pool, &image, &snap);
    if (r < 0 && r != -ENOENT)
      return r;
    bool has_parent = false;
    if (r != -ENOENT) {
      parent = pool + "/" + image + "@" + snap;
      has_parent = true;
    }

    if (f) {
      f->open_object_section("image");
      f->dump_string("id", entry.id);
      f->dump_string("name", entry.name);
      f->dump_string("source", del_source);
      f->dump_string("deleted_at", time_str);
      f->dump_string("status",
                     delete_status(entry.deferment_end_time));
      if (has_parent) {
        f->open_object_section("parent");
        f->dump_string("pool", pool);
        f->dump_string("image", image);
        f->dump_string("snapshot", snap);
        f->close_section();
      }
      f->close_section();
    } else {
      tbl << entry.id
          << entry.name
          << del_source
          << time_str
          << delete_status(entry.deferment_end_time);
      if (has_parent)
        tbl << parent;
      tbl << TextTable::endrow;
    }
  }

  if (f) {
    f->close_section();
    f->flush(std::cout);
  } else if (!trash_entries.empty()) {
    std::cout << tbl;
  }

  return r < 0 ? r : 0;
}

void get_list_arguments(po::options_description *positional,
                        po::options_description *options) {
  at::add_pool_options(positional, options);
  options->add_options()
    ("all,a", po::bool_switch(), "list images from all sources");
  options->add_options()
    ("long,l", po::bool_switch(), "long listing format");
  at::add_format_options(options);
}

int execute_list(const po::variables_map &vm) {
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
  r = do_list(rbd, io_ctx, vm["long"].as<bool>(), vm["all"].as<bool>(),
              formatter.get());
  if (r < 0) {
    std::cerr << "rbd: trash list: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

void get_restore_arguments(po::options_description *positional,
                            po::options_description *options) {
  positional->add_options()
    (at::IMAGE_ID.c_str(), "image id\n(example: [<pool-name>/]<image-id>)");
  at::add_pool_option(options, at::ARGUMENT_MODIFIER_NONE);
  at::add_image_id_option(options);
  at::add_image_option(options, at::ARGUMENT_MODIFIER_NONE, "");
}

int execute_restore(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_id;
  int r = utils::get_pool_image_id(vm, &arg_index, &pool_name, &image_id);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  std::string name;
  if (vm.find(at::IMAGE_NAME) != vm.end()) {
    name = vm[at::IMAGE_NAME].as<std::string>();
  }

  librbd::RBD rbd;
  r = rbd.trash_restore(io_ctx, image_id.c_str(), name.c_str());
  if (r < 0) {
    if (r == -ENOENT) {
      std::cerr << "rbd: error: image does not exist in trash"
                << std::endl;
    } else if (r == -EEXIST) {
      std::cerr << "rbd: error: an image with the same name already exists, "
                << "try again with with a different name"
                << std::endl;
    } else {
      std::cerr << "rbd: restore error: " << cpp_strerror(r) << std::endl;
    }
    return r;
  }

  return r;
}


Shell::Action action_move(
    {"trash", "move"}, {"trash", "mv"}, "Move an image to the trash.", "",
    &get_move_arguments, &execute_move);

Shell::Action action_remove(
  {"trash", "remove"}, {"trash", "rm"}, "Remove an image from trash.", "",
  &get_remove_arguments, &execute_remove);

Shell::SwitchArguments switched_arguments({"long", "l"});
Shell::Action action_list(
  {"trash", "list"}, {"trash", "ls"}, "List trash images.", "",
  &get_list_arguments, &execute_list);

Shell::Action action_restore(
    {"trash", "restore"}, {}, "Restore an image from trash.", "",
    &get_restore_arguments, &execute_restore);

} // namespace trash
} // namespace action
} // namespace rbd
