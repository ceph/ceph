// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/Context.h"
#include "include/stringify.h"
#include "include/types.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/TextTable.h"
#include <iostream>
#include <boost/program_options.hpp>
#include "global/global_context.h"

namespace rbd {

namespace action {
namespace list {

namespace at = argument_types;
namespace po = boost::program_options;

enum WorkerState {
  STATE_IDLE = 0,
  STATE_OPENED,
  STATE_DONE
} ;

struct WorkerEntry {
  librbd::Image img;
  librbd::RBD::AioCompletion* completion;
  WorkerState state;
  string name;

  WorkerEntry() {
    state = STATE_IDLE;
    completion = nullptr;
  }
};


int list_process_image(librados::Rados* rados, WorkerEntry* w, bool lflag, Formatter *f, TextTable &tbl)
{
  int r = 0;
  librbd::image_info_t info;
  std::string pool, image, snap, parent;

  // handle second-nth trips through loop
  r = w->img.parent_info(&pool, &image, &snap);
  if (r < 0 && r != -ENOENT)
    return r;
  bool has_parent = false;
  if (r != -ENOENT) {
    parent = pool + "/" + image + "@" + snap;
    has_parent = true;
  }

  if (w->img.stat(info, sizeof(info)) < 0) {
    return -EINVAL;
  }

  uint8_t old_format;
  w->img.old_format(&old_format);

  std::list<librbd::locker_t> lockers;
  bool exclusive;
  r = w->img.list_lockers(&lockers, &exclusive, NULL);
  if (r < 0)
    return r;
  std::string lockstr;
  if (!lockers.empty()) {
    lockstr = (exclusive) ? "excl" : "shr";
  }

  if (f) {
    f->open_object_section("image");
    f->dump_string("image", w->name);
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
    tbl << w->name
        << stringify(si_t(info.size))
        << parent
        << ((old_format) ? '1' : '2')
        << ""                         // protect doesn't apply to images
        << lockstr
        << TextTable::endrow;
  }

  std::vector<librbd::snap_info_t> snaplist;
  if (w->img.snap_list(snaplist) >= 0 && !snaplist.empty()) {
    for (std::vector<librbd::snap_info_t>::iterator s = snaplist.begin();
         s != snaplist.end(); ++s) {
      bool is_protected;
      bool has_parent = false;
      parent.clear();
      w->img.snap_set(s->name.c_str());
      r = w->img.snap_is_protected(s->name.c_str(), &is_protected);
      if (r < 0)
        return r;
      if (w->img.parent_info(&pool, &image, &snap) >= 0) {
        parent = pool + "/" + image + "@" + snap;
        has_parent = true;
      }
      if (f) {
        f->open_object_section("snapshot");
        f->dump_string("image", w->name);
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
        tbl << w->name + "@" + s->name
            << stringify(si_t(s->size))
            << parent
            << ((old_format) ? '1' : '2')
            << (is_protected ? "yes" : "")
            << ""                     // locks don't apply to snaps
            << TextTable::endrow;
      }
    }
  }

  return 0;
}

int do_list(std::string &pool_name, bool lflag, int threads, Formatter *f) {
  std::vector<WorkerEntry*> workers;
  std::vector<std::string> names;
  librados::Rados rados;
  librbd::RBD rbd;
  librados::IoCtx ioctx;

  if (threads < 1) {
    threads = 1;
  }
  if (threads > 32) {
    threads = 32;
  }

  int r = utils::init(pool_name, &rados, &ioctx);
  if (r < 0) {
    return r;
  }

  r = rbd.list(ioctx, names);
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

  for (int left = 0; left < std::min(threads, (int)names.size()); left++) {
    workers.push_back(new WorkerEntry());
  }

  auto i = names.begin();
  while (true) {
    size_t workers_idle = 0;
    for (auto comp : workers) {
      switch (comp->state) {
	case STATE_DONE:
	  comp->completion->wait_for_complete();
	  comp->state = STATE_IDLE;
	  comp->completion->release();
	  comp->completion = nullptr;
	  // we want it to fall through in this case
	case STATE_IDLE:
	  if (i == names.end()) {
	    workers_idle++;
	    continue;
	  }
	  comp->name = *i;
	  comp->completion = new librbd::RBD::AioCompletion(nullptr, nullptr);
	  r = rbd.aio_open_read_only(ioctx, comp->img, i->c_str(), NULL, comp->completion);
	  i++;
	  comp->state = STATE_OPENED;
	  break;
	case STATE_OPENED:
	  comp->completion->wait_for_complete();
	  // image might disappear between rbd.list() and rbd.open(); ignore
	  // that, warn about other possible errors (EPERM, say, for opening
	  // an old-format image, because you need execute permission for the
	  // class method)
	  r = comp->completion->get_return_value();
	  comp->completion->release();
	  if (r < 0) {
	    if (r != -ENOENT) {
	      std::cerr << "rbd: error opening " << *i << ": " << cpp_strerror(r)
			<< std::endl;
	    }
	    // in any event, continue to next image
	    comp->state = STATE_IDLE;
	    continue;
	  }
	  r = list_process_image(&rados, comp, lflag, f, tbl);
	  if (r < 0) {
	      std::cerr << "rbd: error processing image  " << comp->name << ": " << cpp_strerror(r)
			<< std::endl;
	  }
	  comp->completion = new librbd::RBD::AioCompletion(nullptr, nullptr);
	  r = comp->img.aio_close(comp->completion);
	  comp->state = STATE_DONE;
	  break;
      }
    }
    if (workers_idle == workers.size()) {
	break;
    }
  }

  if (f) {
    f->close_section();
    f->flush(std::cout);
  } else if (!names.empty()) {
    std::cout << tbl;
  }

  rados.shutdown();

  for (auto comp : workers) {
    delete comp;
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

int execute(const po::variables_map &vm,
            const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  at::Format::Formatter formatter;
  int r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }

  r = do_list(pool_name, vm["long"].as<bool>(),
              g_conf->get_val<int64_t>("rbd_concurrent_management_ops"),
              formatter.get());
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
