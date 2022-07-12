// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Schedule.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/ceph_context.h"
#include "common/ceph_json.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/TextTable.h"
#include "global/global_context.h"
#include "include/stringify.h"

#include <iostream>
#include <list>
#include <map>
#include <string>
#include <boost/program_options.hpp>

#include "json_spirit/json_spirit.h"

namespace rbd {
namespace action {
namespace mirror_snapshot_schedule {

namespace at = argument_types;
namespace po = boost::program_options;

namespace {

class ScheduleStatus {
public:
  ScheduleStatus() {
  }

  int parse(const std::string &status) {
    json_spirit::mValue json_root;
    if(!json_spirit::read(status, json_root)) {
      std::cerr << "rbd: invalid schedule status JSON received" << std::endl;
      return -EBADMSG;
    }

    try {
      auto &s = json_root.get_obj();

      if (s["scheduled_images"].type() != json_spirit::array_type) {
        std::cerr << "rbd: unexpected schedule JSON received: "
                  << "scheduled_images is not array" << std::endl;
        return -EBADMSG;
      }

      for (auto &item_val : s["scheduled_images"].get_array()) {
        if (item_val.type() != json_spirit::obj_type) {
          std::cerr << "rbd: unexpected schedule status JSON received: "
                    << "schedule item is not object" << std::endl;
          return -EBADMSG;
        }

        auto &item = item_val.get_obj();

        if (item["schedule_time"].type() != json_spirit::str_type) {
          std::cerr << "rbd: unexpected schedule JSON received: "
                    << "schedule_time is not string" << std::endl;
          return -EBADMSG;
        }
        auto schedule_time = item["schedule_time"].get_str();

        if (item["image"].type() != json_spirit::str_type) {
          std::cerr << "rbd: unexpected schedule JSON received: "
                    << "image is not string" << std::endl;
          return -EBADMSG;
        }
        auto image = item["image"].get_str();

        scheduled_images.push_back({schedule_time, image});
      }

    } catch (std::runtime_error &) {
      std::cerr << "rbd: invalid schedule JSON received" << std::endl;
      return -EBADMSG;
    }

    return 0;
  }

  void dump(Formatter *f) {
    f->open_array_section("scheduled_images");
    for (auto &image : scheduled_images) {
      f->open_object_section("image");
      f->dump_string("schedule_time", image.first);
      f->dump_string("image", image.second);
      f->close_section(); // image
    }
    f->close_section(); // scheduled_images
  }

  friend std::ostream& operator<<(std::ostream& os, ScheduleStatus &d);

private:

  std::list<std::pair<std::string, std::string>> scheduled_images;
};

std::ostream& operator<<(std::ostream& os, ScheduleStatus &s) {
  TextTable tbl;
  tbl.define_column("SCHEDULE TIME", TextTable::LEFT, TextTable::LEFT);
  tbl.define_column("IMAGE", TextTable::LEFT, TextTable::LEFT);

  for (auto &[schedule_time, image] : s.scheduled_images) {
    tbl << schedule_time << image << TextTable::endrow;
  }

  os << tbl;
  return os;
}

} // anonymous namespace

void get_arguments_add(po::options_description *positional,
                       po::options_description *options) {
  add_level_spec_options(options);
  add_schedule_options(positional, true);
}

int execute_add(const po::variables_map &vm,
                const std::vector<std::string> &ceph_global_init_args) {
  std::map<std::string, std::string> args;

  int r = get_level_spec_args(vm, &args);
  if (r < 0) {
    return r;
  }
  r = get_schedule_args(vm, true, &args);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  r = utils::init_rados(&rados);
  if (r < 0) {
    return r;
  }

  normalize_level_spec_args(&args);
  r = utils::mgr_command(rados, "rbd mirror snapshot schedule add", args,
                         &std::cout, &std::cerr);
  if (r < 0) {
    return r;
  }

  return 0;
}

void get_arguments_remove(po::options_description *positional,
                          po::options_description *options) {
  add_level_spec_options(options);
  add_schedule_options(positional, false);
}

int execute_remove(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  std::map<std::string, std::string> args;

  int r = get_level_spec_args(vm, &args);
  if (r < 0) {
    return r;
  }
  r = get_schedule_args(vm, false, &args);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  r = utils::init_rados(&rados);
  if (r < 0) {
    return r;
  }

  normalize_level_spec_args(&args);
  r = utils::mgr_command(rados, "rbd mirror snapshot schedule remove", args,
                         &std::cout, &std::cerr);
  if (r < 0) {
    return r;
  }

  return 0;
}

void get_arguments_list(po::options_description *positional,
                        po::options_description *options) {
  add_level_spec_options(options);
  options->add_options()
    ("recursive,R", po::bool_switch(), "list all schedules");
  at::add_format_options(options);
}

int execute_list(const po::variables_map &vm,
                 const std::vector<std::string> &ceph_global_init_args) {
  std::map<std::string, std::string> args;

  int r = get_level_spec_args(vm, &args);
  if (r < 0) {
    return r;
  }

  at::Format::Formatter formatter;
  r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  r = utils::init_rados(&rados);
  if (r < 0) {
    return r;
  }

  normalize_level_spec_args(&args);
  std::stringstream out;
  r = utils::mgr_command(rados, "rbd mirror snapshot schedule list", args, &out,
                         &std::cerr);
  if (r < 0) {
    return r;
  }

  ScheduleList schedule_list;
  r = schedule_list.parse(out.str());
  if (r < 0) {
    return r;
  }

  if (vm["recursive"].as<bool>()) {
    if (formatter.get()) {
      schedule_list.dump(formatter.get());
      formatter->flush(std::cout);
    } else {
      std::cout << schedule_list;
    }
  } else {
    auto schedule = schedule_list.find(args["level_spec"]);
    if (schedule == nullptr) {
      return -ENOENT;
    }

    if (formatter.get()) {
      schedule->dump(formatter.get());
      formatter->flush(std::cout);
    } else {
      std::cout << *schedule << std::endl;
    }
  }

  return 0;
}

void get_arguments_status(po::options_description *positional,
                          po::options_description *options) {
  add_level_spec_options(options);
  at::add_format_options(options);
}

int execute_status(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  std::map<std::string, std::string> args;

  int r = get_level_spec_args(vm, &args);
  if (r < 0) {
    return r;
  }

  at::Format::Formatter formatter;
  r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  r = utils::init_rados(&rados);
  if (r < 0) {
    return r;
  }

  normalize_level_spec_args(&args);
  std::stringstream out;
  r = utils::mgr_command(rados, "rbd mirror snapshot schedule status", args,
                         &out, &std::cerr);
  ScheduleStatus schedule_status;
  r = schedule_status.parse(out.str());
  if (r < 0) {
    return r;
  }

  if (formatter.get()) {
    schedule_status.dump(formatter.get());
    formatter->flush(std::cout);
  } else {
    std::cout << schedule_status;
  }

  return 0;
}

Shell::Action add_action(
  {"mirror", "snapshot", "schedule", "add"}, {},
  "Add mirror snapshot schedule.", "", &get_arguments_add, &execute_add);
Shell::Action remove_action(
  {"mirror", "snapshot", "schedule", "remove"},
  {"mirror", "snapshot", "schedule", "rm"}, "Remove mirror snapshot schedule.",
  "", &get_arguments_remove, &execute_remove);
Shell::Action list_action(
  {"mirror", "snapshot", "schedule", "list"},
  {"mirror", "snapshot", "schedule", "ls"}, "List mirror snapshot schedule.",
  "", &get_arguments_list, &execute_list);
Shell::Action status_action(
  {"mirror", "snapshot", "schedule", "status"}, {},
  "Show mirror snapshot schedule status.", "", &get_arguments_status, &execute_status);

} // namespace mirror_snapshot_schedule
} // namespace action
} // namespace rbd
