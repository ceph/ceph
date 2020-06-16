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
namespace snapshot_schedule {

namespace at = argument_types;
namespace po = boost::program_options;

namespace {

class Status {
public:
  Status(const std::string &name) : name(name) {
  }

  int parse(const std::string &status) {
    json_spirit::mValue json_root;
    if(!json_spirit::read(status, json_root)) {
      std::cerr << "rbd: invalid schedule status JSON received" << std::endl;
      return -EBADMSG;
    }

    try {
      auto &s = json_root.get_obj();

      if (s[name].type() != json_spirit::array_type) {
        std::cerr << "rbd: unexpected schedule JSON received: "
                  << name  << " is not array" << std::endl;
        return -EBADMSG;
      }

      for (auto &item_val : s[name].get_array()) {
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

        images.push_back({schedule_time, image});
      }

    } catch (std::runtime_error &) {
      std::cerr << "rbd: invalid schedule JSON received" << std::endl;
      return -EBADMSG;
    }

    return 0;
  }

  void dump(Formatter *f) {
    f->open_array_section(name);
    for (auto &image : images) {
      f->open_object_section("image");
      f->dump_string("schedule_time", image.first);
      f->dump_string("image", image.second);
      f->close_section(); // image
    }
    f->close_section(); // name
  }

  friend std::ostream& operator<<(std::ostream& os, Status &d);

private:

  const std::string name;
  std::list<std::pair<std::string, std::string>> images;
};

std::ostream& operator<<(std::ostream& os, Status &s) {
  TextTable tbl;
  tbl.define_column("SCHEDULE TIME", TextTable::LEFT, TextTable::LEFT);
  tbl.define_column("IMAGE", TextTable::LEFT, TextTable::LEFT);

  for (auto &[schedule_time, image] : s.images) {
    tbl << schedule_time << image << TextTable::endrow;
  }

  os << tbl;
  return os;
}

} // anonymous namespace

void get_arguments_period_add(po::options_description *positional,
                              po::options_description *options) {
  add_level_spec_options(options);
  add_schedule_options(positional);
}

int execute_period_add(const po::variables_map &vm,
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

  r = utils::mgr_command(rados, "rbd snapshot schedule period add", args,
                         &std::cout, &std::cerr);
  if (r < 0) {
    return r;
  }

  return 0;
}

void get_arguments_period_remove(po::options_description *positional,
                                 po::options_description *options) {
  add_level_spec_options(options);
  add_schedule_options(positional);
}

int execute_period_remove(
    const po::variables_map &vm,
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

  r = utils::mgr_command(rados, "rbd snapshot schedule period remove", args,
                         &std::cout, &std::cerr);
  if (r < 0) {
    return r;
  }

  return 0;
}

void get_arguments_period_list(po::options_description *positional,
                               po::options_description *options) {
  add_level_spec_options(options);
  options->add_options()
    ("recursive,R", po::bool_switch(), "list all schedules");
  at::add_format_options(options);
}

int execute_period_list(const po::variables_map &vm,
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

  std::stringstream out;
  r = utils::mgr_command(rados, "rbd snapshot schedule period list", args, &out,
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

void get_arguments_period_status(po::options_description *positional,
                                 po::options_description *options) {
  add_level_spec_options(options);
  at::add_format_options(options);
}

int execute_period_status(
    const po::variables_map &vm,
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

  std::stringstream out;
  r = utils::mgr_command(rados, "rbd snapshot schedule period status", args,
                         &out, &std::cerr);
  Status status("scheduled_images");
  r = status.parse(out.str());
  if (r < 0) {
    return r;
  }

  if (formatter.get()) {
    status.dump(formatter.get());
    formatter->flush(std::cout);
  } else {
    std::cout << status;
  }

  return 0;
}


void get_arguments_retention_add(po::options_description *positional,
                                 po::options_description *options) {
  add_level_spec_options(options);
  add_retention_options(positional, true);
}

int execute_retention_add(
    const po::variables_map &vm,
    const std::vector<std::string> &ceph_global_init_args) {
  std::map<std::string, std::string> args;

  int r = get_level_spec_args(vm, &args);
  if (r < 0) {
    return r;
  }
  r = get_retention_args(vm, true, true, &args);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  r = utils::init_rados(&rados);
  if (r < 0) {
    return r;
  }

  r = utils::mgr_command(rados, "rbd snapshot schedule retention add", args,
                         &std::cout, &std::cerr);
  if (r < 0) {
    return r;
  }

  return 0;
}

void get_arguments_retention_remove(po::options_description *positional,
                                    po::options_description *options) {
  add_level_spec_options(options);
  add_retention_options(positional, false);
}

int execute_retention_remove(
    const po::variables_map &vm,
    const std::vector<std::string> &ceph_global_init_args) {
  std::map<std::string, std::string> args;

  int r = get_level_spec_args(vm, &args);
  if (r < 0) {
    return r;
  }
  r = get_retention_args(vm, false, false, &args);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  r = utils::init_rados(&rados);
  if (r < 0) {
    return r;
  }

  r = utils::mgr_command(rados, "rbd snapshot schedule retention remove", args,
                         &std::cout, &std::cerr);
  if (r < 0) {
    return r;
  }

  return 0;
}

void get_arguments_retention_list(po::options_description *positional,
                                  po::options_description *options) {
  add_level_spec_options(options);
  options->add_options()
    ("recursive,R", po::bool_switch(), "list all policies");
  at::add_format_options(options);
}

int execute_retention_list(
    const po::variables_map &vm,
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

  std::stringstream out;
  r = utils::mgr_command(rados, "rbd snapshot schedule retention list", args,
                         &out, &std::cerr);
  if (r < 0) {
    return r;
  }

  RetentionPolicyList retention_list;
  r = retention_list.parse(out.str());
  if (r < 0) {
    return r;
  }

  if (vm["recursive"].as<bool>()) {
    if (formatter.get()) {
      retention_list.dump(formatter.get());
      formatter->flush(std::cout);
    } else {
      std::cout << retention_list;
    }
  } else {
    auto retention_policy = retention_list.find(args["level_spec"]);
    if (retention_policy == nullptr) {
      return -ENOENT;
    }

    if (formatter.get()) {
      retention_policy->dump(formatter.get());
      formatter->flush(std::cout);
    } else {
      std::cout << *retention_policy << std::endl;
    }
  }

  return 0;
}

void get_arguments_retention_status(po::options_description *positional,
                                    po::options_description *options) {
  add_level_spec_options(options);
  at::add_format_options(options);
}

int execute_retention_status(
    const po::variables_map &vm,
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

  std::stringstream out;
  r = utils::mgr_command(rados, "rbd snapshot schedule retention status", args,
                         &out, &std::cerr);
  Status status("retained_images");
  r = status.parse(out.str());
  if (r < 0) {
    return r;
  }

  if (formatter.get()) {
    status.dump(formatter.get());
    formatter->flush(std::cout);
  } else {
    std::cout << status;
  }

  return 0;
}

Shell::Action period_add_action(
  {"snapshot", "schedule", "period", "add"}, {},
  "Add snapshot schedule period.", "", &get_arguments_period_add,
  &execute_period_add);
Shell::Action period_remove_action(
  {"snapshot", "schedule", "period", "remove"},
  {"snapshot", "schedule", "period", "rm"},
  "Remove snapshot schedule period(s).", "", &get_arguments_period_remove,
  &execute_period_remove);
Shell::Action period_list_action(
  {"snapshot", "schedule", "period", "list"},
  {"snapshot", "schedule", "period", "ls"}, "List snapshot schedule periods.",
  "", &get_arguments_period_list, &execute_period_list);
Shell::Action period_status_action(
  {"snapshot", "schedule", "period", "status"}, {},
  "Show snapshot schedule status.", "", &get_arguments_period_status,
  &execute_period_status);
Shell::Action retention_add_action(
  {"snapshot", "schedule", "retention", "add"}, {},
  "Add snapshot schedule retention policy.", "", &get_arguments_retention_add,
  &execute_retention_add);
Shell::Action retention_remove_action(
  {"snapshot", "schedule", "retention", "remove"},
  {"snapshot", "schedule", "retention", "rm"},
  "Remove snapshot schedule retention policy.", "",
  &get_arguments_retention_remove, &execute_retention_remove);
Shell::Action retention_list_action(
  {"snapshot", "schedule", "retention", "list"},
  {"snapshot", "schedule", "retention", "ls"},
  "List snapshot schedule retention policy.", "", &get_arguments_retention_list,
  &execute_retention_list);
Shell::Action retention_status_action(
  {"snapshot", "schedule", "retention", "status"}, {},
  "Show snapshot schedule retention status.", "",
  &get_arguments_retention_status, &execute_retention_status);

} // namespace snapshot_schedule
} // namespace action
} // namespace rbd
