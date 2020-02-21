// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/ceph_context.h"
#include "common/ceph_json.h"
#include "common/errno.h"
#include "common/escape.h"
#include "common/Formatter.h"
#include "common/TextTable.h"
#include "global/global_context.h"
#include "include/stringify.h"

#include <iostream>
#include <list>
#include <map>
#include <regex>
#include <string>
#include <boost/program_options.hpp>

#include "json_spirit/json_spirit.h"

namespace rbd {
namespace action {
namespace mirror_snapshot_schedule {

namespace at = argument_types;
namespace po = boost::program_options;

namespace {

struct Args {
  std::map<std::string, std::string> args;

  Args() {
  }
  Args(const std::map<std::string, std::string> &args)
    : args(args) {
  }

  std::string str() const {
    std::string out = "";

    std::string delimiter;
    for (auto &it : args) {
      out += delimiter + "\"" + it.first + "\": \"" +
        stringify(json_stream_escaper(it.second)) + "\"";
      delimiter = ",\n";
    }

    return out;
  }
};

class Schedule {
public:
  Schedule() {
  }

  int parse(json_spirit::mValue &schedule_val) {
    if (schedule_val.type() != json_spirit::array_type) {
      std::cerr << "rbd: unexpected schedule JSON received: "
                << "schedule is not array" << std::endl;
      return -EBADMSG;
    }

    try {
      for (auto &item_val : schedule_val.get_array()) {
        if (item_val.type() != json_spirit::obj_type) {
          std::cerr << "rbd: unexpected schedule JSON received: "
                    << "schedule item is not object" << std::endl;
          return -EBADMSG;
        }

        auto &item = item_val.get_obj();

        if (item["interval"].type() != json_spirit::str_type) {
          std::cerr << "rbd: unexpected schedule JSON received: "
                    << "interval is not string" << std::endl;
          return -EBADMSG;
        }
        auto interval = item["interval"].get_str();

        std::string start_time;
        if (item["start_time"].type() == json_spirit::str_type) {
          start_time = item["start_time"].get_str();
        }

        items.push_back({interval, start_time});
      }

    } catch (std::runtime_error &) {
      std::cerr << "rbd: invalid schedule JSON received" << std::endl;
      return -EBADMSG;
    }

    return 0;
  }

  void dump(Formatter *f) {
    f->open_array_section("items");
    for (auto &item : items) {
      f->open_object_section("item");
      f->dump_string("interval", item.first);
      f->dump_string("start_time", item.second);
      f->close_section(); // item
    }
    f->close_section(); // items
  }

  friend std::ostream& operator<<(std::ostream& os, Schedule &s);

private:
  std::string name;
  std::list<std::pair<std::string, std::string>> items;
};

std::ostream& operator<<(std::ostream& os, Schedule &s) {
  std::string delimiter;
  for (auto &item : s.items) {
    os << delimiter << "every " << item.first;
    if (!item.second.empty()) {
      os << " starting at " << item.second;
    }
    delimiter = ", ";
  }
  return os;
}

int parse_schedule_name(const std::string &name, std::string *pool_name,
                        std::string *namespace_name, std::string *image_name) {
  // parse names like:
  // '', 'rbd/', 'rbd/ns/', 'rbd/image', 'rbd/ns/image'
  std::regex pattern("^(?:([^/]+)/(?:(?:([^/]+)/|)(?:([^/@]+))?)?)?$");
  std::smatch match;
  if (!std::regex_match(name, match, pattern)) {
    return -EINVAL;
  }

  if (match[1].matched) {
    *pool_name = match[1];
  } else {
    *pool_name = "-";
  }

  if (match[2].matched) {
    *namespace_name = match[2];
  } else if (match[3].matched) {
    *namespace_name = "";
  } else {
    *namespace_name = "-";
  }

  if (match[3].matched) {
    *image_name = match[3];
  } else {
    *image_name = "-";
  }

  return 0;
}

class ScheduleList {
public:
  ScheduleList() {
  }

  int parse(const std::string &list) {
    json_spirit::mValue json_root;
    if (!json_spirit::read(list, json_root)) {
      std::cerr << "rbd: invalid schedule list JSON received" << std::endl;
      return -EBADMSG;
    }

    try {
      for (auto &[id, schedule_val] : json_root.get_obj()) {
        if (schedule_val.type() != json_spirit::obj_type) {
          std::cerr << "rbd: unexpected schedule list JSON received: "
                    << "schedule_val is not object" << std::endl;
          return -EBADMSG;
        }
        auto &schedule = schedule_val.get_obj();
        if (schedule["name"].type() != json_spirit::str_type) {
          std::cerr << "rbd: unexpected schedule list JSON received: "
                    << "schedule name is not string" << std::endl;
          return -EBADMSG;
        }
        auto name = schedule["name"].get_str();

        if (schedule["schedule"].type() != json_spirit::array_type) {
          std::cerr << "rbd: unexpected schedule list JSON received: "
                    << "schedule is not array" << std::endl;
          return -EBADMSG;
        }

        Schedule s;
        int r = s.parse(schedule["schedule"]);
        if (r < 0) {
          return r;
        }
        schedules[name] = s;
      }
    } catch (std::runtime_error &) {
      std::cerr << "rbd: invalid schedule list JSON received" << std::endl;
      return -EBADMSG;
    }

    return 0;
  }

  Schedule *find(const std::string &name) {
    auto it = schedules.find(name);
    if (it == schedules.end()) {
      return nullptr;
    }

    return &it->second;
  }

  void dump(Formatter *f) {
    f->open_array_section("schedules");
    for (auto &[name, s] : schedules) {
      std::string pool_name;
      std::string namespace_name;
      std::string image_name;

      int r = parse_schedule_name(name, &pool_name, &namespace_name,
                                  &image_name);
      if (r < 0) {
        continue;
      }

      f->open_object_section("schedule");
      f->dump_string("pool", pool_name);
      f->dump_string("namespace", namespace_name);
      f->dump_string("image", image_name);
      s.dump(f);
      f->close_section();
    }
    f->close_section();
  }

  friend std::ostream& operator<<(std::ostream& os, ScheduleList &d);

private:
  std::map<std::string, Schedule> schedules;
};

std::ostream& operator<<(std::ostream& os, ScheduleList &l) {
  TextTable tbl;
  tbl.define_column("POOL", TextTable::LEFT, TextTable::LEFT);
  tbl.define_column("NAMESPACE", TextTable::LEFT, TextTable::LEFT);
  tbl.define_column("IMAGE", TextTable::LEFT, TextTable::LEFT);
  tbl.define_column("SCHEDULE", TextTable::LEFT, TextTable::LEFT);

  for (auto &[name, s] : l.schedules) {
    std::string pool_name;
    std::string namespace_name;
    std::string image_name;

    int r = parse_schedule_name(name, &pool_name, &namespace_name,
                                &image_name);
    if (r < 0) {
      continue;
    }

    std::stringstream ss;
    ss << s;

    tbl << pool_name << namespace_name << image_name << ss.str()
        << TextTable::endrow;
  }

  os << tbl;
  return os;
}

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

int ceph_rbd_mirror_snapshot_schedule(librados::Rados& rados,
                                      const std::string& cmd,
                                      const Args& args,
                                      std::ostream *out_os,
                                      std::ostream *err_os) {
  std::string command = R"(
    {
      "prefix": "rbd mirror snapshot schedule )" + cmd + R"(",
      )" + args.str() + R"(
    })";

  bufferlist in_bl;
  bufferlist out_bl;
  std::string outs;
  int r = rados.mgr_command(command, in_bl, &out_bl, &outs);
  if (r == -EOPNOTSUPP) {
    (*err_os) << "rbd: 'rbd_support' mgr module is not enabled."
              << std::endl << std::endl
              << "Use 'ceph mgr module enable rbd_support' to enable."
              << std::endl;
    return r;
  } else if (r < 0) {
    (*err_os) << "rbd: " << cmd << " failed: " << cpp_strerror(r);
    if (!outs.empty()) {
      (*err_os) << ": " << outs;
    }
    (*err_os) << std::endl;
    return r;
  }

  if (out_bl.length() != 0) {
    (*out_os) << out_bl.c_str();
  }

  return 0;
}

void add_level_spec_options(po::options_description *options) {
  at::add_pool_option(options, at::ARGUMENT_MODIFIER_NONE);
  at::add_namespace_option(options, at::ARGUMENT_MODIFIER_NONE);
  at::add_image_option(options, at::ARGUMENT_MODIFIER_NONE);
}

int get_level_spec_name(const po::variables_map &vm,
                        std::string *level_spec_name) {
  if (vm.count(at::IMAGE_NAME)) {
    std::string pool_name;
    std::string namespace_name;
    std::string image_name;

    int r = utils::extract_spec(vm[at::IMAGE_NAME].as<std::string>(),
                                &pool_name, &namespace_name, &image_name,
                                nullptr, utils::SPEC_VALIDATION_FULL);
    if (r < 0) {
      return r;
    }

    if (!pool_name.empty()) {
      if (vm.count(at::POOL_NAME)) {
        std::cerr << "rbd: pool is specified both via pool and image options"
                  << std::endl;
        return -EINVAL;
      }
      if (vm.count(at::NAMESPACE_NAME)) {
        std::cerr << "rbd: namespace is specified both via namespace and image"
                  << " options" << std::endl;
        return -EINVAL;
      }
    }

    if (vm.count(at::POOL_NAME)) {
      pool_name = vm[at::POOL_NAME].as<std::string>();
    } else if (pool_name.empty()) {
      pool_name = utils::get_default_pool_name();
    }

    if (vm.count(at::NAMESPACE_NAME)) {
      namespace_name = vm[at::NAMESPACE_NAME].as<std::string>();
    }

    if (namespace_name.empty()) {
      *level_spec_name = pool_name + "/" + image_name;
    } else {
      *level_spec_name = pool_name + "/" + namespace_name + "/" + image_name;
    }
    return 0;
  }

  if (vm.count(at::NAMESPACE_NAME)) {
    std::string pool_name;
    std::string namespace_name;

    if (vm.count(at::POOL_NAME)) {
      pool_name = vm[at::POOL_NAME].as<std::string>();
    } else {
      pool_name = utils::get_default_pool_name();
    }

    namespace_name = vm[at::NAMESPACE_NAME].as<std::string>();

    *level_spec_name = pool_name + "/" + namespace_name + "/";

    return 0;
  }

  if (vm.count(at::POOL_NAME)) {
    std::string pool_name = vm[at::POOL_NAME].as<std::string>();

    *level_spec_name = pool_name + "/";

    return 0;
  }

  *level_spec_name = "";

  return 0;
}

} // anonymous namespace

void get_arguments_add(po::options_description *positional,
                       po::options_description *options) {
  add_level_spec_options(options);
  positional->add_options()
    ("interval", "schedule interval");
  positional->add_options()
    ("start-time", "schedule start time");
}

int execute_add(const po::variables_map &vm,
                const std::vector<std::string> &ceph_global_init_args) {
  std::string level_spec_name;
  int r = get_level_spec_name(vm, &level_spec_name);
  if (r < 0) {
    return r;
  }

  size_t arg_index = 0;
  std::string interval = utils::get_positional_argument(vm, arg_index++);
  if (interval.empty()) {
    std::cerr << "rbd: missing 'interval' argument" << std::endl;
    return -EINVAL;
  }

  Args args({{"level_spec", level_spec_name}, {"interval", interval}});

  std::string start_time = utils::get_positional_argument(vm, arg_index++);
  if (!start_time.empty()) {
    args.args["start_time"] = start_time;
  }

  librados::Rados rados;
  r = utils::init_rados(&rados);
  if (r < 0) {
    return r;
  }

  r = ceph_rbd_mirror_snapshot_schedule(rados, "add", args, &std::cout,
                                        &std::cerr);
  if (r < 0) {
    return r;
  }

  return 0;
}

void get_arguments_remove(po::options_description *positional,
                          po::options_description *options) {
  add_level_spec_options(options);
  positional->add_options()
    ("interval", "schedule interval");
  positional->add_options()
    ("start-time", "schedule start time");
}

int execute_remove(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  std::string level_spec_name;
  int r = get_level_spec_name(vm, &level_spec_name);
  if (r < 0) {
    return r;
  }

  Args args({{"level_spec", level_spec_name}});

  size_t arg_index = 0;
  std::string interval = utils::get_positional_argument(vm, arg_index++);
  if (!interval.empty()) {
    args.args["interval"] = interval;
  }

  std::string start_time = utils::get_positional_argument(vm, arg_index++);
  if (!start_time.empty()) {
    args.args["start_time"] = start_time;
  }

  librados::Rados rados;
  r = utils::init_rados(&rados);
  if (r < 0) {
    return r;
  }

  r = ceph_rbd_mirror_snapshot_schedule(rados, "remove", args, &std::cout,
                                        &std::cerr);
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
  std::string level_spec_name;
  int r = get_level_spec_name(vm, &level_spec_name);
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

  Args args({{"level_spec", level_spec_name}});
  std::stringstream out;
  r = ceph_rbd_mirror_snapshot_schedule(rados, "list", args, &out, &std::cerr);
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
    auto schedule = schedule_list.find(level_spec_name);
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
  std::string level_spec_name;
  int r = get_level_spec_name(vm, &level_spec_name);
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

  Args args({{"level_spec", level_spec_name}});

  std::stringstream out;
  r = ceph_rbd_mirror_snapshot_schedule(rados, "status", args, &out,
                                        &std::cerr);
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
