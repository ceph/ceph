// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/Formatter.h"
#include "common/TextTable.h"
#include "common/ceph_json.h"
#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Schedule.h"
#include "tools/rbd/Utils.h"

#include <iostream>
#include <regex>

namespace rbd {

namespace at = argument_types;
namespace po = boost::program_options;

namespace {

int parse_schedule_name(const std::string &name, bool allow_images,
                        std::string *pool_name, std::string *namespace_name,
                        std::string *image_name) {
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
    if (!allow_images) {
        return -EINVAL;
    }
    *image_name = match[3];
  } else {
    *image_name = "-";
  }

  return 0;
}

} // anonymous namespace

void add_level_spec_options(po::options_description *options,
                            bool allow_image) {
  at::add_pool_option(options, at::ARGUMENT_MODIFIER_NONE);
  at::add_namespace_option(options, at::ARGUMENT_MODIFIER_NONE);
  if (allow_image) {
    at::add_image_option(options, at::ARGUMENT_MODIFIER_NONE);
  }
}

int get_level_spec_args(const po::variables_map &vm,
                        std::map<std::string, std::string> *args) {
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
    }

    if (vm.count(at::NAMESPACE_NAME)) {
      namespace_name = vm[at::NAMESPACE_NAME].as<std::string>();
    }

    if (namespace_name.empty()) {
      (*args)["level_spec"] = pool_name + "/" + image_name;
    } else {
      (*args)["level_spec"] = pool_name + "/" + namespace_name + "/" +
        image_name;
    }
    return 0;
  }

  if (vm.count(at::NAMESPACE_NAME)) {
    std::string pool_name;
    std::string namespace_name;

    if (vm.count(at::POOL_NAME)) {
      pool_name = vm[at::POOL_NAME].as<std::string>();
    }

    namespace_name = vm[at::NAMESPACE_NAME].as<std::string>();

    (*args)["level_spec"] = pool_name + "/" + namespace_name + "/";

    return 0;
  }

  if (vm.count(at::POOL_NAME)) {
    std::string pool_name = vm[at::POOL_NAME].as<std::string>();

    (*args)["level_spec"] = pool_name + "/";

    return 0;
  }

  (*args)["level_spec"] = "";

  return 0;
}

void normalize_level_spec_args(std::map<std::string, std::string> *args) {
  std::map<std::string, std::string> raw_args;
  std::swap(raw_args, *args);

  auto default_pool_name = utils::get_default_pool_name();
  for (auto [key, value] : raw_args) {
    if (key == "level_spec" && !value.empty() && value[0] == '/') {
      value = default_pool_name + value;
    }

    (*args)[key] = value;
  }
}

void add_schedule_options(po::options_description *positional,
                          bool mandatory) {
  if (mandatory) {
    positional->add_options()
      ("interval", "schedule interval");
  } else {
    positional->add_options()
      ("interval", po::value<std::string>()->default_value(""),
       "schedule interval");
  }
  positional->add_options()
    ("start-time", po::value<std::string>()->default_value(""),
     "schedule start time");
}

int get_schedule_args(const po::variables_map &vm, bool mandatory,
                      std::map<std::string, std::string> *args) {
  size_t arg_index = 0;

  std::string interval = utils::get_positional_argument(vm, arg_index++);
  if (interval.empty()) {
    if (mandatory) {
      std::cerr << "rbd: missing 'interval' argument" << std::endl;
      return -EINVAL;
    }
    return 0;
  }
  (*args)["interval"] = interval;

  std::string start_time = utils::get_positional_argument(vm, arg_index++);
  if (!start_time.empty()) {
    (*args)["start_time"] = start_time;
  }

  return 0;
}

int Schedule::parse(json_spirit::mValue &schedule_val) {
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

void Schedule::dump(ceph::Formatter *f) {
  f->open_array_section("items");
  for (auto &item : items) {
    f->open_object_section("item");
    f->dump_string("interval", item.first);
    f->dump_string("start_time", item.second);
    f->close_section(); // item
  }
  f->close_section(); // items
}

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

int ScheduleList::parse(const std::string &list) {
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

Schedule *ScheduleList::find(const std::string &name) {
  auto it = schedules.find(name);
  if (it == schedules.end()) {
    return nullptr;
  }

  return &it->second;
}

void ScheduleList::dump(ceph::Formatter *f) {
  f->open_array_section("schedules");
  for (auto &[name, s] : schedules) {
    std::string pool_name;
    std::string namespace_name;
    std::string image_name;

    int r = parse_schedule_name(name, allow_images, &pool_name, &namespace_name,
                                &image_name);
    if (r < 0) {
      continue;
    }

    f->open_object_section("schedule");
    f->dump_string("pool", pool_name);
    f->dump_string("namespace", namespace_name);
    if (allow_images) {
      f->dump_string("image", image_name);
    }
    s.dump(f);
    f->close_section();
  }
  f->close_section();
}

std::ostream& operator<<(std::ostream& os, ScheduleList &l) {
  TextTable tbl;
  tbl.define_column("POOL", TextTable::LEFT, TextTable::LEFT);
  tbl.define_column("NAMESPACE", TextTable::LEFT, TextTable::LEFT);
  if (l.allow_images) {
    tbl.define_column("IMAGE", TextTable::LEFT, TextTable::LEFT);
  }
  tbl.define_column("SCHEDULE", TextTable::LEFT, TextTable::LEFT);

  for (auto &[name, s] : l.schedules) {
    std::string pool_name;
    std::string namespace_name;
    std::string image_name;

    int r = parse_schedule_name(name, l.allow_images, &pool_name,
                                &namespace_name, &image_name);
    if (r < 0) {
      continue;
    }

    std::stringstream ss;
    ss << s;

    tbl << pool_name << namespace_name;
    if (l.allow_images) {
      tbl << image_name;
    }
    tbl << ss.str() << TextTable::endrow;
  }

  os << tbl;
  return os;
}

} // namespace rbd

