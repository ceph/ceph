// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstdlib>
#include <iostream>
#include <optional>
#include <regex>
#include <string>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <fmt/format.h>
#include <seastar/core/resource.hh>
#include <seastar/core/app-template.hh>

struct ctest_resource {
  int id;
  int slots;
  ctest_resource(int id, int slots) : id(id), slots(slots) {}
};

static std::vector<ctest_resource> parse_ctest_resources(const std::string& resource_spec) {
  std::vector<std::string> resources;
  boost::split(resources, resource_spec, boost::is_any_of(";"));
  std::regex res_regex("id:([0-9]+),slots:([0-9]+)");
  std::vector<ctest_resource> ctest_resources;
  for (auto& resource : resources) {
    std::smatch matched;
    if (std::regex_match(resource, matched, res_regex)) {
      int id = std::stoi(matched[1].str());
      int slots = std::stoi(matched[2].str());
      ctest_resources.emplace_back(id, slots);
    }
  }
  return ctest_resources;
}

static std::optional<seastar::resource::cpuset> get_cpuset_from_ctest_resource_group() {
  int nr_groups = 0;
  auto group_count = std::getenv("CTEST_RESOURCE_GROUP_COUNT");
  if (group_count != nullptr) {
    nr_groups = std::stoi(group_count);
  } else {
    return {};
  }

  seastar::resource::cpuset cpuset;
  for (int num = 0; num < nr_groups; num++) {
    std::string resource_type_name;
    fmt::format_to(std::back_inserter(resource_type_name), "CTEST_RESOURCE_GROUP_{}", num);
    // only a single resource type is supported for now
    std::string resource_type = std::getenv(resource_type_name.data());
    if (resource_type == "cpus") {
      std::transform(resource_type.begin(), resource_type.end(), resource_type.begin(), ::toupper);
      std::string resource_group;
      fmt::format_to(std::back_inserter(resource_group), "CTEST_RESOURCE_GROUP_{}_{}", num, resource_type);
      std::string resource_spec = std::getenv(resource_group.data());
      for (auto& resource : parse_ctest_resources(resource_spec)) {
        // each id has a single cpu slot
        cpuset.insert(resource.id);
      }
    } else {
      fmt::print(std::cerr, "unsupported resource type: {}", resource_type);
    }
  }
  return cpuset;
}

static seastar::app_template::seastar_options get_smp_opts_from_ctest() {
  seastar::app_template::seastar_options opts;
  auto cpuset = get_cpuset_from_ctest_resource_group();
  if (cpuset) {
    opts.smp_opts.cpuset.set_value(*cpuset);
  }
  return opts;
}
