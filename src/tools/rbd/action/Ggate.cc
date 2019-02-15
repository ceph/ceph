// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/param.h>
#include <errno.h>
#include <unistd.h>

#include "include/stringify.h"
#include "common/SubProcess.h"

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/program_options.hpp>

#include <iostream>

namespace rbd {
namespace action {
namespace ggate {

namespace at = argument_types;
namespace po = boost::program_options;

static int call_ggate_cmd(const po::variables_map &vm,
                          const std::vector<std::string> &args,
                          const std::vector<std::string> &ceph_global_args) {
  SubProcess process("rbd-ggate", SubProcess::KEEP, SubProcess::KEEP,
                     SubProcess::KEEP);

  for (auto &arg : ceph_global_args) {
    process.add_cmd_arg(arg.c_str());
  }

  for (auto &arg : args) {
    process.add_cmd_arg(arg.c_str());
  }

  if (process.spawn()) {
    std::cerr << "rbd: failed to run rbd-ggate: " << process.err() << std::endl;
    return -EINVAL;
  } else if (process.join()) {
    std::cerr << "rbd: rbd-ggate failed with error: " << process.err()
              << std::endl;
    return -EINVAL;
  }

  return 0;
}

int get_image_or_snap_spec(const po::variables_map &vm, std::string *spec) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string nspace_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &nspace_name,
    &image_name, &snap_name, true,
    utils::SNAPSHOT_PRESENCE_PERMITTED, utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  spec->append(pool_name);
  spec->append("/");
  if (!nspace_name.empty()) {
    spec->append(nspace_name);
    spec->append("/");
  }
  spec->append(image_name);
  if (!snap_name.empty()) {
    spec->append("@");
    spec->append(snap_name);
  }

  return 0;
}

int parse_options(const std::vector<std::string> &options,
                  std::vector<std::string> *args) {
  for (auto &opts : options) {
    std::vector<std::string> args_;
    boost::split(args_, opts, boost::is_any_of(","));
    for (auto &o : args_) {
      args->push_back("--" + o);
    }
  }

  return 0;
}

int execute_list(const po::variables_map &vm,
                 const std::vector<std::string> &ceph_global_init_args) {
#if !defined(__FreeBSD__)
  std::cerr << "rbd: ggate is only supported on FreeBSD" << std::endl;
  return -EOPNOTSUPP;
#endif
  std::vector<std::string> args;

  args.push_back("list");

  if (vm.count("format")) {
    args.push_back("--format");
    args.push_back(vm["format"].as<at::Format>().value);
  }
  if (vm["pretty-format"].as<bool>()) {
    args.push_back("--pretty-format");
  }

  return call_ggate_cmd(vm, args, ceph_global_init_args);
}

int execute_map(const po::variables_map &vm,
                const std::vector<std::string> &ceph_global_init_args) {
#if !defined(__FreeBSD__)
  std::cerr << "rbd: ggate is only supported on FreeBSD" << std::endl;
  return -EOPNOTSUPP;
#endif
  std::vector<std::string> args;

  args.push_back("map");
  std::string img;
  int r = get_image_or_snap_spec(vm, &img);
  if (r < 0) {
    return r;
  }
  args.push_back(img);

  if (vm["read-only"].as<bool>()) {
    args.push_back("--read-only");
  }

  if (vm["exclusive"].as<bool>()) {
    args.push_back("--exclusive");
  }

  if (vm.count("options")) {
    r = parse_options(vm["options"].as<std::vector<std::string>>(), &args);
    if (r < 0) {
      return r;
    }
  }

  return call_ggate_cmd(vm, args, ceph_global_init_args);
}

int execute_unmap(const po::variables_map &vm,
                  const std::vector<std::string> &ceph_global_init_args) {
#if !defined(__FreeBSD__)
  std::cerr << "rbd: ggate is only supported on FreeBSD" << std::endl;
  return -EOPNOTSUPP;
#endif
  std::string device_name = utils::get_positional_argument(vm, 0);
  if (!boost::starts_with(device_name, "/dev/")) {
    device_name.clear();
  }

  std::string image_name;
  if (device_name.empty()) {
    int r = get_image_or_snap_spec(vm, &image_name);
    if (r < 0) {
      return r;
    }
  }

  if (device_name.empty() && image_name.empty()) {
    std::cerr << "rbd: unmap requires either image name or device path"
              << std::endl;
    return -EINVAL;
  }

  std::vector<std::string> args;

  args.push_back("unmap");
  args.push_back(device_name.empty() ? image_name : device_name);

  if (vm.count("options")) {
    int r = parse_options(vm["options"].as<std::vector<std::string>>(), &args);
    if (r < 0) {
      return r;
    }
  }

  return call_ggate_cmd(vm, args, ceph_global_init_args);
}

} // namespace ggate
} // namespace action
} // namespace rbd
