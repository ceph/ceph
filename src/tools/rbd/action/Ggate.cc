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

#if defined(__FreeBSD__)
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
#endif

int execute_list(const po::variables_map &vm,
                 const std::vector<std::string> &ceph_global_init_args) {
#if !defined(__FreeBSD__)
  std::cerr << "rbd: ggate is only supported on FreeBSD" << std::endl;
  return -EOPNOTSUPP;
#else
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
#endif
}

int execute_map(const po::variables_map &vm,
                const std::vector<std::string> &ceph_global_init_args) {
#if !defined(__FreeBSD__)
  std::cerr << "rbd: ggate is only supported on FreeBSD" << std::endl;
  return -EOPNOTSUPP;
#else
  std::vector<std::string> args;

  args.push_back("map");
  std::string img;
  int r = utils::get_image_or_snap_spec(vm, &img);
  if (r < 0) {
    return r;
  }
  args.push_back(img);

  if (vm["quiesce"].as<bool>()) {
    std::cerr << "rbd: warning: quiesce is not supported" << std::endl;
  }

  if (vm["read-only"].as<bool>()) {
    args.push_back("--read-only");
  }

  if (vm["exclusive"].as<bool>()) {
    args.push_back("--exclusive");
  }

  if (vm.count("quiesce-hook")) {
    std::cerr << "rbd: warning: quiesce-hook is not supported" << std::endl;
  }

  if (vm.count("options")) {
    utils::append_options_as_args(vm["options"].as<std::vector<std::string>>(),
                                  &args);
  }

  return call_ggate_cmd(vm, args, ceph_global_init_args);
#endif
}

int execute_unmap(const po::variables_map &vm,
                  const std::vector<std::string> &ceph_global_init_args) {
#if !defined(__FreeBSD__)
  std::cerr << "rbd: ggate is only supported on FreeBSD" << std::endl;
  return -EOPNOTSUPP;
#else
  std::string device_name = utils::get_positional_argument(vm, 0);
  if (!boost::starts_with(device_name, "/dev/")) {
    device_name.clear();
  }

  std::string image_name;
  if (device_name.empty()) {
    int r = utils::get_image_or_snap_spec(vm, &image_name);
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
    utils::append_options_as_args(vm["options"].as<std::vector<std::string>>(),
                                  &args);
  }

  return call_ggate_cmd(vm, args, ceph_global_init_args);
#endif
}

int execute_attach(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
#if !defined(__FreeBSD__)
  std::cerr << "rbd: ggate is only supported on FreeBSD" << std::endl;
#else
  std::cerr << "rbd: ggate attach command not supported" << std::endl;
#endif
  return -EOPNOTSUPP;
}

int execute_detach(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
#if !defined(__FreeBSD__)
  std::cerr << "rbd: ggate is only supported on FreeBSD" << std::endl;
#else
  std::cerr << "rbd: ggate detach command not supported" << std::endl;
#endif
  return -EOPNOTSUPP;
}

} // namespace ggate
} // namespace action
} // namespace rbd
