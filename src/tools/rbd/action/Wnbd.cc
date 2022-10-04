// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/stringify.h"
#include "common/SubProcess.h"
#include <iostream>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace wnbd {

namespace at = argument_types;
namespace po = boost::program_options;

#if defined(_WIN32)
static int call_wnbd_cmd(const po::variables_map &vm,
                        const std::vector<std::string> &args,
                        const std::vector<std::string> &ceph_global_init_args) {
  char exe_path[PATH_MAX];
  ssize_t exe_path_bytes = get_self_exe_path(exe_path, PATH_MAX);

  if (exe_path_bytes > 4) {
    // Drop .exe suffix as we're going to add the "-wnbd" suffix.
    exe_path[strlen(exe_path) - 4] = '\0';
    exe_path_bytes -= 4;
  }

  if (exe_path_bytes < 0) {
    strcpy(exe_path, "rbd-wnbd");
  } else {
    if (snprintf(exe_path + exe_path_bytes,
                 sizeof(exe_path) - exe_path_bytes,
                 "-wnbd") < 0) {
      return -EOVERFLOW;
    }
  }

  SubProcess process(exe_path, SubProcess::KEEP, SubProcess::KEEP, SubProcess::KEEP);

  for (auto &arg : ceph_global_init_args) {
    process.add_cmd_arg(arg.c_str());
  }

  for (auto &arg : args) {
    process.add_cmd_arg(arg.c_str());
  }

  if (process.spawn()) {
    std::cerr << "rbd: failed to run rbd-wnbd: " << process.err() << std::endl;
    return -EINVAL;
  }
  int exit_code = process.join();
  if (exit_code) {
    std::cerr << "rbd: rbd-wnbd failed with error: " << process.err() << std::endl;
    return exit_code;
  }

  return 0;
}
#endif

int execute_list(const po::variables_map &vm,
                 const std::vector<std::string> &ceph_global_init_args) {
#if !defined(_WIN32)
  std::cerr << "rbd: wnbd is only supported on Windows" << std::endl;
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

  return call_wnbd_cmd(vm, args, ceph_global_init_args);
#endif
}

int execute_map(const po::variables_map &vm,
                const std::vector<std::string> &ceph_global_init_args) {
#if !defined(_WIN32)
  std::cerr << "rbd: wnbd is only supported on Windows" << std::endl;
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

  if (vm["read-only"].as<bool>()) {
    args.push_back("--read-only");
  }

  if (vm["exclusive"].as<bool>()) {
    args.push_back("--exclusive");
  }

  if (vm.count("options")) {
    utils::append_options_as_args(vm["options"].as<std::vector<std::string>>(),
                                  &args);
  }

  return call_wnbd_cmd(vm, args, ceph_global_init_args);
#endif
}

int execute_unmap(const po::variables_map &vm,
                  const std::vector<std::string> &ceph_global_init_args) {
#if !defined(_WIN32)
  std::cerr << "rbd: wnbd is only supported on Windows" << std::endl;
  return -EOPNOTSUPP;
#else
  std::string device_name = utils::get_positional_argument(vm, 0);

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

  return call_wnbd_cmd(vm, args, ceph_global_init_args);
#endif
}

int execute_attach(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
#if !defined(_WIN32)
  std::cerr << "rbd: wnbd is only supported on Windows" << std::endl;
#else
  std::cerr << "rbd: wnbd attach command not supported" << std::endl;
#endif
  return -EOPNOTSUPP;
}

int execute_detach(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
#if !defined(_WIN32)
  std::cerr << "rbd: wnbd is only supported on Windows" << std::endl;
#else
  std::cerr << "rbd: wnbd detach command not supported" << std::endl;
#endif
  return -EOPNOTSUPP;
}

} // namespace wnbd
} // namespace action
} // namespace rbd
