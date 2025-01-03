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
namespace nbd {

namespace at = argument_types;
namespace po = boost::program_options;

static int call_nbd_cmd(const po::variables_map &vm,
                        const std::vector<std::string> &args,
                        const std::vector<std::string> &ceph_global_init_args) {
  #if defined(__FreeBSD__) || defined(_WIN32)
  std::cerr << "rbd: nbd device is not supported" << std::endl;
  return -EOPNOTSUPP;
  #else
  char exe_path[PATH_MAX];
  ssize_t exe_path_bytes = readlink("/proc/self/exe", exe_path,
				    sizeof(exe_path) - 1);
  if (exe_path_bytes < 0) {
    strcpy(exe_path, "rbd-nbd");
  } else {
    if (snprintf(exe_path + exe_path_bytes,
                 sizeof(exe_path) - exe_path_bytes,
                 "-nbd") < 0) {
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
    std::cerr << "rbd: failed to run rbd-nbd: " << process.err() << std::endl;
    return -EINVAL;
  } else if (process.join()) {
    std::cerr << "rbd: rbd-nbd failed with error: " << process.err() << std::endl;
    return -EINVAL;
  }

  return 0;
  #endif
}

int execute_list(const po::variables_map &vm,
                 const std::vector<std::string> &ceph_global_init_args) {
#if defined(__FreeBSD__) || defined(_WIN32)
  std::cerr << "rbd: nbd device is not supported" << std::endl;
  return -EOPNOTSUPP;
#else
  std::vector<std::string> args;

  args.push_back("list-mapped");

  if (vm.count("format")) {
    args.push_back("--format");
    args.push_back(vm["format"].as<at::Format>().value);
  }
  if (vm["pretty-format"].as<bool>()) {
    args.push_back("--pretty-format");
  }

  return call_nbd_cmd(vm, args, ceph_global_init_args);
#endif
}

int execute_attach(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
#if defined(__FreeBSD__) || defined(_WIN32)
  std::cerr << "rbd: nbd device is not supported" << std::endl;
  return -EOPNOTSUPP;
#else
  std::vector<std::string> args;
  std::string device_path;

  args.push_back("attach");
  std::string img;
  int r = utils::get_image_or_snap_spec(vm, &img);
  if (r < 0) {
    return r;
  }
  args.push_back(img);

  if (vm.count("device")) {
    device_path = vm["device"].as<std::string>();
    args.push_back("--device");
    args.push_back(device_path);
  } else {
    std::cerr << "rbd: device was not specified" << std::endl;
    return -EINVAL;
  }

  if (vm["show-cookie"].as<bool>()) {
    args.push_back("--show-cookie");
  }

  if (vm.count("cookie")) {
    args.push_back("--cookie");
    args.push_back(vm["cookie"].as<std::string>());
  } else if (!vm["force"].as<bool>()) {
    std::cerr << "rbd: could not validate attach request\n";
    std::cerr << "rbd: mismatching the image and the device may lead to data corruption\n";
    std::cerr << "rbd: must specify --cookie <arg> or --force to proceed" << std::endl;
    return -EINVAL;
  }

  if (vm.count(at::SNAPSHOT_ID)) {
    args.push_back("--snap-id");
    args.push_back(std::to_string(vm[at::SNAPSHOT_ID].as<uint64_t>()));
  }

  if (vm["quiesce"].as<bool>()) {
    args.push_back("--quiesce");
  }

  if (vm["read-only"].as<bool>()) {
    args.push_back("--read-only");
  }

  if (vm["exclusive"].as<bool>()) {
    args.push_back("--exclusive");
  }

  if (vm.count("quiesce-hook")) {
    args.push_back("--quiesce-hook");
    args.push_back(vm["quiesce-hook"].as<std::string>());
  }

  if (vm.count("options")) {
    utils::append_options_as_args(vm["options"].as<std::vector<std::string>>(),
                                  &args);
  }

  return call_nbd_cmd(vm, args, ceph_global_init_args);
#endif
}

int execute_detach(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
#if defined(__FreeBSD__) || defined(_WIN32)
  std::cerr << "rbd: nbd device is not supported" << std::endl;
  return -EOPNOTSUPP;
#else
  std::string device_name = utils::get_positional_argument(vm, 0);
  if (!boost::starts_with(device_name, "/dev/")) {
    device_name.clear();
  }

  std::vector<std::string> args;

  args.push_back("detach");
  std::string image_name;
  if (device_name.empty()) {
    int r = utils::get_image_or_snap_spec(vm, &image_name);
    if (r < 0) {
      return r;
    }

    if (image_name.empty()) {
      std::cerr << "rbd: detach requires either image name or device path"
                << std::endl;
      return -EINVAL;
    }

    if (vm.count(at::SNAPSHOT_ID)) {
      args.push_back("--snap-id");
      args.push_back(std::to_string(vm[at::SNAPSHOT_ID].as<uint64_t>()));
    }
  }

  args.push_back(device_name.empty() ? image_name : device_name);

  if (vm.count("options")) {
    utils::append_options_as_args(vm["options"].as<std::vector<std::string>>(),
                                  &args);
  }

  return call_nbd_cmd(vm, args, ceph_global_init_args);
#endif
}

int execute_map(const po::variables_map &vm,
                const std::vector<std::string> &ceph_global_init_args) {
#if defined(__FreeBSD__) || defined(_WIN32)
  std::cerr << "rbd: nbd device is not supported" << std::endl;
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
    args.push_back("--quiesce");
  }

  if (vm["show-cookie"].as<bool>()) {
    args.push_back("--show-cookie");
  }

  if (vm.count("cookie")) {
    args.push_back("--cookie");
    args.push_back(vm["cookie"].as<std::string>());
  }

  if (vm.count(at::SNAPSHOT_ID)) {
    args.push_back("--snap-id");
    args.push_back(std::to_string(vm[at::SNAPSHOT_ID].as<uint64_t>()));
  }

  if (vm["read-only"].as<bool>()) {
    args.push_back("--read-only");
  }

  if (vm["exclusive"].as<bool>()) {
    args.push_back("--exclusive");
  }

  if (vm.count("quiesce-hook")) {
    args.push_back("--quiesce-hook");
    args.push_back(vm["quiesce-hook"].as<std::string>());
  }

  if (vm.count("options")) {
    utils::append_options_as_args(vm["options"].as<std::vector<std::string>>(),
                                  &args);
  }

  return call_nbd_cmd(vm, args, ceph_global_init_args);
#endif
}

int execute_unmap(const po::variables_map &vm,
                  const std::vector<std::string> &ceph_global_init_args) {
#if defined(__FreeBSD__) || defined(_WIN32)
  std::cerr << "rbd: nbd device is not supported" << std::endl;
  return -EOPNOTSUPP;
#else
  std::string device_name = utils::get_positional_argument(vm, 0);
  if (!boost::starts_with(device_name, "/dev/")) {
    device_name.clear();
  }

  std::vector<std::string> args;

  args.push_back("unmap");
  std::string image_name;
  if (device_name.empty()) {
    int r = utils::get_image_or_snap_spec(vm, &image_name);
    if (r < 0) {
      return r;
    }

    if (image_name.empty()) {
      std::cerr << "rbd: unmap requires either image name or device path"
                << std::endl;
      return -EINVAL;
    }

    if (vm.count(at::SNAPSHOT_ID)) {
      args.push_back("--snap-id");
      args.push_back(std::to_string(vm[at::SNAPSHOT_ID].as<uint64_t>()));
    }
  }

  args.push_back(device_name.empty() ? image_name : device_name);

  if (vm.count("options")) {
    utils::append_options_as_args(vm["options"].as<std::vector<std::string>>(),
                                  &args);
  }

  return call_nbd_cmd(vm, args, ceph_global_init_args);
#endif
}

void get_list_arguments_deprecated(po::options_description *positional,
                                   po::options_description *options) {
  at::add_format_options(options);
}

int execute_list_deprecated(const po::variables_map &vm,
                            const std::vector<std::string> &ceph_global_args) {
  std::cerr << "rbd: 'nbd list' command is deprecated, "
            << "use 'device list -t nbd' instead" << std::endl;
  return execute_list(vm, ceph_global_args);
}

void get_map_arguments_deprecated(po::options_description *positional,
                                  po::options_description *options) {
  at::add_image_or_snap_spec_options(positional, options,
                                     at::ARGUMENT_MODIFIER_NONE);
  options->add_options()
    ("read-only", po::bool_switch(), "map read-only")
    ("exclusive", po::bool_switch(), "forbid writes by other clients")
    ("device", po::value<std::string>(), "specify nbd device")
    ("nbds_max", po::value<std::string>(), "override module param nbds_max")
    ("max_part", po::value<std::string>(), "override module param max_part")
    ("timeout", po::value<std::string>(), "set nbd request timeout (seconds)");
}

int execute_map_deprecated(const po::variables_map &vm_deprecated,
                           const std::vector<std::string> &ceph_global_args) {
  std::cerr << "rbd: 'nbd map' command is deprecated, "
            << "use 'device map -t nbd' instead" << std::endl;

  po::options_description options;
  options.add_options()
    ("options,o", po::value<std::vector<std::string>>()
                  ->default_value(std::vector<std::string>(), ""), "");

  po::variables_map vm = vm_deprecated;
  po::store(po::command_line_parser({}).options(options).run(), vm);

  std::vector<std::string> opts;
  if (vm_deprecated.count("device")) {
    opts.push_back("device=" + vm_deprecated["device"].as<std::string>());
  }
  if (vm_deprecated.count("nbds_max")) {
    opts.push_back("nbds_max=" + vm_deprecated["nbds_max"].as<std::string>());
  }
  if (vm_deprecated.count("max_part")) {
    opts.push_back("max_part=" + vm_deprecated["max_part"].as<std::string>());
  }
  if (vm_deprecated.count("timeout")) {
    opts.push_back("timeout=" + vm_deprecated["timeout"].as<std::string>());
  }

  vm.at("options").value() = boost::any(opts);

  return execute_map(vm, ceph_global_args);
}

void get_unmap_arguments_deprecated(po::options_description *positional,
                                    po::options_description *options) {
  positional->add_options()
    ("image-or-snap-or-device-spec",
     "image, snapshot, or device specification\n"
     "[<pool-name>/]<image-name>[@<snap-name>] or <device-path>");
  at::add_pool_option(options, at::ARGUMENT_MODIFIER_NONE);
  at::add_image_option(options, at::ARGUMENT_MODIFIER_NONE);
  at::add_snap_option(options, at::ARGUMENT_MODIFIER_NONE);
}

int execute_unmap_deprecated(const po::variables_map &vm,
                             const std::vector<std::string> &ceph_global_args) {
  std::cerr << "rbd: 'nbd unmap' command is deprecated, "
            << "use 'device unmap -t nbd' instead" << std::endl;
  return execute_unmap(vm, ceph_global_args);
}

Shell::Action action_show_deprecated(
  {"nbd", "list"}, {"nbd", "ls"}, "List the nbd devices already used.", "",
  &get_list_arguments_deprecated, &execute_list_deprecated, false);

Shell::Action action_map_deprecated(
  {"nbd", "map"}, {}, "Map image to a nbd device.", "",
  &get_map_arguments_deprecated, &execute_map_deprecated, false);

Shell::Action action_unmap_deprecated(
  {"nbd", "unmap"}, {}, "Unmap a nbd device.", "",
  &get_unmap_arguments_deprecated, &execute_unmap_deprecated, false);

} // namespace nbd
} // namespace action
} // namespace rbd
