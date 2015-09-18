// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include <iostream>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/program_options.hpp>
#include <boost/scope_exit.hpp>

namespace rbd {
namespace action {
namespace kernel {

namespace at = argument_types;
namespace po = boost::program_options;

void get_show_arguments(po::options_description *positional,
                        po::options_description *options) {
  at::add_format_options(options);
}

int execute_show(const po::variables_map &vm) {
  at::Format::Formatter formatter;
  int r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }

  utils::init_context();

  return 0;
}

void get_map_arguments(po::options_description *positional,
                       po::options_description *options) {
  at::add_image_or_snap_spec_options(positional, options,
                                     at::ARGUMENT_MODIFIER_NONE);
  options->add_options()
    ("options,o", po::value<std::string>(), "mapping options")
    ("read-only", po::bool_switch(), "mount read-only");
}

int execute_map(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_PERMITTED);
  if (r < 0) {
    return r;
  }

  utils::init_context();

  return 0;
}

void get_unmap_arguments(po::options_description *positional,
                   po::options_description *options) {
  positional->add_options()
    ("image-or-snap-or-device-spec",
     "image, snapshot, or device specification\n"
     "[<pool-name>/]<image-name>[@<snapshot-name>] or <device-path>");
  at::add_pool_option(options, at::ARGUMENT_MODIFIER_NONE);
  at::add_image_option(options, at::ARGUMENT_MODIFIER_NONE);
  at::add_snap_option(options, at::ARGUMENT_MODIFIER_NONE);
}

int execute_unmap(const po::variables_map &vm) {
  std::string device_name = utils::get_positional_argument(vm, 0);
  if (!boost::starts_with(device_name, "/dev/")) {
    device_name.clear();
  }

  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r;
  if (device_name.empty()) {
    r = utils::get_pool_image_snapshot_names(
      vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
      &snap_name, utils::SNAPSHOT_PRESENCE_PERMITTED,
      false);
    if (r < 0) {
      return r;
    }
  }

  if (device_name.empty() && image_name.empty()) {
    std::cerr << "rbd: unmap requires either image name or device path"
              << std::endl;
    return -EINVAL;
  }

  utils::init_context();

  return 0;
}

Shell::SwitchArguments switched_arguments({"read-only"});
Shell::Action action_show(
  {"showmapped"}, {}, "Show the rbd images mapped by the kernel.", "",
  &get_show_arguments, &execute_show);

Shell::Action action_map(
  {"map"}, {}, "Map image to a block device using the kernel.", "",
  &get_map_arguments, &execute_map);

Shell::Action action_unmap(
  {"unmap"}, {}, "Unmap a rbd device that was used by the kernel.", "",
  &get_unmap_arguments, &execute_unmap);

} // namespace kernel
} // namespace action
} // namespace rbd
