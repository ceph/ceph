// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "acconfig.h"
#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"

#include <boost/program_options.hpp>

#include "include/ceph_assert.h"

namespace rbd {
namespace action {

namespace at = argument_types;
namespace po = boost::program_options;

#define DECLARE_DEVICE_OPERATIONS(ns)                                   \
  namespace ns {                                                        \
  int execute_list(const po::variables_map &vm,                         \
                   const std::vector<std::string> &ceph_global_args);   \
  int execute_map(const po::variables_map &vm,                          \
                  const std::vector<std::string> &ceph_global_args);    \
  int execute_unmap(const po::variables_map &vm,                        \
                    const std::vector<std::string> &ceph_global_args);  \
  }

DECLARE_DEVICE_OPERATIONS(ggate);
DECLARE_DEVICE_OPERATIONS(kernel);
DECLARE_DEVICE_OPERATIONS(nbd);

namespace device {

namespace {

struct DeviceOperations {
  int (*execute_list)(const po::variables_map &vm,
                      const std::vector<std::string> &ceph_global_args);
  int (*execute_map)(const po::variables_map &vm,
                     const std::vector<std::string> &ceph_global_args);
  int (*execute_unmap)(const po::variables_map &vm,
                       const std::vector<std::string> &ceph_global_args);
};

const DeviceOperations ggate_operations = {
  ggate::execute_list,
  ggate::execute_map,
  ggate::execute_unmap,
};

const DeviceOperations krbd_operations = {
  kernel::execute_list,
  kernel::execute_map,
  kernel::execute_unmap,
};

const DeviceOperations nbd_operations = {
  nbd::execute_list,
  nbd::execute_map,
  nbd::execute_unmap,
};

enum device_type_t {
  DEVICE_TYPE_GGATE,
  DEVICE_TYPE_KRBD,
  DEVICE_TYPE_NBD,
};

struct DeviceType {};

void validate(boost::any& v, const std::vector<std::string>& values,
              DeviceType *target_type, int) {
  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);
  if (s == "ggate") {
    v = boost::any(DEVICE_TYPE_GGATE);
  } else if (s == "krbd") {
    v = boost::any(DEVICE_TYPE_KRBD);
  } else if (s == "nbd") {
    v = boost::any(DEVICE_TYPE_NBD);
  } else {
    throw po::validation_error(po::validation_error::invalid_option_value);
  }
}

void add_device_type_option(po::options_description *options) {
  options->add_options()
    ("device-type,t", po::value<DeviceType>(),
     "device type [ggate, krbd (default), nbd]");
}

void add_device_specific_options(po::options_description *options) {
  options->add_options()
    ("options,o", po::value<std::vector<std::string>>(),
     "device specific options");
}

device_type_t get_device_type(const po::variables_map &vm) {
  if (vm.count("device-type")) {
    return vm["device-type"].as<device_type_t>();
  }
  return DEVICE_TYPE_KRBD;
}

const DeviceOperations *get_device_operations(const po::variables_map &vm) {
  switch (get_device_type(vm)) {
  case DEVICE_TYPE_GGATE:
    return &ggate_operations;
  case DEVICE_TYPE_KRBD:
    return &krbd_operations;
  case DEVICE_TYPE_NBD:
    return &nbd_operations;
  default:
    ceph_abort();
    return nullptr;
  }
}

} // anonymous namespace

void get_list_arguments(po::options_description *positional,
                        po::options_description *options) {
  add_device_type_option(options);
  at::add_format_options(options);
}

int execute_list(const po::variables_map &vm,
                 const std::vector<std::string> &ceph_global_init_args) {
  return (*get_device_operations(vm)->execute_list)(vm, ceph_global_init_args);
}

void get_map_arguments(po::options_description *positional,
                       po::options_description *options) {
  add_device_type_option(options);
  at::add_image_or_snap_spec_options(positional, options,
                                     at::ARGUMENT_MODIFIER_NONE);
  options->add_options()
    ("read-only", po::bool_switch(), "map read-only")
    ("exclusive", po::bool_switch(), "disable automatic exclusive lock transitions")
    ("quiesce", po::bool_switch(), "use quiesce hooks")
    ("quiesce-hook", po::value<std::string>(), "quiesce hook path");
  add_device_specific_options(options);
}

int execute_map(const po::variables_map &vm,
                const std::vector<std::string> &ceph_global_init_args) {
  return (*get_device_operations(vm)->execute_map)(vm, ceph_global_init_args);
}

void get_unmap_arguments(po::options_description *positional,
                         po::options_description *options) {
  add_device_type_option(options);
  positional->add_options()
    ("image-or-snap-or-device-spec",
     "image, snapshot, or device specification\n"
     "[<pool-name>/]<image-name>[@<snapshot-name>] or <device-path>");
  at::add_pool_option(options, at::ARGUMENT_MODIFIER_NONE);
  at::add_image_option(options, at::ARGUMENT_MODIFIER_NONE);
  at::add_snap_option(options, at::ARGUMENT_MODIFIER_NONE);
  add_device_specific_options(options);
}

int execute_unmap(const po::variables_map &vm,
                  const std::vector<std::string> &ceph_global_init_args) {
  return (*get_device_operations(vm)->execute_unmap)(vm, ceph_global_init_args);
}

Shell::SwitchArguments switched_arguments({"read-only", "exclusive"});
Shell::Action action_list(
  {"device", "list"}, {"showmapped"}, "List mapped rbd images.", "",
  &get_list_arguments, &execute_list);
// yet another alias for list command
Shell::Action action_ls(
  {"device", "ls"}, {}, "List mapped rbd images.", "",
  &get_list_arguments, &execute_list, false);

Shell::Action action_map(
  {"device", "map"}, {"map"}, "Map an image to a block device.", "",
  &get_map_arguments, &execute_map);

Shell::Action action_unmap(
  {"device", "unmap"}, {"unmap"}, "Unmap a rbd device.", "",
  &get_unmap_arguments, &execute_unmap);

} // namespace device
} // namespace action
} // namespace rbd
