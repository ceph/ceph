// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/Formatter.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace lock {

namespace at = argument_types;
namespace po = boost::program_options;

namespace {

void add_id_option(po::options_description *positional) {
  positional->add_options()
    ("lock-id", "unique lock id");
}

int get_id(const po::variables_map &vm, std::string *id) {
  *id = utils::get_positional_argument(vm, 1);
  if (id->empty()) {
    std::cerr << "rbd: lock id was not specified" << std::endl;
    return -EINVAL;
  }
  return 0;
}

} // anonymous namespace

void get_list_arguments(po::options_description *positional,
                        po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_format_options(options);
}

int execute_list(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_NONE);
  if (r < 0) {
    return r;
  }

  at::Format::Formatter formatter;
  r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, "", true,
                                 &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  return 0;
}

void get_add_arguments(po::options_description *positional,
                       po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  add_id_option(positional);
  options->add_options()
    ("shared", po::value<std::string>(), "shared lock tag");
}

int execute_add(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_NONE);
  if (r < 0) {
    return r;
  }

  std::string lock_cookie;
  r = get_id(vm, &lock_cookie);
  if (r < 0) {
    return r;
  }

  std::string lock_tag;
  if (vm.count("shared")) {
    lock_tag = vm["shared"].as<std::string>();
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, "", false,
                                 &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  return 0;
}

void get_remove_arguments(po::options_description *positional,
                          po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  add_id_option(positional);
  positional->add_options()
    ("locker", "locker client");
}

int execute_remove(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_NONE);
  if (r < 0) {
    return r;
  }

  std::string lock_cookie;
  r = get_id(vm, &lock_cookie);
  if (r < 0) {
    return r;
  }

  std::string lock_client = utils::get_positional_argument(vm, 2);
  if (lock_client.empty()) {
    std::cerr << "rbd: locker was not specified" << std::endl;
    return -EINVAL;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, "", false,
                                 &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  return 0;
}

Shell::Action action_list(
  {"lock", "list"}, {}, "Show locks held on an image.", "",
  &get_list_arguments, &execute_list);
Shell::Action action_add(
  {"lock", "add"}, {}, "Take a lock on an image.", "",
  &get_add_arguments, &execute_add);
Shell::Action action_remove(
  {"lock", "remove"}, {"lock", "rm"}, "Release a lock on an image.", "",
  &get_remove_arguments, &execute_remove);

} // namespace lock
} // namespace action
} // namespace rbd
