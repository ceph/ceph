// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/errno.h"
#include "common/Formatter.h"

namespace rbd {
namespace action {
namespace consgrp {

namespace at = argument_types;
namespace po = boost::program_options;

int execute_create(const po::variables_map &vm) {
  size_t arg_index = 0;

  std::string group_name;
  std::string pool_name;

  int r = utils::get_pool_group_names(vm, at::ARGUMENT_MODIFIER_NONE,
                                      &arg_index, &pool_name, &group_name);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;

  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }
  librbd::RBD rbd;
  r = rbd.group_create(io_ctx, group_name.c_str());
  if (r < 0) {
    std::cerr << "rbd: create error: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

int execute_list(const po::variables_map &vm) {

  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  at::Format::Formatter formatter;
  int r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }
  Formatter *f = formatter.get();

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  std::vector<std::string> names;
  r = rbd.group_list(io_ctx, names);

  if (r == -ENOENT)
    r = 0;
  if (r < 0)
    return r;

  if (f)
    f->open_array_section("consistency_groups");
  for (auto i : names) {
    if (f)
      f->dump_string("name", i);
    else
      std::cout << i << std::endl;
  }
  if (f) {
    f->close_section();
    f->flush(std::cout);
  }

  return 0;
}

int execute_remove(const po::variables_map &vm) {
  size_t arg_index = 0;

  std::string group_name;
  std::string pool_name;

  int r = utils::get_pool_group_names(vm, at::ARGUMENT_MODIFIER_NONE,
                                      &arg_index, &pool_name, &group_name);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;

  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }
  librbd::RBD rbd;

  r = rbd.group_remove(io_ctx, group_name.c_str());
  if (r < 0) {
    std::cerr << "rbd: remove error: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

void get_create_arguments(po::options_description *positional,
                          po::options_description *options) {
  at::add_group_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
}

void get_remove_arguments(po::options_description *positional,
                          po::options_description *options) {
  at::add_group_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
}

void get_list_arguments(po::options_description *positional,
                        po::options_description *options) {
  add_pool_option(options, at::ARGUMENT_MODIFIER_NONE);
  at::add_format_options(options);
}

Shell::Action action_create(
  {"group", "create"}, {}, "Create a consistency group.",
  "", &get_create_arguments, &execute_create);
Shell::Action action_remove(
  {"group", "remove"}, {"group", "rm"}, "Delete a consistency group.",
  "", &get_remove_arguments, &execute_remove);
Shell::Action action_list(
  {"group", "list"}, {"group", "ls"}, "List rbd consistency groups.",
  "", &get_list_arguments, &execute_list);

} // namespace snap
} // namespace action
} // namespace rbd
