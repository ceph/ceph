
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "common/Formatter.h"
#include "common/TextTable.h"
#include <algorithm>
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace ns {

namespace at = argument_types;
namespace po = boost::program_options;

void get_create_arguments(po::options_description *positional,
                          po::options_description *options) {
  at::add_pool_options(positional, options, true);
}

int execute_create(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  std::string pool_name;
  std::string namespace_name;
  size_t arg_index = 0;
  int r = utils::get_pool_and_namespace_names(vm, true, true, &pool_name,
                                              &namespace_name, &arg_index);
  if (r < 0) {
    return r;
  }

  if (namespace_name.empty()) {
    std::cerr << "rbd: namespace name was not specified" << std::endl;
    return -EINVAL;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, "", &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.namespace_create(io_ctx, namespace_name.c_str());
  if (r < 0) {
    std::cerr << "rbd: failed to created namespace: " << cpp_strerror(r)
              << std::endl;
    return r;
  }

  return 0;
}

void get_remove_arguments(po::options_description *positional,
                          po::options_description *options) {
  at::add_pool_options(positional, options, true);
}

int execute_remove(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  std::string pool_name;
  std::string namespace_name;
  size_t arg_index = 0;
  int r = utils::get_pool_and_namespace_names(vm, true, true, &pool_name,
                                              &namespace_name, &arg_index);
  if (r < 0) {
    return r;
  }

  if (namespace_name.empty()) {
    std::cerr << "rbd: namespace name was not specified" << std::endl;
    return -EINVAL;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, "", &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.namespace_remove(io_ctx, namespace_name.c_str());
  if (r == -EBUSY) {
    std::cerr << "rbd: namespace contains images which must be deleted first."
              << std::endl;
    return r;
  } else if (r == -ENOENT) {
    std::cerr << "rbd: namespace does not exist." << std::endl;
    return r;
  } else if (r < 0) {
    std::cerr << "rbd: failed to remove namespace: " << cpp_strerror(r)
              << std::endl;
    return r;
  }

  return 0;
}

void get_list_arguments(po::options_description *positional,
                        po::options_description *options) {
  at::add_pool_options(positional, options, false);
  at::add_format_options(options);
}

int execute_list(const po::variables_map &vm,
                 const std::vector<std::string> &ceph_global_init_args) {
  std::string pool_name;
  size_t arg_index = 0;
  int r = utils::get_pool_and_namespace_names(vm, true, true, &pool_name,
                                              nullptr, &arg_index);
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
  r = utils::init(pool_name, "", &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  std::vector<std::string> names;
  r = rbd.namespace_list(io_ctx, &names);
  if (r < 0 && r != -ENOENT) {
    std::cerr << "rbd: failed to list namespaces: " << cpp_strerror(r)
              << std::endl;
    return r;
  }

  std::sort(names.begin(), names.end());

  TextTable tbl;
  if (formatter) {
    formatter->open_array_section("namespaces");
  } else {
    tbl.define_column("NAME", TextTable::LEFT, TextTable::LEFT);
  }

  for (auto& name : names) {
    if (formatter) {
      formatter->open_object_section("namespace");
      formatter->dump_string("name", name);
      formatter->close_section();
    } else {
      tbl << name << TextTable::endrow;
    }
  }

  if (formatter) {
    formatter->close_section();
    formatter->flush(std::cout);
  } else if (!names.empty()) {
    std::cout << tbl;
  }

  return 0;
}

Shell::Action action_create(
  {"namespace", "create"}, {},
   "Create an RBD image namespace.", "",
  &get_create_arguments, &execute_create);

Shell::Action action_remove(
  {"namespace", "remove"}, {"namespace", "rm"},
   "Remove an RBD image namespace.", "",
  &get_remove_arguments, &execute_remove);

Shell::Action action_list(
  {"namespace", "list"}, {"namespace", "ls"}, "List RBD image namespaces.", "",
  &get_list_arguments, &execute_list);

} // namespace ns
} // namespace action
} // namespace rbd
