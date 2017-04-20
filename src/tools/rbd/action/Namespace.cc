// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/rbd_types.h"
#include "cls/rbd/cls_rbd_types.h"
#include "common/errno.h"
#include "common/Formatter.h"

namespace rbd {
namespace action {
namespace ns {

namespace at = argument_types;
namespace po = boost::program_options;

int execute_remove(const po::variables_map &vm) {
  size_t arg_index = 0;

  std::string ns_name;
  std::string pool_name;

  int r = utils::get_special_pool_ns_names(vm, &arg_index,
					   &pool_name,
					   &ns_name);
  if (r < 0) {
    std::cerr << "rbd: namespace remove error: " << cpp_strerror(r) << std::endl;
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, "", &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.namespace_remove(io_ctx, ns_name);
  if (r < 0) {
    std::cerr << "rbd: namespace remove error: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}


void get_remove_arguments(po::options_description *positional,
                          po::options_description *options) {
  add_pool_option(options, at::ARGUMENT_MODIFIER_NONE);
  positional->add_options()
    ("namespace", "namespace name.");
}

int execute_create(const po::variables_map &vm) {
  size_t arg_index = 0;

  std::string ns_name;
  std::string pool_name;

  int r = utils::get_special_pool_ns_names(vm, &arg_index,
					   &pool_name,
					   &ns_name);
  if (r < 0) {
    std::cerr << "rbd: namespace create error: " << cpp_strerror(r) << std::endl;
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, "", &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.namespace_add(io_ctx, ns_name);
  if (r < 0) {
    std::cerr << "rbd: namespace create error: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}


void get_create_arguments(po::options_description *positional,
                          po::options_description *options) {
  add_pool_option(options, at::ARGUMENT_MODIFIER_NONE);
  positional->add_options()
    ("namespace", "namespace name.");
}

int execute_list(const po::variables_map &vm) {
  size_t arg_index = 0;

  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  at::Format::Formatter formatter;
  int r = utils::get_formatter(vm, &formatter);
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
  set<std::string> namespaces;
  r = rbd.namespace_list(io_ctx, namespaces);
  if (r < 0) {
    return r;
  }

  Formatter *f = formatter.get();
  if (f) {
    f->open_array_section("namespaces");
  }

  for (auto ns : namespaces) {
    if (ns == "")
      ns = "default(\"\")";

    if (f)
      f->dump_string("name", ns);
    else
      std::cout << ns << std::endl;
  }

  if (f) {
    f->close_section();
    f->flush(std::cout);
  }

  return 0;
}


void get_list_arguments(po::options_description *positional,
                        po::options_description *options) {
  add_pool_option(options, at::ARGUMENT_MODIFIER_NONE);
  positional->add_options()
    ("namespace", "namespace name.");
  at::add_format_options(options);
}

Shell::Action action_create(
  {"ns", "create"}, {}, "Create a namespace of a pool.",
  "", &get_create_arguments, &execute_create);
Shell::Action action_remove(
  {"ns", "remove"}, {"ns", "rm"}, "Create a namespace of a pool.",
  "", &get_remove_arguments, &execute_remove);
Shell::Action action_list(
  {"ns", "list"}, {"ns", "ls"}, "List all namespaces of a pool.",
  "", &get_list_arguments, &execute_list);
} // namespace ns
} // namespace action
} // namespace rbd
