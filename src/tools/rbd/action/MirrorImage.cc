// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/stringify.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/TextTable.h"
#include "global/global_context.h"
#include <iostream>
#include <boost/program_options.hpp>
#include <boost/regex.hpp>

namespace rbd {
namespace action {
namespace mirror_image {

namespace at = argument_types;
namespace po = boost::program_options;


void get_arguments(po::options_description *positional,
                           po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
}

void get_arguments_disable(po::options_description *positional,
                           po::options_description *options) {
  options->add_options()
    ("force", po::bool_switch(), "disable even if not primary");
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
}

int execute_enable_disable(const po::variables_map &vm, bool enable,
                           bool force) {
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

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, "", false,
                                 &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = enable ? image.mirror_image_enable() : image.mirror_image_disable(force);
  if (r < 0) {
    return r;
  }

  std::cout << (enable ? "Mirroring enabled" : "Mirroring disabled")
    << std::endl;

  return 0;
}

int execute_disable(const po::variables_map &vm) {
  return execute_enable_disable(vm, false, vm["force"].as<bool>());
}

int execute_enable(const po::variables_map &vm) {
  return execute_enable_disable(vm, true, false);
}

Shell::Action action_enable(
  {"mirror", "image", "enable"}, {},
  "Enable RBD mirroring for an image.", "",
  &get_arguments, &execute_enable);
Shell::Action action_disable(
  {"mirror", "image", "disable"}, {},
  "Disable RBD mirroring for an image.", "",
  &get_arguments_disable, &execute_disable);

} // namespace mirror_image
} // namespace action
} // namespace rbd
