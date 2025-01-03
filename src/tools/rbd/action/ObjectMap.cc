// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/errno.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace object_map {

namespace at = argument_types;
namespace po = boost::program_options;

static int do_object_map_rebuild(librbd::Image &image, bool no_progress)
{
  utils::ProgressContext pc("Object Map Rebuild", no_progress);
  int r = image.rebuild_object_map(pc);
  if (r < 0) {
    pc.fail();
    return r;
  }
  pc.finish();
  return 0;
}

void get_rebuild_arguments(po::options_description *positional,
			   po::options_description *options) {
  at::add_image_or_snap_spec_options(positional, options,
                                     at::ARGUMENT_MODIFIER_NONE);
  at::add_no_progress_option(options);
}

int execute_rebuild(const po::variables_map &vm,
                    const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &namespace_name,
    &image_name, &snap_name, true, utils::SNAPSHOT_PRESENCE_PERMITTED,
    utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, namespace_name, image_name, "",
                                 snap_name, false, &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = do_object_map_rebuild(image, vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    std::cerr << "rbd: rebuilding object map failed: " << cpp_strerror(r)
              << std::endl;
    return r;
  }
  return 0;
}

static int do_object_map_check(librbd::Image &image, bool no_progress)
{
  utils::ProgressContext pc("Object Map Check", no_progress);
  int r = image.check_object_map(pc);
  if (r < 0) {
    pc.fail();
    return r;
  }
  pc.finish();
  return 0;
}

void get_check_arguments(po::options_description *positional,
		   po::options_description *options) {
  at::add_image_or_snap_spec_options(positional, options,
				     at::ARGUMENT_MODIFIER_NONE);
  at::add_no_progress_option(options);
}

int execute_check(const po::variables_map &vm,
                  const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &namespace_name,
    &image_name, &snap_name, true, utils::SNAPSHOT_PRESENCE_PERMITTED,
    utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, namespace_name, image_name, "",
                                 snap_name, false, &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = do_object_map_check(image, vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    std::cerr << "rbd: checking object map failed: " << cpp_strerror(r)
	      << std::endl;
    return r;
  }
  return 0;
}

Shell::Action action_rebuild(
  {"object-map", "rebuild"}, {}, "Rebuild an invalid object map.", "",
  &get_rebuild_arguments, &execute_rebuild);
Shell::Action action_check(
  {"object-map", "check"}, {}, "Verify the object map is correct.", "",
  &get_check_arguments, &execute_check);

} // namespace object_map
} // namespace action
} // namespace rbd
