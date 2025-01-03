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
namespace copy {

namespace at = argument_types;
namespace po = boost::program_options;

static int do_copy(librbd::Image &src, librados::IoCtx& dest_pp,
		   const char *destname, librbd::ImageOptions& opts,
		   bool no_progress,
		   size_t sparse_size)
{
  utils::ProgressContext pc("Image copy", no_progress);
  int r = src.copy_with_progress4(dest_pp, destname, opts, pc, sparse_size);
  if (r < 0){
    pc.fail();
    return r;
  }
  pc.finish();
  return 0;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_or_snap_spec_options(positional, options,
                                     at::ARGUMENT_MODIFIER_SOURCE);
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_DEST);
  at::add_create_image_options(options, false);
  at::add_sparse_size_option(options);
  at::add_no_progress_option(options);
}

int execute(const po::variables_map &vm,
            const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_SOURCE, &arg_index, &pool_name, &namespace_name,
    &image_name, &snap_name, true, utils::SNAPSHOT_PRESENCE_PERMITTED,
    utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  std::string dst_pool_name;
  std::string dst_namespace_name;
  std::string dst_image_name;
  std::string dst_snap_name;
  r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_DEST, &arg_index, &dst_pool_name,
    &dst_namespace_name, &dst_image_name, &dst_snap_name, true,
    utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_FULL);
  if (r < 0) {
    return r;
  }

  librbd::ImageOptions opts;
  r = utils::get_image_options(vm, false, &opts);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, namespace_name, image_name, "",
                                 snap_name, true, &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  librados::IoCtx dst_io_ctx;
  r = utils::init_io_ctx(rados, dst_pool_name, dst_namespace_name, &dst_io_ctx);
  if (r < 0) {
    return r;
  }

  size_t sparse_size = utils::RBD_DEFAULT_SPARSE_SIZE;
  if (vm.count(at::IMAGE_SPARSE_SIZE)) {
    sparse_size = vm[at::IMAGE_SPARSE_SIZE].as<size_t>();
  }
  r = do_copy(image, dst_io_ctx, dst_image_name.c_str(), opts,
              vm[at::NO_PROGRESS].as<bool>(), sparse_size);
  if (r < 0) {
    std::cerr << "rbd: copy failed: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

Shell::Action action(
  {"copy"}, {"cp"}, "Copy src image to dest.", at::get_long_features_help(),
  &get_arguments, &execute);

static int do_deep_copy(librbd::Image &src, librados::IoCtx& dest_pp,
		        const char *destname, librbd::ImageOptions& opts,
		        bool no_progress)
{
  utils::ProgressContext pc("Image deep copy", no_progress);
  int r = src.deep_copy_with_progress(dest_pp, destname, opts, pc);
  if (r < 0){
    pc.fail();
    return r;
  }
  pc.finish();
  return 0;
}

void get_arguments_deep(po::options_description *positional,
                        po::options_description *options) {
  at::add_image_or_snap_spec_options(positional, options,
                                     at::ARGUMENT_MODIFIER_SOURCE);
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_DEST);
  at::add_create_image_options(options, false);
  at::add_flatten_option(options);
  at::add_no_progress_option(options);
}

int execute_deep(const po::variables_map &vm,
                 const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_SOURCE, &arg_index, &pool_name, &namespace_name,
    &image_name, &snap_name, true, utils::SNAPSHOT_PRESENCE_PERMITTED,
    utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  std::string dst_pool_name;
  std::string dst_namespace_name;
  std::string dst_image_name;
  std::string dst_snap_name;
  r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_DEST, &arg_index, &dst_pool_name,
    &dst_namespace_name, &dst_image_name, &dst_snap_name, true,
    utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_FULL);
  if (r < 0) {
    return r;
  }

  librbd::ImageOptions opts;
  r = utils::get_image_options(vm, false, &opts);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, namespace_name, image_name, "",
                                 snap_name, true, &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  librados::IoCtx dst_io_ctx;
  r = utils::init_io_ctx(rados, dst_pool_name, dst_namespace_name, &dst_io_ctx);
  if (r < 0) {
    return r;
  }

  r = do_deep_copy(image, dst_io_ctx, dst_image_name.c_str(), opts,
                   vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    std::cerr << "rbd: deep copy failed: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

Shell::Action action_deep(
  {"deep", "copy"}, {"deep", "cp"}, "Deep copy (including snapshots) src image to dest.",
  at::get_long_features_help(), &get_arguments_deep, &execute_deep);

} // namespace copy
} // namespace action
} // namespace rbd
