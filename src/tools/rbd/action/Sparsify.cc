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
namespace sparsify {

namespace at = argument_types;
namespace po = boost::program_options;

static int do_sparsify(librbd::Image& image, size_t sparse_size,
                       bool no_progress)
{
  utils::ProgressContext pc("Image sparsify", no_progress);
  int r = image.sparsify_with_progress(sparse_size, pc);
  if (r < 0) {
    pc.fail();
    return r;
  }
  pc.finish();
  return 0;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_no_progress_option(options);
  at::add_sparse_size_option(options);
}

int execute(const po::variables_map &vm,
            const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &namespace_name,
    &image_name, &snap_name, true, utils::SNAPSHOT_PRESENCE_NONE,
    utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, namespace_name, image_name, "", "",
                                 false, &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  size_t sparse_size = utils::RBD_DEFAULT_SPARSE_SIZE;
  if (vm.count(at::IMAGE_SPARSE_SIZE)) {
    sparse_size = vm[at::IMAGE_SPARSE_SIZE].as<size_t>();
  }

  r = do_sparsify(image, sparse_size, vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    std::cerr << "rbd: sparsify error: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

Shell::Action action(
  {"sparsify"}, {},
  "Reclaim space for zeroed image extents.", "",
  &get_arguments, &execute);

} // namespace sparsify
} // namespace action
} // namespace rbd
