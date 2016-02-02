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
namespace remove {

namespace at = argument_types;
namespace po = boost::program_options;

static int do_delete(librbd::RBD &rbd, librados::IoCtx& io_ctx,
                     const char *imgname, bool no_progress)
{
  utils::ProgressContext pc("Removing image", no_progress);
  int r = rbd.remove_with_progress(io_ctx, imgname, pc);
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
}

int execute(const po::variables_map &vm) {
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
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = do_delete(rbd, io_ctx, image_name.c_str(),
                vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    if (r == -ENOTEMPTY) {
      std::cerr << "rbd: image has snapshots - these must be deleted"
                << " with 'rbd snap purge' before the image can be removed."
                << std::endl;
    } else if (r == -EBUSY) {
      std::cerr << "rbd: error: image still has watchers"
                << std::endl
                << "This means the image is still open or the client using "
                << "it crashed. Try again after closing/unmapping it or "
                << "waiting 30s for the crashed client to timeout."
                << std::endl;
    } else {
      std::cerr << "rbd: delete error: " << cpp_strerror(r) << std::endl;
    }
    return r ;
  }
  return 0;
}

Shell::Action action(
  {"remove"}, {"rm"}, "Delete an image.", "", &get_arguments, &execute);

} // namespace remove
} // namespace action
} // namespace rbd
