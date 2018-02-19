// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/errno.h"
#include "include/stringify.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace remove {

namespace {

bool is_auto_delete_snapshot(librbd::Image* image,
                             const librbd::snap_info_t &snap_info) {
  librbd::snap_namespace_type_t namespace_type;
  int r = image->snap_get_namespace_type(snap_info.id, &namespace_type);
  if (r < 0) {
    return false;
  }

  switch (namespace_type) {
  case RBD_SNAP_NAMESPACE_TYPE_TRASH:
    return true;
  default:
    return false;
  }
}

} // anonymous namespace

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

int execute(const po::variables_map &vm,
            const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  io_ctx.set_osdmap_full_try();

  librbd::RBD rbd;
  r = do_delete(rbd, io_ctx, image_name.c_str(),
                vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    if (r == -ENOTEMPTY) {
      librbd::Image image;
      std::vector<librbd::snap_info_t> snaps;
      int image_r = utils::open_image(io_ctx, image_name, true, &image);
      if (image_r >= 0) {
        image_r = image.snap_list(snaps);
      }
      if (image_r >= 0) {
        snaps.erase(std::remove_if(snaps.begin(), snaps.end(),
			           [&image](const librbd::snap_info_t& snap) {
                                     return is_auto_delete_snapshot(&image,
                                                                    snap);
                                   }),
                    snaps.end());
      }

      if (!snaps.empty()) {
        std::cerr << "rbd: image has snapshots - these must be deleted"
                  << " with 'rbd snap purge' before the image can be removed."
                  << std::endl;
      } else {
        std::cerr << "rbd: image has snapshots with linked clones - these must "
                  << "be deleted or flattened before the image can be removed."
                  << std::endl;
      }
    } else if (r == -EBUSY) {
      std::cerr << "rbd: error: image still has watchers"
                << std::endl
                << "This means the image is still open or the client using "
                << "it crashed. Try again after closing/unmapping it or "
                << "waiting 30s for the crashed client to timeout."
                << std::endl;
    } else if (r == -EMLINK) {
      librbd::Image image;
      int image_r = utils::open_image(io_ctx, image_name, true, &image);
      librbd::group_info_t group_info;
      if (image_r == 0) {
	image_r = image.get_group(&group_info, sizeof(group_info));
      }
      if (image_r == 0) {
        std::string pool_name = "";
        librados::Rados rados(io_ctx);
        librados::IoCtx pool_io_ctx;
        image_r = rados.ioctx_create2(group_info.pool, pool_io_ctx);
        if (image_r < 0) {
          pool_name = "<missing data pool " + stringify(group_info.pool) + ">";
        } else {
          pool_name = pool_io_ctx.get_pool_name();
        }
        std::cerr << "rbd: error: image belongs to a group "
                  << pool_name << "/" << group_info.name;
      } else
	std::cerr << "rbd: error: image belongs to a group";

      std::cerr << std::endl
		<< "Remove the image from the group and try again."
		<< std::endl;
      image.close();
    } else {
      std::cerr << "rbd: delete error: " << cpp_strerror(r) << std::endl;
    }
    return r;
  }
  return 0;
}

Shell::Action action(
  {"remove"}, {"rm"}, "Delete an image.", "", &get_arguments, &execute);

} // namespace remove
} // namespace action
} // namespace rbd
