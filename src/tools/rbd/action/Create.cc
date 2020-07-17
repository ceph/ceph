// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/ceph_mutex.h"
#include "common/config_proxy.h"
#include "common/errno.h"
#include "global/global_context.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace create {

namespace at = argument_types;
namespace po = boost::program_options;

static int do_create(librbd::RBD &rbd, librados::IoCtx& io_ctx,
                     const char *imgname, uint64_t size,
		     librbd::ImageOptions& opts) {
  return rbd.create4(io_ctx, imgname, size, opts);
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_create_image_options(options, true);
  options->add_options()
    (at::IMAGE_THICK_PROVISION.c_str(), po::bool_switch(), "fully allocate storage and zero image");
  at::add_size_option(options);
  at::add_no_progress_option(options);
}

void thick_provision_writer_completion(rbd_completion_t, void *);

struct thick_provision_writer {
  librbd::Image *image;
  ceph::mutex lock = ceph::make_mutex("thick_provision_writer::lock");
  ceph::condition_variable cond;
  uint64_t chunk_size;
  uint64_t concurr;
  struct {
    uint64_t in_flight;
    int io_error;
  } io_status;

  // Constructor
  explicit thick_provision_writer(librbd::Image *i, librbd::ImageOptions &o)
    : image(i)
  {
    // If error cases occur, the code is aborted, because
    // constructor cannot return error value.
    ceph_assert(g_ceph_context != nullptr);

    librbd::image_info_t info;
    int r = image->stat(info, sizeof(info));
    ceph_assert(r >= 0);

    uint64_t order = info.order;
    if (order == 0) {
      order = g_conf().get_val<uint64_t>("rbd_default_order");
    }

    auto stripe_count = std::max<uint64_t>(1U, image->get_stripe_count());
    chunk_size = (1ull << order) * stripe_count;

    concurr = std::max<uint64_t>(
      1U, g_conf().get_val<uint64_t>("rbd_concurrent_management_ops") /
            stripe_count);

    io_status.in_flight = 0;
    io_status.io_error = 0;
  }

  int start_io(uint64_t write_offset)
  {
    {
      std::lock_guard l{lock};
      io_status.in_flight++;
      if (io_status.in_flight > concurr) {
        io_status.in_flight--;
        return -EINVAL;
      }
    }

    librbd::RBD::AioCompletion *c;
    c = new librbd::RBD::AioCompletion(this, thick_provision_writer_completion);
    int r;
    r = image->aio_write_zeroes(write_offset, chunk_size, c,
                                RBD_WRITE_ZEROES_FLAG_THICK_PROVISION,
                                LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL);
    if (r < 0) {
      std::lock_guard l{lock};
      io_status.io_error = r;
    }
    return r;
  }

  int wait_for(uint64_t max) {
    std::unique_lock l{lock};
    int r = io_status.io_error;

    while (io_status.in_flight > max) {
      cond.wait_for(l, 200ms);
    }
    return r;
  }
};

void thick_provision_writer_completion(rbd_completion_t rc, void *pc) {
  librbd::RBD::AioCompletion *ac = (librbd::RBD::AioCompletion *)rc;
  thick_provision_writer *tc = static_cast<thick_provision_writer *>(pc);

  int r = ac->get_return_value();
  tc->lock.lock();
  if (r < 0 &&  tc->io_status.io_error >= 0) {
    tc->io_status.io_error = r;
  }
  tc->io_status.in_flight--;
  tc->cond.notify_all();
  tc->lock.unlock();
  ac->release();
}

int write_data(librbd::Image &image, librbd::ImageOptions &opts,
               bool no_progress) {
  uint64_t image_size;
  int r = 0;
  utils::ProgressContext pc("Thick provisioning", no_progress);

  if (image.size(&image_size) != 0) {
    return -EINVAL;
  }

  thick_provision_writer tpw(&image, opts);
  uint64_t off;
  uint64_t i;
  for (off = 0; off < image_size;) {
    i = 0;
    while (i < tpw.concurr && off < image_size) {
      tpw.wait_for(tpw.concurr - 1);
      r = tpw.start_io(off);
      if (r != 0) {
        goto err_writesame;
      }
      ++i;
      off += tpw.chunk_size;
      if(off > image_size) {
        off = image_size;
      }
      pc.update_progress(off, image_size);
    }
  }

  tpw.wait_for(0);
  r = image.flush();
  if (r < 0) {
    std::cerr << "rbd: failed to flush at the end: " << cpp_strerror(r)
              << std::endl;
    goto err_writesame;
  }
  pc.finish();

  return r;

err_writesame:
  tpw.wait_for(0);
  pc.fail();

  return r;
}

int thick_write(const std::string &image_name,librados::IoCtx &io_ctx,
                librbd::ImageOptions &opts, bool no_progress) {
  int r = 0;
  librbd::Image image;

  // To prevent writesame from discarding data, thick_write sets
  // the rbd_discard_on_zeroed_write_same option to false.
  ceph_assert(g_ceph_context != nullptr);
  r = g_conf().set_val("rbd_discard_on_zeroed_write_same", "false");
  ceph_assert(r == 0);
  r = utils::open_image(io_ctx, image_name, false, &image);
  if (r < 0) {
    return r;
  }

  r = write_data(image, opts, no_progress);

  image.close();

  return r;
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
    utils::SPEC_VALIDATION_FULL);
  if (r < 0) {
    return r;
  }

  librbd::ImageOptions opts;
  r = utils::get_image_options(vm, true, &opts);
  if (r < 0) {
    return r;
  }

  uint64_t size;
  r = utils::get_image_size(vm, &size);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = do_create(rbd, io_ctx, image_name.c_str(), size, opts);
  if (!namespace_name.empty() && r == -ENOENT) {
    std::cerr << "rbd: namespace not found - it must be created with "
              << "'rbd namespace create' before creating an image."
              << std::endl;
    return r;
  } else if (r < 0) {
    std::cerr << "rbd: create error: " << cpp_strerror(r) << std::endl;
    return r;
  }

  if (vm.count(at::IMAGE_THICK_PROVISION) && vm[at::IMAGE_THICK_PROVISION].as<bool>()) {
    r = thick_write(image_name, io_ctx, opts, vm[at::NO_PROGRESS].as<bool>());
    if (r < 0) {
      std::cerr << "rbd: image created but error encountered during thick provisioning: "
                << cpp_strerror(r) << std::endl;
      return r;
    }
  }
  return 0;
}

Shell::Action action(
  {"create"}, {}, "Create an empty image.", at::get_long_features_help(),
  &get_arguments, &execute);

} // namespace create
} // namespace action
} // namespace rbd
