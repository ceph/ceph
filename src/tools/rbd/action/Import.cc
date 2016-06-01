// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/Context.h"
#include "common/blkdev.h"
#include "common/errno.h"
#include "common/Throttle.h"
#include <iostream>
#include <boost/program_options.hpp>
#include <boost/scoped_ptr.hpp>

namespace rbd {
namespace action {
namespace import {

namespace at = argument_types;
namespace po = boost::program_options;

class C_Import : public Context {
public:
  C_Import(SimpleThrottle &simple_throttle, librbd::Image &image,
           bufferlist &bl, uint64_t offset)
    : m_throttle(simple_throttle), m_image(image),
      m_aio_completion(
        new librbd::RBD::AioCompletion(this, &utils::aio_context_callback)),
      m_bufferlist(bl), m_offset(offset)
  {
  }

  void send()
  {
    m_throttle.start_op();

    int op_flags = LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL |
                   LIBRADOS_OP_FLAG_FADVISE_NOCACHE;
    int r = m_image.aio_write2(m_offset, m_bufferlist.length(), m_bufferlist,
                               m_aio_completion, op_flags);
    if (r < 0) {
      std::cerr << "rbd: error requesting write to destination image"
                << std::endl;
      m_aio_completion->release();
      m_throttle.end_op(r);
    }
  }

  virtual void finish(int r)
  {
    if (r < 0) {
      std::cerr << "rbd: error writing to destination image at offset "
                << m_offset << ": " << cpp_strerror(r) << std::endl;
    }
    m_throttle.end_op(r);
  }

private:
  SimpleThrottle &m_throttle;
  librbd::Image &m_image;
  librbd::RBD::AioCompletion *m_aio_completion;
  bufferlist m_bufferlist;
  uint64_t m_offset;
};

static int do_import(librbd::RBD &rbd, librados::IoCtx& io_ctx,
                     const char *imgname, const char *path,
		     librbd::ImageOptions& opts, bool no_progress)
{
  int fd, r;
  struct stat stat_buf;
  utils::ProgressContext pc("Importing image", no_progress);

  assert(imgname);

  uint64_t order;
  r = opts.get(RBD_IMAGE_OPTION_ORDER, &order);
  assert(r == 0);

  // try to fill whole imgblklen blocks for sparsification
  uint64_t image_pos = 0;
  size_t imgblklen = 1 << order;
  char *p = new char[imgblklen];
  size_t reqlen = imgblklen;    // amount requested from read
  ssize_t readlen;              // amount received from one read
  size_t blklen = 0;            // amount accumulated from reads to fill blk
  librbd::Image image;
  uint64_t size = 0;

  boost::scoped_ptr<SimpleThrottle> throttle;
  bool from_stdin = !strcmp(path, "-");
  if (from_stdin) {
    throttle.reset(new SimpleThrottle(1, false));
    fd = 0;
    size = 1ULL << order;
  } else {
    throttle.reset(new SimpleThrottle(
      max(g_conf->rbd_concurrent_management_ops, 1), false));
    if ((fd = open(path, O_RDONLY)) < 0) {
      r = -errno;
      std::cerr << "rbd: error opening " << path << std::endl;
      goto done2;
    }

    if ((fstat(fd, &stat_buf)) < 0) {
      r = -errno;
      std::cerr << "rbd: stat error " << path << std::endl;
      goto done;
    }
    if (S_ISDIR(stat_buf.st_mode)) {
      r = -EISDIR;
      std::cerr << "rbd: cannot import a directory" << std::endl;
      goto done;
    }
    if (stat_buf.st_size)
      size = (uint64_t)stat_buf.st_size;

    if (!size) {
      int64_t bdev_size = 0;
      r = get_block_device_size(fd, &bdev_size);
      if (r < 0) {
        std::cerr << "rbd: unable to get size of file/block device"
                  << std::endl;
        goto done;
      }
      assert(bdev_size >= 0);
      size = (uint64_t) bdev_size;
    }

    posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
  }

  uint64_t format;
  r = opts.get(RBD_IMAGE_OPTION_FORMAT, &format);
  assert(r == 0);
  if (format == 1) {
    uint64_t stripe_unit, stripe_count;
    r = opts.get(RBD_IMAGE_OPTION_STRIPE_UNIT, &stripe_unit);
    assert(r == 0);
    r = opts.get(RBD_IMAGE_OPTION_STRIPE_COUNT, &stripe_count);
    assert(r == 0);

    // weird striping not allowed with format 1!
    if ((stripe_unit || stripe_count) &&
        (stripe_unit != (1ull << order) && stripe_count != 1)) {
      std::cerr << "non-default striping not allowed with format 1; "
                << "use --image-format 2" << std::endl;
      return -EINVAL;
    }
    int order_ = order;
    r = rbd.create(io_ctx, imgname, size, &order_);
  } else {
    r = rbd.create4(io_ctx, imgname, size, opts);
  }
  if (r < 0) {
    std::cerr << "rbd: image creation failed" << std::endl;
    goto done;
  }

  r = rbd.open(io_ctx, image, imgname);
  if (r < 0) {
    std::cerr << "rbd: failed to open image" << std::endl;
    goto done;
  }

  // loop body handles 0 return, as we may have a block to flush
  while ((readlen = ::read(fd, p + blklen, reqlen)) >= 0) {
    if (throttle->pending_error()) {
      break;
    }

    blklen += readlen;
    // if read was short, try again to fill the block before writing
    if (readlen && ((size_t)readlen < reqlen)) {
      reqlen -= readlen;
      continue;
    }
    if (!from_stdin)
      pc.update_progress(image_pos, size);

    bufferlist bl(blklen);
    bl.append(p, blklen);
    // resize output image by binary expansion as we go for stdin
    if (from_stdin && (image_pos + (size_t)blklen) > size) {
      size *= 2;
      r = image.resize(size);
      if (r < 0) {
        std::cerr << "rbd: can't resize image during import" << std::endl;
        goto done;
      }
    }

    // write as much as we got; perhaps less than imgblklen
    // but skip writing zeros to create sparse images
    if (!bl.is_zero()) {
      C_Import *ctx = new C_Import(*throttle, image, bl, image_pos);
      ctx->send();
    }

    // done with whole block, whether written or not
    image_pos += blklen;
    // if read had returned 0, we're at EOF and should quit
    if (readlen == 0)
      break;
    blklen = 0;
    reqlen = imgblklen;
  }
  r = throttle->wait_for_ret();
  if (r < 0) {
    goto done;
  }

  if (from_stdin) {
    r = image.resize(image_pos);
    if (r < 0) {
      std::cerr << "rbd: final image resize failed" << std::endl;
      goto done;
    }
  }

  r = image.close();

 done:
  if (!from_stdin) {
    if (r < 0)
      pc.fail();
    else
      pc.finish();
    close(fd);
  }
 done2:
  delete[] p;
  return r;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_path_options(positional, options,
                       "import file (or '-' for stdin)");
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_DEST);
  at::add_create_image_options(options, true);
  at::add_no_progress_option(options);

  // TODO legacy rbd allowed import to accept both 'image'/'dest' and
  //      'pool'/'dest-pool'
  at::add_pool_option(options, at::ARGUMENT_MODIFIER_NONE, " (deprecated)");
  at::add_image_option(options, at::ARGUMENT_MODIFIER_NONE, " (deprecated)");
}

int execute(const po::variables_map &vm) {
  std::string path;
  int r = utils::get_path(vm, utils::get_positional_argument(vm, 0), &path);
  if (r < 0) {
    return r;
  }

  // odd check to support legacy / deprecated behavior of import
  std::string deprecated_pool_name;
  if (vm.count(at::POOL_NAME)) {
    deprecated_pool_name = vm[at::POOL_NAME].as<std::string>();
    std::cerr << "rbd: --pool is deprecated for import, use --dest-pool"
              << std::endl;
  }

  std::string deprecated_image_name;
  if (vm.count(at::IMAGE_NAME)) {
    utils::extract_spec(vm[at::IMAGE_NAME].as<std::string>(),
                        &deprecated_pool_name, &deprecated_image_name, nullptr,
                        utils::SPEC_VALIDATION_FULL);
    std::cerr << "rbd: --image is deprecated for import, use --dest"
              << std::endl;
  } else {
    deprecated_image_name = path.substr(path.find_last_of("/") + 1);
  }

  size_t arg_index = 1;
  std::string pool_name = deprecated_pool_name;
  std::string image_name;
  std::string snap_name;
  r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_DEST, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_FULL,
    false);
  if (r < 0) {
    return r;
  }

  if (image_name.empty()) {
    image_name = deprecated_image_name;
  }

  librbd::ImageOptions opts;
  r = utils::get_image_options(vm, true, &opts);
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
  r = do_import(rbd, io_ctx, image_name.c_str(), path.c_str(),
                opts, vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    std::cerr << "rbd: import failed: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

Shell::Action action(
  {"import"}, {}, "Import image from file.", at::get_long_features_help(),
  &get_arguments, &execute);

} // namespace import
} // namespace action
} // namespace rbd
