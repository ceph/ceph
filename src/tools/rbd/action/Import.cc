// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "tools/rbd/ExportImport.h"
#include "include/Context.h"
#include "common/blkdev.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Throttle.h"
#include "include/compat.h"
#include "include/encoding.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include <iostream>
#include <boost/program_options.hpp>
#include <boost/scoped_ptr.hpp>
#include "include/ceph_assert.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd

namespace rbd {
namespace action {
namespace import {

int do_import_diff(librados::Rados &rados, librbd::Image &image,
		   const char *path, bool no_progress, size_t sparse_size)
{
  int r;
  int fd;

  if (strcmp(path, "-") == 0) {
    fd = STDIN_FILENO;
  } else {
    fd = open(path, O_RDONLY|O_BINARY);
    if (fd < 0) {
      r = -errno;
      std::cerr << "rbd: error opening " << path << std::endl;
      return r;
    }
  }
  r = utils::do_import_diff_fd(rados, image, fd, no_progress, 1, sparse_size);

  if (fd != 0)
    close(fd);
  return r;
}

namespace at = argument_types;
namespace po = boost::program_options;

void get_arguments_diff(po::options_description *positional,
                   po::options_description *options) {
  at::add_path_options(positional, options,
                       "import file (or '-' for stdin)");
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_sparse_size_option(options);
  at::add_no_progress_option(options);
}

int execute_diff(const po::variables_map &vm,
                 const std::vector<std::string> &ceph_global_init_args) {
  std::string path;
  size_t arg_index = 0;
  int r = utils::get_path(vm, &arg_index, &path);
  if (r < 0) {
    return r;
  }

  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &namespace_name,
    &image_name, &snap_name, true, utils::SNAPSHOT_PRESENCE_NONE,
    utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  size_t sparse_size = utils::RBD_DEFAULT_SPARSE_SIZE;
  if (vm.count(at::IMAGE_SPARSE_SIZE)) {
    sparse_size = vm[at::IMAGE_SPARSE_SIZE].as<size_t>();
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, namespace_name, image_name, "", "",
                                 false, &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = do_import_diff(rados, image, path.c_str(),
		     vm[at::NO_PROGRESS].as<bool>(), sparse_size);
  if (r == -EDOM) {
    r = -EBADMSG;
  }
  if (r < 0) {
    cerr << "rbd: import-diff failed: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

Shell::Action action_diff(
  {"import-diff"}, {}, "Import an incremental diff.", "", &get_arguments_diff,
  &execute_diff);

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

  void finish(int r) override
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

static int do_import_v2(librados::Rados &rados, int fd, librbd::Image &image,
			uint64_t size, size_t imgblklen,
			utils::ProgressContext &pc, size_t sparse_size)
{
  int r = 0;
  r = utils::validate_banner(fd, utils::RBD_IMAGE_DIFFS_BANNER_V2);
  if (r < 0) {
    return r;
  }

  char buf[sizeof(uint64_t)];
  r = safe_read_exact(fd, buf, sizeof(buf));
  if (r < 0) {
    std::cerr << "rbd: failed to decode diff count" << std::endl;
    return r;
  }
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  auto p = bl.cbegin();
  uint64_t diff_num;
  decode(diff_num, p);
  for (size_t i = 0; i < diff_num; i++) {
    r = utils::do_import_diff_fd(rados, image, fd, true, 2, sparse_size);
    if (r < 0) {
      pc.fail();
      std::cerr << "rbd: import-diff failed: " << cpp_strerror(r) << std::endl;
      return r;
    }
    pc.update_progress(i + 1, diff_num);
  }

  return r;
}

static int do_import_v1(int fd, librbd::Image &image, uint64_t size,
                        size_t imgblklen, utils::ProgressContext &pc,
			size_t sparse_size)
{
  int r = 0;
  size_t reqlen = imgblklen;    // amount requested from read
  ssize_t readlen;              // amount received from one read
  size_t blklen = 0;            // amount accumulated from reads to fill blk
  char *p = new char[imgblklen];
  uint64_t image_pos = 0;
  bool from_stdin = (fd == STDIN_FILENO);
  boost::scoped_ptr<SimpleThrottle> throttle;

  if (from_stdin) {
    throttle.reset(new SimpleThrottle(1, false));
  } else {
    throttle.reset(new SimpleThrottle(
      g_conf().get_val<uint64_t>("rbd_concurrent_management_ops"), false));
  }

  reqlen = min<uint64_t>(reqlen, size);
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

    bufferptr blkptr(p, blklen); 
    // resize output image by binary expansion as we go for stdin
    if (from_stdin && (image_pos + (size_t)blklen) > size) {
      size *= 2;
      r = image.resize(size);
      if (r < 0) {
	std::cerr << "rbd: can't resize image during import" << std::endl;
	goto out;
      }
    }

    // write as much as we got; perhaps less than imgblklen
    // but skip writing zeros to create sparse images
    size_t buffer_offset = 0;
    while (buffer_offset < blklen) {
      size_t write_length = 0;
      bool zeroed = false;
      utils::calc_sparse_extent(blkptr, sparse_size, buffer_offset, blklen,
				&write_length, &zeroed);

      if (!zeroed) {
	bufferlist write_bl;
	bufferptr write_ptr(blkptr, buffer_offset, write_length);
	write_bl.push_back(write_ptr);
	ceph_assert(write_bl.length() == write_length);

	C_Import *ctx = new C_Import(*throttle, image, write_bl,
				     image_pos + buffer_offset);
	ctx->send();
      }

      buffer_offset += write_length;
    }

    // done with whole block, whether written or not
    image_pos += blklen;
    if (!from_stdin && image_pos >= size)
      break;
    // if read had returned 0, we're at EOF and should quit
    if (readlen == 0)
      break;
    blklen = 0;
    reqlen = imgblklen;
  }
  r = throttle->wait_for_ret();
  if (r < 0) {
    goto out;
  }

  if (fd == STDIN_FILENO) {
    r = image.resize(image_pos);
    if (r < 0) {
      std::cerr << "rbd: final image resize failed" << std::endl;
      goto out;
    }
  }
out:
  delete[] p;
  return r;
}

static int do_import(librados::Rados &rados, librbd::RBD &rbd,
		     librados::IoCtx& io_ctx, const char *imgname,
		     const char *path, librbd::ImageOptions& opts,
		     bool no_progress, int import_format, size_t sparse_size)
{
  int fd, r;
  struct stat stat_buf;
  utils::ProgressContext pc("Importing image", no_progress);
  std::map<std::string, std::string> imagemetas;

  ceph_assert(imgname);

  uint64_t order;
  if (opts.get(RBD_IMAGE_OPTION_ORDER, &order) != 0) {
    order = g_conf().get_val<uint64_t>("rbd_default_order");
  }

  // try to fill whole imgblklen blocks for sparsification
  size_t imgblklen = 1 << order;
  librbd::Image image;
  uint64_t size = 0;

  bool from_stdin = !strcmp(path, "-");
  if (from_stdin) {
    fd = STDIN_FILENO;
    size = 1ULL << order;
  } else {
    if ((fd = open(path, O_RDONLY|O_BINARY)) < 0) {
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
      BlkDev blkdev(fd);
      r = blkdev.get_size(&bdev_size);
      if (r < 0) {
        std::cerr << "rbd: unable to get size of file/block device"
                  << std::endl;
        goto done;
      }
      ceph_assert(bdev_size >= 0);
      size = (uint64_t) bdev_size;
    }
#ifdef HAVE_POSIX_FADVISE
    posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif
  }

  r = utils::do_import_header(fd, import_format, opts, &imagemetas);
  if (r < 0) {
    std::cerr << "rbd: import header failed." << std::endl;
    goto done;
  }

  r = rbd.create4(io_ctx, imgname, size, opts);
  if (r < 0) {
    std::cerr << "rbd: image creation failed" << std::endl;
    goto done;
  }

  r = rbd.open(io_ctx, image, imgname);
  if (r < 0) {
    std::cerr << "rbd: failed to open image" << std::endl;
    goto err;
  }

  r = utils::do_import_metadata(import_format, image, imagemetas);
  if (r < 0) {
    std::cerr << "rbd: failed to import image-meta" << std::endl;
    goto err;
  }

  if (import_format == 1) {
    r = do_import_v1(fd, image, size, imgblklen, pc, sparse_size);
  } else {
    r = do_import_v2(rados, fd, image, size, imgblklen, pc, sparse_size);
  }
  if (r < 0) {
    std::cerr << "rbd: failed to import image" << std::endl;
    image.close();
    goto err;
  }

  r = image.close();
err:
  if (r < 0)
    rbd.remove(io_ctx, imgname);
done:
  if (r < 0)
    pc.fail();
  else
    pc.finish();
  if (!from_stdin)
    close(fd);
done2:
  return r;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_path_options(positional, options,
                       "import file (or '-' for stdin)");
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_DEST);
  at::add_create_image_options(options, true);
  at::add_sparse_size_option(options);
  at::add_no_progress_option(options);
  at::add_export_format_option(options);

  // TODO legacy rbd allowed import to accept both 'image'/'dest' and
  //      'pool'/'dest-pool'
  at::add_pool_option(options, at::ARGUMENT_MODIFIER_NONE, " deprecated[:dest-pool]");
  at::add_image_option(options, at::ARGUMENT_MODIFIER_NONE, " deprecated[:dest]");
}

int execute(const po::variables_map &vm,
            const std::vector<std::string> &ceph_global_init_args) {
  std::string path;
  size_t arg_index = 0;
  int r = utils::get_path(vm, &arg_index, &path);
  if (r < 0) {
    return r;
  }

  // odd check to support legacy / deprecated behavior of import
  std::string deprecated_pool_name;
  if (vm.count(at::POOL_NAME)) {
    deprecated_pool_name = vm[at::POOL_NAME].as<std::string>();
  }

  std::string deprecated_image_name;
  if (vm.count(at::IMAGE_NAME)) {
    deprecated_image_name = vm[at::IMAGE_NAME].as<std::string>();
  } else {
    deprecated_image_name = path.substr(path.find_last_of("/\\") + 1);
  }

  std::string deprecated_snap_name;
  r = utils::extract_spec(deprecated_image_name, &deprecated_pool_name,
                          nullptr, &deprecated_image_name,
                          &deprecated_snap_name, utils::SPEC_VALIDATION_FULL);
  if (r < 0) {
    return r;
  }

  size_t sparse_size = utils::RBD_DEFAULT_SPARSE_SIZE;
  if (vm.count(at::IMAGE_SPARSE_SIZE)) {
    sparse_size = vm[at::IMAGE_SPARSE_SIZE].as<size_t>();
  }

  std::string pool_name = deprecated_pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name = deprecated_snap_name;
  r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_DEST, &arg_index, &pool_name, &namespace_name,
    &image_name, &snap_name, false, utils::SNAPSHOT_PRESENCE_NONE,
    utils::SPEC_VALIDATION_FULL);
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
  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  int format = 1;
  if (vm.count("export-format"))
    format = vm["export-format"].as<uint64_t>();

  librbd::RBD rbd;
  r = do_import(rados, rbd, io_ctx, image_name.c_str(), path.c_str(),
                opts, vm[at::NO_PROGRESS].as<bool>(), format, sparse_size);
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
