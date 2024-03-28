// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
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

using std::cerr;
using std::string;

namespace rbd {
namespace action {
namespace import {

struct ImportDiffContext {
  librbd::Image *image;
  int fd;
  size_t size;
  utils::ProgressContext pc;
  OrderedThrottle throttle;
  uint64_t last_offset;

  ImportDiffContext(librbd::Image *image, int fd, size_t size, bool no_progress)
    : image(image), fd(fd), size(size), pc("Importing image diff", no_progress),
      throttle((fd == STDIN_FILENO) ? 1 :
                  g_conf().get_val<uint64_t>("rbd_concurrent_management_ops"),
               false),
      last_offset(0) {
  }

  void update_size(size_t new_size)
  {
    if (fd == STDIN_FILENO) {
      size = new_size;
    }
  }

  void update_progress(uint64_t off)
  {
    if (size) {
      pc.update_progress(off, size);
      last_offset = off;
    }
  }

  void update_progress()
  {
    uint64_t off = last_offset;
    if (fd != STDIN_FILENO) {
      off = lseek(fd, 0, SEEK_CUR);
    }

    update_progress(off);
  }

  void finish(int r)
  {
    if (r < 0) {
      pc.fail();
    } else {
      pc.finish();
    }
  }
};

class C_ImportDiff : public Context {
public:
  C_ImportDiff(ImportDiffContext *idiffctx, bufferlist data, uint64_t offset,
               uint64_t length, bool write_zeroes)
    : m_idiffctx(idiffctx), m_data(data), m_offset(offset), m_length(length),
      m_write_zeroes(write_zeroes) {
    // use block offset (stdin) or import file position to report
    // progress.
    if (m_idiffctx->fd == STDIN_FILENO) {
      m_prog_offset = offset;
    } else {
      m_prog_offset  = lseek(m_idiffctx->fd, 0, SEEK_CUR);
    }
  }

  int send()
  {
    if (m_idiffctx->throttle.pending_error()) {
      return m_idiffctx->throttle.wait_for_ret();
    }

    C_OrderedThrottle *ctx = m_idiffctx->throttle.start_op(this);
    librbd::RBD::AioCompletion *aio_completion =
      new librbd::RBD::AioCompletion(ctx, &utils::aio_context_callback);

    int r;
    if (m_write_zeroes) {
      r = m_idiffctx->image->aio_write_zeroes(m_offset, m_length,
                                              aio_completion, 0U,
                                              LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
    } else {
      r = m_idiffctx->image->aio_write2(m_offset, m_length, m_data,
                                        aio_completion,
                                        LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
    }

    if (r < 0) {
      aio_completion->release();
      ctx->complete(r);
    }

    return r;
  }

  void finish(int r) override
  {
    m_idiffctx->update_progress(m_prog_offset);
    m_idiffctx->throttle.end_op(r);
  }

private:
  ImportDiffContext *m_idiffctx;
  bufferlist m_data;
  uint64_t m_offset;
  uint64_t m_length;
  bool m_write_zeroes;
  uint64_t m_prog_offset;
};

static int do_image_snap_from(ImportDiffContext *idiffctx)
{
  int r;
  string from;
  r = utils::read_string(idiffctx->fd, 4096, &from);   // 4k limit to make sure we don't get a garbage string
  if (r < 0) {
    std::cerr << "rbd: failed to decode start snap name" << std::endl;
    return r;
  }

  bool exists;
  r = idiffctx->image->snap_exists2(from.c_str(), &exists);
  if (r < 0) {
    std::cerr << "rbd: failed to query start snap state" << std::endl;
    return r;
  }

  if (!exists) {
    std::cerr << "start snapshot '" << from
              << "' does not exist in the image, aborting" << std::endl;
    return -EINVAL;
  }

  idiffctx->update_progress();
  return 0;
}

static int do_image_snap_to(ImportDiffContext *idiffctx, std::string *tosnap)
{
  int r;
  string to;
  r = utils::read_string(idiffctx->fd, 4096, &to);   // 4k limit to make sure we don't get a garbage string
  if (r < 0) {
    std::cerr << "rbd: failed to decode end snap name" << std::endl;
    return r;
  }

  bool exists;
  r = idiffctx->image->snap_exists2(to.c_str(), &exists);
  if (r < 0) {
    std::cerr << "rbd: failed to query end snap state" << std::endl;
    return r;
  }

  if (exists) {
    std::cerr << "end snapshot '" << to << "' already exists, aborting"
              << std::endl;
    return -EEXIST;
  }

  *tosnap = to;
  idiffctx->update_progress();

  return 0;
}

static int get_snap_protection_status(ImportDiffContext *idiffctx,
                                      bool *is_protected)
{
  int r;
  char buf[sizeof(__u8)];
  r = safe_read_exact(idiffctx->fd, buf, sizeof(buf));
  if (r < 0) {
    std::cerr << "rbd: failed to decode snap protection status" << std::endl;
    return r;
  }

  *is_protected = (buf[0] != 0);
  idiffctx->update_progress();

  return 0;
}

static int do_image_resize(ImportDiffContext *idiffctx)
{
  int r;
  char buf[sizeof(uint64_t)];
  uint64_t end_size;
  r = safe_read_exact(idiffctx->fd, buf, sizeof(buf));
  if (r < 0) {
    std::cerr << "rbd: failed to decode image size" << std::endl;
    return r;
  }

  bufferlist bl;
  bl.append(buf, sizeof(buf));
  auto p = bl.cbegin();
  decode(end_size, p);

  uint64_t cur_size;
  idiffctx->image->size(&cur_size);
  if (cur_size != end_size) {
    idiffctx->image->resize(end_size);
  }

  idiffctx->update_size(end_size);
  idiffctx->update_progress();
  return 0;
}

static int do_image_io(ImportDiffContext *idiffctx, bool write_zeroes,
                       size_t sparse_size)
{
  int r;
  char buf[16];
  r = safe_read_exact(idiffctx->fd, buf, sizeof(buf));
  if (r < 0) {
    std::cerr << "rbd: failed to decode IO length" << std::endl;
    return r;
  }

  bufferlist bl;
  bl.append(buf, sizeof(buf));
  auto p = bl.cbegin();

  uint64_t image_offset, buffer_length;
  decode(image_offset, p);
  decode(buffer_length, p);

  if (!write_zeroes) {
    bufferptr bp = buffer::create(buffer_length);
    r = safe_read_exact(idiffctx->fd, bp.c_str(), buffer_length);
    if (r < 0) {
      std::cerr << "rbd: failed to decode write data" << std::endl;
      return r;
    }

    size_t buffer_offset = 0;
    while (buffer_offset < buffer_length) {
      size_t write_length = 0;
      bool zeroed = false;
      utils::calc_sparse_extent(bp, sparse_size, buffer_offset, buffer_length,
				&write_length, &zeroed);
      ceph_assert(write_length > 0);

      bufferlist write_bl;
      if (!zeroed) {
	bufferptr write_ptr(bp, buffer_offset, write_length);
	write_bl.push_back(write_ptr);
	ceph_assert(write_bl.length() == write_length);
      }

      C_ImportDiff *ctx = new C_ImportDiff(idiffctx, write_bl,
					   image_offset + buffer_offset,
					   write_length, zeroed);
      r = ctx->send();
      if (r < 0) {
	return r;
      }

      buffer_offset += write_length;
    }
  } else {
    bufferlist data;
    C_ImportDiff *ctx = new C_ImportDiff(idiffctx, data, image_offset,
					 buffer_length, true);
    return ctx->send();
  }
  return r;
}

static int validate_banner(int fd, std::string banner)
{
  int r;
  char buf[banner.size() + 1];
  memset(buf, 0, sizeof(buf));
  r = safe_read_exact(fd, buf, banner.size());
  if (r < 0) {
    std::cerr << "rbd: failed to decode diff banner" << std::endl;
    return r;
  }

  buf[banner.size()] = '\0';
  if (strcmp(buf, banner.c_str())) {
    std::cerr << "rbd: invalid or unexpected diff banner" << std::endl;
    return -EINVAL;
  }

  return 0;
}

static int skip_tag(int fd, uint64_t length)
{
  int r;

  if (fd == STDIN_FILENO) {
    // read the appending data out to skip this tag.
    char buf[4096];
    uint64_t len = std::min<uint64_t>(length, sizeof(buf));
    while (len > 0) {
      r = safe_read_exact(fd, buf, len);
      if (r < 0) {
        std::cerr << "rbd: failed to decode skipped tag data" << std::endl;
        return r;
      }
      length -= len;
      len = std::min<uint64_t>(length, sizeof(buf));
    }
  } else {
    // lseek to skip this tag
    off64_t offs = lseek64(fd, length, SEEK_CUR);
    if (offs < 0) {
      return -errno;
    }
  }

  return 0;
}

static int read_tag(int fd, __u8 end_tag, int format, __u8 *tag, uint64_t *readlen)
{
  int r;
  __u8 read_tag;

  r = safe_read_exact(fd, &read_tag, sizeof(read_tag));
  if (r < 0) {
    std::cerr << "rbd: failed to decode tag" << std::endl;
    return r;
  }

  *tag = read_tag;
  if (read_tag != end_tag && format == 2) {
    char buf[sizeof(uint64_t)];
    r = safe_read_exact(fd, buf, sizeof(buf));
    if (r < 0) {
      std::cerr << "rbd: failed to decode tag length" << std::endl;
      return r;
    }

    bufferlist bl;
    bl.append(buf, sizeof(buf));
    auto p = bl.cbegin();
    decode(*readlen, p);
  }

  return 0;
}

int do_import_diff_fd(librados::Rados &rados, librbd::Image &image, int fd,
		      bool no_progress, int format, size_t sparse_size)
{
  int r;

  uint64_t size = 0;
  bool from_stdin = (fd == STDIN_FILENO);
  if (!from_stdin) {
    struct stat stat_buf;
    r = ::fstat(fd, &stat_buf);
    if (r < 0) {
      std::cerr << "rbd: failed to stat specified diff file" << std::endl;
      return r;
    }
    size = (uint64_t)stat_buf.st_size;
  }

  r = validate_banner(fd, (format == 1 ? utils::RBD_DIFF_BANNER :
                           utils::RBD_DIFF_BANNER_V2));
  if (r < 0) {
    return r;
  }

  // begin image import
  std::string tosnap;
  bool is_protected = false;
  ImportDiffContext idiffctx(&image, fd, size, no_progress);
  while (r == 0) {
    __u8 tag;
    uint64_t length = 0;

    r = read_tag(fd, RBD_DIFF_END, format, &tag, &length);
    if (r < 0 || tag == RBD_DIFF_END) {
      break;
    }

    if (tag == RBD_DIFF_FROM_SNAP) {
      r = do_image_snap_from(&idiffctx);
    } else if (tag == RBD_DIFF_TO_SNAP) {
      r = do_image_snap_to(&idiffctx, &tosnap);
    } else if (tag == RBD_SNAP_PROTECTION_STATUS) {
      r = get_snap_protection_status(&idiffctx, &is_protected);
    } else if (tag == RBD_DIFF_IMAGE_SIZE) {
      r = do_image_resize(&idiffctx);
    } else if (tag == RBD_DIFF_WRITE || tag == RBD_DIFF_ZERO) {
      r = do_image_io(&idiffctx, (tag == RBD_DIFF_ZERO), sparse_size);
    } else {
      std::cerr << "unrecognized tag byte " << (int)tag << " in stream; skipping"
                << std::endl;
      r = skip_tag(fd, length);
    }
  }

  int temp_r = idiffctx.throttle.wait_for_ret();
  r = (r < 0) ? r : temp_r; // preserve original error
  if (r == 0 && tosnap.length()) {
    r = idiffctx.image->snap_create(tosnap.c_str());
    if (r == 0 && is_protected) {
      r = idiffctx.image->snap_protect(tosnap.c_str());
    }
  }

  idiffctx.finish(r);
  return r;
}

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
  r = do_import_diff_fd(rados, image, fd, no_progress, 1, sparse_size);

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
  {"import-diff"}, {},
  "Apply an incremental diff to image HEAD, then create a snapshot.", "",
  &get_arguments_diff, &execute_diff);

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

static int decode_and_set_image_option(int fd, uint64_t imageopt, librbd::ImageOptions& opts)
{
  int r;
  char buf[sizeof(uint64_t)];

  r = safe_read_exact(fd, buf, sizeof(buf));
  if (r < 0) {
    std::cerr << "rbd: failed to decode image option" << std::endl;
    return r;
  }

  bufferlist bl;
  bl.append(buf, sizeof(buf));
  auto it = bl.cbegin();

  uint64_t val;
  decode(val, it);

  if (opts.get(imageopt, &val) != 0) {
    opts.set(imageopt, val);
  }

  return 0;
}

static int do_import_metadata(int import_format, librbd::Image& image,
                              const std::map<std::string, std::string> &imagemetas)
{
  int r = 0;

  //v1 format
  if (import_format == 1) {
    return 0;
  }

  for (std::map<std::string, std::string>::const_iterator it = imagemetas.begin();
       it != imagemetas.end(); ++it) {
    r = image.metadata_set(it->first, it->second);
    if (r < 0)
      return r;
  }

  return 0;
}

static int decode_imagemeta(int fd, uint64_t length, std::map<std::string, std::string>* imagemetas)
{
  int r;
  string key;
  string value;

  r = utils::read_string(fd, length, &key);
  if (r < 0) {
    std::cerr << "rbd: failed to decode metadata key" << std::endl;
    return r;
  }

  r = utils::read_string(fd, length, &value);
  if (r < 0) {
    std::cerr << "rbd: failed to decode metadata value" << std::endl;
    return r;
  }

  (*imagemetas)[key] = value;
  return 0;
}

static int do_import_header(int fd, int import_format, librbd::ImageOptions& opts,
                            std::map<std::string, std::string>* imagemetas)
{
  // There is no header in v1 image.
  if (import_format == 1) {
    return 0;
  }

  int r;
  r = validate_banner(fd, utils::RBD_IMAGE_BANNER_V2);
  if (r < 0) {
    return r;
  }

  // As V1 format for image is already deprecated, import image in V2 by default.
  uint64_t image_format = 2;
  if (opts.get(RBD_IMAGE_OPTION_FORMAT, &image_format) != 0) {
    opts.set(RBD_IMAGE_OPTION_FORMAT, image_format);
  }

  while (r == 0) {
    __u8 tag;
    uint64_t length = 0;
    r = read_tag(fd, RBD_EXPORT_IMAGE_END, image_format, &tag, &length);
    if (r < 0 || tag == RBD_EXPORT_IMAGE_END) {
      break;
    }

    if (tag == RBD_EXPORT_IMAGE_ORDER) {
      r = decode_and_set_image_option(fd, RBD_IMAGE_OPTION_ORDER, opts);
    } else if (tag == RBD_EXPORT_IMAGE_FEATURES) {
      r = decode_and_set_image_option(fd, RBD_IMAGE_OPTION_FEATURES, opts);
    } else if (tag == RBD_EXPORT_IMAGE_STRIPE_UNIT) {
      r = decode_and_set_image_option(fd, RBD_IMAGE_OPTION_STRIPE_UNIT, opts);
    } else if (tag == RBD_EXPORT_IMAGE_STRIPE_COUNT) {
      r = decode_and_set_image_option(fd, RBD_IMAGE_OPTION_STRIPE_COUNT, opts);
    } else if (tag == RBD_EXPORT_IMAGE_META) {
      r = decode_imagemeta(fd, length, imagemetas);
    } else {
      std::cerr << "rbd: invalid tag in image properties zone: " << tag << "Skip it."
                << std::endl;
      r = skip_tag(fd, length);
    }
  }

  return r;
}

static int do_import_v2(librados::Rados &rados, int fd, librbd::Image &image,
			uint64_t size, size_t imgblklen,
			utils::ProgressContext &pc, size_t sparse_size)
{
  int r = 0;
  r = validate_banner(fd, utils::RBD_IMAGE_DIFFS_BANNER_V2);
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
    r = do_import_diff_fd(rados, image, fd, true, 2, sparse_size);
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

  reqlen = std::min<uint64_t>(reqlen, size);
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
		     const char *path, librbd::ImageOptions& opts, bool skip_create,
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

  if (skip_create && import_format == 1) {
    std::cerr << "rbd: export format 1 does not support skip-create" << std::endl;
    return -EINVAL;
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

  r = do_import_header(fd, import_format, opts, &imagemetas);
  if (r < 0) {
    std::cerr << "rbd: import header failed." << std::endl;
    goto done;
  }

  if (skip_create)
    goto open_rbd;

  r = rbd.create4(io_ctx, imgname, size, opts);
  if (r < 0) {
    std::cerr << "rbd: image creation failed" << std::endl;
    goto done;
  }

open_rbd:
  r = rbd.open(io_ctx, image, imgname);
  if (r < 0) {
    std::cerr << "rbd: failed to open image" << std::endl;
    goto err;
  }

  r = do_import_metadata(import_format, image, imagemetas);
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
  if (r < 0 && !skip_create)
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
  at::add_skip_create_option(options);

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
                opts, vm[at::SKIP_CREATE].as<bool>(), vm[at::NO_PROGRESS].as<bool>(),
                format, sparse_size);
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
