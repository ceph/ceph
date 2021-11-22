// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include "common/errno.h"
#include "tools/rbd/Utils.h"
#include "tools/rbd/ExportImport.h"

namespace rbd {
namespace utils {

int C_ExportDiff::send()
{
  if (m_export_diff_context->throttle.pending_error()) {
    return m_export_diff_context->throttle.wait_for_ret();
  }

  C_OrderedThrottle *ctx = m_export_diff_context->throttle.start_op(this);
  if (m_exists) {
    librbd::RBD::AioCompletion *aio_completion =
      new librbd::RBD::AioCompletion(ctx, &utils::aio_context_callback);

    int op_flags = LIBRADOS_OP_FLAG_FADVISE_NOCACHE;
    int r = m_export_diff_context->image->aio_read2(
      m_offset, m_length, m_read_data, aio_completion, op_flags);
    if (r < 0) {
      aio_completion->release();
      ctx->complete(r);
    }
  } else {
    ctx->complete(0);
  }
  return 0;
}

int C_ExportDiff::export_diff_cb(uint64_t offset, size_t length, int exists,
			    void *arg)
{
  ExportDiffContext *edc = reinterpret_cast<ExportDiffContext *>(arg);

  C_ExportDiff *context = new C_ExportDiff(edc, offset, length,
                                          exists, edc->export_format);
  return context->send();
}

void C_ExportDiff::finish(int r)
{
  if (r >= 0) {
    if (m_exists) {
      m_exists = !m_read_data.is_zero();
    }
    r = write_extent(m_export_diff_context, m_offset, m_length,
                    m_exists, m_export_format);
    if (r == 0 && m_exists) {
      r = m_read_data.write_fd(m_export_diff_context->fd);
    }
  }
  m_export_diff_context->throttle.end_op(r);
}

int C_ExportDiff::write_extent(ExportDiffContext *edc, uint64_t offset,
                          uint64_t length, bool exists, int export_format)
{
  // extent
  bufferlist bl;
  __u8 tag = exists ? RBD_DIFF_WRITE : RBD_DIFF_ZERO;
  uint64_t len = 0;
  encode(tag, bl);
  if (export_format == 2) {
    if (tag == RBD_DIFF_WRITE)
      len = 8 + 8 + length;
    else
      len = 8 + 8;
    encode(len, bl);
  }
  encode(offset, bl);
  encode(length, bl);
  int r = bl.write_fd(edc->fd);

  edc->pc.update_progress(offset, edc->totalsize);
  return r;
}

int C_ImportDiff::send()
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

int open_export_file(const char *path, int &fd, bool &to_stdout)
{
  to_stdout = (strcmp(path, "-") == 0);

  if (to_stdout) {
    fd = STDOUT_FILENO;
  } else {
    fd = open(path, O_WRONLY | O_CREAT | O_EXCL | O_BINARY, 0644);
    if (fd < 0) {
      std::cerr << "Open file err:" << cpp_strerror(errno) << std::endl;
      return -errno;
    }
#ifdef HAVE_POSIX_FADVISE
    posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif
  }

  return 0;
}

int image_export_metadata(librbd::Image& image, librbd::image_info_t &info, int fd)
{
  int r = 0;
  // header
  bufferlist bl;
  bl.append(RBD_IMAGE_BANNER_V2);

  __u8 tag;
  uint64_t length;
  // encode order
  tag = RBD_EXPORT_IMAGE_ORDER;
  length = 8;
  encode(tag, bl);
  encode(length, bl);
  encode(uint64_t(info.order), bl);

  // encode features
  tag = RBD_EXPORT_IMAGE_FEATURES;
  uint64_t features;
  image.features(&features);
  length = 8;
  encode(tag, bl);
  encode(length, bl);
  encode(features, bl);

  // encode stripe_unit and stripe_count
  tag = RBD_EXPORT_IMAGE_STRIPE_UNIT;
  uint64_t stripe_unit;
  stripe_unit = image.get_stripe_unit();
  length = 8;
  encode(tag, bl);
  encode(length, bl);
  encode(stripe_unit, bl);

  tag = RBD_EXPORT_IMAGE_STRIPE_COUNT;
  uint64_t stripe_count;
  stripe_count = image.get_stripe_count();
  length = 8;
  encode(tag, bl);
  encode(length, bl);
  encode(stripe_count, bl);

  //retrieve metadata of image
  std::map<std::string, std::string> imagemetas;
  std::string last_key;
  bool more_results = true;
  while (more_results) {
    std::map<std::string, bufferlist> pairs;
    r = image.metadata_list(last_key, MAX_KEYS, &pairs);
    if (r < 0) {
      std::cerr << "failed to retrieve metadata of image : " << cpp_strerror(r)
                << std::endl;
      return r;
    }

    if (!pairs.empty()) {
      last_key = pairs.rbegin()->first;

      for (auto kv : pairs) {
        std::string key = kv.first;
        std::string val(kv.second.c_str(), kv.second.length());
        imagemetas[key] = val;
      }
    }
    more_results = (pairs.size() == MAX_KEYS);
  }

  //encode imageMeta key and value
  for (std::map<std::string, std::string>::iterator it = imagemetas.begin();
       it != imagemetas.end(); ++it) {
    std::string key = it->first;
    std::string value = it->second;

    tag = RBD_EXPORT_IMAGE_META;
    length = key.length() + value.length() + 4 * 2;
    encode(tag, bl);
    encode(length, bl);
    encode(key, bl);
    encode(value, bl);
  }

  // encode end tag
  tag = RBD_EXPORT_IMAGE_END;
  encode(tag, bl);

  // write bl to fd.
  r = bl.write_fd(fd);
  if (r < 0) {
    return r;
  }

  return r;
}

int image_export_diff_header(librbd::Image& image, librbd::image_info_t &info,
                            const char *fromsnapname, const char *endsnapname,
                            int fd, const int export_format)
{
  int r = 0;
  // header
  bufferlist bl;
  if (export_format == 1)
    bl.append(utils::RBD_DIFF_BANNER);
  else
    bl.append(utils::RBD_DIFF_BANNER_V2);

  __u8 tag;
  uint64_t len = 0;
  if (fromsnapname) {
    tag = RBD_DIFF_FROM_SNAP;
    encode(tag, bl);
    std::string from(fromsnapname);
    if (export_format == 2) {
      len = from.length() + 4;
      encode(len, bl);
    }
    encode(from, bl);
  }

  if (endsnapname) {
    tag = RBD_DIFF_TO_SNAP;
    encode(tag, bl);
    std::string to(endsnapname);
    if (export_format == 2) {
      len = to.length() + 4;
      encode(len, bl);
    }
    encode(to, bl);
  }

  if (endsnapname && export_format == 2) {
    tag = RBD_SNAP_PROTECTION_STATUS;
    encode(tag, bl);
    bool is_protected = false;
    r = image.snap_is_protected(endsnapname, &is_protected);
    if (r < 0) {
      return r;
    }
    len = 1;
    encode(len, bl);
    encode(is_protected, bl);
  }

  tag = RBD_DIFF_IMAGE_SIZE;
  encode(tag, bl);
  uint64_t endsize = info.size;
  if (export_format == 2) {
    len = 8;
    encode(len, bl);
  }
  encode(endsize, bl);

  r = bl.write_fd(fd);
  if (r < 0) {
    return r;
  }

  return r;
}

int read_tag(int fd, __u8 end_tag, int format, __u8 *tag, uint64_t *readlen)
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

int skip_tag(int fd, uint64_t length)
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

int validate_banner(int fd, std::string banner)
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

static int decode_and_set_image_option(int fd, uint64_t imageopt,
                                      librbd::ImageOptions& opts)
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

static int decode_imagemeta(int fd, uint64_t length,
                            std::map<std::string, std::string>* imagemetas)
{
  int r;
  std::string key;
  std::string value;

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

int do_import_header(int fd, int import_format, librbd::ImageOptions& opts,
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

static int do_image_snap_from(utils::ImportDiffContext *idiffctx)
{
  int r;
  std::string from;
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

static int do_image_snap_to(utils::ImportDiffContext *idiffctx, std::string *tosnap)
{
  int r;
  std::string to;
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

static int get_snap_protection_status(utils::ImportDiffContext *idiffctx,
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

static int do_image_resize(utils::ImportDiffContext *idiffctx)
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

  r = validate_banner(fd, (format == 1 ? RBD_DIFF_BANNER :
                           RBD_DIFF_BANNER_V2));
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

    r = utils::read_tag(fd, RBD_DIFF_END, format, &tag, &length);
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
      r = utils::skip_tag(fd, length);
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

int do_import_metadata(int import_format, librbd::Image& image,
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

} // namespace utils
} // namespace rbd