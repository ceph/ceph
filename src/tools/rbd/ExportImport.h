// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_EXPORTIMPORT_H
#define CEPH_RBD_EXPORTIMPORT_H

#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include "common/Throttle.h"

namespace rbd {
namespace utils {

const uint32_t MAX_KEYS = 64;

struct ExportDiffContext {
  librbd::Image *image;
  int fd;
  int export_format;
  uint64_t totalsize;
  utils::ProgressContext pc;
  OrderedThrottle throttle;

  ExportDiffContext(librbd::Image *i, int f, uint64_t t, int max_ops,
                    bool no_progress, int eformat) :
    image(i), fd(f), export_format(eformat), totalsize(t),
    pc("Exporting image", no_progress), throttle(max_ops, true) {
  }
};

class C_ExportDiff : public Context {
public:
  C_ExportDiff(ExportDiffContext *edc, uint64_t offset, uint64_t length,
               bool exists, int export_format)
    : m_export_diff_context(edc), m_offset(offset), m_length(length),
      m_exists(exists), m_export_format(export_format) {
  }

  int send();

  static int export_diff_cb(uint64_t offset, size_t length, int exists,
			    void *arg);

protected:
  void finish(int r) override;

private:
  ExportDiffContext *m_export_diff_context;
  uint64_t m_offset;
  uint64_t m_length;
  bool m_exists;
  int m_export_format;
  bufferlist m_read_data;

  static int write_extent(ExportDiffContext *edc, uint64_t offset,
                          uint64_t length, bool exists, int export_format);
};

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

  int send();

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

int open_export_file(const char *path, int &fd, bool &to_stdout);
int image_export_metadata(librbd::Image& image, librbd::image_info_t &info, int fd);
int image_export_diff_header(librbd::Image& image, librbd::image_info_t &info,
                            const char *fromsnapname, const char *endsnapname,
                            int fd, const int export_format);
int read_tag(int fd, __u8 end_tag, int format, __u8 *tag, uint64_t *readlen);
int skip_tag(int fd, uint64_t length);
int validate_banner(int fd, std::string banner);
int do_import_header(int fd, int import_format, librbd::ImageOptions& opts,
                            std::map<std::string, std::string>* imagemetas);
int do_import_diff_fd(librados::Rados &rados, librbd::Image &image, int fd,
          bool no_progress, int format, size_t sparse_size);
int do_import_metadata(int import_format, librbd::Image& image,
                      const std::map<std::string, std::string> &imagemetas);

} //namespace utils
} //namespace rbd

#endif //CEPH_RBD_EXPORTIMPORT_H