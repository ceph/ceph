// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_FILE_AIO_FILE
#define CEPH_LIBRBD_CACHE_FILE_AIO_FILE

#include "include/buffer_fwd.h"
#include <string>

struct Context;
struct ContextWQ;

namespace librbd {

struct ImageCtx;

namespace cache {
namespace file {

template <typename ImageCtxT>
class AioFile {
public:
  AioFile(ImageCtxT &image_ctx, ContextWQ &work_queue, const std::string &name);
  ~AioFile();

  // TODO use IO queue instead of individual commands so operations can be
  // submitted in batch

  // TODO use scatter/gather API

  void open(Context *on_finish);
  void close(Context *on_finish);

  void read(uint64_t offset, uint64_t length, ceph::bufferlist *bl,
            Context *on_finish);
  void write(uint64_t offset, ceph::bufferlist &&bl, bool fdatasync,
             Context *on_finish);
  void discard(uint64_t offset, uint64_t length,
               bool fdatasync, Context *on_finish);
  void truncate(uint64_t length, bool fdatasync, Context *on_finish);

  void fsync(Context *on_finish);
  void fdatasync(Context *on_finish);

private:
  ImageCtxT &m_image_ctx;
  ContextWQ &m_work_queue;
  std::string m_name;
  int m_fd = -1;

  int write(uint64_t offset, const ceph::bufferlist &bl, bool fdatasync);
  int read(uint64_t offset, uint64_t length, ceph::bufferlist *bl);
  int discard(uint64_t offset, uint64_t length, bool fdatasync);
  int truncate(uint64_t length, bool fdatasync);
  int fdatasync();
};

} // namespace file
} // namespace cache
} // namespace librbd

extern template class librbd::cache::file::AioFile<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_FILE_AIO_FILE
