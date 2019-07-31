// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_IMAGE_CACHE
#define CEPH_LIBRBD_CACHE_IMAGE_CACHE

#include "include/buffer_fwd.h"
#include "include/int_types.h"
#include <vector>

class Context;

namespace librbd {
namespace cache {

/**
 * client-side, image extent cache interface
 */
struct ImageCache {
  typedef std::vector<std::pair<uint64_t,uint64_t> > Extents;

  virtual ~ImageCache() {
  }

  /// client AIO methods
  virtual void aio_read(Extents&& image_extents, ceph::bufferlist* bl,
                        int fadvise_flags, Context *on_finish) = 0;
  virtual void aio_write(Extents&& image_extents, ceph::bufferlist&& bl,
                         int fadvise_flags, Context *on_finish) = 0;
  virtual void aio_discard(uint64_t offset, uint64_t length,
                           uint32_t discard_granularity_bytes,
                           Context *on_finish) = 0;
  virtual void aio_flush(Context *on_finish) = 0;
  virtual void aio_writesame(uint64_t offset, uint64_t length,
                             ceph::bufferlist&& bl,
                             int fadvise_flags, Context *on_finish) = 0;
  virtual void aio_compare_and_write(Extents&& image_extents,
                                     ceph::bufferlist&& cmp_bl,
                                     ceph::bufferlist&& bl,
                                     uint64_t *mismatch_offset,
                                     int fadvise_flags,
                                     Context *on_finish) = 0;

  /// internal state methods
  virtual void init(Context *on_finish) = 0;
  virtual void shut_down(Context *on_finish) = 0;

  virtual void invalidate(Context *on_finish) = 0;
  virtual void flush(Context *on_finish) = 0;

};

} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_IMAGE_CACHE
