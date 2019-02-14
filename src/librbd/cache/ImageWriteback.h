// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_IMAGE_WRITEBACK
#define CEPH_LIBRBD_CACHE_IMAGE_WRITEBACK

#include "include/buffer_fwd.h"
#include "include/int_types.h"
#include <vector>

class Context;

namespace librbd {

struct ImageCtx;

namespace cache {

/**
 * client-side, image extent cache writeback handler
 */
template <typename ImageCtxT = librbd::ImageCtx>
class ImageWriteback {
public:
  typedef std::vector<std::pair<uint64_t,uint64_t> > Extents;

  explicit ImageWriteback(ImageCtxT &image_ctx);

  void aio_read(Extents &&image_extents, ceph::bufferlist *bl,
                int fadvise_flags, Context *on_finish);
  void aio_write(Extents &&image_extents, ceph::bufferlist&& bl,
                 int fadvise_flags, Context *on_finish);
  void aio_discard(uint64_t offset, uint64_t length,
                   uint32_t discard_granularity_bytes, Context *on_finish);
  void aio_flush(Context *on_finish);
  void aio_writesame(uint64_t offset, uint64_t length,
                     ceph::bufferlist&& bl,
                     int fadvise_flags, Context *on_finish);
  void aio_compare_and_write(Extents &&image_extents,
                             ceph::bufferlist&& cmp_bl,
                             ceph::bufferlist&& bl,
                             uint64_t *mismatch_offset,
                             int fadvise_flags, Context *on_finish);
private:
  ImageCtxT &m_image_ctx;

};

} // namespace cache
} // namespace librbd

extern template class librbd::cache::ImageWriteback<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_IMAGE_WRITEBACK
