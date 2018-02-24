// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_IMAGE_WRITEBACK
#define CEPH_LIBRBD_CACHE_IMAGE_WRITEBACK

#include "include/buffer_fwd.h"
#include "include/int_types.h"
#include "include/Context.h"
#include "librbd/cache/ImageCache.h"
#include <vector>

class Context;

namespace librbd {

struct ImageCtx;

namespace cache {

/**
 * client-side, image extent cache writeback handler
 */
template <typename ImageCtxT = librbd::ImageCtx>
class ImageWriteback : public ImageCache<ImageCtxT> {
public:
  using typename ImageCache<ImageCtxT>::Extent;
  using typename ImageCache<ImageCtxT>::Extents;

  ImageWriteback(ImageCtxT &image_ctx);
  ~ImageWriteback();

  void aio_read(Extents &&image_extents, ceph::bufferlist *bl,
                int fadvise_flags, Context *on_finish);
  void aio_write(Extents &&image_extents, ceph::bufferlist&& bl,
                 int fadvise_flags, Context *on_finish);
  void aio_discard(uint64_t offset, uint64_t length,
                   bool skip_partial_discard, Context *on_finish);
  void aio_flush(Context *on_finish);
  void aio_writesame(uint64_t offset, uint64_t length,
                     ceph::bufferlist&& bl,
                     int fadvise_flags, Context *on_finish);
  void aio_compare_and_write(Extents &&image_extents,
                             ceph::bufferlist&& cmp_bl,
                             ceph::bufferlist&& bl,
                             uint64_t *mismatch_offset,
                             int fadvise_flags, Context *on_finish);

  /* ImageWriteback has no internal ImageCache state */
  void init(Context *on_finish) override {on_finish->complete(0);};
  void shut_down(Context *on_finish) override {on_finish->complete(0);};

  void invalidate(Context *on_finish) override {on_finish->complete(0);};
  void flush(Context *on_finish) override {on_finish->complete(0);};
private:
  ImageCtxT &m_image_ctx;
};

} // namespace cache
} // namespace librbd

extern template class librbd::cache::ImageWriteback<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_IMAGE_WRITEBACK
