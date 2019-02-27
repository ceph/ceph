// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_PASSTHROUGH_IMAGE_CACHE
#define CEPH_LIBRBD_CACHE_PASSTHROUGH_IMAGE_CACHE

#include "ImageCache.h"
#include "ImageWriteback.h"

namespace librbd {

struct ImageCtx;

namespace cache {

/**
 * Example passthrough client-side, image extent cache
 */
template <typename ImageCtxT = librbd::ImageCtx>
class PassthroughImageCache : public ImageCache {
public:
  explicit PassthroughImageCache(ImageCtx &image_ctx);

  /// client AIO methods
  void aio_read(Extents&& image_extents, ceph::bufferlist *bl,
                int fadvise_flags, Context *on_finish) override;
  void aio_write(Extents&& image_extents, ceph::bufferlist&& bl,
                 int fadvise_flags, Context *on_finish) override;
  void aio_discard(uint64_t offset, uint64_t length,
                   uint32_t discard_granularity_bytes,
                   Context *on_finish) override;
  void aio_flush(Context *on_finish) override;
  void aio_writesame(uint64_t offset, uint64_t length,
                     ceph::bufferlist&& bl,
                     int fadvise_flags, Context *on_finish) override;
  void aio_compare_and_write(Extents&& image_extents,
                             ceph::bufferlist&& cmp_bl, ceph::bufferlist&& bl,
                             uint64_t *mismatch_offset,int fadvise_flags,
                             Context *on_finish) override;

  /// internal state methods
  void init(Context *on_finish) override;
  void shut_down(Context *on_finish) override;

  void invalidate(Context *on_finish) override;
  void flush(Context *on_finish) override;

private:
  ImageCtxT &m_image_ctx;
  ImageWriteback<ImageCtxT> m_image_writeback;

};

} // namespace cache
} // namespace librbd

extern template class librbd::cache::PassthroughImageCache<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_PASSTHROUGH_IMAGE_CACHE
