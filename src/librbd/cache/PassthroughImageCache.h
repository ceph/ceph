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
  PassthroughImageCache(ImageCtx &image_ctx);

  /// client AIO methods
  virtual void aio_read(Extents&& image_extents, ceph::bufferlist *bl,
                        int fadvise_flags, Context *on_finish);
  virtual void aio_write(Extents&& image_extents, ceph::bufferlist&& bl,
                         int fadvise_flags, Context *on_finish);
  virtual void aio_discard(uint64_t offset, uint64_t length,
                           Context *on_finish);
  virtual void aio_flush(Context *on_finish);

  /// internal state methods
  virtual void init(Context *on_finish);
  virtual void shut_down(Context *on_finish);

  virtual void invalidate(Context *on_finish);
  virtual void flush(Context *on_finish);

private:
  ImageCtxT &m_image_ctx;
  ImageWriteback<ImageCtxT> m_image_writeback;

};

} // namespace cache
} // namespace librbd

extern template class librbd::cache::PassthroughImageCache<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_PASSTHROUGH_IMAGE_CACHE
