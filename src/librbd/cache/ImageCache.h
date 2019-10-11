// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_IMAGE_CACHE
#define CEPH_LIBRBD_CACHE_IMAGE_CACHE

#include "include/buffer_fwd.h"
#include "include/int_types.h"
#include "librbd/io/Types.h"
#include <vector>

class Context;
class JSONFormattable;

namespace librbd {
struct ImageCtx;
namespace cache {

enum ImageCacheType {
  IMAGE_CACHE_TYPE_RWL = 1,
};

template <typename ImageCtxT = ImageCtx>
class ImageCacheState {
protected:
  ImageCtxT* m_image_ctx;

  ImageCacheState(ImageCtxT* image_ctx): m_image_ctx(image_ctx) {}

  ImageCacheState(ImageCtxT* image_ctx, JSONFormattable& f);

public:
  bool m_present = true;
  bool m_empty = true;
  bool m_clean = true;
  int m_cache_type;

  virtual ~ImageCacheState() {}

  void write_image_cache_state(Context *on_finish);

  void clear_image_cache_state(Context *on_finish);

  virtual void dump(Formatter *f) const;
  virtual bool is_valid() {
    return true;
  }
};

/**
 * client-side, image extent cache interface
 */
template <typename ImageCtxT = ImageCtx>
class ImageCache {
protected:
  ImageCache() {}
  ImageCache(ImageCacheState<ImageCtxT>* cache_state): m_cache_state(cache_state) {}
  ImageCacheState<ImageCtxT>* m_cache_state = nullptr;
public:
  typedef io::Extent Extent;
  typedef io::Extents Extents;

  virtual ~ImageCache() {}

  /// client AIO methods
  virtual void aio_read(Extents&& image_extents, ceph::bufferlist* bl,
                        int fadvise_flags, Context *on_finish) = 0;
  virtual void aio_write(Extents&& image_extents, ceph::bufferlist&& bl,
                         int fadvise_flags, Context *on_finish) = 0;
  virtual void aio_discard(uint64_t offset, uint64_t length,
                           uint32_t discard_granularity_bytes,
                           Context *on_finish) = 0;
  virtual void aio_flush(io::FlushSource flush_source, Context *on_finish) = 0;
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

  virtual void invalidate(bool discard_unflushed_writes, Context *on_finish) = 0;
  virtual void flush(Context *on_finish) = 0;
  void clear_image_cache_state(Context *on_finish);
};

} // namespace cache
} // namespace librbd

extern template class librbd::cache::ImageCache<librbd::ImageCtx>;
extern template class librbd::cache::ImageCacheState<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_IMAGE_CACHE
