// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_WRITE_LOG_CACHE
#define CEPH_LIBRBD_CACHE_WRITE_LOG_CACHE

#include "librbd/cache/ImageCache.h"

namespace librbd {

struct ImageCtx;

namespace cache {

namespace pwl {
template <typename> class AbstractWriteLog;
template <typename> class ImageCacheState;
}

template <typename ImageCtxT>
class WriteLogCache : public ImageCache<ImageCtxT> {
public:
  using typename ImageCache<ImageCtxT>::Extent;
  using typename ImageCache<ImageCtxT>::Extents;

  WriteLogCache(ImageCtxT &image_ctx, librbd::cache::pwl::ImageCacheState<ImageCtxT>* cache_state);
  ~WriteLogCache();
  WriteLogCache(const WriteLogCache&) = delete;
  WriteLogCache &operator=(const WriteLogCache&) = delete;

  /// client AIO methods
  void aio_read(Extents&& image_extents, ceph::bufferlist *bl,
                int fadvise_flags, Context *on_finish) override;
  void aio_write(Extents&& image_extents, ceph::bufferlist&& bl,
                 int fadvise_flags, Context *on_finish) override;
  void aio_discard(uint64_t offset, uint64_t length,
                   uint32_t discard_granularity_bytes,
                   Context *on_finish) override;
  void aio_flush(io::FlushSource flush_source, Context *on_finish) override;
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

  librbd::cache::pwl::AbstractWriteLog<ImageCtxT> *m_write_log;
};

} // namespace cache
} // namespace librbd

extern template class librbd::cache::WriteLogCache<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_WRITE_LOG_CACHE
