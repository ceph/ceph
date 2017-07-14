// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_FILE_IMAGE_CACHE
#define CEPH_LIBRBD_CACHE_FILE_IMAGE_CACHE

#include "librbd/cache/ImageCache.h"
#include "librbd/Utils.h"
#include "librbd/cache/BlockGuard.h"
#include "librbd/cache/ImageWriteback.h"
#include "librbd/cache/file/Policy.h"
#include "common/WorkQueue.h"
#include <functional>
#include <list>

namespace librbd {

struct ImageCtx;

namespace cache {

namespace file {

template <typename> class ImageStore;
template <typename> class MetaStore;

} // namespace file

/**
 * Prototype file-based, client-side, image extent cache
 */
template <typename ImageCtxT = librbd::ImageCtx>
class FileImageCache : public ImageCache {
public:
  FileImageCache(ImageCtx &image_ctx);
  ~FileImageCache();

  // client AIO methods
  void aio_read(Extents&& image_extents, ceph::bufferlist *bl,
                int fadvise_flags, Context *on_finish) override;
  void aio_write(Extents&& image_extents, ceph::bufferlist&& bl,
                 int fadvise_flags, Context *on_finish) override;
  void aio_discard(uint64_t offset, uint64_t length,
                   bool skip_partial_discard, Context *on_finish);
  void aio_flush(Context *on_finish) override;
  void aio_writesame(uint64_t offset, uint64_t length,
                     ceph::bufferlist&& bl,
                     int fadvise_flags, Context *on_finish) override;
  void aio_compare_and_write(Extents&& image_extents,
                             ceph::bufferlist&& cmp_bl, ceph::bufferlist&& bl,
                             uint64_t *mismatch_offset,int fadvise_flags,
                             Context *on_finish) override;

  // internal state methods
  void init(Context *on_finish) override;
  void remove(Context *on_finish) override;
  void shut_down(Context *on_finish) override;

  void invalidate(Context *on_finish) override;
  void flush(Context *on_finish) override;
  void load_snap_as_base(Context *on_finish);
  void load_meta_to_policy();
  void set_parent();

  ImageWriteback<ImageCtxT> m_image_writeback;
  ImageWriteback<ImageCtxT> m_parent_snap_image_writeback;
  file::Policy *m_policy = nullptr;
  file::MetaStore<ImageCtx> *m_meta_store = nullptr;
  file::ImageStore<ImageCtx> *m_image_store = nullptr;
  file::ImageStore<ImageCtx> *m_parent_image_store = nullptr;
  ContextWQ* pcache_op_work_queue;

private:
  typedef std::function<void(uint64_t)> ReleaseBlock;
  typedef std::list<Context *> Contexts;

  ImageCtxT &m_image_ctx;
  BlockGuard m_block_guard;

  util::AsyncOpTracker m_async_op_tracker;

  mutable Mutex m_lock;
  bool m_wake_up_scheduled = false;

  Contexts m_post_work_contexts;

  void map_blocks(IOType io_type, Extents &&image_extents,
                  BlockGuard::C_BlockRequest *block_request);
  void map_block(BlockGuard::BlockIO &&block_io);
  void invalidate(Extents&& image_extents, Context *on_finish);
  bool if_cloned_volume;

};

} // namespace cache
} // namespace librbd

extern template class librbd::cache::FileImageCache<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_FILE_IMAGE_CACHE
