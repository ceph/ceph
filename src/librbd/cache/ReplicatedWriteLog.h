// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG
#define CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG

//#if defined(HAVE_PMEM)
//#include <libpmem.h>
#include <libpmemobj.h>
//#endif
#include "librbd/cache/ImageCache.h"
#include "librbd/cache/FileImageCache.h"
#include "librbd/Utils.h"
#include "librbd/cache/BlockGuard.h"
#include "librbd/cache/ImageWriteback.h"
#include "librbd/cache/file/Policy.h"
#include <functional>
#include <list>

namespace librbd {

struct ImageCtx;

namespace cache {

namespace file {

template <typename> class ImageStore;
template <typename> class JournalStore;
template <typename> class MetaStore;

} // namespace file

/**
 * Prototype pmem-based, client-side, replicated write log
 */
template <typename ImageCtxT = librbd::ImageCtx>
class ReplicatedWriteLog : public ImageCache {
public:
  //using typename FileImageCache<ImageCtxT>::Extents;
  ReplicatedWriteLog(ImageCtx &image_ctx);
  ~ReplicatedWriteLog();

  /// client AIO methods
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

  /// internal state methods
  void init(Context *on_finish) override;
  void shut_down(Context *on_finish) override;

  void invalidate(Context *on_finish) override;
  void flush(Context *on_finish) override;

private:
  typedef std::function<void(uint64_t)> ReleaseBlock;
  typedef std::function<void(BlockGuard::BlockIO)> AppendDetainedBlock;
  typedef std::list<Context *> Contexts;

  ImageCtxT &m_image_ctx;

  std::string m_log_pool_name;
  PMEMobjpool *m_log_pool;
  uint64_t m_log_pool_size;
  
  uint64_t m_free_log_entries;
  uint64_t m_free_blocks;

  ImageWriteback<ImageCtxT> m_image_writeback;
  FileImageCache<ImageCtxT> m_image_cache;
  //BlockGuard m_persist_pending_guard;
  BlockGuard m_block_guard;

  file::Policy *m_policy = nullptr;

  ReleaseBlock m_release_block;
  AppendDetainedBlock m_detain_block;

  util::AsyncOpTracker m_async_op_tracker;

  mutable Mutex m_lock;
  BlockGuard::BlockIOs m_deferred_block_ios;
  BlockGuard::BlockIOs m_detained_block_ios;

  bool m_wake_up_scheduled = false;

  Contexts m_post_work_contexts;

  void map_blocks(IOType io_type, Extents &&image_extents,
                  BlockGuard::C_BlockRequest *block_request);
  void map_block(bool detain_block, BlockGuard::BlockIO &&block_io);
  void release_block(uint64_t block);
  void append_detain_block(BlockGuard::BlockIO &block_io);

  void wake_up();
  void process_work();

  bool is_work_available() const;
  void process_writeback_dirty_blocks();
  void process_detained_block_ios();
  void process_deferred_block_ios();

  void invalidate(Extents&& image_extents, Context *on_finish);

};

} // namespace cache
} // namespace librbd

extern template class librbd::cache::ReplicatedWriteLog<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG
