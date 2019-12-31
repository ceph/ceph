// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG
#define CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG

#include "common/RWLock.h"
#include "common/WorkQueue.h"
#include "common/AsyncOpTracker.h"
#include "librbd/cache/ImageCache.h"
#include "librbd/cache/ImageWriteback.h"
#include "librbd/Utils.h"
#include "librbd/BlockGuard.h"
#include "librbd/cache/Types.h"
#include <functional>
#include <list>

class Context;
class SafeTimer;

namespace librbd {

struct ImageCtx;

namespace cache {

namespace rwl {

class GenericLogEntry;
typedef std::list<std::shared_ptr<GenericLogEntry>> GenericLogEntries;

class DeferredContexts;
template <typename> class ImageCacheState;
} // namespace rwl


template <typename ImageCtxT>
class ReplicatedWriteLog : public ImageCache<ImageCtxT> {
public:
  using typename ImageCache<ImageCtxT>::Extents;

  ReplicatedWriteLog(ImageCtxT &image_ctx, librbd::cache::rwl::ImageCacheState<ImageCtxT>* cache_state);
  ~ReplicatedWriteLog();
  ReplicatedWriteLog(const ReplicatedWriteLog&) = delete;
  ReplicatedWriteLog &operator=(const ReplicatedWriteLog&) = delete;

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
  void invalidate(Context *on_finish);
  void flush(Context *on_finish) override;

private:
  librbd::cache::rwl::ImageCacheState<ImageCtxT>* m_cache_state = nullptr;

  std::atomic<bool> m_initialized = {false};
  PMEMobjpool *m_log_pool = nullptr;
  const char* m_rwl_pool_layout_name;

  ImageCtxT &m_image_ctx;

  std::string m_log_pool_name;
  bool m_log_is_poolset = false;
  uint64_t m_log_pool_config_size; /* Configured size of RWL */
  uint64_t m_log_pool_actual_size = 0; /* Actual size of RWL pool */

  uint32_t m_total_log_entries = 0;
  uint32_t m_free_log_entries = 0;

  std::atomic<uint64_t> m_bytes_allocated = {0}; /* Total bytes allocated in write buffers */
  uint64_t m_bytes_cached = 0;    /* Total bytes used in write buffers */
  uint64_t m_bytes_dirty = 0;     /* Total bytes yet to flush to RBD */
  uint64_t m_bytes_allocated_cap = 0;

  ImageWriteback<ImageCtxT> m_image_writeback;

  /*
   * When m_first_free_entry == m_first_valid_entry, the log is
   * empty. There is always at least one free entry, which can't be
   * used.
   */
  uint64_t m_first_free_entry = 0;  /* Entries from here to m_first_valid_entry-1 are free */
  uint64_t m_first_valid_entry = 0; /* Entries from here to m_first_free_entry-1 are valid */

  /* Starts at 0 for a new write log. Incremented on every flush. */
  uint64_t m_current_sync_gen = 0;
  /* Starts at 0 on each sync gen increase. Incremented before applied
     to an operation */
  uint64_t m_last_op_sequence_num = 0;
  /* All writes bearing this and all prior sync gen numbers are flushed */
  uint64_t m_flushed_sync_gen = 0;

  /* Acquire locks in order declared here */

  /* Used for most synchronization */
  mutable ceph::mutex m_lock;

  librbd::cache::Contexts m_flush_complete_contexts;

  /* New entries are at the back. Oldest at the front */
  rwl::GenericLogEntries m_log_entries;
  rwl::GenericLogEntries m_dirty_log_entries;

  PerfCounters *m_perfcounter = nullptr;

  /* Initialized from config, then set false during shutdown */
  std::atomic<bool> m_periodic_stats_enabled = {false};
  SafeTimer *m_timer = nullptr; /* Used with m_timer_lock */
  mutable ceph::mutex *m_timer_lock = nullptr; /* Used with and by m_timer */
  Context *m_timer_ctx = nullptr;

  ThreadPool m_thread_pool;
  ContextWQ m_work_queue;

  void perf_start(const std::string name);
  void perf_stop();
  void log_perf();
  void periodic_stats();
  void arm_periodic_stats();

  void rwl_init(Context *on_finish, rwl::DeferredContexts &later);
  void update_image_cache_state(Context *on_finish);
};

} // namespace cache
} // namespace librbd

extern template class librbd::cache::ReplicatedWriteLog<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG
