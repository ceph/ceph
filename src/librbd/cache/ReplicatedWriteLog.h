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
#include "librbd/cache/rwl/LogOperation.h"
#include "librbd/cache/rwl/Request.h"
#include "librbd/cache/rwl/LogMap.h"
#include <functional>
#include <list>

class Context;
class SafeTimer;

namespace librbd {

struct ImageCtx;

namespace cache {

namespace rwl {

class SyncPointLogEntry;
class GenericWriteLogEntry;
class WriteLogEntry;
class GenericLogEntry;

typedef std::list<std::shared_ptr<WriteLogEntry>> WriteLogEntries;
typedef std::list<std::shared_ptr<GenericLogEntry>> GenericLogEntries;
typedef std::list<std::shared_ptr<GenericWriteLogEntry>> GenericWriteLogEntries;
typedef std::vector<std::shared_ptr<GenericLogEntry>> GenericLogEntriesVector;

typedef LogMapEntries<GenericWriteLogEntry> WriteLogMapEntries;
typedef LogMap<GenericWriteLogEntry> WriteLogMap;

/**** Write log entries end ****/

typedef librbd::BlockGuard<GuardedRequest> WriteLogGuard;

class DeferredContexts;
template <typename> class ImageCacheState;

template <typename T>
struct C_BlockIORequest;

template <typename T>
struct C_WriteRequest;

using GenericLogOperations = std::list<GenericLogOperationSharedPtr>;

} // namespace rwl


template <typename ImageCtxT>
class ReplicatedWriteLog : public ImageCache<ImageCtxT> {
public:
  using typename ImageCache<ImageCtxT>::Extent;
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

  using This = ReplicatedWriteLog<ImageCtxT>;
  using C_WriteRequestT = rwl::C_WriteRequest<This>;
  using C_BlockIORequestT = rwl::C_BlockIORequest<This>;
  using C_FlushRequestT = rwl::C_FlushRequest<This>;
  using C_DiscardRequestT = rwl::C_DiscardRequest<This>;
  using C_WriteSameRequestT = rwl::C_WriteSameRequest<This>;
  using C_CompAndWriteRequestT = rwl::C_CompAndWriteRequest<This>;

  CephContext * get_context();
  void release_guarded_request(BlockGuardCell *cell);
  void release_write_lanes(C_BlockIORequestT *req);
  bool alloc_resources(C_BlockIORequestT *req);
  template <typename V>
  void flush_pmem_buffer(V& ops);
  void schedule_append(rwl::GenericLogOperationsVector &ops);
  void schedule_append(rwl::GenericLogOperationSharedPtr op);
  void schedule_flush_and_append(rwl::GenericLogOperationsVector &ops);
  void flush_new_sync_point(C_FlushRequestT *flush_req, rwl::DeferredContexts &later);
  std::shared_ptr<rwl::SyncPoint> get_current_sync_point() {
    return m_current_sync_point;
  }
  bool get_persist_on_flush() {
    return m_persist_on_flush;
  }
  void inc_last_op_sequence_num() {
    m_perfcounter->inc(l_librbd_rwl_log_ops, 1);
    ++m_last_op_sequence_num;
  }
  uint64_t get_last_op_sequence_num() {
    return m_last_op_sequence_num;
  }
  uint64_t get_current_sync_gen() {
    return m_current_sync_gen;
  }
  unsigned int get_free_lanes() {
    return m_free_lanes;
  }
  uint32_t get_free_log_entries() {
    return m_free_log_entries;
  }
  void add_into_log_map(rwl::GenericWriteLogEntries &log_entries);
private:
  typedef std::list<rwl::C_WriteRequest<This> *> C_WriteRequests;
  typedef std::list<rwl::C_BlockIORequest<This> *> C_BlockIORequests;

  BlockGuardCell* detain_guarded_request_helper(rwl::GuardedRequest &req);
  BlockGuardCell* detain_guarded_request_barrier_helper(rwl::GuardedRequest &req);
  void detain_guarded_request(C_BlockIORequestT *request,
                              rwl::GuardedRequestFunctionContext *guarded_ctx,
                              bool is_barrier);

  librbd::cache::rwl::ImageCacheState<ImageCtxT>* m_cache_state = nullptr;

  std::atomic<bool> m_initialized = {false};
  std::atomic<bool> m_shutting_down = {false};
  std::atomic<bool> m_invalidating = {false};
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

  utime_t m_last_alloc_fail;      /* Entry or buffer allocation fail seen */
  std::atomic<bool> m_alloc_failed_since_retire = {false};

  ImageWriteback<ImageCtxT> m_image_writeback;
  rwl::WriteLogGuard m_write_log_guard;
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

  bool m_persist_on_write_until_flush = true;

  AsyncOpTracker m_async_op_tracker;
  /* Debug counters for the places m_async_op_tracker is used */
  std::atomic<int> m_async_flush_ops = {0};
  std::atomic<int> m_async_append_ops = {0};
  std::atomic<int> m_async_complete_ops = {0};
  std::atomic<int> m_async_null_flush_finish = {0};
  std::atomic<int> m_async_process_work = {0};

  /* Acquire locks in order declared here */

  mutable ceph::mutex m_log_retire_lock;
  /* Hold a read lock on m_entry_reader_lock to add readers to log entry
   * bufs. Hold a write lock to prevent readers from being added (e.g. when
   * removing log entrys from the map). No lock required to remove readers. */
  mutable RWLock m_entry_reader_lock;
  /* Hold m_deferred_dispatch_lock while consuming from m_deferred_ios. */
  mutable ceph::mutex m_deferred_dispatch_lock;
  /* Hold m_log_append_lock while appending or retiring log entries. */
  mutable ceph::mutex m_log_append_lock;
  /* Used for most synchronization */
  mutable ceph::mutex m_lock;

  /* Used in release/detain to make BlockGuard preserve submission order */
  mutable ceph::mutex m_blockguard_lock;

  /* Use m_blockguard_lock for the following 3 things */
  rwl::WriteLogGuard::BlockOperations m_awaiting_barrier;
  bool m_barrier_in_progress = false;
  BlockGuardCell *m_barrier_cell = nullptr;

  bool m_wake_up_requested = false;
  bool m_wake_up_scheduled = false;
  bool m_wake_up_enabled = true;
  bool m_appending = false;
  bool m_dispatching_deferred_ops = false;

  Contexts m_flush_complete_contexts;

  rwl::GenericLogOperations m_ops_to_flush; /* Write ops needing flush in local log */
  rwl::GenericLogOperations m_ops_to_append; /* Write ops needing event append in local log */

  rwl::WriteLogMap m_blocks_to_log_entries;

  /* New entries are at the back. Oldest at the front */
  rwl::GenericLogEntries m_log_entries;
  rwl::GenericLogEntries m_dirty_log_entries;

  PerfCounters *m_perfcounter = nullptr;

  std::shared_ptr<rwl::SyncPoint> m_current_sync_point = nullptr;
  bool m_persist_on_flush = false; /* If false, persist each write before completion */

  int m_flush_ops_in_flight = 0;
  int m_flush_bytes_in_flight = 0;
  uint64_t m_lowest_flushing_sync_gen = 0;

  /* Writes that have left the block guard, but are waiting for resources */
  C_BlockIORequests m_deferred_ios;
  /* Throttle writes concurrently allocating & replicating */
  unsigned int m_free_lanes = rwl::MAX_CONCURRENT_WRITES;
  unsigned int m_unpublished_reserves = 0;

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
  void wake_up();
  void process_work();

  void flush_dirty_entries(Context *on_finish);
  bool can_flush_entry(const std::shared_ptr<rwl::GenericLogEntry> log_entry);
  Context *construct_flush_entry_ctx(const std::shared_ptr<rwl::GenericLogEntry> log_entry);
  void persist_last_flushed_sync_gen();
  bool handle_flushed_sync_point(std::shared_ptr<rwl::SyncPointLogEntry> log_entry);
  void sync_point_writer_flushed(std::shared_ptr<rwl::SyncPointLogEntry> log_entry);
  void process_writeback_dirty_entries();
  bool can_retire_entry(const std::shared_ptr<rwl::GenericLogEntry> log_entry);
  bool retire_entries(const unsigned long int frees_per_tx);

  void init_flush_new_sync_point(rwl::DeferredContexts &later);
  void new_sync_point(rwl::DeferredContexts &later);
  rwl::C_FlushRequest<ReplicatedWriteLog<ImageCtxT>>* make_flush_req(Context *on_finish);
  void flush_new_sync_point_if_needed(C_FlushRequestT *flush_req, rwl::DeferredContexts &later);

  void dispatch_deferred_writes(void);
  void alloc_and_dispatch_io_req(C_BlockIORequestT *write_req);
  void append_scheduled_ops(void);
  void enlist_op_appender();
  void schedule_append(rwl::GenericLogOperations &ops);
  void flush_then_append_scheduled_ops(void);
  void enlist_op_flusher();
  void alloc_op_log_entries(rwl::GenericLogOperations &ops);
  void flush_op_log_entries(rwl::GenericLogOperationsVector &ops);
  int append_op_log_entries(rwl::GenericLogOperations &ops);
  void complete_op_log_entries(rwl::GenericLogOperations &&ops, const int r);
  void schedule_complete_op_log_entries(rwl::GenericLogOperations &&ops, const int r);
  void internal_flush(bool invalidate, Context *on_finish);
};

} // namespace cache
} // namespace librbd

extern template class librbd::cache::ReplicatedWriteLog<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG
