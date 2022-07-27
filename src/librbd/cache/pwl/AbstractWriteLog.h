// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_PARENT_WRITE_LOG
#define CEPH_LIBRBD_CACHE_PARENT_WRITE_LOG

#include "common/Timer.h"
#include "common/RWLock.h"
#include "common/WorkQueue.h"
#include "common/AsyncOpTracker.h"
#include "librbd/cache/ImageWriteback.h"
#include "librbd/Utils.h"
#include "librbd/BlockGuard.h"
#include "librbd/cache/Types.h"
#include "librbd/cache/pwl/LogOperation.h"
#include "librbd/cache/pwl/ReadRequest.h"
#include "librbd/cache/pwl/Request.h"
#include "librbd/cache/pwl/LogMap.h"
#include "librbd/cache/pwl/Builder.h"
#include <functional>
#include <list>

class Context;

namespace librbd {

struct ImageCtx;

namespace plugin { template <typename> struct Api; }

namespace cache {
namespace pwl {

class GenericLogEntry;
class GenericWriteLogEntry;
class SyncPointLogEntry;
class WriteLogEntry;
struct WriteLogCacheEntry;

typedef std::list<std::shared_ptr<WriteLogEntry>> WriteLogEntries;
typedef std::list<std::shared_ptr<GenericLogEntry>> GenericLogEntries;
typedef std::list<std::shared_ptr<GenericWriteLogEntry>> GenericWriteLogEntries;
typedef std::vector<std::shared_ptr<GenericLogEntry>> GenericLogEntriesVector;

typedef LogMapEntries<GenericWriteLogEntry> WriteLogMapEntries;
typedef LogMap<GenericWriteLogEntry> WriteLogMap;

/**** Write log entries end ****/

typedef librbd::BlockGuard<GuardedRequest> WriteLogGuard;

class DeferredContexts;
template <typename>
class ImageCacheState;

template<typename T>
class Builder;

template <typename T>
struct C_BlockIORequest;

template <typename T>
struct C_WriteRequest;

using GenericLogOperations = std::list<GenericLogOperationSharedPtr>;


template <typename ImageCtxT>
class AbstractWriteLog {
public:
  typedef io::Extent Extent;
  typedef io::Extents Extents;
  using This = AbstractWriteLog<ImageCtxT>;
  Builder<This> *m_builder;

  AbstractWriteLog(ImageCtxT &image_ctx,
                   librbd::cache::pwl::ImageCacheState<ImageCtxT>* cache_state,
                   Builder<This> *builder,
                   cache::ImageWritebackInterface& image_writeback,
		   plugin::Api<ImageCtxT>& plugin_api);
  virtual ~AbstractWriteLog();
  AbstractWriteLog(const AbstractWriteLog&) = delete;
  AbstractWriteLog &operator=(const AbstractWriteLog&) = delete;

  /// IO methods
  void read(
      Extents&& image_extents, ceph::bufferlist *bl,
      int fadvise_flags, Context *on_finish);
  void write(
      Extents&& image_extents, ceph::bufferlist&& bl,
      int fadvise_flags,
      Context *on_finish);
  void discard(
      uint64_t offset, uint64_t length,
      uint32_t discard_granularity_bytes,
      Context *on_finish);
  void flush(
      io::FlushSource flush_source, Context *on_finish);
  void writesame(
      uint64_t offset, uint64_t length,
      ceph::bufferlist&& bl,
      int fadvise_flags, Context *on_finish);
  void compare_and_write(
      Extents&& image_extents,
      ceph::bufferlist&& cmp_bl, ceph::bufferlist&& bl,
      uint64_t *mismatch_offset,int fadvise_flags,
      Context *on_finish);

  /// internal state methods
  void init(Context *on_finish);
  void shut_down(Context *on_finish);
  void invalidate(Context *on_finish);
  void flush(Context *on_finish);

  using C_WriteRequestT = pwl::C_WriteRequest<This>;
  using C_BlockIORequestT = pwl::C_BlockIORequest<This>;
  using C_FlushRequestT = pwl::C_FlushRequest<This>;
  using C_DiscardRequestT = pwl::C_DiscardRequest<This>;
  using C_WriteSameRequestT = pwl::C_WriteSameRequest<This>;

  CephContext * get_context();
  void release_guarded_request(BlockGuardCell *cell);
  void release_write_lanes(C_BlockIORequestT *req);
  virtual bool alloc_resources(C_BlockIORequestT *req) = 0;
  virtual void setup_schedule_append(
      pwl::GenericLogOperationsVector &ops, bool do_early_flush,
      C_BlockIORequestT *req) = 0;
  void schedule_append(pwl::GenericLogOperationsVector &ops, C_BlockIORequestT *req = nullptr);
  void schedule_append(pwl::GenericLogOperationSharedPtr op, C_BlockIORequestT *req = nullptr);
  void flush_new_sync_point(C_FlushRequestT *flush_req,
                            pwl::DeferredContexts &later);

  std::shared_ptr<pwl::SyncPoint> get_current_sync_point() {
    return m_current_sync_point;
  }
  bool get_persist_on_flush() {
    return m_persist_on_flush;
  }
  void inc_last_op_sequence_num() {
    m_perfcounter->inc(l_librbd_pwl_log_ops, 1);
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
  void add_into_log_map(pwl::GenericWriteLogEntries &log_entries,
                        C_BlockIORequestT *req);
  virtual void complete_user_request(Context *&user_req, int r) = 0;
  virtual void copy_bl_to_buffer(
      WriteRequestResources *resources,
      std::unique_ptr<WriteLogOperationSet> &op_set) {}

private:
 typedef std::list<pwl::C_WriteRequest<This> *> C_WriteRequests;
 typedef std::list<pwl::C_BlockIORequest<This> *> C_BlockIORequests;

 std::atomic<bool> m_initialized = {false};

  uint64_t m_bytes_dirty = 0;     /* Total bytes yet to flush to RBD */
  utime_t m_last_alloc_fail;      /* Entry or buffer allocation fail seen */

  pwl::WriteLogGuard m_write_log_guard;

  /* Starts at 0 for a new write log. Incremented on every flush. */
  uint64_t m_current_sync_gen = 0;
  /* Starts at 0 on each sync gen increase. Incremented before applied
     to an operation */
  uint64_t m_last_op_sequence_num = 0;

  bool m_persist_on_write_until_flush = true;

  pwl::WriteLogGuard m_flush_guard;
  mutable ceph::mutex m_flush_guard_lock;

 /* Debug counters for the places m_async_op_tracker is used */
  std::atomic<int> m_async_complete_ops = {0};
  std::atomic<int> m_async_null_flush_finish = {0};
  std::atomic<int> m_async_process_work = {0};

  /* Hold m_deferred_dispatch_lock while consuming from m_deferred_ios. */
  mutable ceph::mutex m_deferred_dispatch_lock;

  /* Used in release/detain to make BlockGuard preserve submission order */
  mutable ceph::mutex m_blockguard_lock;

  /* Use m_blockguard_lock for the following 3 things */
  bool m_barrier_in_progress = false;
  BlockGuardCell *m_barrier_cell = nullptr;

  bool m_wake_up_enabled = true;

  Contexts m_flush_complete_contexts;

  std::shared_ptr<pwl::SyncPoint> m_current_sync_point = nullptr;
  bool m_persist_on_flush = false; //If false, persist each write before completion

  int m_flush_ops_in_flight = 0;
  int m_flush_bytes_in_flight = 0;
  uint64_t m_lowest_flushing_sync_gen = 0;

  /* Writes that have left the block guard, but are waiting for resources */
  C_BlockIORequests m_deferred_ios;
  /* Throttle writes concurrently allocating & replicating */
  unsigned int m_free_lanes = pwl::MAX_CONCURRENT_WRITES;

  SafeTimer *m_timer = nullptr; /* Used with m_timer_lock */
  mutable ceph::mutex *m_timer_lock = nullptr; /* Used with and by m_timer */
  Context *m_timer_ctx = nullptr;

  ThreadPool m_thread_pool;

  uint32_t m_discard_granularity_bytes;

  BlockGuardCell* detain_guarded_request_helper(pwl::GuardedRequest &req);
  BlockGuardCell* detain_guarded_request_barrier_helper(
      pwl::GuardedRequest &req);
  void detain_guarded_request(C_BlockIORequestT *request,
                              pwl::GuardedRequestFunctionContext *guarded_ctx,
                              bool is_barrier);
  void perf_start(const std::string name);
  void perf_stop();
  void log_perf();
  void periodic_stats();
  void arm_periodic_stats();

  void pwl_init(Context *on_finish, pwl::DeferredContexts &later);
  void update_image_cache_state(Context *on_finish);
  void handle_update_image_cache_state(int r);
  void check_image_cache_state_clean();

  void flush_dirty_entries(Context *on_finish);
  bool can_flush_entry(const std::shared_ptr<pwl::GenericLogEntry> log_entry);
  bool handle_flushed_sync_point(
      std::shared_ptr<pwl::SyncPointLogEntry> log_entry);
  void sync_point_writer_flushed(
      std::shared_ptr<pwl::SyncPointLogEntry> log_entry);

  void init_flush_new_sync_point(pwl::DeferredContexts &later);
  void new_sync_point(pwl::DeferredContexts &later);
  pwl::C_FlushRequest<AbstractWriteLog<ImageCtxT>>* make_flush_req(
      Context *on_finish);
  void flush_new_sync_point_if_needed(C_FlushRequestT *flush_req,
                                      pwl::DeferredContexts &later);

  void alloc_and_dispatch_io_req(C_BlockIORequestT *write_req);
  void schedule_complete_op_log_entries(pwl::GenericLogOperations &&ops,
                                        const int r);
  void internal_flush(bool invalidate, Context *on_finish);

protected:
  librbd::cache::pwl::ImageCacheState<ImageCtxT>* m_cache_state = nullptr;

  std::atomic<bool> m_shutting_down = {false};
  std::atomic<bool> m_invalidating = {false};

  ImageCtxT &m_image_ctx;

  std::string m_log_pool_name;
  uint64_t m_log_pool_size;

  uint32_t m_total_log_entries = 0;
  uint32_t m_free_log_entries = 0;

  std::atomic<uint64_t> m_bytes_allocated = {0}; /* Total bytes allocated in write buffers */
  uint64_t m_bytes_cached = 0;    /* Total bytes used in write buffers */
  uint64_t m_bytes_allocated_cap = 0;

  std::atomic<bool> m_alloc_failed_since_retire = {false};

  cache::ImageWritebackInterface& m_image_writeback;
  plugin::Api<ImageCtxT>& m_plugin_api;

  /*
   * When m_first_free_entry == m_first_valid_entry, the log is
   * empty. There is always at least one free entry, which can't be
   * used.
   */
  uint64_t m_first_free_entry = 0;  /* Entries from here to m_first_valid_entry-1 are free */
  uint64_t m_first_valid_entry = 0; /* Entries from here to m_first_free_entry-1 are valid */

  /* All writes bearing this and all prior sync gen numbers are flushed */
  uint64_t m_flushed_sync_gen = 0;

  AsyncOpTracker m_async_op_tracker;
  /* Debug counters for the places m_async_op_tracker is used */
  std::atomic<int> m_async_flush_ops = {0};
  std::atomic<int> m_async_append_ops = {0};

  /* Acquire locks in order declared here */

  mutable ceph::mutex m_log_retire_lock;
  /* Hold a read lock on m_entry_reader_lock to add readers to log entry
   * bufs. Hold a write lock to prevent readers from being added (e.g. when
   * removing log entrys from the map). No lock required to remove readers. */
  mutable RWLock m_entry_reader_lock;
  /* Hold m_log_append_lock while appending or retiring log entries. */
  mutable ceph::mutex m_log_append_lock;
  /* Used for most synchronization */
  mutable ceph::mutex m_lock;

  /* Use m_blockguard_lock for the following 3 things */
  pwl::WriteLogGuard::BlockOperations m_awaiting_barrier;

  bool m_wake_up_requested = false;
  bool m_wake_up_scheduled = false;
  bool m_appending = false;
  bool m_dispatching_deferred_ops = false;

  pwl::GenericLogOperations m_ops_to_flush; /* Write ops needing flush in local log */
  pwl::GenericLogOperations m_ops_to_append; /* Write ops needing event append in local log */

  pwl::WriteLogMap m_blocks_to_log_entries;

  /* New entries are at the back. Oldest at the front */
  pwl::GenericLogEntries m_log_entries;
  pwl::GenericLogEntries m_dirty_log_entries;

  PerfCounters *m_perfcounter = nullptr;

  unsigned int m_unpublished_reserves = 0;

  ContextWQ m_work_queue;

  void wake_up();

  void update_entries(
      std::shared_ptr<pwl::GenericLogEntry> *log_entry,
      pwl::WriteLogCacheEntry *cache_entry,
      std::map<uint64_t, bool> &missing_sync_points,
      std::map<uint64_t,
      std::shared_ptr<pwl::SyncPointLogEntry>> &sync_point_entries,
      uint64_t entry_index);
  void update_sync_points(
      std::map<uint64_t, bool> &missing_sync_points,
      std::map<uint64_t,
      std::shared_ptr<pwl::SyncPointLogEntry>> &sync_point_entries,
      pwl::DeferredContexts &later);
  virtual void inc_allocated_cached_bytes(
      std::shared_ptr<pwl::GenericLogEntry> log_entry) = 0;
  Context *construct_flush_entry(
      const std::shared_ptr<pwl::GenericLogEntry> log_entry, bool invalidating);
  void detain_flush_guard_request(std::shared_ptr<GenericLogEntry> log_entry,
                                  GuardedRequestFunctionContext *guarded_ctx);
  void process_writeback_dirty_entries();
  bool can_retire_entry(const std::shared_ptr<pwl::GenericLogEntry> log_entry);

  void dispatch_deferred_writes(void);
  void complete_op_log_entries(pwl::GenericLogOperations &&ops, const int r);

  bool check_allocation(
      C_BlockIORequestT *req, uint64_t bytes_cached, uint64_t bytes_dirtied,
      uint64_t bytes_allocated, uint32_t num_lanes, uint32_t num_log_entries,
      uint32_t num_unpublished_reserves);
  void append_scheduled(
      pwl::GenericLogOperations &ops, bool &ops_remain, bool &appending,
      bool isRWL=false);

  virtual void process_work() = 0;
  virtual void append_scheduled_ops(void) = 0;
  virtual void schedule_append_ops(pwl::GenericLogOperations &ops, C_BlockIORequestT *req) = 0;
  virtual void remove_pool_file() = 0;
  virtual bool initialize_pool(Context *on_finish,
                               pwl::DeferredContexts &later) = 0;
  virtual void collect_read_extents(
      uint64_t read_buffer_offset, LogMapEntry<GenericWriteLogEntry> map_entry,
      std::vector<std::shared_ptr<GenericWriteLogEntry>> &log_entries_to_read,
      std::vector<bufferlist*> &bls_to_read, uint64_t entry_hit_length,
      Extent hit_extent, pwl::C_ReadRequest *read_ctx) = 0;
  virtual void complete_read(
      std::vector<std::shared_ptr<GenericWriteLogEntry>> &log_entries_to_read,
      std::vector<bufferlist*> &bls_to_read, Context *ctx) = 0;
  virtual void write_data_to_buffer(
      std::shared_ptr<pwl::WriteLogEntry> ws_entry,
      pwl::WriteLogCacheEntry *cache_entry) {}
  virtual void release_ram(
      const std::shared_ptr<pwl::GenericLogEntry> log_entry) {}
  virtual void alloc_op_log_entries(pwl::GenericLogOperations &ops) {}
  virtual bool retire_entries(const unsigned long int frees_per_tx) {
    return false;
  }
  virtual void schedule_flush_and_append(
      pwl::GenericLogOperationsVector &ops) {}
  virtual void persist_last_flushed_sync_gen() {}
  virtual void reserve_cache(C_BlockIORequestT *req, bool &alloc_succeeds,
                             bool &no_space) {}
  virtual void construct_flush_entries(pwl::GenericLogEntries entries_to_flush,
					DeferredContexts &post_unlock,
					bool has_write_entry) = 0;
  virtual uint64_t get_max_extent() {
    return 0;
  }
  void update_image_cache_state(void);
};

} // namespace pwl
} // namespace cache
} // namespace librbd

extern template class librbd::cache::pwl::AbstractWriteLog<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_PARENT_WRITE_LOG
