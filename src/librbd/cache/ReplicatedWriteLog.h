// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG
#define CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG

#include "common/RWLock.h"
#include "common/Timer.h"
#include "common/WorkQueue.h"
#include "common/AsyncOpTracker.h"
#include "ImageCache.h"
#include "ImageWriteback.h"
#include "librbd/Utils.h"
#include "librbd/BlockGuard.h"
#include <functional>
#include <list>
#include "common/Finisher.h"
#include "include/ceph_assert.h"
#include "librbd/cache/rwl/LogMap.h"

enum {
  l_librbd_rwl_first = 26500,

  // All read requests
  l_librbd_rwl_rd_req,           // read requests
  l_librbd_rwl_rd_bytes,         // bytes read
  l_librbd_rwl_rd_latency,       // average req completion latency

  // Read requests completed from RWL (no misses)
  l_librbd_rwl_rd_hit_req,       // read requests
  l_librbd_rwl_rd_hit_bytes,     // bytes read
  l_librbd_rwl_rd_hit_latency,   // average req completion latency

  // Reed requests with hit and miss extents
  l_librbd_rwl_rd_part_hit_req,  // read ops

  // All write requests
  l_librbd_rwl_wr_req,             // write requests
  l_librbd_rwl_wr_req_def,         // write requests deferred for resources
  l_librbd_rwl_wr_req_def_lanes,   // write requests deferred for lanes
  l_librbd_rwl_wr_req_def_log,     // write requests deferred for log entries
  l_librbd_rwl_wr_req_def_buf,     // write requests deferred for buffer space
  l_librbd_rwl_wr_req_overlap,     // write requests detained for overlap
  l_librbd_rwl_wr_req_queued,      // write requests queued for prior barrier
  l_librbd_rwl_wr_bytes,           // bytes written

  // Write log operations (1 .. n per request that appends to the log)
  l_librbd_rwl_log_ops,            // log append ops
  l_librbd_rwl_log_op_bytes,       // average bytes written per log op

  /*

   Req and op average latencies to the beginning of and over various phases:

   +------------------------------+------+-------------------------------+
   | Phase                        | Name | Description                   |
   +------------------------------+------+-------------------------------+
   | Arrive at RWL                | arr  |Arrives as a request           |
   +------------------------------+------+-------------------------------+
   | Allocate resources           | all  |time spent in block guard for  |
   |                              |      |overlap sequencing occurs      |
   |                              |      |before this point              |
   +------------------------------+------+-------------------------------+
   | Dispatch                     | dis  |Op lifetime begins here. time  |
   |                              |      |spent in allocation waiting for|
   |                              |      |resources occurs before this   |
   |                              |      |point                          |
   +------------------------------+------+-------------------------------+
   | Payload buffer persist and   | buf  |time spent queued for          |
   |replicate                     |      |replication occurs before here |
   +------------------------------+------+-------------------------------+
   | Payload buffer persist       | bufc |bufc - buf is just the persist |
   |complete                      |      |time                           |
   +------------------------------+------+-------------------------------+
   | Log append                   | app  |time spent queued for append   |
   |                              |      |occurs before here             |
   +------------------------------+------+-------------------------------+
   | Append complete              | appc |appc - app is just the time    |
   |                              |      |spent in the append operation  |
   +------------------------------+------+-------------------------------+
   | Complete                     | cmp  |write persisted, replicated,   |
   |                              |      |and globally visible           |
   +------------------------------+------+-------------------------------+

  */

  /* Request times */
  l_librbd_rwl_req_arr_to_all_t,   // arrival to allocation elapsed time - same as time deferred in block guard
  l_librbd_rwl_req_arr_to_dis_t,   // arrival to dispatch elapsed time
  l_librbd_rwl_req_all_to_dis_t,   // Time spent allocating or waiting to allocate resources
  l_librbd_rwl_wr_latency,         // average req (persist) completion latency
  l_librbd_rwl_wr_latency_hist,    // Histogram of write req (persist) completion latency vs. bytes written
  l_librbd_rwl_wr_caller_latency,  // average req completion (to caller) latency

  /* Request times for requests that never waited for space*/
  l_librbd_rwl_nowait_req_arr_to_all_t,   // arrival to allocation elapsed time - same as time deferred in block guard
  l_librbd_rwl_nowait_req_arr_to_dis_t,   // arrival to dispatch elapsed time
  l_librbd_rwl_nowait_req_all_to_dis_t,   // Time spent allocating or waiting to allocate resources
  l_librbd_rwl_nowait_wr_latency,         // average req (persist) completion latency
  l_librbd_rwl_nowait_wr_latency_hist,    // Histogram of write req (persist) completion latency vs. bytes written
  l_librbd_rwl_nowait_wr_caller_latency,  // average req completion (to caller) latency

  /* Log operation times */
  l_librbd_rwl_log_op_alloc_t,      // elapsed time of pmemobj_reserve()
  l_librbd_rwl_log_op_alloc_t_hist, // Histogram of elapsed time of pmemobj_reserve()

  l_librbd_rwl_log_op_dis_to_buf_t, // dispatch to buffer persist elapsed time
  l_librbd_rwl_log_op_dis_to_app_t, // dispatch to log append elapsed time
  l_librbd_rwl_log_op_dis_to_cmp_t, // dispatch to persist completion elapsed time
  l_librbd_rwl_log_op_dis_to_cmp_t_hist, // Histogram of dispatch to persist completion elapsed time

  l_librbd_rwl_log_op_buf_to_app_t, // data buf persist + append wait time
  l_librbd_rwl_log_op_buf_to_bufc_t,// data buf persist / replicate elapsed time
  l_librbd_rwl_log_op_buf_to_bufc_t_hist,// data buf persist time vs bytes histogram
  l_librbd_rwl_log_op_app_to_cmp_t, // log entry append + completion wait time
  l_librbd_rwl_log_op_app_to_appc_t, // log entry append / replicate elapsed time
  l_librbd_rwl_log_op_app_to_appc_t_hist, // log entry append time (vs. op bytes) histogram

  l_librbd_rwl_discard,
  l_librbd_rwl_discard_bytes,
  l_librbd_rwl_discard_latency,

  l_librbd_rwl_aio_flush,
  l_librbd_rwl_aio_flush_def,
  l_librbd_rwl_aio_flush_latency,
  l_librbd_rwl_ws,
  l_librbd_rwl_ws_bytes, // Bytes modified by write same, probably much larger than WS payload bytes
  l_librbd_rwl_ws_latency,

  l_librbd_rwl_cmp,
  l_librbd_rwl_cmp_bytes,
  l_librbd_rwl_cmp_latency,
  l_librbd_rwl_cmp_fails,

  l_librbd_rwl_flush,
  l_librbd_rwl_invalidate_cache,
  l_librbd_rwl_invalidate_discard_cache,

  l_librbd_rwl_append_tx_t,
  l_librbd_rwl_retire_tx_t,
  l_librbd_rwl_append_tx_t_hist,
  l_librbd_rwl_retire_tx_t_hist,

  l_librbd_rwl_last,
};

namespace librbd {

struct ImageCtx;

namespace cache {

template <typename ImageCtxT = librbd::ImageCtx>
class ReplicatedWriteLog;
struct WriteBufferAllocation;

namespace rwl {
typedef std::list<Context *> Contexts;
typedef std::vector<Context*> ContextsV;

static const uint32_t MIN_WRITE_ALLOC_SIZE = 512;
/* Enables use of dedicated finishers for some RWL work */
static const bool use_finishers = false;

static const int IN_FLIGHT_FLUSH_WRITE_LIMIT = 64;
static const int IN_FLIGHT_FLUSH_BYTES_LIMIT = (1 * 1024 * 1024);

/* Limit work between sync points */
static const uint64_t MAX_WRITES_PER_SYNC_POINT = 256;
static const uint64_t MAX_BYTES_PER_SYNC_POINT = (1024 * 1024 * 8);

static const double LOG_STATS_INTERVAL_SECONDS = 5;

/**** Write log entries ****/

static const unsigned long int MAX_ALLOC_PER_TRANSACTION = 8;
static const unsigned long int MAX_FREE_PER_TRANSACTION = 1;
static const unsigned int MAX_CONCURRENT_WRITES = 256;
static const uint64_t DEFAULT_POOL_SIZE = 1u<<30;
//static const uint64_t MIN_POOL_SIZE = 1u<<23;
// force pools to be 1G until thread::arena init issue is resolved
static const uint64_t MIN_POOL_SIZE = DEFAULT_POOL_SIZE;
static const double USABLE_SIZE = (7.0 / 10);
static const uint64_t BLOCK_ALLOC_OVERHEAD_BYTES = 16;
static const uint8_t RWL_POOL_VERSION = 1;
static const uint64_t MAX_LOG_ENTRIES = (1024 * 1024);
//static const uint64_t MAX_LOG_ENTRIES = (1024 * 128);
static const double AGGRESSIVE_RETIRE_HIGH_WATER = 0.75;
static const double RETIRE_HIGH_WATER = 0.50;
static const double RETIRE_LOW_WATER = 0.40;
static const int RETIRE_BATCH_TIME_LIMIT_MS = 250;

class SyncPointLogEntry;
class GeneralWriteLogEntry;
class WriteLogEntry;
class WriteSameLogEntry;
class DiscardLogEntry;
class GenericLogEntry;

typedef std::list<std::shared_ptr<GeneralWriteLogEntry>> GeneralWriteLogEntries;
typedef std::list<std::shared_ptr<WriteLogEntry>> WriteLogEntries;
typedef std::list<std::shared_ptr<GenericLogEntry>> GenericLogEntries;
typedef std::vector<std::shared_ptr<GenericLogEntry>> GenericLogEntriesVector;

/**** Write log entries end ****/

template <typename T>
class SyncPoint {
public:
  T &rwl;
  std::shared_ptr<SyncPointLogEntry> log_entry;
  /* Use m_lock for earlier/later links */
  std::shared_ptr<SyncPoint<T>> earlier_sync_point; /* NULL if earlier has completed */
  std::shared_ptr<SyncPoint<T>> later_sync_point;
  uint64_t m_final_op_sequence_num = 0;
  /* A sync point can't appear in the log until all the writes bearing
   * it and all the prior sync points have been appended and
   * persisted.
   *
   * Writes bearing this sync gen number and the prior sync point will be
   * sub-ops of this Gather. This sync point will not be appended until all
   * these complete to the point where their persist order is guaranteed. */
  C_Gather *m_prior_log_entries_persisted;
  int m_prior_log_entries_persisted_result = 0;
  int m_prior_log_entries_persisted_complete = false;
  /* The finisher for this will append the sync point to the log.  The finisher
   * for m_prior_log_entries_persisted will be a sub-op of this. */
  C_Gather *m_sync_point_persist;
  bool m_append_scheduled = false;
  bool m_appending = false;
  /* Signal these when this sync point is appending to the log, and its order
   * of appearance is guaranteed. One of these is is a sub-operation of the
   * next sync point's m_prior_log_entries_persisted Gather. */
  std::vector<Context*> m_on_sync_point_appending;
  /* Signal these when this sync point is appended and persisted. User
   * aio_flush() calls are added to this. */
  std::vector<Context*> m_on_sync_point_persisted;

  SyncPoint(T &rwl, const uint64_t sync_gen_num);
  ~SyncPoint();
  SyncPoint(const SyncPoint&) = delete;
  SyncPoint &operator=(const SyncPoint&) = delete;
  std::ostream &format(std::ostream &os) const;
  friend std::ostream &operator<<(std::ostream &os,
				  const SyncPoint &p) {
    return p.format(os);
  }
};

template <typename T>
class WriteLogOperationSet;

template <typename T>
class WriteLogOperation;

template <typename T>
class GeneralWriteLogOperation;

template <typename T>
class SyncPointLogOperation;

template <typename T>
class DiscardLogOperation;

template <typename T>
class GenericLogOperation {
public:
  T &rwl;
  utime_t m_dispatch_time;         // When op created
  utime_t m_buf_persist_time;      // When buffer persist begins
  utime_t m_buf_persist_comp_time; // When buffer persist completes
  utime_t m_log_append_time;       // When log append begins
  utime_t m_log_append_comp_time;  // When log append completes
  GenericLogOperation(T &rwl, const utime_t dispatch_time);
  virtual ~GenericLogOperation() { };
  GenericLogOperation(const GenericLogOperation&) = delete;
  GenericLogOperation &operator=(const GenericLogOperation&) = delete;
  virtual std::ostream &format(std::ostream &os) const {
    os << "m_dispatch_time=[" << m_dispatch_time << "], "
       << "m_buf_persist_time=[" << m_buf_persist_time << "], "
       << "m_buf_persist_comp_time=[" << m_buf_persist_comp_time << "], "
       << "m_log_append_time=[" << m_log_append_time << "], "
       << "m_log_append_comp_time=[" << m_log_append_comp_time << "], ";
    return os;
  };
  friend std::ostream &operator<<(std::ostream &os,
				  const GenericLogOperation &op) {
    return op.format(os);
  }
  virtual const std::shared_ptr<GenericLogEntry> get_log_entry() = 0;
  virtual const std::shared_ptr<SyncPointLogEntry> get_sync_point_log_entry() { return nullptr; }
  virtual const std::shared_ptr<GeneralWriteLogEntry> get_gen_write_log_entry() { return nullptr; }
  virtual const std::shared_ptr<WriteLogEntry> get_write_log_entry() { return nullptr; }
  virtual const std::shared_ptr<WriteSameLogEntry> get_write_same_log_entry() { return nullptr; }
  virtual const std::shared_ptr<DiscardLogEntry> get_discard_log_entry() { return nullptr; }
  virtual void appending() = 0;
  virtual void complete(int r) = 0;
  virtual bool is_write() { return false; }
  virtual bool is_sync_point() { return false; }
  virtual bool is_discard() { return false; }
  virtual bool is_writesame() { return false; }
  virtual bool is_writing_op() { return false; }
  virtual GeneralWriteLogOperation<T> *get_gen_write_op() { return nullptr; };
  virtual WriteLogOperation<T> *get_write_op() { return nullptr; };
};

template <typename T>
class SyncPointLogOperation : public GenericLogOperation<T> {
public:
  using GenericLogOperation<T>::rwl;
  std::shared_ptr<SyncPoint<T>> sync_point;
  SyncPointLogOperation(T &rwl,
			std::shared_ptr<SyncPoint<T>> sync_point,
			const utime_t dispatch_time);
  ~SyncPointLogOperation();
  SyncPointLogOperation(const SyncPointLogOperation&) = delete;
  SyncPointLogOperation &operator=(const SyncPointLogOperation&) = delete;
  std::ostream &format(std::ostream &os) const {
    os << "(Sync Point) ";
    GenericLogOperation<T>::format(os);
    os << ", "
       << "sync_point=[" << *sync_point << "]";
    return os;
  };
  friend std::ostream &operator<<(std::ostream &os,
				  const SyncPointLogOperation<T> &op) {
    return op.format(os);
  }
  const std::shared_ptr<GenericLogEntry> get_log_entry() { return get_sync_point_log_entry(); }
  const std::shared_ptr<SyncPointLogEntry> get_sync_point_log_entry() { return sync_point->log_entry; }
  bool is_sync_point() { return true; }
  void appending();
  void complete(int r);
};

template <typename T>
class GeneralWriteLogOperation : public GenericLogOperation<T> {
protected:
  friend class WriteLogOperation<T>;
  friend class DiscardLogOperation<T>;
  ceph::mutex m_lock;
public:
  using GenericLogOperation<T>::rwl;
  std::shared_ptr<SyncPoint<T>> sync_point;
  Context *on_write_append = nullptr;  /* Completion for things waiting on this
					* write's position in the log to be
					* guaranteed */
  Context *on_write_persist = nullptr; /* Completion for things waiting on this
					* write to persist */
  GeneralWriteLogOperation(T &rwl,
			   std::shared_ptr<SyncPoint<T>> sync_point,
			   const utime_t dispatch_time);
  ~GeneralWriteLogOperation();
  GeneralWriteLogOperation(const GeneralWriteLogOperation&) = delete;
  GeneralWriteLogOperation &operator=(const GeneralWriteLogOperation&) = delete;
  std::ostream &format(std::ostream &os) const {
    GenericLogOperation<T>::format(os);
    return os;
  };
  friend std::ostream &operator<<(std::ostream &os,
				  const GeneralWriteLogOperation<T> &op) {
    return op.format(os);
  }
  GeneralWriteLogOperation<T> *get_gen_write_op() { return this; };
  bool is_writing_op() { return true; }
  void appending();
  void complete(int r);
};

template <typename T>
class WriteLogOperation : public GeneralWriteLogOperation<T> {
public:
  using GenericLogOperation<T>::rwl;
  using GeneralWriteLogOperation<T>::m_lock;
  using GeneralWriteLogOperation<T>::sync_point;
  using GeneralWriteLogOperation<T>::on_write_append;
  using GeneralWriteLogOperation<T>::on_write_persist;
  std::shared_ptr<WriteLogEntry> log_entry;
  bufferlist bl;
  WriteBufferAllocation *buffer_alloc = nullptr;
  WriteLogOperation(WriteLogOperationSet<T> &set, const uint64_t image_offset_bytes, const uint64_t write_bytes);
  ~WriteLogOperation();
  WriteLogOperation(const WriteLogOperation&) = delete;
  WriteLogOperation &operator=(const WriteLogOperation&) = delete;
  std::ostream &format(std::ostream &os) const;
  friend std::ostream &operator<<(std::ostream &os,
				  const WriteLogOperation<T> &op) {
    return op.format(os);
  }
  const std::shared_ptr<GenericLogEntry> get_log_entry() { return get_write_log_entry(); }
  const std::shared_ptr<WriteLogEntry> get_write_log_entry() { return log_entry; }
  WriteLogOperation<T> *get_write_op() override { return this; }
  bool is_write() { return true; }
};

template <typename T>
class WriteSameLogOperation : public WriteLogOperation<T> {
public:
  using GenericLogOperation<T>::rwl;
  using GeneralWriteLogOperation<T>::m_lock;
  using GeneralWriteLogOperation<T>::sync_point;
  using GeneralWriteLogOperation<T>::on_write_append;
  using GeneralWriteLogOperation<T>::on_write_persist;
  using WriteLogOperation<T>::log_entry;
  using WriteLogOperation<T>::bl;
  using WriteLogOperation<T>::buffer_alloc;
  WriteSameLogOperation(WriteLogOperationSet<T> &set, const uint64_t image_offset_bytes,
			const uint64_t write_bytes, const uint32_t data_len);
  ~WriteSameLogOperation();
  WriteSameLogOperation(const WriteSameLogOperation&) = delete;
  WriteSameLogOperation &operator=(const WriteSameLogOperation&) = delete;
  std::ostream &format(std::ostream &os) const {
    os << "(Write Same) ";
    WriteLogOperation<T>::format(os);
    return os;
  };
  friend std::ostream &operator<<(std::ostream &os,
				  const WriteSameLogOperation<T> &op) {
    return op.format(os);
  }
  const std::shared_ptr<GenericLogEntry> get_log_entry() { return get_write_same_log_entry(); }
  const std::shared_ptr<WriteSameLogEntry> get_write_same_log_entry() {
    return static_pointer_cast<WriteSameLogEntry>(log_entry);
  }
  bool is_write() { return false; }
  bool is_writesame() { return true; }
};

template <typename T>
class DiscardLogOperation : public GeneralWriteLogOperation<T> {
public:
  using GenericLogOperation<T>::rwl;
  using GeneralWriteLogOperation<T>::m_lock;
  using GeneralWriteLogOperation<T>::sync_point;
  using GeneralWriteLogOperation<T>::on_write_append;
  using GeneralWriteLogOperation<T>::on_write_persist;
  std::shared_ptr<DiscardLogEntry> log_entry;
  DiscardLogOperation(T &rwl,
		      std::shared_ptr<SyncPoint<T>> sync_point,
		      const uint64_t image_offset_bytes,
		      const uint64_t write_bytes,
		      const utime_t dispatch_time);
  ~DiscardLogOperation();
  DiscardLogOperation(const DiscardLogOperation&) = delete;
  DiscardLogOperation &operator=(const DiscardLogOperation&) = delete;
  std::ostream &format(std::ostream &os) const;
  friend std::ostream &operator<<(std::ostream &os,
				  const DiscardLogOperation<T> &op) {
    return op.format(os);
  }
  const std::shared_ptr<GenericLogEntry> get_log_entry() { return get_discard_log_entry(); }
  const std::shared_ptr<DiscardLogEntry> get_discard_log_entry() { return log_entry; }
  bool is_discard() { return true; }
};

template <typename T>
using GenericLogOperationSharedPtr = std::shared_ptr<GenericLogOperation<T>>;

template <typename T>
using GenericLogOperations = std::list<GenericLogOperationSharedPtr<T>>;

template <typename T>
using GenericLogOperationsVector = std::vector<GenericLogOperationSharedPtr<T>>;

template <typename T>
using WriteLogOperationSharedPtr = std::shared_ptr<WriteLogOperation<T>>;

template <typename T>
using WriteLogOperations = std::list<WriteLogOperationSharedPtr<T>>;

template <typename T>
using WriteSameLogOperationSharedPtr = std::shared_ptr<WriteSameLogOperation<T>>;

template <typename T>
class WriteLogOperationSet {
public:
  T &rwl;
  BlockExtent m_extent; /* in blocks */
  Context *m_on_finish;
  bool m_persist_on_flush;
  BlockGuardCell *m_cell;
  C_Gather *m_extent_ops_appending;
  Context *m_on_ops_appending;
  C_Gather *m_extent_ops_persist;
  Context *m_on_ops_persist;
  GenericLogOperationsVector<T> operations;
  utime_t m_dispatch_time; /* When set created */
  std::shared_ptr<SyncPoint<T>> sync_point;
  WriteLogOperationSet(T &rwl, const utime_t dispatched, std::shared_ptr<SyncPoint<T>> sync_point,
		       const bool persist_on_flush, BlockExtent extent, Context *on_finish);
  ~WriteLogOperationSet();
  WriteLogOperationSet(const WriteLogOperationSet&) = delete;
  WriteLogOperationSet &operator=(const WriteLogOperationSet&) = delete;
  friend std::ostream &operator<<(std::ostream &os,
				  const WriteLogOperationSet<T> &s) {
    os << "m_extent=[" << s.m_extent.block_start << "," << s.m_extent.block_end << "] "
       << "m_on_finish=" << s.m_on_finish << ", "
       << "m_cell=" << (void*)s.m_cell << ", "
       << "m_extent_ops_appending=[" << s.m_extent_ops_appending << ", "
       << "m_extent_ops_persist=[" << s.m_extent_ops_persist << "]";
    return os;
  };
};

struct BlockGuardReqState {
  bool barrier = false; /* This is a barrier request */
  bool current_barrier = false; /* This is the currently active barrier */
  bool detained = false;
  bool queued = false; /* Queued for barrier */
  friend std::ostream &operator<<(std::ostream &os,
				  const BlockGuardReqState &r) {
    os << "barrier=" << r.barrier << ", "
       << "current_barrier=" << r.current_barrier << ", "
       << "detained=" << r.detained << ", "
       << "queued=" << r.queued;
    return os;
  };
};

class GuardedRequestFunctionContext : public Context {
private:
  boost::function<void(GuardedRequestFunctionContext&)> m_callback;
  void finish(int r) override;
public:
  BlockGuardCell *m_cell = nullptr;
  BlockGuardReqState m_state;
  GuardedRequestFunctionContext(boost::function<void(GuardedRequestFunctionContext&)> &&callback);
  ~GuardedRequestFunctionContext(void);
  GuardedRequestFunctionContext(const GuardedRequestFunctionContext&) = delete;
  GuardedRequestFunctionContext &operator=(const GuardedRequestFunctionContext&) = delete;
};

struct GuardedRequest {
  const BlockExtent block_extent;
  GuardedRequestFunctionContext *guard_ctx; /* Work to do when guard on range obtained */

  GuardedRequest(const BlockExtent block_extent,
		 GuardedRequestFunctionContext *on_guard_acquire, bool barrier = false)
    : block_extent(block_extent), guard_ctx(on_guard_acquire) {
    guard_ctx->m_state.barrier = barrier;
  }
  friend std::ostream &operator<<(std::ostream &os,
				  const GuardedRequest &r) {
    os << "guard_ctx->m_state=[" << r.guard_ctx->m_state << "], "
       << "block_extent.block_start=" << r.block_extent.block_start << ", "
       << "block_extent.block_start=" << r.block_extent.block_end;
    return os;
  };
};

typedef librbd::BlockGuard<GuardedRequest> WriteLogGuard;
typedef LogMapEntry<GeneralWriteLogEntry> WriteLogMapEntry;
typedef LogMapEntries<GeneralWriteLogEntry> WriteLogMapEntries;
typedef LogMap<GeneralWriteLogEntry, GeneralWriteLogEntries> WriteLogMap;

template <typename T>
struct C_GuardedBlockIORequest;

class DeferredContexts;

} // namespace rwl

using namespace librbd::cache::rwl;

template <typename T>
struct C_BlockIORequest;

template <typename T>
struct C_WriteRequest;

template <typename T>
struct C_FlushRequest;

template <typename T>
struct C_DiscardRequest;

template <typename T>
struct C_WriteSameRequest;

template <typename T>
struct C_CompAndWriteRequest;

/**
 * Prototype pmem-based, client-side, replicated write log
 */
template <typename ImageCtxT>
class ImageCacheStateRWL: public ImageCacheState<ImageCtxT> {
  using ImageCacheState<ImageCtxT>::m_image_ctx;
public:
  std::string m_host;
  std::string m_path;
  uint64_t m_size;
  
  bool m_remove_on_close;
  bool m_log_stats_on_close;
  bool m_log_periodic_stats;
  bool m_invalidate_on_flush;

  ImageCacheStateRWL(ImageCtxT *image_ctx, JSONFormattable &f);
  ImageCacheStateRWL(ImageCtxT *image_ctx);
  bool is_valid() override;
  void dump(Formatter *f) const override;
};

class ReplicatedWriteLogInternal;
template <typename ImageCtxT>
class ReplicatedWriteLog : public ImageCache<ImageCtxT> {
public:
  using typename ImageCache<ImageCtxT>::Extent;
  using typename ImageCache<ImageCtxT>::Extents;
  using This = ReplicatedWriteLog<ImageCtxT>;
  using SyncPointT = rwl::SyncPoint<This>;
  using GenericLogOperationT = rwl::GenericLogOperation<This>;
  using GenericLogOperationSharedPtrT = rwl::GenericLogOperationSharedPtr<This>;
  using WriteLogOperationT = rwl::WriteLogOperation<This>;
  using WriteLogOperationSetT = rwl::WriteLogOperationSet<This>;
  using SyncPointLogOperationT = rwl::SyncPointLogOperation<This>;
  using WriteSameLogOperationT = rwl::WriteSameLogOperation<This>;
  using DiscardLogOperationT = rwl::DiscardLogOperation<This>;
  using GenericLogOperationsT = rwl::GenericLogOperations<This>;
  using GenericLogOperationsVectorT = rwl::GenericLogOperationsVector<This>;
  using C_BlockIORequestT = C_BlockIORequest<This>;
  using C_WriteRequestT = C_WriteRequest<This>;
  using C_FlushRequestT = C_FlushRequest<This>;
  using C_DiscardRequestT = C_DiscardRequest<This>;
  using C_WriteSameRequestT = C_WriteSameRequest<This>;
  using C_CompAndWriteRequestT = C_CompAndWriteRequest<This>;
  using ImageCache<ImageCtxT>::m_cache_state;
  ReplicatedWriteLog(ImageCtxT &image_ctx, ImageCacheState<ImageCtxT>* cache_state);
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
  void get_state(bool &clean, bool &empty, bool &present);

  void flush(bool invalidate, bool discard_unflushed_writes, Context *on_finish);
  void flush(Context *on_finish) override;

  void invalidate(bool discard_unflushed_writes, Context *on_finish) override;

private:
  friend class rwl::SyncPoint<This>;
  friend class rwl::GenericLogOperation<This>;
  friend class rwl::GeneralWriteLogOperation<This>;
  friend class rwl::WriteLogOperation<This>;
  friend class rwl::WriteLogOperationSet<This>;
  friend class rwl::DiscardLogOperation<This>;
  friend class rwl::WriteSameLogOperation<This>;
  friend class rwl::SyncPointLogOperation<This>;
  friend struct rwl::C_GuardedBlockIORequest<This>;
  friend struct C_BlockIORequest<This>;
  friend struct C_WriteRequest<This>;
  friend struct C_FlushRequest<This>;
  friend struct C_DiscardRequest<This>;
  friend struct C_WriteSameRequest<This>;
  friend struct C_CompAndWriteRequest<This>;
  typedef std::list<C_WriteRequest<This> *> C_WriteRequests;
  typedef std::list<C_BlockIORequest<This> *> C_BlockIORequests;

  BlockGuardCell* detain_guarded_request_helper(GuardedRequest &req);
  BlockGuardCell* detain_guarded_request_barrier_helper(GuardedRequest &req);
  void detain_guarded_request(GuardedRequest &&req);
  void release_guarded_request(BlockGuardCell *cell);

  std::atomic<bool> m_initialized = {false};
  std::atomic<bool> m_shutting_down = {false};
  std::atomic<bool> m_invalidating = {false};
  ReplicatedWriteLogInternal *m_internal;
  const char* rwl_pool_layout_name;

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
  WriteLogGuard m_write_log_guard;

  /*
   * When m_first_free_entry == m_first_valid_entry, the log is
   * empty. There is always at least one free entry, which can't be
   * used.
   */
  uint64_t m_first_free_entry = 0;  /* Entries from here to m_first_valid_entry-1 are free */
  uint64_t m_first_valid_entry = 0; /* Entries from here to m_first_free_entry-1 are valid */

  /* Starts at 0 for a new write log. Incremented on every flush. */
  uint64_t m_current_sync_gen = 0;
  std::shared_ptr<SyncPointT> m_current_sync_point = nullptr;
  /* Starts at 0 on each sync gen increase. Incremented before applied
     to an operation */
  uint64_t m_last_op_sequence_num = 0;
  /* All writes bearing this and all prior sync gen numbers are flushed */
  uint64_t m_flushed_sync_gen = 0;

  bool m_persist_on_write_until_flush = true;
  /* True if it's safe to complete a user request in persist-on-flush
   * mode before the write is persisted. This is only true if there is
   * a local copy of the write data, or if local write failure always
   * causes local node failure. */
  bool m_persist_on_flush_early_user_comp = false; /* Assume local write failure does not cause node failure */
  bool m_persist_on_flush = false; /* If false, persist each write before completion */
  bool m_flush_seen = false;

  AsyncOpTracker m_async_op_tracker;
  /* Debug counters for the places m_async_op_tracker is used */
  std::atomic<int> m_async_flush_ops = {0};
  std::atomic<int> m_async_append_ops = {0};
  std::atomic<int> m_async_complete_ops = {0};
  std::atomic<int> m_async_null_flush_finish = {0};
  std::atomic<int> m_async_process_work = {0};

  /* Acquire locks in order declared here */

  mutable ceph::mutex m_timer_lock; /* Used with and by m_timer */
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
  /* Used in WriteLogEntry::get_pmem_bl() to syncronize between threads making entries readable */
  mutable ceph::mutex m_entry_bl_lock;

  /* Use m_blockguard_lock for the following 3 things */
  WriteLogGuard::BlockOperations m_awaiting_barrier;
  bool m_barrier_in_progress = false;
  BlockGuardCell *m_barrier_cell = nullptr;

  bool m_wake_up_requested = false;
  bool m_wake_up_scheduled = false;
  bool m_wake_up_enabled = true;
  bool m_appending = false;
  bool m_dispatching_deferred_ops = false;

  Contexts m_flush_complete_contexts;
  Finisher m_persist_finisher;
  Finisher m_log_append_finisher;
  Finisher m_on_persist_finisher;

  GenericLogOperationsT m_ops_to_flush; /* Write ops needing flush in local log */
  GenericLogOperationsT m_ops_to_append; /* Write ops needing event append in local log */

  WriteLogMap m_blocks_to_log_entries;

  /* New entries are at the back. Oldest at the front */
  GenericLogEntries m_log_entries;
  GenericLogEntries m_dirty_log_entries;

  int m_flush_ops_in_flight = 0;
  int m_flush_bytes_in_flight = 0;
  uint64_t m_lowest_flushing_sync_gen = 0;

  /* Writes that have left the block guard, but are waiting for resources */
  C_BlockIORequests m_deferred_ios;
  /* Throttle writes concurrently allocating & replicating */
  unsigned int m_free_lanes = MAX_CONCURRENT_WRITES;
  unsigned int m_unpublished_reserves = 0;
  PerfCounters *m_perfcounter = nullptr;

  /* Initialized from config, then set false during shutdown */
  std::atomic<bool> m_periodic_stats_enabled = {false};
  SafeTimer m_timer; /* Used with m_timer_lock */

  ThreadPool m_thread_pool;
  ContextWQ m_work_queue;

  const bool m_flush_on_close;
  const bool m_retire_on_close;

  /* Returned by get_state() */
  std::atomic<bool> m_clean = {false};
  std::atomic<bool> m_empty = {false};
  std::atomic<bool> m_present = {true};

  const typename ImageCache<ImageCtxT>::Extent whole_volume_extent(void);
  void perf_start(const std::string name);
  void perf_stop();
  void log_perf();
  void periodic_stats();
  void arm_periodic_stats();

  void rwl_init(Context *on_finish, DeferredContexts &later);
  void update_image_cache_state(Context *on_finish);
  void start_workers();
  void load_existing_entries(DeferredContexts &later);
  void wake_up();
  void process_work();

  void flush_dirty_entries(Context *on_finish);
  bool can_flush_entry(const std::shared_ptr<GenericLogEntry> log_entry);
  Context *construct_flush_entry_ctx(const std::shared_ptr<GenericLogEntry> log_entry);
  void persist_last_flushed_sync_gen(void);
  bool handle_flushed_sync_point(std::shared_ptr<SyncPointLogEntry> log_entry);
  void sync_point_writer_flushed(std::shared_ptr<SyncPointLogEntry> log_entry);
  void process_writeback_dirty_entries();
  bool can_retire_entry(const std::shared_ptr<GenericLogEntry> log_entry);
  bool retire_entries(const unsigned long int frees_per_tx = MAX_FREE_PER_TRANSACTION);

  void init_flush_new_sync_point(DeferredContexts &later);
  void new_sync_point(DeferredContexts &later);
  C_FlushRequest<ReplicatedWriteLog<ImageCtxT>>* make_flush_req(Context *on_finish);
  void flush_new_sync_point(C_FlushRequestT *flush_req, DeferredContexts &later);
  void flush_new_sync_point_if_needed(C_FlushRequestT *flush_req, DeferredContexts &later);

  void dispatch_deferred_writes(void);
  void release_write_lanes(C_WriteRequestT *write_req);
  void alloc_and_dispatch_io_req(C_BlockIORequestT *write_req);
  void append_scheduled_ops(void);
  void enlist_op_appender();
  void schedule_append(GenericLogOperationsVectorT &ops);
  void schedule_append(GenericLogOperationsT &ops);
  void schedule_append(GenericLogOperationSharedPtrT op);
  void flush_then_append_scheduled_ops(void);
  void enlist_op_flusher();
  void schedule_flush_and_append(GenericLogOperationsVectorT &ops);
  template <typename V>
  void flush_pmem_buffer(V& ops);
  void alloc_op_log_entries(GenericLogOperationsT &ops);
  void flush_op_log_entries(GenericLogOperationsVectorT &ops);
  int append_op_log_entries(GenericLogOperationsT &ops);
  void complete_op_log_entries(GenericLogOperationsT &&ops, const int r);
  void schedule_complete_op_log_entries(GenericLogOperationsT &&ops, const int r);
  void internal_flush(Context *on_finish, bool invalidate=false, bool discard_unflushed_writes=false);
};

} // namespace cache
} // namespace librbd

extern template class librbd::cache::ReplicatedWriteLog<librbd::ImageCtx>;
extern template class librbd::cache::ImageCacheStateRWL<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG

/* Local Variables: */
/* eval: (c-set-offset 'innamespace 0) */
/* End: */
