// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_RWL_REQUEST_H 
#define CEPH_LIBRBD_CACHE_RWL_REQUEST_H 

#include "include/Context.h"
#include "librbd/cache/ImageCache.h"
#include "librbd/cache/rwl/Types.h"
#include "librbd/cache/rwl/LogOperation.h"

namespace librbd {
class BlockGuardCell;

namespace cache {
namespace rwl {

class GuardedRequestFunctionContext;

struct WriteRequestResources {
  bool allocated = false;
  std::vector<WriteBufferAllocation> buffers;
};

/**
 * A request that can be deferred in a BlockGuard to sequence
 * overlapping operations.
 * This is the custodian of the BlockGuard cell for this IO, and the
 * state information about the progress of this IO. This object lives
 * until the IO is persisted in all (live) log replicas.  User request
 * may be completed from here before the IO persists.
 */
template <typename T>
class C_BlockIORequest : public Context {
public:
  T &rwl;
  io::Extents image_extents;
  bufferlist bl;
  int fadvise_flags;
  Context *user_req; /* User write request */
  ExtentsSummary<io::Extents> image_extents_summary;
  bool detained = false;                /* Detained in blockguard (overlapped with a prior IO) */
  utime_t allocated_time;               /* When allocation began */
  bool waited_lanes = false;            /* This IO waited for free persist/replicate lanes */
  bool waited_entries = false;          /* This IO waited for free log entries */
  bool waited_buffers = false;          /* This IO waited for data buffers (pmemobj_reserve() failed) */

  C_BlockIORequest(T &rwl, const utime_t arrived, io::Extents &&extents,
                   bufferlist&& bl, const int fadvise_flags, Context *user_req);
  ~C_BlockIORequest() override;
  C_BlockIORequest(const C_BlockIORequest&) = delete;
  C_BlockIORequest &operator=(const C_BlockIORequest&) = delete;

  void set_cell(BlockGuardCell *cell);
  BlockGuardCell *get_cell(void);
  void release_cell();

  void complete_user_request(int r);
  void finish(int r);
  virtual void finish_req(int r) = 0;

  virtual bool alloc_resources() = 0;

  void deferred();

  virtual void deferred_handler() = 0;

  virtual void dispatch()  = 0;

  virtual const char *get_name() const {
    return "C_BlockIORequest";
  }
  uint64_t get_image_extents_size() {
    return image_extents.size();
  }
  void set_io_waited_for_lanes(bool waited) {
    waited_lanes = waited;
  }
  void set_io_waited_for_entries(bool waited) {
    waited_entries = waited;
  }
  void set_io_waited_for_buffers(bool waited) {
    waited_buffers = waited;
  }
  bool has_io_waited_for_buffers() {
    return waited_buffers;
  }
  std::vector<WriteBufferAllocation>& get_resources_buffers() {
    return m_resources.buffers;
  }

  void set_allocated(bool allocated) {
    if (allocated) {
      m_resources.allocated = true;
    } else {
      m_resources.buffers.clear();
    }
  }

  virtual void setup_buffer_resources(
      uint64_t &bytes_cached, uint64_t &bytes_dirtied, uint64_t &bytes_allocated,
      uint64_t &number_lanes, uint64_t &number_log_entries,
      uint64_t &number_unpublished_reserves) {};

protected:
  utime_t m_arrived_time;
  utime_t m_dispatched_time;              /* When dispatch began */
  utime_t m_user_req_completed_time;
  std::atomic<bool> m_deferred = {false}; /* Deferred because this or a prior IO had to wait for write resources */
  WriteRequestResources m_resources;

private:
  std::atomic<bool> m_user_req_completed = {false};
  std::atomic<bool> m_finish_called = {false};
  std::atomic<bool> m_cell_released = {false};
  BlockGuardCell* m_cell = nullptr;

  template <typename U>
  friend std::ostream &operator<<(std::ostream &os,
                                  const C_BlockIORequest<U> &req);
};

/**
 * This is the custodian of the BlockGuard cell for this write. Block
 * guard is not released until the write persists everywhere (this is
 * how we guarantee to each log replica that they will never see
 * overlapping writes).
 */
template <typename T>
class C_WriteRequest : public C_BlockIORequest<T> {
public:
  using C_BlockIORequest<T>::rwl;
  unique_ptr<WriteLogOperationSet> op_set = nullptr;

  C_WriteRequest(T &rwl, const utime_t arrived, io::Extents &&image_extents,
                 bufferlist&& bl, const int fadvise_flags, ceph::mutex &lock,
                 PerfCounters *perfcounter, Context *user_req);

  ~C_WriteRequest() override;

  void blockguard_acquired(GuardedRequestFunctionContext &guard_ctx);

  /* Common finish to plain write and compare-and-write (if it writes) */
  void finish_req(int r) override;

  /* Compare and write will override this */
  virtual void update_req_stats(utime_t &now) {
    // TODO: Add in later PRs
  }
  bool alloc_resources() override;

  void deferred_handler() override { }

  void dispatch() override;

  virtual std::shared_ptr<WriteLogOperation> create_operation(uint64_t offset, uint64_t len);

  virtual void setup_log_operations(DeferredContexts &on_exit);

  bool append_write_request(std::shared_ptr<SyncPoint> sync_point);

  virtual void schedule_append();

  const char *get_name() const override {
    return "C_WriteRequest";
  }

protected:
  using C_BlockIORequest<T>::m_resources;
  PerfCounters *m_perfcounter = nullptr;
  /* Plain writes will allocate one buffer per request extent */
  void setup_buffer_resources(
      uint64_t &bytes_cached, uint64_t &bytes_dirtied, uint64_t &bytes_allocated,
      uint64_t &number_lanes, uint64_t &number_log_entries,
      uint64_t &number_unpublished_reserves) override;

private:
  bool m_do_early_flush = false;
  std::atomic<int> m_appended = {0};
  bool m_queued = false;
  ceph::mutex &m_lock;
  template <typename U>
  friend std::ostream &operator<<(std::ostream &os,
                                  const C_WriteRequest<U> &req);
};

/**
 * This is the custodian of the BlockGuard cell for this
 * aio_flush. Block guard is released as soon as the new
 * sync point (if required) is created. Subsequent IOs can
 * proceed while this flush waits for prior IOs to complete
 * and any required sync points to be persisted.
 */
template <typename T>
class C_FlushRequest : public C_BlockIORequest<T> {
public:
  using C_BlockIORequest<T>::rwl;
  bool internal = false;
  std::shared_ptr<SyncPoint> to_append;

  C_FlushRequest(T &rwl, const utime_t arrived,
                 io::Extents &&image_extents,
                 bufferlist&& bl, const int fadvise_flags,
                 ceph::mutex &lock, PerfCounters *perfcounter,
                 Context *user_req);

  ~C_FlushRequest() override {}

  bool alloc_resources() override;

  void dispatch() override;

  const char *get_name() const override {
    return "C_FlushRequest";
  }

  void setup_buffer_resources(
      uint64_t &bytes_cached, uint64_t &bytes_dirtied, uint64_t &bytes_allocated,
      uint64_t &number_lanes, uint64_t &number_log_entries,
      uint64_t &number_unpublished_reserves) override;
private:
  std::shared_ptr<SyncPointLogOperation> op;
  ceph::mutex &m_lock;
  PerfCounters *m_perfcounter = nullptr;

  void finish_req(int r) override;
  void deferred_handler() override {
    m_perfcounter->inc(l_librbd_rwl_aio_flush_def, 1);
  }

  template <typename U>
  friend std::ostream &operator<<(std::ostream &os,
                                  const C_FlushRequest<U> &req);
};

class C_ReadRequest : public Context {
public:
  io::Extents miss_extents; // move back to caller
  ImageExtentBufs read_extents;
  bufferlist miss_bl;

  C_ReadRequest(CephContext *cct, utime_t arrived, PerfCounters *perfcounter, bufferlist *out_bl, Context *on_finish)
    : m_cct(cct), m_on_finish(on_finish), m_out_bl(out_bl),
      m_arrived_time(arrived), m_perfcounter(perfcounter) {}
  ~C_ReadRequest() {}

  void finish(int r) override;

  const char *get_name() const {
    return "C_ReadRequest";
  }

private:
  CephContext *m_cct;
  Context *m_on_finish;
  bufferlist *m_out_bl;
  utime_t m_arrived_time;
  PerfCounters *m_perfcounter;
};

/**
 * This is the custodian of the BlockGuard cell for this discard. As in the
 * case of write, the block guard is not released until the discard persists
 * everywhere.
 */
template <typename T>
class C_DiscardRequest : public C_BlockIORequest<T> {
public:
  using C_BlockIORequest<T>::rwl;
  std::shared_ptr<DiscardLogOperation> op;

  C_DiscardRequest(T &rwl, const utime_t arrived, io::Extents &&image_extents,
                   uint32_t discard_granularity_bytes, ceph::mutex &lock,
                   PerfCounters *perfcounter, Context *user_req);

  ~C_DiscardRequest() override;
  void finish_req(int r) override {}

  bool alloc_resources() override;

  void deferred_handler() override { }

  void setup_log_operations();

  void dispatch() override;

  void blockguard_acquired(GuardedRequestFunctionContext &guard_ctx);

  const char *get_name() const override {
    return "C_DiscardRequest";
  }
  void setup_buffer_resources(
      uint64_t &bytes_cached, uint64_t &bytes_dirtied, uint64_t &bytes_allocated,
      uint64_t &number_lanes, uint64_t &number_log_entries,
      uint64_t &number_unpublished_reserves) override;
private:
  uint32_t m_discard_granularity_bytes;
  ceph::mutex &m_lock;
  PerfCounters *m_perfcounter = nullptr;
  template <typename U>
  friend std::ostream &operator<<(std::ostream &os,
                                  const C_DiscardRequest<U> &req);
};

/**
 * This is the custodian of the BlockGuard cell for this write same.
 *
 * A writesame allocates and persists a data buffer like a write, but the
 * data buffer is usually much shorter than the write same.
 */
template <typename T>
class C_WriteSameRequest : public C_WriteRequest<T> {
public:
  using C_BlockIORequest<T>::rwl;
  C_WriteSameRequest(T &rwl, const utime_t arrived, io::Extents &&image_extents,
                     bufferlist&& bl, const int fadvise_flags, ceph::mutex &lock,
                     PerfCounters *perfcounter, Context *user_req);

  ~C_WriteSameRequest() override;

  void update_req_stats(utime_t &now) override;

  void setup_buffer_resources(
      uint64_t &bytes_cached, uint64_t &bytes_dirtied, uint64_t &bytes_allocated,
      uint64_t &number_lanes, uint64_t &number_log_entries,
      uint64_t &number_unpublished_reserves) override;

  std::shared_ptr<WriteLogOperation> create_operation(uint64_t offset, uint64_t len) override;

  const char *get_name() const override {
    return "C_WriteSameRequest";
  }

  template<typename U>
  friend std::ostream &operator<<(std::ostream &os,
                                  const C_WriteSameRequest<U> &req);
};

/**
 * This is the custodian of the BlockGuard cell for this compare and write. The
 * block guard is acquired before the read begins to guarantee atomicity of this
 * operation.  If this results in a write, the block guard will be released
 * when the write completes to all replicas.
 */
template <typename T>
class C_CompAndWriteRequest : public C_WriteRequest<T> {
public:
  using C_BlockIORequest<T>::rwl;
  bool compare_succeeded = false;
  uint64_t *mismatch_offset;
  bufferlist cmp_bl;
  bufferlist read_bl;
  C_CompAndWriteRequest(T &rwl, const utime_t arrived, io::Extents &&image_extents,
                        bufferlist&& cmp_bl, bufferlist&& bl, uint64_t *mismatch_offset,
                        int fadvise_flags, ceph::mutex &lock, PerfCounters *perfcounter,
                        Context *user_req);
  ~C_CompAndWriteRequest();

  void finish_req(int r) override;

  void update_req_stats(utime_t &now) override;

  /*
   * Compare and write doesn't implement alloc_resources(), deferred_handler(),
   * or dispatch(). We use the implementation in C_WriteRequest(), and only if the
   * compare phase succeeds and a write is actually performed.
   */

  const char *get_name() const override {
    return "C_CompAndWriteRequest";
  }
  template <typename U>
  friend std::ostream &operator<<(std::ostream &os,
                                  const C_CompAndWriteRequest<U> &req);
};

struct BlockGuardReqState {
  bool barrier = false; /* This is a barrier request */
  bool current_barrier = false; /* This is the currently active barrier */
  bool detained = false;
  bool queued = false; /* Queued for barrier */
  friend std::ostream &operator<<(std::ostream &os,
                                  const BlockGuardReqState &r);
};

class GuardedRequestFunctionContext : public Context {
public:
  BlockGuardCell *cell = nullptr;
  BlockGuardReqState state;
  GuardedRequestFunctionContext(boost::function<void(GuardedRequestFunctionContext&)> &&callback);
  ~GuardedRequestFunctionContext(void) override;
  GuardedRequestFunctionContext(const GuardedRequestFunctionContext&) = delete;
  GuardedRequestFunctionContext &operator=(const GuardedRequestFunctionContext&) = delete;

private:
  boost::function<void(GuardedRequestFunctionContext&)> m_callback;
  void finish(int r) override;
};

class GuardedRequest {
public:
  const BlockExtent block_extent;
  GuardedRequestFunctionContext *guard_ctx; /* Work to do when guard on range obtained */

  GuardedRequest(const BlockExtent block_extent,
                 GuardedRequestFunctionContext *on_guard_acquire, bool barrier = false)
    : block_extent(block_extent), guard_ctx(on_guard_acquire) {
    guard_ctx->state.barrier = barrier;
  }
  friend std::ostream &operator<<(std::ostream &os,
                                  const GuardedRequest &r);
};

} // namespace rwl
} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_RWL_REQUEST_H
