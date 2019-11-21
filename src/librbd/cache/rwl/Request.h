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

/**
 * A request that can be deferred in a BlockGuard to sequence
 * overlapping operations.
 */
template <typename T>
class C_GuardedBlockIORequest : public Context {
public:
  T &rwl;
  C_GuardedBlockIORequest(T &rwl);
  ~C_GuardedBlockIORequest();
  C_GuardedBlockIORequest(const C_GuardedBlockIORequest&) = delete;
  C_GuardedBlockIORequest &operator=(const C_GuardedBlockIORequest&) = delete;

  virtual const char *get_name() const = 0;
  void set_cell(BlockGuardCell *cell);
  BlockGuardCell *get_cell(void);
  void release_cell();
  
private:
  std::atomic<bool> m_cell_released = {false};
  BlockGuardCell* m_cell = nullptr;
};

/**
 * This is the custodian of the BlockGuard cell for this IO, and the
 * state information about the progress of this IO. This object lives
 * until the IO is persisted in all (live) log replicas.  User request
 * may be completed from here before the IO persists.
 */
template <typename T>
class C_BlockIORequest : public C_GuardedBlockIORequest<T> {
public:
  using C_GuardedBlockIORequest<T>::rwl;

  io::Extents image_extents;
  bufferlist bl;
  int fadvise_flags;
  Context *user_req; /* User write request */
  ExtentsSummary<io::Extents> image_extents_summary;
  bool detained = false;                /* Detained in blockguard (overlapped with a prior IO) */

  C_BlockIORequest(T &rwl, const utime_t arrived, io::Extents &&extents,
                   bufferlist&& bl, const int fadvise_flags, Context *user_req);
  virtual ~C_BlockIORequest();

  void complete_user_request(int r);
  void finish(int r);
  virtual void finish_req(int r) = 0;

  virtual bool alloc_resources() = 0;

  void deferred();

  virtual void deferred_handler() = 0;

  virtual void dispatch()  = 0;

  virtual const char *get_name() const override {
    return "C_BlockIORequest";
  }

protected:
  utime_t m_arrived_time;
  utime_t m_allocated_time;               /* When allocation began */
  utime_t m_dispatched_time;              /* When dispatch began */
  utime_t m_user_req_completed_time;
  bool m_waited_lanes = false;            /* This IO waited for free persist/replicate lanes */
  bool m_waited_entries = false;          /* This IO waited for free log entries */
  bool m_waited_buffers = false;          /* This IO waited for data buffers (pmemobj_reserve() failed) */
  std::atomic<bool> m_deferred = {false}; /* Deferred because this or a prior IO had to wait for write resources */

private:
  std::atomic<bool> m_user_req_completed = {false};
  std::atomic<bool> m_finish_called = {false};

  template <typename U>
  friend std::ostream &operator<<(std::ostream &os,
                                  const C_BlockIORequest<U> &req);
};

struct WriteRequestResources {
  bool allocated = false;
  std::vector<WriteBufferAllocation> buffers;
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
  WriteRequestResources resources;

  C_WriteRequest(T &rwl, const utime_t arrived, io::Extents &&image_extents,
                 bufferlist&& bl, const int fadvise_flags, Context *user_req);

  ~C_WriteRequest();

  void blockguard_acquired(GuardedRequestFunctionContext &guard_ctx);

  /* Common finish to plain write and compare-and-write (if it writes) */
  virtual void finish_req(int r);

  /* Compare and write will override this */
  virtual void update_req_stats(utime_t &now) {
    // TODO: Add in later PRs
  }
  virtual bool alloc_resources() override;

  /* Plain writes will allocate one buffer per request extent */
  virtual void setup_buffer_resources(uint64_t &bytes_cached, uint64_t &bytes_dirtied);

  void deferred_handler() override { }

  void dispatch() override;

  virtual void setup_log_operations();

  virtual void schedule_append();

  const char *get_name() const override {
    return "C_WriteRequest";
  }

private:
  unique_ptr<WriteLogOperationSet<T>> m_op_set = nullptr;
  bool m_do_early_flush = false;
  std::atomic<int> m_appended = {0};
  bool m_queued = false;

  template <typename U>
  friend std::ostream &operator<<(std::ostream &os,
                                  const C_WriteRequest<U> &req);
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
  BlockGuardCell *m_cell = nullptr;
  BlockGuardReqState m_state;
  GuardedRequestFunctionContext(boost::function<void(GuardedRequestFunctionContext&)> &&callback);
  ~GuardedRequestFunctionContext(void);
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
    guard_ctx->m_state.barrier = barrier;
  }
  friend std::ostream &operator<<(std::ostream &os,
                                  const GuardedRequest &r);
};

} // namespace rwl 
} // namespace cache 
} // namespace librbd 

#endif // CEPH_LIBRBD_CACHE_RWL_REQUEST_H 
