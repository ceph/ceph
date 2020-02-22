// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Request.h"
#include "librbd/BlockGuard.h"
#include "librbd/cache/rwl/LogEntry.h"

#define dout_subsys ceph_subsys_rbd_rwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::rwl::Request: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace rwl {

typedef std::list<std::shared_ptr<GenericWriteLogEntry>> GenericWriteLogEntries;

template <typename T>
C_BlockIORequest<T>::C_BlockIORequest(T &rwl, const utime_t arrived, io::Extents &&extents,
                                      bufferlist&& bl, const int fadvise_flags, Context *user_req)
  : rwl(rwl), image_extents(std::move(extents)),
    bl(std::move(bl)), fadvise_flags(fadvise_flags),
    user_req(user_req), image_extents_summary(image_extents), m_arrived_time(arrived) {
  ldout(rwl.get_context(), 99) << this << dendl;
}

template <typename T>
C_BlockIORequest<T>::~C_BlockIORequest() {
  ldout(rwl.get_context(), 99) << this << dendl;
  ceph_assert(m_cell_released || !m_cell);
}

template <typename T>
std::ostream &operator<<(std::ostream &os,
                         const C_BlockIORequest<T> &req) {
   os << "image_extents=[" << req.image_extents << "], "
      << "image_extents_summary=[" << req.image_extents_summary << "], "
      << "bl=" << req.bl << ", "
      << "user_req=" << req.user_req << ", "
      << "m_user_req_completed=" << req.m_user_req_completed << ", "
      << "m_deferred=" << req.m_deferred << ", "
      << "detained=" << req.detained << ", "
      << "waited_lanes=" << req.waited_lanes << ", "
      << "waited_entries=" << req.waited_entries << ", "
      << "waited_buffers=" << req.waited_buffers << "";
   return os;
}

template <typename T>
void C_BlockIORequest<T>::set_cell(BlockGuardCell *cell) {
  ldout(rwl.get_context(), 20) << this << " cell=" << cell << dendl;
  ceph_assert(cell);
  ceph_assert(!m_cell);
  m_cell = cell;
}

template <typename T>
BlockGuardCell *C_BlockIORequest<T>::get_cell(void) {
  ldout(rwl.get_context(), 20) << this << " cell=" << m_cell << dendl;
  return m_cell;
}

template <typename T>
void C_BlockIORequest<T>::release_cell() {
  ldout(rwl.get_context(), 20) << this << " cell=" << m_cell << dendl;
  ceph_assert(m_cell);
  bool initial = false;
  if (m_cell_released.compare_exchange_strong(initial, true)) {
    rwl.release_guarded_request(m_cell);
  } else {
    ldout(rwl.get_context(), 5) << "cell " << m_cell << " already released for " << this << dendl;
  }
}

template <typename T>
void C_BlockIORequest<T>::complete_user_request(int r) {
  bool initial = false;
  if (m_user_req_completed.compare_exchange_strong(initial, true)) {
    ldout(rwl.get_context(), 15) << this << " completing user req" << dendl;
    m_user_req_completed_time = ceph_clock_now();
    user_req->complete(r);
    // Set user_req as null as it is deleted
    user_req = nullptr;
  } else {
    ldout(rwl.get_context(), 20) << this << " user req already completed" << dendl;
  }
}

template <typename T>
void C_BlockIORequest<T>::finish(int r) {
  ldout(rwl.get_context(), 20) << this << dendl;

  complete_user_request(r);
  bool initial = false;
  if (m_finish_called.compare_exchange_strong(initial, true)) {
    ldout(rwl.get_context(), 15) << this << " finishing" << dendl;
    finish_req(0);
  } else {
    ldout(rwl.get_context(), 20) << this << " already finished" << dendl;
    ceph_assert(0);
  }
}

template <typename T>
void C_BlockIORequest<T>::deferred() {
  bool initial = false;
  if (m_deferred.compare_exchange_strong(initial, true)) {
    deferred_handler();
  }
}

template <typename T>
C_WriteRequest<T>::C_WriteRequest(T &rwl, const utime_t arrived, io::Extents &&image_extents,
                                  bufferlist&& bl, const int fadvise_flags, ceph::mutex &lock,
                                  PerfCounters *perfcounter, Context *user_req)
  : C_BlockIORequest<T>(rwl, arrived, std::move(image_extents), std::move(bl), fadvise_flags, user_req),
    m_lock(lock), m_perfcounter(perfcounter) {
  ldout(rwl.get_context(), 99) << this << dendl;
}

template <typename T>
C_WriteRequest<T>::~C_WriteRequest() {
  ldout(rwl.get_context(), 99) << this << dendl;
}

template <typename T>
std::ostream &operator<<(std::ostream &os,
                         const C_WriteRequest<T> &req) {
  os << (C_BlockIORequest<T>&)req
     << " m_resources.allocated=" << req.m_resources.allocated;
  if (req.op_set) {
     os << "op_set=" << *req.op_set;
  }
  return os;
};

template <typename T>
void C_WriteRequest<T>::blockguard_acquired(GuardedRequestFunctionContext &guard_ctx) {
  ldout(rwl.get_context(), 20) << __func__ << " write_req=" << this << " cell=" << guard_ctx.cell << dendl;

  ceph_assert(guard_ctx.cell);
  this->detained = guard_ctx.state.detained; /* overlapped */
  this->m_queued = guard_ctx.state.queued; /* queued behind at least one barrier */
  this->set_cell(guard_ctx.cell);
}

template <typename T>
void C_WriteRequest<T>::finish_req(int r) {
  ldout(rwl.get_context(), 15) << "write_req=" << this << " cell=" << this->get_cell() << dendl;

  /* Completed to caller by here (in finish(), which calls this) */
  utime_t now = ceph_clock_now();
  rwl.release_write_lanes(this);
  ceph_assert(m_resources.allocated);
  m_resources.allocated = false;
  this->release_cell(); /* TODO: Consider doing this in appending state */
  update_req_stats(now);
}

template <typename T>
void C_WriteRequest<T>::setup_buffer_resources(
    uint64_t &bytes_cached, uint64_t &bytes_dirtied, uint64_t &bytes_allocated,
    uint64_t &number_lanes, uint64_t &number_log_entries,
    uint64_t &number_unpublished_reserves) {

  ceph_assert(!m_resources.allocated);

  auto image_extents_size = this->image_extents.size();
  m_resources.buffers.reserve(image_extents_size);

  bytes_cached = 0;
  bytes_allocated = 0;
  number_lanes = image_extents_size;
  number_log_entries = image_extents_size;
  number_unpublished_reserves = image_extents_size;

  for (auto &extent : this->image_extents) {
    m_resources.buffers.emplace_back();
    struct WriteBufferAllocation &buffer = m_resources.buffers.back();
    buffer.allocation_size = MIN_WRITE_ALLOC_SIZE;
    buffer.allocated = false;
    bytes_cached += extent.second;
    if (extent.second > buffer.allocation_size) {
      buffer.allocation_size = extent.second;
    }
    bytes_allocated += buffer.allocation_size;
  }
  bytes_dirtied = bytes_cached;
}

template <typename T>
void C_WriteRequest<T>::setup_log_operations() {
  {
    std::lock_guard locker(m_lock);
    // TODO: Add sync point if necessary
    std::shared_ptr<SyncPoint> current_sync_point = rwl.get_current_sync_point();
    uint64_t current_sync_gen = rwl.get_current_sync_gen();
    op_set =
      make_unique<WriteLogOperationSet>(this->m_dispatched_time,
                                        m_perfcounter,
                                        current_sync_point,
                                        rwl.get_persist_on_flush(),
                                        rwl.get_context(), this);
    ldout(rwl.get_context(), 20) << "write_req=" << *this << " op_set=" << op_set.get() << dendl;
    ceph_assert(m_resources.allocated);
    /* op_set->operations initialized differently for plain write or write same */
    auto allocation = m_resources.buffers.begin();
    uint64_t buffer_offset = 0;
    for (auto &extent : this->image_extents) {
      /* operation->on_write_persist connected to m_prior_log_entries_persisted Gather */
      auto operation =
        std::make_shared<WriteLogOperation>(*op_set, extent.first, extent.second, rwl.get_context());
      op_set->operations.emplace_back(operation);

      /* A WS is also a write */
      ldout(rwl.get_context(), 20) << "write_req=" << *this << " op_set=" << op_set.get()
                                   << " operation=" << operation << dendl;
      rwl.inc_last_op_sequence_num();
      operation->init(true, allocation, current_sync_gen,
                      rwl.get_last_op_sequence_num(), this->bl, buffer_offset, op_set->persist_on_flush);
      buffer_offset += operation->log_entry->write_bytes();
      ldout(rwl.get_context(), 20) << "operation=[" << *operation << "]" << dendl;
      allocation++;
    }
  }
    /* All extent ops subs created */
  op_set->extent_ops_appending->activate();
  op_set->extent_ops_persist->activate();

  /* Write data */
  for (auto &operation : op_set->operations) {
    operation->copy_bl_to_pmem_buffer();
  }
}

template <typename T>
bool C_WriteRequest<T>::append_write_request(std::shared_ptr<SyncPoint> sync_point) {
  std::lock_guard locker(m_lock);
  auto write_req_sp = this;
  if (sync_point->earlier_sync_point) {
    Context *schedule_append_ctx = new LambdaContext([this, write_req_sp](int r) {
        write_req_sp->schedule_append();
      });
    sync_point->earlier_sync_point->on_sync_point_appending.push_back(schedule_append_ctx);
    return true;
  }
  return false;
}

template <typename T>
void C_WriteRequest<T>::schedule_append() {
  ceph_assert(++m_appended == 1);
  if (m_do_early_flush) {
    /* This caller is waiting for persist, so we'll use their thread to
     * expedite it */
    rwl.flush_pmem_buffer(this->op_set->operations);
    rwl.schedule_append(this->op_set->operations);
  } else {
    /* This is probably not still the caller's thread, so do the payload
     * flushing/replicating later. */
    rwl.schedule_flush_and_append(this->op_set->operations);
  }
}

/**
 * Attempts to allocate log resources for a write. Returns true if successful.
 *
 * Resources include 1 lane per extent, 1 log entry per extent, and the payload
 * data space for each extent.
 *
 * Lanes are released after the write persists via release_write_lanes()
 */
template <typename T>
bool C_WriteRequest<T>::alloc_resources() {
  this->allocated_time = ceph_clock_now();
  return rwl.alloc_resources(this);
}

/**
 * Takes custody of write_req. Resources must already be allocated.
 *
 * Locking:
 * Acquires lock
 */
template <typename T>
void C_WriteRequest<T>::dispatch()
{
  CephContext *cct = rwl.get_context();
  utime_t now = ceph_clock_now();
  this->m_dispatched_time = now;

  ldout(cct, 15) << "write_req=" << this << " cell=" << this->get_cell() << dendl;
  setup_log_operations();

  bool append_deferred = false;
  if (!op_set->persist_on_flush &&
      append_write_request(op_set->sync_point)) {
    /* In persist-on-write mode, we defer the append of this write until the
     * previous sync point is appending (meaning all the writes before it are
     * persisted and that previous sync point can now appear in the
     * log). Since we insert sync points in persist-on-write mode when writes
     * have already completed to the current sync point, this limits us to
     * one inserted sync point in flight at a time, and gives the next
     * inserted sync point some time to accumulate a few writes if they
     * arrive soon. Without this we can insert an absurd number of sync
     * points, each with one or two writes. That uses a lot of log entries,
     * and limits flushing to very few writes at a time. */
    m_do_early_flush = false;
    append_deferred = true;
  } else {
    /* The prior sync point is done, so we'll schedule append here. If this is
     * persist-on-write, and probably still the caller's thread, we'll use this
     * caller's thread to perform the persist & replication of the payload
     * buffer. */
    m_do_early_flush =
      !(this->detained || this->m_queued || this->m_deferred || op_set->persist_on_flush);
  }
  if (!append_deferred) {
    this->schedule_append();
  }
}

std::ostream &operator<<(std::ostream &os,
                         const BlockGuardReqState &r) {
  os << "barrier=" << r.barrier << ", "
     << "current_barrier=" << r.current_barrier << ", "
     << "detained=" << r.detained << ", "
     << "queued=" << r.queued;
  return os;
};

GuardedRequestFunctionContext::GuardedRequestFunctionContext(boost::function<void(GuardedRequestFunctionContext&)> &&callback)
  : m_callback(std::move(callback)){ }

GuardedRequestFunctionContext::~GuardedRequestFunctionContext(void) { }

void GuardedRequestFunctionContext::finish(int r) {
  ceph_assert(cell);
  m_callback(*this);
}

std::ostream &operator<<(std::ostream &os,
                         const GuardedRequest &r) {
  os << "guard_ctx->state=[" << r.guard_ctx->state << "], "
     << "block_extent.block_start=" << r.block_extent.block_start << ", "
     << "block_extent.block_start=" << r.block_extent.block_end;
  return os;
};

} // namespace rwl
} // namespace cache
} // namespace librbd
