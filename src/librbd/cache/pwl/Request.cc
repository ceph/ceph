// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Request.h"
#include "librbd/BlockGuard.h"
#include "librbd/cache/pwl/LogEntry.h"
#include "librbd/cache/pwl/AbstractWriteLog.h"
#include "common/Clock.h" // for ceph_clock_now()
#include "common/debug.h"

#define dout_subsys ceph_subsys_rbd_pwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::pwl::Request: " << this << " " \
                           <<  __func__ << ": "

using namespace std;

namespace librbd {
namespace cache {
namespace pwl {

template <typename T>
C_BlockIORequest<T>::C_BlockIORequest(T &pwl, const utime_t arrived, io::Extents &&extents,
                                      bufferlist&& bl, const int fadvise_flags, Context *user_req)
  : pwl(pwl), image_extents(std::move(extents)),
    bl(std::move(bl)), fadvise_flags(fadvise_flags),
    user_req(user_req), image_extents_summary(image_extents), m_arrived_time(arrived) {
  ldout(pwl.get_context(), 99) << this << dendl;
}

template <typename T>
C_BlockIORequest<T>::~C_BlockIORequest() {
  ldout(pwl.get_context(), 99) << this << dendl;
  ceph_assert(m_cell_released || !m_cell);
}

template <typename T>
std::ostream &operator<<(std::ostream &os,
                         const C_BlockIORequest<T> &req) {
   os << "image_extents=" << req.image_extents
      << ", image_extents_summary=[" << req.image_extents_summary
      << "], bl=" << req.bl
      << ", user_req=" << req.user_req
      << ", m_user_req_completed=" << req.m_user_req_completed
      << ", m_deferred=" << req.m_deferred
      << ", detained=" << req.detained;
   return os;
}

template <typename T>
void C_BlockIORequest<T>::set_cell(BlockGuardCell *cell) {
  ldout(pwl.get_context(), 20) << this << " cell=" << cell << dendl;
  ceph_assert(cell);
  ceph_assert(!m_cell);
  m_cell = cell;
}

template <typename T>
BlockGuardCell *C_BlockIORequest<T>::get_cell(void) {
  ldout(pwl.get_context(), 20) << this << " cell=" << m_cell << dendl;
  return m_cell;
}

template <typename T>
void C_BlockIORequest<T>::release_cell() {
  ldout(pwl.get_context(), 20) << this << " cell=" << m_cell << dendl;
  ceph_assert(m_cell);
  bool initial = false;
  if (m_cell_released.compare_exchange_strong(initial, true)) {
    pwl.release_guarded_request(m_cell);
  } else {
    ldout(pwl.get_context(), 5) << "cell " << m_cell << " already released for " << this << dendl;
  }
}

template <typename T>
void C_BlockIORequest<T>::complete_user_request(int r) {
  bool initial = false;
  if (m_user_req_completed.compare_exchange_strong(initial, true)) {
    ldout(pwl.get_context(), 15) << this << " completing user req" << dendl;
    m_user_req_completed_time = ceph_clock_now();
    pwl.complete_user_request(user_req, r);
  } else {
    ldout(pwl.get_context(), 20) << this << " user req already completed" << dendl;
  }
}

template <typename T>
void C_BlockIORequest<T>::finish(int r) {
  ldout(pwl.get_context(), 20) << this << dendl;

  complete_user_request(r);
  bool initial = false;
  if (m_finish_called.compare_exchange_strong(initial, true)) {
    ldout(pwl.get_context(), 15) << this << " finishing" << dendl;
    finish_req(0);
  } else {
    ldout(pwl.get_context(), 20) << this << " already finished" << dendl;
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
C_WriteRequest<T>::C_WriteRequest(T &pwl, const utime_t arrived, io::Extents &&image_extents,
                                  bufferlist&& bl, const int fadvise_flags, ceph::mutex &lock,
                                  PerfCounters *perfcounter, Context *user_req)
  : C_BlockIORequest<T>(pwl, arrived, std::move(image_extents), std::move(bl), fadvise_flags, user_req),
    m_perfcounter(perfcounter), m_lock(lock) {
  ldout(pwl.get_context(), 99) << this << dendl;
}

template <typename T>
C_WriteRequest<T>::C_WriteRequest(T &pwl, const utime_t arrived, io::Extents &&image_extents,
                                  bufferlist&& cmp_bl, bufferlist&& bl, uint64_t *mismatch_offset,
                                  int fadvise_flags, ceph::mutex &lock, PerfCounters *perfcounter,
                                  Context *user_req)
  : C_BlockIORequest<T>(pwl, arrived, std::move(image_extents), std::move(bl), fadvise_flags, user_req),
  mismatch_offset(mismatch_offset), cmp_bl(std::move(cmp_bl)),
  m_perfcounter(perfcounter), m_lock(lock) {
  is_comp_and_write = true;
  ldout(pwl.get_context(), 20) << dendl;
}

template <typename T>
C_WriteRequest<T>::~C_WriteRequest() {
  ldout(pwl.get_context(), 99) << this << dendl;
}

template <typename T>
std::ostream &operator<<(std::ostream &os,
                         const C_WriteRequest<T> &req) {
  os << (C_BlockIORequest<T>&)req
     << " m_resources.allocated=" << req.m_resources.allocated;
  if (req.op_set) {
     os << " op_set=[" << *req.op_set << "]";
  }
  return os;
}

template <typename T>
void C_WriteRequest<T>::blockguard_acquired(GuardedRequestFunctionContext &guard_ctx) {
  ldout(pwl.get_context(), 20) << __func__ << " write_req=" << this << " cell=" << guard_ctx.cell << dendl;

  ceph_assert(guard_ctx.cell);
  this->detained = guard_ctx.state.detained; /* overlapped */
  this->m_queued = guard_ctx.state.queued; /* queued behind at least one barrier */
  this->set_cell(guard_ctx.cell);
}

template <typename T>
void C_WriteRequest<T>::finish_req(int r) {
  ldout(pwl.get_context(), 15) << "write_req=" << this << " cell=" << this->get_cell() << dendl;

  /* Completed to caller by here (in finish(), which calls this) */
  utime_t now = ceph_clock_now();
  if(is_comp_and_write && !compare_succeeded) {
    update_req_stats(now);
    return;
  }
  pwl.release_write_lanes(this);
  ceph_assert(m_resources.allocated);
  m_resources.allocated = false;
  this->release_cell(); /* TODO: Consider doing this in appending state */
  update_req_stats(now);
}

template <typename T>
std::shared_ptr<WriteLogOperation> C_WriteRequest<T>::create_operation(
    uint64_t offset, uint64_t len) {
  return pwl.m_builder->create_write_log_operation(
      *op_set, offset, len, pwl.get_context(),
      pwl.m_builder->create_write_log_entry(op_set->sync_point->log_entry, offset, len));
}

template <typename T>
void C_WriteRequest<T>::setup_log_operations(DeferredContexts &on_exit) {
  GenericWriteLogEntries log_entries;
  {
    std::lock_guard locker(m_lock);
    std::shared_ptr<SyncPoint> current_sync_point = pwl.get_current_sync_point();
    if ((!pwl.get_persist_on_flush() && current_sync_point->log_entry->writes_completed) ||
        (current_sync_point->log_entry->writes > MAX_WRITES_PER_SYNC_POINT) ||
        (current_sync_point->log_entry->bytes > MAX_BYTES_PER_SYNC_POINT)) {
      /* Create new sync point and persist the previous one. This sequenced
       * write will bear a sync gen number shared with no already completed
       * writes. A group of sequenced writes may be safely flushed concurrently
       * if they all arrived before any of them completed. We'll insert one on
       * an aio_flush() from the application. Here we're inserting one to cap
       * the number of bytes and writes per sync point. When the application is
       * not issuing flushes, we insert sync points to record some observed
       * write concurrency information that enables us to safely issue >1 flush
       * write (for writes observed here to have been in flight simultaneously)
       * at a time in persist-on-write mode.
       */
      pwl.flush_new_sync_point(nullptr, on_exit);
      current_sync_point = pwl.get_current_sync_point();
    }
    uint64_t current_sync_gen = pwl.get_current_sync_gen();
    op_set =
      make_unique<WriteLogOperationSet>(this->m_dispatched_time,
                                        m_perfcounter,
                                        current_sync_point,
                                        pwl.get_persist_on_flush(),
                                        pwl.get_context(), this);
    ldout(pwl.get_context(), 20) << "write_req=[" << *this
                                 << "], op_set=" << op_set.get() << dendl;
    ceph_assert(m_resources.allocated);
    /* op_set->operations initialized differently for plain write or write same */
    auto allocation = m_resources.buffers.begin();
    uint64_t buffer_offset = 0;
    for (auto &extent : this->image_extents) {
      /* operation->on_write_persist connected to m_prior_log_entries_persisted Gather */
      auto operation = this->create_operation(extent.first, extent.second);
      this->op_set->operations.emplace_back(operation);

      /* A WS is also a write */
      ldout(pwl.get_context(), 20) << "write_req=[" << *this
                                   << "], op_set=" << op_set.get()
                                   << ", operation=" << operation << dendl;
      log_entries.emplace_back(operation->log_entry);
      if (!op_set->persist_on_flush) {
        pwl.inc_last_op_sequence_num();
      }
      operation->init(true, allocation, current_sync_gen,
                      pwl.get_last_op_sequence_num(), this->bl, buffer_offset, op_set->persist_on_flush);
      buffer_offset += operation->log_entry->write_bytes();
      ldout(pwl.get_context(), 20) << "operation=[" << *operation << "]" << dendl;
      allocation++;
    }
  }
    /* All extent ops subs created */
  op_set->extent_ops_appending->activate();
  op_set->extent_ops_persist->activate();

  pwl.add_into_log_map(log_entries, this);
}

template <typename T>
void C_WriteRequest<T>::copy_cache() {
  pwl.copy_bl_to_buffer(&m_resources, op_set);
}

template <typename T>
bool C_WriteRequest<T>::append_write_request(std::shared_ptr<SyncPoint> sync_point) {
  std::lock_guard locker(m_lock);
  auto write_req_sp = this;
  if (sync_point->earlier_sync_point) {
    Context *schedule_append_ctx = new LambdaContext([write_req_sp](int r) {
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
  pwl.setup_schedule_append(this->op_set->operations, m_do_early_flush, this);
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
  return pwl.alloc_resources(this);
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
  CephContext *cct = pwl.get_context();
  DeferredContexts on_exit;
  utime_t now = ceph_clock_now();
  this->m_dispatched_time = now;

  ldout(cct, 15) << "write_req=" << this << " cell=" << this->get_cell() << dendl;
  this->setup_log_operations(on_exit);

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

template <typename T>
C_FlushRequest<T>::C_FlushRequest(T &pwl, const utime_t arrived,
                                  io::Extents &&image_extents,
                                  bufferlist&& bl, const int fadvise_flags,
                                  ceph::mutex &lock, PerfCounters *perfcounter,
                                  Context *user_req)
  : C_BlockIORequest<T>(pwl, arrived, std::move(image_extents), std::move(bl),
                        fadvise_flags, user_req),
    m_lock(lock), m_perfcounter(perfcounter) {
  ldout(pwl.get_context(), 20) << this << dendl;
}

template <typename T>
void C_FlushRequest<T>::finish_req(int r) {
  ldout(pwl.get_context(), 20) << "flush_req=" << this
                               << " cell=" << this->get_cell() << dendl;
  /* Block guard already released */
  ceph_assert(!this->get_cell());

  /* Completed to caller by here */
  utime_t now = ceph_clock_now();
  m_perfcounter->tinc(l_librbd_pwl_aio_flush_latency, now - this->m_arrived_time);
}

template <typename T>
bool C_FlushRequest<T>::alloc_resources() {
  ldout(pwl.get_context(), 20) << "req type=" << get_name()
                               << " req=[" << *this << "]" << dendl;
  return pwl.alloc_resources(this);
}

template <typename T>
void C_FlushRequest<T>::dispatch() {
  utime_t now = ceph_clock_now();
  ldout(pwl.get_context(), 20) << "req type=" << get_name()
                               << " req=[" << *this << "]" << dendl;
  ceph_assert(this->m_resources.allocated);
  this->m_dispatched_time = now;

  op = std::make_shared<SyncPointLogOperation>(m_lock,
                                               to_append,
                                               now,
                                               m_perfcounter,
                                               pwl.get_context());

  m_perfcounter->inc(l_librbd_pwl_log_ops, 1);
  pwl.schedule_append(op);
}

template <typename T>
void C_FlushRequest<T>::setup_buffer_resources(
    uint64_t *bytes_cached, uint64_t *bytes_dirtied, uint64_t *bytes_allocated,
    uint64_t *number_lanes, uint64_t *number_log_entries,
    uint64_t *number_unpublished_reserves) {
  *number_log_entries = 1;
}

template <typename T>
std::ostream &operator<<(std::ostream &os,
                         const C_FlushRequest<T> &req) {
  os << (C_BlockIORequest<T>&)req
     << " m_resources.allocated=" << req.m_resources.allocated;
  return os;
}

template <typename T>
C_DiscardRequest<T>::C_DiscardRequest(T &pwl, const utime_t arrived, io::Extents &&image_extents,
                                      uint32_t discard_granularity_bytes, ceph::mutex &lock,
                                      PerfCounters *perfcounter, Context *user_req)
  : C_BlockIORequest<T>(pwl, arrived, std::move(image_extents), bufferlist(), 0, user_req),
  m_discard_granularity_bytes(discard_granularity_bytes),
  m_lock(lock),
  m_perfcounter(perfcounter) {
  ldout(pwl.get_context(), 20) << this << dendl;
}

template <typename T>
C_DiscardRequest<T>::~C_DiscardRequest() {
  ldout(pwl.get_context(), 20) << this << dendl;
}

template <typename T>
bool C_DiscardRequest<T>::alloc_resources() {
  ldout(pwl.get_context(), 20) << "req type=" << get_name()
                               << " req=[" << *this << "]" << dendl;
  return pwl.alloc_resources(this);
}

template <typename T>
void C_DiscardRequest<T>::setup_log_operations() {
  std::lock_guard locker(m_lock);
  GenericWriteLogEntries log_entries;
  for (auto &extent : this->image_extents) {
    op = pwl.m_builder->create_discard_log_operation(
        pwl.get_current_sync_point(), extent.first, extent.second,
        m_discard_granularity_bytes, this->m_dispatched_time, m_perfcounter,
        pwl.get_context());
    log_entries.emplace_back(op->log_entry);
    break;
  }
  uint64_t current_sync_gen = pwl.get_current_sync_gen();
  bool persist_on_flush = pwl.get_persist_on_flush();
  if (!persist_on_flush) {
    pwl.inc_last_op_sequence_num();
  }
  auto discard_req = this;
  Context *on_write_append = pwl.get_current_sync_point()->prior_persisted_gather_new_sub();

  Context *on_write_persist = new LambdaContext(
    [this, discard_req](int r) {
      ldout(pwl.get_context(), 20) << "discard_req=" << discard_req
                                   << " cell=" << discard_req->get_cell() << dendl;
      ceph_assert(discard_req->get_cell());
      discard_req->complete_user_request(r);
      discard_req->release_cell();
    });
  op->init_op(current_sync_gen, persist_on_flush, pwl.get_last_op_sequence_num(),
              on_write_persist, on_write_append);
  pwl.add_into_log_map(log_entries, this);
}

template <typename T>
void C_DiscardRequest<T>::dispatch() {
  utime_t now = ceph_clock_now();
  ldout(pwl.get_context(), 20) << "req type=" << get_name()
                               << " req=[" << *this << "]" << dendl;
  ceph_assert(this->m_resources.allocated);
  this->m_dispatched_time = now;
  setup_log_operations();
  m_perfcounter->inc(l_librbd_pwl_log_ops, 1);
  pwl.schedule_append(op);
}

template <typename T>
void C_DiscardRequest<T>::setup_buffer_resources(
    uint64_t *bytes_cached, uint64_t *bytes_dirtied, uint64_t *bytes_allocated,
    uint64_t *number_lanes, uint64_t *number_log_entries,
    uint64_t *number_unpublished_reserves) {
  *number_log_entries = 1;
  /* No bytes are allocated for a discard, but we count the discarded bytes
   * as dirty.  This means it's possible to have more bytes dirty than
   * there are bytes cached or allocated. */
  for (auto &extent : this->image_extents) {
    *bytes_dirtied = extent.second;
    break;
  }
}

template <typename T>
void C_DiscardRequest<T>::blockguard_acquired(GuardedRequestFunctionContext &guard_ctx) {
  ldout(pwl.get_context(), 20) << " cell=" << guard_ctx.cell << dendl;

  ceph_assert(guard_ctx.cell);
  this->detained = guard_ctx.state.detained; /* overlapped */
  this->set_cell(guard_ctx.cell);
}

template <typename T>
std::ostream &operator<<(std::ostream &os,
                         const C_DiscardRequest<T> &req) {
  os << (C_BlockIORequest<T>&)req;
  if (req.op) {
    os << " op=[" << *req.op << "]";
  } else {
    os << " op=nullptr";
  }
  return os;
}

template <typename T>
C_WriteSameRequest<T>::C_WriteSameRequest(
    T &pwl, const utime_t arrived, io::Extents &&image_extents,
    bufferlist&& bl, const int fadvise_flags, ceph::mutex &lock,
    PerfCounters *perfcounter, Context *user_req)
  : C_WriteRequest<T>(pwl, arrived, std::move(image_extents), std::move(bl),
      fadvise_flags, lock, perfcounter, user_req) {
  ldout(pwl.get_context(), 20) << this << dendl;
}

template <typename T>
C_WriteSameRequest<T>::~C_WriteSameRequest() {
   ldout(pwl.get_context(), 20) << this << dendl;
}

template <typename T>
void C_WriteSameRequest<T>::update_req_stats(utime_t &now) {
  /* Write same stats excluded from most write stats
   * because the read phase will make them look like slow writes in
   * those histograms. */
  ldout(pwl.get_context(), 20) << this << dendl;
  utime_t comp_latency = now - this->m_arrived_time;
  this->m_perfcounter->tinc(l_librbd_pwl_ws_latency, comp_latency);
}

template <typename T>
std::shared_ptr<WriteLogOperation> C_WriteSameRequest<T>::create_operation(
    uint64_t offset, uint64_t len) {
  ceph_assert(this->image_extents.size() == 1);
  WriteLogOperationSet &set = *this->op_set.get();
  return pwl.m_builder->create_write_log_operation(
      *this->op_set.get(), offset, len, this->bl.length(), pwl.get_context(),
      pwl.m_builder->create_writesame_log_entry(set.sync_point->log_entry, offset,
                                                len, this->bl.length()));
}

template <typename T>
std::ostream &operator<<(std::ostream &os,
                         const C_WriteSameRequest<T> &req) {
  os << (C_WriteRequest<T>&)req;
  return os;
}

template <typename T>
void C_WriteRequest<T>::update_req_stats(utime_t &now) {
  /* Compare-and-write stats. Compare-and-write excluded from most write
   * stats because the read phase will make them look like slow writes in
   * those histograms. */
  if(is_comp_and_write) {
    if (!compare_succeeded) {
      this->m_perfcounter->inc(l_librbd_pwl_cmp_fails, 1);
    }
    utime_t comp_latency = now - this->m_arrived_time;
    this->m_perfcounter->tinc(l_librbd_pwl_cmp_latency, comp_latency);
  }
}

} // namespace pwl
} // namespace cache
} // namespace librbd

template class librbd::cache::pwl::C_BlockIORequest<librbd::cache::pwl::AbstractWriteLog<librbd::ImageCtx> >;
template class librbd::cache::pwl::C_WriteRequest<librbd::cache::pwl::AbstractWriteLog<librbd::ImageCtx> >;
template class librbd::cache::pwl::C_FlushRequest<librbd::cache::pwl::AbstractWriteLog<librbd::ImageCtx> >;
template class librbd::cache::pwl::C_DiscardRequest<librbd::cache::pwl::AbstractWriteLog<librbd::ImageCtx> >;
template class librbd::cache::pwl::C_WriteSameRequest<librbd::cache::pwl::AbstractWriteLog<librbd::ImageCtx> >;
