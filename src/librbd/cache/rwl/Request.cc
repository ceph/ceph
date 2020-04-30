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
    m_perfcounter(perfcounter), m_lock(lock) {
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
std::shared_ptr<WriteLogOperation> C_WriteRequest<T>::create_operation(uint64_t offset, uint64_t len) {
  return std::make_shared<WriteLogOperation>(*op_set, offset, len, rwl.get_context());
}

template <typename T>
void C_WriteRequest<T>::setup_log_operations(DeferredContexts &on_exit) {
  GenericWriteLogEntries log_entries;
  {
    std::lock_guard locker(m_lock);
    std::shared_ptr<SyncPoint> current_sync_point = rwl.get_current_sync_point();
    if ((!rwl.get_persist_on_flush() && current_sync_point->log_entry->writes_completed) ||
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
      rwl.flush_new_sync_point(nullptr, on_exit);
      current_sync_point = rwl.get_current_sync_point();
    }
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
      auto operation = this->create_operation(extent.first, extent.second);
      this->op_set->operations.emplace_back(operation);

      /* A WS is also a write */
      ldout(rwl.get_context(), 20) << "write_req=" << *this << " op_set=" << op_set.get()
                                   << " operation=" << operation << dendl;
      log_entries.emplace_back(operation->log_entry);
      if (!op_set->persist_on_flush) {
        rwl.inc_last_op_sequence_num();
      }
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
  rwl.add_into_log_map(log_entries);
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
C_FlushRequest<T>::C_FlushRequest(T &rwl, const utime_t arrived,
                                  io::Extents &&image_extents,
                                  bufferlist&& bl, const int fadvise_flags,
                                  ceph::mutex &lock, PerfCounters *perfcounter,
                                  Context *user_req)
  : C_BlockIORequest<T>(rwl, arrived, std::move(image_extents), std::move(bl),
                        fadvise_flags, user_req),
    m_lock(lock), m_perfcounter(perfcounter) {
  ldout(rwl.get_context(), 20) << this << dendl;
}

template <typename T>
void C_FlushRequest<T>::finish_req(int r) {
  ldout(rwl.get_context(), 20) << "flush_req=" << this
                               << " cell=" << this->get_cell() << dendl;
  /* Block guard already released */
  ceph_assert(!this->get_cell());

  /* Completed to caller by here */
  utime_t now = ceph_clock_now();
  m_perfcounter->tinc(l_librbd_rwl_aio_flush_latency, now - this->m_arrived_time);
}

template <typename T>
bool C_FlushRequest<T>::alloc_resources() {
  ldout(rwl.get_context(), 20) << "req type=" << get_name() << " "
                               << "req=[" << *this << "]" << dendl;
  return rwl.alloc_resources(this);
}

template <typename T>
void C_FlushRequest<T>::dispatch() {
  utime_t now = ceph_clock_now();
  ldout(rwl.get_context(), 20) << "req type=" << get_name() << " "
                               << "req=[" << *this << "]" << dendl;
  ceph_assert(this->m_resources.allocated);
  this->m_dispatched_time = now;

  op = std::make_shared<SyncPointLogOperation>(m_lock,
                                               to_append,
                                               now,
                                               m_perfcounter,
                                               rwl.get_context());

  m_perfcounter->inc(l_librbd_rwl_log_ops, 1);
  rwl.schedule_append(op);
}

template <typename T>
void C_FlushRequest<T>::setup_buffer_resources(
    uint64_t &bytes_cached, uint64_t &bytes_dirtied, uint64_t &bytes_allocated,
    uint64_t &number_lanes, uint64_t &number_log_entries,
    uint64_t &number_unpublished_reserves) {
  number_log_entries = 1;
}

template <typename T>
std::ostream &operator<<(std::ostream &os,
                         const C_FlushRequest<T> &req) {
  os << (C_BlockIORequest<T>&)req
     << " m_resources.allocated=" << req.m_resources.allocated;
  return os;
};

void C_ReadRequest::finish(int r) {
  ldout(m_cct, 20) << "(" << get_name() << "): r=" << r << dendl;
  int hits = 0;
  int misses = 0;
  int hit_bytes = 0;
  int miss_bytes = 0;
  if (r >= 0) {
    /*
     * At this point the miss read has completed. We'll iterate through
     * read_extents and produce *m_out_bl by assembling pieces of miss_bl
     * and the individual hit extent bufs in the read extents that represent
     * hits.
     */
    uint64_t miss_bl_offset = 0;
    for (auto &extent : read_extents) {
      if (extent.m_bl.length()) {
        /* This was a hit */
        ceph_assert(extent.second == extent.m_bl.length());
        ++hits;
        hit_bytes += extent.second;
        m_out_bl->claim_append(extent.m_bl);
      } else {
        /* This was a miss. */
        ++misses;
        miss_bytes += extent.second;
        bufferlist miss_extent_bl;
        miss_extent_bl.substr_of(miss_bl, miss_bl_offset, extent.second);
        /* Add this read miss bufferlist to the output bufferlist */
        m_out_bl->claim_append(miss_extent_bl);
        /* Consume these bytes in the read miss bufferlist */
        miss_bl_offset += extent.second;
      }
    }
  }
  ldout(m_cct, 20) << "(" << get_name() << "): r=" << r << " bl=" << *m_out_bl << dendl;
  utime_t now = ceph_clock_now();
  ceph_assert((int)m_out_bl->length() == hit_bytes + miss_bytes);
  m_on_finish->complete(r);
  m_perfcounter->inc(l_librbd_rwl_rd_bytes, hit_bytes + miss_bytes);
  m_perfcounter->inc(l_librbd_rwl_rd_hit_bytes, hit_bytes);
  m_perfcounter->tinc(l_librbd_rwl_rd_latency, now - m_arrived_time);
  if (!misses) {
    m_perfcounter->inc(l_librbd_rwl_rd_hit_req, 1);
    m_perfcounter->tinc(l_librbd_rwl_rd_hit_latency, now - m_arrived_time);
  } else {
    if (hits) {
      m_perfcounter->inc(l_librbd_rwl_rd_part_hit_req, 1);
    }
  }
}

template <typename T>
C_DiscardRequest<T>::C_DiscardRequest(T &rwl, const utime_t arrived, io::Extents &&image_extents,
                                      uint32_t discard_granularity_bytes, ceph::mutex &lock,
                                      PerfCounters *perfcounter, Context *user_req)
  : C_BlockIORequest<T>(rwl, arrived, std::move(image_extents), bufferlist(), 0, user_req),
  m_discard_granularity_bytes(discard_granularity_bytes),
  m_lock(lock),
  m_perfcounter(perfcounter) {
  ldout(rwl.get_context(), 20) << this << dendl;
}

template <typename T>
C_DiscardRequest<T>::~C_DiscardRequest() {
  ldout(rwl.get_context(), 20) << this << dendl;
}

template <typename T>
bool C_DiscardRequest<T>::alloc_resources() {
  ldout(rwl.get_context(), 20) << "req type=" << get_name() << " "
                               << "req=[" << *this << "]" << dendl;
  return rwl.alloc_resources(this);
}

template <typename T>
void C_DiscardRequest<T>::setup_log_operations() {
  std::lock_guard locker(m_lock);
  GenericWriteLogEntries log_entries;
  for (auto &extent : this->image_extents) {
    op = std::make_shared<DiscardLogOperation>(rwl.get_current_sync_point(),
                                               extent.first,
                                               extent.second,
                                               m_discard_granularity_bytes,
                                               this->m_dispatched_time,
                                               m_perfcounter,
                                               rwl.get_context());
    log_entries.emplace_back(op->log_entry);
    break;
  }
  uint64_t current_sync_gen = rwl.get_current_sync_gen();
  bool persist_on_flush = rwl.get_persist_on_flush();
  if (!persist_on_flush) {
    rwl.inc_last_op_sequence_num();
  }
  auto discard_req = this;
  Context *on_write_persist = new LambdaContext(
    [this, discard_req](int r) {
      ldout(rwl.get_context(), 20) << "discard_req=" << discard_req
                                   << " cell=" << discard_req->get_cell() << dendl;
      ceph_assert(discard_req->get_cell());
      discard_req->complete_user_request(r);
      discard_req->release_cell();
    });
  op->init(current_sync_gen, persist_on_flush, rwl.get_last_op_sequence_num(), on_write_persist);
  rwl.add_into_log_map(log_entries);
}

template <typename T>
void C_DiscardRequest<T>::dispatch() {
  utime_t now = ceph_clock_now();
  ldout(rwl.get_context(), 20) << "req type=" << get_name() << " "
                               << "req=[" << *this << "]" << dendl;
  ceph_assert(this->m_resources.allocated);
  this->m_dispatched_time = now;
  setup_log_operations();
  m_perfcounter->inc(l_librbd_rwl_log_ops, 1);
  rwl.schedule_append(op);
}

template <typename T>
void C_DiscardRequest<T>::setup_buffer_resources(
    uint64_t &bytes_cached, uint64_t &bytes_dirtied, uint64_t &bytes_allocated,
    uint64_t &number_lanes, uint64_t &number_log_entries,
    uint64_t &number_unpublished_reserves) {
  number_log_entries = 1;
  /* No bytes are allocated for a discard, but we count the discarded bytes
   * as dirty.  This means it's possible to have more bytes dirty than
   * there are bytes cached or allocated. */
  for (auto &extent : this->image_extents) {
    bytes_dirtied = extent.second;
    break;
  }
}

template <typename T>
void C_DiscardRequest<T>::blockguard_acquired(GuardedRequestFunctionContext &guard_ctx) {
  ldout(rwl.get_context(), 20) << " cell=" << guard_ctx.cell << dendl;

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
};

template <typename T>
C_WriteSameRequest<T>::C_WriteSameRequest(T &rwl, const utime_t arrived, io::Extents &&image_extents,
                                          bufferlist&& bl, const int fadvise_flags, ceph::mutex &lock,
                                          PerfCounters *perfcounter, Context *user_req)
  : C_WriteRequest<T>(rwl, arrived, std::move(image_extents), std::move(bl), fadvise_flags, lock, perfcounter, user_req) {
  ldout(rwl.get_context(), 20) << this << dendl;
}

template <typename T>
C_WriteSameRequest<T>::~C_WriteSameRequest() {
   ldout(rwl.get_context(), 20) << this << dendl;
}

template <typename T>
void C_WriteSameRequest<T>::update_req_stats(utime_t &now) {
  /* Write same stats excluded from most write stats
   * because the read phase will make them look like slow writes in
   * those histograms. */
  ldout(rwl.get_context(), 20) << this << dendl;
  utime_t comp_latency = now - this->m_arrived_time;
  this->m_perfcounter->tinc(l_librbd_rwl_ws_latency, comp_latency);
}

/* Write sames will allocate one buffer, the size of the repeating pattern */
template <typename T>
void C_WriteSameRequest<T>::setup_buffer_resources(
    uint64_t &bytes_cached, uint64_t &bytes_dirtied, uint64_t &bytes_allocated,
    uint64_t &number_lanes, uint64_t &number_log_entries,
    uint64_t &number_unpublished_reserves) {
  ldout(rwl.get_context(), 20) << this << dendl;
  ceph_assert(this->image_extents.size() == 1);
  bytes_dirtied += this->image_extents[0].second;
  auto pattern_length = this->bl.length();
  this->m_resources.buffers.emplace_back();
  struct WriteBufferAllocation &buffer = this->m_resources.buffers.back();
  buffer.allocation_size = MIN_WRITE_ALLOC_SIZE;
  buffer.allocated = false;
  bytes_cached += pattern_length;
  if (pattern_length > buffer.allocation_size) {
    buffer.allocation_size = pattern_length;
  }
}

template <typename T>
std::shared_ptr<WriteLogOperation> C_WriteSameRequest<T>::create_operation(uint64_t offset, uint64_t len) {
  ceph_assert(this->image_extents.size() == 1);
  return std::make_shared<WriteSameLogOperation>(*this->op_set.get(), offset, len,
                                                 this->bl.length(), rwl.get_context());
}

template <typename T>
std::ostream &operator<<(std::ostream &os,
                         const C_WriteSameRequest<T> &req) {
  os << (C_WriteRequest<T>&)req;
  return os;
};

template <typename T>
C_CompAndWriteRequest<T>::C_CompAndWriteRequest(T &rwl, const utime_t arrived, io::Extents &&image_extents,
                                                bufferlist&& cmp_bl, bufferlist&& bl, uint64_t *mismatch_offset,
                                                int fadvise_flags, ceph::mutex &lock, PerfCounters *perfcounter,
                                                Context *user_req)
  : C_WriteRequest<T>(rwl, arrived, std::move(image_extents), std::move(bl), fadvise_flags, lock, perfcounter, user_req),
  mismatch_offset(mismatch_offset), cmp_bl(std::move(cmp_bl)) {
  ldout(rwl.get_context(), 20) << dendl;
}

template <typename T>
C_CompAndWriteRequest<T>::~C_CompAndWriteRequest() {
  ldout(rwl.get_context(), 20) << dendl;
}

template <typename T>
void C_CompAndWriteRequest<T>::finish_req(int r) {
  if (compare_succeeded) {
    C_WriteRequest<T>::finish_req(r);
  } else {
    utime_t now = ceph_clock_now();
    update_req_stats(now);
  }
}

template <typename T>
void C_CompAndWriteRequest<T>::update_req_stats(utime_t &now) {
  /* Compare-and-write stats. Compare-and-write excluded from most write
   * stats because the read phase will make them look like slow writes in
   * those histograms. */
  if (!compare_succeeded) {
    this->m_perfcounter->inc(l_librbd_rwl_cmp_fails, 1);
  }
  utime_t comp_latency = now - this->m_arrived_time;
  this->m_perfcounter->tinc(l_librbd_rwl_cmp_latency, comp_latency);
}

template <typename T>
std::ostream &operator<<(std::ostream &os,
                         const C_CompAndWriteRequest<T> &req) {
  os << (C_WriteRequest<T>&)req
     << "cmp_bl=" << req.cmp_bl << ", "
     << "read_bl=" << req.read_bl << ", "
     << "compare_succeeded=" << req.compare_succeeded << ", "
     << "mismatch_offset=" << req.mismatch_offset;
  return os;
};

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
