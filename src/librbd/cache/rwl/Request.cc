// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Request.h"
#include "librbd/BlockGuard.h"
#include "librbd/cache/rwl/LogEntry.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::rwl::Request: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace rwl {

typedef std::list<std::shared_ptr<GeneralWriteLogEntry>> GeneralWriteLogEntries;

template <typename T>
C_GuardedBlockIORequest<T>::C_GuardedBlockIORequest(T &rwl)
  : rwl(rwl) {
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
  }
}

template <typename T>
C_GuardedBlockIORequest<T>::~C_GuardedBlockIORequest() {
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
  }
  ceph_assert(m_cell_released || !m_cell);
}

template <typename T>
void C_GuardedBlockIORequest<T>::set_cell(BlockGuardCell *cell) {
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << this << " cell=" << cell << dendl;
  }
  ceph_assert(cell);
  ceph_assert(!m_cell);
  m_cell = cell;
}

template <typename T>
BlockGuardCell *C_GuardedBlockIORequest<T>::get_cell(void) {
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << this << " cell=" << m_cell << dendl;
  }
  return m_cell;
}

template <typename T>
void C_GuardedBlockIORequest<T>::release_cell() {
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << this << " cell=" << m_cell << dendl;
  }
  ceph_assert(m_cell);
  bool initial = false;
  if (m_cell_released.compare_exchange_strong(initial, true)) {
    rwl.release_guarded_request(m_cell);
  } else {
    ldout(rwl.m_image_ctx.cct, 5) << "cell " << m_cell << " already released for " << this << dendl;
  }
}

template <typename T>
C_BlockIORequest<T>::C_BlockIORequest(T &rwl, const utime_t arrived, io::Extents &&extents,
                 bufferlist&& bl, const int fadvise_flags, Context *user_req)
  : C_GuardedBlockIORequest<T>(rwl), image_extents(std::move(extents)),
    bl(std::move(bl)), fadvise_flags(fadvise_flags),
    user_req(user_req), image_extents_summary(image_extents), m_arrived_time(arrived) {
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
  }
  /* Remove zero length image extents from input */
  for (auto it = image_extents.begin(); it != image_extents.end(); ) {
    if (0 == it->second) {
      it = image_extents.erase(it);
      continue;
    }
    ++it;
  }
}

template <typename T>
C_BlockIORequest<T>::~C_BlockIORequest() {
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
  }
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
      << "m_waited_lanes=" << req.m_waited_lanes << ", "
      << "m_waited_entries=" << req.m_waited_entries << ", "
      << "m_waited_buffers=" << req.m_waited_buffers << "";
   return os;
 }

template <typename T>
void C_BlockIORequest<T>::complete_user_request(int r) {
  bool initial = false;
  if (m_user_req_completed.compare_exchange_strong(initial, true)) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 15) << this << " completing user req" << dendl;
    }
    m_user_req_completed_time = ceph_clock_now();
    user_req->complete(r);
    // Set user_req as null as it is deleted
    user_req = nullptr;
  } else {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 20) << this << " user req already completed" << dendl;
    }
  }
}

template <typename T>
void C_BlockIORequest<T>::finish(int r) {
  ldout(rwl.m_image_ctx.cct, 20) << this << dendl;

  complete_user_request(r);
  bool initial = false;
  if (m_finish_called.compare_exchange_strong(initial, true)) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 15) << this << " finishing" << dendl;
    }
    finish_req(0);
  } else {
    ldout(rwl.m_image_ctx.cct, 20) << this << " already finished" << dendl;
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
               bufferlist&& bl, const int fadvise_flags, Context *user_req)
  : C_BlockIORequest<T>(rwl, arrived, std::move(image_extents), std::move(bl), fadvise_flags, user_req) {
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
  }
}

template <typename T>
C_WriteRequest<T>::~C_WriteRequest() {
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
  }
}

template <typename T>
std::ostream &operator<<(std::ostream &os,
                                const C_WriteRequest<T> &req) {
  os << (C_BlockIORequest<T>&)req
     << " m_resources.allocated=" << req.resources.allocated;
  if (req.m_op_set) {
     os << "m_op_set=" << *req.m_op_set;
  }
  return os;
};

template <typename T>
void C_WriteRequest<T>::blockguard_acquired(GuardedRequestFunctionContext &guard_ctx) {
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << __func__ << " write_req=" << this << " cell=" << guard_ctx.m_cell << dendl;
  }

  ceph_assert(guard_ctx.m_cell);
  this->detained = guard_ctx.m_state.detained; /* overlapped */
  this->m_queued = guard_ctx.m_state.queued; /* queued behind at least one barrier */
  this->set_cell(guard_ctx.m_cell);
}

template <typename T>
void C_WriteRequest<T>::finish_req(int r) {
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 15) << "write_req=" << this << " cell=" << this->get_cell() << dendl;
  }

  /* Completed to caller by here (in finish(), which calls this) */
  utime_t now = ceph_clock_now();
  rwl.release_write_lanes(this);
  this->release_cell(); /* TODO: Consider doing this in appending state */
  update_req_stats(now);
}

template <typename T>
void C_WriteRequest<T>::setup_buffer_resources(uint64_t &bytes_cached, uint64_t &bytes_dirtied) {
  for (auto &extent : this->image_extents) {
    resources.buffers.emplace_back();
    struct WriteBufferAllocation &buffer = resources.buffers.back();
    buffer.allocation_size = MIN_WRITE_ALLOC_SIZE;
    buffer.allocated = false;
    bytes_cached += extent.second;
    if (extent.second > buffer.allocation_size) {
      buffer.allocation_size = extent.second;
    }
  }
  bytes_dirtied = bytes_cached;
}

template <typename T>
void C_WriteRequest<T>::setup_log_operations() {
  for (auto &extent : this->image_extents) {
    /* operation->on_write_persist connected to m_prior_log_entries_persisted Gather */
    auto operation =
      std::make_shared<WriteLogOperation<T>>(*m_op_set, extent.first, extent.second);
    m_op_set->operations.emplace_back(operation);
  }
}

template <typename T>
void C_WriteRequest<T>::schedule_append() {
  ceph_assert(++m_appended == 1);
  if (m_do_early_flush) {
    /* This caller is waiting for persist, so we'll use their thread to
     * expedite it */
    rwl.flush_pmem_buffer(this->m_op_set->operations);
    rwl.schedule_append(this->m_op_set->operations);
  } else {
    /* This is probably not still the caller's thread, so do the payload
     * flushing/replicating later. */
    rwl.schedule_flush_and_append(this->m_op_set->operations);
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
bool C_WriteRequest<T>::alloc_resources()
{
  bool alloc_succeeds = true;
  bool no_space = false;
  utime_t alloc_start = ceph_clock_now();
  uint64_t bytes_allocated = 0;
  uint64_t bytes_cached = 0;
  uint64_t bytes_dirtied = 0;

  ceph_assert(!resources.allocated);
  resources.buffers.reserve(this->image_extents.size());
  {
    std::lock_guard locker(rwl.m_lock);
    if (rwl.m_free_lanes < this->image_extents.size()) {
      this->m_waited_lanes = true;
      if (RWL_VERBOSE_LOGGING) {
        ldout(rwl.m_image_ctx.cct, 20) << "not enough free lanes (need "
                                       <<  this->image_extents.size()
                                       << ", have " << rwl.m_free_lanes << ") "
                                       << *this << dendl;
      }
      alloc_succeeds = false;
      /* This isn't considered a "no space" alloc fail. Lanes are a throttling mechanism. */
    }
    if (rwl.m_free_log_entries < this->image_extents.size()) {
      this->m_waited_entries = true;
      if (RWL_VERBOSE_LOGGING) {
        ldout(rwl.m_image_ctx.cct, 20) << "not enough free entries (need "
                                       <<  this->image_extents.size()
                                       << ", have " << rwl.m_free_log_entries << ") "
                                       << *this << dendl;
      }
      alloc_succeeds = false;
      no_space = true; /* Entries must be retired */
    }
    /* Don't attempt buffer allocate if we've exceeded the "full" threshold */
    if (rwl.m_bytes_allocated > rwl.m_bytes_allocated_cap) {
      if (!this->m_waited_buffers) {
        this->m_waited_buffers = true;
        if (RWL_VERBOSE_LOGGING) {
          ldout(rwl.m_image_ctx.cct, 1) << "Waiting for allocation cap (cap=" << rwl.m_bytes_allocated_cap
                                        << ", allocated=" << rwl.m_bytes_allocated
                                        << ") in write [" << *this << "]" << dendl;
        }
      }
      alloc_succeeds = false;
      no_space = true; /* Entries must be retired */
    }
  }
  if (alloc_succeeds) {
    setup_buffer_resources(bytes_cached, bytes_dirtied);
  }

  if (alloc_succeeds) {
    for (auto &buffer : resources.buffers) {
      bytes_allocated += buffer.allocation_size;
      utime_t before_reserve = ceph_clock_now();
      buffer.buffer_oid = pmemobj_reserve(rwl.m_log_pool,
                                          &buffer.buffer_alloc_action,
                                          buffer.allocation_size,
                                          0 /* Object type */);
      buffer.allocation_lat = ceph_clock_now() - before_reserve;
      if (TOID_IS_NULL(buffer.buffer_oid)) {
        if (!this->m_waited_buffers) {
          this->m_waited_buffers = true;
        }
        if (RWL_VERBOSE_LOGGING) {
          ldout(rwl.m_image_ctx.cct, 5) << "can't allocate all data buffers: "
                                        << pmemobj_errormsg() << ". "
                                        << *this << dendl;
        }
        alloc_succeeds = false;
        no_space = true; /* Entries need to be retired */
        break;
      } else {
        buffer.allocated = true;
      }
      if (RWL_VERBOSE_LOGGING) {
        ldout(rwl.m_image_ctx.cct, 20) << "Allocated " << buffer.buffer_oid.oid.pool_uuid_lo
                                       << "." << buffer.buffer_oid.oid.off
                                       << ", size=" << buffer.allocation_size << dendl;
      }
    }
  }

  if (alloc_succeeds) {
    unsigned int num_extents = this->image_extents.size();
    std::lock_guard locker(rwl.m_lock);
    /* We need one free log entry per extent (each is a separate entry), and
     * one free "lane" for remote replication. */
    if ((rwl.m_free_lanes >= num_extents) &&
        (rwl.m_free_log_entries >= num_extents)) {
      rwl.m_free_lanes -= num_extents;
      rwl.m_free_log_entries -= num_extents;
      rwl.m_unpublished_reserves += num_extents;
      rwl.m_bytes_allocated += bytes_allocated;
      rwl.m_bytes_cached += bytes_cached;
      rwl.m_bytes_dirty += bytes_dirtied;
      resources.allocated = true;
    } else {
      alloc_succeeds = false;
    }
  }

  if (!alloc_succeeds) {
    /* On alloc failure, free any buffers we did allocate */
    for (auto &buffer : resources.buffers) {
      if (buffer.allocated) {
        pmemobj_cancel(rwl.m_log_pool, &buffer.buffer_alloc_action, 1);
      }
    }
    resources.buffers.clear();
    if (no_space) {
      /* Expedite flushing and/or retiring */
      std::lock_guard locker(rwl.m_lock);
      rwl.m_alloc_failed_since_retire = true;
      rwl.m_last_alloc_fail = ceph_clock_now();
    }
  }

  this->m_allocated_time = alloc_start;
  return alloc_succeeds;
}

/**
 * Takes custody of write_req. Resources must already be allocated.
 *
 * Locking:
 * Acquires m_lock
 */
template <typename T>
void C_WriteRequest<T>::dispatch()
{
  CephContext *cct = rwl.m_image_ctx.cct;
  GeneralWriteLogEntries log_entries;
  DeferredContexts on_exit;
  utime_t now = ceph_clock_now();
  auto write_req_sp = this;
  this->m_dispatched_time = now;

  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 15) << "name: " << rwl.m_image_ctx.name << " id: " << rwl.m_image_ctx.id
                   << "write_req=" << this << " cell=" << this->get_cell() << dendl;
  }

  {
    uint64_t buffer_offset = 0;
    std::lock_guard locker(rwl.m_lock);
    Context *set_complete = this;
    // TODO: Add sync point if necessary
    //
    m_op_set =
      make_unique<WriteLogOperationSet<T>>(rwl, now, rwl.m_current_sync_point, rwl.m_persist_on_flush,
                                           set_complete);
    if (RWL_VERBOSE_LOGGING) {
      ldout(cct, 20) << "write_req=" << this << " m_op_set=" << m_op_set.get() << dendl;
    }
    ceph_assert(resources.allocated);
    /* m_op_set->operations initialized differently for plain write or write same */
    this->setup_log_operations();
    auto allocation = resources.buffers.begin();
    for (auto &gen_op : m_op_set->operations) {
      /* A WS is also a write */
      auto operation = gen_op->get_write_op();
      if (RWL_VERBOSE_LOGGING) {
        ldout(cct, 20) << "write_req=" << this << " m_op_set=" << m_op_set.get()
                       << " operation=" << operation << dendl;
      }
      log_entries.emplace_back(operation->log_entry);
      rwl.m_perfcounter->inc(l_librbd_rwl_log_ops, 1);

      operation->log_entry->ram_entry.has_data = 1;
      operation->log_entry->ram_entry.write_data = allocation->buffer_oid;
      // TODO: make std::shared_ptr
      operation->buffer_alloc = &(*allocation);
      ceph_assert(!TOID_IS_NULL(operation->log_entry->ram_entry.write_data));
      operation->log_entry->pmem_buffer = D_RW(operation->log_entry->ram_entry.write_data);
      operation->log_entry->ram_entry.sync_gen_number = rwl.m_current_sync_gen;
      if (m_op_set->persist_on_flush) {
        /* Persist on flush. Sequence #0 is never used. */
        operation->log_entry->ram_entry.write_sequence_number = 0;
      } else {
        /* Persist on write */
        operation->log_entry->ram_entry.write_sequence_number = ++rwl.m_last_op_sequence_num;
        operation->log_entry->ram_entry.sequenced = 1;
      }
      operation->log_entry->ram_entry.sync_point = 0;
      operation->log_entry->ram_entry.discard = 0;
      operation->bl.substr_of(this->bl, buffer_offset,
                              operation->log_entry->write_bytes());
      buffer_offset += operation->log_entry->write_bytes();
      if (RWL_VERBOSE_LOGGING) {
        ldout(cct, 20) << "operation=[" << *operation << "]" << dendl;
      }
      allocation++;
    }
  }
  /* All extent ops subs created */
  m_op_set->extent_ops_appending->activate();
  m_op_set->extent_ops_persist->activate();

  /* Write data */
  for (auto &operation : m_op_set->operations) {
    /* operation is a shared_ptr, so write_op is only good as long as operation is in scope */
    auto write_op = operation->get_write_op();
    ceph_assert(write_op != nullptr);
    bufferlist::iterator i(&write_op->bl);
    rwl.m_perfcounter->inc(l_librbd_rwl_log_op_bytes, write_op->log_entry->write_bytes());
    if (RWL_VERBOSE_LOGGING) {
      ldout(cct, 20) << write_op->bl << dendl;
    }
    i.copy((unsigned)write_op->log_entry->write_bytes(), (char*)write_op->log_entry->pmem_buffer);
  }

  // TODO: Add to log map for read

  /*
   * Entries are added to m_log_entries in alloc_op_log_entries() when their
   * order is established. They're added to m_dirty_log_entries when the write
   * completes to all replicas. They must not be flushed before then. We don't
   * prevent the application from reading these before they persist. If we
   * supported coherent shared access, that might be a problem (the write could
   * fail after another initiator had read it). As it is the cost of running
   * reads through the block guard (and exempting them from the barrier, which
   * doesn't need to apply to them) to prevent reading before the previous
   * write of that data persists doesn't seem justified.
   */

  if (rwl.m_persist_on_flush_early_user_comp &&
      m_op_set->persist_on_flush) {
    /*
     * We're done with the caller's buffer, and not guaranteeing
     * persistence until the next flush. The block guard for this
     * write_req will not be released until the write is persisted
     * everywhere, but the caller's request can complete now.
     */
    this->complete_user_request(0);
  }

  bool append_deferred = false;
  {
    std::lock_guard locker(rwl.m_lock);
    if (!m_op_set->persist_on_flush &&
        m_op_set->sync_point->earlier_sync_point) {
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
      Context *schedule_append_ctx = new LambdaContext([this, write_req_sp](int r) {
          write_req_sp->schedule_append();
        });
      m_op_set->sync_point->earlier_sync_point->on_sync_point_appending.push_back(schedule_append_ctx);
      append_deferred = true;
    } else {
      /* The prior sync point is done, so we'll schedule append here. If this is
       * persist-on-write, and probably still the caller's thread, we'll use this
       * caller's thread to perform the persist & replication of the payload
       * buffer. */
      m_do_early_flush =
        !(this->detained || this->m_queued || this->m_deferred || m_op_set->persist_on_flush);
    }
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
  ceph_assert(m_cell);
  m_callback(*this);
}

std::ostream &operator<<(std::ostream &os,
                         const GuardedRequest &r) {
  os << "guard_ctx->m_state=[" << r.guard_ctx->m_state << "], "
     << "block_extent.block_start=" << r.block_extent.block_start << ", "
     << "block_extent.block_start=" << r.block_extent.block_end;
  return os;
};

} // namespace rwl 
} // namespace cache 
} // namespace librbd 
