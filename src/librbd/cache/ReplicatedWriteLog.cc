// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <libpmemobj.h>
#include "ReplicatedWriteLog.h"
#include "ReplicatedWriteLogInternal.h"
#include "common/perf_counters.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "common/deleter.h"
#include "common/dout.h"
#include "common/environment.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include <map>
#include <vector>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::ReplicatedWriteLog: " << this << " " \
			   <<  __func__ << ": "

namespace librbd {
namespace cache {

using namespace librbd::cache::rwl;

const unsigned long int ops_appended_together = MAX_ALLOC_PER_TRANSACTION;
/*
 * Performs the log event append operation for all of the scheduled
 * events.
 */
template <typename I>
void ReplicatedWriteLog<I>::append_scheduled_ops(void)
{
  GenericLogOperationsT ops;
  int append_result = 0;
  bool ops_remain = false;
  bool appending = false; /* true if we set m_appending */
  if (RWL_VERBOSE_LOGGING) {
    ldout(m_image_ctx.cct, 20) << dendl;
  }
  do {
    ops.clear();

    {
      Mutex::Locker locker(m_lock);
      if (!appending && m_appending) {
	/* Another thread is appending */
	if (RWL_VERBOSE_LOGGING) {
	  ldout(m_image_ctx.cct, 15) << "Another thread is appending" << dendl;
	}
	return;
      }
      if (m_ops_to_append.size()) {
	appending = true;
	m_appending = true;
	auto last_in_batch = m_ops_to_append.begin();
	unsigned int ops_to_append = m_ops_to_append.size();
	if (ops_to_append > ops_appended_together) {
	  ops_to_append = ops_appended_together;
	}
	std::advance(last_in_batch, ops_to_append);
	ops.splice(ops.end(), m_ops_to_append, m_ops_to_append.begin(), last_in_batch);
	ops_remain = true; /* Always check again before leaving */
	if (RWL_VERBOSE_LOGGING) {
	  ldout(m_image_ctx.cct, 20) << "appending " << ops.size() << ", " << m_ops_to_append.size() << " remain" << dendl;
	}
      } else {
	ops_remain = false;
	if (appending) {
	  appending = false;
	  m_appending = false;
	}
      }
    }

    if (ops.size()) {
      Mutex::Locker locker(m_log_append_lock);
      alloc_op_log_entries(ops);
      append_result = append_op_log_entries(ops);
    }

    int num_ops = ops.size();
    if (num_ops) {
      /* New entries may be flushable. Completion will wake up flusher. */
      complete_op_log_entries(std::move(ops), append_result);
    }
  } while (ops_remain);
}

template <typename I>
void ReplicatedWriteLog<I>::enlist_op_appender()
{
  m_async_append_ops++;
  m_async_op_tracker.start_op();
  Context *append_ctx = new FunctionContext([this](int r) {
      append_scheduled_ops();
      m_async_append_ops--;
      m_async_op_tracker.finish_op();
    });
  if (use_finishers) {
    m_log_append_finisher.queue(append_ctx);
  } else {
    m_work_queue.queue(append_ctx);
  }
}

/*
 * Takes custody of ops. They'll all get their log entries appended,
 * and have their on_write_persist contexts completed once they and
 * all prior log entries are persisted everywhere.
 */
template <typename I>
void ReplicatedWriteLog<I>::schedule_append(GenericLogOperationsT &ops)
{
  bool need_finisher;
  GenericLogOperationsVectorT appending;

  std::copy(std::begin(ops), std::end(ops), std::back_inserter(appending));
  {
    Mutex::Locker locker(m_lock);

    need_finisher = m_ops_to_append.empty() && !m_appending;
    m_ops_to_append.splice(m_ops_to_append.end(), ops);
  }

  if (need_finisher) {
    enlist_op_appender();
  }

  for (auto &op : appending) {
    op->appending();
  }
}

template <typename I>
void ReplicatedWriteLog<I>::schedule_append(GenericLogOperationsVectorT &ops)
{
  GenericLogOperationsT to_append(ops.begin(), ops.end());

  schedule_append(to_append);
}

template <typename I>
void ReplicatedWriteLog<I>::schedule_append(GenericLogOperationSharedPtrT op)
{
  GenericLogOperationsT to_append { op };

  schedule_append(to_append);
}

const unsigned long int ops_flushed_together = 4;
/*
 * Performs the pmem buffer flush on all scheduled ops, then schedules
 * the log event append operation for all of them.
 */
template <typename I>
void ReplicatedWriteLog<I>::flush_then_append_scheduled_ops(void)
{
  GenericLogOperationsT ops;
  bool ops_remain = false;
  if (RWL_VERBOSE_LOGGING) {
    ldout(m_image_ctx.cct, 20) << dendl;
  }
  do {
    {
      ops.clear();
      Mutex::Locker locker(m_lock);
      if (m_ops_to_flush.size()) {
	auto last_in_batch = m_ops_to_flush.begin();
	unsigned int ops_to_flush = m_ops_to_flush.size();
	if (ops_to_flush > ops_flushed_together) {
	  ops_to_flush = ops_flushed_together;
	}
	if (RWL_VERBOSE_LOGGING) {
	  ldout(m_image_ctx.cct, 20) << "should flush " << ops_to_flush << dendl;
	}
	std::advance(last_in_batch, ops_to_flush);
	ops.splice(ops.end(), m_ops_to_flush, m_ops_to_flush.begin(), last_in_batch);
	ops_remain = !m_ops_to_flush.empty();
	if (RWL_VERBOSE_LOGGING) {
	  ldout(m_image_ctx.cct, 20) << "flushing " << ops.size() << ", " << m_ops_to_flush.size() << " remain" << dendl;
	}
      } else {
	ops_remain = false;
      }
    }

    if (ops_remain) {
      enlist_op_flusher();
    }

    /* Ops subsequently scheduled for flush may finish before these,
     * which is fine. We're unconcerned with completion order until we
     * get to the log message append step. */
    if (ops.size()) {
      flush_pmem_buffer(ops);
      schedule_append(ops);
    }
  } while (ops_remain);
  append_scheduled_ops();
}

template <typename I>
void ReplicatedWriteLog<I>::enlist_op_flusher()
{
  m_async_flush_ops++;
  m_async_op_tracker.start_op();
  Context *flush_ctx = new FunctionContext([this](int r) {
      flush_then_append_scheduled_ops();
      m_async_flush_ops--;
      m_async_op_tracker.finish_op();
    });
  if (use_finishers) {
    m_persist_finisher.queue(flush_ctx);
  } else {
    m_work_queue.queue(flush_ctx);
  }
}

/*
 * Takes custody of ops. They'll all get their pmem blocks flushed,
 * then get their log entries appended.
 */
template <typename I>
void ReplicatedWriteLog<I>::schedule_flush_and_append(GenericLogOperationsVectorT &ops)
{
  GenericLogOperationsT to_flush(ops.begin(), ops.end());
  bool need_finisher;
  if (RWL_VERBOSE_LOGGING) {
    ldout(m_image_ctx.cct, 20) << dendl;
  }
  {
    Mutex::Locker locker(m_lock);

    need_finisher = m_ops_to_flush.empty();
    m_ops_to_flush.splice(m_ops_to_flush.end(), to_flush);
  }

  if (need_finisher) {
    enlist_op_flusher();
  }
}

/*
 * Flush the pmem regions for the data blocks of a set of operations
 *
 * V is expected to be GenericLogOperations<I>, or GenericLogOperationsVector<I>
 */
template <typename I>
template <typename V>
void ReplicatedWriteLog<I>::flush_pmem_buffer(V& ops)
{
  for (auto &operation : ops) {
    if (operation->is_write() || operation->is_writesame()) {
      operation->m_buf_persist_time = ceph_clock_now();
      auto write_entry = operation->get_write_log_entry();

      pmemobj_flush(m_internal->m_log_pool, write_entry->pmem_buffer, write_entry->write_bytes());
    }
  }

  /* Drain once for all */
  pmemobj_drain(m_internal->m_log_pool);

  utime_t now = ceph_clock_now();
  for (auto &operation : ops) {
    if (operation->is_write() || operation->is_writesame()) {
      operation->m_buf_persist_comp_time = now;
    } else {
      if (RWL_VERBOSE_LOGGING) {
	ldout(m_image_ctx.cct, 20) << "skipping non-write op: " << *operation << dendl;
      }
    }
  }
}

/*
 * Allocate the (already reserved) write log entries for a set of operations.
 *
 * Locking:
 * Acquires m_lock
 */
template <typename I>
void ReplicatedWriteLog<I>::alloc_op_log_entries(GenericLogOperationsT &ops)
{
  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_internal->m_log_pool, struct WriteLogPoolRoot);
  struct WriteLogPmemEntry *pmem_log_entries = D_RW(D_RW(pool_root)->log_entries);

  assert(m_log_append_lock.is_locked_by_me());

  /* Allocate the (already reserved) log entries */
  Mutex::Locker locker(m_lock);

  for (auto &operation : ops) {
    uint32_t entry_index = m_first_free_entry;
    m_first_free_entry = (m_first_free_entry + 1) % m_total_log_entries;
    //if (m_log_entries.back()) {
    //  assert((m_log_entries.back()->log_entry_index + 1) % m_total_log_entries == entry_index);
    //}
    auto &log_entry = operation->get_log_entry();
    log_entry->log_entry_index = entry_index;
    log_entry->ram_entry.entry_index = entry_index;
    log_entry->pmem_entry = &pmem_log_entries[entry_index];
    log_entry->ram_entry.entry_valid = 1;
    m_log_entries.push_back(log_entry);
    if (RWL_VERBOSE_LOGGING) {
      ldout(m_image_ctx.cct, 20) << "operation=[" << *operation << "]" << dendl;
    }
  }
}

/*
 * Flush the persistent write log entries set of ops. The entries must
 * be contiguous in persistent memory.
 */
template <typename I>
void ReplicatedWriteLog<I>::flush_op_log_entries(GenericLogOperationsVectorT &ops)
{
  if (ops.empty()) return;

  if (ops.size() > 1) {
    assert(ops.front()->get_log_entry()->pmem_entry < ops.back()->get_log_entry()->pmem_entry);
  }

  if (RWL_VERBOSE_LOGGING) {
    ldout(m_image_ctx.cct, 20) << "entry count=" << ops.size() << " "
			       << "start address=" << ops.front()->get_log_entry()->pmem_entry << " "
			       << "bytes=" << ops.size() * sizeof(*(ops.front()->get_log_entry()->pmem_entry))
			       << dendl;
  }
  pmemobj_flush(m_internal->m_log_pool,
		ops.front()->get_log_entry()->pmem_entry,
		ops.size() * sizeof(*(ops.front()->get_log_entry()->pmem_entry)));
}

/*
 * Write and persist the (already allocated) write log entries and
 * data buffer allocations for a set of ops. The data buffer for each
 * of these must already have been persisted to its reserved area.
 */
template <typename I>
int ReplicatedWriteLog<I>::append_op_log_entries(GenericLogOperationsT &ops)
{
  CephContext *cct = m_image_ctx.cct;
  GenericLogOperationsVectorT entries_to_flush;
  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_internal->m_log_pool, struct WriteLogPoolRoot);
  int ret = 0;

  assert(m_log_append_lock.is_locked_by_me());

  if (ops.empty()) return 0;
  entries_to_flush.reserve(ops_appended_together);

  /* Write log entries to ring and persist */
  utime_t now = ceph_clock_now();
  for (auto &operation : ops) {
    if (!entries_to_flush.empty()) {
      /* Flush these and reset the list if the current entry wraps to the
       * tail of the ring */
      if (entries_to_flush.back()->get_log_entry()->log_entry_index >
	  operation->get_log_entry()->log_entry_index) {
	if (RWL_VERBOSE_LOGGING) {
	  ldout(m_image_ctx.cct, 20) << "entries to flush wrap around the end of the ring at "
				     << "operation=[" << *operation << "]" << dendl;
	}
	flush_op_log_entries(entries_to_flush);
	entries_to_flush.clear();
	now = ceph_clock_now();
      }
    }
    if (RWL_VERBOSE_LOGGING) {
      ldout(m_image_ctx.cct, 20) << "Copying entry for operation at index="
				 << operation->get_log_entry()->log_entry_index << " "
				 << "from " << &operation->get_log_entry()->ram_entry << " "
				 << "to " << operation->get_log_entry()->pmem_entry << " "
				 << "operation=[" << *operation << "]" << dendl;
    }
    if (RWL_VERBOSE_LOGGING) {
      ldout(m_image_ctx.cct, 05) << "APPENDING: index="
				 << operation->get_log_entry()->log_entry_index << " "
				 << "operation=[" << *operation << "]" << dendl;
    }
    operation->m_log_append_time = now;
    *operation->get_log_entry()->pmem_entry = operation->get_log_entry()->ram_entry;
    if (RWL_VERBOSE_LOGGING) {
      ldout(m_image_ctx.cct, 20) << "APPENDING: index="
				 << operation->get_log_entry()->log_entry_index << " "
				 << "pmem_entry=[" << *operation->get_log_entry()->pmem_entry << "]" << dendl;
    }
    entries_to_flush.push_back(operation);
  }
  flush_op_log_entries(entries_to_flush);

  /* Drain once for all */
  pmemobj_drain(m_internal->m_log_pool);

  /*
   * Atomically advance the log head pointer and publish the
   * allocations for all the data buffers they refer to.
   */
  utime_t tx_start = ceph_clock_now();
  TX_BEGIN(m_internal->m_log_pool) {
    D_RW(pool_root)->first_free_entry = m_first_free_entry;
    for (auto &operation : ops) {
      if (operation->is_write() || operation->is_writesame()) {
	auto write_op = (std::shared_ptr<WriteLogOperationT>&) operation;
	pmemobj_tx_publish(&write_op->buffer_alloc->buffer_alloc_action, 1);
      } else {
	if (RWL_VERBOSE_LOGGING) {
	  ldout(m_image_ctx.cct, 20) << "skipping non-write op: " << *operation << dendl;
	}
      }
    }
  } TX_ONCOMMIT {
  } TX_ONABORT {
    lderr(cct) << "failed to commit " << ops.size() << " log entries (" << m_log_pool_name << ")" << dendl;
    assert(false);
    ret = -EIO;
  } TX_FINALLY {
  } TX_END;

  utime_t tx_end = ceph_clock_now();
  m_perfcounter->tinc(l_librbd_rwl_append_tx_t, tx_end - tx_start);
  m_perfcounter->hinc(l_librbd_rwl_append_tx_t_hist, utime_t(tx_end - tx_start).to_nsec(), ops.size());
  for (auto &operation : ops) {
    operation->m_log_append_comp_time = tx_end;
  }

  return ret;
}

/*
 * Complete a set of write ops with the result of append_op_entries.
 */
template <typename I>
void ReplicatedWriteLog<I>::complete_op_log_entries(GenericLogOperationsT &&ops, const int result)
{
  GenericLogEntries dirty_entries;
  int published_reserves = 0;
  if (RWL_VERBOSE_LOGGING) {
    ldout(m_image_ctx.cct, 20) << __func__ << ": completing" << dendl;
  }
  for (auto &op : ops) {
    utime_t now = ceph_clock_now();
    auto log_entry = op->get_log_entry();
    log_entry->completed = true;
    if (op->is_writing_op()) {
      op->get_gen_write_op()->sync_point->log_entry->m_writes_completed++;
      dirty_entries.push_back(log_entry);
    }
    if (op->is_write() || op->is_writesame()) {
      published_reserves++;
    }
    if (op->is_discard()) {
      if (RWL_VERBOSE_LOGGING) {
	ldout(m_image_ctx.cct, 20) << __func__ << ": completing discard" << dendl;
      }
    }
    op->complete(result);
    if (op->is_write()) {
      m_perfcounter->tinc(l_librbd_rwl_log_op_dis_to_buf_t, op->m_buf_persist_time - op->m_dispatch_time);
    }
    m_perfcounter->tinc(l_librbd_rwl_log_op_dis_to_app_t, op->m_log_append_time - op->m_dispatch_time);
    m_perfcounter->tinc(l_librbd_rwl_log_op_dis_to_cmp_t, now - op->m_dispatch_time);
    m_perfcounter->hinc(l_librbd_rwl_log_op_dis_to_cmp_t_hist, utime_t(now - op->m_dispatch_time).to_nsec(),
			log_entry->ram_entry.write_bytes);
    if (op->is_write()) {
      utime_t buf_lat = op->m_buf_persist_comp_time - op->m_buf_persist_time;
      m_perfcounter->tinc(l_librbd_rwl_log_op_buf_to_bufc_t, buf_lat);
      m_perfcounter->hinc(l_librbd_rwl_log_op_buf_to_bufc_t_hist, buf_lat.to_nsec(),
			  log_entry->ram_entry.write_bytes);
      m_perfcounter->tinc(l_librbd_rwl_log_op_buf_to_app_t, op->m_log_append_time - op->m_buf_persist_time);
    }
    utime_t app_lat = op->m_log_append_comp_time - op->m_log_append_time;
    m_perfcounter->tinc(l_librbd_rwl_log_op_app_to_appc_t, app_lat);
    m_perfcounter->hinc(l_librbd_rwl_log_op_app_to_appc_t_hist, app_lat.to_nsec(),
			log_entry->ram_entry.write_bytes);
    m_perfcounter->tinc(l_librbd_rwl_log_op_app_to_cmp_t, now - op->m_log_append_time);
  }

  {
    Mutex::Locker locker(m_lock);
    m_unpublished_reserves -= published_reserves;
    m_dirty_log_entries.splice(m_dirty_log_entries.end(), dirty_entries);

    /* New entries may be flushable */
    wake_up();
  }
}

/*
 * Push op log entry completion to a WQ.
 */
template <typename I>
void ReplicatedWriteLog<I>::schedule_complete_op_log_entries(GenericLogOperationsT &&ops, const int result)
{
  if (RWL_VERBOSE_LOGGING) {
    ldout(m_image_ctx.cct, 20) << dendl;
  }
  m_async_complete_ops++;
  m_async_op_tracker.start_op();
  Context *complete_ctx = new FunctionContext([this, ops=move(ops), result](int r) {
      auto captured_ops = std::move(ops);
      complete_op_log_entries(std::move(captured_ops), result);
      m_async_complete_ops--;
      m_async_op_tracker.finish_op();
    });
  if (use_finishers) {
    m_on_persist_finisher.queue(complete_ctx);
  } else {
    m_work_queue.queue(complete_ctx);
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

  assert(!rwl.m_lock.is_locked_by_me());
  assert(!m_resources.allocated);
  m_resources.buffers.reserve(this->m_image_extents.size());
  {
    Mutex::Locker locker(rwl.m_lock);
    if (rwl.m_free_lanes < this->m_image_extents.size()) {
      this->m_waited_lanes = true;
      if (RWL_VERBOSE_LOGGING) {
	ldout(rwl.m_image_ctx.cct, 20) << "not enough free lanes (need "
				       <<  this->m_image_extents.size()
				       << ", have " << rwl.m_free_lanes << ") "
				       << *this << dendl;
      }
      alloc_succeeds = false;
      /* This isn't considered a "no space" alloc fail. Lanes are a throttling mechanism. */
    }
    if (rwl.m_free_log_entries < this->m_image_extents.size()) {
      this->m_waited_entries = true;
      if (RWL_VERBOSE_LOGGING) {
	ldout(rwl.m_image_ctx.cct, 20) << "not enough free entries (need "
				       <<  this->m_image_extents.size()
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
    for (auto &buffer : m_resources.buffers) {
      bytes_allocated += buffer.allocation_size;
      utime_t before_reserve = ceph_clock_now();
      buffer.buffer_oid = pmemobj_reserve(rwl.m_internal->m_log_pool,
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
    unsigned int num_extents = this->m_image_extents.size();
    Mutex::Locker locker(rwl.m_lock);
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
      m_resources.allocated = true;
    } else {
      alloc_succeeds = false;
    }
  }

  if (!alloc_succeeds) {
    /* On alloc failure, free any buffers we did allocate */
    for (auto &buffer : m_resources.buffers) {
      if (buffer.allocated) {
	pmemobj_cancel(rwl.m_internal->m_log_pool, &buffer.buffer_alloc_action, 1);
      }
    }
    m_resources.buffers.clear();
    if (no_space) {
      /* Expedite flushing and/or retiring */
      Mutex::Locker locker(rwl.m_lock);
      rwl.m_alloc_failed_since_retire = true;
      rwl.m_last_alloc_fail = ceph_clock_now();
    }
  }

 this->m_allocated_time = alloc_start;
  return alloc_succeeds;
}

/**
 * Dispatch as many deferred writes as possible
 */
template <typename I>
void ReplicatedWriteLog<I>::dispatch_deferred_writes(void)
{
  C_BlockIORequestT *front_req = nullptr;     /* req still on front of deferred list */
  C_BlockIORequestT *allocated_req = nullptr; /* req that was allocated, and is now off the list */
  bool allocated = false; /* front_req allocate succeeded */
  bool cleared_dispatching_flag = false;

  /* If we can't become the dispatcher, we'll exit */
  {
    Mutex::Locker locker(m_lock);
    if (m_dispatching_deferred_ops ||
	!m_deferred_ios.size()) {
      return;
    }
    m_dispatching_deferred_ops = true;
  }

  /* There are ops to dispatch, and this should be the only thread dispatching them */
  {
    Mutex::Locker deferred_dispatch(m_deferred_dispatch_lock);
    do {
      {
	Mutex::Locker locker(m_lock);
	assert(m_dispatching_deferred_ops);
	if (allocated) {
	  /* On the 2..n-1 th time we get m_lock, front_req->alloc_resources() will
	   * have succeeded, and we'll need to pop it off the deferred ops list
	   * here. */
	  assert(front_req);
	  assert(!allocated_req);
	  m_deferred_ios.pop_front();
	  allocated_req = front_req;
	  front_req = nullptr;
	  allocated = false;
	}
	assert(!allocated);
	if (!allocated && front_req) {
	  /* front_req->alloc_resources() failed on the last iteration. We'll stop dispatching. */
	  front_req = nullptr;
	  assert(!cleared_dispatching_flag);
	  m_dispatching_deferred_ops = false;
	  cleared_dispatching_flag = true;
	} else {
	  assert(!front_req);
	  if (m_deferred_ios.size()) {
	    /* New allocation candidate */
	    front_req = m_deferred_ios.front();
	  } else {
	    assert(!cleared_dispatching_flag);
	    m_dispatching_deferred_ops = false;
	    cleared_dispatching_flag = true;
	  }
	}
      }
      /* Try allocating for front_req before we decide what to do with allocated_req
       * (if any) */
      if (front_req) {
	assert(!cleared_dispatching_flag);
	allocated = front_req->alloc_resources();
      }
      if (allocated_req && front_req && allocated) {
	/* Push dispatch of the first allocated req to a wq */
	m_work_queue.queue(new FunctionContext(
	  [this, allocated_req](int r) {
	    allocated_req->dispatch();
	  }), 0);
	allocated_req = nullptr;
      }
      assert(!(allocated_req && front_req && allocated));

      /* Continue while we're still considering the front of the deferred ops list */
    } while (front_req);
    assert(!allocated);
  }
  ceph_assert(cleared_dispatching_flag);

  /* If any deferred requests were allocated, the last one will still be in allocated_req */
  if (allocated_req) {
    allocated_req->dispatch();
  }
}

/**
 * Returns the lanes used by this write, and attempts to dispatch the next
 * deferred write
 */
template <typename I>
void ReplicatedWriteLog<I>::release_write_lanes(C_WriteRequestT *write_req)
{
  {
    Mutex::Locker locker(m_lock);
    assert(write_req->m_resources.allocated);
    m_free_lanes += write_req->m_image_extents.size();
    write_req->m_resources.allocated = false;
  }
  dispatch_deferred_writes();
}

/**
 * Attempts to allocate log resources for a write. Write is dispatched if
 * resources are available, or queued if they aren't.
 */
template <typename I>
void ReplicatedWriteLog<I>::alloc_and_dispatch_io_req(C_BlockIORequestT *req)
{
  bool dispatch_here = false;

  {
    /* If there are already deferred writes, queue behind them for resources */
    {
      Mutex::Locker locker(m_lock);
      dispatch_here = m_deferred_ios.empty();
    }
    if (dispatch_here) {
      dispatch_here = req->alloc_resources();
    }
    if (dispatch_here) {
      if (RWL_VERBOSE_LOGGING) {
	ldout(m_image_ctx.cct, 20) << "dispatching" << dendl;
      }
      req->dispatch();
    } else {
      req->deferred();
      {
	Mutex::Locker locker(m_lock);
	m_deferred_ios.push_back(req);
      }
      if (RWL_VERBOSE_LOGGING) {
	ldout(m_image_ctx.cct, 20) << "deferred IOs: " << m_deferred_ios.size() << dendl;
      }
      dispatch_deferred_writes();
    }
  }
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
  auto write_req_sp = shared_from_this();
  this->m_dispatched_time = now;

  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(rwl.m_internal->m_log_pool, struct WriteLogPoolRoot);

  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 15) << "name: " << rwl.m_image_ctx.name << " id: " << rwl.m_image_ctx.id
		   << "write_req=" << this << " cell=" << this->get_cell() << dendl;
  }

  {
    uint64_t buffer_offset = 0;
    Mutex::Locker locker(rwl.m_lock);
    Context *set_complete = this;
    if (use_finishers) {
      set_complete = new C_OnFinisher(this, &rwl.m_on_persist_finisher);
    }
    if ((!rwl.m_persist_on_flush && rwl.m_current_sync_point->log_entry->m_writes_completed) ||
	(rwl.m_current_sync_point->log_entry->m_writes > MAX_WRITES_PER_SYNC_POINT) ||
	(rwl.m_current_sync_point->log_entry->m_bytes > MAX_BYTES_PER_SYNC_POINT)) {
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
    }
    m_op_set =
      make_unique<WriteLogOperationSet<T>>(rwl, now, rwl.m_current_sync_point, rwl.m_persist_on_flush,
					   this->m_image_extents_summary.block_extent(), set_complete);
    if (RWL_VERBOSE_LOGGING) {
      ldout(cct, 20) << "write_req=" << this << " m_op_set=" << m_op_set.get() << dendl;
    }
    assert(m_resources.allocated);
    /* m_op_set->operations initialized differently for plain write or write same */
    this->setup_log_operations();
    auto allocation = m_resources.buffers.begin();
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
      assert(!TOID_IS_NULL(operation->log_entry->ram_entry.write_data));
      operation->log_entry->pmem_buffer = D_RW(operation->log_entry->ram_entry.write_data);
      operation->log_entry->ram_entry.sync_gen_number = rwl.m_current_sync_gen;
      if (m_op_set->m_persist_on_flush) {
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
  m_op_set->m_extent_ops_appending->activate();
  m_op_set->m_extent_ops_persist->activate();

  /* Write data */
  for (auto &operation : m_op_set->operations) {
    /* operation is a shared_ptr, so write_op is only good as long as operation is in scope */
    auto write_op = operation->get_write_op();
    assert(write_op != nullptr);
    bufferlist::iterator i(&write_op->bl);
    rwl.m_perfcounter->inc(l_librbd_rwl_log_op_bytes, write_op->log_entry->write_bytes());
    if (RWL_VERBOSE_LOGGING) {
      ldout(cct, 20) << write_op->bl << dendl;
    }
    i.copy((unsigned)write_op->log_entry->write_bytes(), (char*)write_op->log_entry->pmem_buffer);
  }

  /* Adding these entries to the map of blocks to log entries makes them
   * readable by this application. They aren't persisted yet, so they may
   * disappear after certain failures. We'll indicate our guaratee of the
   * persistence of these writes with the completion of this request, or the
   * following aio_flush(), according to the configured policy. */
  rwl.m_blocks_to_log_entries.add_log_entries(log_entries);

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
      m_op_set->m_persist_on_flush) {
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
    Mutex::Locker locker(rwl.m_lock);
    if (!m_op_set->m_persist_on_flush &&
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
      Context *schedule_append_ctx = new FunctionContext([this, write_req_sp](int r) {
	  write_req_sp->schedule_append();
	});
      m_op_set->sync_point->earlier_sync_point->m_on_sync_point_appending.push_back(schedule_append_ctx);
      append_deferred = true;
    } else {
      /* The prior sync point is done, so we'll schedule append here. If this is
       * persist-on-write, and probably still the caller's thread, we'll use this
       * caller's thread to perform the persist & replication of the payload
       * buffer. */
      m_do_early_flush =
	!(this->m_detained || this->m_queued || this->m_deferred || m_op_set->m_persist_on_flush);
    }
  }
  if (!append_deferred) {
    this->schedule_append();
  }
}

template <typename I>
void ReplicatedWriteLog<I>::aio_write(Extents &&image_extents,
				      bufferlist&& bl,
				      int fadvise_flags,
				      Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 20) << "aio_write" << dendl;
  }
  utime_t now = ceph_clock_now();
  m_perfcounter->inc(l_librbd_rwl_wr_req, 1);

  assert(m_initialized);
  {
    RWLock::RLocker image_locker(m_image_ctx.image_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  if (ExtentsSummary<Extents>(image_extents).total_bytes == 0) {
    on_finish->complete(0);
    return;
  }

  auto *write_req =
    C_WriteRequestT::create(*this, now, std::move(image_extents), std::move(bl), fadvise_flags, on_finish).get();
  m_perfcounter->inc(l_librbd_rwl_wr_bytes, write_req->m_image_extents_summary.total_bytes);

  /* The lambda below will be called when the block guard for all
   * blocks affected by this write is obtained */
  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext([this, write_req](GuardedRequestFunctionContext &guard_ctx) {
      write_req->blockguard_acquired(guard_ctx);
      alloc_and_dispatch_io_req(write_req);
    });

  detain_guarded_request(GuardedRequest(write_req->m_image_extents_summary.block_extent(),
					guarded_ctx));
}

template <typename T>
bool C_DiscardRequest<T>::alloc_resources() {
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << "req type=" << get_name() << " "
				   << "req=[" << *this << "]" << dendl;
  }
  assert(!m_log_entry_allocated);
  bool allocated_here = false;
  Mutex::Locker locker(rwl.m_lock);
  if (rwl.m_free_log_entries) {
    rwl.m_free_log_entries--;
    /* No bytes are allocated for a discard, but we count the discarded bytes
     * as dirty.  This means it's possible to have more bytes dirty than
     * there are bytes cached or allocated. */
    rwl.m_bytes_dirty += op->log_entry->bytes_dirty();
    m_log_entry_allocated = true;
    allocated_here = true;
  }
  return allocated_here;
}

template <typename T>
void C_DiscardRequest<T>::dispatch() {
  utime_t now = ceph_clock_now();
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << "req type=" << get_name() << " "
				   << "req=[" << *this << "]" << dendl;
  }
  assert(m_log_entry_allocated);
  this->m_dispatched_time = now;

  rwl.m_blocks_to_log_entries.add_log_entry(op->log_entry);

  rwl.m_perfcounter->inc(l_librbd_rwl_log_ops, 1);
  rwl.schedule_append(op);
}

template <typename I>
void ReplicatedWriteLog<I>::aio_discard(uint64_t offset, uint64_t length,
					uint32_t discard_granularity_bytes, Context *on_finish) {
  utime_t now = ceph_clock_now();
  Extents discard_extents = {{offset, length}};
  m_perfcounter->inc(l_librbd_rwl_discard, 1);

  CephContext *cct = m_image_ctx.cct;
  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 20) << "name: " << m_image_ctx.name << " id: " << m_image_ctx.id
		   << "offset=" << offset << ", "
		   << "length=" << length << ", "
		   << "on_finish=" << on_finish << dendl;
  }

  assert(m_initialized);
  {
    RWLock::RLocker image_locker(m_image_ctx.image_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  if (length == 0) {
    on_finish->complete(0);
    return;
  }

  auto discard_req_sp =
    C_DiscardRequestT::create(*this, now, std::move(discard_extents), discard_granularity_bytes, on_finish);
  auto *discard_req = discard_req_sp.get();
  // TODO: Add discard stats
  //m_perfcounter->inc(l_librbd_rwl_wr_bytes, write_req->m_image_extents_summary.total_bytes);
  {
    Mutex::Locker locker(m_lock);
    discard_req->op = std::make_shared<DiscardLogOperationT>(*this, m_current_sync_point,
							     offset, length, now);

    discard_req->op->log_entry->ram_entry.sync_gen_number = m_current_sync_gen;
    if (m_persist_on_flush) {
      /* Persist on flush. Sequence #0 is never used. */
      discard_req->op->log_entry->ram_entry.write_sequence_number = 0;
    } else {
      /* Persist on write */
      discard_req->op->log_entry->ram_entry.write_sequence_number = ++m_last_op_sequence_num;
      discard_req->op->log_entry->ram_entry.sequenced = 1;
    }
  }
  discard_req->op->on_write_persist = new FunctionContext(
    [this, discard_req_sp](int r) {
      if (RWL_VERBOSE_LOGGING) {
	ldout(m_image_ctx.cct, 20) << "discard_req=" << discard_req_sp
				   << " cell=" << discard_req_sp->get_cell() << dendl;
      }
      assert(discard_req_sp->get_cell());
      discard_req_sp->complete_user_request(r);

      /* Completed to caller by here */
      // TODO: add discard timing stats
      //utime_t now = ceph_clock_now();
      //m_perfcounter->tinc(l_librbd_rwl_aio_flush_latency, now - flush_req->m_arrived_time);

      discard_req_sp->release_cell();
    });

  /* The lambda below will be called when the block guard for all
   * blocks affected by this write is obtained */
  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext([this, discard_req](GuardedRequestFunctionContext &guard_ctx) {
      CephContext *cct = m_image_ctx.cct;
      if (RWL_VERBOSE_LOGGING) {
	ldout(cct, 20) << __func__ << " discard_req=" << discard_req << " cell=" << guard_ctx.m_cell << dendl;
      }

      assert(guard_ctx.m_cell);
      discard_req->m_detained = guard_ctx.m_state.detained;
      discard_req->set_cell(guard_ctx.m_cell);
      // TODO: more missing discard stats
      if (discard_req->m_detained) {
	//m_perfcounter->inc(l_librbd_rwl_wr_req_overlap, 1);
      }
      alloc_and_dispatch_io_req(discard_req);
    });

  detain_guarded_request(GuardedRequest(discard_req->m_image_extents_summary.block_extent(),
					guarded_ctx));
}

template <typename T>
bool C_FlushRequest<T>::alloc_resources() {
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << "req type=" << get_name() << " "
				   << "req=[" << *this << "]" << dendl;
  }

  assert(!m_log_entry_allocated);
  bool allocated_here = false;
  Mutex::Locker locker(rwl.m_lock);
  if (rwl.m_free_log_entries) {
    rwl.m_free_log_entries--;
    m_log_entry_allocated = true;
    allocated_here = true;
  }
  return allocated_here;
}

template <typename T>
void C_FlushRequest<T>::dispatch() {
  utime_t now = ceph_clock_now();
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << "req type=" << get_name() << " "
				   << "req=[" << *this << "]" << dendl;
  }
  assert(m_log_entry_allocated);
  this->m_dispatched_time = now;

  op = std::make_shared<SyncPointLogOperation<T>>(rwl, to_append, now);

  rwl.m_perfcounter->inc(l_librbd_rwl_log_ops, 1);
  rwl.schedule_append(op);
}

template <typename I>
C_FlushRequest<ReplicatedWriteLog<I>>* ReplicatedWriteLog<I>::make_flush_req(Context *on_finish) {
  utime_t flush_begins = ceph_clock_now();
  bufferlist bl;

  auto *flush_req =
    C_FlushRequestT::create(*this, flush_begins, Extents({whole_volume_extent()}),
			    std::move(bl), 0, on_finish).get();

  return flush_req;
}

/* Make a new sync point and flush the previous during initialization, when there may or may
 * not be a previous sync point */
template <typename I>
void ReplicatedWriteLog<I>::init_flush_new_sync_point(DeferredContexts &later) {
  assert(m_lock.is_locked_by_me());
  assert(!m_initialized); /* Don't use this after init */

  if (!m_current_sync_point) {
    /* First sync point since start */
    new_sync_point(later);
  } else {
    flush_new_sync_point(nullptr, later);
  }
}

template <typename I>
void ReplicatedWriteLog<I>::flush_new_sync_point(C_FlushRequestT *flush_req, DeferredContexts &later) {
  assert(m_lock.is_locked_by_me());

  if (!flush_req) {
    m_async_null_flush_finish++;
    m_async_op_tracker.start_op();
    Context *flush_ctx = new FunctionContext([this](int r) {
	m_async_null_flush_finish--;
	m_async_op_tracker.finish_op();
      });
    flush_req = make_flush_req(flush_ctx);
    flush_req->m_internal = true;
  }

  /* Add a new sync point. */
  new_sync_point(later);
  std::shared_ptr<SyncPointT> to_append = m_current_sync_point->earlier_sync_point;
  assert(to_append);

  /* This flush request will append/persist the (now) previous sync point */
  flush_req->to_append = to_append;
  to_append->m_append_scheduled = true;

  /* All prior sync points that are still in this list must already be scheduled for append */
  std::shared_ptr<SyncPointT> previous = to_append->earlier_sync_point;
  while (previous) {
    assert(previous->m_append_scheduled);
    previous = previous->earlier_sync_point;
  }

  /* When the m_sync_point_persist Gather completes this sync point can be
   * appended.  The only sub for this Gather is the finisher Context for
   * m_prior_log_entries_persisted, which records the result of the Gather in
   * the sync point, and completes. TODO: Do we still need both of these
   * Gathers?*/
  to_append->m_sync_point_persist->
    set_finisher(new FunctionContext([this, flush_req](int r) {
	  if (RWL_VERBOSE_LOGGING) {
	    ldout(m_image_ctx.cct, 20) << "Flush req=" << flush_req
				       << " sync point =" << flush_req->to_append
				       << ". Ready to persist." << dendl;
	  }
	  alloc_and_dispatch_io_req(flush_req);
	}));

  /* The m_sync_point_persist Gather has all the subs it will ever have, and
   * now has its finisher. If the sub is already complete, activation will
   * complete the Gather. The finisher will acquire m_lock, so we'll activate
   * this when we release m_lock.*/
  later.add(new FunctionContext([this, to_append](int r) {
	to_append->m_sync_point_persist->activate();
      }));

  /* The flush request completes when the sync point persists */
  to_append->m_on_sync_point_persisted.push_back(flush_req);
}

template <typename I>
void ReplicatedWriteLog<I>::flush_new_sync_point_if_needed(C_FlushRequestT *flush_req, DeferredContexts &later) {
  assert(m_lock.is_locked_by_me());

  /* If there have been writes since the last sync point ... */
  if (m_current_sync_point->log_entry->m_writes) {
    flush_new_sync_point(flush_req, later);
  } else {
    /* There have been no writes to the current sync point. */
    if (m_current_sync_point->earlier_sync_point) {
      /* If previous sync point hasn't completed, complete this flush
       * with the earlier sync point. No alloc or dispatch needed. */
      m_current_sync_point->earlier_sync_point->m_on_sync_point_persisted.push_back(flush_req);
      assert(m_current_sync_point->earlier_sync_point->m_append_scheduled);
    } else {
      /* The previous sync point has already completed and been
       * appended. The current sync point has no writes, so this flush
       * has nothing to wait for. This flush completes now. */
      later.add(flush_req);
    }
  }
}

/**
 * Aio_flush completes when all previously completed writes are
 * flushed to persistent cache. We make a best-effort attempt to also
 * defer until all in-progress writes complete, but we may not know
 * about all of the writes the application considers in-progress yet,
 * due to uncertainty in the IO submission workq (multiple WQ threads
 * may allow out-of-order submission).
 *
 * This flush operation will not wait for writes deferred for overlap
 * in the block guard.
 */
template <typename I>
void ReplicatedWriteLog<I>::aio_flush(io::FlushSource flush_source, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 2) << "on_finish=" << on_finish << " flush_source=" << flush_source << dendl;
  }

  switch (flush_source) {
  case io::FLUSH_SOURCE_SHUTDOWN:
    if (m_flush_on_close) {
      /* Normally we flush RWL to the cache below on close */
      internal_flush(on_finish, false, false);
      return;
    } else {
      /* If flushing to RADOS on close is disabled, do a USER flush
	 and complete on another thread. */
      ldout(cct, 2) << "flush on close supressed" << dendl;
      on_finish = new FunctionContext([this, on_finish](int r) {
	  m_image_ctx.op_work_queue->queue(on_finish, r);
	});
      break;
    }
  case io::FLUSH_SOURCE_INTERNAL:
    internal_flush(on_finish, false, false);
    return;
  case io::FLUSH_SOURCE_USER:
  case io::FLUSH_SOURCE_WRITEBACK:
    break;
  }
  m_perfcounter->inc(l_librbd_rwl_aio_flush, 1);

  /* May be called even if initialization fails */
  if (!m_initialized) {
    ldout(cct, 05) << "never initialized" << dendl;
    /* Deadlock if completed here */
    m_image_ctx.op_work_queue->queue(on_finish);
    return;
  }

  {
    RWLock::RLocker image_locker(m_image_ctx.image_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  auto flush_req = make_flush_req(on_finish);

  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext([this, flush_req](GuardedRequestFunctionContext &guard_ctx) {
      if (RWL_VERBOSE_LOGGING) {
	ldout(m_image_ctx.cct, 20) << "flush_req=" << flush_req << " cell=" << guard_ctx.m_cell << dendl;
      }
      assert(guard_ctx.m_cell);
      flush_req->m_detained = guard_ctx.m_state.detained;
      /* We don't call flush_req->set_cell(), because the block guard will be released here */
      if (flush_req->m_detained) {
	//m_perfcounter->inc(l_librbd_rwl_aio_flush_overlap, 1);
      }
      {
	DeferredContexts post_unlock; /* Do these when the lock below is released */
	Mutex::Locker locker(m_lock);

	if (!m_flush_seen) {
	  ldout(m_image_ctx.cct, 15) << "flush seen" << dendl;
	  m_flush_seen = true;
	  if (!m_persist_on_flush && m_persist_on_write_until_flush) {
	    m_persist_on_flush = true;
	    ldout(m_image_ctx.cct, 5) << "now persisting on flush" << dendl;
	  }
	}

	/*
	 * Create a new sync point if there have been writes since the last
	 * one.
	 *
	 * We do not flush the caches below the RWL here.
	 */
	flush_new_sync_point_if_needed(flush_req, post_unlock);
      }

      release_guarded_request(guard_ctx.m_cell);
    });

  detain_guarded_request(GuardedRequest(flush_req->m_image_extents_summary.block_extent(),
					guarded_ctx, true));
}

template <typename I>
void ReplicatedWriteLog<I>::aio_writesame(uint64_t offset, uint64_t length,
					  bufferlist&& bl, int fadvise_flags,
					  Context *on_finish) {
  utime_t now = ceph_clock_now();
  Extents ws_extents = {{offset, length}};
  m_perfcounter->inc(l_librbd_rwl_ws, 1);
  assert(m_initialized);
  {
    RWLock::RLocker image_locker(m_image_ctx.image_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  if ((0 == length) || bl.length() == 0) {
    /* Zero length write or pattern */
    on_finish->complete(0);
    return;
  }

  if (length % bl.length()) {
    /* Length must be integer multiple of pattern length */
    on_finish->complete(-EINVAL);
    return;
  }

  ldout(m_image_ctx.cct, 06) << "name: " << m_image_ctx.name << " id: " << m_image_ctx.id
			     << "offset=" << offset << ", "
			     << "length=" << length << ", "
			     << "data_len=" << bl.length() << ", "
			     << "on_finish=" << on_finish << dendl;

  /* A write same request is also a write request. The key difference is the
   * write same data buffer is shorter than the extent of the request. The full
   * extent will be used in the block guard, and appear in
   * m_blocks_to_log_entries_map. The data buffer allocated for the WS is only
   * as long as the length of the bl here, which is the pattern that's repeated
   * in the image for the entire length of this WS. Read hits and flushing of
   * write sames are different than normal writes. */
  auto *ws_req =
    C_WriteSameRequestT::create(*this, now, std::move(ws_extents), std::move(bl), fadvise_flags, on_finish).get();
  m_perfcounter->inc(l_librbd_rwl_ws_bytes, ws_req->m_image_extents_summary.total_bytes);

  /* The lambda below will be called when the block guard for all
   * blocks affected by this write is obtained */
  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext([this, ws_req](GuardedRequestFunctionContext &guard_ctx) {
      ws_req->blockguard_acquired(guard_ctx);
      alloc_and_dispatch_io_req(ws_req);
    });

  detain_guarded_request(GuardedRequest(ws_req->m_image_extents_summary.block_extent(),
					guarded_ctx));
}

template <typename I>
void ReplicatedWriteLog<I>::aio_compare_and_write(Extents &&image_extents,
						  bufferlist&& cmp_bl,
						  bufferlist&& bl,
						  uint64_t *mismatch_offset,
						  int fadvise_flags,
						  Context *on_finish) {
  utime_t now = ceph_clock_now();
  m_perfcounter->inc(l_librbd_rwl_cmp, 1);
  assert(m_initialized);

  unsigned int compare_bytes = ExtentsSummary<Extents>(image_extents).total_bytes;

  if (0 == compare_bytes) {
    on_finish->complete(0);
    return;
  }

  /* Repeat compare buffer as necessary for length of read */
  bufferlist adjusted_cmp_bl;
  for (unsigned int i = 0; i < compare_bytes / cmp_bl.length(); i++) {
    adjusted_cmp_bl.append(cmp_bl);
  }
  int trailing_partial = compare_bytes % cmp_bl.length();
  if (trailing_partial) {
    bufferlist trailing_cmp_bl;
    trailing_cmp_bl.substr_of(cmp_bl, 0, trailing_partial);
    adjusted_cmp_bl.claim_append(trailing_cmp_bl);
  }

  {
    RWLock::RLocker image_locker(m_image_ctx.image_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  /* A compare and write request is also a write request. We only allocate
   * resources and dispatch this write request if the compare phase
   * succeeds. */
  auto *cw_req =
    C_CompAndWriteRequestT::create(*this, now, std::move(image_extents), std::move(adjusted_cmp_bl), std::move(bl),
				   mismatch_offset, fadvise_flags, on_finish).get();
  m_perfcounter->inc(l_librbd_rwl_cmp_bytes, cw_req->m_image_extents_summary.total_bytes);

  /* The lambda below will be called when the block guard for all
   * blocks affected by this write is obtained */
  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext([this, cw_req](GuardedRequestFunctionContext &guard_ctx) {
      cw_req->blockguard_acquired(guard_ctx);

      auto read_complete_ctx = new FunctionContext(
	[this, cw_req](int r) {
	  if (RWL_VERBOSE_LOGGING) {
	    ldout(m_image_ctx.cct, 20) << "name: " << m_image_ctx.name << " id: " << m_image_ctx.id
				       << "cw_req=" << cw_req << dendl;
	  }

	  /* Compare read_bl to cmp_bl to determine if this will produce a write */
	  if (cw_req->m_cmp_bl.contents_equal(cw_req->m_read_bl)) {
	    /* Compare phase succeeds. Begin write */
	    if (RWL_VERBOSE_LOGGING) {
	      ldout(m_image_ctx.cct, 05) << __func__ << " cw_req=" << cw_req << " compare matched" << dendl;
	    }
	    cw_req->m_compare_succeeded = true;
	    *cw_req->m_mismatch_offset = 0;
	    /* Continue with this request as a write. Blockguard release and
	     * user request completion handled as if this were a plain
	     * write. */
	    alloc_and_dispatch_io_req(cw_req);
	  } else {
	    /* Compare phase fails. Comp-and write ends now. */
	    if (RWL_VERBOSE_LOGGING) {
	      ldout(m_image_ctx.cct, 15) << __func__ << " cw_req=" << cw_req << " compare failed" << dendl;
	    }
	    /* Bufferlist doesn't tell us where they differed, so we'll have to determine that here */
	    assert(cw_req->m_read_bl.length() == cw_req->m_cmp_bl.length());
	    uint64_t bl_index = 0;
	    for (bl_index = 0; bl_index < cw_req->m_cmp_bl.length(); bl_index++) {
	      if (cw_req->m_cmp_bl[bl_index] != cw_req->m_read_bl[bl_index]) {
		if (RWL_VERBOSE_LOGGING) {
		  ldout(m_image_ctx.cct, 15) << __func__ << " cw_req=" << cw_req << " mismatch at " << bl_index << dendl;
		}
		break;
	      }
	    }
	    cw_req->m_compare_succeeded = false;
	    *cw_req->m_mismatch_offset = bl_index;
	    cw_req->complete_user_request(-EILSEQ);
	    cw_req->release_cell();
	    cw_req->complete(0);
	  }
	});

      /* Read phase of comp-and-write must read through RWL */
      Extents image_extents_copy = cw_req->m_image_extents;
      aio_read(std::move(image_extents_copy), &cw_req->m_read_bl, cw_req->fadvise_flags, read_complete_ctx);
    });

  detain_guarded_request(GuardedRequest(cw_req->m_image_extents_summary.block_extent(),
					guarded_ctx));
}

/**
 * Begin a new sync point
 */
template <typename I>
void ReplicatedWriteLog<I>::new_sync_point(DeferredContexts &later) {
  CephContext *cct = m_image_ctx.cct;
  std::shared_ptr<SyncPointT> old_sync_point = m_current_sync_point;
  std::shared_ptr<SyncPointT> new_sync_point;
  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 20) << dendl;
  }

  assert(m_lock.is_locked_by_me());

  /* The first time this is called, if this is a newly created log,
   * this makes the first sync gen number we'll use 1. On the first
   * call for a re-opened log m_current_sync_gen will be the highest
   * gen number from all the sync point entries found in the re-opened
   * log, and this advances to the next sync gen number. */
  ++m_current_sync_gen;

  new_sync_point = std::make_shared<SyncPointT>(*this, m_current_sync_gen);
  m_current_sync_point = new_sync_point;

  /* If this log has been re-opened, old_sync_point will initially be
   * nullptr, but m_current_sync_gen may not be zero. */
  if (old_sync_point) {
    new_sync_point->earlier_sync_point = old_sync_point;
    new_sync_point->log_entry->m_prior_sync_point_flushed = false;
    old_sync_point->log_entry->m_next_sync_point_entry = new_sync_point->log_entry;
    old_sync_point->later_sync_point = new_sync_point;
    old_sync_point->m_final_op_sequence_num = m_last_op_sequence_num;
    if (!old_sync_point->m_appending) {
      /* Append of new sync point deferred until old sync point is appending */
      old_sync_point->m_on_sync_point_appending.push_back(new_sync_point->m_prior_log_entries_persisted->new_sub());
    }
    /* This sync point will acquire no more sub-ops. Activation needs
     * to acquire m_lock, so defer to later*/
    later.add(new FunctionContext(
      [this, old_sync_point](int r) {
	old_sync_point->m_prior_log_entries_persisted->activate();
      }));
  }

  Context *sync_point_persist_ready = new_sync_point->m_sync_point_persist->new_sub();
  new_sync_point->m_prior_log_entries_persisted->
    set_finisher(new FunctionContext([this, new_sync_point, sync_point_persist_ready](int r) {
	  if (RWL_VERBOSE_LOGGING) {
	    ldout(m_image_ctx.cct, 20) << "Prior log entries persisted for sync point =["
				       << new_sync_point << "]" << dendl;
	  }
	  new_sync_point->m_prior_log_entries_persisted_result = r;
	  new_sync_point->m_prior_log_entries_persisted_complete = true;
	  sync_point_persist_ready->complete(r);
	}));

  if (old_sync_point) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(cct,6) << "new sync point = [" << *m_current_sync_point
		   << "], prior = [" << *old_sync_point << "]" << dendl;
    }
  } else {
    ldout(cct,6) << "first sync point = [" << *m_current_sync_point
		 << "]" << dendl;
  }
}

template <typename I>
void ReplicatedWriteLog<I>::get_state(bool &clean, bool &empty, bool &present) {
  /* State of this cache to be recorded in image metadata */
  clean = m_clean;     /* true if there's nothing to flush */
  empty = m_empty;     /* true if there's nothing to invalidate */
  present = m_present; /* true if there's no storage to release */
}

template <typename I>
void ReplicatedWriteLog<I>::wake_up() {
  CephContext *cct = m_image_ctx.cct;
  assert(m_lock.is_locked());

  if (!m_wake_up_enabled) {
    // wake_up is disabled during shutdown after flushing completes
    ldout(m_image_ctx.cct, 6) << "deferred processing disabled" << dendl;
    return;
  }

  if (m_wake_up_requested && m_wake_up_scheduled) {
    return;
  }

  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 20) << dendl;
  }

  /* Wake-up can be requested while it's already scheduled */
  m_wake_up_requested = true;

  /* Wake-up cannot be scheduled if it's already scheduled */
  if (m_wake_up_scheduled) {
    return;
  }
  m_wake_up_scheduled = true;
  m_async_process_work++;
  m_async_op_tracker.start_op();
  m_work_queue.queue(new FunctionContext(
    [this](int r) {
      process_work();
      m_async_op_tracker.finish_op();
      m_async_process_work--;
    }), 0);
}

template <typename I>
void ReplicatedWriteLog<I>::process_work() {
  CephContext *cct = m_image_ctx.cct;
  int max_iterations = 4;
  bool wake_up_requested = false;
  uint64_t aggressive_high_water_bytes = m_bytes_allocated_cap * AGGRESSIVE_RETIRE_HIGH_WATER;
  uint64_t high_water_bytes = m_bytes_allocated_cap * RETIRE_HIGH_WATER;
  uint64_t low_water_bytes = m_bytes_allocated_cap * RETIRE_LOW_WATER;
  uint64_t aggressive_high_water_entries = m_total_log_entries * AGGRESSIVE_RETIRE_HIGH_WATER;
  uint64_t high_water_entries = m_total_log_entries * RETIRE_HIGH_WATER;
  uint64_t low_water_entries = m_total_log_entries * RETIRE_LOW_WATER;

  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 20) << dendl;
  }

  do {
    {
      Mutex::Locker locker(m_lock);
      m_wake_up_requested = false;
    }
    if (m_alloc_failed_since_retire || (m_shutting_down && m_retire_on_close) || m_invalidating ||
        m_bytes_allocated > high_water_bytes || (m_log_entries.size() > high_water_entries)) {
      int retired = 0;
      utime_t started = ceph_clock_now();
      ldout(m_image_ctx.cct, 10) << "alloc_fail=" << m_alloc_failed_since_retire
                                 << ", allocated > high_water="
                                 << (m_bytes_allocated > high_water_bytes)
                                 << ", allocated_entries > high_water="
                                 << (m_log_entries.size() > high_water_entries)
                                 << dendl;
      while (m_alloc_failed_since_retire || (m_shutting_down && m_retire_on_close) || m_invalidating ||
            (m_bytes_allocated > high_water_bytes) ||
            (m_log_entries.size() > high_water_entries) ||
            (((m_bytes_allocated > low_water_bytes) || (m_log_entries.size() > low_water_entries)) &&
            (utime_t(ceph_clock_now() - started).to_msec() < RETIRE_BATCH_TIME_LIMIT_MS))) {
        if (!retire_entries((m_shutting_down || m_invalidating ||
           (m_bytes_allocated > aggressive_high_water_bytes) ||
           (m_log_entries.size() > aggressive_high_water_entries))
            ? MAX_ALLOC_PER_TRANSACTION
            : MAX_FREE_PER_TRANSACTION)) {
          break;
        }
        retired++;
        dispatch_deferred_writes();
        process_writeback_dirty_entries();
      }
      ldout(m_image_ctx.cct, 10) << "Retired " << retired << " entries" << dendl;
    }
    dispatch_deferred_writes();
    process_writeback_dirty_entries();

    {
      Mutex::Locker locker(m_lock);
      wake_up_requested = m_wake_up_requested;
    }
  } while (wake_up_requested && --max_iterations > 0);

  {
    Mutex::Locker locker(m_lock);
    m_wake_up_scheduled = false;
    /* Reschedule if it's still requested */
    if (m_wake_up_requested) {
      wake_up();
    }
  }
}

template <typename I>
bool ReplicatedWriteLog<I>::can_flush_entry(std::shared_ptr<GenericLogEntry> log_entry) {
  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 20) << "" << dendl;
  assert(log_entry->ram_entry.is_writer());
  assert(m_lock.is_locked_by_me());

  if (m_invalidating) return true;

  /* For OWB we can flush entries with the same sync gen number (write between
   * aio_flush() calls) concurrently. Here we'll consider an entry flushable if
   * its sync gen number is <= the lowest sync gen number carried by all the
   * entries currently flushing.
   *
   * If the entry considered here bears a sync gen number lower than a
   * previously flushed entry, the application had to have submitted the write
   * bearing the higher gen number before the write with the lower gen number
   * completed. So, flushing these concurrently is OK.
   *
   * If the entry considered here bears a sync gen number higher than a
   * currently flushing entry, the write with the lower gen number may have
   * completed to the application before the write with the higher sync gen
   * number was submitted, and the application may rely on that completion
   * order for volume consistency. In this case the entry will not be
   * considered flushable until all the entries bearing lower sync gen numbers
   * finish flushing.
   */

  if (m_flush_ops_in_flight &&
      (log_entry->ram_entry.sync_gen_number > m_lowest_flushing_sync_gen)) {
    return false;
  }

  auto gen_write_entry = log_entry->get_gen_write_log_entry();
  if (gen_write_entry &&
      !gen_write_entry->ram_entry.sequenced &&
      (gen_write_entry->sync_point_entry &&
       !gen_write_entry->sync_point_entry->completed)) {
    /* Sync point for this unsequenced writing entry is not persisted */
    return false;
  }

  return (log_entry->completed &&
	  (m_flush_ops_in_flight <= IN_FLIGHT_FLUSH_WRITE_LIMIT) &&
	  (m_flush_bytes_in_flight <= IN_FLIGHT_FLUSH_BYTES_LIMIT));
}

/* Update/persist the last flushed sync point in the log */
template <typename I>
void ReplicatedWriteLog<I>::persist_last_flushed_sync_gen(void)
{
  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_internal->m_log_pool, struct WriteLogPoolRoot);
  uint64_t flushed_sync_gen;

  Mutex::Locker append_locker(m_log_append_lock);
  {
    Mutex::Locker locker(m_lock);
    flushed_sync_gen = m_flushed_sync_gen;
  }

  if (D_RO(pool_root)->flushed_sync_gen < flushed_sync_gen) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(m_image_ctx.cct, 15) << "flushed_sync_gen in log updated from "
				 << D_RO(pool_root)->flushed_sync_gen << " to "
				 << flushed_sync_gen << dendl;
    }
    //tx_start = ceph_clock_now();
    TX_BEGIN(m_internal->m_log_pool) {
      D_RW(pool_root)->flushed_sync_gen = flushed_sync_gen;
    } TX_ONCOMMIT {
    } TX_ONABORT {
      lderr(m_image_ctx.cct) << "failed to commit update of flushed sync point" << dendl;
      assert(false);
    } TX_FINALLY {
    } TX_END;
    //tx_end = ceph_clock_now();
    //assert(last_retired_entry_index == (first_valid_entry - 1) % m_total_log_entries);
  }
}

/* Returns true if the specified SyncPointLogEntry is considered flushed, and
 * the log will be updated to reflect this. */
static const unsigned int HANDLE_FLUSHED_SYNC_POINT_RECURSE_LIMIT = 4;
template <typename I>
bool ReplicatedWriteLog<I>::handle_flushed_sync_point(std::shared_ptr<SyncPointLogEntry> log_entry)
{
  assert(m_lock.is_locked_by_me());
  assert(log_entry);

  if ((log_entry->m_writes_flushed == log_entry->m_writes) &&
      log_entry->completed && log_entry->m_prior_sync_point_flushed &&
      log_entry->m_next_sync_point_entry) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(m_image_ctx.cct, 20) << "All writes flushed up to sync point="
				 << *log_entry << dendl;
    }
    log_entry->m_next_sync_point_entry->m_prior_sync_point_flushed = true;
    /* Don't move the flushed sync gen num backwards. */
    if (m_flushed_sync_gen < log_entry->ram_entry.sync_gen_number) {
      m_flushed_sync_gen = log_entry->ram_entry.sync_gen_number;
    }
    m_async_op_tracker.start_op();
    m_work_queue.queue(new FunctionContext(
      [this, log_entry](int r) {
	bool handled_by_next;
	{
	  Mutex::Locker locker(m_lock);
	  handled_by_next = handle_flushed_sync_point(log_entry->m_next_sync_point_entry);
	}
	if (!handled_by_next) {
	  persist_last_flushed_sync_gen();
	}
	m_async_op_tracker.finish_op();
      }));
    return true;
  }
  return false;
}

template <typename I>
void ReplicatedWriteLog<I>::sync_point_writer_flushed(std::shared_ptr<SyncPointLogEntry> log_entry)
{
  assert(m_lock.is_locked_by_me());
  assert(log_entry);
  log_entry->m_writes_flushed++;

  /* If this entry might be completely flushed, look closer */
  if ((log_entry->m_writes_flushed == log_entry->m_writes) && log_entry->completed) {
    ldout(m_image_ctx.cct, 15) << "All writes flushed for sync point="
			       << *log_entry << dendl;
    handle_flushed_sync_point(log_entry);
  }
}

static const bool COPY_PMEM_FOR_FLUSH = true;
template <typename I>
Context* ReplicatedWriteLog<I>::construct_flush_entry_ctx(std::shared_ptr<GenericLogEntry> log_entry) {
  CephContext *cct = m_image_ctx.cct;
  bool invalidating = m_invalidating; // snapshot so we behave consistently
  bufferlist entry_bl;

  ldout(cct, 20) << "" << dendl;
  assert(log_entry->is_writer());
  if (!(log_entry->is_write() || log_entry->is_discard() || log_entry->is_writesame())) {
    ldout(cct, 02) << "Flushing from log entry=" << *log_entry
		   << " unimplemented" << dendl;
  }
  assert(log_entry->is_write() || log_entry->is_discard() || log_entry->is_writesame());
  assert(m_entry_reader_lock.is_locked());
  assert(m_lock.is_locked_by_me());
  if (!m_flush_ops_in_flight ||
      (log_entry->ram_entry.sync_gen_number < m_lowest_flushing_sync_gen)) {
    m_lowest_flushing_sync_gen = log_entry->ram_entry.sync_gen_number;
  }
  auto gen_write_entry = static_pointer_cast<GeneralWriteLogEntry>(log_entry);
  m_flush_ops_in_flight += 1;
  /* For write same this is the bytes affected bt the flush op, not the bytes transferred */
  m_flush_bytes_in_flight += gen_write_entry->ram_entry.write_bytes;

  gen_write_entry->flushing = true;

  /* Construct bl for pmem buffer now while we hold m_entry_reader_lock */
  if (invalidating || log_entry->is_discard()) {
    /* If we're invalidating the RWL, we don't actually flush, so don't create
     * the buffer.  If we're flushing a discard, we also don't need the
     * buffer. */
  } else {
    assert(log_entry->is_write() || log_entry->is_writesame());
    auto write_entry = static_pointer_cast<WriteLogEntry>(log_entry);
    if (COPY_PMEM_FOR_FLUSH) {
      /* Pass a copy of the pmem buffer to ImageWriteback (which may hang on to the bl evcen after flush()). */
      buffer::list entry_bl_copy;
      write_entry->copy_pmem_bl(m_entry_bl_lock, &entry_bl_copy);
      entry_bl_copy.copy(0, write_entry->write_bytes(), entry_bl);
    } else {
      /* Pass a bl that refers to the pmem buffers to ImageWriteback */
      entry_bl.substr_of(write_entry->get_pmem_bl(m_entry_bl_lock), 0, write_entry->write_bytes());
    }
  }

  /* Flush write completion action */
  Context *ctx = new FunctionContext(
    [this, gen_write_entry, invalidating](int r) {
      {
	Mutex::Locker locker(m_lock);
	gen_write_entry->flushing = false;
	if (r < 0) {
	  lderr(m_image_ctx.cct) << "failed to flush log entry"
				 << cpp_strerror(r) << dendl;
	  m_dirty_log_entries.push_front(gen_write_entry);
	} else {
	  assert(!gen_write_entry->flushed);
	  gen_write_entry->flushed = true;
	  assert(m_bytes_dirty >= gen_write_entry->bytes_dirty());
	  m_bytes_dirty -= gen_write_entry->bytes_dirty();
	  sync_point_writer_flushed(gen_write_entry->sync_point_entry);
	  ldout(m_image_ctx.cct, 20) << "flushed: " << gen_write_entry
	  << " invalidating=" << invalidating << dendl;
	}
	m_flush_ops_in_flight -= 1;
	m_flush_bytes_in_flight -= gen_write_entry->ram_entry.write_bytes;
	wake_up();
      }
    });
  /* Flush through lower cache before completing */
  ctx = new FunctionContext(
    [this, ctx](int r) {
      if (r < 0) {
	lderr(m_image_ctx.cct) << "failed to flush log entry"
			       << cpp_strerror(r) << dendl;
	ctx->complete(r);
      } else {
	m_image_writeback.aio_flush(ctx);
      }
    });

  if (invalidating) {
    /* When invalidating we just do the flush bookkeeping */
    return(ctx);
  } else {
    if (log_entry->is_write()) {
      /* entry_bl is moved through the layers of lambdas here, and ultimately into the
       * m_image_writeback call */
      return new FunctionContext(
	[this, gen_write_entry, entry_bl=move(entry_bl), ctx](int r) {
	  auto captured_entry_bl = std::move(entry_bl);
	  m_image_ctx.op_work_queue->queue(new FunctionContext(
	    [this, gen_write_entry, entry_bl=move(captured_entry_bl), ctx](int r) {
	      auto captured_entry_bl = std::move(entry_bl);
	      ldout(m_image_ctx.cct, 15) << "flushing:" << gen_write_entry
					 << " " << *gen_write_entry << dendl;
	      m_image_writeback.aio_write({{gen_write_entry->ram_entry.image_offset_bytes,
					     gen_write_entry->ram_entry.write_bytes}},
					   std::move(captured_entry_bl), 0, ctx);
	    }));
	});
    } else if (log_entry->is_writesame()) {
      auto ws_entry = static_pointer_cast<WriteSameLogEntry>(log_entry);
      /* entry_bl is moved through the layers of lambdas here, and ultimately into the
       * m_image_writeback call */
      return new FunctionContext(
	[this, ws_entry, entry_bl=move(entry_bl), ctx](int r) {
	  auto captured_entry_bl = std::move(entry_bl);
	  m_image_ctx.op_work_queue->queue(new FunctionContext(
	    [this, ws_entry, entry_bl=move(captured_entry_bl), ctx](int r) {
	      auto captured_entry_bl = std::move(entry_bl);
	      ldout(m_image_ctx.cct, 02) << "flushing:" << ws_entry
					 << " " << *ws_entry << dendl;
	      m_image_writeback.aio_writesame(ws_entry->ram_entry.image_offset_bytes,
					       ws_entry->ram_entry.write_bytes,
					       std::move(captured_entry_bl), 0, ctx);
	    }));
	});
    } else if (log_entry->is_discard()) {
      return new FunctionContext(
	[this, log_entry, ctx](int r) {
	  m_image_ctx.op_work_queue->queue(new FunctionContext(
	    [this, log_entry, ctx](int r) {
	      auto discard_entry = static_pointer_cast<DiscardLogEntry>(log_entry);
	      /* Invalidate from caches below. We always set skip_partial false
	       * here, because we need all the caches below to behave the same
	       * way in terms of discard granularity and alignment so they
	       * remain consistent. */
	      m_image_writeback.aio_discard(discard_entry->ram_entry.image_offset_bytes,
					     discard_entry->ram_entry.write_bytes,
					     1, ctx);
	    }));
	});
    } else {
      lderr(cct) << "Flushing from log entry=" << *log_entry
		 << " unimplemented" << dendl;
      assert(false);
      return nullptr;
    }
  }
}

template <typename I>
void ReplicatedWriteLog<I>::process_writeback_dirty_entries() {
  CephContext *cct = m_image_ctx.cct;
  bool all_clean = false;
  int flushed = 0;

  ldout(cct, 20) << "Look for dirty entries" << dendl;
  {
    DeferredContexts post_unlock;
    RWLock::RLocker entry_reader_locker(m_entry_reader_lock);
    while (flushed < IN_FLIGHT_FLUSH_WRITE_LIMIT) {
      Mutex::Locker locker(m_lock);
      if (m_shutting_down && !m_flush_on_close) {
	ldout(cct, 5) << "Flush during shutdown supressed" << dendl;
	/* Do flush complete only when all flush ops are finished */
	all_clean = !m_flush_ops_in_flight;
	break;
      }
      if (m_dirty_log_entries.empty()) {
	ldout(cct, 20) << "Nothing new to flush" << dendl;
	/* Do flush complete only when all flush ops are finished */
	all_clean = !m_flush_ops_in_flight;
	break;
      }
      auto candidate = m_dirty_log_entries.front();
      bool flushable = can_flush_entry(candidate);
      if (flushable) {
	post_unlock.add(construct_flush_entry_ctx(candidate));
	flushed++;
      }
      if (flushable || !candidate->ram_entry.is_writer()) {
	/* Remove if we're flushing it, or it's not a writer */
	if (!candidate->ram_entry.is_writer()) {
	  ldout(cct, 20) << "Removing non-writing entry from m_dirty_log_entries:"
			 << *m_dirty_log_entries.front() << dendl;
	}
	m_dirty_log_entries.pop_front();
      } else {
	ldout(cct, 20) << "Next dirty entry isn't flushable yet" << dendl;
	break;
      }
    }
  }

  if (all_clean) {
    /* All flushing complete, drain outside lock */
    Contexts flush_contexts;
    {
      Mutex::Locker locker(m_lock);
      flush_contexts.swap(m_flush_complete_contexts);
    }
    finish_contexts(m_image_ctx.cct, flush_contexts, 0);
  }
}

template <typename I>
bool ReplicatedWriteLog<I>::can_retire_entry(std::shared_ptr<GenericLogEntry> log_entry) {
  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 20) << "" << dendl;
  assert(m_lock.is_locked_by_me());
  if (!log_entry->completed) {
    return false;
  }
  if (log_entry->is_write() || log_entry->is_writesame()) {
    auto write_entry = static_pointer_cast<WriteLogEntry>(log_entry);
    return (write_entry->flushed &&
	    0 == write_entry->reader_count());
  } else {
    return true;
  }
}

/**
 * Retire up to MAX_ALLOC_PER_TRANSACTION of the oldest log entries
 * that are eligible to be retired. Returns true if anything was
 * retired.
 */
template <typename I>
bool ReplicatedWriteLog<I>::retire_entries(const unsigned long int frees_per_tx) {
  CephContext *cct = m_image_ctx.cct;
  GenericLogEntriesVector retiring_entries;
  uint32_t initial_first_valid_entry;
  uint32_t first_valid_entry;

  Mutex::Locker retire_locker(m_log_retire_lock);
  ldout(cct, 20) << "Look for entries to retire" << dendl;
  {
    /* Entry readers can't be added while we hold m_entry_reader_lock */
    RWLock::WLocker entry_reader_locker(m_entry_reader_lock);
    Mutex::Locker locker(m_lock);
    initial_first_valid_entry = m_first_valid_entry;
    first_valid_entry = m_first_valid_entry;
    auto entry = m_log_entries.front();
    while (!m_log_entries.empty() &&
	   retiring_entries.size() < frees_per_tx &&
	   can_retire_entry(entry)) {
      assert(entry->completed);
      if (entry->log_entry_index != first_valid_entry) {
	lderr(cct) << "Retiring entry index (" << entry->log_entry_index
		   << ") and first valid log entry index (" << first_valid_entry
		   << ") must be ==." << dendl;
      }
      assert(entry->log_entry_index == first_valid_entry);
      first_valid_entry = (first_valid_entry + 1) % m_total_log_entries;
      m_log_entries.pop_front();
      retiring_entries.push_back(entry);
      /* Remove entry from map so there will be no more readers */
      if (entry->is_write() || entry->is_writesame()) {
	auto write_entry = static_pointer_cast<WriteLogEntry>(entry);
	m_blocks_to_log_entries.remove_log_entry(write_entry);
	assert(!write_entry->flushing);
	assert(write_entry->flushed);
	assert(!write_entry->reader_count());
	assert(!write_entry->referring_map_entries);
      }
      entry = m_log_entries.front();
    }
  }

  if (retiring_entries.size()) {
    ldout(cct, 20) << "Retiring " << retiring_entries.size() << " entries" << dendl;
    TOID(struct WriteLogPoolRoot) pool_root;
    pool_root = POBJ_ROOT(m_internal->m_log_pool, struct WriteLogPoolRoot);

    utime_t tx_start;
    utime_t tx_end;
    /* Advance first valid entry and release buffers */
    {
      uint64_t flushed_sync_gen;
      Mutex::Locker append_locker(m_log_append_lock);
      {
	Mutex::Locker locker(m_lock);
	flushed_sync_gen = m_flushed_sync_gen;
      }
      //uint32_t last_retired_entry_index;

      tx_start = ceph_clock_now();
      TX_BEGIN(m_internal->m_log_pool) {
	if (D_RO(pool_root)->flushed_sync_gen < flushed_sync_gen) {
	  ldout(m_image_ctx.cct, 20) << "flushed_sync_gen in log updated from "
				     << D_RO(pool_root)->flushed_sync_gen << " to "
				     << flushed_sync_gen << dendl;
	  D_RW(pool_root)->flushed_sync_gen = flushed_sync_gen;
	}
	D_RW(pool_root)->first_valid_entry = first_valid_entry;
	for (auto &entry: retiring_entries) {
	  //last_retired_entry_index = entry->log_entry_index;
	  if (entry->is_write() || entry->is_writesame()) {
	    if (RWL_VERBOSE_LOGGING) {
	      ldout(cct, 20) << "Freeing " << entry->ram_entry.write_data.oid.pool_uuid_lo
			     <<	"." << entry->ram_entry.write_data.oid.off << dendl;
	    }
	    TX_FREE(entry->ram_entry.write_data);
	  } else {
	    if (RWL_VERBOSE_LOGGING) {
	      ldout(cct, 20) << "Retiring non-write: " << *entry << dendl;
	    }
	  }
	}
      } TX_ONCOMMIT {
      } TX_ONABORT {
	lderr(cct) << "failed to commit free of" << retiring_entries.size() << " log entries (" << m_log_pool_name << ")" << dendl;
	assert(false);
      } TX_FINALLY {
      } TX_END;
      tx_end = ceph_clock_now();
      //assert(last_retired_entry_index == (first_valid_entry - 1) % m_total_log_entries);
    }
    m_perfcounter->tinc(l_librbd_rwl_retire_tx_t, tx_end - tx_start);
    m_perfcounter->hinc(l_librbd_rwl_retire_tx_t_hist, utime_t(tx_end - tx_start).to_nsec(), retiring_entries.size());

    /* Update runtime copy of first_valid, and free entries counts */
    {
      Mutex::Locker locker(m_lock);

      ceph_assert(m_first_valid_entry == initial_first_valid_entry);
      m_first_valid_entry = first_valid_entry;
      m_free_log_entries += retiring_entries.size();
      for (auto &entry: retiring_entries) {
	if (entry->is_write() || entry->is_writesame()) {
	  assert(m_bytes_cached >= entry->write_bytes());
	  m_bytes_cached -= entry->write_bytes();
	  uint64_t entry_allocation_size = entry->write_bytes();
	  if (entry_allocation_size < MIN_WRITE_ALLOC_SIZE) {
	    entry_allocation_size = MIN_WRITE_ALLOC_SIZE;
	  }
	  assert(m_bytes_allocated >= entry_allocation_size);
	  m_bytes_allocated -= entry_allocation_size;
	} else	if (entry->ram_entry.is_discard()) {
	  /* Discards don't record any allocated or cached bytes,
	   * but do record dirty bytes until they're flushed. */
	}
      }
      m_alloc_failed_since_retire = false;
      wake_up();
    }
  } else {
    ldout(cct, 20) << "Nothing to retire" << dendl;
    return false;
  }
  return true;
}

/*
 * Flushes all dirty log entries, then flushes the cache below. On completion
 * there will be no dirty entries.
 *
 * Optionally invalidates the cache (RWL invalidate interface comes here with
 * invalidate=true). On completion with invalidate=true there will be no entries
 * in the log. Until the next write, all subsequent reads will complete from the
 * layer below. When invalidatingm the cache below is invalidated instead of
 * flushed.
 *
 * If discard_unflushed_writes is true, invalidate must also be true. Unflushed
 * writes are discarded instead of flushed.
*/
template <typename I>
void ReplicatedWriteLog<I>::flush(bool invalidate, bool discard_unflushed_writes, Context *on_finish) {
  ldout(m_image_ctx.cct, 20) << "name: " << m_image_ctx.name << " id: " << m_image_ctx.id
			     << "invalidate=" << invalidate
			     << " discard_unflushed_writes=" << discard_unflushed_writes << dendl;
  if (m_perfcounter) {
    if (discard_unflushed_writes) {
      ldout(m_image_ctx.cct, 1) << "Write back cache discarded (not flushed)" << dendl;
      m_perfcounter->inc(l_librbd_rwl_invalidate_discard_cache, 1);
    } else if (invalidate) {
      m_perfcounter->inc(l_librbd_rwl_invalidate_cache, 1);
    } else {
      m_perfcounter->inc(l_librbd_rwl_flush, 1);
    }
  }

  internal_flush(on_finish, invalidate, discard_unflushed_writes);
}

template <typename I>
void ReplicatedWriteLog<I>::flush(Context *on_finish) {
  auto cache_state = dynamic_cast<ImageCacheStateRWL<I>*>(m_cache_state);
  flush(cache_state->m_invalidate_on_flush, false, on_finish);
};

template <typename I>
void ReplicatedWriteLog<I>::invalidate(bool discard_unflushed_writes, Context *on_finish) {
  flush(true, discard_unflushed_writes, on_finish);
};

template <typename I>
void ReplicatedWriteLog<I>::internal_flush(Context *on_finish, bool invalidate, bool discard_unflushed_writes) {
  ldout(m_image_ctx.cct, 20) << "invalidate=" << invalidate
			     << " discard_unflushed_writes=" << discard_unflushed_writes << dendl;
  if (discard_unflushed_writes) {
    assert(invalidate);
  }

  /* May be called even if initialization fails */
  if (!m_initialized) {
    ldout(m_image_ctx.cct, 05) << "never initialized" << dendl;
    /* Deadlock if completed here */
    m_image_ctx.op_work_queue->queue(on_finish);
    return;
  }

  /* Flush/invalidate must pass through block guard to ensure all layers of
   * cache are consistently flush/invalidated. This ensures no in-flight write leaves
   * some layers with valid regions, which may later produce inconsistent read
   * results. */
  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext(
      [this, on_finish, invalidate, discard_unflushed_writes](GuardedRequestFunctionContext &guard_ctx) {
	DeferredContexts on_exit;
	ldout(m_image_ctx.cct, 20) << "cell=" << guard_ctx.m_cell << dendl;
	assert(guard_ctx.m_cell);

	Context *ctx = new FunctionContext(
	  [this, cell=guard_ctx.m_cell, invalidate, discard_unflushed_writes, on_finish](int r) {
	    Mutex::Locker locker(m_lock);
	    m_invalidating = false;
	    ldout(m_image_ctx.cct, 6) << "Done flush/invalidating (invalidate="
				      << invalidate << " discard="
				      << discard_unflushed_writes << ")" << dendl;
	    if (m_log_entries.size()) {
	      ldout(m_image_ctx.cct, 1) << "m_log_entries.size()=" << m_log_entries.size() << ", "
					<< "front()=" << *m_log_entries.front() << dendl;
	    }
	    if (invalidate) {
	      assert(m_log_entries.size() == 0);
	    }
	    assert(m_dirty_log_entries.size() == 0);
	    m_image_ctx.op_work_queue->queue(on_finish, r);
	    release_guarded_request(cell);
	  });
	ctx = new FunctionContext(
	  [this, ctx, invalidate, discard_unflushed_writes](int r) {
	    Context *next_ctx = ctx;
	    if (r < 0) {
	      /* Override on_finish status with this error */
	      next_ctx = new FunctionContext([r, ctx](int _r) {
		  ctx->complete(r);
		});
	    }
	    if (invalidate) {
	      {
		Mutex::Locker locker(m_lock);
		assert(m_dirty_log_entries.size() == 0);
		if (discard_unflushed_writes) {
		  assert(m_invalidating);
		} else {
		  /* If discard_unflushed_writes was false, we should only now be
		   * setting m_invalidating. All writes are now flushed.  with
		   * m_invalidating set, retire_entries() will proceed without
		   * the normal limits that keep it from interfering with
		   * appending new writes (we hold the block guard, so that can't
		   * be happening). */
		  assert(!m_invalidating);
		  ldout(m_image_ctx.cct, 6) << "Invalidating" << dendl;
		  m_invalidating = true;
		}
	      }
	      /* Discards all RWL entries */
	      while (retire_entries(MAX_ALLOC_PER_TRANSACTION)) { }
	      /* Invalidate from caches below */
	      //m_image_writeback.invalidate(next_ctx);
              next_ctx->complete(0);
	    } else {
	      {
		Mutex::Locker locker(m_lock);
		assert(m_dirty_log_entries.size() == 0);
		  assert(!m_invalidating);
	      }
	      m_image_writeback.aio_flush(next_ctx);
	    }
	  });
	ctx = new FunctionContext(
	  [this, ctx, discard_unflushed_writes](int r) {
	    /* If discard_unflushed_writes was true, m_invalidating should be
	     * set now.
	     *
	     * With m_invalidating set, flush discards everything in the dirty
	     * entries list without writing them to OSDs. It also waits for
	     * in-flight flushes to complete, and keeps the flushing stats
	     * consistent.
	     *
	     * If discard_unflushed_writes was false, this is a normal
	     * flush. */
	    {
		Mutex::Locker locker(m_lock);
		assert(m_invalidating == discard_unflushed_writes);
	    }
	    flush_dirty_entries(ctx);
	  });
	Mutex::Locker locker(m_lock);
	if (discard_unflushed_writes) {
	  ldout(m_image_ctx.cct, 6) << "Invalidating" << dendl;
	  m_invalidating = true;
	}
	/* Even if we're throwing everything away, but we want the last entry to
	 * be a sync point so we can cleanly resume.
	 *
	 * Also, the blockguard only guarantees the replication of this op
	 * can't overlap with prior ops. It doesn't guarantee those are all
	 * completed and eligible for flush & retire, which we require here.
	 */
	auto flush_req = make_flush_req(ctx);
	flush_new_sync_point_if_needed(flush_req, on_exit);
      });
  BlockExtent invalidate_block_extent(block_extent(whole_volume_extent()));
  detain_guarded_request(GuardedRequest(invalidate_block_extent,
					guarded_ctx, true));
}

/*
 * RWL internal flush - will actually flush the RWL.
 *
 * User flushes should arrive at aio_flush(), and only flush prior
 * writes to all log replicas.
 *
 * Librbd internal flushes will arrive at flush(invalidate=false,
 * discard=false), and traverse the block guard to ensure in-flight writes are
 * flushed.
 */
template <typename I>
void ReplicatedWriteLog<I>::flush_dirty_entries(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  bool all_clean;
  bool flushing;
  bool stop_flushing;

  {
    Mutex::Locker locker(m_lock);
    flushing = (0 != m_flush_ops_in_flight);
    all_clean = m_dirty_log_entries.empty();
    stop_flushing = (m_shutting_down && !m_flush_on_close);
  }

  if (!flushing && (all_clean || stop_flushing)) {
    /* Complete without holding m_lock */
    if (all_clean) {
      ldout(cct, 20) << "no dirty entries" << dendl;
    } else {
      ldout(cct, 5) << "flush during shutdown suppressed" << dendl;
    }
    on_finish->complete(0);
  } else {
    if (all_clean) {
      ldout(cct, 5) << "flush ops still in progress" << dendl;
    } else {
      ldout(cct, 20) << "dirty entries remain" << dendl;
    }
    Mutex::Locker locker(m_lock);
    /* on_finish can't be completed yet */
    m_flush_complete_contexts.push_back(new FunctionContext(
      [this, on_finish](int r) {
	flush_dirty_entries(on_finish);
      }));
    wake_up();
  }
}

} // namespace cache
} // namespace librbd

template class librbd::cache::ReplicatedWriteLog<librbd::ImageCtx>;
template class librbd::cache::ImageCacheStateRWL<librbd::ImageCtx>;

/* Local Variables: */
/* eval: (c-set-offset 'innamespace 0) */
/* End: */
