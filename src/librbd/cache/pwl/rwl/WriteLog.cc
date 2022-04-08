// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "WriteLog.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "include/ceph_assert.h"
#include "common/deleter.h"
#include "common/dout.h"
#include "common/environment.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "common/Timer.h"
#include "common/perf_counters.h"
#include "librbd/ImageCtx.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/cache/pwl/ImageCacheState.h"
#include "librbd/cache/pwl/LogEntry.h"
#include "librbd/plugin/Api.h"
#include <map>
#include <vector>

#undef dout_subsys
#define dout_subsys ceph_subsys_rbd_pwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::pwl::rwl::WriteLog: " << this \
                           << " " <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace pwl {
using namespace librbd::cache::pwl;
namespace rwl {

const unsigned long int OPS_APPENDED_TOGETHER = MAX_ALLOC_PER_TRANSACTION;

template <typename I>
Builder<AbstractWriteLog<I>>* WriteLog<I>::create_builder() {
  m_builderobj = new Builder<This>();
  return m_builderobj;
}

template <typename I>
WriteLog<I>::WriteLog(
    I &image_ctx, librbd::cache::pwl::ImageCacheState<I>* cache_state,
    ImageWritebackInterface& image_writeback,
    plugin::Api<I>& plugin_api)
: AbstractWriteLog<I>(image_ctx, cache_state, create_builder(), image_writeback,
                      plugin_api),
  m_pwl_pool_layout_name(POBJ_LAYOUT_NAME(rbd_pwl))
{
}

template <typename I>
WriteLog<I>::~WriteLog() {
  m_log_pool = nullptr;
  delete m_builderobj;
}

template <typename I>
void WriteLog<I>::collect_read_extents(
      uint64_t read_buffer_offset, LogMapEntry<GenericWriteLogEntry> map_entry,
      std::vector<std::shared_ptr<GenericWriteLogEntry>> &log_entries_to_read,
      std::vector<bufferlist*> &bls_to_read, uint64_t entry_hit_length,
      Extent hit_extent, pwl::C_ReadRequest *read_ctx) {
  /* Make a bl for this hit extent. This will add references to the
   * write_entry->pmem_bp */
  buffer::list hit_bl;

  /* Create buffer object referring to pmem pool for this read hit */
  auto write_entry = map_entry.log_entry;

  buffer::list entry_bl_copy;
  write_entry->copy_cache_bl(&entry_bl_copy);
  entry_bl_copy.begin(read_buffer_offset).copy(entry_hit_length, hit_bl);
  ceph_assert(hit_bl.length() == entry_hit_length);

  /* Add hit extent to read extents */
  auto hit_extent_buf = std::make_shared<ImageExtentBuf>(hit_extent, hit_bl);
  read_ctx->read_extents.push_back(hit_extent_buf);
}

template <typename I>
void WriteLog<I>::complete_read(
    std::vector<std::shared_ptr<GenericWriteLogEntry>> &log_entries_to_read,
    std::vector<bufferlist*> &bls_to_read, Context *ctx) {
  ctx->complete(0);
}

/*
 * Allocate the (already reserved) write log entries for a set of operations.
 *
 * Locking:
 * Acquires lock
 */
template <typename I>
void WriteLog<I>::alloc_op_log_entries(GenericLogOperations &ops)
{
  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);
  struct WriteLogCacheEntry *pmem_log_entries = D_RW(D_RW(pool_root)->log_entries);

  ceph_assert(ceph_mutex_is_locked_by_me(this->m_log_append_lock));

  /* Allocate the (already reserved) log entries */
  std::lock_guard locker(m_lock);

  for (auto &operation : ops) {
    uint32_t entry_index = this->m_first_free_entry;
    this->m_first_free_entry = (this->m_first_free_entry + 1) % this->m_total_log_entries;
    auto &log_entry = operation->get_log_entry();
    log_entry->log_entry_index = entry_index;
    log_entry->ram_entry.entry_index = entry_index;
    log_entry->cache_entry = &pmem_log_entries[entry_index];
    log_entry->ram_entry.entry_valid = 1;
    m_log_entries.push_back(log_entry);
    ldout(m_image_ctx.cct, 20) << "operation=[" << *operation << "]" << dendl;
  }
  if (m_cache_state->empty && !m_log_entries.empty()) {
    m_cache_state->empty = false;
    this->update_image_cache_state();
  }
}

/*
 * Write and persist the (already allocated) write log entries and
 * data buffer allocations for a set of ops. The data buffer for each
 * of these must already have been persisted to its reserved area.
 */
template <typename I>
int WriteLog<I>::append_op_log_entries(GenericLogOperations &ops)
{
  CephContext *cct = m_image_ctx.cct;
  GenericLogOperationsVector entries_to_flush;
  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);
  int ret = 0;

  ceph_assert(ceph_mutex_is_locked_by_me(this->m_log_append_lock));

  if (ops.empty()) {
    return 0;
  }
  entries_to_flush.reserve(OPS_APPENDED_TOGETHER);

  /* Write log entries to ring and persist */
  utime_t now = ceph_clock_now();
  for (auto &operation : ops) {
    if (!entries_to_flush.empty()) {
      /* Flush these and reset the list if the current entry wraps to the
       * tail of the ring */
      if (entries_to_flush.back()->get_log_entry()->log_entry_index >
          operation->get_log_entry()->log_entry_index) {
        ldout(m_image_ctx.cct, 20) << "entries to flush wrap around the end of the ring at "
                                   << "operation=[" << *operation << "]" << dendl;
        flush_op_log_entries(entries_to_flush);
        entries_to_flush.clear();
        now = ceph_clock_now();
      }
    }
    ldout(m_image_ctx.cct, 20) << "Copying entry for operation at index="
                               << operation->get_log_entry()->log_entry_index << " "
                               << "from " << &operation->get_log_entry()->ram_entry << " "
                               << "to " << operation->get_log_entry()->cache_entry << " "
                               << "operation=[" << *operation << "]" << dendl;
    ldout(m_image_ctx.cct, 05) << "APPENDING: index="
                               << operation->get_log_entry()->log_entry_index << " "
                               << "operation=[" << *operation << "]" << dendl;
    operation->log_append_start_time = now;
    *operation->get_log_entry()->cache_entry = operation->get_log_entry()->ram_entry;
    ldout(m_image_ctx.cct, 20) << "APPENDING: index="
                               << operation->get_log_entry()->log_entry_index << " "
                               << "pmem_entry=[" << *operation->get_log_entry()->cache_entry
                               << "]" << dendl;
    entries_to_flush.push_back(operation);
  }
  flush_op_log_entries(entries_to_flush);

  /* Drain once for all */
  pmemobj_drain(m_log_pool);

  /*
   * Atomically advance the log head pointer and publish the
   * allocations for all the data buffers they refer to.
   */
  utime_t tx_start = ceph_clock_now();
  TX_BEGIN(m_log_pool) {
    D_RW(pool_root)->first_free_entry = this->m_first_free_entry;
    for (auto &operation : ops) {
      if (operation->reserved_allocated()) {
        auto write_op = (std::shared_ptr<WriteLogOperation>&) operation;
        pmemobj_tx_publish(&write_op->buffer_alloc->buffer_alloc_action, 1);
      } else {
        ldout(m_image_ctx.cct, 20) << "skipping non-write op: " << *operation << dendl;
      }
    }
  } TX_ONCOMMIT {
  } TX_ONABORT {
    lderr(cct) << "failed to commit " << ops.size()
               << " log entries (" << this->m_log_pool_name << ")" << dendl;
    ceph_assert(false);
    ret = -EIO;
  } TX_FINALLY {
  } TX_END;

  utime_t tx_end = ceph_clock_now();
  m_perfcounter->tinc(l_librbd_pwl_append_tx_t, tx_end - tx_start);
  m_perfcounter->hinc(
    l_librbd_pwl_append_tx_t_hist, utime_t(tx_end - tx_start).to_nsec(), ops.size());
  for (auto &operation : ops) {
    operation->log_append_comp_time = tx_end;
  }

  return ret;
}

/*
 * Flush the persistent write log entries set of ops. The entries must
 * be contiguous in persistent memory.
 */
template <typename I>
void WriteLog<I>::flush_op_log_entries(GenericLogOperationsVector &ops)
{
  if (ops.empty()) {
    return;
  }

  if (ops.size() > 1) {
    ceph_assert(ops.front()->get_log_entry()->cache_entry < ops.back()->get_log_entry()->cache_entry);
  }

  ldout(m_image_ctx.cct, 20) << "entry count=" << ops.size() << " "
                             << "start address="
                             << ops.front()->get_log_entry()->cache_entry << " "
                             << "bytes="
                             << ops.size() * sizeof(*(ops.front()->get_log_entry()->cache_entry))
                             << dendl;
  pmemobj_flush(m_log_pool,
                ops.front()->get_log_entry()->cache_entry,
                ops.size() * sizeof(*(ops.front()->get_log_entry()->cache_entry)));
}

template <typename I>
void WriteLog<I>::remove_pool_file() {
  if (m_log_pool) {
    ldout(m_image_ctx.cct, 6) << "closing pmem pool" << dendl;
    pmemobj_close(m_log_pool);
  }
  if (m_cache_state->clean) {
      ldout(m_image_ctx.cct, 5) << "Removing empty pool file: " << this->m_log_pool_name << dendl;
      if (remove(this->m_log_pool_name.c_str()) != 0) {
        lderr(m_image_ctx.cct) << "failed to remove empty pool \"" << this->m_log_pool_name << "\": "
          << pmemobj_errormsg() << dendl;
      } else {
        m_cache_state->present = false;
      }
  } else {
    ldout(m_image_ctx.cct, 5) << "Not removing pool file: " << this->m_log_pool_name << dendl;
  }
}

template <typename I>
bool WriteLog<I>::initialize_pool(Context *on_finish, pwl::DeferredContexts &later) {
  CephContext *cct = m_image_ctx.cct;
  TOID(struct WriteLogPoolRoot) pool_root;
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
  if (access(this->m_log_pool_name.c_str(), F_OK) != 0) {
    if ((m_log_pool =
         pmemobj_create(this->m_log_pool_name.c_str(),
                        this->m_pwl_pool_layout_name,
                        this->m_log_pool_size,
                        (S_IWUSR | S_IRUSR))) == NULL) {
      lderr(cct) << "failed to create pool (" << this->m_log_pool_name << ")"
                 << pmemobj_errormsg() << dendl;
      m_cache_state->present = false;
      m_cache_state->clean = true;
      m_cache_state->empty = true;
      /* TODO: filter/replace errnos that are meaningless to the caller */
      on_finish->complete(-errno);
      return false;
    }
    m_cache_state->present = true;
    m_cache_state->clean = true;
    m_cache_state->empty = true;
    pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);

    /* new pool, calculate and store metadata */
    size_t effective_pool_size = (size_t)(this->m_log_pool_size * USABLE_SIZE);
    size_t small_write_size = MIN_WRITE_ALLOC_SIZE + BLOCK_ALLOC_OVERHEAD_BYTES + sizeof(struct WriteLogCacheEntry);
    uint64_t num_small_writes = (uint64_t)(effective_pool_size / small_write_size);
    if (num_small_writes > MAX_LOG_ENTRIES) {
      num_small_writes = MAX_LOG_ENTRIES;
    }
    if (num_small_writes <= 2) {
      lderr(cct) << "num_small_writes needs to > 2" << dendl;
      on_finish->complete(-EINVAL);
      return false;
    }
    this->m_bytes_allocated_cap = effective_pool_size;
    /* Log ring empty */
    m_first_free_entry = 0;
    m_first_valid_entry = 0;
    TX_BEGIN(m_log_pool) {
      TX_ADD(pool_root);
      D_RW(pool_root)->header.layout_version = RWL_LAYOUT_VERSION;
      D_RW(pool_root)->log_entries =
        TX_ZALLOC(struct WriteLogCacheEntry,
                  sizeof(struct WriteLogCacheEntry) * num_small_writes);
      D_RW(pool_root)->pool_size = this->m_log_pool_size;
      D_RW(pool_root)->flushed_sync_gen = this->m_flushed_sync_gen;
      D_RW(pool_root)->block_size = MIN_WRITE_ALLOC_SIZE;
      D_RW(pool_root)->num_log_entries = num_small_writes;
      D_RW(pool_root)->first_free_entry = m_first_free_entry;
      D_RW(pool_root)->first_valid_entry = m_first_valid_entry;
    } TX_ONCOMMIT {
      this->m_total_log_entries = D_RO(pool_root)->num_log_entries;
      this->m_free_log_entries = D_RO(pool_root)->num_log_entries - 1; // leave one free
    } TX_ONABORT {
      this->m_total_log_entries = 0;
      this->m_free_log_entries = 0;
      lderr(cct) << "failed to initialize pool (" << this->m_log_pool_name << ")" << dendl;
      on_finish->complete(-pmemobj_tx_errno());
      return false;
    } TX_FINALLY {
    } TX_END;
  } else {
    ceph_assert(m_cache_state->present);
    /* Open existing pool */
    if ((m_log_pool =
         pmemobj_open(this->m_log_pool_name.c_str(),
                      this->m_pwl_pool_layout_name)) == NULL) {
      lderr(cct) << "failed to open pool (" << this->m_log_pool_name << "): "
                 << pmemobj_errormsg() << dendl;
      on_finish->complete(-errno);
      return false;
    }
    pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);
    if (D_RO(pool_root)->header.layout_version != RWL_LAYOUT_VERSION) {
      // TODO: will handle upgrading version in the future
      lderr(cct) << "Pool layout version is "
                 << D_RO(pool_root)->header.layout_version
                 << " expected " << RWL_LAYOUT_VERSION << dendl;
      on_finish->complete(-EINVAL);
      return false;
    }
    if (D_RO(pool_root)->block_size != MIN_WRITE_ALLOC_SIZE) {
      lderr(cct) << "Pool block size is " << D_RO(pool_root)->block_size
                 << " expected " << MIN_WRITE_ALLOC_SIZE << dendl;
      on_finish->complete(-EINVAL);
      return false;
    }
    this->m_log_pool_size = D_RO(pool_root)->pool_size;
    this->m_flushed_sync_gen = D_RO(pool_root)->flushed_sync_gen;
    this->m_total_log_entries = D_RO(pool_root)->num_log_entries;
    m_first_free_entry = D_RO(pool_root)->first_free_entry;
    m_first_valid_entry = D_RO(pool_root)->first_valid_entry;
    if (m_first_free_entry < m_first_valid_entry) {
      /* Valid entries wrap around the end of the ring, so first_free is lower
       * than first_valid.  If first_valid was == first_free+1, the entry at
       * first_free would be empty. The last entry is never used, so in
       * that case there would be zero free log entries. */
     this->m_free_log_entries = this->m_total_log_entries - (m_first_valid_entry - m_first_free_entry) -1;
    } else {
      /* first_valid is <= first_free. If they are == we have zero valid log
       * entries, and n-1 free log entries */
      this->m_free_log_entries = this->m_total_log_entries - (m_first_free_entry - m_first_valid_entry) -1;
    }
    size_t effective_pool_size = (size_t)(this->m_log_pool_size * USABLE_SIZE);
    this->m_bytes_allocated_cap = effective_pool_size;
    load_existing_entries(later);
    m_cache_state->clean = this->m_dirty_log_entries.empty();
    m_cache_state->empty = m_log_entries.empty();
  }
  return true;
}

/*
 * Loads the log entries from an existing log.
 *
 * Creates the in-memory structures to represent the state of the
 * re-opened log.
 *
 * Finds the last appended sync point, and any sync points referred to
 * in log entries, but missing from the log. These missing sync points
 * are created and scheduled for append. Some rudimentary consistency
 * checking is done.
 *
 * Rebuilds the m_blocks_to_log_entries map, to make log entries
 * readable.
 *
 * Places all writes on the dirty entries list, which causes them all
 * to be flushed.
 *
 */

template <typename I>
void WriteLog<I>::load_existing_entries(DeferredContexts &later) {
  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);
  struct WriteLogCacheEntry *pmem_log_entries = D_RW(D_RW(pool_root)->log_entries);
  uint64_t entry_index = m_first_valid_entry;
  /* The map below allows us to find sync point log entries by sync
   * gen number, which is necessary so write entries can be linked to
   * their sync points. */
  std::map<uint64_t, std::shared_ptr<SyncPointLogEntry>> sync_point_entries;
  /* The map below tracks sync points referred to in writes but not
   * appearing in the sync_point_entries map.  We'll use this to
   * determine which sync points are missing and need to be
   * created. */
  std::map<uint64_t, bool> missing_sync_points;

  /*
   * Read the existing log entries. Construct an in-memory log entry
   * object of the appropriate type for each. Add these to the global
   * log entries list.
   *
   * Write entries will not link to their sync points yet. We'll do
   * that in the next pass. Here we'll accumulate a map of sync point
   * gen numbers that are referred to in writes but do not appearing in
   * the log.
   */
  while (entry_index != m_first_free_entry) {
    WriteLogCacheEntry *pmem_entry = &pmem_log_entries[entry_index];
    std::shared_ptr<GenericLogEntry> log_entry = nullptr;
    ceph_assert(pmem_entry->entry_index == entry_index);

    this->update_entries(&log_entry, pmem_entry, missing_sync_points,
        sync_point_entries, entry_index);

    log_entry->ram_entry = *pmem_entry;
    log_entry->cache_entry = pmem_entry;
    log_entry->log_entry_index = entry_index;
    log_entry->completed = true;

    m_log_entries.push_back(log_entry);

    entry_index = (entry_index + 1) % this->m_total_log_entries;
  }

  this->update_sync_points(missing_sync_points, sync_point_entries, later);
}

template <typename I>
void WriteLog<I>::inc_allocated_cached_bytes(
    std::shared_ptr<pwl::GenericLogEntry> log_entry) {
  if (log_entry->is_write_entry()) {
    this->m_bytes_allocated += std::max(log_entry->write_bytes(), MIN_WRITE_ALLOC_SIZE);
    this->m_bytes_cached += log_entry->write_bytes();
  }
}

template <typename I>
void WriteLog<I>::write_data_to_buffer(
    std::shared_ptr<pwl::WriteLogEntry> ws_entry,
    WriteLogCacheEntry *pmem_entry) {
  ws_entry->cache_buffer = D_RW(pmem_entry->write_data);
}

/**
 * Retire up to MAX_ALLOC_PER_TRANSACTION of the oldest log entries
 * that are eligible to be retired. Returns true if anything was
 * retired.
 */
template <typename I>
bool WriteLog<I>::retire_entries(const unsigned long int frees_per_tx) {
  CephContext *cct = m_image_ctx.cct;
  GenericLogEntriesVector retiring_entries;
  uint32_t initial_first_valid_entry;
  uint32_t first_valid_entry;

  std::lock_guard retire_locker(this->m_log_retire_lock);
  ldout(cct, 20) << "Look for entries to retire" << dendl;
  {
    /* Entry readers can't be added while we hold m_entry_reader_lock */
    RWLock::WLocker entry_reader_locker(this->m_entry_reader_lock);
    std::lock_guard locker(m_lock);
    initial_first_valid_entry = this->m_first_valid_entry;
    first_valid_entry = this->m_first_valid_entry;
    auto entry = m_log_entries.front();
    while (!m_log_entries.empty() &&
           retiring_entries.size() < frees_per_tx &&
           this->can_retire_entry(entry)) {
      if (entry->log_entry_index != first_valid_entry) {
        lderr(cct) << "Retiring entry index (" << entry->log_entry_index
                   << ") and first valid log entry index (" << first_valid_entry
                   << ") must be ==." << dendl;
      }
      ceph_assert(entry->log_entry_index == first_valid_entry);
      first_valid_entry = (first_valid_entry + 1) % this->m_total_log_entries;
      m_log_entries.pop_front();
      retiring_entries.push_back(entry);
      /* Remove entry from map so there will be no more readers */
      if ((entry->write_bytes() > 0) || (entry->bytes_dirty() > 0)) {
        auto gen_write_entry = static_pointer_cast<GenericWriteLogEntry>(entry);
        if (gen_write_entry) {
          this->m_blocks_to_log_entries.remove_log_entry(gen_write_entry);
        }
      }
      entry = m_log_entries.front();
    }
  }

  if (retiring_entries.size()) {
    ldout(cct, 20) << "Retiring " << retiring_entries.size() << " entries" << dendl;
    TOID(struct WriteLogPoolRoot) pool_root;
    pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);

    utime_t tx_start;
    utime_t tx_end;
    /* Advance first valid entry and release buffers */
    {
      uint64_t flushed_sync_gen;
      std::lock_guard append_locker(this->m_log_append_lock);
      {
        std::lock_guard locker(m_lock);
        flushed_sync_gen = this->m_flushed_sync_gen;
      }

      tx_start = ceph_clock_now();
      TX_BEGIN(m_log_pool) {
        if (D_RO(pool_root)->flushed_sync_gen < flushed_sync_gen) {
          ldout(m_image_ctx.cct, 20) << "flushed_sync_gen in log updated from "
                                     << D_RO(pool_root)->flushed_sync_gen << " to "
                                     << flushed_sync_gen << dendl;
          D_RW(pool_root)->flushed_sync_gen = flushed_sync_gen;
        }
        D_RW(pool_root)->first_valid_entry = first_valid_entry;
        for (auto &entry: retiring_entries) {
          if (entry->write_bytes()) {
            ldout(cct, 20) << "Freeing " << entry->ram_entry.write_data.oid.pool_uuid_lo
                           << "." << entry->ram_entry.write_data.oid.off << dendl;
            TX_FREE(entry->ram_entry.write_data);
          } else {
            ldout(cct, 20) << "Retiring non-write: " << *entry << dendl;
          }
        }
      } TX_ONCOMMIT {
      } TX_ONABORT {
        lderr(cct) << "failed to commit free of" << retiring_entries.size()
                   << " log entries (" << this->m_log_pool_name << ")" << dendl;
        ceph_assert(false);
      } TX_FINALLY {
      } TX_END;
      tx_end = ceph_clock_now();
    }
    m_perfcounter->tinc(l_librbd_pwl_retire_tx_t, tx_end - tx_start);
    m_perfcounter->hinc(l_librbd_pwl_retire_tx_t_hist, utime_t(tx_end - tx_start).to_nsec(),
        retiring_entries.size());

    /* Update runtime copy of first_valid, and free entries counts */
    {
      std::lock_guard locker(m_lock);

      ceph_assert(this->m_first_valid_entry == initial_first_valid_entry);
      this->m_first_valid_entry = first_valid_entry;
      this->m_free_log_entries += retiring_entries.size();
      if (!m_cache_state->empty && m_log_entries.empty()) {
        m_cache_state->empty = true;
        this->update_image_cache_state();
      }
      for (auto &entry: retiring_entries) {
        if (entry->write_bytes()) {
          ceph_assert(this->m_bytes_cached >= entry->write_bytes());
          this->m_bytes_cached -= entry->write_bytes();
          uint64_t entry_allocation_size = entry->write_bytes();
          if (entry_allocation_size < MIN_WRITE_ALLOC_SIZE) {
            entry_allocation_size = MIN_WRITE_ALLOC_SIZE;
          }
          ceph_assert(this->m_bytes_allocated >= entry_allocation_size);
          this->m_bytes_allocated -= entry_allocation_size;
        }
      }
      this->m_alloc_failed_since_retire = false;
      this->wake_up();
    }
  } else {
    ldout(cct, 20) << "Nothing to retire" << dendl;
    return false;
  }
  return true;
}

template <typename I>
void WriteLog<I>::construct_flush_entries(pwl::GenericLogEntries entries_to_flush,
				          DeferredContexts &post_unlock,
					  bool has_write_entry) {
  bool invalidating = this->m_invalidating; // snapshot so we behave consistently

  for (auto &log_entry : entries_to_flush) {
    GuardedRequestFunctionContext *guarded_ctx =
      new GuardedRequestFunctionContext([this, log_entry, invalidating]
        (GuardedRequestFunctionContext &guard_ctx) {
          log_entry->m_cell = guard_ctx.cell;
          Context *ctx = this->construct_flush_entry(log_entry, invalidating);

	  if (!invalidating) {
	    ctx = new LambdaContext(
	      [this, log_entry, ctx](int r) {
	      m_image_ctx.op_work_queue->queue(new LambdaContext(
	        [this, log_entry, ctx](int r) {
		  ldout(m_image_ctx.cct, 15) << "flushing:" << log_entry
		                             << " " << *log_entry << dendl;
		  log_entry->writeback(this->m_image_writeback, ctx);
	      }), 0);
	  });
	}

	ctx->complete(0);
    });
   this->detain_flush_guard_request(log_entry, guarded_ctx);
  }
}

const unsigned long int ops_flushed_together = 4;
/*
 * Performs the pmem buffer flush on all scheduled ops, then schedules
 * the log event append operation for all of them.
 */
template <typename I>
void WriteLog<I>::flush_then_append_scheduled_ops(void)
{
  GenericLogOperations ops;
  bool ops_remain = false;
  ldout(m_image_ctx.cct, 20) << dendl;
  do {
    {
      ops.clear();
      std::lock_guard locker(m_lock);
      if (m_ops_to_flush.size()) {
        auto last_in_batch = m_ops_to_flush.begin();
        unsigned int ops_to_flush = m_ops_to_flush.size();
        if (ops_to_flush > ops_flushed_together) {
          ops_to_flush = ops_flushed_together;
        }
        ldout(m_image_ctx.cct, 20) << "should flush " << ops_to_flush << dendl;
        std::advance(last_in_batch, ops_to_flush);
        ops.splice(ops.end(), m_ops_to_flush, m_ops_to_flush.begin(), last_in_batch);
        ops_remain = !m_ops_to_flush.empty();
        ldout(m_image_ctx.cct, 20) << "flushing " << ops.size() << ", "
                                   << m_ops_to_flush.size() << " remain" << dendl;
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
      schedule_append_ops(ops, nullptr);
    }
  } while (ops_remain);
  append_scheduled_ops();
}

/*
 * Performs the log event append operation for all of the scheduled
 * events.
 */
template <typename I>
void WriteLog<I>::append_scheduled_ops(void) {
  GenericLogOperations ops;
  int append_result = 0;
  bool ops_remain = false;
  bool appending = false; /* true if we set m_appending */
  ldout(m_image_ctx.cct, 20) << dendl;
  do {
    ops.clear();
    this->append_scheduled(ops, ops_remain, appending, true);

    if (ops.size()) {
      std::lock_guard locker(this->m_log_append_lock);
      alloc_op_log_entries(ops);
      append_result = append_op_log_entries(ops);
    }

    int num_ops = ops.size();
    if (num_ops) {
      /* New entries may be flushable. Completion will wake up flusher. */
      this->complete_op_log_entries(std::move(ops), append_result);
    }
  } while (ops_remain);
}

template <typename I>
void WriteLog<I>::enlist_op_flusher()
{
  this->m_async_flush_ops++;
  this->m_async_op_tracker.start_op();
  Context *flush_ctx = new LambdaContext([this](int r) {
      flush_then_append_scheduled_ops();
      this->m_async_flush_ops--;
      this->m_async_op_tracker.finish_op();
    });
  this->m_work_queue.queue(flush_ctx);
}

template <typename I>
void WriteLog<I>::setup_schedule_append(
    pwl::GenericLogOperationsVector &ops, bool do_early_flush,
    C_BlockIORequestT *req) {
  if (do_early_flush) {
    /* This caller is waiting for persist, so we'll use their thread to
     * expedite it */
    flush_pmem_buffer(ops);
    this->schedule_append(ops);
  } else {
    /* This is probably not still the caller's thread, so do the payload
     * flushing/replicating later. */
    schedule_flush_and_append(ops);
  }
}

/*
 * Takes custody of ops. They'll all get their log entries appended,
 * and have their on_write_persist contexts completed once they and
 * all prior log entries are persisted everywhere.
 */
template <typename I>
void WriteLog<I>::schedule_append_ops(GenericLogOperations &ops, C_BlockIORequestT *req)
{
  bool need_finisher;
  GenericLogOperationsVector appending;

  std::copy(std::begin(ops), std::end(ops), std::back_inserter(appending));
  {
    std::lock_guard locker(m_lock);

    need_finisher = this->m_ops_to_append.empty() && !this->m_appending;
    this->m_ops_to_append.splice(this->m_ops_to_append.end(), ops);
  }

  if (need_finisher) {
    //enlist op appender
    this->m_async_append_ops++;
    this->m_async_op_tracker.start_op();
    Context *append_ctx = new LambdaContext([this](int r) {
        append_scheduled_ops();
        this->m_async_append_ops--;
        this->m_async_op_tracker.finish_op();
        });
    this->m_work_queue.queue(append_ctx);
  }

  for (auto &op : appending) {
    op->appending();
  }
}

/*
 * Takes custody of ops. They'll all get their pmem blocks flushed,
 * then get their log entries appended.
 */
template <typename I>
void WriteLog<I>::schedule_flush_and_append(GenericLogOperationsVector &ops)
{
  GenericLogOperations to_flush(ops.begin(), ops.end());
  bool need_finisher;
  ldout(m_image_ctx.cct, 20) << dendl;
  {
    std::lock_guard locker(m_lock);

    need_finisher = m_ops_to_flush.empty();
    m_ops_to_flush.splice(m_ops_to_flush.end(), to_flush);
  }

  if (need_finisher) {
    enlist_op_flusher();
  }
}

template <typename I>
void WriteLog<I>::process_work() {
  CephContext *cct = m_image_ctx.cct;
  int max_iterations = 4;
  bool wake_up_requested = false;
  uint64_t aggressive_high_water_bytes = this->m_bytes_allocated_cap * AGGRESSIVE_RETIRE_HIGH_WATER;
  uint64_t high_water_bytes = this->m_bytes_allocated_cap * RETIRE_HIGH_WATER;
  uint64_t low_water_bytes = this->m_bytes_allocated_cap * RETIRE_LOW_WATER;
  uint64_t aggressive_high_water_entries = this->m_total_log_entries * AGGRESSIVE_RETIRE_HIGH_WATER;
  uint64_t high_water_entries = this->m_total_log_entries * RETIRE_HIGH_WATER;
  uint64_t low_water_entries = this->m_total_log_entries * RETIRE_LOW_WATER;

  ldout(cct, 20) << dendl;

  do {
    {
      std::lock_guard locker(m_lock);
      this->m_wake_up_requested = false;
    }
    if (this->m_alloc_failed_since_retire || this->m_invalidating ||
        this->m_bytes_allocated > high_water_bytes ||
        (m_log_entries.size() > high_water_entries)) {
      int retired = 0;
      utime_t started = ceph_clock_now();
      ldout(m_image_ctx.cct, 10) << "alloc_fail=" << this->m_alloc_failed_since_retire
                                 << ", allocated > high_water="
                                 << (this->m_bytes_allocated > high_water_bytes)
                                 << ", allocated_entries > high_water="
                                 << (m_log_entries.size() > high_water_entries)
                                 << dendl;
      while (this->m_alloc_failed_since_retire || this->m_invalidating ||
            (this->m_bytes_allocated > high_water_bytes) ||
            (m_log_entries.size() > high_water_entries) ||
            (((this->m_bytes_allocated > low_water_bytes) ||
              (m_log_entries.size() > low_water_entries)) &&
            (utime_t(ceph_clock_now() - started).to_msec() < RETIRE_BATCH_TIME_LIMIT_MS))) {
        if (!retire_entries((this->m_shutting_down || this->m_invalidating ||
           (this->m_bytes_allocated > aggressive_high_water_bytes) ||
           (m_log_entries.size() > aggressive_high_water_entries) ||
           this->m_alloc_failed_since_retire)
            ? MAX_ALLOC_PER_TRANSACTION
            : MAX_FREE_PER_TRANSACTION)) {
          break;
        }
        retired++;
        this->dispatch_deferred_writes();
        this->process_writeback_dirty_entries();
      }
      ldout(m_image_ctx.cct, 10) << "Retired " << retired << " times" << dendl;
    }
    this->dispatch_deferred_writes();
    this->process_writeback_dirty_entries();

    {
      std::lock_guard locker(m_lock);
      wake_up_requested = this->m_wake_up_requested;
    }
  } while (wake_up_requested && --max_iterations > 0);

  {
    std::lock_guard locker(m_lock);
    this->m_wake_up_scheduled = false;
    /* Reschedule if it's still requested */
    if (this->m_wake_up_requested) {
      this->wake_up();
    }
  }
}

/*
 * Flush the pmem regions for the data blocks of a set of operations
 *
 * V is expected to be GenericLogOperations<I>, or GenericLogOperationsVector<I>
 */
template <typename I>
template <typename V>
void WriteLog<I>::flush_pmem_buffer(V& ops)
{
  utime_t now = ceph_clock_now();
  for (auto &operation : ops) {
    if (operation->reserved_allocated()) {
      operation->buf_persist_start_time = now;
    } else {
      ldout(m_image_ctx.cct, 20) << "skipping non-write op: "
                                 << *operation << dendl;
    }
  }

  for (auto &operation : ops) {
    if(operation->is_writing_op()) {
      auto log_entry = static_pointer_cast<WriteLogEntry>(operation->get_log_entry());
      pmemobj_flush(m_log_pool, log_entry->cache_buffer, log_entry->write_bytes());
    }
  }

  /* Drain once for all */
  pmemobj_drain(m_log_pool);

  now = ceph_clock_now();
  for (auto &operation : ops) {
    if (operation->reserved_allocated()) {
      operation->buf_persist_comp_time = now;
    } else {
      ldout(m_image_ctx.cct, 20) << "skipping non-write op: "
                                 << *operation << dendl;
    }
  }
}

/**
 * Update/persist the last flushed sync point in the log
 */
template <typename I>
void WriteLog<I>::persist_last_flushed_sync_gen()
{
  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);
  uint64_t flushed_sync_gen;

  std::lock_guard append_locker(this->m_log_append_lock);
  {
    std::lock_guard locker(m_lock);
    flushed_sync_gen = this->m_flushed_sync_gen;
  }

  if (D_RO(pool_root)->flushed_sync_gen < flushed_sync_gen) {
    ldout(m_image_ctx.cct, 15) << "flushed_sync_gen in log updated from "
                               << D_RO(pool_root)->flushed_sync_gen << " to "
                               << flushed_sync_gen << dendl;
    TX_BEGIN(m_log_pool) {
      D_RW(pool_root)->flushed_sync_gen = flushed_sync_gen;
    } TX_ONCOMMIT {
    } TX_ONABORT {
      lderr(m_image_ctx.cct) << "failed to commit update of flushed sync point" << dendl;
      ceph_assert(false);
    } TX_FINALLY {
    } TX_END;
  }
}

template <typename I>
void WriteLog<I>::reserve_cache(C_BlockIORequestT *req,
                                         bool &alloc_succeeds, bool &no_space) {
  std::vector<WriteBufferAllocation>& buffers = req->get_resources_buffers();
  for (auto &buffer : buffers) {
    utime_t before_reserve = ceph_clock_now();
    buffer.buffer_oid = pmemobj_reserve(m_log_pool,
                                        &buffer.buffer_alloc_action,
                                        buffer.allocation_size,
                                        0 /* Object type */);
    buffer.allocation_lat = ceph_clock_now() - before_reserve;
    if (TOID_IS_NULL(buffer.buffer_oid)) {
      if (!req->has_io_waited_for_buffers()) {
        req->set_io_waited_for_buffers(true);
      }
      ldout(m_image_ctx.cct, 5) << "can't allocate all data buffers: "
                                << pmemobj_errormsg() << ". "
                                << *req << dendl;
      alloc_succeeds = false;
      no_space = true; /* Entries need to be retired */

      if (this->m_free_log_entries == this->m_total_log_entries - 1) {
        /* When the cache is empty, there is still no space to allocate.
         * Defragment. */
        pmemobj_defrag(m_log_pool, NULL, 0, NULL);
      }
      break;
    } else {
      buffer.allocated = true;
    }
    ldout(m_image_ctx.cct, 20) << "Allocated " << buffer.buffer_oid.oid.pool_uuid_lo
                               << "." << buffer.buffer_oid.oid.off
                               << ", size=" << buffer.allocation_size << dendl;
  }
}

template<typename I>
void WriteLog<I>::copy_bl_to_buffer(
    WriteRequestResources *resources, std::unique_ptr<WriteLogOperationSet> &op_set) {
  auto allocation = resources->buffers.begin();
  for (auto &operation : op_set->operations) {
    operation->copy_bl_to_cache_buffer(allocation);
    allocation++;
  }
}

template <typename I>
bool WriteLog<I>::alloc_resources(C_BlockIORequestT *req) {
  bool alloc_succeeds = true;
  uint64_t bytes_allocated = 0;
  uint64_t bytes_cached = 0;
  uint64_t bytes_dirtied = 0;
  uint64_t num_lanes = 0;
  uint64_t num_unpublished_reserves = 0;
  uint64_t num_log_entries = 0;

  ldout(m_image_ctx.cct, 20) << dendl;
  // Setup buffer, and get all the number of required resources
  req->setup_buffer_resources(&bytes_cached, &bytes_dirtied, &bytes_allocated,
                              &num_lanes, &num_log_entries, &num_unpublished_reserves);

  alloc_succeeds = this->check_allocation(req, bytes_cached, bytes_dirtied,
                                          bytes_allocated, num_lanes, num_log_entries,
                                          num_unpublished_reserves);

  std::vector<WriteBufferAllocation>& buffers = req->get_resources_buffers();
  if (!alloc_succeeds) {
    /* On alloc failure, free any buffers we did allocate */
    for (auto &buffer : buffers) {
      if (buffer.allocated) {
        pmemobj_cancel(m_log_pool, &buffer.buffer_alloc_action, 1);
      }
    }
  }

  req->set_allocated(alloc_succeeds);
  return alloc_succeeds;
}

template <typename I>
void WriteLog<I>::complete_user_request(Context *&user_req, int r) {
  user_req->complete(r);
  // Set user_req as null as it is deleted
  user_req = nullptr;
}

} // namespace rwl
} // namespace pwl
} // namespace cache
} // namespace librbd

template class librbd::cache::pwl::rwl::WriteLog<librbd::ImageCtx>;
