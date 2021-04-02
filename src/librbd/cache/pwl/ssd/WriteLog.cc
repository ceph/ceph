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
#include <map>
#include <vector>

#undef dout_subsys
#define dout_subsys ceph_subsys_rbd_pwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::pwl::ssd::WriteLog: " \
                           << this << " " <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace pwl {
namespace ssd {

using namespace librbd::cache::pwl;

// SSD: this number can be updated later
const unsigned long int ops_appended_together = MAX_WRITES_PER_SYNC_POINT;

template <typename I>
Builder<AbstractWriteLog<I>>* WriteLog<I>::create_builder() {
  m_builderobj = new Builder<This>();
  return m_builderobj;
}

template <typename I>
WriteLog<I>::WriteLog(
    I &image_ctx, librbd::cache::pwl::ImageCacheState<I>* cache_state,
    cache::ImageWritebackInterface& image_writeback,
    plugin::Api<I>& plugin_api)
  : AbstractWriteLog<I>(image_ctx, cache_state, create_builder(),
                        image_writeback, plugin_api)
{
}

template <typename I>
WriteLog<I>::~WriteLog() {
  delete m_builderobj;
}

template <typename I>
void WriteLog<I>::collect_read_extents(
    uint64_t read_buffer_offset, LogMapEntry<GenericWriteLogEntry> map_entry,
    std::vector<WriteLogCacheEntry*> &log_entries_to_read,
    std::vector<bufferlist*> &bls_to_read,
    uint64_t entry_hit_length, Extent hit_extent,
    pwl::C_ReadRequest *read_ctx) {
    // Make a bl for this hit extent. This will add references to the
    // write_entry->cache_bl */
    ldout(m_image_ctx.cct, 5) << dendl;
    auto write_entry = static_pointer_cast<WriteLogEntry>(map_entry.log_entry);
    buffer::list hit_bl;
    hit_bl = write_entry->get_cache_bl();
    bool writesame = write_entry->is_writesame_entry();
    auto hit_extent_buf = std::make_shared<ImageExtentBuf>(
        hit_extent, hit_bl, true, read_buffer_offset, writesame);
    read_ctx->read_extents.push_back(hit_extent_buf);

    if(!hit_bl.length()) {
      ldout(m_image_ctx.cct, 5) << "didn't hit RAM" << dendl;
      auto read_extent = read_ctx->read_extents.back();
      log_entries_to_read.push_back(&write_entry->ram_entry);
      bls_to_read.push_back(&read_extent->m_bl);
    }
}

template <typename I>
void WriteLog<I>::complete_read(
    std::vector<WriteLogCacheEntry*> &log_entries_to_read,
    std::vector<bufferlist*> &bls_to_read,
    Context *ctx) {
  if (!log_entries_to_read.empty()) {
    aio_read_data_block(log_entries_to_read, bls_to_read, ctx);
  } else {
    ctx->complete(0);
  }
}

template <typename I>
void WriteLog<I>::initialize_pool(Context *on_finish,
                                  pwl::DeferredContexts &later) {
  CephContext *cct = m_image_ctx.cct;
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
  if (access(this->m_log_pool_name.c_str(), F_OK) != 0) {
    int fd = ::open(this->m_log_pool_name.c_str(), O_RDWR|O_CREAT, 0644);
    bool succeed = true;
    if (fd >= 0) {
      if (truncate(this->m_log_pool_name.c_str(),
                   this->m_log_pool_config_size) != 0) {
        succeed = false;
      }
      ::close(fd);
    } else {
      succeed = false;
    }
    if (!succeed) {
      m_cache_state->present = false;
      m_cache_state->clean = true;
      m_cache_state->empty = true;
      /* TODO: filter/replace errnos that are meaningless to the caller */
      on_finish->complete(-errno);
      return;
    }

    bdev = BlockDevice::create(cct, this->m_log_pool_name, aio_cache_cb,
                               nullptr, nullptr, nullptr);
    int r = bdev->open(this->m_log_pool_name);
    if (r < 0) {
      delete bdev;
      on_finish->complete(-1);
      return;
    }
    m_cache_state->present = true;
    m_cache_state->clean = true;
    m_cache_state->empty = true;
    /* new pool, calculate and store metadata */
    size_t small_write_size = MIN_WRITE_ALLOC_SSD_SIZE + sizeof(struct WriteLogCacheEntry);

    uint64_t num_small_writes = (uint64_t)(this->m_log_pool_config_size / small_write_size);
    if (num_small_writes > MAX_LOG_ENTRIES) {
      num_small_writes = MAX_LOG_ENTRIES;
    }
    assert(num_small_writes > 2);
    m_log_pool_ring_buffer_size = this->m_log_pool_config_size - DATA_RING_BUFFER_OFFSET;
    /* Log ring empty */
    m_first_free_entry = DATA_RING_BUFFER_OFFSET;
    m_first_valid_entry = DATA_RING_BUFFER_OFFSET;

    pool_size = this->m_log_pool_config_size;
    auto new_root = std::make_shared<WriteLogPoolRoot>(pool_root);
    new_root->pool_size = this->m_log_pool_config_size;
    new_root->flushed_sync_gen = this->m_flushed_sync_gen;
    new_root->block_size = MIN_WRITE_ALLOC_SSD_SIZE;
    new_root->first_free_entry = m_first_free_entry;
    new_root->first_valid_entry = m_first_valid_entry;
    new_root->num_log_entries = num_small_writes;
    pool_root = *new_root;

    r = update_pool_root_sync(new_root);
    if (r != 0) {
      this->m_total_log_entries = 0;
      this->m_free_log_entries = 0;
      lderr(m_image_ctx.cct) << "failed to initialize pool ("
                             << this->m_log_pool_name << ")" << dendl;
      on_finish->complete(r);
    }
    this->m_total_log_entries = new_root->num_log_entries;
    this->m_free_log_entries = new_root->num_log_entries - 1;
   } else {
     m_cache_state->present = true;
     bdev = BlockDevice::create(
         cct, this->m_log_pool_name, aio_cache_cb,
         static_cast<void*>(this), nullptr, static_cast<void*>(this));
     int r = bdev->open(this->m_log_pool_name);
     if (r < 0) {
       delete bdev;
       on_finish->complete(r);
       return;
     }
     load_existing_entries(later);
     if (m_first_free_entry < m_first_valid_entry) {
      /* Valid entries wrap around the end of the ring, so first_free is lower
       * than first_valid.  If first_valid was == first_free+1, the entry at
       * first_free would be empty. The last entry is never used, so in
       * that case there would be zero free log entries. */
       this->m_free_log_entries = this->m_total_log_entries -
         (m_first_valid_entry - m_first_free_entry) - 1;
     } else {
      /* first_valid is <= first_free. If they are == we have zero valid log
       * entries, and n-1 free log entries */
       this->m_free_log_entries = this->m_total_log_entries -
         (m_first_free_entry - m_first_valid_entry) - 1;
     }
     m_cache_state->clean = this->m_dirty_log_entries.empty();
     m_cache_state->empty = m_log_entries.empty();
  }
}

template <typename I>
void WriteLog<I>::remove_pool_file() {
  ceph_assert(bdev);
  bdev->close();
  delete bdev;
  bdev = nullptr;
  ldout(m_image_ctx.cct, 5) << "block device is closed" << dendl;

  if (m_cache_state->clean) {
    ldout(m_image_ctx.cct, 5) << "Removing empty pool file: "
                              << this->m_log_pool_name << dendl;
    if (remove(this->m_log_pool_name.c_str()) != 0) {
      lderr(m_image_ctx.cct) << "failed to remove empty pool \""
                             << this->m_log_pool_name << "\": " << dendl;
    } else {
      m_cache_state->clean = true;
      m_cache_state->empty = true;
      m_cache_state->present = false;
    }
  } else {
    ldout(m_image_ctx.cct, 5) << "Not removing pool file: "
                              << this->m_log_pool_name << dendl;
  }
}

template <typename I>
void WriteLog<I>::load_existing_entries(pwl::DeferredContexts &later) {
  bufferlist bl;
  CephContext *cct = m_image_ctx.cct;
  ::IOContext ioctx(cct, nullptr);
  bdev->read(0, MIN_WRITE_ALLOC_SSD_SIZE, &bl, &ioctx, false);
  SuperBlock superblock;

  auto p = bl.cbegin();
  decode(superblock, p);
  ldout(cct,5) << "Decoded superblock" << dendl;

  WriteLogPoolRoot current_pool_root = superblock.root;
  uint64_t next_log_pos = pool_root.first_valid_entry;
  uint64_t first_free_entry =  pool_root.first_free_entry;
  uint64_t curr_log_pos;

  pool_root = current_pool_root;
  m_first_free_entry = first_free_entry;
  m_first_valid_entry = next_log_pos;
  this->m_total_log_entries = current_pool_root.num_log_entries;
  this->m_flushed_sync_gen = current_pool_root.flushed_sync_gen;
  this->m_log_pool_actual_size = current_pool_root.pool_size;

  std::map<uint64_t, std::shared_ptr<SyncPointLogEntry>> sync_point_entries;

  std::map<uint64_t, bool> missing_sync_points;

  // Iterate through the log_entries and append all the write_bytes
  // of each entry to fetch the pos of next 4k of log_entries. Iterate
  // through the log entries and append them to the in-memory vector
  while (next_log_pos != first_free_entry) {
    // read the entries from SSD cache and decode
    bufferlist bl_entries;
    ::IOContext ioctx_entry(cct, nullptr);
    bdev->read(next_log_pos, MIN_WRITE_ALLOC_SSD_SIZE, &bl_entries,
               &ioctx_entry, false);
    std::vector<WriteLogCacheEntry> ssd_log_entries;
    auto pl = bl_entries.cbegin();
    decode(ssd_log_entries, pl);
    ldout(cct, 5) << "decoded ssd log entries" << dendl;
    curr_log_pos = next_log_pos;
    std::shared_ptr<GenericLogEntry> log_entry = nullptr;

    for (auto it = ssd_log_entries.begin(); it != ssd_log_entries.end(); ++it) {
      this->update_entries(&log_entry, &*it, missing_sync_points,
                           sync_point_entries, curr_log_pos);
      log_entry->ram_entry = *it;
      log_entry->log_entry_index = curr_log_pos;
      log_entry->completed = true;
      m_log_entries.push_back(log_entry);
      next_log_pos += round_up_to(it->write_bytes, MIN_WRITE_ALLOC_SSD_SIZE);
    }
    // along with the write_bytes, add control block size too
    next_log_pos += MIN_WRITE_ALLOC_SSD_SIZE;
    if (next_log_pos >= this->m_log_pool_actual_size) {
      next_log_pos = next_log_pos % this->m_log_pool_actual_size + DATA_RING_BUFFER_OFFSET;
    }
 }
  this->update_sync_points(missing_sync_points, sync_point_entries, later,
                           MIN_WRITE_ALLOC_SSD_SIZE);
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

  // Setup buffer, and get all the number of required resources
  req->setup_buffer_resources(&bytes_cached, &bytes_dirtied, &bytes_allocated,
                              &num_lanes, &num_log_entries,
                              &num_unpublished_reserves);

  bytes_allocated += num_log_entries * MIN_WRITE_ALLOC_SSD_SIZE;

  alloc_succeeds = this->check_allocation(req, bytes_cached, bytes_dirtied,
                                          bytes_allocated, num_lanes,
                                          num_log_entries,
                                          num_unpublished_reserves,
                                          m_log_pool_ring_buffer_size);
  req->set_allocated(alloc_succeeds);
  return alloc_succeeds;
}

template <typename I>
bool WriteLog<I>::has_sync_point_logs(GenericLogOperations &ops) {
  for (auto &op : ops) {
    if (op->get_log_entry()->is_sync_point()) {
      return true;
      break;
    }
  }
  return false;
}

template<typename I>
void WriteLog<I>::enlist_op_appender() {
  this->m_async_append_ops++;
  this->m_async_op_tracker.start_op();
  Context *append_ctx = new LambdaContext([this](int r) {
      append_scheduled_ops();
      });
  this->m_work_queue.queue(append_ctx);
}
/*
 * Takes custody of ops. They'll all get their log entries appended,
 * and have their on_write_persist contexts completed once they and
 * all prior log entries are persisted everywhere.
 */
template<typename I>
void WriteLog<I>::schedule_append_ops(GenericLogOperations &ops) {
  bool need_finisher = false;
  GenericLogOperationsVector appending;

  std::copy(std::begin(ops), std::end(ops), std::back_inserter(appending));
  {
    std::lock_guard locker(m_lock);

    bool persist_on_flush = this->get_persist_on_flush();
    need_finisher = !this->m_appending &&
       ((this->m_ops_to_append.size() >= CONTROL_BLOCK_MAX_LOG_ENTRIES) ||
        !persist_on_flush);

    // Only flush logs into SSD when there is internal/external flush request
    if (!need_finisher) {
      need_finisher = has_sync_point_logs(ops);
    }
    this->m_ops_to_append.splice(this->m_ops_to_append.end(), ops);
  }

  if (need_finisher) {
    this->enlist_op_appender();
  }

  for (auto &op : appending) {
    op->appending();
  }
}

template <typename I>
void WriteLog<I>::setup_schedule_append(pwl::GenericLogOperationsVector &ops,
                                        bool do_early_flush,
                                        C_BlockIORequestT *req) {
  this->schedule_append(ops);
  if (this->get_persist_on_flush()) {
    req->complete_user_request(0);
  }
  req->release_cell();
}

template <typename I>
void WriteLog<I>::append_scheduled_ops(void) {
  GenericLogOperations ops;
  ldout(m_image_ctx.cct, 20) << dendl;

  bool ops_remain = false; //no-op variable for SSD
  bool appending = false; //no-op variable for SSD
  this->append_scheduled(ops, ops_remain, appending);

  if (ops.size()) {
    alloc_op_log_entries(ops);
    append_op_log_entries(ops);
  } else {
    this->m_async_append_ops--;
    this->m_async_op_tracker.finish_op();
  }
}

/*
 * Write and persist the (already allocated) write log entries and
 * data buffer allocations for a set of ops. The data buffer for each
 * of these must already have been persisted to its reserved area.
 */
template <typename I>
void WriteLog<I>::append_op_log_entries(GenericLogOperations &ops) {
  ceph_assert(!ops.empty());
  ldout(m_image_ctx.cct, 20) << dendl;
  Context *ctx = new LambdaContext([this, ops](int r) {
    assert(r == 0);
    ldout(m_image_ctx.cct, 20) << "Finished root update " << dendl;
    this->m_async_update_superblock--;
    this->m_async_op_tracker.finish_op();

    auto captured_ops = std::move(ops);
    this->complete_op_log_entries(std::move(captured_ops), r);

    bool need_finisher = false;
    {
      std::lock_guard locker1(m_lock);
      bool persist_on_flush = this->get_persist_on_flush();
      need_finisher = ((this->m_ops_to_append.size() >= CONTROL_BLOCK_MAX_LOG_ENTRIES) ||
                       !persist_on_flush);

      if (!need_finisher) {
        need_finisher = has_sync_point_logs(this->m_ops_to_append);
      }
    }

    if (need_finisher) {
      this->enlist_op_appender();
    }
  });
  uint64_t *new_first_free_entry = new(uint64_t);
  Context *append_ctx = new LambdaContext(
      [this, new_first_free_entry, ops, ctx](int r) {
      std::shared_ptr<WriteLogPoolRoot> new_root;
      {
        ldout(m_image_ctx.cct, 20) << "Finished appending at "
                                   << *new_first_free_entry << dendl;
        utime_t now = ceph_clock_now();
        for (auto &operation : ops) {
          operation->log_append_comp_time = now;
        }
        this->m_async_append_ops--;
        this->m_async_op_tracker.finish_op();

        std::lock_guard locker(this->m_log_append_lock);
        std::lock_guard locker1(m_lock);
        assert(this->m_appending);
        this->m_appending = false;
        new_root = std::make_shared<WriteLogPoolRoot>(pool_root);
        pool_root.first_free_entry = *new_first_free_entry;
        new_root->first_free_entry = *new_first_free_entry;
        delete new_first_free_entry;
        schedule_update_root(new_root, ctx);
      }
  });
  // Append logs and update first_free_update
  uint64_t bytes_allocated_updated;
  append_ops(ops, append_ctx, new_first_free_entry, bytes_allocated_updated);

  {
    std::lock_guard locker1(m_lock);
    m_first_free_entry = *new_first_free_entry;
    m_bytes_allocated -= bytes_allocated_updated;
  }

  if (ops.size()) {
    this->dispatch_deferred_writes();
  }
}

template <typename I>
void WriteLog<I>::release_ram(std::shared_ptr<GenericLogEntry> log_entry) {
  log_entry->remove_cache_bl();
}

template <typename I>
void WriteLog<I>::alloc_op_log_entries(GenericLogOperations &ops) {
  std::lock_guard locker(m_lock);

  for (auto &operation : ops) {
    auto &log_entry = operation->get_log_entry();
    log_entry->ram_entry.entry_valid = 1;
    m_log_entries.push_back(log_entry);
    ldout(m_image_ctx.cct, 20) << "operation=[" << *operation << "]" << dendl;
  }
}

template <typename I>
Context* WriteLog<I>::construct_flush_entry_ctx(
    std::shared_ptr<GenericLogEntry> log_entry) {
  // snapshot so we behave consistently
  bool invalidating = this->m_invalidating;

  Context *ctx = this->construct_flush_entry(log_entry, invalidating);

  if (invalidating) {
    return ctx;
  }
  if(log_entry->is_write_entry()) {
      bufferlist *read_bl_ptr = new bufferlist;
      ctx = new LambdaContext(
          [this, log_entry, read_bl_ptr, ctx](int r) {
            bufferlist captured_entry_bl;
            captured_entry_bl.claim_append(*read_bl_ptr);
            free(read_bl_ptr);
            m_image_ctx.op_work_queue->queue(new LambdaContext(
              [this, log_entry, entry_bl=move(captured_entry_bl), ctx](int r) {
               auto captured_entry_bl = std::move(entry_bl);
               ldout(m_image_ctx.cct, 15) << "flushing:" << log_entry
                                          << " " << *log_entry << dendl;
               log_entry->writeback_bl(this->m_image_writeback, ctx,
                                       std::move(captured_entry_bl));
              }), 0);
      });
      ctx = new LambdaContext(
        [this, log_entry, read_bl_ptr, ctx](int r) {
          aio_read_data_block(&log_entry->ram_entry, read_bl_ptr, ctx);
      });
    return ctx;
  } else {
    return new LambdaContext(
      [this, log_entry, ctx](int r) {
        m_image_ctx.op_work_queue->queue(new LambdaContext(
          [this, log_entry, ctx](int r) {
            ldout(m_image_ctx.cct, 15) << "flushing:" << log_entry
                                       << " " << *log_entry << dendl;
            log_entry->writeback(this->m_image_writeback, ctx);
          }), 0);
      });
  }
}

template <typename I>
void WriteLog<I>::process_work() {
  CephContext *cct = m_image_ctx.cct;
  int max_iterations = 4;
  bool wake_up_requested = false;
  uint64_t aggressive_high_water_bytes = m_log_pool_ring_buffer_size * AGGRESSIVE_RETIRE_HIGH_WATER;
  uint64_t aggressive_high_water_entries = this->m_total_log_entries * AGGRESSIVE_RETIRE_HIGH_WATER;
  uint64_t high_water_bytes = m_log_pool_ring_buffer_size * RETIRE_HIGH_WATER;
  uint64_t high_water_entries = this->m_total_log_entries * RETIRE_HIGH_WATER;

  ldout(cct, 20) << dendl;

  do {
    {
      std::lock_guard locker(m_lock);
      this->m_wake_up_requested = false;
    }
    if (this->m_alloc_failed_since_retire || (this->m_shutting_down) ||
        this->m_invalidating || m_bytes_allocated > high_water_bytes ||
        (m_log_entries.size() > high_water_entries)) {
      ldout(m_image_ctx.cct, 10) << "alloc_fail=" << this->m_alloc_failed_since_retire
                                 << ", allocated > high_water="
                                 << (m_bytes_allocated > high_water_bytes)
                                 << ", allocated_entries > high_water="
                                 << (m_log_entries.size() > high_water_entries)
                                 << dendl;
      retire_entries((this->m_shutting_down || this->m_invalidating ||
                    (m_bytes_allocated > aggressive_high_water_bytes) ||
                    (m_log_entries.size() > aggressive_high_water_entries))
                    ? MAX_ALLOC_PER_TRANSACTION : MAX_FREE_PER_TRANSACTION);
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
    // Reschedule if it's still requested
    if (this->m_wake_up_requested) {
      this->wake_up();
    }
  }
}

/**
 * Retire up to MAX_ALLOC_PER_TRANSACTION of the oldest log entries
 * that are eligible to be retired. Returns true if anything was
 * retired.
 *
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
    // Entry readers can't be added while we hold m_entry_reader_lock
    RWLock::WLocker entry_reader_locker(this->m_entry_reader_lock);
    std::lock_guard locker(m_lock);
    initial_first_valid_entry = m_first_valid_entry;
    first_valid_entry = m_first_valid_entry;
    while (retiring_entries.size() < frees_per_tx && !m_log_entries.empty()) {
      GenericLogEntriesVector retiring_subentries;
      auto entry = m_log_entries.front();
      uint64_t control_block_pos = entry->log_entry_index;
      uint64_t data_length = 0;
      for (auto it = m_log_entries.begin(); it != m_log_entries.end(); ++it) {
        if (this->can_retire_entry(*it)) {
          // log_entry_index is valid after appending to SSD
          if ((*it)->log_entry_index != control_block_pos) {
            ldout(cct, 20) << "Old log_entry_index is " << control_block_pos
                           << ",New log_entry_index is "
                           << (*it)->log_entry_index
                           << ",data length is " << data_length << dendl;
            ldout(cct, 20) << "The log entry is " << *(*it) << dendl;
            if ((*it)->log_entry_index < control_block_pos) {
              ceph_assert((*it)->log_entry_index ==
                (control_block_pos + data_length + MIN_WRITE_ALLOC_SSD_SIZE)
                % this->m_log_pool_config_size + DATA_RING_BUFFER_OFFSET);
            } else {
              ceph_assert((*it)->log_entry_index == control_block_pos +
                  data_length + MIN_WRITE_ALLOC_SSD_SIZE);
            }
            break;
          } else {
            retiring_subentries.push_back(*it);
            if ((*it)->is_write_entry()) {
              data_length += (*it)->get_aligned_data_size();
            }
          }
        } else {
          retiring_subentries.clear();
          break;
        }
      }
      // SSD: retiring_subentries in a span
      if (!retiring_subentries.empty()) {
        for (auto it = retiring_subentries.begin();
            it != retiring_subentries.end(); it++) {
          ceph_assert(m_log_entries.front() == *it);
          m_log_entries.pop_front();
          if (entry->is_write_entry()) {
            auto write_entry = static_pointer_cast<WriteLogEntry>(entry);
            this->m_blocks_to_log_entries.remove_log_entry(write_entry);
          }
        }
        retiring_entries.insert(
            retiring_entries.end(), retiring_subentries.begin(),
            retiring_subentries.end());
      } else {
        break;
      }
    }
  }
  if (retiring_entries.size()) {
    ldout(cct, 1) << "Retiring " << retiring_entries.size()
                  << " entries" << dendl;

    // Advance first valid entry and release buffers
    uint64_t flushed_sync_gen;
    std::lock_guard append_locker(this->m_log_append_lock);
    {
      std::lock_guard locker(m_lock);
      flushed_sync_gen = this->m_flushed_sync_gen;
    }

    //calculate new first_valid_entry based on last entry to retire
    auto entry = retiring_entries.back();
    if (entry->is_write_entry() || entry->is_writesame_entry()) {
      first_valid_entry = entry->ram_entry.write_data_pos +
        entry->get_aligned_data_size();
    } else {
      first_valid_entry = entry->log_entry_index + MIN_WRITE_ALLOC_SSD_SIZE;
    }
    if (first_valid_entry >= this->m_log_pool_config_size) {
        first_valid_entry = first_valid_entry % this->m_log_pool_config_size +
          DATA_RING_BUFFER_OFFSET;
    }
    ceph_assert(first_valid_entry != initial_first_valid_entry);
    auto new_root = std::make_shared<WriteLogPoolRoot>(pool_root);
    new_root->flushed_sync_gen = flushed_sync_gen;
    new_root->first_valid_entry = first_valid_entry;
    pool_root.flushed_sync_gen = flushed_sync_gen;
    pool_root.first_valid_entry = first_valid_entry;

    Context *ctx = new LambdaContext(
          [this, flushed_sync_gen, first_valid_entry,
          initial_first_valid_entry, retiring_entries](int r) {
          uint64_t allocated_bytes = 0;
          uint64_t cached_bytes = 0;
          uint64_t former_log_pos = 0;
          for (auto &entry : retiring_entries) {
            ceph_assert(entry->log_entry_index != 0);
            if (entry->log_entry_index != former_log_pos ) {
              // Space for control blocks
              allocated_bytes  += MIN_WRITE_ALLOC_SSD_SIZE;
              former_log_pos = entry->log_entry_index;
            }
            if (entry->is_write_entry()) {
              cached_bytes += entry->write_bytes();
              //space for userdata
              allocated_bytes += entry->get_aligned_data_size();
            }
          }
          {
            std::lock_guard locker(m_lock);
            m_first_valid_entry = first_valid_entry;
            ceph_assert(m_first_valid_entry % MIN_WRITE_ALLOC_SSD_SIZE == 0);
            this->m_free_log_entries += retiring_entries.size();
            ceph_assert(this->m_bytes_cached >= cached_bytes);
            this->m_bytes_cached -= cached_bytes;

            ldout(m_image_ctx.cct, 20)
              << "Finished root update: " << "initial_first_valid_entry="
              << initial_first_valid_entry << ", " << "m_first_valid_entry="
              << m_first_valid_entry << "," << "release space = "
              << allocated_bytes << "," << "m_bytes_allocated="
              << m_bytes_allocated << "," << "release cached space="
              << allocated_bytes << "," << "m_bytes_cached="
              << this->m_bytes_cached << dendl;

            this->m_alloc_failed_since_retire = false;
            this->wake_up();
            m_async_update_superblock--;
            this->m_async_op_tracker.finish_op();
          }

          this->dispatch_deferred_writes();
          this->process_writeback_dirty_entries();
        });

      std::lock_guard locker(m_lock);
      schedule_update_root(new_root, ctx);
  } else {
    ldout(cct, 20) << "Nothing to retire" << dendl;
    return false;
  }
  return true;
}

template <typename I>
void WriteLog<I>::append_ops(GenericLogOperations &ops, Context *ctx,
                             uint64_t* new_first_free_entry,
                             uint64_t &bytes_allocated) {
  GenericLogEntriesVector log_entries;
  CephContext *cct = m_image_ctx.cct;
  uint64_t span_payload_len = 0;
  bytes_allocated = 0;
  ldout(cct, 20) << "Appending " << ops.size() << " log entries." << dendl;

  AioTransContext* aio = new AioTransContext(cct, ctx);

  utime_t now = ceph_clock_now();
  for (auto &operation : ops) {
    operation->log_append_time = now;
    auto log_entry = operation->get_log_entry();

    if (log_entries.size() == CONTROL_BLOCK_MAX_LOG_ENTRIES ||
        span_payload_len >= SPAN_MAX_DATA_LEN) {
      if (log_entries.size() > 1) {
        bytes_allocated += (log_entries.size() - 1) * MIN_WRITE_ALLOC_SSD_SIZE;
      }
      write_log_entries(log_entries, aio);
      log_entries.clear();
      span_payload_len = 0;
    }
    log_entries.push_back(log_entry);
    span_payload_len += log_entry->write_bytes();
  }
  if (!span_payload_len || !log_entries.empty()) {
    if (log_entries.size() > 1) {
      bytes_allocated += (log_entries.size() - 1) * MIN_WRITE_ALLOC_SSD_SIZE;
    }
    write_log_entries(log_entries, aio);
  }
  bdev->aio_submit(&aio->ioc);
  *new_first_free_entry = pool_root.first_free_entry;
}

template <typename I>
void WriteLog<I>::write_log_entries(GenericLogEntriesVector log_entries,
                                    AioTransContext *aio) {
  CephContext *cct = m_image_ctx.cct;
  ldout(m_image_ctx.cct, 20) << dendl;
  bufferlist data_bl;
  // The first block is for log entries
  uint64_t data_pos = pool_root.first_free_entry + MIN_WRITE_ALLOC_SSD_SIZE;
  ldout(m_image_ctx.cct, 20) << "data_pos: " << data_pos << dendl;
  if (data_pos == pool_root.pool_size ) {
    data_pos = data_pos % pool_root.pool_size + DATA_RING_BUFFER_OFFSET;
  }

  std::vector<WriteLogCacheEntry> persist_log_entries;
  for (auto &log_entry : log_entries) {
    log_entry->log_entry_index = pool_root.first_free_entry;
    // Append data buffer for write operations
    persist_log_entries.push_back(log_entry->ram_entry);
    if (log_entry->is_write_entry()) {
      auto write_entry = static_pointer_cast<WriteLogEntry>(log_entry);
      auto cache_bl = write_entry->get_cache_bl();
      auto align_size = write_entry->get_aligned_data_size();
      data_bl.append(cache_bl);
      data_bl.append_zero(align_size - cache_bl.length());

      write_entry->ram_entry.write_data_pos = data_pos;
      data_pos += align_size;
      if (data_pos >= pool_root.pool_size) {
        data_pos = data_pos % pool_root.pool_size + DATA_RING_BUFFER_OFFSET;
      }
    }
  }

  //aio write
  bufferlist bl;
  encode(persist_log_entries, bl);
  ceph_assert(bl.length() <= MIN_WRITE_ALLOC_SSD_SIZE);
  bl.append_zero(MIN_WRITE_ALLOC_SSD_SIZE - bl.length());
  bl.append(data_bl);
  ceph_assert(bl.length() % MIN_WRITE_ALLOC_SSD_SIZE == 0);
  if (pool_root.first_free_entry + bl.length() > pool_root.pool_size) {
    //exceeds border, need to split
    uint64_t size = bl.length();
    auto end = pool_root.pool_size - pool_root.first_free_entry;
    bufferlist bl1;
    bl.splice(0, end, &bl1);
    ceph_assert(bl.length() == (size - bl1.length()));
    ldout(cct, 20) << "The write on " << pool_root.first_free_entry
                   << " with length " << size << " is split into two: "
                   << "pos=" << pool_root.first_free_entry << ", "
                   << "length=" << bl1.length() << "; "
                   << "pos=" << DATA_RING_BUFFER_OFFSET << ", "
                   << "length=" << bl.length() << dendl;

    bdev->aio_write(pool_root.first_free_entry, bl1, &aio->ioc, false,
                    WRITE_LIFE_NOT_SET);
    bdev->aio_write(DATA_RING_BUFFER_OFFSET, bl, &aio->ioc, false,
                    WRITE_LIFE_NOT_SET);
  } else {
    ldout(cct, 20) << "first_free_entry: " << pool_root.first_free_entry
                   << " bl length: " << bl.length() << dendl;
    bdev->aio_write(pool_root.first_free_entry, bl, &aio->ioc, false,
                    WRITE_LIFE_NOT_SET);
    ldout(cct, 20) << "finished aio_write log entries" << dendl;
  }
  // New first free entry
  pool_root.first_free_entry = data_pos;
}

template <typename I>
void WriteLog<I>::schedule_update_root(
    std::shared_ptr<WriteLogPoolRoot> root, Context *ctx) {
  bool need_finisher;
  {
    ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
    need_finisher = m_poolroot_to_update.empty() && !m_updating_pool_root;
    std::shared_ptr<WriteLogPoolRootUpdate> entry =
      std::make_shared<WriteLogPoolRootUpdate>(root, ctx);
    this->m_async_update_superblock++;
    this->m_async_op_tracker.start_op();
    m_poolroot_to_update.emplace_back(entry);
  }
  if (need_finisher) {
    enlist_op_update_root();
  }
}

template <typename I>
void WriteLog<I>::enlist_op_update_root() {
  Context *append_ctx = new LambdaContext([this](int r) {
    update_root_scheduled_ops();
  });
  this->m_work_queue.queue(append_ctx);
}

template <typename I>
void WriteLog<I>::update_root_scheduled_ops() {
  ldout(m_image_ctx.cct, 20) << dendl;

  std::shared_ptr<WriteLogPoolRoot> root;
  WriteLogPoolRootUpdateList root_updates;
  Context *ctx = nullptr;
  {
    std::lock_guard locker(m_lock);
    if (m_updating_pool_root) {
      /* Another thread is appending */
      ldout(m_image_ctx.cct, 15) << "Another thread is updating pool root"
                                 << dendl;
      return;
    }
    if (m_poolroot_to_update.size()) {
      m_updating_pool_root = true;
      root_updates.swap(m_poolroot_to_update);
    }
  }
  ceph_assert(!root_updates.empty());
  ldout(m_image_ctx.cct, 15) << "Update root number: " << root_updates.size()
                             << dendl;
  // We just update the last one, and call all the completions.
  auto entry = root_updates.back();
  root = entry->root;

  ctx = new LambdaContext([this, updates = std::move(root_updates)](int r) {
    ldout(m_image_ctx.cct, 15) << "Start to callback." << dendl;
    for (auto it = updates.begin(); it != updates.end(); it++) {
      Context *it_ctx = (*it)->ctx;
      it_ctx->complete(r);
    }
  });
  Context *append_ctx = new LambdaContext([this, ctx](int r) {
    ldout(m_image_ctx.cct, 15) << "Finish the update of pool root." << dendl;
    bool need_finisher = false;;
    assert(r == 0);
    {
      std::lock_guard locker(m_lock);
      m_updating_pool_root = false;
      need_finisher = !m_poolroot_to_update.empty();
    }
    if (need_finisher) {
      enlist_op_update_root();
    }
    ctx->complete(r);
  });
  AioTransContext* aio = new AioTransContext(m_image_ctx.cct, append_ctx);
  update_pool_root(root, aio);
}

template <typename I>
void WriteLog<I>::update_pool_root(std::shared_ptr<WriteLogPoolRoot> root,
                                   AioTransContext *aio) {
  bufferlist bl;
  SuperBlock superblock;
  superblock.root = *root;
  encode(superblock, bl);
  bl.append_zero(MIN_WRITE_ALLOC_SSD_SIZE - bl.length());
  ceph_assert(bl.length() % MIN_WRITE_ALLOC_SSD_SIZE == 0);
  bdev->aio_write(0, bl, &aio->ioc, false, WRITE_LIFE_NOT_SET);
  bdev->aio_submit(&aio->ioc);
}

template <typename I>
int WriteLog<I>::update_pool_root_sync(
    std::shared_ptr<WriteLogPoolRoot> root) {
  bufferlist bl;
  SuperBlock superblock;
  superblock.root = *root;
  encode(superblock, bl);
  bl.append_zero(MIN_WRITE_ALLOC_SSD_SIZE - bl.length());
  ceph_assert(bl.length() % MIN_WRITE_ALLOC_SSD_SIZE == 0);
  return bdev->write(0, bl, false);
}

template <typename I>
void WriteLog<I>::pre_io_check(WriteLogCacheEntry *log_entry,
                               uint64_t &length) {
  assert(log_entry->is_write() || log_entry->is_writesame());
  ceph_assert(log_entry->write_data_pos <= pool_size);

  length = log_entry->is_write() ? log_entry->write_bytes :
                                   log_entry->ws_datalen;
  length = round_up_to(length, MIN_WRITE_ALLOC_SSD_SIZE);
  ceph_assert(length != 0 && log_entry->write_data_pos + length <= pool_size);
}

template <typename I>
void WriteLog<I>::aio_read_data_block(
  WriteLogCacheEntry *log_entry, bufferlist *bl, Context *ctx) {
  std::vector<WriteLogCacheEntry*> log_entries {log_entry};
  std::vector<bufferlist *> bls {bl};
  aio_read_data_block(log_entries, bls, ctx);
}

template <typename I>
void WriteLog<I>::aio_read_data_block(
    std::vector<WriteLogCacheEntry*> &log_entries,
    std::vector<bufferlist *> &bls, Context *ctx) {
  ceph_assert(log_entries.size() == bls.size());

  //get the valid part
  Context *read_ctx = new LambdaContext(
    [this, log_entries, bls, ctx](int r) {
      for (unsigned int i = 0; i < log_entries.size(); i++) {
        bufferlist valid_data_bl;
        auto length = log_entries[i]->is_write() ? log_entries[i]->write_bytes :
                                                   log_entries[i]->ws_datalen;
        valid_data_bl.substr_of(*bls[i], 0, length);
        bls[i]->clear();
        bls[i]->append(valid_data_bl);
      }
     ctx->complete(r);
    });

  CephContext *cct = m_image_ctx.cct;
  AioTransContext *aio = new AioTransContext(cct, read_ctx);
  for (unsigned int i = 0; i < log_entries.size(); i++) {
    auto log_entry = log_entries[i];

    uint64_t length;
    pre_io_check(log_entry, length);
    ldout(cct, 20) << "Read at " << log_entry->write_data_pos
                   << ", length " << length << dendl;

    bdev->aio_read(log_entry->write_data_pos, length, bls[i], &aio->ioc);
  }
  bdev->aio_submit(&aio->ioc);
}

template <typename I>
void WriteLog<I>::complete_user_request(Context *&user_req, int r) {
  m_image_ctx.op_work_queue->queue(user_req, r);
}

} // namespace ssd
} // namespace pwl
} // namespace cache
} // namespace librbd

template class librbd::cache::pwl::ssd::WriteLog<librbd::ImageCtx>;
