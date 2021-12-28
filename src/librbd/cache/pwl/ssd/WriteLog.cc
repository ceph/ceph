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

static bool is_valid_pool_root(const WriteLogPoolRoot& root) {
  return root.pool_size % MIN_WRITE_ALLOC_SSD_SIZE == 0 &&
         root.first_valid_entry >= DATA_RING_BUFFER_OFFSET &&
         root.first_valid_entry < root.pool_size &&
         root.first_valid_entry % MIN_WRITE_ALLOC_SSD_SIZE == 0 &&
         root.first_free_entry >= DATA_RING_BUFFER_OFFSET &&
         root.first_free_entry < root.pool_size &&
         root.first_free_entry % MIN_WRITE_ALLOC_SSD_SIZE == 0;
}

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
    std::vector<std::shared_ptr<GenericWriteLogEntry>> &log_entries_to_read,
    std::vector<bufferlist*> &bls_to_read,
    uint64_t entry_hit_length, Extent hit_extent,
    pwl::C_ReadRequest *read_ctx) {
  // Make a bl for this hit extent. This will add references to the
  // write_entry->cache_bl */
  ldout(m_image_ctx.cct, 5) << dendl;
  auto write_entry = static_pointer_cast<WriteLogEntry>(map_entry.log_entry);
  buffer::list hit_bl;
  write_entry->copy_cache_bl(&hit_bl);
  bool writesame = write_entry->is_writesame_entry();
  auto hit_extent_buf = std::make_shared<ImageExtentBuf>(
      hit_extent, hit_bl, true, read_buffer_offset, writesame);
  read_ctx->read_extents.push_back(hit_extent_buf);

  if (!hit_bl.length()) {
    ldout(m_image_ctx.cct, 5) << "didn't hit RAM" << dendl;
    auto read_extent = read_ctx->read_extents.back();
    write_entry->inc_bl_refs();
    log_entries_to_read.push_back(std::move(write_entry));
    bls_to_read.push_back(&read_extent->m_bl);
  }
}

template <typename I>
void WriteLog<I>::complete_read(
    std::vector<std::shared_ptr<GenericWriteLogEntry>> &log_entries_to_read,
    std::vector<bufferlist*> &bls_to_read,
    Context *ctx) {
  if (!log_entries_to_read.empty()) {
    aio_read_data_blocks(log_entries_to_read, bls_to_read, ctx);
  } else {
    ctx->complete(0);
  }
}

template <typename I>
int WriteLog<I>::create_and_open_bdev() {
  CephContext *cct = m_image_ctx.cct;

  bdev = BlockDevice::create(cct, this->m_log_pool_name, aio_cache_cb,
                             nullptr, nullptr, nullptr);
  int r = bdev->open(this->m_log_pool_name);
  if (r < 0) {
    lderr(cct) << "failed to open bdev" << dendl;
    delete bdev;
    return r;
  }

  ceph_assert(this->m_log_pool_size % MIN_WRITE_ALLOC_SSD_SIZE == 0);
  if (bdev->get_size() != this->m_log_pool_size) {
    lderr(cct) << "size mismatch: bdev size " << bdev->get_size()
               << " (block size " << bdev->get_block_size()
               << ") != pool size " << this->m_log_pool_size << dendl;
    bdev->close();
    delete bdev;
    return -EINVAL;
  }

  return 0;
}

template <typename I>
bool WriteLog<I>::initialize_pool(Context *on_finish,
                                  pwl::DeferredContexts &later) {
  int r;
  CephContext *cct = m_image_ctx.cct;

  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
  if (access(this->m_log_pool_name.c_str(), F_OK) != 0) {
    int fd = ::open(this->m_log_pool_name.c_str(), O_RDWR|O_CREAT, 0644);
    bool succeed = true;
    if (fd >= 0) {
      if (truncate(this->m_log_pool_name.c_str(),
                   this->m_log_pool_size) != 0) {
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
      return false;
    }

    r = create_and_open_bdev();
    if (r < 0) {
      on_finish->complete(r);
      return false;
    }
    m_cache_state->present = true;
    m_cache_state->clean = true;
    m_cache_state->empty = true;
    /* new pool, calculate and store metadata */

    /* Keep ring buffer at least MIN_WRITE_ALLOC_SSD_SIZE bytes free.
     * In this way, when all ring buffer spaces are allocated,
     * m_first_free_entry and m_first_valid_entry will not be equal.
     * Equal only means the cache is empty. */
    this->m_bytes_allocated_cap = this->m_log_pool_size -
        DATA_RING_BUFFER_OFFSET - MIN_WRITE_ALLOC_SSD_SIZE;
    /* Log ring empty */
    m_first_free_entry = DATA_RING_BUFFER_OFFSET;
    m_first_valid_entry = DATA_RING_BUFFER_OFFSET;

    auto new_root = std::make_shared<WriteLogPoolRoot>(pool_root);
    new_root->layout_version = SSD_LAYOUT_VERSION;
    new_root->pool_size = this->m_log_pool_size;
    new_root->flushed_sync_gen = this->m_flushed_sync_gen;
    new_root->block_size = MIN_WRITE_ALLOC_SSD_SIZE;
    new_root->first_free_entry = m_first_free_entry;
    new_root->first_valid_entry = m_first_valid_entry;
    new_root->num_log_entries = 0;
    pool_root = *new_root;

    r = update_pool_root_sync(new_root);
    if (r != 0) {
      lderr(cct) << "failed to initialize pool ("
                 << this->m_log_pool_name << ")" << dendl;
      bdev->close();
      delete bdev;
      on_finish->complete(r);
      return false;
    }
  } else {
    ceph_assert(m_cache_state->present);
    r = create_and_open_bdev();
    if (r < 0) {
      on_finish->complete(r);
      return false;
    }

    bufferlist bl;
    SuperBlock superblock;
    ::IOContext ioctx(cct, nullptr);
    r = bdev->read(0, MIN_WRITE_ALLOC_SSD_SIZE, &bl, &ioctx, false);
    if (r < 0) {
      lderr(cct) << "Read ssd cache superblock failed " << dendl;
      goto error_handle;
    }
    auto p = bl.cbegin();
    decode(superblock, p);
    pool_root = superblock.root;
    ldout(cct, 1) << "Decoded root: pool_size=" << pool_root.pool_size
                  << " first_valid_entry=" << pool_root.first_valid_entry
                  << " first_free_entry=" << pool_root.first_free_entry
                  << " flushed_sync_gen=" << pool_root.flushed_sync_gen
                  << dendl;
    ceph_assert(is_valid_pool_root(pool_root));
    if (pool_root.layout_version != SSD_LAYOUT_VERSION) {
      lderr(cct) << "Pool layout version is "
                 << pool_root.layout_version
                 << " expected " << SSD_LAYOUT_VERSION
                 << dendl;
      goto error_handle;
    }
    if (pool_root.block_size != MIN_WRITE_ALLOC_SSD_SIZE) {
      lderr(cct) << "Pool block size is " << pool_root.block_size
                 << " expected " << MIN_WRITE_ALLOC_SSD_SIZE
                 << dendl;
      goto error_handle;
    }

    this->m_log_pool_size = pool_root.pool_size;
    this->m_flushed_sync_gen = pool_root.flushed_sync_gen;
    this->m_first_valid_entry = pool_root.first_valid_entry;
    this->m_first_free_entry = pool_root.first_free_entry;
    this->m_bytes_allocated_cap = this->m_log_pool_size -
                                  DATA_RING_BUFFER_OFFSET -
                                  MIN_WRITE_ALLOC_SSD_SIZE;

    load_existing_entries(later);
    m_cache_state->clean = this->m_dirty_log_entries.empty();
    m_cache_state->empty = m_log_entries.empty();
  }
  return true;

error_handle:
  bdev->close();
  delete bdev;
  on_finish->complete(-EINVAL);
  return false;
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
  CephContext *cct = m_image_ctx.cct;
  std::map<uint64_t, std::shared_ptr<SyncPointLogEntry>> sync_point_entries;
  std::map<uint64_t, bool> missing_sync_points;

  // Iterate through the log_entries and append all the write_bytes
  // of each entry to fetch the pos of next 4k of log_entries. Iterate
  // through the log entries and append them to the in-memory vector
  for (uint64_t next_log_pos = this->m_first_valid_entry;
       next_log_pos != this->m_first_free_entry; ) {
    // read the entries from SSD cache and decode
    bufferlist bl_entries;
    ::IOContext ioctx_entry(cct, nullptr);
    bdev->read(next_log_pos, MIN_WRITE_ALLOC_SSD_SIZE, &bl_entries,
               &ioctx_entry, false);
    std::vector<WriteLogCacheEntry> ssd_log_entries;
    auto pl = bl_entries.cbegin();
    decode(ssd_log_entries, pl);
    ldout(cct, 5) << "decoded ssd log entries" << dendl;
    uint64_t curr_log_pos = next_log_pos;
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
    if (next_log_pos >= this->m_log_pool_size) {
      next_log_pos = next_log_pos % this->m_log_pool_size + DATA_RING_BUFFER_OFFSET;
    }
  }
  this->update_sync_points(missing_sync_points, sync_point_entries, later);
  if (m_first_valid_entry > m_first_free_entry) {
    m_bytes_allocated = this->m_log_pool_size - m_first_valid_entry +
			  m_first_free_entry - DATA_RING_BUFFER_OFFSET;
  } else {
    m_bytes_allocated = m_first_free_entry - m_first_valid_entry;
  }
}

// For SSD we don't calc m_bytes_allocated in this
template <typename I>
void WriteLog<I>::inc_allocated_cached_bytes(
    std::shared_ptr<pwl::GenericLogEntry> log_entry) {
  if (log_entry->is_write_entry()) {
    this->m_bytes_cached += log_entry->write_bytes();
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

  // Setup buffer, and get all the number of required resources
  req->setup_buffer_resources(&bytes_cached, &bytes_dirtied, &bytes_allocated,
                              &num_lanes, &num_log_entries,
                              &num_unpublished_reserves);

  ceph_assert(!num_lanes);
  if (num_log_entries) {
    bytes_allocated += num_log_entries * MIN_WRITE_ALLOC_SSD_SIZE;
    num_log_entries = 0;
  }
  ceph_assert(!num_unpublished_reserves);

  alloc_succeeds = this->check_allocation(req, bytes_cached, bytes_dirtied,
                                          bytes_allocated, num_lanes,
                                          num_log_entries,
                                          num_unpublished_reserves);
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
void WriteLog<I>::schedule_append_ops(GenericLogOperations &ops, C_BlockIORequestT *req) {
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

    // To preserve the order of overlapping IOs, release_cell() may be
    // called only after the ops are added to m_ops_to_append.
    // As soon as m_lock is released, the appended ops can be picked up
    // by append_scheduled_ops() in another thread and req can be freed.
    if (req != nullptr) {
      if (persist_on_flush) {
        req->complete_user_request(0);
      }
      req->release_cell();
    }
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
  this->schedule_append(ops, req);
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
    this->m_async_update_superblock--;
    this->m_async_op_tracker.finish_op();
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
      this->m_async_append_ops--;
      this->m_async_op_tracker.finish_op();
    });
  // Append logs and update first_free_update
  append_ops(ops, append_ctx, new_first_free_entry);

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
  if (m_cache_state->empty && !m_log_entries.empty()) {
    m_cache_state->empty = false;
    this->update_image_cache_state();
  }
}

template <typename I>
void WriteLog<I>::construct_flush_entries(pwl::GenericLogEntries entries_to_flush,
					  DeferredContexts &post_unlock,
					  bool has_write_entry) {
  // snapshot so we behave consistently
  bool invalidating = this->m_invalidating;

  if (invalidating || !has_write_entry) {
    for (auto &log_entry : entries_to_flush) {
      GuardedRequestFunctionContext *guarded_ctx =
        new GuardedRequestFunctionContext([this, log_entry, invalidating]
          (GuardedRequestFunctionContext &guard_ctx) {
            log_entry->m_cell = guard_ctx.cell;
            Context *ctx = this->construct_flush_entry(log_entry, invalidating);

            if (!invalidating) {
              ctx = new LambdaContext([this, log_entry, ctx](int r) {
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
  } else {
    int count = entries_to_flush.size();
    std::vector<std::shared_ptr<GenericWriteLogEntry>> write_entries;
    std::vector<bufferlist *> read_bls;

    write_entries.reserve(count);
    read_bls.reserve(count);

    for (auto &log_entry : entries_to_flush) {
      if (log_entry->is_write_entry()) {
	bufferlist *bl = new bufferlist;
	auto write_entry = static_pointer_cast<WriteLogEntry>(log_entry);
	write_entry->inc_bl_refs();
	write_entries.push_back(write_entry);
	read_bls.push_back(bl);
      }
    }

    Context *ctx = new LambdaContext(
      [this, entries_to_flush, read_bls](int r) {
        int i = 0;
	GuardedRequestFunctionContext *guarded_ctx = nullptr;

	for (auto &log_entry : entries_to_flush) {
	  if (log_entry->is_write_entry()) {
	    bufferlist captured_entry_bl;
	    captured_entry_bl.claim_append(*read_bls[i]);
	    delete read_bls[i++];

	    guarded_ctx = new GuardedRequestFunctionContext([this, log_entry, captured_entry_bl]
              (GuardedRequestFunctionContext &guard_ctx) {
                log_entry->m_cell = guard_ctx.cell;
                Context *ctx = this->construct_flush_entry(log_entry, false);

	        m_image_ctx.op_work_queue->queue(new LambdaContext(
	          [this, log_entry, entry_bl=std::move(captured_entry_bl), ctx](int r) {
		    auto captured_entry_bl = std::move(entry_bl);
		    ldout(m_image_ctx.cct, 15) << "flushing:" << log_entry
			                       << " " << *log_entry << dendl;
		    log_entry->writeback_bl(this->m_image_writeback, ctx,
                                            std::move(captured_entry_bl));
	          }), 0);
	      });
	  } else {
	    guarded_ctx = new GuardedRequestFunctionContext([this, log_entry]
              (GuardedRequestFunctionContext &guard_ctx) {
                log_entry->m_cell = guard_ctx.cell;
                Context *ctx = this->construct_flush_entry(log_entry, false);
	        m_image_ctx.op_work_queue->queue(new LambdaContext(
		  [this, log_entry, ctx](int r) {
		    ldout(m_image_ctx.cct, 15) << "flushing:" << log_entry
                                               << " " << *log_entry << dendl;
		    log_entry->writeback(this->m_image_writeback, ctx);
		  }), 0);
            });
	  }
          this->detain_flush_guard_request(log_entry, guarded_ctx);
	}
      });

    aio_read_data_blocks(write_entries, read_bls, ctx);
  }
}

template <typename I>
void WriteLog<I>::process_work() {
  CephContext *cct = m_image_ctx.cct;
  int max_iterations = 4;
  bool wake_up_requested = false;
  uint64_t aggressive_high_water_bytes =
      this->m_bytes_allocated_cap * AGGRESSIVE_RETIRE_HIGH_WATER;
  uint64_t high_water_bytes = this->m_bytes_allocated_cap * RETIRE_HIGH_WATER;

  ldout(cct, 20) << dendl;

  do {
    {
      std::lock_guard locker(m_lock);
      this->m_wake_up_requested = false;
    }
    if (this->m_alloc_failed_since_retire || (this->m_shutting_down) ||
        this->m_invalidating || m_bytes_allocated > high_water_bytes) {
      ldout(m_image_ctx.cct, 10) << "alloc_fail=" << this->m_alloc_failed_since_retire
                                 << ", allocated > high_water="
                                 << (m_bytes_allocated > high_water_bytes)
                                 << dendl;
      retire_entries((this->m_shutting_down || this->m_invalidating ||
                      m_bytes_allocated > aggressive_high_water_bytes)
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
  uint64_t initial_first_valid_entry;
  uint64_t first_valid_entry;

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
      uint64_t control_block_pos = m_log_entries.front()->log_entry_index;
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
                  (control_block_pos + data_length + MIN_WRITE_ALLOC_SSD_SIZE) %
                  this->m_log_pool_size + DATA_RING_BUFFER_OFFSET);
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
          if ((*it)->write_bytes() > 0 || (*it)->bytes_dirty() > 0) {
            auto gen_write_entry = static_pointer_cast<GenericWriteLogEntry>(*it);
            if (gen_write_entry) {
                this->m_blocks_to_log_entries.remove_log_entry(gen_write_entry);
            }
          }
        }

        ldout(cct, 20) << "span with " << retiring_subentries.size()
                       << " entries: control_block_pos=" << control_block_pos
                       << " data_length=" << data_length
                       << dendl;
        retiring_entries.insert(
            retiring_entries.end(), retiring_subentries.begin(),
            retiring_subentries.end());

        first_valid_entry = control_block_pos + data_length +
            MIN_WRITE_ALLOC_SSD_SIZE;
        if (first_valid_entry >= this->m_log_pool_size) {
          first_valid_entry = first_valid_entry % this->m_log_pool_size +
              DATA_RING_BUFFER_OFFSET;
        }
      } else {
        break;
      }
    }
  }
  if (retiring_entries.size()) {
    ldout(cct, 20) << "Retiring " << retiring_entries.size() << " entries"
                   << dendl;

    // Advance first valid entry and release buffers
    uint64_t flushed_sync_gen;
    std::lock_guard append_locker(this->m_log_append_lock);
    {
      std::lock_guard locker(m_lock);
      flushed_sync_gen = this->m_flushed_sync_gen;
    }

    ceph_assert(first_valid_entry != initial_first_valid_entry);
    auto new_root = std::make_shared<WriteLogPoolRoot>(pool_root);
    new_root->flushed_sync_gen = flushed_sync_gen;
    new_root->first_valid_entry = first_valid_entry;
    pool_root.flushed_sync_gen = flushed_sync_gen;
    pool_root.first_valid_entry = first_valid_entry;

    Context *ctx = new LambdaContext(
      [this, first_valid_entry, initial_first_valid_entry,
       retiring_entries](int r) {
        uint64_t allocated_bytes = 0;
        uint64_t cached_bytes = 0;
        uint64_t former_log_pos = 0;
        for (auto &entry : retiring_entries) {
          ceph_assert(entry->log_entry_index != 0);
          if (entry->log_entry_index != former_log_pos ) {
            // Space for control blocks
            allocated_bytes += MIN_WRITE_ALLOC_SSD_SIZE;
            former_log_pos = entry->log_entry_index;
          }
          if (entry->is_write_entry()) {
            cached_bytes += entry->write_bytes();
            // space for userdata
            allocated_bytes += entry->get_aligned_data_size();
          }
        }
        {
          std::lock_guard locker(m_lock);
          m_first_valid_entry = first_valid_entry;
          ceph_assert(m_first_valid_entry % MIN_WRITE_ALLOC_SSD_SIZE == 0);
          ceph_assert(this->m_bytes_allocated >= allocated_bytes);
          this->m_bytes_allocated -= allocated_bytes;
          ceph_assert(this->m_bytes_cached >= cached_bytes);
          this->m_bytes_cached -= cached_bytes;
          if (!m_cache_state->empty && m_log_entries.empty()) {
            m_cache_state->empty = true;
            this->update_image_cache_state();
          }

          ldout(m_image_ctx.cct, 20)
            << "Finished root update: " << "initial_first_valid_entry="
            << initial_first_valid_entry << ", " << "m_first_valid_entry="
            << m_first_valid_entry << "," << "release space = "
            << allocated_bytes << "," << "m_bytes_allocated="
            << m_bytes_allocated << "," << "release cached space="
            << cached_bytes << "," << "m_bytes_cached="
            << this->m_bytes_cached << dendl;

          this->m_alloc_failed_since_retire = false;
          this->wake_up();
        }

        this->dispatch_deferred_writes();
        this->process_writeback_dirty_entries();
        m_async_update_superblock--;
        this->m_async_op_tracker.finish_op();
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
                             uint64_t* new_first_free_entry) {
  GenericLogEntriesVector log_entries;
  CephContext *cct = m_image_ctx.cct;
  uint64_t span_payload_len = 0;
  uint64_t bytes_to_free = 0;
  ldout(cct, 20) << "Appending " << ops.size() << " log entries." << dendl;

  *new_first_free_entry = pool_root.first_free_entry;
  AioTransContext* aio = new AioTransContext(cct, ctx);

  utime_t now = ceph_clock_now();
  for (auto &operation : ops) {
    operation->log_append_start_time = now;
    auto log_entry = operation->get_log_entry();

    if (log_entries.size() == CONTROL_BLOCK_MAX_LOG_ENTRIES ||
        span_payload_len >= SPAN_MAX_DATA_LEN) {
      if (log_entries.size() > 1) {
        bytes_to_free += (log_entries.size() - 1) * MIN_WRITE_ALLOC_SSD_SIZE;
      }
      write_log_entries(log_entries, aio, new_first_free_entry);
      log_entries.clear();
      span_payload_len = 0;
    }
    log_entries.push_back(log_entry);
    span_payload_len += log_entry->write_bytes();
  }
  if (!span_payload_len || !log_entries.empty()) {
    if (log_entries.size() > 1) {
      bytes_to_free += (log_entries.size() - 1) * MIN_WRITE_ALLOC_SSD_SIZE;
    }
    write_log_entries(log_entries, aio, new_first_free_entry);
  }

  {
    std::lock_guard locker1(m_lock);
    m_first_free_entry = *new_first_free_entry;
    m_bytes_allocated -= bytes_to_free;
  }

  bdev->aio_submit(&aio->ioc);
}

template <typename I>
void WriteLog<I>::write_log_entries(GenericLogEntriesVector log_entries,
                                    AioTransContext *aio, uint64_t *pos) {
  CephContext *cct = m_image_ctx.cct;
  ldout(m_image_ctx.cct, 20) << "pos=" << *pos << dendl;
  ceph_assert(*pos >= DATA_RING_BUFFER_OFFSET &&
              *pos < this->m_log_pool_size &&
              *pos % MIN_WRITE_ALLOC_SSD_SIZE == 0);

  // The first block is for log entries
  uint64_t control_block_pos = *pos;
  *pos += MIN_WRITE_ALLOC_SSD_SIZE;
  if (*pos == this->m_log_pool_size) {
    *pos = DATA_RING_BUFFER_OFFSET;
  }

  std::vector<WriteLogCacheEntry> persist_log_entries;
  bufferlist data_bl;
  for (auto &log_entry : log_entries) {
    log_entry->log_entry_index = control_block_pos;
    // Append data buffer for write operations
    if (log_entry->is_write_entry()) {
      auto write_entry = static_pointer_cast<WriteLogEntry>(log_entry);
      auto cache_bl = write_entry->get_cache_bl();
      auto align_size = write_entry->get_aligned_data_size();
      data_bl.append(cache_bl);
      data_bl.append_zero(align_size - cache_bl.length());

      write_entry->ram_entry.write_data_pos = *pos;
      *pos += align_size;
      if (*pos >= this->m_log_pool_size) {
        *pos = *pos % this->m_log_pool_size + DATA_RING_BUFFER_OFFSET;
      }
    }
    // push_back _after_ setting write_data_pos
    persist_log_entries.push_back(log_entry->ram_entry);
  }

  //aio write
  bufferlist bl;
  encode(persist_log_entries, bl);
  ceph_assert(bl.length() <= MIN_WRITE_ALLOC_SSD_SIZE);
  bl.append_zero(MIN_WRITE_ALLOC_SSD_SIZE - bl.length());
  bl.append(data_bl);
  ceph_assert(bl.length() % MIN_WRITE_ALLOC_SSD_SIZE == 0);
  if (control_block_pos + bl.length() > this->m_log_pool_size) {
    //exceeds border, need to split
    uint64_t size = bl.length();
    bufferlist bl1;
    bl.splice(0, this->m_log_pool_size - control_block_pos, &bl1);
    ceph_assert(bl.length() == (size - bl1.length()));

    ldout(cct, 20) << "write " << control_block_pos << "~"
		   << size << " spans boundary, split into "
		   << control_block_pos << "~" << bl1.length()
		   << " and " << DATA_RING_BUFFER_OFFSET << "~"
		   << bl.length() << dendl;
    bdev->aio_write(control_block_pos, bl1, &aio->ioc, false,
                    WRITE_LIFE_NOT_SET);
    bdev->aio_write(DATA_RING_BUFFER_OFFSET, bl, &aio->ioc, false,
                    WRITE_LIFE_NOT_SET);
  } else {
    ldout(cct, 20) << "write " << control_block_pos << "~"
                   << bl.length() << dendl;
    bdev->aio_write(control_block_pos, bl, &aio->ioc, false,
                    WRITE_LIFE_NOT_SET);
  }
}

template <typename I>
void WriteLog<I>::schedule_update_root(
    std::shared_ptr<WriteLogPoolRoot> root, Context *ctx) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 15) << "New root: pool_size=" << root->pool_size
                 << " first_valid_entry=" << root->first_valid_entry
                 << " first_free_entry=" << root->first_free_entry
                 << " flushed_sync_gen=" << root->flushed_sync_gen
                 << dendl;
  ceph_assert(is_valid_pool_root(*root));

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
void WriteLog<I>::aio_read_data_block(std::shared_ptr<GenericWriteLogEntry> log_entry,
                                      bufferlist *bl, Context *ctx) {
  std::vector<std::shared_ptr<GenericWriteLogEntry>> log_entries = {std::move(log_entry)};
  std::vector<bufferlist *> bls {bl};
  aio_read_data_blocks(log_entries, bls, ctx);
}

template <typename I>
void WriteLog<I>::aio_read_data_blocks(
    std::vector<std::shared_ptr<GenericWriteLogEntry>> &log_entries,
    std::vector<bufferlist *> &bls, Context *ctx) {
  ceph_assert(log_entries.size() == bls.size());

  //get the valid part
  Context *read_ctx = new LambdaContext(
    [log_entries, bls, ctx](int r) {
      for (unsigned int i = 0; i < log_entries.size(); i++) {
        bufferlist valid_data_bl;
        auto write_entry = static_pointer_cast<WriteLogEntry>(log_entries[i]);
        auto length = write_entry->ram_entry.is_write() ? write_entry->ram_entry.write_bytes
                                                        : write_entry->ram_entry.ws_datalen;

        valid_data_bl.substr_of(*bls[i], 0, length);
        bls[i]->clear();
        bls[i]->append(valid_data_bl);
        write_entry->dec_bl_refs();
      }
     ctx->complete(r);
    });

  CephContext *cct = m_image_ctx.cct;
  AioTransContext *aio = new AioTransContext(cct, read_ctx);
  for (unsigned int i = 0; i < log_entries.size(); i++) {
    WriteLogCacheEntry *log_entry = &log_entries[i]->ram_entry;

    ceph_assert(log_entry->is_write() || log_entry->is_writesame());
    uint64_t len = log_entry->is_write() ? log_entry->write_bytes :
                                           log_entry->ws_datalen;
    uint64_t align_len = round_up_to(len, MIN_WRITE_ALLOC_SSD_SIZE);

    ldout(cct, 20) << "entry i=" << i << " " << log_entry->write_data_pos
                   << "~" << len << dendl;
    ceph_assert(log_entry->write_data_pos >= DATA_RING_BUFFER_OFFSET &&
                log_entry->write_data_pos < pool_root.pool_size);
    ceph_assert(align_len);
    if (log_entry->write_data_pos + align_len > pool_root.pool_size) {
      // spans boundary, need to split
      uint64_t len1 = pool_root.pool_size - log_entry->write_data_pos;
      uint64_t len2 = align_len - len1;

      ldout(cct, 20) << "read " << log_entry->write_data_pos << "~"
                     << align_len << " spans boundary, split into "
                     << log_entry->write_data_pos << "~" << len1
                     << " and " << DATA_RING_BUFFER_OFFSET << "~"
                     << len2 << dendl;
      bdev->aio_read(log_entry->write_data_pos, len1, bls[i], &aio->ioc);
      bdev->aio_read(DATA_RING_BUFFER_OFFSET, len2, bls[i], &aio->ioc);
    } else {
      ldout(cct, 20) << "read " << log_entry->write_data_pos << "~"
                     << align_len << dendl;
      bdev->aio_read(log_entry->write_data_pos, align_len, bls[i], &aio->ioc);
    }
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
