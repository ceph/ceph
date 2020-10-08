// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "SSDWriteLog.h"
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
#define dout_prefix *_dout << "librbd::cache::pwl::SSDWriteLog: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace pwl {

using namespace librbd::cache::pwl;

// SSD: this number can be updated later
const unsigned long int ops_appended_together = MAX_WRITES_PER_SYNC_POINT;

template <typename I>
SSDWriteLog<I>::SSDWriteLog(
    I &image_ctx, librbd::cache::pwl::ImageCacheState<I>* cache_state)
  : AbstractWriteLog<I>(image_ctx, cache_state)
{
}

template <typename I>
void SSDWriteLog<I>::initialize_pool(Context *on_finish, pwl::DeferredContexts &later) {
  CephContext *cct = m_image_ctx.cct;
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
  if (access(this->m_log_pool_name.c_str(), F_OK) != 0) {
    int fd = ::open(this->m_log_pool_name.c_str(), O_RDWR|O_CREAT, 0644);
    bool succeed = true;
    if (fd >= 0) {
      if (truncate(this->m_log_pool_name.c_str(), this->m_log_pool_config_size) != 0) {
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
    size_t small_write_size = MIN_WRITE_ALLOC_SIZE + sizeof(struct WriteLogPmemEntry);

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
    new_root->block_size = MIN_WRITE_ALLOC_SIZE;
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
     //load_existing_entries(later); #TODO: Implement and uncomment in later PR
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
int SSDWriteLog<I>::update_pool_root_sync(
    std::shared_ptr<WriteLogPoolRoot> root) {
  bufferlist bl;
  SuperBlock superblock;
  superblock.root = *root;
  encode(superblock, bl);
  bl.append_zero(MIN_WRITE_ALLOC_SIZE - bl.length());
  ceph_assert(bl.length() % MIN_WRITE_ALLOC_SIZE == 0);
  return bdev->write(0, bl, false);
}

} // namespace pwl
} // namespace cache
} // namespace librbd

template class librbd::cache::pwl::SSDWriteLog<librbd::ImageCtx>;
