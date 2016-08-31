// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/file/StupidPolicy.h"
#include "common/dout.h"
#include "librbd/ImageCtx.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::file::StupidPolicy: " << this \
                           << " " <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace file {

template <typename I>
StupidPolicy<I>::StupidPolicy(I &image_ctx)
  : m_image_ctx(image_ctx),
    m_lock("librbd::cache::file::StupidPolicy::m_lock") {

  // TODO support resizing of entries based on number of provisioned blocks
  m_entries.resize(262144); // 1GB of storage
  for (auto &entry : m_entries) {
    m_free_lru.insert_tail(&entry);
  }
}

template <typename I>
void StupidPolicy<I>::set_block_count(uint64_t block_count) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "block_count=" << block_count << dendl;

  // TODO ensure all entries are in-bound
  Mutex::Locker locker(m_lock);
  m_block_count = block_count;

}

template <typename I>
int StupidPolicy<I>::invalidate(uint64_t block) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "block=" << block << dendl;

  // TODO handle case where block is in prison (shouldn't be possible
  // if core properly registered blocks)

  Mutex::Locker locker(m_lock);
  auto entry_it = m_block_to_entries.find(block);
  if (entry_it == m_block_to_entries.end()) {
    return 0;
  }

  Entry *entry = entry_it->second;
  m_block_to_entries.erase(entry_it);

  LRUList *lru;
  if (entry->dirty) {
    lru = &m_dirty_lru;
  } else {
    lru = &m_clean_lru;
  }
  lru->remove(entry);

  m_free_lru.insert_tail(entry);
  return 0;
}

template <typename I>
int StupidPolicy<I>::map(OpType op_type, uint64_t block, bool partial_block,
                         MapResult *map_result, uint64_t *replace_cache_block) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "block=" << block << dendl;

  Mutex::Locker locker(m_lock);
  if (block >= m_block_count) {
    lderr(cct) << "block outside of valid range" << dendl;
    *map_result = MAP_RESULT_MISS;
    // TODO return error once resize handling is in-place
    return 0;
  }

  Entry *entry;
  auto entry_it = m_block_to_entries.find(block);
  if (entry_it != m_block_to_entries.end()) {
    // cache hit -- move entry to the front of the queue
    ldout(cct, 20) << "cache hit" << dendl;
    *map_result = MAP_RESULT_HIT;

    entry = entry_it->second;
    LRUList *lru;
    if (entry->dirty) {
      lru = &m_dirty_lru;
    } else {
      lru = &m_clean_lru;
    }

    lru->remove(entry);
    lru->insert_head(entry);
    return 0;
  }

  // cache miss
  entry = reinterpret_cast<Entry*>(m_free_lru.get_head());
  if (entry != nullptr) {
    // entries are available -- allocate a slot
    ldout(cct, 20) << "cache miss -- new entry" << dendl;
    *map_result = MAP_RESULT_NEW;
    m_free_lru.remove(entry);

    entry->block = block;
    m_block_to_entries[block] = entry;
    m_clean_lru.insert_head(entry);
    return 0;
  }

  // if we have clean entries we can demote, attempt to steal the oldest
  entry = reinterpret_cast<Entry*>(m_clean_lru.get_tail());
  if (entry != nullptr) {
    // TODO attempt to lock would-be evicted block
    if (true) {
      ldout(cct, 20) << "cache miss -- replace entry" << dendl;
      *map_result = MAP_RESULT_REPLACE;
      *replace_cache_block = entry->block;

      m_block_to_entries.erase(entry->block);
      m_clean_lru.remove(entry);

      entry->block = block;
      m_block_to_entries[block] = entry;
      m_clean_lru.insert_head(entry);
      return 0;
    }
  }

  // no clean entries to evict -- treat this as a miss
  ldout(cct, 20) << "cache miss" << dendl;
  *map_result = MAP_RESULT_MISS;
  return 0;
}

template <typename I>
void StupidPolicy<I>::tick() {
  // stupid policy -- do nothing
}

} // namespace file
} // namespace cache
} // namespace librbd

template class librbd::cache::file::StupidPolicy<librbd::ImageCtx>;
