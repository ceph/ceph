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

  set_block_count(offset_to_block(image_ctx.size));
  // TODO support resizing of entries based on number of provisioned blocks
  m_entries.resize(offset_to_block(image_ctx.ssd_cache_size < m_image_ctx.size?image_ctx.ssd_cache_size:m_image_ctx.size)); // 1GB of storage
  uint64_t block_id = 0;
  for (auto &entry : m_entries) {
    entry.on_disk_id = block_id++;
    m_free_lru.insert_tail(&entry);
  }
  m_block_map = new BlockMap(m_block_count);
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
  ldout(cct, 1) << "block=" << block << dendl;

  // TODO handle case where block is in prison (shouldn't be possible
  // if core properly registered blocks)

  Mutex::Locker locker(m_lock);
  Block* block_info = m_block_map->find_block(block);
  assert(block_info != nullptr);
  Entry *entry = block_info->entry;
  block_info->status = NOT_IN_CACHE;

  if (entry != nullptr) {
    m_clean_lru.remove(entry);
    m_free_lru.insert_tail(entry);
  }

  return 0;
}

template <typename I>
int StupidPolicy<I>::map(IOType io_type, uint64_t block, bool partial_block,
                         PolicyMapResult *policy_map_result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "block=" << block << dendl;

  Mutex::Locker locker(m_lock);
  if (block >= m_block_count) {
    ldout(cct, 1) << "block outside of valid range" << dendl;
    *policy_map_result = POLICY_MAP_RESULT_MISS;
    // TODO return error once resize handling is in-place
    return 0;
  }

  Entry *entry;
  Block* block_info = m_block_map->find_block(block);
  assert (block_info != nullptr);
  {
    // cache hit -- move entry to the front of the queue
    entry = block_info->entry;
    
    if (io_type == IO_TYPE_WRITE) {
      ldout(cct, 1) << "io_type == write, block: " << block << dendl;
      switch(block_info->status) {
        case LOCATE_IN_CACHE:
          *policy_map_result = POLICY_MAP_RESULT_HIT;
          m_clean_lru.remove(entry);
          m_free_lru.insert_tail(entry);
          block_info->status = NOT_IN_CACHE;
          break;
        case LOCATE_IN_BASE_CACHE:
          *policy_map_result = POLICY_MAP_RESULT_HIT_IN_BASE;
          block_info->status = NOT_IN_CACHE;
          break;
        case NOT_IN_CACHE:
        default:
          *policy_map_result = POLICY_MAP_RESULT_MISS;
          block_info->status = NOT_IN_CACHE; 
          break;
      }
      return 0;
    }
    switch(block_info->status) {
      case LOCATE_IN_CACHE:
        entry = block_info->entry;
        assert(entry != nullptr);
        *policy_map_result = POLICY_MAP_RESULT_HIT;
        m_clean_lru.remove(entry);
        m_clean_lru.insert_head(entry);
        ldout(cct, 1) << "cache hit, block: " << block << dendl;
        break;
      case LOCATE_IN_BASE_CACHE:
        *policy_map_result = POLICY_MAP_RESULT_HIT_IN_BASE;
        ldout(cct, 1) << "cache hit in base, block: " << block << dendl;
        break;
      case NOT_IN_CACHE:
      default:
        entry = reinterpret_cast<Entry*>(m_free_lru.get_head());
        if (entry != nullptr) {
          ldout(cct, 1) << "cache miss -- new entry, block: " << block << dendl;
          *policy_map_result = POLICY_MAP_RESULT_NEW;
          m_free_lru.remove(entry);
          m_clean_lru.insert_head(entry);
          block_info->entry = entry;
          block_info->status = LOCATE_IN_CACHE;
        } else {
          ldout(cct, 1) << "cache miss, no free slot in cache. block: " << block << dendl;
          *policy_map_result = POLICY_MAP_RESULT_MISS;
        }
        break;
    }
  }
  return 0;
}

template <typename I>
uint64_t StupidPolicy<I>::block_to_offset(uint64_t block) {
  CephContext *cct = m_image_ctx.cct;
  Mutex::Locker locker(m_lock);
  Block* block_info = m_block_map->find_block(block);
  switch (block_info->status) {
    case LOCATE_IN_CACHE:
      return block_info->entry->on_disk_id * m_block_size;
    case LOCATE_IN_BASE_CACHE:
      return block_info->block * m_block_size;
    case NOT_IN_CACHE:
      ldout(cct, 1) << "This block is not in cache, block: " << block << dendl;
      assert(0);
  }
}

template <typename I>
void StupidPolicy<I>::tick() {
  // stupid policy -- do nothing
}

template <typename I>
void StupidPolicy<I>::set_to_base_cache(uint64_t block) {
  Block* block_info = m_block_map->find_block(block);
  block_info->status = LOCATE_IN_BASE_CACHE;
}

template <typename I>
uint32_t StupidPolicy<I>::get_loc(uint64_t block) {
  Mutex::Locker locker(m_lock);
  uint32_t ret_data;
  Block* block_info = m_block_map->find_block(block);
  assert (block_info != nullptr);
  switch( block_info->status ) {
    case LOCATE_IN_BASE_CACHE:
      ret_data = ( block | (block_info->status << 30) ) ; 
      return ret_data;
    case LOCATE_IN_CACHE:
      assert(block_info->entry->on_disk_id <= MAX_BLOCK_ID);
      ret_data = ( block_info->entry->on_disk_id | (block_info->status << 30) ); 
      return ret_data;
    case NOT_IN_CACHE:
    default:
      return (NOT_IN_CACHE << 30);
  }
}

template <typename I>
void StupidPolicy<I>::set_loc(uint32_t *src) {
  Mutex::Locker locker(m_lock);
  Entry* entry;
  uint8_t loc;
  uint64_t on_disk_id;
  uint64_t block_id;
  for(auto block_it = m_block_map->begin(); block_it != m_block_map->end(); block_it++) {
    block_id = block_it->second->block;
    loc = src[block_id] >> 30;
    switch(loc) {
      case LOCATE_IN_CACHE:
        on_disk_id = (src[block_id] & MAX_BLOCK_ID);
        entry = &m_entries[on_disk_id];
        assert(entry != nullptr);
        m_free_lru.remove(entry);
        m_clean_lru.insert_head(entry);
        block_it->second->entry = entry;
        block_it->second->status = LOCATE_IN_CACHE;
        break;
      case LOCATE_IN_BASE_CACHE:
        on_disk_id = (src[block_id] & MAX_BLOCK_ID);
        assert(on_disk_id == block_id);
        block_it->second->status = LOCATE_IN_BASE_CACHE;
        break;
      case NOT_IN_CACHE:
      default:
        block_it->second->status = NOT_IN_CACHE;
        break;
    }
  }
}


} // namespace file
} // namespace cache
} // namespace librbd

template class librbd::cache::file::StupidPolicy<librbd::ImageCtx>;
