#include "BlueFSCache.h"
#include "bluestore_common.h"
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include "common/errno.h"
#include "common/dout.h"
#include "common/debug.h"
#include "include/buffer_fwd.h"
#include "common/ceph_context.h"
#include "include/denc.h"
#include "tools/cephfs/EventOutput.h"


#define dout_context cct
#define dout_subsys ceph_subsys_bluefs
#undef dout_prefix
#define dout_prefix *_dout << "bluefs "

LRUCache::LRUCache(size_t cache_capacity, size_t evict_bytes, CephContext* cct)
        : cache_capacity(cache_capacity), evict_bytes(evict_bytes), current_bytes(0), cct(cct) {}

cache_bufferlist LRUCache::get_nearest_offset(uint64_t inode, uint64_t offset,
                                              uint64_t *buffer_offset) {

  dout(20) << __func__ << " inode " << inode << std::hex << " offset 0x" << offset << std::dec << dendl;
  std::shared_lock lock(mutex);
  inode_offset_pair key = {inode, offset};
  auto it = kv.find(key);
  if (it != kv.end()) {
    dout(20) << __func__ << " inode " << inode << std::hex << " offset 0x" << offset << " exists in cache" << std::dec << dendl;
    lock.unlock();
    *buffer_offset = 0;
    update_recency(key);
    return it->second;
  }
  it = kv.upper_bound({inode, offset});
  --it;
  if (it != kv.end() && it->first.first == inode &&
      (offset >= it->first.second &&
       offset < (it->first.second + it->second->length()))) {
    key = it->first;
    lock.unlock();
    update_recency(key);
    *buffer_offset = offset - it->first.second;
    dout(20) << __func__ << " inode " << inode << std::hex << " offset 0x" << offset << " exists within offset 0x" << it->first.second  << " at buffer offset 0x" << buffer_offset << std::dec <<  dendl;
    return it->second;
  }
  return NULL;
}

int LRUCache::put(uint64_t inode, uint64_t offset, cache_bufferlist value) {
  size_t new_size = value->length();

  std::unique_lock lock(mutex);

  auto it = kv.find({inode, offset});

  if (it != kv.end()) {
    size_t old_size = it->second->length();
    // if the bufferlist value is less than the existing bufferlist, no need to
    // update the map
    if (new_size < old_size) {
      dout(20) << __func__ << " inode " << inode << std::hex << " 0x" << offset << "~" << new_size << " is less than the already existing bufferlist size 0x" << old_size << std::dec << dendl;
      return 0;
    }

    if (new_size > cache_capacity) {
      dout(20) << __func__ << " inode " << inode << std::hex << " 0x" << offset << "~" << new_size << " is greater than the cache capacity 0x" << cache_capacity << std::dec << dendl;
      return -EINVAL;
    }

    if (current_bytes + new_size - old_size > cache_capacity) {
      dout(20) << __func__ << " inode " << inode << std::hex << " 0x" << offset << "~" << new_size << " evict cache necessary" << std::dec << dendl;
      evict(std::max(evict_bytes, new_size));
      // while evicting, the key could be deleted, hence not using the iterator
      // to update the map
      it = kv.find({inode, offset});
      if (it == kv.end()) {
        dout(20) << __func__ << " inode " << inode << std::hex << " 0x" << offset << "~" << new_size << " adding new entry to cache" << std::dec << dendl;
        kv[{inode, offset}] = value;
        current_bytes += new_size;
        update_recency({inode, offset});
        dout(20) << __func__ << " current cache size 0x" << std::hex << current_bytes << " and cache capacity 0x" << cache_capacity << std::dec << dendl;
        return 0;
      }
    }

    it->second->clear();
    it->second->claim_append(*value);
    current_bytes -= old_size;
    current_bytes += new_size;
    dout(20) << __func__ << " inode " << inode << std::hex << " 0x" << offset << "~" << new_size << " updating existing entry in cache" << std::dec << dendl;
    dout(20) << __func__ << " current cache size 0x" << std::hex << current_bytes << " and cache capacity 0x" << cache_capacity << std::dec << dendl;
    update_recency({inode, offset});
    return 0;
  }

  // New insertion: check space
  if (new_size > cache_capacity) {
    dout(20) << __func__ << " inode " << inode << std::hex << " 0x" << offset << "~" << new_size << " is greater than the cache capacity 0x" << cache_capacity << std::dec << dendl;
    return -EINVAL;
  }

  // Evict until enough space
  if (current_bytes + new_size > cache_capacity) {
    dout(20) << __func__ << " inode " << inode << std::hex << " 0x" << offset << "~" << new_size << " evict cache necessary" << std::dec << dendl;
    evict(std::max(evict_bytes, cache_capacity));
  }

  dout(20) << __func__ << " inode " << inode << std::hex << " 0x" << offset << "~" << new_size << " adding new entry to cache" << std::dec << dendl;
  std::unique_lock lockIter(mutex_iter);
  kv[{inode, offset}] = value;
  access_order.push_front({inode, offset});
  key_iter[{inode, offset}] = access_order.begin();
  current_bytes += new_size;
  dout(20) << __func__ << " current cache size 0x" << std::hex << current_bytes << " and cache capacity 0x" << cache_capacity << std::dec << dendl;
  return 0;
}

void LRUCache::remove(std::pair<uint64_t, uint64_t> key) {
  // Move key to front (most recently used)

  dout(20) << __func__ << " removing key from cache for inode " << key.first << std::hex << " 0x" << key.second << std::dec << dendl;
  {
    std::unique_lock lock(mutex);
    auto kv_it = kv.find(key);
    if (kv_it != kv.end()) {
      kv_it->second->clear();
      kv.erase(key);
      dout(20) << __func__ << " removed key from cache for inode " << key.first << std::hex << " 0x" << key.second << std::dec << dendl;
    }
  }

  {
    std::unique_lock lock_iter(mutex_iter);
    auto list_it = key_iter.find(key);
    if (list_it != key_iter.end()) {
      key_iter.erase(key);
      access_order.remove(key);
      dout(20) << __func__ << " removed key from LRU list " << key.first << std::hex << " 0x" << key.second << std::dec << dendl;
    }
  }
}

void LRUCache::print_cache() {
  dout(30) << __func__ << "Cache Contents (Total Bytes: 0x" << std::hex << current_bytes << std::dec << ")" << dendl;
  for (auto key : access_order) {
    auto &val = kv[key];
    dout(30) << __func__ << std::hex << " inode " << key.first << " and offset 0x" << key.second << std::dec << dendl;
  }
}

void LRUCache::update_recency(std::pair<uint64_t, uint64_t> key) {
  // Move key to front (most recently used)

  dout(20) << __func__ << " updating recency for inode " << key.first << " and offset 0x" << std::hex << key.second << std::dec << dendl;
  std::unique_lock lock(mutex_iter);
  auto it = key_iter.find(key);
  if (it != key_iter.end()) {
    access_order.erase(key_iter[key]);
  }
  access_order.push_front(key);
  key_iter[key] = access_order.begin();
}

void LRUCache::evict(size_t target_free_bytes) {
  dout(20) << __func__ << " evicting to free the cache 0x" << std::hex << target_free_bytes << std::dec << " bytes" << dendl;
  size_t freed = 0;
  std::unique_lock lockIter(mutex_iter);
  std::map<std::pair<uint64_t, uint64_t>, cache_bufferlist>::iterator it;

  while (!access_order.empty() && freed < target_free_bytes) {
    std::pair key_to_remove = access_order.back();
    dout(20) << __func__ << " evicted inode " <<  key_to_remove.first << " offset 0x" << std::hex << key_to_remove.second << std::dec << " from cache" << dendl;
    access_order.pop_back();

    it = kv.find(key_to_remove);
    freed += it->second->length();
    current_bytes -= it->second->length();

    it->second->clear();
    kv.erase(key_to_remove);
    key_iter.erase(key_to_remove);
  }

  dout(20) << __func__ << " Evicted to free the cache 0x" << std::hex << freed << std::dec << " bytes" << dendl;
}

bool LRUCache::is_empty() {
  std::shared_lock lock(mutex);
  return kv.empty();
}

cache_bufferlist LRUCache::create_bufferlist() {
  return std::make_shared<bufferlist>();
}