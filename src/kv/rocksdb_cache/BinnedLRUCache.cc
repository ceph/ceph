// Copyright (c) 2018-Present Red Hat Inc.  All rights reserved.
//
// Copyright (c) 2011-2018, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 and Apache 2.0 License
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#include "common/admin_socket.h"
#include "common/pretty_binary.h"
#include <fmt/format.h>
#endif

#include "BinnedLRUCache.h"

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include "common/debug.h"
#include "common/perf_counters_collection.h"

#define dout_context cct
#define dout_subsys ceph_subsys_rocksdb
#undef dout_prefix
#define dout_prefix *_dout << "rocksdb: "

using namespace std;
namespace rocksdb_cache {

BinnedLRUHandleTable::BinnedLRUHandleTable() : list_(nullptr), length_(0), elems_(0) {
  Resize();
}

BinnedLRUHandleTable::~BinnedLRUHandleTable() {
  ApplyToAllCacheEntries([](BinnedLRUHandle* h) {
    if (h->refs == 1) {
      h->Free();
    }
  });
  delete[] list_;
}

BinnedLRUHandle* BinnedLRUHandleTable::Lookup(const rocksdb::Slice& key, uint32_t hash) {
  return *FindPointer(key, hash);
}

BinnedLRUHandle* BinnedLRUHandleTable::Insert(BinnedLRUHandle* h) {
  BinnedLRUHandle** ptr = FindPointer(h->key(), h->hash);
  BinnedLRUHandle* old = *ptr;
  h->next_hash = (old == nullptr ? nullptr : old->next_hash);
  *ptr = h;
  if (old == nullptr) {
    ++elems_;
    if (elems_ > length_) {
      // Since each cache entry is fairly large, we aim for a small
      // average linked list length (<= 1).
      Resize();
    }
  }
  return old;
}

BinnedLRUHandle* BinnedLRUHandleTable::Remove(const rocksdb::Slice& key, uint32_t hash) {
  BinnedLRUHandle** ptr = FindPointer(key, hash);
  BinnedLRUHandle* result = *ptr;
  if (result != nullptr) {
    *ptr = result->next_hash;
    --elems_;
  }
  return result;
}

BinnedLRUHandle** BinnedLRUHandleTable::FindPointer(const rocksdb::Slice& key, uint32_t hash) {
  BinnedLRUHandle** ptr = &list_[hash & (length_ - 1)];
  while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
    ptr = &(*ptr)->next_hash;
  }
  return ptr;
}

void BinnedLRUHandleTable::Resize() {
  uint32_t new_length = 16;
  while (new_length < elems_ * 1.5) {
    new_length *= 2;
  }
  BinnedLRUHandle** new_list = new BinnedLRUHandle*[new_length];
  memset(new_list, 0, sizeof(new_list[0]) * new_length);
  uint32_t count = 0;
  for (uint32_t i = 0; i < length_; i++) {
    BinnedLRUHandle* h = list_[i];
    while (h != nullptr) {
      BinnedLRUHandle* next = h->next_hash;
      uint32_t hash = h->hash;
      BinnedLRUHandle** ptr = &new_list[hash & (new_length - 1)];
      h->next_hash = *ptr;
      *ptr = h;
      h = next;
      count++;
    }
  }
  ceph_assert(elems_ == count);
  delete[] list_;
  list_ = new_list;
  length_ = new_length;
}

BinnedLRUCacheShard::BinnedLRUCacheShard(
  BinnedLRUCache* cache,
  CephContext *c, size_t capacity, bool strict_capacity_limit,
  double high_pri_pool_ratio)
    : cache(cache),
      cct(c),
      capacity_(0),
      high_pri_pool_usage_(0),
      strict_capacity_limit_(strict_capacity_limit),
      high_pri_pool_ratio_(high_pri_pool_ratio),
      high_pri_pool_capacity_(0),
      usage_(0),
      lru_usage_(0),
      age_bins(1)
{
  shift_bins();
  // Make empty circular linked list
  lru_.next = &lru_;
  lru_.prev = &lru_;
  lru_low_pri_ = &lru_;
  SetCapacity(capacity);
  low_lru_count = 0;
  high_usage_spike = capacity;
}

BinnedLRUCacheShard::~BinnedLRUCacheShard() {}

bool BinnedLRUCacheShard::Unref(BinnedLRUHandle* e) {
  ceph_assert(e->refs > 0);
  e->refs--;
  return e->refs == 0;
}

// Call deleter and free

void BinnedLRUCacheShard::EraseUnRefEntries() {
  BinnedLRUHandle* deleted = nullptr;
  {
    std::lock_guard<std::mutex> l(mutex_);
    while (lru_.next != &lru_) {
      BinnedLRUHandle* old = lru_.next;
      ceph_assert(old->InCache());
      ceph_assert(old->refs ==
             1);  // LRU list contains elements which may be evicted
      LRU_Remove(old);
      table_.Remove(old->key(), old->hash);
      old->SetInCache(false);
      Unref(old);
      usage_ -= old->charge;
      ceph_assert(!old->next);
      old->next = deleted;
      deleted = old;
    }
  }

  FreeDeleted(deleted);
}

void BinnedLRUCacheShard::ApplyToAllCacheEntries(
  const std::function<void(const rocksdb::Slice& key,
                           void* value,
                           size_t charge,
                           DeleterFn)>& callback,
  bool thread_safe)
{
  if (thread_safe) {
    mutex_.lock();
  }
  table_.ApplyToAllCacheEntries(
    [callback](BinnedLRUHandle* h) {
      callback(h->key(), h->value, h->charge, h->deleter);
    });
  if (thread_safe) {
    mutex_.unlock();
  }
}

void BinnedLRUCacheShard::TEST_GetLRUList(BinnedLRUHandle** lru, BinnedLRUHandle** lru_low_pri) {
  *lru = &lru_;
  *lru_low_pri = lru_low_pri_;
}

size_t BinnedLRUCacheShard::TEST_GetLRUSize() {
  BinnedLRUHandle* lru_handle = lru_.next;
  size_t lru_size = 0;
  while (lru_handle != &lru_) {
    lru_size++;
    lru_handle = lru_handle->next;
  }
  return lru_size;
}

double BinnedLRUCacheShard::GetHighPriPoolRatio() const {
  std::lock_guard<std::mutex> l(mutex_);
  return high_pri_pool_ratio_;
}

size_t BinnedLRUCacheShard::GetHighPriPoolUsage() const {
  std::lock_guard<std::mutex> l(mutex_);
  return high_pri_pool_usage_;
}

void BinnedLRUCacheShard::LRU_Remove(BinnedLRUHandle* e) {
  ceph_assert(e->next != nullptr);
  ceph_assert(e->prev != nullptr);
  if (lru_low_pri_ == e) {
    lru_low_pri_ = e->prev;
  }
  e->next->prev = e->prev;
  e->prev->next = e->next;
  e->prev = e->next = nullptr;
  lru_usage_ -= e->charge;
  if (e->InHighPriPool()) {
    ceph_assert(high_pri_pool_usage_ >= e->charge);
    high_pri_pool_usage_ -= e->charge;
  } else {
    ceph_assert(*(e->age_bin) >= e->charge);
    *(e->age_bin) -= e->charge;
  }
}

void BinnedLRUCacheShard::LRU_Insert(BinnedLRUHandle* e) {
  ceph_assert(e->next == nullptr);
  ceph_assert(e->prev == nullptr);
  e->age_bin = age_bins.front();

  if (high_pri_pool_ratio_ > 0 && e->IsHighPri()) {
    // Inset "e" to head of LRU list.
    e->next = &lru_;
    e->prev = lru_.prev;
    e->prev->next = e;
    e->next->prev = e;
    e->SetInHighPriPool(true);
    high_pri_pool_usage_ += e->charge;
    MaintainPoolSize();
  } else {
    // Insert "e" to the head of low-pri pool. Note that when
    // high_pri_pool_ratio is 0, head of low-pri pool is also head of LRU list.
    e->next = lru_low_pri_->next;
    e->prev = lru_low_pri_;
    e->prev->next = e;
    e->next->prev = e;
    e->SetInHighPriPool(false);
    lru_low_pri_ = e;
    *(e->age_bin) += e->charge;
  }
  lru_usage_ += e->charge;
}

uint64_t BinnedLRUCacheShard::sum_bins(uint32_t start, uint32_t end) const {
  std::lock_guard<std::mutex> l(mutex_);
  auto size = age_bins.size();
  if (size < start) {
    return 0;
  }
  uint64_t bytes = 0;
  end = (size < end) ? size : end;
  for (auto i = start; i < end; i++) {
    bytes += *(age_bins[i]);
  }
  return bytes;
}

void BinnedLRUCacheShard::MaintainPoolSize() {
  while (high_pri_pool_usage_ > high_pri_pool_capacity_) {
    // Overflow last entry in high-pri pool to low-pri pool.
    lru_low_pri_ = lru_low_pri_->next;
    ceph_assert(lru_low_pri_ != &lru_);
    lru_low_pri_->SetInHighPriPool(false);
    high_pri_pool_usage_ -= lru_low_pri_->charge;
    *(lru_low_pri_->age_bin) += lru_low_pri_->charge;
  }
}

void BinnedLRUCacheShard::EvictFromLRU(size_t charge,
                                 BinnedLRUHandle*& deleted) {

  while (usage_ + charge > capacity_ && lru_.next != &lru_) {
    BinnedLRUHandle* old = lru_.next;
    ceph_assert(old->InCache());
    ceph_assert(old->refs == 1);  // LRU list contains elements which may be evicted
    stats[l_elems]--;
    LRU_Remove(old);
    table_.Remove(old->key(), old->hash);
    old->SetInCache(false);
    Unref(old);
    usage_ -= old->charge;
    ceph_assert(!old->next);
    old->next = deleted;
    deleted = old;
  }
}

int BinnedLRUCacheShard::FreeDeleted(BinnedLRUHandle* deleted) {
  int del = 0;
  while (deleted) {
    auto* entry = deleted;
    deleted = deleted->next;
    entry->Free();
    del++;
  }
  return del;
}

void BinnedLRUCacheShard::SetCapacity(size_t capacity) {
  BinnedLRUHandle* deleted = nullptr;
  {
    std::lock_guard<std::mutex> l(mutex_);
    capacity_ = capacity;
    high_pri_pool_capacity_ = capacity_ * high_pri_pool_ratio_;
    EvictFromLRU(0, deleted);
  }
  // we free the entries here outside of mutex for
  // performance reasons
  FreeDeleted(deleted);
}

ShardStats BinnedLRUCacheShard::GetStats() {
  std::lock_guard<std::mutex> l(mutex_);
  stats[l_capacity] = capacity_;
  stats[l_usage] = usage_;
  stats[l_pinned] = usage_ - lru_usage_;
  stats[l_misses] = stats[l_lookups] - stats[l_hits];
  return stats;
}

void BinnedLRUCacheShard::ClearStats() {
  std::lock_guard<std::mutex> l(mutex_);
  for (int i = l_inserts; i <= l_misses; i++) {
    stats[i] = 0;
  }
}

void BinnedLRUCacheShard::print_bins(std::stringstream& out) const
{
  for (const auto& i : age_bins) {
    out << *i << " ";
  }
  out << std::endl;
}

void BinnedLRUCacheShard::SetStrictCapacityLimit(bool strict_capacity_limit) {
  std::lock_guard<std::mutex> l(mutex_);
  strict_capacity_limit_ = strict_capacity_limit;
}

rocksdb::Cache::Handle* BinnedLRUCacheShard::Lookup(const rocksdb::Slice& key, uint32_t hash) {
  std::lock_guard<std::mutex> l(mutex_);
  stats[l_lookups]++;
  BinnedLRUHandle* e = table_.Lookup(key, hash);
  if (e != nullptr) {
    ceph_assert(e->InCache());
    if (e->refs == 1) {
      LRU_Remove(e);
    }
    e->refs++;
    e->SetHit();
    stats[l_hits]++;
  }
  return reinterpret_cast<rocksdb::Cache::Handle*>(e);
}

bool BinnedLRUCacheShard::Ref(rocksdb::Cache::Handle* h) {
  BinnedLRUHandle* handle = reinterpret_cast<BinnedLRUHandle*>(h);
  std::lock_guard<std::mutex> l(mutex_);
  if (handle->InCache() && handle->refs == 1) {
    LRU_Remove(handle);
  }
  handle->refs++;
  return true;
}

void BinnedLRUCacheShard::SetHighPriPoolRatio(double high_pri_pool_ratio) {
  std::lock_guard<std::mutex> l(mutex_);
  high_pri_pool_ratio_ = high_pri_pool_ratio;
  high_pri_pool_capacity_ = capacity_ * high_pri_pool_ratio_;
  MaintainPoolSize();
}

bool BinnedLRUCacheShard::Release(rocksdb::Cache::Handle* handle, bool force_erase) {
  if (handle == nullptr) {
    return false;
  }
  BinnedLRUHandle* e = reinterpret_cast<BinnedLRUHandle*>(handle);
  bool last_reference = false;
  {
    std::lock_guard<std::mutex> l(mutex_);
    last_reference = Unref(e);
    if (last_reference) {
      usage_ -= e->charge;
      stats[l_elems]--;
    }
    if (e->refs == 1 && e->InCache()) {
      // The item is still in cache, and nobody else holds a reference to it
      if (usage_ > capacity_ || force_erase) {
        // the cache is full
        // The LRU list must be empty since the cache is full
        ceph_assert(!(usage_ > capacity_) || lru_.next == &lru_);
        // take this opportunity and remove the item
        table_.Remove(e->key(), e->hash);
        e->SetInCache(false);
        Unref(e);
        usage_ -= e->charge;
        last_reference = true;
        stats[l_elems]--;
      } else {
        // put the item on the list to be potentially freed
        LRU_Insert(e);
      }
    }
  }

  // free outside of mutex
  if (last_reference) {
    e->Free();
  }
  return last_reference;
}

rocksdb::Status BinnedLRUCacheShard::Insert(const rocksdb::Slice& key, uint32_t hash, void* value,
                             size_t charge,
                             DeleterFn deleter,
                             rocksdb::Cache::Handle** handle, rocksdb::Cache::Priority priority) {
  auto e = new BinnedLRUHandle();
  rocksdb::Status s;
  BinnedLRUHandle* deleted = nullptr;

  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->key_data = new char[e->key_length];
  e->flags = 0;
  e->hash = hash;
  e->refs = (handle == nullptr
                 ? 1
                 : 2);  // One from BinnedLRUCache, one for the returned handle
  e->next = e->prev = nullptr;
  e->SetInCache(true);
  e->SetPriority(priority);
  std::copy_n(key.data(), e->key_length, e->key_data);
  bool need_rebalancing = false;
  {
    std::lock_guard<std::mutex> l(mutex_);
    stats[l_elems]++;
    stats[l_inserts]++;
    // Free the space following strict LRU policy until enough space
    // is freed or the lru list is empty
    {
      // insert into the cache
      // note that the cache might get larger than its capacity if not enough
      // space was freed
      BinnedLRUHandle* old = table_.Insert(e);
      usage_ += e->charge;
      if (old != nullptr) {
        old->SetInCache(false);
        if (Unref(old)) {
          usage_ -= old->charge;
          // old is on LRU because it's in cache and its reference count
          // was just 1 (Unref returned 0)
          LRU_Remove(old);
          ceph_assert(!old->next);
          old->next = deleted;
          deleted = old;
        }
      }
      if (handle == nullptr) {
        LRU_Insert(e);
      } else {
        *handle = reinterpret_cast<rocksdb::Cache::Handle*>(e);
      }
      s = rocksdb::Status::OK();
    }
    //if usage spiked suddenly or lru elements went below warn level, rebalance
    need_rebalancing = cache->rebalance_enabled && (
      (usage_ > high_usage_spike) ||
      ((usage_ > capacity_) && (stats[l_elems] < low_lru_count)) );
    if (!need_rebalancing) {
      EvictFromLRU(0, deleted);
    } else {
      if (capacity_ < usage_) {
        // will fix capacity with rebalancing but we need to make usage <= capacity for now
        capacity_ = usage_;
      }
    }
  }
  if (need_rebalancing) {
    // rebalance must be called without shard lock
    cache->rebalance();
  }
  // we free the entries here outside of mutex for
  // performance reasons
  FreeDeleted(deleted);

  return s;
}

void BinnedLRUCacheShard::Erase(const rocksdb::Slice& key, uint32_t hash) {
  BinnedLRUHandle* e;
  bool last_reference = false;
  {
    std::lock_guard<std::mutex> l(mutex_);
    stats[l_elems]--;
    e = table_.Remove(key, hash);
    if (e != nullptr) {
      last_reference = Unref(e);
      if (last_reference) {
        usage_ -= e->charge;
      }
      if (last_reference && e->InCache()) {
        LRU_Remove(e);
      }
      e->SetInCache(false);
    }
  }

  // mutex not held here
  // last_reference will only be true if e != nullptr
  if (last_reference) {
    e->Free();
  }
}

size_t BinnedLRUCacheShard::GetUsage() const {
  std::lock_guard<std::mutex> l(mutex_);
  return usage_;
}

size_t BinnedLRUCacheShard::GetPinnedUsage() const {
  std::lock_guard<std::mutex> l(mutex_);
  ceph_assert(usage_ >= lru_usage_);
  return usage_ - lru_usage_;
}

void BinnedLRUCacheShard::shift_bins() {
  std::lock_guard<std::mutex> l(mutex_);
  age_bins.push_front(std::make_shared<uint64_t>(0));
}

uint32_t BinnedLRUCacheShard::get_bin_count() const {
  std::lock_guard<std::mutex> l(mutex_);
  return age_bins.capacity();
}

void BinnedLRUCacheShard::set_bin_count(uint32_t count) {
  std::lock_guard<std::mutex> l(mutex_);
  age_bins.set_capacity(count);
}

std::string BinnedLRUCacheShard::GetPrintableOptions() const {
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  {
    std::lock_guard<std::mutex> l(mutex_);
    snprintf(buffer, kBufferSize, "    high_pri_pool_ratio: %.3lf\n",
             high_pri_pool_ratio_);
  }
  return std::string(buffer);
}

DeleterFn BinnedLRUCacheShard::GetDeleter(rocksdb::Cache::Handle* h) const
{
  auto* handle = reinterpret_cast<BinnedLRUHandle*>(h);
  return handle->deleter;
}

#undef dout_context
#define dout_context cache.cct

class BinnedLRUCache::SocketHook : public AdminSocketHook {
  BinnedLRUCache& cache;

public:
  SocketHook( BinnedLRUCache& _cache)
  : cache(_cache)
  {
    AdminSocket *admin_socket = cache.cct->get_admin_socket();
    if (admin_socket) {
      int r = admin_socket->register_command(
        string("rocksdb show cache ") + cache.name + string(" name=shard_no,type=CephInt,req=false"),
        this, "show details of cache " + cache.name);
      if (r != 0) {
        dout(1) << __func__ << " cannot register SocketHook" << dendl;
        return;
      }
      r = admin_socket->register_command(
        string("rocksdb reset cache ") + cache.name,
        this, "clear stats of cache " + cache.name);
      ceph_assert(r == 0);
    }
  };
  ~SocketHook() {
    AdminSocket *admin_socket = cache.cct->get_admin_socket();
    if (admin_socket) {
      admin_socket->unregister_commands(this);
    }
  };
  int call(std::string_view command,
           const cmdmap_t& cmdmap,
           const bufferlist& inbl,
           Formatter *f,
           std::ostream& ss,
           bufferlist& out)
  {
    int r = 0;
    if (command == string("rocksdb show cache ") + cache.name) {
      int64_t shard_no;
      stringstream outstr;
      if (!ceph::common::cmd_getval(cmdmap, "shard_no", shard_no)) {
        outstr << fmt::format("{:>5}", "shard");
        for (int j = 0; j < stat_cnt; j++) {
          outstr << fmt::format("{:>10}", ShardStats::stat_name[j]);
        }
        outstr << std::endl;
        for (int i = 0; i < cache.num_shards_; i++) {
          outstr << fmt::format("{:>5}", i);
          ShardStats s = cache.shards_[i].GetStats();
          for (int j = 0; j < stat_cnt; j++) {
            outstr << fmt::format("{:>10}", s[j]);
          }
          outstr << std::endl;
        }
      } else {
        cache.printshard(shard_no, outstr);
      }
      out.append(outstr.str());
    } else if(command == string("rocksdb reset cache ") + cache.name) {
      for (int i = 0; i < cache.num_shards_; i++) {
        cache.shards_[i].ClearStats();
      }
    } else {
     ss << "Invalid command" << std::endl;
      r = -ENOSYS;
    }
    return r;
  };
};

#undef dout_context
#define dout_context cct

BinnedLRUCache::BinnedLRUCache(
  CephContext *c,
  const string& name,
  size_t capacity,
  int num_shard_bits,
  bool strict_capacity_limit,
  double high_pri_pool_ratio)
  : ShardedCache(capacity, num_shard_bits, strict_capacity_limit)
  , cct(c)
  , name(name)
{
  num_shards_ = 1 << num_shard_bits;
  // TODO: Switch over to use mempool
  int rc = posix_memalign((void**) &shards_, 
                          CACHE_LINE_SIZE, 
                          sizeof(BinnedLRUCacheShard) * num_shards_);
  if (rc != 0) {
    throw std::bad_alloc();
  } 
  size_t per_shard = (capacity + (num_shards_ - 1)) / num_shards_;
  for (int i = 0; i < num_shards_; i++) {
    new (&shards_[i])
        BinnedLRUCacheShard(this, c, per_shard, strict_capacity_limit, high_pri_pool_ratio);
  }
  rebalance_enabled = cct->_conf->rocksdb_cache_rebalance;
  SetupPerfCounters();
  asok_hook = new SocketHook(*this);
}

void BinnedLRUCache::SetupPerfCounters()
{
  int l_first = 0;
  int l_last = l_first + 1 + stat_cnt;
  if (rebalance_enabled) {
    l_last += 2;
  }
  PerfCountersBuilder b(cct, string("rocksdb-cache-") + name, l_first, l_last);
  for (uint32_t j = l_capacity; j <= l_misses; j++) {
    b.add_u64(1 + j, ShardStats::stat_name[j], ShardStats::stat_descr[j]);
  }
  if (rebalance_enabled) {
    b.add_time_avg(l_rebalance, "rebalance", "execution time for shard rebalance");
    b.add_time_avg(l_rebalance_free, "rebalance_free", "execution time for eviction of items that go over capacity");
  }
  perfstats = b.create_perf_counters();
  cct->get_perfcounters_collection()->add(perfstats);
}

BinnedLRUCache::~BinnedLRUCache() {
  for (int i = 0; i < num_shards_; i++) {
    shards_[i].~BinnedLRUCacheShard();
  }
  aligned_free(shards_);
  cct->get_perfcounters_collection()->remove(perfstats);
  delete perfstats;
  perfstats = nullptr;
  delete asok_hook;
  asok_hook = nullptr;
}

CacheShard* BinnedLRUCache::GetShard(int shard) {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

const CacheShard* BinnedLRUCache::GetShard(int shard) const {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

void* BinnedLRUCache::Value(Handle* handle) {
  return reinterpret_cast<const BinnedLRUHandle*>(handle)->value;
}

size_t BinnedLRUCache::GetCharge(Handle* handle) const {
  return reinterpret_cast<const BinnedLRUHandle*>(handle)->charge;
}

uint32_t BinnedLRUCache::GetHash(Handle* handle) const {
  return reinterpret_cast<const BinnedLRUHandle*>(handle)->hash;
}

void BinnedLRUCache::DisownData() {
// Do not drop data if compile with ASAN to suppress leak warning.
#ifndef __SANITIZE_ADDRESS__
  shards_ = nullptr;
#endif  // !__SANITIZE_ADDRESS__
}

#if (ROCKSDB_MAJOR >= 7 || (ROCKSDB_MAJOR == 6 && ROCKSDB_MINOR >= 22))
DeleterFn BinnedLRUCache::GetDeleter(Handle* handle) const
{
  return reinterpret_cast<const BinnedLRUHandle*>(handle)->deleter;
}
#endif

size_t BinnedLRUCache::TEST_GetLRUSize() {
  size_t lru_size_of_all_shards = 0;
  for (int i = 0; i < num_shards_; i++) {
    lru_size_of_all_shards += shards_[i].TEST_GetLRUSize();
  }
  return lru_size_of_all_shards;
}

void BinnedLRUCache::SetHighPriPoolRatio(double high_pri_pool_ratio) {
  for (int i = 0; i < num_shards_; i++) {
    shards_[i].SetHighPriPoolRatio(high_pri_pool_ratio);
  }
}

double BinnedLRUCache::GetHighPriPoolRatio() const {
  double result = 0.0;
  if (num_shards_ > 0) {
    result = shards_[0].GetHighPriPoolRatio();
  }
  return result;
}

size_t BinnedLRUCache::GetHighPriPoolUsage() const {
  // We will not lock the cache when getting the usage from shards.
  size_t usage = 0;
  for (int s = 0; s < num_shards_; s++) {
    usage += shards_[s].GetHighPriPoolUsage();
  }
  return usage;
}

void BinnedLRUCache::rebalance(size_t target_capacity)
{
  std::lock_guard l(capacity_mutex_);
  auto r_start = mono_clock::now();
  // lock all, as it becomes possible
  std::vector<bool> locked(num_shards_, false);
  bool all_locked = true;
  bool wait_on_first_unlocked = false;
  do {
    all_locked = true;
    for (int s = 0; s < num_shards_; s++) {
      if (!locked[s]) {
        if (shards_[s].mutex_.try_lock()) {
          locked[s] = true;
        } else {
          all_locked = false;
          if (wait_on_first_unlocked) {
            shards_[s].mutex_.lock();
            locked[s] = true;
            wait_on_first_unlocked = false;
          }
        }
      }
    }
    wait_on_first_unlocked = true;
  } while (!all_locked);

  size_t current_usage = 0;
  size_t current_items = 0;
  if (target_capacity == 0) {
    target_capacity = capacity_;
  } else {
    ceph_assert(target_capacity > 0);
    capacity_ = target_capacity;
  }

  std::vector<BinnedLRUCacheShard*> ordered(num_shards_, nullptr);
  for (int s = 0; s < num_shards_; s++) {
    auto& shard = shards_[s];
    shard.reb_ctx.new_capacity = shard.usage_;
    current_usage += shard.usage_;
    current_items += shard.stats[l_elems];
    shard.reb_ctx.new_item_count = shard.stats[l_elems];
    shard.reb_ctx.lru_end = shard.lru_.next;
  }
  size_t avg_item = current_items > 10 ? current_usage / current_items : 4200;

  uint64_t max_steps = cct->_conf->rocksdb_cache_rebalance_max_steps;
  if (max_steps == 0) {
    // fast algorithm
    // bonus can be negative !
    ssize_t bonus = ssize_t(target_capacity - current_usage) / (ssize_t)avg_item;
    ssize_t average_count = (ssize_t(current_items) + bonus) / num_shards_;
    ceph_assert(average_count >= 0);

    dout(10) << "current_usage=" << current_usage
      << " target_capacity=" << target_capacity
      << " avg.item=" << avg_item
      << " bonus items=" << bonus
      << " average_count=" << average_count << dendl;

    for (int s = 0; s < num_shards_; s++) {
      auto& shard = shards_[s];
      ssize_t more_items = average_count - shard.reb_ctx.new_item_count;
      shard.reb_ctx.new_capacity += more_items * avg_item;
      shard.reb_ctx.new_item_count += more_items;
    }
  } else {
    // more precise algorithm
    dout(10) << "current_usage=" << current_usage
      << " target_capacity=" << target_capacity
      << " avg.item=" << avg_item << dendl;

    std::vector<int> shard_order(num_shards_);
    for (int s = 0; s < num_shards_; s++) {
      shard_order[s] = s;
    }
    std::sort(shard_order.begin(), shard_order.end(), [&](int a, int b) {
      return shards_[a].stats[l_elems] > shards_[b].stats[l_elems];
    });

    // select shard X that has most elements
    // select shard Y that has least elements
    // repeat:
    // update X:
    //   element count by -1
    //   pop element from lru end (that is, if possible)
    //   reduce X's capacity, reduce total capacity
    //   elect new X
    // if total capacity is below target capacity (less by almost_median)
    //   if X has +-1 the same count elements as Y, STOP
    //   update Y:
    //     element count by +1
    //     add capacity almost_median
    //     and +1 elem
    //     elect new Y
    int last_shard = num_shards_ - 1;
    int Xidx = 0;
    int Yidx = last_shard;
    uint32_t iter;
    for (iter = 0; iter < max_steps; iter++) {
      auto &X = shards_[shard_order[Xidx]];
      auto &Y = shards_[shard_order[Yidx]];
      if (Y.reb_ctx.new_item_count + 1 >= X.reb_ctx.new_item_count) {
        // almost equal +/-1
        break;
      }
      if (current_usage + avg_item <= target_capacity) {
        Y.reb_ctx.new_item_count++;
        Y.reb_ctx.new_capacity += avg_item;
        current_usage += avg_item;
        // elect new Y
        ceph_assert(Yidx >= 1);
        if (Y.reb_ctx.new_item_count >
            shards_[shard_order[Yidx - 1]].reb_ctx.new_item_count) {
          Yidx--;
        } else {
          Yidx = 0;
        }
      } else {
        if (X.reb_ctx.new_item_count == 0)
          break; // sanity break
        X.reb_ctx.new_item_count--;
        if (X.reb_ctx.lru_end != &X.lru_) {
          // some real element to pick up from lru end
          X.reb_ctx.new_capacity -= X.reb_ctx.lru_end->charge;
          current_usage -= X.reb_ctx.lru_end->charge;
          X.reb_ctx.lru_end = X.reb_ctx.lru_end->next;
        } else {
          // lru completely exhaused, pretend we pop something
          size_t v = X.reb_ctx.new_capacity / X.reb_ctx.new_item_count;
          X.reb_ctx.new_capacity -= v;
          current_usage -= v;
        }
        // elect new X
        if (Xidx == last_shard) {
          Xidx = 0;
        } else {
          if (X.reb_ctx.new_item_count <
              shards_[shard_order[Xidx + 1]].reb_ctx.new_item_count) {
            Xidx++;
          } else {
            Xidx = 0;
          }
        }
      }
    }
    if (iter == max_steps) {
      dout(5) << "Stopped after " << iter << " steps." << dendl;
    }
    current_usage = 0;
    for (int s = 0; s < num_shards_; s++) {
      auto &shard = shards_[s];
      current_usage += shard.reb_ctx.new_capacity;
    }
    size_t extra_per_shard = 0;
    if (current_usage < target_capacity) {
      extra_per_shard = (target_capacity - current_usage) / num_shards_;
      for (int s = 0; s < num_shards_; s++) {
        auto &shard = shards_[s];
        shard.reb_ctx.new_capacity += extra_per_shard;
      }
    }
    dout(10) << "planned_usage= " << current_usage
             << "target_capacity=" << target_capacity
             << "extra=" << extra_per_shard << dendl;
  }
  {
    dout(10);
    for (int s = 0; s < num_shards_; s++) {
      auto& shard = shards_[s];
      *_dout << std::endl << "shard " << s
        << ": capacity=" << shard.capacity_
        << "->" << shard.reb_ctx.new_capacity
        << " elems=" << shard.stats[l_elems]
        << "->" << shard.reb_ctx.new_item_count;
    }
    *_dout << dendl;
  }
  for (int s = 0; s < num_shards_; s++) {
    auto& shard = shards_[s];
    shard.capacity_ = shard.reb_ctx.new_capacity;
    shard.high_usage_spike = shard.capacity_ * cct->_conf->rocksdb_cache_rebalance_capacity;
    shard.low_lru_count = shard.reb_ctx.new_item_count * cct->_conf->rocksdb_cache_rebalance_items;
    shard.reb_ctx.lru_end = nullptr;
    shard.EvictFromLRU(0, shard.reb_ctx.lru_end);
    shard.mutex_.unlock();
  }
  auto r_before_free = mono_clock::now();

  for (int s = 0; s < num_shards_; s++) {
    auto& shard = shards_[s];
    shard.FreeDeleted(shard.reb_ctx.lru_end);
  }
  auto r_finish = mono_clock::now();
  double elapsed = ceph::to_seconds<double>(r_finish - r_start);
  dout(5) << "rebalance " << name << " took " << elapsed << "s" << dendl;

  perfstats->tinc(l_rebalance_free, r_finish - r_before_free);
  perfstats->tinc(l_rebalance, r_finish - r_start);
}
// PriCache

int64_t BinnedLRUCache::request_cache_bytes(PriorityCache::Priority pri, uint64_t total_cache) const
{
  int64_t assigned = get_cache_bytes(pri);
  int64_t request = 0;

  switch(pri) {
  // PRI0 is for rocksdb's high priority items (indexes/filters)
  case PriorityCache::Priority::PRI0:
    {
      // Because we want the high pri cache to grow independently of the low
      // pri cache, request a chunky allocation independent of the other
      // priorities.
      request = PriorityCache::get_chunk(GetHighPriPoolUsage(), total_cache);
      break;
    }
  case PriorityCache::Priority::LAST:
    {
      auto max = get_bin_count();
      request = GetUsage();
      request -= GetHighPriPoolUsage();
      request -= sum_bins(0, max);
      break;
    }
  default:
    {
      ceph_assert(pri > 0 && pri < PriorityCache::Priority::LAST);
      auto prev_pri = static_cast<PriorityCache::Priority>(pri - 1);
      uint64_t start = get_bins(prev_pri);
      uint64_t end = get_bins(pri);
      request = sum_bins(start, end);
      break;
    }
  }
  request = (request > assigned) ? request - assigned : 0;
  ldout(cct, 10) << __func__ << " Priority: " << static_cast<uint32_t>(pri)
                 << " Request: " << request << dendl;
  return request;
}

int64_t BinnedLRUCache::commit_cache_size(uint64_t total_bytes)
{
  size_t old_bytes = GetCapacity();
  int64_t new_bytes = PriorityCache::get_chunk(
      get_cache_bytes(), total_bytes);
  ldout(cct, 10) << __func__ << " old: " << old_bytes
                 << " new: " << new_bytes << dendl;

  if (rebalance_enabled) {
    rebalance(new_bytes);
  } else {
    // evenly split capacity between shards
    SetCapacity((size_t) new_bytes);

  }

  double ratio = 0;
  if (new_bytes > 0) {
    int64_t pri0_bytes = get_cache_bytes(PriorityCache::Priority::PRI0);
    ratio = (double) pri0_bytes / new_bytes;
  }
  ldout(cct, 5) << __func__ << " High Pri Pool Ratio set to " << ratio << dendl;
  SetHighPriPoolRatio(ratio);

  // not related to cache size, but calledd periodically
  UpdatePerfCounters();
  return new_bytes;
}

void BinnedLRUCache::UpdatePerfCounters() {
  ShardStats stats;
  for (int i = 0; i < num_shards_; i++) {
    ShardStats s = shards_[i].GetStats();
    stats.add(s);
  }
  //set these
  for (int j = l_capacity ; j <= l_elems; j++) {
    perfstats->set(1 + j, stats[j]);
  }
  //increment these, so one can reset perf counters
  ShardStats tmp = stats;
  tmp.sub(prev_stats);
  for (int j = l_inserts ; j <= l_misses; j++) {
    perfstats->inc(1 + j, tmp[j]);
  }
  prev_stats = stats;
}

void BinnedLRUCache::printshard(int shard_no, std::stringstream& out) {
  if (shard_no < num_shards_) {
    shards_[shard_no].print_bins(out);
  }
}

void BinnedLRUCache::shift_bins() {
  for (int s = 0; s < num_shards_; s++) {
    shards_[s].shift_bins();
  }
}

uint64_t BinnedLRUCache::sum_bins(uint32_t start, uint32_t end) const {
  uint64_t bytes = 0;
  for (int s = 0; s < num_shards_; s++) {
    bytes += shards_[s].sum_bins(start, end);
  }
  return bytes;
}

uint32_t BinnedLRUCache::get_bin_count() const {
  uint32_t result = 0;
  if (num_shards_ > 0) {
    result = shards_[0].get_bin_count();
  }
  return result;
}

void BinnedLRUCache::set_bin_count(uint32_t count) {
  for (int s = 0; s < num_shards_; s++) {
    shards_[s].set_bin_count(count);
  }
}

std::shared_ptr<rocksdb::Cache> NewBinnedLRUCache(
    CephContext *c,
    const string& name,
    size_t capacity,
    int num_shard_bits,
    bool strict_capacity_limit,
    double high_pri_pool_ratio) {
  if (num_shard_bits >= 20) {
    return nullptr;  // the cache cannot be sharded into too many fine pieces
  }
  if (high_pri_pool_ratio < 0.0 || high_pri_pool_ratio > 1.0) {
    // invalid high_pri_pool_ratio
    return nullptr;
  }
  if (num_shard_bits < 0) {
    num_shard_bits = GetDefaultCacheShardBits(capacity);
  }
  return std::make_shared<BinnedLRUCache>(
      c, name, capacity, num_shard_bits, strict_capacity_limit, high_pri_pool_ratio);
}

}  // namespace rocksdb_cache
