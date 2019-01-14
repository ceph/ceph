// Copyright (c) 2018-Present Red Hat Inc.  All rights reserved.
//
// Copyright (c) 2011-2018, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 and Apache 2.0 License
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_BINNED_LRU_CACHE
#define ROCKSDB_BINNED_LRU_CACHE

#include <string>
#include <mutex>

#include "ShardedCache.h"
#include "common/autovector.h"
#include "common/dout.h"
#include "include/ceph_assert.h"
#include "common/ceph_context.h"

namespace rocksdb_cache {

// LRU cache implementation

// An entry is a variable length heap-allocated structure.
// Entries are referenced by cache and/or by any external entity.
// The cache keeps all its entries in table. Some elements
// are also stored on LRU list.
//
// BinnedLRUHandle can be in these states:
// 1. Referenced externally AND in hash table.
//  In that case the entry is *not* in the LRU. (refs > 1 && in_cache == true)
// 2. Not referenced externally and in hash table. In that case the entry is
// in the LRU and can be freed. (refs == 1 && in_cache == true)
// 3. Referenced externally and not in hash table. In that case the entry is
// in not on LRU and not in table. (refs >= 1 && in_cache == false)
//
// All newly created BinnedLRUHandles are in state 1. If you call
// BinnedLRUCacheShard::Release
// on entry in state 1, it will go into state 2. To move from state 1 to
// state 3, either call BinnedLRUCacheShard::Erase or BinnedLRUCacheShard::Insert with the
// same key.
// To move from state 2 to state 1, use BinnedLRUCacheShard::Lookup.
// Before destruction, make sure that no handles are in state 1. This means
// that any successful BinnedLRUCacheShard::Lookup/BinnedLRUCacheShard::Insert have a
// matching
// RUCache::Release (to move into state 2) or BinnedLRUCacheShard::Erase (for state 3)

std::shared_ptr<rocksdb::Cache> NewBinnedLRUCache(
    CephContext *c,
    size_t capacity,
    int num_shard_bits = -1,
    bool strict_capacity_limit = false,
    double high_pri_pool_ratio = 0.0);

struct BinnedLRUHandle {
  void* value;
  void (*deleter)(const rocksdb::Slice&, void* value);
  BinnedLRUHandle* next_hash;
  BinnedLRUHandle* next;
  BinnedLRUHandle* prev;
  size_t charge;  // TODO(opt): Only allow uint32_t?
  size_t key_length;
  uint32_t refs;     // a number of refs to this entry
                     // cache itself is counted as 1

  // Include the following flags:
  //   in_cache:    whether this entry is referenced by the hash table.
  //   is_high_pri: whether this entry is high priority entry.
  //   in_high_pri_pool: whether this entry is in high-pri pool.
  char flags;

  uint32_t hash;     // Hash of key(); used for fast sharding and comparisons

  char* key_data = nullptr;  // Beginning of key

  rocksdb::Slice key() const {
    // For cheaper lookups, we allow a temporary Handle object
    // to store a pointer to a key in "value".
    if (next == this) {
      return *(reinterpret_cast<rocksdb::Slice*>(value));
    } else {
      return rocksdb::Slice(key_data, key_length);
    }
  }

  bool InCache() { return flags & 1; }
  bool IsHighPri() { return flags & 2; }
  bool InHighPriPool() { return flags & 4; }
  bool HasHit() { return flags & 8; }

  void SetInCache(bool in_cache) {
    if (in_cache) {
      flags |= 1;
    } else {
      flags &= ~1;
    }
  }

  void SetPriority(rocksdb::Cache::Priority priority) {
    if (priority == rocksdb::Cache::Priority::HIGH) {
      flags |= 2;
    } else {
      flags &= ~2;
    }
  }

  void SetInHighPriPool(bool in_high_pri_pool) {
    if (in_high_pri_pool) {
      flags |= 4;
    } else {
      flags &= ~4;
    }
  }

  void SetHit() { flags |= 8; }

  void Free() {
    ceph_assert((refs == 1 && InCache()) || (refs == 0 && !InCache()));
    if (deleter) {
      (*deleter)(key(), value);
    }
    delete[] key_data;
    delete this;
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class BinnedLRUHandleTable {
 public:
  BinnedLRUHandleTable();
  ~BinnedLRUHandleTable();

  BinnedLRUHandle* Lookup(const rocksdb::Slice& key, uint32_t hash);
  BinnedLRUHandle* Insert(BinnedLRUHandle* h);
  BinnedLRUHandle* Remove(const rocksdb::Slice& key, uint32_t hash);

  template <typename T>
  void ApplyToAllCacheEntries(T func) {
    for (uint32_t i = 0; i < length_; i++) {
      BinnedLRUHandle* h = list_[i];
      while (h != nullptr) {
        auto n = h->next_hash;
        ceph_assert(h->InCache());
        func(h);
        h = n;
      }
    }
  }

 private:
  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  BinnedLRUHandle** FindPointer(const rocksdb::Slice& key, uint32_t hash);

  void Resize();

  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  BinnedLRUHandle** list_;
  uint32_t length_;
  uint32_t elems_;
};

// A single shard of sharded cache.
class alignas(CACHE_LINE_SIZE) BinnedLRUCacheShard : public CacheShard {
 public:
  BinnedLRUCacheShard(size_t capacity, bool strict_capacity_limit,
                double high_pri_pool_ratio);
  virtual ~BinnedLRUCacheShard();

  // Separate from constructor so caller can easily make an array of BinnedLRUCache
  // if current usage is more than new capacity, the function will attempt to
  // free the needed space
  virtual void SetCapacity(size_t capacity) override;

  // Set the flag to reject insertion if cache if full.
  virtual void SetStrictCapacityLimit(bool strict_capacity_limit) override;

  // Set percentage of capacity reserved for high-pri cache entries.
  void SetHighPriPoolRatio(double high_pri_pool_ratio);

  // Like Cache methods, but with an extra "hash" parameter.
  virtual rocksdb::Status Insert(const rocksdb::Slice& key, uint32_t hash, void* value,
                        size_t charge,
                        void (*deleter)(const rocksdb::Slice& key, void* value),
                        rocksdb::Cache::Handle** handle,
                        rocksdb::Cache::Priority priority) override;
  virtual rocksdb::Cache::Handle* Lookup(const rocksdb::Slice& key, uint32_t hash) override;
  virtual bool Ref(rocksdb::Cache::Handle* handle) override;
  virtual bool Release(rocksdb::Cache::Handle* handle,
                       bool force_erase = false) override;
  virtual void Erase(const rocksdb::Slice& key, uint32_t hash) override;

  // Although in some platforms the update of size_t is atomic, to make sure
  // GetUsage() and GetPinnedUsage() work correctly under any platform, we'll
  // protect them with mutex_.

  virtual size_t GetUsage() const override;
  virtual size_t GetPinnedUsage() const override;

  virtual void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                      bool thread_safe) override;

  virtual void EraseUnRefEntries() override;

  virtual std::string GetPrintableOptions() const override;

  void TEST_GetLRUList(BinnedLRUHandle** lru, BinnedLRUHandle** lru_low_pri);

  //  Retrieves number of elements in LRU, for unit test purpose only
  //  not threadsafe
  size_t TEST_GetLRUSize();

  //  Retrieves high pri pool ratio
  double GetHighPriPoolRatio() const;

  // Retrieves high pri pool usage
  size_t GetHighPriPoolUsage() const;

 private:
  void LRU_Remove(BinnedLRUHandle* e);
  void LRU_Insert(BinnedLRUHandle* e);

  // Overflow the last entry in high-pri pool to low-pri pool until size of
  // high-pri pool is no larger than the size specify by high_pri_pool_pct.
  void MaintainPoolSize();

  // Just reduce the reference count by 1.
  // Return true if last reference
  bool Unref(BinnedLRUHandle* e);

  // Free some space following strict LRU policy until enough space
  // to hold (usage_ + charge) is freed or the lru list is empty
  // This function is not thread safe - it needs to be executed while
  // holding the mutex_
  void EvictFromLRU(size_t charge, ceph::autovector<BinnedLRUHandle*>* deleted);

  // Initialized before use.
  size_t capacity_;

  // Memory size for entries in high-pri pool.
  size_t high_pri_pool_usage_;

  // Whether to reject insertion if cache reaches its full capacity.
  bool strict_capacity_limit_;

  // Ratio of capacity reserved for high priority cache entries.
  double high_pri_pool_ratio_;

  // High-pri pool size, equals to capacity * high_pri_pool_ratio.
  // Remember the value to avoid recomputing each time.
  double high_pri_pool_capacity_;

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // LRU contains items which can be evicted, ie reference only by cache
  BinnedLRUHandle lru_;

  // Pointer to head of low-pri pool in LRU list.
  BinnedLRUHandle* lru_low_pri_;

  // ------------^^^^^^^^^^^^^-----------
  // Not frequently modified data members
  // ------------------------------------
  //
  // We separate data members that are updated frequently from the ones that
  // are not frequently updated so that they don't share the same cache line
  // which will lead into false cache sharing
  //
  // ------------------------------------
  // Frequently modified data members
  // ------------vvvvvvvvvvvvv-----------
  BinnedLRUHandleTable table_;

  // Memory size for entries residing in the cache
  size_t usage_;

  // Memory size for entries residing only in the LRU list
  size_t lru_usage_;

  // mutex_ protects the following state.
  // We don't count mutex_ as the cache's internal state so semantically we
  // don't mind mutex_ invoking the non-const actions.
  mutable std::mutex mutex_;
};

class BinnedLRUCache : public ShardedCache {
 public:
  BinnedLRUCache(CephContext *c, size_t capacity, int num_shard_bits,
      bool strict_capacity_limit, double high_pri_pool_ratio);
  virtual ~BinnedLRUCache();
  virtual const char* Name() const override { return "BinnedLRUCache"; }
  virtual CacheShard* GetShard(int shard) override;
  virtual const CacheShard* GetShard(int shard) const override;
  virtual void* Value(Handle* handle) override;
  virtual size_t GetCharge(Handle* handle) const override;
  virtual uint32_t GetHash(Handle* handle) const override;
  virtual void DisownData() override;

  //  Retrieves number of elements in LRU, for unit test purpose only
  size_t TEST_GetLRUSize();
  // Sets the high pri pool ratio
  void SetHighPriPoolRatio(double high_pri_pool_ratio);
  //  Retrieves high pri pool ratio
  double GetHighPriPoolRatio() const;
  // Retrieves high pri pool usage
  size_t GetHighPriPoolUsage() const;

  // PriorityCache
  virtual int64_t request_cache_bytes(
      PriorityCache::Priority pri, uint64_t total_cache) const;
  virtual int64_t commit_cache_size(uint64_t total_cache);
  virtual int64_t get_committed_size() const {
    return GetCapacity();
  }
  virtual std::string get_cache_name() const {
    return "RocksDB Binned LRU Cache";
  }

 private:
  CephContext *cct;
  BinnedLRUCacheShard* shards_;
  int num_shards_ = 0;
};

}  // namespace rocksdb_cache

#endif // ROCKSDB_BINNED_LRU_CACHE
