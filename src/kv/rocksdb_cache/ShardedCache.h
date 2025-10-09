// Copyright (c) 2018-Present Red Hat Inc.  All rights reserved.
//
// Copyright (c) 2011-2018, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 and Apache 2.0 License
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_SHARDED_CACHE
#define ROCKSDB_SHARDED_CACHE

#include <atomic>
#include <string>
#include <mutex>

#include "rocksdb/version.h"
#include "rocksdb/cache.h"
#include "include/ceph_hash.h"
#include "common/PriorityCache.h"
//#include "hash.h"

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64 // XXX arch-specific define 
#endif

namespace rocksdb_cache {

using DeleterFn = void (*)(const rocksdb::Slice& key, void* value);

// Single cache shard interface.
class CacheShard {
 public:
  CacheShard() = default;
  virtual ~CacheShard() = default;

  virtual rocksdb::Status Insert(const rocksdb::Slice& key, uint32_t hash, void* value,
                                 size_t charge,
                                 DeleterFn deleter,
                                 rocksdb::Cache::Handle** handle, rocksdb::Cache::Priority priority) = 0;
  virtual rocksdb::Cache::Handle* Lookup(const rocksdb::Slice& key, uint32_t hash) = 0;
  virtual bool Ref(rocksdb::Cache::Handle* handle) = 0;
  virtual bool Release(rocksdb::Cache::Handle* handle, bool force_erase = false) = 0;
  virtual void Erase(const rocksdb::Slice& key, uint32_t hash) = 0;
  virtual void SetCapacity(size_t capacity) = 0;
  virtual void SetStrictCapacityLimit(bool strict_capacity_limit) = 0;
  virtual size_t GetUsage() const = 0;
  virtual size_t GetPinnedUsage() const = 0;
  virtual void ApplyToAllCacheEntries(
    const std::function<void(const rocksdb::Slice& key,
                             void* value,
                             size_t charge,
                             DeleterFn)>& callback,
    bool thread_safe) = 0;
  virtual void EraseUnRefEntries() = 0;
  virtual std::string GetPrintableOptions() const { return ""; }
  virtual DeleterFn GetDeleter(rocksdb::Cache::Handle* handle) const = 0;
};

// Generic cache interface which shards cache by hash of keys. 2^num_shard_bits
// shards will be created, with capacity split evenly to each of the shards.
// Keys are sharded by the highest num_shard_bits bits of hash value.
class ShardedCache : public rocksdb::Cache, public PriorityCache::PriCache {
 public:
  ShardedCache(size_t capacity, int num_shard_bits, bool strict_capacity_limit);
  virtual ~ShardedCache() = default;
  // rocksdb::Cache
  virtual const char* Name() const override = 0;
  using rocksdb::Cache::Insert;
  virtual rocksdb::Status Insert(const rocksdb::Slice& key, void* value, size_t charge,
                                 DeleterFn,
                                 rocksdb::Cache::Handle** handle, Priority priority) override;
  using rocksdb::Cache::Lookup;
  virtual rocksdb::Cache::Handle* Lookup(const rocksdb::Slice& key, rocksdb::Statistics* stats) override;
  virtual bool Ref(rocksdb::Cache::Handle* handle) override;
  using rocksdb::Cache::Release;
  virtual bool Release(rocksdb::Cache::Handle* handle, bool force_erase = false) override;
  virtual void* Value(Handle* handle) override = 0;
  virtual void Erase(const rocksdb::Slice& key) override;
  virtual uint64_t NewId() override;
  virtual void SetCapacity(size_t capacity) override;
  virtual void SetStrictCapacityLimit(bool strict_capacity_limit) override;
  virtual bool HasStrictCapacityLimit() const override;
  virtual size_t GetCapacity() const override;
  virtual size_t GetUsage() const override;
  virtual size_t GetUsage(rocksdb::Cache::Handle* handle) const override;
  virtual size_t GetPinnedUsage() const override;
  virtual size_t GetCharge(Handle* handle) const = 0;
#if (ROCKSDB_MAJOR >= 7 || (ROCKSDB_MAJOR == 6 && ROCKSDB_MINOR >= 22))
  virtual DeleterFn GetDeleter(Handle* handle) const override;
#endif
  virtual void DisownData() override = 0;
#if (ROCKSDB_MAJOR >= 7 || (ROCKSDB_MAJOR == 6 && ROCKSDB_MINOR >= 22))
  virtual void ApplyToAllEntries(
      const std::function<void(const rocksdb::Slice& key, void* value, size_t charge,
                               DeleterFn deleter)>& callback,
      const ApplyToAllEntriesOptions& opts) override;
#else
  virtual void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                      bool thread_safe) override;
#endif
  virtual void EraseUnRefEntries() override;
  virtual std::string GetPrintableOptions() const override;
  virtual CacheShard* GetShard(int shard) = 0;
  virtual const CacheShard* GetShard(int shard) const = 0;
  virtual uint32_t GetHash(Handle* handle) const = 0;

  int GetNumShardBits() const { return num_shard_bits_; }

  virtual uint32_t get_bin_count() const = 0;
  virtual void set_bin_count(uint32_t count) = 0;

  // PriCache
  virtual int64_t get_cache_bytes(PriorityCache::Priority pri) const {
    return cache_bytes[pri];
  }
  virtual int64_t get_cache_bytes() const {
    int64_t total = 0;
    for (int i = 0; i < PriorityCache::Priority::LAST + 1; i++) {
      PriorityCache::Priority pri = static_cast<PriorityCache::Priority>(i);
      total += get_cache_bytes(pri);
    }
    return total;
  }
  virtual void set_cache_bytes(PriorityCache::Priority pri, int64_t bytes) {
    cache_bytes[pri] = bytes;
  }
  virtual void add_cache_bytes(PriorityCache::Priority pri, int64_t bytes) {
    cache_bytes[pri] += bytes;
  }
  virtual double get_cache_ratio() const {
    return cache_ratio;
  }
  virtual void set_cache_ratio(double ratio) {
    cache_ratio = ratio;
  }
    virtual uint64_t get_bins(PriorityCache::Priority pri) const {
    if (pri > PriorityCache::Priority::PRI0 &&
        pri < PriorityCache::Priority::LAST) {
      return bins[pri];
    }
    return 0;
  }
  virtual void set_bins(PriorityCache::Priority pri, uint64_t end_bin) {
    if (pri <= PriorityCache::Priority::PRI0 ||
        pri >= PriorityCache::Priority::LAST) {
      return;
    }
    bins[pri] = end_bin;
    uint64_t max = 0;
    for (int pri = 1; pri < PriorityCache::Priority::LAST; pri++) {
      if (bins[pri] > max) {
        max = bins[pri];
      }
    }
    set_bin_count(max);
  }
  virtual void import_bins(const std::vector<uint64_t> &bins_v) {
    uint64_t max = 0;
    for (int pri = 1; pri < PriorityCache::Priority::LAST; pri++) {
      unsigned i = (unsigned) pri - 1;
      if (i < bins_v.size()) {
        bins[pri] = bins_v[i];
        if (bins[pri] > max) {
          max = bins[pri];
        }
      } else {
        bins[pri] = 0;
      }
    }
    set_bin_count(max);
  }
  virtual std::string get_cache_name() const = 0;

 private:
  static inline uint32_t HashSlice(const rocksdb::Slice& s) {
     return ceph_str_hash(CEPH_STR_HASH_RJENKINS, s.data(), s.size());
//    return Hash(s.data(), s.size(), 0);
  }

  uint32_t Shard(uint32_t hash) const {
    // Note, hash >> 32 yields hash in gcc, not the zero we expect!
    return (num_shard_bits_ > 0) ? (hash >> (32 - num_shard_bits_)) : 0;
  }

  uint64_t bins[PriorityCache::Priority::LAST+1] = {0};
  int64_t cache_bytes[PriorityCache::Priority::LAST+1] = {0};
  double cache_ratio = 0;

  int num_shard_bits_;
  mutable std::mutex capacity_mutex_;
  size_t capacity_;
  bool strict_capacity_limit_;
  std::atomic<uint64_t> last_id_;
};

extern int GetDefaultCacheShardBits(size_t capacity);

}  // namespace rocksdb_cache
#endif // ROCKSDB_SHARDED_CACHE
