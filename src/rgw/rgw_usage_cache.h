// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>
#include <memory>
#include <optional>
#include <atomic>
#include <shared_mutex>
#include <chrono>
#include <lmdb.h>

#include "include/buffer.h"
#include "include/encoding.h"
#include "common/ceph_time.h"

// Forward declarations
class CephContext;
class PerfCounters;

namespace rgw {

struct UsageStats {
  uint64_t bytes_used;
  uint64_t num_objects;
  ceph::real_time last_updated;
  
  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& bl);
  WRITE_CLASS_ENCODER(UsageStats)
};

class UsageCache {
public:
  struct Config {
    std::string db_path;
    size_t max_db_size;
    uint32_t max_readers;
    std::chrono::seconds ttl;
    
    Config() 
      : db_path("/var/lib/ceph/radosgw/usage_cache.mdb"),
        max_db_size(1 << 30),  // 1GB default
        max_readers(126),
        ttl(300) {}  // 5 min TTL
  };

  explicit UsageCache(const Config& config);
  UsageCache(CephContext* cct, const Config& config);
  ~UsageCache();
  
  // Move semantics
  UsageCache(UsageCache&& other) noexcept;
  UsageCache& operator=(UsageCache&& other) noexcept;
  
  // Delete copy semantics
  UsageCache(const UsageCache&) = delete;
  UsageCache& operator=(const UsageCache&) = delete;
  
  // Lifecycle
  int init();
  void shutdown();
  
  // User stats operations (non-const to update counters)
  int update_user_stats(const std::string& user_id, 
                       uint64_t bytes_used, 
                       uint64_t num_objects);
  std::optional<UsageStats> get_user_stats(const std::string& user_id);
  int remove_user_stats(const std::string& user_id);
  
  // Bucket stats operations (non-const to update counters)
  int update_bucket_stats(const std::string& bucket_name,
                         uint64_t bytes_used,
                         uint64_t num_objects);
  std::optional<UsageStats> get_bucket_stats(const std::string& bucket_name);
  int remove_bucket_stats(const std::string& bucket_name);
  
  // Maintenance
  int clear_expired_entries();
  size_t get_cache_size() const;
  
  // Performance metrics
  uint64_t get_cache_hits() const;
  uint64_t get_cache_misses() const;
  double get_hit_rate() const;

private:
  // Database operations
  int open_database();
  void close_database();
  
  template<typename T>
  int put_stats(MDB_dbi dbi, const std::string& key, const T& stats);
  
  template<typename T>
  std::optional<T> get_stats(MDB_dbi dbi, const std::string& key);
  
  // Performance counter helpers
  void init_perf_counters();
  void cleanup_perf_counters();
  void inc_counter(int counter, uint64_t amount = 1);
  void set_counter(int counter, uint64_t value);
  
  // Internal helper for cache size (assumes lock is held)
  size_t get_cache_size_internal() const;
  
  Config config;
  MDB_env* env = nullptr;
  MDB_dbi user_dbi = 0;
  MDB_dbi bucket_dbi = 0;
  
  mutable std::shared_mutex db_mutex;
  std::atomic<bool> initialized{false};
  
  // Performance counters
  CephContext* cct;
  PerfCounters* perf_counters;
  
  // Mutable atomic counters for thread-safe statistics
  mutable std::atomic<uint64_t> cache_hits{0};
  mutable std::atomic<uint64_t> cache_misses{0};
};

} // namespace rgw