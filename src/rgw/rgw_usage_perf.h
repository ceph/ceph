// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <thread>
#include <atomic>
#include <chrono>

#include "common/perf_counters.h"
#include "rgw_usage_cache.h"

class CephContext;

namespace rgw {

// Performance counter indices
enum {
  l_rgw_usage_first = 920000,
  l_rgw_user_used_bytes,
  l_rgw_user_num_objects,
  l_rgw_bucket_used_bytes,
  l_rgw_bucket_num_objects,
  l_rgw_usage_cache_hit,
  l_rgw_usage_cache_miss,
  l_rgw_usage_cache_update,
  l_rgw_usage_cache_evict,
  l_rgw_usage_last
};

class UsagePerfCounters {
private:
  CephContext* cct;
  std::unique_ptr<UsageCache> cache;
  
  mutable std::shared_mutex counters_mutex;
  
  // Track raw pointers for proper cleanup
  std::unordered_map<std::string, PerfCounters*> user_perf_counters;
  std::unordered_map<std::string, PerfCounters*> bucket_perf_counters;
  
  PerfCounters* global_counters;
  
  // Cleanup thread management
  std::thread cleanup_thread;
  std::atomic<bool> shutdown_flag{false};
  std::chrono::seconds cleanup_interval{300}; // 5 minutes
  
  void create_global_counters();
  PerfCounters* create_user_counters(const std::string& user_id);
  PerfCounters* create_bucket_counters(const std::string& bucket_name);
  
  void cleanup_worker();

public:
  explicit UsagePerfCounters(CephContext* cct, 
                            const UsageCache::Config& cache_config);
  explicit UsagePerfCounters(CephContext* cct) 
    : UsagePerfCounters(cct, UsageCache::Config{}) {}
  ~UsagePerfCounters();
  
  // Lifecycle management
  int init();
  void start();
  void stop();
  void shutdown();
  
  // User stats updates
  void update_user_stats(const std::string& user_id,
                        uint64_t bytes_used,
                        uint64_t num_objects,
                        bool update_cache = true);
  
  // Bucket stats updates
  void update_bucket_stats(const std::string& bucket_name,
                          uint64_t bytes_used,
                          uint64_t num_objects,
                          bool update_cache = true);
  
  // Cache operations
  void refresh_from_cache(const std::string& user_id,
                         const std::string& bucket_name);
  void evict_from_cache(const std::string& user_id,
                       const std::string& bucket_name);
  
  // Stats retrieval (from cache)
  std::optional<UsageStats> get_user_stats(const std::string& user_id);
  std::optional<UsageStats> get_bucket_stats(const std::string& bucket_name);
  
  // Maintenance
  void cleanup_expired_entries();
  size_t get_cache_size() const;
  
  // Set cleanup interval
  void set_cleanup_interval(std::chrono::seconds interval) {
    cleanup_interval = interval;
  }
};

// Global singleton access
UsagePerfCounters* get_usage_perf_counters();
void set_usage_perf_counters(UsagePerfCounters* counters);

} // namespace rgw