// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_usage_perf.h"
#include "common/ceph_context.h"
#include "common/perf_counters.h"
#include "common/dout.h"
#include "common/perf_counters_collection.h"
#include "common/errno.h" 
#include "common/async/yield_context.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw {

// Global singleton
static UsagePerfCounters* g_usage_perf_counters = nullptr;

UsagePerfCounters* get_usage_perf_counters() {
  return g_usage_perf_counters;
}

void set_usage_perf_counters(UsagePerfCounters* counters) {
  g_usage_perf_counters = counters;
}

UsagePerfCounters::UsagePerfCounters(CephContext* cct, 
                                     const UsageCache::Config& cache_config)
                                    : cct(cct), driver(nullptr), 
                                      cache(std::make_unique<UsageCache>(cct, cache_config)),
                                      global_counters(nullptr) 
{
    create_global_counters();
}

UsagePerfCounters::UsagePerfCounters(CephContext* cct) 
  : UsagePerfCounters(cct, UsageCache::Config{}) {}

UsagePerfCounters::UsagePerfCounters(CephContext* cct,
                                    rgw::sal::Driver* driver,
                                    const UsageCache::Config& cache_config)
                                    : cct(cct),
                                      driver(driver),  // ADD THIS
                                      cache(std::make_unique<UsageCache>(cct, cache_config)),
                                      global_counters(nullptr)
{
  create_global_counters();
}

// Update the destructor
UsagePerfCounters::~UsagePerfCounters() {
  shutdown();
}

void UsagePerfCounters::create_global_counters() {
  PerfCountersBuilder b(cct, "rgw_usage", l_rgw_usage_first, l_rgw_usage_last);
  
  // Placeholder counters for indices that aren't globally used
  b.add_u64(l_rgw_user_used_bytes, "user_used_bytes", 
           "User bytes placeholder", nullptr, 0, unit_t(UNIT_BYTES));
  b.add_u64(l_rgw_user_num_objects, "user_num_objects",
           "User objects placeholder", nullptr, 0, unit_t(0));
  b.add_u64(l_rgw_bucket_used_bytes, "bucket_used_bytes",
           "Bucket bytes placeholder", nullptr, 0, unit_t(UNIT_BYTES));
  b.add_u64(l_rgw_bucket_num_objects, "bucket_num_objects",
           "Bucket objects placeholder", nullptr, 0, unit_t(0));
  
  // Global cache metrics
  b.add_u64_counter(l_rgw_usage_cache_hit, "cache_hit", 
                   "Number of cache hits", nullptr, 0, unit_t(0));
  b.add_u64_counter(l_rgw_usage_cache_miss, "cache_miss",
                   "Number of cache misses", nullptr, 0, unit_t(0));
  b.add_u64_counter(l_rgw_usage_cache_update, "cache_update",
                   "Number of cache updates", nullptr, 0, unit_t(0));
  b.add_u64_counter(l_rgw_usage_cache_evict, "cache_evict",
                   "Number of cache evictions", nullptr, 0, unit_t(0));
  
  global_counters = b.create_perf_counters();
  cct->get_perfcounters_collection()->add(global_counters);
}

PerfCounters* UsagePerfCounters::create_user_counters(const std::string& user_id) {

  std::string name = "rgw_user_" + user_id;

  // Sanitize name for perf counters (replace non-alphanumeric with underscore)
  for (char& c : name) {
    if (!std::isalnum(c) && c != '_') {
      c = '_';
    }
  }

  // Create a separate enum range for user-specific counters
  enum {
    l_rgw_user_first = 930000,  // Different range from main counters
    l_rgw_user_bytes,
    l_rgw_user_objects,
    l_rgw_user_last
  };

  PerfCountersBuilder b(cct, name, l_rgw_user_first, l_rgw_user_last);

  b.add_u64(l_rgw_user_bytes, "used_bytes",
           "Bytes used by user", nullptr, 0, unit_t(UNIT_BYTES));
  b.add_u64(l_rgw_user_objects, "num_objects",
           "Number of objects owned by user", nullptr, 0, unit_t(0));

  PerfCounters* counters = b.create_perf_counters();
  cct->get_perfcounters_collection()->add(counters);

  return counters;
}

PerfCounters* UsagePerfCounters::create_bucket_counters(const std::string& bucket_name) {

  std::string name = "rgw_bucket_" + bucket_name;
  
  // Sanitize name for perf counters
  for (char& c : name) {
    if (!std::isalnum(c) && c != '_') {
      c = '_';
    }
  }

  // Create a separate enum range for bucket-specific counters
  enum {
    l_rgw_bucket_first = 940000,  // Different range from main counters
    l_rgw_bucket_bytes,
    l_rgw_bucket_objects,
    l_rgw_bucket_last
  };

  PerfCountersBuilder b(cct, name, l_rgw_bucket_first, l_rgw_bucket_last);

  b.add_u64(l_rgw_bucket_bytes, "used_bytes",
           "Bytes used in bucket", nullptr, 0, unit_t(UNIT_BYTES));
  b.add_u64(l_rgw_bucket_objects, "num_objects",
           "Number of objects in bucket", nullptr, 0, unit_t(0));

  PerfCounters* counters = b.create_perf_counters();
  cct->get_perfcounters_collection()->add(counters);

  return counters;
}

void UsagePerfCounters::cleanup_worker() {
  ldout(cct, 10) << "Starting usage cache cleanup worker thread" << dendl;
  
  while (!shutdown_flag.load()) {
    // Sleep with periodic checks for shutdown
    for (int i = 0; i < cleanup_interval.count(); ++i) {
      if (shutdown_flag.load()) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    if (!shutdown_flag.load()) {
      cleanup_expired_entries();
    }
  }
  
  ldout(cct, 10) << "Usage cache cleanup worker thread exiting" << dendl;
}

int UsagePerfCounters::init() {
  int ret = cache->init();
  if (ret < 0) {
    ldout(cct, 0) << "Failed to initialize usage cache: " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  
  create_global_counters();
  ldout(cct, 10) << "Usage performance counters initialized successfully" << dendl;
  return 0;
}

void UsagePerfCounters::start() {
  ldout(cct, 10) << "Starting usage perf counters" << dendl;
  shutdown_flag = false;
  
  // Start cleanup thread
  cleanup_thread = std::thread(&UsagePerfCounters::cleanup_worker, this);
  
  // Start refresh thread
  refresh_thread = std::thread(&UsagePerfCounters::refresh_worker, this);
  
  ldout(cct, 10) << "Started usage perf counters threads" << dendl;
}

void UsagePerfCounters::stop() {
  ldout(cct, 10) << "Stopping usage perf counters" << dendl;
  
  // Signal threads to stop
  shutdown_flag = true;
  
  // Wait for cleanup thread
  if (cleanup_thread.joinable()) {
    cleanup_thread.join();
  }
  
  // Wait for refresh thread
  if (refresh_thread.joinable()) {
    refresh_thread.join();
  }
  
  ldout(cct, 10) << "Stopped usage perf counters threads" << dendl;
}

void UsagePerfCounters::shutdown() {
  shutdown_flag = true;
  
  if (cleanup_thread.joinable()) {
    cleanup_thread.join();
  }
  
  if (refresh_thread.joinable()) {
    refresh_thread.join();
  }
  
  // Clean up perf counters
  {
    std::unique_lock lock(counters_mutex);
    
    auto* collection = cct->get_perfcounters_collection();
    
    // Remove and delete user counters
    for (auto& [_, counters] : user_perf_counters) {
      collection->remove(counters);
      delete counters;
    }
    user_perf_counters.clear();
    
    // Remove and delete bucket counters
    for (auto& [_, counters] : bucket_perf_counters) {
      collection->remove(counters);
      delete counters;
    }
    bucket_perf_counters.clear();
    
    // Remove global counters
    if (global_counters) {
      collection->remove(global_counters);
      delete global_counters;
      global_counters = nullptr;
    }
  }
  
  // Shutdown cache
  cache->shutdown();
  
  ldout(cct, 10) << "Shutdown usage perf counters" << dendl;
}
void UsagePerfCounters::update_bucket_stats(const std::string& bucket_name,
                                            uint64_t bytes_used,
                                            uint64_t num_objects,
                                            bool update_cache) {
  ldout(cct, 20) << "update_bucket_stats: bucket=" << bucket_name
                 << " bytes=" << bytes_used 
                 << " objects=" << num_objects 
                 << " update_cache=" << update_cache << dendl;
  
  // Update cache if requested - ALWAYS write the total values passed in
  if (update_cache && cache) {
    int ret = cache->update_bucket_stats(bucket_name, bytes_used, num_objects);
    if (ret == 0) {
      global_counters->inc(l_rgw_usage_cache_update);
      ldout(cct, 15) << "Cache updated for bucket " << bucket_name 
                     << " total_bytes=" << bytes_used 
                     << " total_objects=" << num_objects << dendl;
    } else {
      ldout(cct, 5) << "Failed to update bucket cache: " << cpp_strerror(-ret) << dendl;
    }
  }
  
  // Define local enum
  enum {
    l_rgw_bucket_first = 940000,
    l_rgw_bucket_bytes,
    l_rgw_bucket_objects,
    l_rgw_bucket_last
  };
  
  // Update perf counters
  {
    std::unique_lock lock(counters_mutex);
    
    auto it = bucket_perf_counters.find(bucket_name);
    if (it == bucket_perf_counters.end()) {
      PerfCounters* counters = create_bucket_counters(bucket_name);
      if (counters) {
        bucket_perf_counters[bucket_name] = counters;
        it = bucket_perf_counters.find(bucket_name);
        ldout(cct, 15) << "Created perf counter for bucket " << bucket_name << dendl;
      }
    }
    
    if (it != bucket_perf_counters.end() && it->second) {
      it->second->set(l_rgw_bucket_bytes, bytes_used);
      it->second->set(l_rgw_bucket_objects, num_objects);
      ldout(cct, 15) << "Set perf counter for bucket " << bucket_name 
                     << " bytes=" << bytes_used 
                     << " objects=" << num_objects << dendl;
    }
  }
}

void UsagePerfCounters::update_user_stats(const std::string& user_id,
                                          uint64_t bytes_used,
                                          uint64_t num_objects,
                                          bool update_cache) {
  ldout(cct, 20) << "update_user_stats: user=" << user_id
                 << " bytes=" << bytes_used 
                 << " objects=" << num_objects 
                 << " update_cache=" << update_cache << dendl;
  
  // Update cache if requested - ALWAYS write the total values passed in
  if (update_cache && cache) {
    int ret = cache->update_user_stats(user_id, bytes_used, num_objects);
    if (ret == 0) {
      global_counters->inc(l_rgw_usage_cache_update);
      ldout(cct, 15) << "Cache updated for user " << user_id 
                     << " total_bytes=" << bytes_used 
                     << " total_objects=" << num_objects << dendl;
    } else {
      ldout(cct, 5) << "Failed to update user cache: " << cpp_strerror(-ret) << dendl;
    }
  }
  
  // Define local enum
  enum {
    l_rgw_user_first = 930000,
    l_rgw_user_bytes,
    l_rgw_user_objects,
    l_rgw_user_last
  };
  
  // Update perf counters
  {
    std::unique_lock lock(counters_mutex);
    
    auto it = user_perf_counters.find(user_id);
    if (it == user_perf_counters.end()) {
      PerfCounters* counters = create_user_counters(user_id);
      if (counters) {
        user_perf_counters[user_id] = counters;
        it = user_perf_counters.find(user_id);
        ldout(cct, 15) << "Created perf counter for user " << user_id << dendl;
      }
    }
    
    if (it != user_perf_counters.end() && it->second) {
      it->second->set(l_rgw_user_bytes, bytes_used);
      it->second->set(l_rgw_user_objects, num_objects);
      ldout(cct, 15) << "Set perf counter for user " << user_id 
                     << " bytes=" << bytes_used 
                     << " objects=" << num_objects << dendl;
    }
  }
}

void UsagePerfCounters::mark_bucket_active(const std::string& bucket_name,
                                           const std::string& tenant) {
  std::string key = tenant.empty() ? bucket_name : tenant + "/" + bucket_name;
  
  ldout(cct, 20) << "mark_bucket_active: key=" << key << dendl;
  
  // Add to active set for background refresh
  {
    std::lock_guard<std::mutex> lock(activity_mutex);
    active_buckets.insert(key);
  }
  
  // Ensure perf counter exists
  {
    std::unique_lock lock(counters_mutex);
    if (bucket_perf_counters.find(key) == bucket_perf_counters.end()) {
      PerfCounters* pc = create_bucket_counters(key);
      if (pc) {
        bucket_perf_counters[key] = pc;
      }
    }
  }
  
  // Immediately update from cache
  auto cached_stats = cache->get_bucket_stats(key);
  if (cached_stats) {
    ldout(cct, 15) << "Updating from cache: bucket=" << key 
                   << " bytes=" << cached_stats->bytes_used 
                   << " objects=" << cached_stats->num_objects << dendl;
    
    std::string bucket_only = key;
    size_t pos = key.find('/');
    if (pos != std::string::npos) {
      bucket_only = key.substr(pos + 1);
    }
    
    update_bucket_stats(bucket_only, cached_stats->bytes_used, 
                       cached_stats->num_objects, false);
  }
}

void UsagePerfCounters::mark_user_active(const std::string& user_id) {
  ldout(cct, 20) << "mark_user_active: user=" << user_id << dendl;
  
  // Add to active set for background refresh
  {
    std::lock_guard<std::mutex> lock(activity_mutex);
    active_users.insert(user_id);
  }
  
  // Ensure perf counter exists
  {
    std::unique_lock lock(counters_mutex);
    if (user_perf_counters.find(user_id) == user_perf_counters.end()) {
      PerfCounters* pc = create_user_counters(user_id);
      if (pc) {
        user_perf_counters[user_id] = pc;
      }
    }
  }
  
  // Immediately update from cache
  auto cached_stats = cache->get_user_stats(user_id);
  if (cached_stats) {
    ldout(cct, 15) << "Updating from cache: user=" << user_id
                   << " bytes=" << cached_stats->bytes_used 
                   << " objects=" << cached_stats->num_objects << dendl;
    
    update_user_stats(user_id, cached_stats->bytes_used, 
                     cached_stats->num_objects, false);
  }
}

void UsagePerfCounters::refresh_worker() {
  ldout(cct, 10) << "Started usage stats refresh worker thread" << dendl;
  
  while (!shutdown_flag) {
    // Sleep for the refresh interval with periodic checks for shutdown
    auto sleep_until = std::chrono::steady_clock::now() + refresh_interval;
    
    while (!shutdown_flag && std::chrono::steady_clock::now() < sleep_until) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    if (shutdown_flag) {
      break;
    }
    
    // Get snapshot of active buckets and users
    std::unordered_set<std::string> buckets_to_refresh;
    std::unordered_set<std::string> users_to_refresh;
    
    {
      std::lock_guard<std::mutex> lock(activity_mutex);
      buckets_to_refresh = active_buckets;
      users_to_refresh = active_users;
    }
    
    ldout(cct, 15) << "Background refresh: checking " << buckets_to_refresh.size() 
                   << " buckets and " << users_to_refresh.size() 
                   << " users" << dendl;
    
    // Refresh bucket stats from cache
    for (const auto& bucket_key : buckets_to_refresh) {
      if (shutdown_flag) break;
      refresh_bucket_stats(bucket_key);
    }
    
    // Refresh user stats from cache
    for (const auto& user_id : users_to_refresh) {
      if (shutdown_flag) break;
      refresh_user_stats(user_id);
    }
  }
  
  ldout(cct, 10) << "Stopped usage stats refresh worker thread" << dendl;
}

void UsagePerfCounters::refresh_bucket_stats(const std::string& bucket_key) {
  ldout(cct, 20) << "refresh_bucket_stats: key=" << bucket_key << dendl;
  
  auto cached_stats = cache->get_bucket_stats(bucket_key);
  if (cached_stats) {
    std::string bucket_name = bucket_key;
    size_t pos = bucket_key.find('/');
    if (pos != std::string::npos) {
      bucket_name = bucket_key.substr(pos + 1);
    }
    
    ldout(cct, 15) << "Refreshing bucket " << bucket_key 
                   << " bytes=" << cached_stats->bytes_used 
                   << " objects=" << cached_stats->num_objects << dendl;
    
    update_bucket_stats(bucket_name, cached_stats->bytes_used, 
                       cached_stats->num_objects, false);
  }
}

void UsagePerfCounters::refresh_user_stats(const std::string& user_id) {
  ldout(cct, 20) << "refresh_user_stats: user=" << user_id << dendl;
  
  auto cached_stats = cache->get_user_stats(user_id);
  if (cached_stats) {
    ldout(cct, 15) << "Refreshing user " << user_id
                   << " bytes=" << cached_stats->bytes_used 
                   << " objects=" << cached_stats->num_objects << dendl;
    
    update_user_stats(user_id, cached_stats->bytes_used, 
                     cached_stats->num_objects, false);
  }
}

void UsagePerfCounters::refresh_from_cache(const std::string& user_id,
                                           const std::string& bucket_name) {
  if (!cache) {
    return;
  }
  
  // Refresh user stats
  if (!user_id.empty()) {
    auto user_stats = cache->get_user_stats(user_id);
    if (user_stats) {
      global_counters->inc(l_rgw_usage_cache_hit);
      update_user_stats(user_id, user_stats->bytes_used, 
                       user_stats->num_objects, false);
    } else {
      global_counters->inc(l_rgw_usage_cache_miss);
    }
  }
  
  // Refresh bucket stats
  if (!bucket_name.empty()) {
    auto bucket_stats = cache->get_bucket_stats(bucket_name);
    if (bucket_stats) {
      global_counters->inc(l_rgw_usage_cache_hit);
      update_bucket_stats(bucket_name, bucket_stats->bytes_used,
                         bucket_stats->num_objects, false);
    } else {
      global_counters->inc(l_rgw_usage_cache_miss);
    }
  }
}

void UsagePerfCounters::evict_from_cache(const std::string& user_id,
                                         const std::string& bucket_name) {
  if (!cache) {
    return;
  }
  
  if (!user_id.empty()) {
    cache->remove_user_stats(user_id);
    global_counters->inc(l_rgw_usage_cache_evict);
  }
  
  if (!bucket_name.empty()) {
    cache->remove_bucket_stats(bucket_name);
    global_counters->inc(l_rgw_usage_cache_evict);
  }
}

std::optional<UsageStats> UsagePerfCounters::get_user_stats(const std::string& user_id) {
  if (!cache) {
    return std::nullopt;
  }
  
  auto stats = cache->get_user_stats(user_id);
  if (stats) {
    global_counters->inc(l_rgw_usage_cache_hit);
  } else {
    global_counters->inc(l_rgw_usage_cache_miss);
  }
  
  return stats;
}

std::optional<UsageStats> UsagePerfCounters::get_bucket_stats(const std::string& bucket_name) {
  if (!cache) {
    return std::nullopt;
  }
  
  auto stats = cache->get_bucket_stats(bucket_name);
  if (stats) {
    global_counters->inc(l_rgw_usage_cache_hit);
  } else {
    global_counters->inc(l_rgw_usage_cache_miss);
  }
  
  return stats;
}

void UsagePerfCounters::cleanup_expired_entries() {
  if (cache) {
    int removed = cache->clear_expired_entries();
    if (removed > 0) {
      ldout(cct, 10) << "Cleaned up " << removed << " expired cache entries" << dendl;
    }
  }
}

size_t UsagePerfCounters::get_cache_size() const {
  return cache ? cache->get_cache_size() : 0;
}

} // namespace rgw