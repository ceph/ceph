// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_usage_perf.h"
#include "common/ceph_context.h"
#include "common/perf_counters.h"
#include "common/dout.h"
#include "common/perf_counters_collection.h"

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
  : cct(cct), cache(std::make_unique<UsageCache>(cache_config)) {
  create_global_counters();
}

UsagePerfCounters::~UsagePerfCounters() {
  shutdown();
}

void UsagePerfCounters::create_global_counters() {
  PerfCountersBuilder b(cct, "rgw_usage", l_rgw_usage_first, l_rgw_usage_last);
  
  // These counters need to be defined for ALL enum values between first and last
  // Even if we don't use them in global counters, they need placeholders
  
  // User counters (not used in global, but need placeholders)
  b.add_u64(l_rgw_user_used_bytes, "user_used_bytes", 
           "User bytes placeholder", nullptr, 0, unit_t(UNIT_BYTES));
  b.add_u64(l_rgw_user_num_objects, "user_num_objects",
           "User objects placeholder", nullptr, 0, unit_t(0));
  
  // Bucket counters (not used in global, but need placeholders)  
  b.add_u64(l_rgw_bucket_used_bytes, "bucket_used_bytes",
           "Bucket bytes placeholder", nullptr, 0, unit_t(UNIT_BYTES));
  b.add_u64(l_rgw_bucket_num_objects, "bucket_num_objects",
           "Bucket objects placeholder", nullptr, 0, unit_t(0));
  
  // Cache metrics (these are the actual global counters)
  b.add_u64_counter(l_rgw_usage_cache_hit, "cache_hit", 
                   "Number of cache hits", nullptr, 0, unit_t(0));
  b.add_u64_counter(l_rgw_usage_cache_miss, "cache_miss",
                   "Number of cache misses", nullptr, 0, unit_t(0));
  b.add_u64_counter(l_rgw_usage_cache_update, "cache_update",
                   "Number of cache updates", nullptr, 0, unit_t(0));
  b.add_u64_counter(l_rgw_usage_cache_evict, "cache_evict",
                   "Number of cache evictions", nullptr, 0, unit_t(0));
  
  PerfCounters* raw_counters = b.create_perf_counters();
  cct->get_perfcounters_collection()->add(raw_counters);
  global_counters = std::shared_ptr<PerfCounters>(raw_counters, [](PerfCounters*){});
}

std::shared_ptr<PerfCounters> UsagePerfCounters::create_user_counters(const std::string& user_id) {
    std::string name = "rgw_user_" + user_id;
    PerfCountersBuilder b(cct, name, l_rgw_usage_first, l_rgw_usage_last);
    
    b.add_u64(l_rgw_user_used_bytes, "used_bytes",
             "Bytes used by user", nullptr, 0, unit_t(UNIT_BYTES));
    b.add_u64(l_rgw_user_num_objects, "num_objects",
             "Number of objects owned by user", nullptr, 0, unit_t(0));
    
    PerfCounters* raw_counters = b.create_perf_counters();
    cct->get_perfcounters_collection()->add(raw_counters);

    return std::shared_ptr<PerfCounters>(raw_counters, [](PerfCounters*){});
  }
  
  std::shared_ptr<PerfCounters> UsagePerfCounters::create_bucket_counters(const std::string& bucket_name) {
    std::string name = "rgw_bucket_" + bucket_name;
    PerfCountersBuilder b(cct, name, l_rgw_usage_first, l_rgw_usage_last);
    
    b.add_u64(l_rgw_bucket_used_bytes, "used_bytes",
             "Bytes used in bucket", nullptr, 0, unit_t(UNIT_BYTES));
    b.add_u64(l_rgw_bucket_num_objects, "num_objects",
             "Number of objects in bucket", nullptr, 0, unit_t(0));
    
    PerfCounters* raw_counters = b.create_perf_counters();
    cct->get_perfcounters_collection()->add(raw_counters);
    
    // Wrap in shared_ptr with custom deleter that doesn't delete
    return std::shared_ptr<PerfCounters>(raw_counters, [](PerfCounters*){});
  }

int UsagePerfCounters::init() {
  int ret = cache->init();
  if (ret < 0) {
    return ret;
  }

  return 0;
}

void UsagePerfCounters::start() {
  ldout(cct, 10) << "Starting usage perf counters" << dendl;
  // Could start background cleanup thread here if needed
}

void UsagePerfCounters::stop() {
  ldout(cct, 10) << "Stopping usage perf counters" << dendl;
  // Stop any background threads
}

void UsagePerfCounters::shutdown() {
  stop();
  
  // Clean up perf counters
  {
    std::unique_lock lock(counters_mutex);
    
    auto* collection = cct->get_perfcounters_collection();
    
    for (const auto& [_, counters] : user_perf_counters) {
      collection->remove(counters.get());
    }
    user_perf_counters.clear();
    
    for (const auto& [_, counters] : bucket_perf_counters) {
      collection->remove(counters.get());
    }
    bucket_perf_counters.clear();
    
    if (global_counters) {
      collection->remove(global_counters.get());
      global_counters.reset();
    }
  }
  
  cache->shutdown();
}

void UsagePerfCounters::update_user_stats(const std::string& user_id,
                                          uint64_t bytes_used,
                                          uint64_t num_objects,
                                          bool update_cache) {
  // Update cache if requested
  if (update_cache && cache) {
    int ret = cache->update_user_stats(user_id, bytes_used, num_objects);
    if (ret == 0) {
      global_counters->inc(l_rgw_usage_cache_update);
    }
  }
  
  // Update or create perf counters
  {
    std::unique_lock lock(counters_mutex);
    
    auto it = user_perf_counters.find(user_id);
    if (it == user_perf_counters.end()) {
      auto counters = create_user_counters(user_id);
      user_perf_counters[user_id] = counters;
      it = user_perf_counters.find(user_id);
    }
    
    it->second->set(l_rgw_user_used_bytes, bytes_used);
    it->second->set(l_rgw_user_num_objects, num_objects);
  }
  
  ldout(cct, 20) << "Updated user stats: " << user_id 
                 << " bytes=" << bytes_used 
                 << " objects=" << num_objects << dendl;
}

void UsagePerfCounters::update_bucket_stats(const std::string& bucket_name,
                                            uint64_t bytes_used,
                                            uint64_t num_objects,
                                            bool update_cache) {
  // Update cache if requested
  if (update_cache && cache) {
    int ret = cache->update_bucket_stats(bucket_name, bytes_used, num_objects);
    if (ret == 0) {
      global_counters->inc(l_rgw_usage_cache_update);
    }
  }
  
  // Update or create perf counters
  {
    std::unique_lock lock(counters_mutex);
    
    auto it = bucket_perf_counters.find(bucket_name);
    if (it == bucket_perf_counters.end()) {
      auto counters = create_bucket_counters(bucket_name);
      bucket_perf_counters[bucket_name] = counters;
      it = bucket_perf_counters.find(bucket_name);
    }
    
    it->second->set(l_rgw_bucket_used_bytes, bytes_used);
    it->second->set(l_rgw_bucket_num_objects, num_objects);
  }
  
  ldout(cct, 20) << "Updated bucket stats: " << bucket_name
                 << " bytes=" << bytes_used
                 << " objects=" << num_objects << dendl;
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

std::optional<UsageStats> UsagePerfCounters::get_user_stats(const std::string& user_id) const {
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

std::optional<UsageStats> UsagePerfCounters::get_bucket_stats(const std::string& bucket_name) const {
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
    ldout(cct, 10) << "Cleaned up " << removed << " expired cache entries" << dendl;
  }
}

size_t UsagePerfCounters::get_cache_size() const {
  return cache ? cache->get_cache_size() : 0;
}

} // namespace rgw