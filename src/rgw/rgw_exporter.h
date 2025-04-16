#pragma once

#include <map>
#include <string>
#include <thread>
#include <atomic>
#include <mutex>
#include <lmdb.h>
#include "rgw_common.h"
#include "common/ceph_context.h"
#include "common/PerfCounters.h"

// Struct to hold usage metrics counters for a bucket or user
struct RGWUsageStats {
  uint64_t put_obj_ops = 0;
  uint64_t put_obj_bytes = 0;
  uint64_t get_obj_ops = 0;
  uint64_t get_obj_bytes = 0;
  uint64_t del_obj_ops = 0;
  // (Latency metrics could include sum and count for each op if needed)
};

// Forward declaration
class RGWUsageCache;

// RGWExporter manages Prometheus metrics collection for RGW
class RGWExporter {
public:
  RGWExporter(CephContext *cct, RGWRados *store);
  ~RGWExporter();

  // Start background thread for metrics updates
  void start();
  // Stop background thread and close LMDB
  void stop();

  // Increment counters for a specific bucket or user (called on S3 ops)
  void inc_bucket_op(const rgw_bucket& bucket, const std::string& op, uint64_t bytes = 0, uint64_t lat_ns = 0);
  void inc_user_op(const rgw_user& user, const std::string& op, uint64_t bytes = 0, uint64_t lat_ns = 0);

  // Fetch current usage stats for a bucket or user (from cache/LMDB)
  int get_bucket_usage(const rgw_bucket& bucket, RGWUsageStats *stats);
  int get_user_usage(const rgw_user& user, RGWUsageStats *stats);

private:
  CephContext *cct;
  RGWRados *store;
  RGWUsageCache *usage_cache;

  // In-memory caches for active bucket and user metrics (LRU management)
  std::map<std::string, RGWUsageStats> bucket_cache;
  std::map<std::string, RGWUsageStats> user_cache;
  std::map<std::string, utime_t> bucket_last_access;
  std::map<std::string, utime_t> user_last_access;

  // PerfCounter instances for labeled metrics
  std::map<std::string, std::shared_ptr<PerfCounters>> bucket_perf;
  std::map<std::string, std::shared_ptr<PerfCounters>> user_perf;

  // Cache size limits and LRU eviction
  size_t max_buckets;
  size_t max_users;

  // Background update thread
  std::thread updater_thread;
  std::atomic<bool> stop_flag;

  // Internal helper methods
  void run_updater();                   // thread loop
  void flush_cache_to_db();             // flush in-memory counters to LMDB
  void evict_old_entries();             // evict least-recently-used entries if over limit
  std::shared_ptr<PerfCounters> create_perf_counters(const std::string& label, bool is_user);
};

// RGWUsageCache encapsulates LMDB storage for metrics persistence
class RGWUsageCache {
public:
  RGWUsageCache() : env(nullptr) {}
  ~RGWUsageCache();

  int open(const std::string& path);
  void close();
  int get_bucket_stats(const std::string& bucket_key, RGWUsageStats *stats);
  int get_user_stats(const std::string& user_key, RGWUsageStats *stats);
  int put_bucket_stats(const std::string& bucket_key, const RGWUsageStats& stats);
  int put_user_stats(const std::string& user_key, const RGWUsageStats& stats);

private:
  MDB_env *env;
  MDB_dbi dbi_bucket;
  MDB_dbi dbi_user;
  std::mutex write_lock;  // serialize writes (LMDB allows one writer at a time)

  // Helper for LMDB put
  int put(MDB_dbi dbi, const std::string& key, const RGWUsageStats& stats);
  // Helper for LMDB get
  int get(MDB_dbi dbi, const std::string& key, RGWUsageStats *stats);
};

extern RGWExporter *g_rgw_exporter;  // global exporter instance for use in RGW
