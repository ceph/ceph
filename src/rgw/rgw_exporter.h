#pragma once

#include <string>
#include <lmdb.h>
#include "rgw_common.h"
#include "common/ceph_context.h"

// Struct to hold usage metrics counters for a bucket or user
struct RGWUsageStats {
  uint64_t put_obj_ops = 0;
  uint64_t put_obj_bytes = 0;
  uint64_t get_obj_ops = 0;
  uint64_t get_obj_bytes = 0;
  uint64_t del_obj_ops = 0;
};

// Forward declaration
class RGWUsageCache;

class RGWExporter {
public:
  RGWExporter(CephContext *cct, RGWRados *store);
  ~RGWExporter();

  void update_usage();  // Periodically called to refresh perf counters
  void shutdown();      // Closes LMDB environment
};

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
  std::mutex write_lock;

  int put(MDB_dbi dbi, const std::string& key, const RGWUsageStats& stats);
  int get(MDB_dbi dbi, const std::string& key, RGWUsageStats *stats);
};

extern RGWExporter *g_rgw_exporter;