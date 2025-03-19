#ifndef RGW_USAGE_CACHE_H
#define RGW_USAGE_CACHE_H

#include <string>
#include <mutex>
#include <lmdb.h>
#include "common/ceph_context.h"

struct RGWUsageStats {
  uint64_t used_bytes = 0;
  uint64_t num_objects = 0;
};

/**
 * RGWUsageCache manages an LMDB-backed key-value store for storing
 * per-bucket and per-user usage statistics (bytes used, object count).
 * Keys are stored as strings (e.g., "bucket:..."/"user:...").
 */
class RGWUsageCache {
public:
  RGWUsageCache(CephContext* cct, const std::string& path);
  ~RGWUsageCache();

  int init();    // Initialize LMDB env and DB
  void close();  // Close LMDB env
  int clear();   // Wipe the database

  // Store stats
  int put_bucket_stats(const std::string& bucket_key, const RGWUsageStats& stats);
  int put_user_stats(const std::string& user_key, const RGWUsageStats& stats);

  // Fetch stats
  bool get_bucket_stats(const std::string& bucket_key, RGWUsageStats* out);
  bool get_user_stats(const std::string& user_key, RGWUsageStats* out);

private:
  CephContext* cct;
  std::string db_path;

  MDB_env* env;
  MDB_dbi dbi;
  std::mutex write_lock;  // ensures one writer at a time

  // No persistent txn member
};

#endif // RGW_USAGE_CACHE_H
