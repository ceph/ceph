#ifndef RGW_USAGE_CACHE_H
#define RGW_USAGE_CACHE_H

#include <string>
#include <unordered_map>
#include <mutex>
#include <lmdb.h>

/**
 * RGWUsageCache is a wrapper around an LMDB-backed cache for storing
 * usage metrics. It follows a similar style to the bucket cache used in
 * the posix driver.
 */
class RGWUsageCache {
public:
  RGWUsageCache(const std::string &lmdb_path);
  ~RGWUsageCache();

  // Update the usage counters in the in-memory cache.
  void update_bucket_usage(const std::string &bucket, uint64_t bytes_used);
  void update_user_usage(const std::string &user, uint64_t bytes_used);

  // Retrieve usage values.
  uint64_t get_bucket_usage(const std::string &bucket);
  uint64_t get_user_usage(const std::string &user);

  // Flush in-memory cache to LMDB.
  bool flush_to_lmdb();

private:
  std::unordered_map<std::string, uint64_t> bucket_usage_cache;
  std::unordered_map<std::string, uint64_t> user_usage_cache;
  std::mutex cache_mutex;

  // LMDB environment and database handle.
  MDB_env *env;
  MDB_dbi dbi;
  std::string lmdb_path;

  // Initialize the LMDB environment (similar style as in bucket_cache.h)
  bool init_lmdb();
  // Close the LMDB environment.
  void close_lmdb();
};

#endif // RGW_USAGE_CACHE_H
