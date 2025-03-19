#ifndef RGW_USAGE_CACHE_H
#define RGW_USAGE_CACHE_H

#include <string>
#include <unordered_map>
#include <mutex>
#include <lmdb.h>
#include "common/ceph_context.h"

struct RGWUsageStats {
  uint64_t used_bytes;
  uint64_t num_objects;
};

/**
 * RGWUsageCache is a wrapper around an LMDB-backed cache for storing
 * usage metrics. It follows a similar style to the bucket cache used in
 * the posix driver.
 */
class RGWUsageCache {
public:
    explicit RGWUsageCache(CephContext *cct, const std::string& path);
    ~RGWUsageCache();
    // Initialize the LMDB environment and databases
    int init();
    // Close the LMDB environment
    void close();
    // Update usage for a bucket (key format "bucket:<owner>/<bucket_name>")
    int put_bucket_usage(const std::string& bucket_key, const RGWUsageStats& stats);
    // Update usage for a user (key format "user:<user_id>")
    int put_user_usage(const std::string& user_key, const RGWUsageStats& stats);
    // Retrieve cached usage for a bucket
    bool get_bucket_usage(const std::string& bucket_key, RGWUsageStats *out);
    // Retrieve cached usage for a user
    bool get_user_usage(const std::string& user_key, RGWUsageStats *out);
    // Clear all entries in the cache (for full refresh)
    int clear();

private:
    CephContext *cct;
    std::string db_path;
    MDB_env *env;
    MDB_dbi dbi; // single database handling both bucket and user entries (key prefixed)
    MDB_txn *txn; // transient; not stored
};

#endif // RGW_USAGE_CACHE_H
