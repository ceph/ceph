#include "rgw_exporter.h"
#include "rgw_common.h"
#include "rgw_bucket.h"
#include "rgw_user.h"
#include "common/ceph_context.h"
#include "common/perf_counters.h"
#include "include/utime.h"
#include <sys/stat.h>
#include <unistd.h>

// Global instance
RGWExporter *g_rgw_exporter = nullptr;

RGWExporter::RGWExporter(CephContext *cct_, RGWRados *store_)
  : cct(cct_), store(store_), usage_cache(new RGWUsageCache),
    max_buckets(0), max_users(0), stop_flag(false) 
{
  // Read cache size limits from config (or use defaults)
  max_buckets = cct->_conf->rgw_bucket_counters_cache_size;
  max_users = cct->_conf->rgw_user_counters_cache_size;
}

RGWExporter::~RGWExporter() {
  stop();
  if (usage_cache) {
    usage_cache->close();
    delete usage_cache;
    usage_cache = nullptr;
  }
}

void RGWExporter::start() {
  // Initialize LMDB environment for usage cache
  std::string fsid;
  // Use cluster handle to get a unique path (fsid or cluster name)
  librados::Rados* cluster = store->getRadosHandle();
  if (cluster) {
    fsid = cluster->get_fsid();  // get cluster FSID as hex string
  } else {
    fsid = "default";
  }
  // Construct LMDB path 
  std::string base_dir = cct->_conf->rgw_posix_data; // reuse posix data dir if set
  if (base_dir.empty()) {
    base_dir = "/var/lib/ceph/radosgw";  // default base
  }
  std::string db_path = base_dir + "/" + fsid + "/rgw_metrics";
  // Ensure directory exists
  ::mkdir((base_dir + "/" + fsid).c_str(), 0755);
  ::mkdir(db_path.c_str(), 0755);

  int ret = usage_cache->open(db_path);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: RGWExporter failed to open LMDB at " << db_path 
                  << " (ret=" << ret << ")" << dendl;
    // Continue anyway without persistence if LMDB fails
  }

  // Launch background updatation thread
  stop_flag = false;
  updater_thread = std::thread(&RGWExporter::run_updater, this);

  ldout(cct, 5) << "RGWExporter started (LMDB path " << db_path << ")" << dendl;
}

void RGWExporter::stop() {
  // Signal thread to stop and join it
  stop_flag.store(true);
  if (updater_thread.joinable()) {
    updater_thread.join();
  }
}

void RGWExporter::run_updater() {
  // Determine update interval (seconds)
  int period = cct->_conf->exporter_stats_period;
  if (period <= 0) {
    period = 5; // default to 5 seconds if not set
  }
  while (!stop_flag.load()) {
    // Sleep for the configured period
    for (int i = 0; i < period*10 && !stop_flag.load(); ++i) {
      usleep(100000); // sleep in 0.1s increments so we can break out faster
    }
    if (stop_flag.load()) break;
    // Flush in-memory counters to LMDB (persist current values)
    flush_cache_to_db();
    // Evict LRU entries if above cache size limits
    evict_old_entries();
  }
  // On exit, do one final flush
  flush_cache_to_db();
}

void RGWExporter::flush_cache_to_db() {
  // Write all current cached metrics to LMDB

  for (auto& kv : bucket_cache) {
    const std::string& bucket_key = kv.first;
    const RGWUsageStats& stats = kv.second;
    usage_cache->put_bucket_stats(bucket_key, stats);
  }
  for (auto& kv : user_cache) {
    const std::string& user_key = kv.first;
    const RGWUsageStats& stats = kv.second;
    usage_cache->put_user_stats(user_key, stats);
  }
}

void RGWExporter::evict_old_entries() {
  // Evict least-recently-used bucket metrics if over limit
  
  if (max_buckets > 0 && bucket_cache.size() > max_buckets) {
    // Determine how many to evict
    size_t to_evict = bucket_cache.size() - max_buckets;
    while (to_evict-- > 0) {
      // find LRU bucket
      std::string lru_bucket;
      utime_t oldest = ceph_clock_now();
      for (auto& kv : bucket_last_access) {
        if (kv.second < oldest) {
          oldest = kv.second;
          lru_bucket = kv.first;
        }
      }
      if (lru_bucket.empty()) break;
      ldout(cct, 5) << "Evicting metrics for bucket " << lru_bucket << dendl;
      // Persist before evict (already done in flush, but ensure latest)
      auto it = bucket_cache.find(lru_bucket);
      if (it != bucket_cache.end()) {
        usage_cache->put_bucket_stats(lru_bucket, it->second);
      }
      // Remove from memory caches and perf counters
      bucket_cache.erase(lru_bucket);
      bucket_last_access.erase(lru_bucket);
      if (bucket_perf.count(lru_bucket)) {
        cct->get_perfcounters_collection()->remove(bucket_perf[lru_bucket]);
        bucket_perf.erase(lru_bucket);
      }
    }
  }
  // Evict least-recently-used user metrics if over limit
  if (max_users > 0 && user_cache.size() > max_users) {
    size_t to_evict = user_cache.size() - max_users;
    while (to_evict-- > 0) {
      std::string lru_user;
      utime_t oldest = ceph_clock_now();
      for (auto& kv : user_last_access) {
        if (kv.second < oldest) {
          oldest = kv.second;
          lru_user = kv.first;
        }
      }
      if (lru_user.empty()) break;
      ldout(cct, 5) << "Evicting metrics for user " << lru_user << dendl;
      auto it = user_cache.find(lru_user);
      if (it != user_cache.end()) {
        usage_cache->put_user_stats(lru_user, it->second);
      }
      user_cache.erase(lru_user);
      user_last_access.erase(lru_user);
      if (user_perf.count(lru_user)) {
        cct->get_perfcounters_collection()->remove(user_perf[lru_user]);
        user_perf.erase(lru_user);
      }
    }
  }
}

std::shared_ptr<PerfCounters> RGWExporter::create_perf_counters(const std::string& label, bool is_user) {
  // Define perf counter schema for RGW op metrics (if not already defined globally)
  // Each PerfCounters instance will have the same metrics but different labels.
  PerfCountersBuilder pcb(cct, PerfCountersBuilder::COUNTER_RGW);  // assume RGW base type for counters
  // Add relevant counters (matching those tracked in RGWUsageStats)
  pcb.add_u64_counter("get_obj_ops", "GET object operations");
  pcb.add_u64_counter("get_obj_bytes", "Bytes read via GET");
  pcb.add_u64_counter("put_obj_ops", "PUT object operations");
  pcb.add_u64_counter("put_obj_bytes", "Bytes written via PUT");
  pcb.add_u64_counter("del_obj_ops", "DELETE object operations");
  
  std::string name = is_user ? "rgw_op_per_user" : "rgw_op_per_bucket";
  // Create PerfCounters instance (not registering globally yet)
  
  std::shared_ptr<PerfCounters> pc = pcb.create_perf_counters(name.c_str());
  // Label the instance for Prometheus output
  // Use lower-case labels "user" or "bucket"
  
  if (is_user) {
    pc->set_label("user", label);
  } else {
    pc->set_label("bucket", label);
  }

  // Register this PerfCounters so it appears in perf dump
  cct->get_perfcounters_collection()->add(pc);
  return pc;
}

void RGWExporter::inc_bucket_op(const rgw_bucket& bucket, const std::string& op, uint64_t bytes, uint64_t /*lat_ns*/) {
  
  // Construct a unique key for the bucket (tenant + bucket name or bucket id)
  std::string bucket_id = bucket.name; 
  if (!bucket.tenant.empty()) {
    bucket_id = bucket.tenant + "/" + bucket.name;
  }
  // If not in cache, try load from LMDB
  if (bucket_cache.find(bucket_id) == bucket_cache.end()) {
    RGWUsageStats stats;
    usage_cache->get_bucket_stats(bucket_id, &stats); // load previous stats if exist
    bucket_cache[bucket_id] = stats;
    bucket_last_access[bucket_id] = ceph_clock_now();
    // Create and register PerfCounters for this bucket
    bucket_perf[bucket_id] = create_perf_counters(bucket_id, false);
    // Initialize counters with loaded stats
    auto pc = bucket_perf[bucket_id];
    pc->set_u64("get_obj_ops", stats.get_obj_ops);
    pc->set_u64("get_obj_bytes", stats.get_obj_bytes);
    pc->set_u64("put_obj_ops", stats.put_obj_ops);
    pc->set_u64("put_obj_bytes", stats.put_obj_bytes);
    pc->set_u64("del_obj_ops", stats.del_obj_ops);
  }

  // Update in-memory stats and perf counters
  RGWUsageStats& stats = bucket_cache[bucket_id];
  if (op == "get") {
    stats.get_obj_ops += 1;
    stats.get_obj_bytes += bytes;
    bucket_perf[bucket_id]->inc("get_obj_ops", 1);
    if (bytes > 0)
      bucket_perf[bucket_id]->inc("get_obj_bytes", bytes);
  } else if (op == "put") {
    stats.put_obj_ops += 1;
    stats.put_obj_bytes += bytes;
    bucket_perf[bucket_id]->inc("put_obj_ops", 1);
    if (bytes > 0)
      bucket_perf[bucket_id]->inc("put_obj_bytes", bytes);
  } else if (op == "delete") {
    stats.del_obj_ops += 1;
    bucket_perf[bucket_id]->inc("del_obj_ops", 1);
  }
  
  // (We could handle other op types similarly)
  bucket_last_access[bucket_id] = ceph_clock_now();
}

void RGWExporter::inc_user_op(const rgw_user& user, const std::string& op, uint64_t bytes, uint64_t /*lat_ns*/) {
  
  // Similar to inc_bucket_op, but for user
  std::string user_id = user.to_str(); // rgw_user usually can be converted to "tenant:uid"
  if (user_cache.find(user_id) == user_cache.end()) {
    RGWUsageStats stats;
    usage_cache->get_user_stats(user_id, &stats);
    user_cache[user_id] = stats;
    user_last_access[user_id] = ceph_clock_now();
    user_perf[user_id] = create_perf_counters(user_id, true);
    // Initialize counters with loaded stats
    auto pc = user_perf[user_id];
    pc->set_u64("get_obj_ops", stats.get_obj_ops);
    pc->set_u64("get_obj_bytes", stats.get_obj_bytes);
    pc->set_u64("put_obj_ops", stats.put_obj_ops);
    pc->set_u64("put_obj_bytes", stats.put_obj_bytes);
    pc->set_u64("del_obj_ops", stats.del_obj_ops);
  }
  RGWUsageStats& stats = user_cache[user_id];
  if (op == "get") {
    stats.get_obj_ops += 1;
    stats.get_obj_bytes += bytes;
    user_perf[user_id]->inc("get_obj_ops", 1);
    if (bytes > 0)
      user_perf[user_id]->inc("get_obj_bytes", bytes);
  } else if (op == "put") {
    stats.put_obj_ops += 1;
    stats.put_obj_bytes += bytes;
    user_perf[user_id]->inc("put_obj_ops", 1);
    if (bytes > 0)
      user_perf[user_id]->inc("put_obj_bytes", bytes);
  } else if (op == "delete") {
    stats.del_obj_ops += 1;
    user_perf[user_id]->inc("del_obj_ops", 1);
  }
  user_last_access[user_id] = ceph_clock_now();
}

int RGWExporter::get_bucket_usage(const rgw_bucket& bucket, RGWUsageStats *stats) {
  // Fetch aggregated usage (object count, bytes) for a bucket using RGW APIs
  RGWBucketInfo info;
  RGWBucketStats bs;
  // Use RGWRados to get up-to-date bucket stats from metadata
  int ret = store->get_bucket_stats(bucket, &bs);  // hypothetical RGWRados::get_bucket_stats
  if (ret < 0) {
    return ret;
  }
  // Fill RGWUsageStats with relevant data (if needed we could include object count, etc.)
  stats->get_obj_ops   = 0;
  stats->put_obj_ops   = 0;
  stats->del_obj_ops   = 0;
  stats->get_obj_bytes = 0;
  stats->put_obj_bytes = 0;
  // The RGWBucketStats (bs) might contain total object count and size
  // If needed, we could expose number of objects and total bytes as separate metrics (as gauges).
  // For now, this function primarily demonstrates fetching bucket info (not directly used in op counters).
  return 0;
}

int RGWExporter::get_user_usage(const rgw_user& user, RGWUsageStats *stats) {
  // Aggregate usage across all buckets of a user
  std::list<rgw_bucket> buckets;
  int ret = store->list_buckets(user, buckets); // get all bucket entries owned by user
  if (ret < 0) {
    return ret;
  }
  uint64_t total_bytes = 0;
  uint64_t total_objects = 0;
  for (auto& b : buckets) {
    RGWBucketStats bs;
    int r = store->get_bucket_stats(b, &bs);
    if (r == 0) {
      total_bytes += bs.num_bytes;
      total_objects += bs.num_objects;
    }
  }
  // Populate stats (here we might use the fields in RGWUsageStats differently for static usage metrics)
  stats->put_obj_bytes = total_bytes;
  stats->put_obj_ops = total_objects;
  return 0;
}

// ---------------- RGWUsageCache (LMDB) Implementation ----------------

RGWUsageCache::~RGWUsageCache() {
  close();
}

int RGWUsageCache::open(const std::string& path) {
  int rc;
  rc = mdb_env_create(&env);
  if (rc != 0) {
    return -rc;
  }
  // Allow two sub-databases (for bucket and user)
  mdb_env_set_maxdbs(env, 2);
  // Set a reasonable map size (e.g., 128 MB)
  mdb_env_set_mapsize(env, 128 * 1024 * 1024);
  // Open the environment (create if not exist)
  rc = mdb_env_open(env, path.c_str(), MDB_NOSUBDIR | MDB_NOSYNC | MDB_NOMETASYNC | MDB_WRITEMAP, 0664);
  if (rc != 0) {
    mdb_env_close(env);
    env = nullptr;
    return -rc;
  }
  // Open bucket and user databases
  MDB_txn *txn;
  rc = mdb_txn_begin(env, NULL, 0, &txn);
  if (rc == 0) {
    rc = mdb_dbi_open(txn, "bucket", MDB_CREATE, &dbi_bucket);
    if (rc == 0) {
      rc = mdb_dbi_open(txn, "user", MDB_CREATE, &dbi_user);
    }
    int commit_rc = mdb_txn_commit(txn);
    if (rc == 0) rc = commit_rc;
  }
  if (rc != 0) {
    mdb_env_close(env);
    env = nullptr;
    return -rc;
  }
  return 0;
}

void RGWUsageCache::close() {
  if (env) {
    // Close database handles
    MDB_txn *txn;
    if (mdb_txn_begin(env, NULL, 0, &txn) == 0) {
      mdb_dbi_close(env, dbi_bucket);
      mdb_dbi_close(env, dbi_user);
      mdb_txn_abort(txn); // we don't need to commit just for closing handles
    }
    mdb_env_close(env);
    env = nullptr;
  }
}

int RGWUsageCache::put(MDB_dbi dbi, const std::string& key, const RGWUsageStats& stats) {
  if (!env) return -EINVAL;
  std::lock_guard<std::mutex> l(write_lock);  // only one writer at a time
  MDB_txn *txn;
  int rc = mdb_txn_begin(env, NULL, 0, &txn);
  if (rc != 0) return -rc;
  MDB_val mdb_key, mdb_val;
  mdb_key.mv_size = key.size();
  mdb_key.mv_data = (void*)key.data();
  // Value is raw struct
  mdb_val.mv_size = sizeof(RGWUsageStats);
  mdb_val.mv_data = (void*)&stats;
  rc = mdb_put(txn, dbi, &mdb_key, &mdb_val, 0);
  if (rc == 0) {
    rc = mdb_txn_commit(txn);
  } else {
    mdb_txn_abort(txn);
  }
  return (rc == 0 ? 0 : -rc);
}

int RGWUsageCache::get(MDB_dbi dbi, const std::string& key, RGWUsageStats *stats) {
  if (!env) return -EINVAL;
  MDB_txn *txn;
  int rc = mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
  if (rc != 0) return -rc;
  MDB_val mdb_key, mdb_val;
  mdb_key.mv_size = key.size();
  mdb_key.mv_data = (void*)key.data();
  rc = mdb_get(txn, dbi, &mdb_key, &mdb_val);
  if (rc == MDB_NOTFOUND) {
    mdb_txn_abort(txn);
    return -ENOENT;
  }
  if (rc != 0) {
    mdb_txn_abort(txn);
    return -rc;
  }
  // Copy data out
  if (mdb_val.mv_size == sizeof(RGWUsageStats)) {
    memcpy(stats, mdb_val.mv_data, sizeof(RGWUsageStats));
  } else {
    // If data size mismatched, treat as not found (possible version change)
    memset(stats, 0, sizeof(RGWUsageStats));
  }
  mdb_txn_abort(txn);
  return 0;
}

int RGWUsageCache::get_bucket_stats(const std::string& bucket_key, RGWUsageStats *stats) {
  return get(dbi_bucket, bucket_key, stats);
}

int RGWUsageCache::get_user_stats(const std::string& user_key, RGWUsageStats *stats) {
  return get(dbi_user, user_key, stats);
}

int RGWUsageCache::put_bucket_stats(const std::string& bucket_key, const RGWUsageStats& stats) {
  return put(dbi_bucket, bucket_key, stats);
}

int RGWUsageCache::put_user_stats(const std::string& user_key, const RGWUsageStats& stats) {
  return put(dbi_user, user_key, stats);
}
