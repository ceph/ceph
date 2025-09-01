// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_usage_cache.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "common/ceph_context.h"
#include "common/perf_counters_collection.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw {

// Performance counter indices
enum {
  PERF_CACHE_FIRST = 100000,
  PERF_CACHE_HIT,
  PERF_CACHE_MISS,
  PERF_CACHE_UPDATE,
  PERF_CACHE_REMOVE,
  PERF_CACHE_EXPIRED,
  PERF_CACHE_SIZE,
  PERF_CACHE_USER_HIT,
  PERF_CACHE_USER_MISS,
  PERF_CACHE_BUCKET_HIT,
  PERF_CACHE_BUCKET_MISS,
  PERF_CACHE_LAST
};

void UsageStats::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  ceph::encode(bytes_used, bl);
  ceph::encode(num_objects, bl);
  ceph::encode(last_updated, bl);
  ENCODE_FINISH(bl);
}

void UsageStats::decode(bufferlist::const_iterator& bl) {
  DECODE_START(1, bl);
  ceph::decode(bytes_used, bl);
  ceph::decode(num_objects, bl);
  ceph::decode(last_updated, bl);
  DECODE_FINISH(bl);
}

UsageCache::UsageCache(const Config& cfg) 
  : config(cfg), cct(nullptr), perf_counters(nullptr) {}

UsageCache::UsageCache(CephContext* cct, const Config& cfg) 
  : config(cfg), cct(cct), perf_counters(nullptr) {
  init_perf_counters();
}

UsageCache::~UsageCache() {
  shutdown();
  cleanup_perf_counters();
}

UsageCache::UsageCache(UsageCache&& other) noexcept {
  std::unique_lock lock(other.db_mutex);
  config = std::move(other.config);
  env = other.env;
  user_dbi = other.user_dbi;
  bucket_dbi = other.bucket_dbi;
  initialized.store(other.initialized.load());
  cct = other.cct;
  perf_counters = other.perf_counters;
  
  other.env = nullptr;
  other.user_dbi = 0;
  other.bucket_dbi = 0;
  other.initialized = false;
  other.cct = nullptr;
  other.perf_counters = nullptr;
}

UsageCache& UsageCache::operator=(UsageCache&& other) noexcept {
  if (this != &other) {
    shutdown();
    cleanup_perf_counters();
    
    std::unique_lock lock(other.db_mutex);
    config = std::move(other.config);
    env = other.env;
    user_dbi = other.user_dbi;
    bucket_dbi = other.bucket_dbi;
    initialized.store(other.initialized.load());
    cct = other.cct;
    perf_counters = other.perf_counters;
    
    other.env = nullptr;
    other.user_dbi = 0;
    other.bucket_dbi = 0;
    other.initialized = false;
    other.cct = nullptr;
    other.perf_counters = nullptr;
  }
  return *this;
}

void UsageCache::init_perf_counters() {
  if (!cct || perf_counters) {
    return;  // No context or already initialized
  }
  
  PerfCountersBuilder pcb(cct, "rgw_usage_cache", 
                          PERF_CACHE_FIRST, PERF_CACHE_LAST);
  
  // Add counter definitions - nick must be 4 chars or less!
  pcb.add_u64_counter(PERF_CACHE_HIT, "cache_hits", 
                     "Total number of cache hits", "hit",
                     PerfCountersBuilder::PRIO_USEFUL);
  pcb.add_u64_counter(PERF_CACHE_MISS, "cache_misses", 
                     "Total number of cache misses", "miss",
                     PerfCountersBuilder::PRIO_USEFUL);
  pcb.add_u64_counter(PERF_CACHE_UPDATE, "cache_updates", 
                     "Total number of cache updates", "upd",
                     PerfCountersBuilder::PRIO_INTERESTING);
  pcb.add_u64_counter(PERF_CACHE_REMOVE, "cache_removes", 
                     "Total number of cache removes", "rm",
                     PerfCountersBuilder::PRIO_INTERESTING);
  pcb.add_u64_counter(PERF_CACHE_EXPIRED, "cache_expired", 
                     "Total number of expired entries", "exp",
                     PerfCountersBuilder::PRIO_DEBUGONLY);
  pcb.add_u64(PERF_CACHE_SIZE, "cache_size", 
             "Current cache size", "size",
             PerfCountersBuilder::PRIO_USEFUL);
  pcb.add_u64_counter(PERF_CACHE_USER_HIT, "user_cache_hits", 
                     "User cache hits", "uhit",  // Changed from "u_hit" to "uhit" (4 chars)
                     PerfCountersBuilder::PRIO_DEBUGONLY);
  pcb.add_u64_counter(PERF_CACHE_USER_MISS, "user_cache_misses", 
                     "User cache misses", "umis",  // Changed from "u_miss" to "umis" (4 chars)
                     PerfCountersBuilder::PRIO_DEBUGONLY);
  pcb.add_u64_counter(PERF_CACHE_BUCKET_HIT, "bucket_cache_hits", 
                     "Bucket cache hits", "bhit",  // Changed from "b_hit" to "bhit" (4 chars)
                     PerfCountersBuilder::PRIO_DEBUGONLY);
  pcb.add_u64_counter(PERF_CACHE_BUCKET_MISS, "bucket_cache_misses", 
                     "Bucket cache misses", "bmis",  // Changed from "b_miss" to "bmis" (4 chars)
                     PerfCountersBuilder::PRIO_DEBUGONLY);
  
  perf_counters = pcb.create_perf_counters();
  cct->get_perfcounters_collection()->add(perf_counters);
}

void UsageCache::cleanup_perf_counters() {
  if (cct && perf_counters) {
    cct->get_perfcounters_collection()->remove(perf_counters);
    delete perf_counters;
    perf_counters = nullptr;
  }
}

void UsageCache::inc_counter(int counter, uint64_t amount) {
  if (perf_counters) {
    perf_counters->inc(counter, amount);
  }
}

void UsageCache::set_counter(int counter, uint64_t value) {
  if (perf_counters) {
    perf_counters->set(counter, value);
  }
}

int UsageCache::init() {
  if (initialized.exchange(true)) {
    return 0;  // Already initialized
  }
  
  int ret = open_database();
  if (ret < 0) {
    initialized = false;
    return ret;
  }
  
  // Update cache size counter
  set_counter(PERF_CACHE_SIZE, get_cache_size());
  
  return 0;
}

void UsageCache::shutdown() {
  if (initialized.exchange(false)) {
    close_database();
  }
}

int UsageCache::open_database() {
  int rc = mdb_env_create(&env);
  if (rc != 0) {
    return -EIO;
  }

  rc = mdb_env_set_mapsize(env, config.max_db_size);
  if (rc != 0) {
    mdb_env_close(env);
    env = nullptr;
    return -EIO;
  }

  rc = mdb_env_set_maxreaders(env, config.max_readers);
  if (rc != 0) {
    mdb_env_close(env);
    env = nullptr;
    return -EIO;
  }

  rc = mdb_env_set_maxdbs(env, 2);  // user_stats and bucket_stats
  if (rc != 0) {
    mdb_env_close(env);
    env = nullptr;
    return -EIO;
  }

  rc = mdb_env_open(env, config.db_path.c_str(), MDB_NOSUBDIR | MDB_NOTLS, 0644);
  if (rc != 0) {
    mdb_env_close(env);
    env = nullptr;
    return -EIO;
  }

  // Open named databases
  MDB_txn* txn = nullptr;
  rc = mdb_txn_begin(env, nullptr, 0, &txn);
  if (rc != 0) {
    mdb_env_close(env);
    env = nullptr;
    return -EIO;
  }

  rc = mdb_dbi_open(txn, "user_stats", MDB_CREATE, &user_dbi);
  if (rc != 0) {
    mdb_txn_abort(txn);
    mdb_env_close(env);
    env = nullptr;
    return -EIO;
  }

  rc = mdb_dbi_open(txn, "bucket_stats", MDB_CREATE, &bucket_dbi);
  if (rc != 0) {
    mdb_txn_abort(txn);
    mdb_env_close(env);
    env = nullptr;
    return -EIO;
  }

  rc = mdb_txn_commit(txn);
  if (rc != 0) {
    mdb_env_close(env);
    env = nullptr;
    return -EIO;
  }

  return 0;
}

void UsageCache::close_database() {
  if (env) {
    mdb_env_close(env);
    env = nullptr;
    user_dbi = 0;
    bucket_dbi = 0;
  }
}

template<typename T>
int UsageCache::put_stats(MDB_dbi dbi, const std::string& key, const T& stats) {
  if (!initialized) {
    return -EINVAL;
  }

  bufferlist bl;
  stats.encode(bl);
  
  MDB_val mdb_key = {key.size(), const_cast<char*>(key.data())};
  MDB_val mdb_val = {bl.length(), bl.c_str()};
  
  MDB_txn* txn = nullptr;
  int rc = mdb_txn_begin(env, nullptr, 0, &txn);
  if (rc != 0) {
    return -EIO;
  }
  
  rc = mdb_put(txn, dbi, &mdb_key, &mdb_val, 0);
  if (rc != 0) {
    mdb_txn_abort(txn);
    return -EIO;
  }
  
  rc = mdb_txn_commit(txn);
  if (rc != 0) {
    return -EIO;
  }
  
  return 0;
}

template<typename T>
std::optional<T> UsageCache::get_stats(MDB_dbi dbi, const std::string& key) const {
  if (!initialized) {
    return std::nullopt;
  }

  MDB_val mdb_key = {key.size(), const_cast<char*>(key.data())};
  MDB_val mdb_val;
  
  MDB_txn* txn = nullptr;
  int rc = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
  if (rc != 0) {
    return std::nullopt;
  }
  
  rc = mdb_get(txn, dbi, &mdb_key, &mdb_val);
  mdb_txn_abort(txn);
  
  if (rc != 0) {
    return std::nullopt;
  }
  
  bufferlist bl;
  bl.append(static_cast<char*>(mdb_val.mv_data), mdb_val.mv_size);
  
  T stats;
  try {
    auto iter = bl.cbegin();
    stats.decode(iter);
    
    // Check TTL
    auto now = ceph::real_clock::now();
    if (now - stats.last_updated > config.ttl) {
      // Entry expired
      const_cast<UsageCache*>(this)->inc_counter(PERF_CACHE_EXPIRED);
      return std::nullopt;
    }
    
    return stats;
  } catch (const buffer::error& e) {
    return std::nullopt;
  }
}

int UsageCache::update_user_stats(const std::string& user_id,
                                  uint64_t bytes_used,
                                  uint64_t num_objects) {
  std::unique_lock lock(db_mutex);
  
  UsageStats stats;
  stats.bytes_used = bytes_used;
  stats.num_objects = num_objects;
  stats.last_updated = ceph::real_clock::now();
  
  int ret = put_stats(user_dbi, user_id, stats);
  if (ret == 0) {
    inc_counter(PERF_CACHE_UPDATE);
    set_counter(PERF_CACHE_SIZE, get_cache_size_internal());
  }
  
  return ret;
}

std::optional<UsageStats> UsageCache::get_user_stats(const std::string& user_id) const {
  std::shared_lock lock(db_mutex);
  auto result = get_stats<UsageStats>(user_dbi, user_id);
  
  // Update performance counters
  if (result.has_value()) {
    const_cast<UsageCache*>(this)->inc_counter(PERF_CACHE_HIT);
    const_cast<UsageCache*>(this)->inc_counter(PERF_CACHE_USER_HIT);
  } else {
    const_cast<UsageCache*>(this)->inc_counter(PERF_CACHE_MISS);
    const_cast<UsageCache*>(this)->inc_counter(PERF_CACHE_USER_MISS);
  }
  
  return result;
}

int UsageCache::remove_user_stats(const std::string& user_id) {
  if (!initialized) {
    return -EINVAL;
  }

  std::unique_lock lock(db_mutex);
  
  MDB_val mdb_key = {user_id.size(), const_cast<char*>(user_id.data())};
  
  MDB_txn* txn = nullptr;
  int rc = mdb_txn_begin(env, nullptr, 0, &txn);
  if (rc != 0) {
    return -EIO;
  }
  
  rc = mdb_del(txn, user_dbi, &mdb_key, nullptr);
  if (rc != 0 && rc != MDB_NOTFOUND) {
    mdb_txn_abort(txn);
    return -EIO;
  }
  
  rc = mdb_txn_commit(txn);
  if (rc != 0) {
    return -EIO;
  }
  
  inc_counter(PERF_CACHE_REMOVE);
  set_counter(PERF_CACHE_SIZE, get_cache_size_internal());
  
  return 0;
}

int UsageCache::update_bucket_stats(const std::string& bucket_name,
                                    uint64_t bytes_used,
                                    uint64_t num_objects) {
  std::unique_lock lock(db_mutex);
  
  UsageStats stats;
  stats.bytes_used = bytes_used;
  stats.num_objects = num_objects;
  stats.last_updated = ceph::real_clock::now();
  
  int ret = put_stats(bucket_dbi, bucket_name, stats);
  if (ret == 0) {
    inc_counter(PERF_CACHE_UPDATE);
    set_counter(PERF_CACHE_SIZE, get_cache_size_internal());
  }
  
  return ret;
}

std::optional<UsageStats> UsageCache::get_bucket_stats(const std::string& bucket_name) const {
  std::shared_lock lock(db_mutex);
  auto result = get_stats<UsageStats>(bucket_dbi, bucket_name);
  
  // Update performance counters
  if (result.has_value()) {
    const_cast<UsageCache*>(this)->inc_counter(PERF_CACHE_HIT);
    const_cast<UsageCache*>(this)->inc_counter(PERF_CACHE_BUCKET_HIT);
  } else {
    const_cast<UsageCache*>(this)->inc_counter(PERF_CACHE_MISS);
    const_cast<UsageCache*>(this)->inc_counter(PERF_CACHE_BUCKET_MISS);
  }
  
  return result;
}

int UsageCache::remove_bucket_stats(const std::string& bucket_name) {
  if (!initialized) {
    return -EINVAL;
  }

  std::unique_lock lock(db_mutex);
  
  MDB_val mdb_key = {bucket_name.size(), const_cast<char*>(bucket_name.data())};
  
  MDB_txn* txn = nullptr;
  int rc = mdb_txn_begin(env, nullptr, 0, &txn);
  if (rc != 0) {
    return -EIO;
  }
  
  rc = mdb_del(txn, bucket_dbi, &mdb_key, nullptr);
  if (rc != 0 && rc != MDB_NOTFOUND) {
    mdb_txn_abort(txn);
    return -EIO;
  }
  
  rc = mdb_txn_commit(txn);
  if (rc != 0) {
    return -EIO;
  }
  
  inc_counter(PERF_CACHE_REMOVE);
  set_counter(PERF_CACHE_SIZE, get_cache_size_internal());
  
  return 0;
}

int UsageCache::clear_expired_entries() {
  if (!initialized) {
    return -EINVAL;
  }

  std::unique_lock lock(db_mutex);
  
  auto now = ceph::real_clock::now();
  int total_removed = 0;
  
  // Helper lambda to clear expired entries from a database
  auto clear_db = [this, &now](MDB_dbi dbi) -> int {
    MDB_txn* txn = nullptr;
    MDB_cursor* cursor = nullptr;
    
    int rc = mdb_txn_begin(env, nullptr, 0, &txn);
    if (rc != 0) {
      return -EIO;
    }
    
    rc = mdb_cursor_open(txn, dbi, &cursor);
    if (rc != 0) {
      mdb_txn_abort(txn);
      return -EIO;
    }
    
    MDB_val key, val;
    int removed = 0;
    
    while (mdb_cursor_get(cursor, &key, &val, MDB_NEXT) == 0) {
      bufferlist bl;
      bl.append(static_cast<char*>(val.mv_data), val.mv_size);
      
      try {
        UsageStats stats;
        auto iter = bl.cbegin();
        stats.decode(iter);
        
        if (now - stats.last_updated > config.ttl) {
          mdb_cursor_del(cursor, 0);
          removed++;
          inc_counter(PERF_CACHE_EXPIRED);
        }
      } catch (const buffer::error& e) {
        // Skip malformed entries
      }
    }
    
    mdb_cursor_close(cursor);
    
    rc = mdb_txn_commit(txn);
    if (rc != 0) {
      return -EIO;
    }
    
    return removed;
  };
  
  int ret = clear_db(user_dbi);
  if (ret >= 0) {
    total_removed += ret;
  }
  
  ret = clear_db(bucket_dbi);
  if (ret >= 0) {
    total_removed += ret;
  }
  
  // Update cache size counter
  set_counter(PERF_CACHE_SIZE, get_cache_size_internal());
  
  return total_removed;
}

size_t UsageCache::get_cache_size() const {
  if (!initialized) {
    return 0;
  }

  std::shared_lock lock(db_mutex);
  return get_cache_size_internal();
}

size_t UsageCache::get_cache_size_internal() const {
  // This method assumes the caller already holds a lock
  if (!initialized) {
    return 0;
  }
  
  MDB_stat stat;
  MDB_txn* txn = nullptr;
  
  int rc = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
  if (rc != 0) {
    return 0;
  }
  
  size_t total = 0;
  
  if (mdb_stat(txn, user_dbi, &stat) == 0) {
    total += stat.ms_entries;
  }
  
  if (mdb_stat(txn, bucket_dbi, &stat) == 0) {
    total += stat.ms_entries;
  }
  
  mdb_txn_abort(txn);
  
  return total;
}

uint64_t UsageCache::get_cache_hits() const {
  if (!perf_counters) {
    return 0;
  }
  return perf_counters->get(PERF_CACHE_HIT);
}

uint64_t UsageCache::get_cache_misses() const {
  if (!perf_counters) {
    return 0;
  }
  return perf_counters->get(PERF_CACHE_MISS);
}

double UsageCache::get_hit_rate() const {
  if (!perf_counters) {
    return 0.0;
  }
  
  uint64_t hits = perf_counters->get(PERF_CACHE_HIT);
  uint64_t misses = perf_counters->get(PERF_CACHE_MISS);
  uint64_t total = hits + misses;
  
  return (total > 0) ? (double)hits / total * 100.0 : 0.0;
}

} // namespace rgw