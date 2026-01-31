// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_usage_cache.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "common/ceph_context.h"
#include "common/perf_counters_collection.h"
#include "common/errno.h" 
#include <sys/stat.h>
#include <sys/types.h>

#define dout_subsys ceph_subsys_rgw

namespace rgw {

// Performance counter indices
enum {
  PERF_CACHE_FIRST = 100000,
  PERF_CACHE_HIT,
  PERF_CACHE_MISS,
  PERF_CACHE_UPDATE,
  PERF_CACHE_SIZE,
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
  cache_hits.store(other.cache_hits.load());
  cache_misses.store(other.cache_misses.load());
  
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
    cache_hits.store(other.cache_hits.load());
    cache_misses.store(other.cache_misses.load());
    
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
    return;
  }
  
  PerfCountersBuilder pcb(cct, "rgw_usage_cache", 
                          PERF_CACHE_FIRST, PERF_CACHE_LAST);
  
  pcb.add_u64_counter(PERF_CACHE_HIT, "cache_hits", 
                     "Total number of cache hits", "hit",
                     PerfCountersBuilder::PRIO_USEFUL);
  pcb.add_u64_counter(PERF_CACHE_MISS, "cache_misses", 
                     "Total number of cache misses", "miss",
                     PerfCountersBuilder::PRIO_USEFUL);
  pcb.add_u64_counter(PERF_CACHE_UPDATE, "cache_updates", 
                     "Total number of cache updates", "upd",
                     PerfCountersBuilder::PRIO_INTERESTING);
  pcb.add_u64(PERF_CACHE_SIZE, "cache_size", 
             "Current cache size", "size",
             PerfCountersBuilder::PRIO_USEFUL);
  
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

int UsageCache::delete_corrupted_database() {
  std::vector<std::string> files_to_delete = {
    config.db_path,           // Main database file (e.g., /tmp/usage_cache.mdb)
    config.db_path + "-lock"  // Lock file (e.g., /tmp/usage_cache.mdb-lock)
  };
  
  bool any_deleted = false;
  for (const auto& file : files_to_delete) {
    if (unlink(file.c_str()) == 0) {
      if (cct) {
        ldout(cct, 1) << "UsageCache: Deleted corrupted file: " << file << dendl;
      }
      any_deleted = true;
    } else if (errno != ENOENT) {
      // ENOENT is okay (file doesn't exist), other errors are problems
      if (cct) {
        ldout(cct, 1) << "UsageCache: Warning - could not delete " << file 
                      << ": " << cpp_strerror(errno) << dendl;
      }
    }
  }
  
  return any_deleted ? 0 : -ENOENT;
}

int UsageCache::init() {
  if (initialized.exchange(true)) {
    return 0;
  }
  
  // Validate database directory exists
  if (cct) {
    std::string db_dir = config.db_path;
    size_t pos = db_dir.find_last_of('/');
    if (pos != std::string::npos) {
      db_dir = db_dir.substr(0, pos);
    }
    
    struct stat st;
    if (stat(db_dir.c_str(), &st) != 0) {
      // Try to create directory
      if (mkdir(db_dir.c_str(), 0755) != 0) {
        ldout(cct, 0) << "ERROR: Failed to create usage cache directory: " 
                      << db_dir << " - " << cpp_strerror(errno) << dendl;
        initialized = false;
        return -errno;
      }
    } else if (!S_ISDIR(st.st_mode)) {
      ldout(cct, 0) << "ERROR: Usage cache path is not a directory: " 
                    << db_dir << dendl;
      initialized = false;
      return -ENOTDIR;
    }
  }
  
  // Try to open database
  int ret = open_database();
  
  // Handle corruption with auto-recovery
  if (ret == -MDB_CORRUPTED || ret == -MDB_INVALID || ret == -MDB_BAD_TXN || ret == -MDB_PANIC) {
    ldout(cct, 0) << "UsageCache: Corrupted cache detected (error=" << ret 
                  << "), attempting recovery..." << dendl;
    
    // Delete corrupted database files
    ret = delete_corrupted_database();
    if (ret < 0) {
      ldout(cct, 0) << "UsageCache: Failed to delete corrupted cache files: " 
                    << cpp_strerror(-ret) << dendl;
      // Continue anyway - open_database will create fresh files
    }
    
    // Try to open fresh database
    ret = open_database();
    if (ret < 0) {
      ldout(cct, 0) << "UsageCache: Failed to recreate cache after corruption: " 
                    << cpp_strerror(-ret) << dendl;
      initialized = false;
      return ret;
    }
    
    ldout(cct, 0) << "UsageCache: Successfully recovered from corruption. "
                  << "Cache will be repopulated on next refresh cycle." << dendl;
  } else if (ret < 0) {
    ldout(cct, 0) << "UsageCache: Failed to open database: " 
                  << cpp_strerror(-ret) << dendl;
    initialized = false;
    return ret;
  }
  
  set_counter(PERF_CACHE_SIZE, get_cache_size());
  
  if (cct) {
    ldout(cct, 1) << "UsageCache: Initialized successfully at " 
                  << config.db_path << dendl;
  }
  
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
    if (cct) {
      ldout(cct, 0) << "LMDB env_create failed: " << mdb_strerror(rc) << dendl;
    }
    return -rc;
  }

  rc = mdb_env_set_mapsize(env, config.max_db_size);
  if (rc != 0) {
    if (cct) {
      ldout(cct, 0) << "LMDB set_mapsize failed: " << mdb_strerror(rc) << dendl;
    }
    mdb_env_close(env);
    env = nullptr;
    return -rc;
  }

  rc = mdb_env_set_maxreaders(env, config.max_readers);
  if (rc != 0) {
    if (cct) {
      ldout(cct, 0) << "LMDB set_maxreaders failed: " << mdb_strerror(rc) << dendl;
    }
    mdb_env_close(env);
    env = nullptr;
    return -rc;
  }

  rc = mdb_env_set_maxdbs(env, 2);
  if (rc != 0) {
    if (cct) {
      ldout(cct, 0) << "LMDB set_maxdbs failed: " << mdb_strerror(rc) << dendl;
    }
    mdb_env_close(env);
    env = nullptr;
    return -rc;
  }

  rc = mdb_env_open(env, config.db_path.c_str(), MDB_NOSUBDIR | MDB_NOTLS, 0644);
  if (rc != 0) {
    if (cct) {
      ldout(cct, 0) << "LMDB env_open failed for " << config.db_path 
                    << ": " << mdb_strerror(rc) << dendl;
    }
    mdb_env_close(env);
    env = nullptr;
    return -rc;
  }

  // Open named databases
  MDB_txn* txn = nullptr;
  rc = mdb_txn_begin(env, nullptr, 0, &txn);
  if (rc != 0) {
    if (cct) {
      ldout(cct, 0) << "LMDB txn_begin failed: " << mdb_strerror(rc) << dendl;
    }
    mdb_env_close(env);
    env = nullptr;
    return -rc;
  }

  rc = mdb_dbi_open(txn, "user_stats", MDB_CREATE, &user_dbi);
  if (rc != 0) {
    if (cct) {
      ldout(cct, 0) << "LMDB dbi_open(user_stats) failed: " 
                    << mdb_strerror(rc) << dendl;
    }
    mdb_txn_abort(txn);
    mdb_env_close(env);
    env = nullptr;
    return -rc;
  }

  rc = mdb_dbi_open(txn, "bucket_stats", MDB_CREATE, &bucket_dbi);
  if (rc != 0) {
    if (cct) {
      ldout(cct, 0) << "LMDB dbi_open(bucket_stats) failed: " 
                    << mdb_strerror(rc) << dendl;
    }
    mdb_txn_abort(txn);
    mdb_env_close(env);
    env = nullptr;
    return -rc;
  }

  rc = mdb_txn_commit(txn);
  if (rc != 0) {
    if (cct) {
      ldout(cct, 0) << "LMDB txn_commit failed: " << mdb_strerror(rc) << dendl;
    }
    mdb_env_close(env);
    env = nullptr;
    return -rc;
  }

  if (cct) {
    ldout(cct, 10) << "LMDB database opened successfully: " << config.db_path << dendl;
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
    if (cct) {
      ldout(cct, 5) << "LMDB txn_begin failed in put_stats: " 
                    << mdb_strerror(rc) << dendl;
    }
    return -EIO;
  }
  
  rc = mdb_put(txn, dbi, &mdb_key, &mdb_val, 0);
  if (rc != 0) {
    if (cct) {
      ldout(cct, 5) << "LMDB put failed for key " << key 
                    << ": " << mdb_strerror(rc) << dendl;
    }
    mdb_txn_abort(txn);
    return -EIO;
  }
  
  rc = mdb_txn_commit(txn);
  if (rc != 0) {
    if (cct) {
      ldout(cct, 5) << "LMDB txn_commit failed in put_stats: " 
                    << mdb_strerror(rc) << dendl;
    }
    return -EIO;
  }
  
  return 0;
}

template<typename T>
std::optional<T> UsageCache::get_stats(MDB_dbi dbi, const std::string& key) {
  if (!initialized) {
    return std::nullopt;
  }

  MDB_val mdb_key = {key.size(), const_cast<char*>(key.data())};
  MDB_val mdb_val;
  
  MDB_txn* txn = nullptr;
  int rc = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
  if (rc != 0) {
    if (cct) {
      ldout(cct, 10) << "LMDB txn_begin failed in get_stats: " 
                     << mdb_strerror(rc) << dendl;
    }
    return std::nullopt;
  }
  
  rc = mdb_get(txn, dbi, &mdb_key, &mdb_val);
  mdb_txn_abort(txn);
  
  if (rc != 0) {
    if (rc != MDB_NOTFOUND && cct) {
      ldout(cct, 10) << "LMDB get failed for key " << key 
                     << ": " << mdb_strerror(rc) << dendl;
    }
    return std::nullopt;
  }
  
  bufferlist bl;
  bl.append(static_cast<char*>(mdb_val.mv_data), mdb_val.mv_size);
  
  T stats;
  try {
    auto iter = bl.cbegin();
    stats.decode(iter);
    
    return stats;
  } catch (const buffer::error& e) {
    if (cct) {
      ldout(cct, 5) << "Failed to decode stats for key " << key 
                    << ": " << e.what() << dendl;
    }
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

std::optional<UsageStats> UsageCache::get_user_stats(const std::string& user_id) {
  std::shared_lock lock(db_mutex);
  auto result = get_stats<UsageStats>(user_dbi, user_id);
  
  // Update performance counters
  if (result.has_value()) {
    cache_hits++;
    inc_counter(PERF_CACHE_HIT);
  } else {
    cache_misses++;
    inc_counter(PERF_CACHE_MISS);
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
    if (cct) {
      ldout(cct, 5) << "LMDB txn_begin failed in remove_user_stats: " 
                    << mdb_strerror(rc) << dendl;
    }
    return -EIO;
  }
  
  rc = mdb_del(txn, user_dbi, &mdb_key, nullptr);
  if (rc != 0 && rc != MDB_NOTFOUND) {
    if (cct) {
      ldout(cct, 5) << "LMDB del failed for user " << user_id 
                    << ": " << mdb_strerror(rc) << dendl;
    }
    mdb_txn_abort(txn);
    return -EIO;
  }
  
  rc = mdb_txn_commit(txn);
  if (rc != 0) {
    if (cct) {
      ldout(cct, 5) << "LMDB txn_commit failed in remove_user_stats: " 
                    << mdb_strerror(rc) << dendl;
    }
    return -EIO;
  }

  set_counter(PERF_CACHE_SIZE, get_cache_size_internal());
  
  return 0;
}

int UsageCache::update_bucket_stats(const std::string& bucket_name,
                                    uint64_t bytes_used,
                                    uint64_t num_objects,
                                    const std::string& user_id) {

  // Only store bucket stats
  // User aggregation is done by background thread reading from RADOS
  
  if (!initialized) {
    return -EINVAL;
  }

  std::unique_lock lock(db_mutex);
  UsageStats stats;
  stats.bytes_used = bytes_used;
  stats.num_objects = num_objects;
  stats.last_updated = ceph::real_clock::now();
  
  int ret = put_stats(bucket_dbi, bucket_name, stats);
  if (ret == 0) {
    inc_counter(PERF_CACHE_UPDATE);
    set_counter(PERF_CACHE_SIZE, get_cache_size_internal());
    
    if (cct) {
      ldout(cct, 15) << "Cache updated for bucket " << bucket_name 
                     << " bytes=" << bytes_used 
                     << " objects=" << num_objects << dendl;
    }
  }
  
  // NOTE: User stats are NOT updated here!
  // Background thread reads from RADOS and updates user totals
  
  return ret;
}

std::optional<UsageStats> UsageCache::get_bucket_stats(const std::string& bucket_name) {
  std::shared_lock lock(db_mutex);
  auto result = get_stats<UsageStats>(bucket_dbi, bucket_name);
  
  // Update performance counters
  if (result.has_value()) {
    cache_hits++;
    inc_counter(PERF_CACHE_HIT);
  } else {
    cache_misses++;
    inc_counter(PERF_CACHE_MISS);
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
    if (cct) {
      ldout(cct, 5) << "LMDB txn_begin failed in remove_bucket_stats: " 
                    << mdb_strerror(rc) << dendl;
    }
    return -EIO;
  }
  
  rc = mdb_del(txn, bucket_dbi, &mdb_key, nullptr);
  if (rc != 0 && rc != MDB_NOTFOUND) {
    if (cct) {
      ldout(cct, 5) << "LMDB del failed for bucket " << bucket_name 
                    << ": " << mdb_strerror(rc) << dendl;
    }
    mdb_txn_abort(txn);
    return -EIO;
  }
  
  rc = mdb_txn_commit(txn);
  if (rc != 0) {
    if (cct) {
      ldout(cct, 5) << "LMDB txn_commit failed in remove_bucket_stats: " 
                    << mdb_strerror(rc) << dendl;
    }
    return -EIO;
  }
  
  set_counter(PERF_CACHE_SIZE, get_cache_size_internal());
  
  if (cct) {
    ldout(cct, 10) << "Removed bucket " << bucket_name << " from cache" << dendl;
  }
  
  // NOTE: User stats are NOT updated here!
  // Background thread reads from RADOS and recalculates user totals

  
  return 0;
}

size_t UsageCache::get_cache_size() const {
  if (!initialized) {
    return 0;
  }

  std::shared_lock lock(db_mutex);
  return get_cache_size_internal();
}

size_t UsageCache::get_cache_size_internal() const {
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
  return cache_hits.load();
}

uint64_t UsageCache::get_cache_misses() const {
  return cache_misses.load();
}

uint64_t UsageCache::get_cache_updates() const {
  return perf_counters ? perf_counters->get(PERF_CACHE_UPDATE) : 0;
}

double UsageCache::get_hit_rate() const {
  uint64_t hits = cache_hits.load();
  uint64_t misses = cache_misses.load();
  uint64_t total = hits + misses;
  
  return (total > 0) ? (double)hits / total * 100.0 : 0.0;
}

std::vector<std::pair<std::string, UsageStats>> UsageCache::get_all_users() {
  std::vector<std::pair<std::string, UsageStats>> result;
  
  if (!env) {
    ldout(cct, 5) << "get_all_users: database not initialized" << dendl;
    return result;
  }
  
  MDB_txn* txn = nullptr;
  int rc = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
  if (rc != 0) {
    ldout(cct, 5) << "LMDB txn_begin failed in get_all_users: " 
                  << mdb_strerror(rc) << dendl;
    return result;
  }
  
  MDB_cursor* cursor = nullptr;
  rc = mdb_cursor_open(txn, user_dbi, &cursor);
  if (rc != 0) {
    ldout(cct, 5) << "LMDB cursor_open failed in get_all_users: " 
                  << mdb_strerror(rc) << dendl;
    mdb_txn_abort(txn);
    return result;
  }
  
  MDB_val key, data;
  while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
    std::string user_id((char*)key.mv_data, key.mv_size);
    
    UsageStats stats;
    try {
      bufferlist bl;
      bl.append((char*)data.mv_data, data.mv_size);
      auto iter = bl.cbegin();
      stats.decode(iter);
      
      result.push_back({user_id, stats});
      ldout(cct, 20) << "get_all_users: loaded user=" << user_id 
                     << " bytes=" << stats.bytes_used 
                     << " objects=" << stats.num_objects << dendl;
    } catch (const std::exception& e) {
      ldout(cct, 1) << "Failed to decode user stats for " << user_id 
                    << ": " << e.what() << dendl;
    }
  }
  
  mdb_cursor_close(cursor);
  mdb_txn_abort(txn);
  
  ldout(cct, 10) << "get_all_users: loaded " << result.size() << " users" << dendl;
  return result;
}

std::vector<std::pair<std::string, UsageStats>> UsageCache::get_all_buckets() {
  std::vector<std::pair<std::string, UsageStats>> result;
  
  if (!env) {
    ldout(cct, 5) << "get_all_buckets: database not initialized" << dendl;
    return result;
  }
  
  MDB_txn* txn = nullptr;
  int rc = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
  if (rc != 0) {
    ldout(cct, 5) << "LMDB txn_begin failed in get_all_buckets: " 
                  << mdb_strerror(rc) << dendl;
    return result;
  }
  
  MDB_cursor* cursor = nullptr;
  rc = mdb_cursor_open(txn, bucket_dbi, &cursor);
  if (rc != 0) {
    ldout(cct, 5) << "LMDB cursor_open failed in get_all_buckets: " 
                  << mdb_strerror(rc) << dendl;
    mdb_txn_abort(txn);
    return result;
  }
  
  MDB_val key, data;
  while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
    std::string bucket_key((char*)key.mv_data, key.mv_size);
    
    UsageStats stats;
    try {
      bufferlist bl;
      bl.append((char*)data.mv_data, data.mv_size);
      auto iter = bl.cbegin();
      stats.decode(iter);
      
      result.push_back({bucket_key, stats});
      ldout(cct, 20) << "get_all_buckets: loaded bucket=" << bucket_key 
                     << " bytes=" << stats.bytes_used 
                     << " objects=" << stats.num_objects << dendl;
    } catch (const std::exception& e) {
      ldout(cct, 1) << "Failed to decode bucket stats for " << bucket_key 
                    << ": " << e.what() << dendl;
    }
  }
  
  mdb_cursor_close(cursor);
  mdb_txn_abort(txn);
  
  ldout(cct, 10) << "get_all_buckets: loaded " << result.size() << " buckets" << dendl;
  return result;
}

// Explicit template instantiations
template int UsageCache::put_stats(MDB_dbi, const std::string&, const UsageStats&);
template std::optional<UsageStats> UsageCache::get_stats(MDB_dbi, const std::string&);

} // namespace rgw