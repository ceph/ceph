#include "rgw_usage_cache.h"
#include <iostream>
#include <cstring>

RGWUsageCache::RGWUsageCache(const std::string &lmdb_path)
  : env(nullptr), dbi(0), lmdb_path(lmdb_path)
{
  if (!init_lmdb()) {
    std::cerr << "RGWUsageCache: Failed to initialize LMDB at " << lmdb_path << std::endl;
  }
}

RGWUsageCache::~RGWUsageCache() {
  flush_to_lmdb();
  close_lmdb();
}

bool RGWUsageCache::init_lmdb() {
  int rc = mdb_env_create(&env);
  if (rc != MDB_SUCCESS) {
    std::cerr << "LMDB env create failed: " << mdb_strerror(rc) << std::endl;
    return false;
  }
  // Set a map size (similar to bucket cache settings)
  rc = mdb_env_set_mapsize(env, 104857600); // 100 MB
  if (rc != MDB_SUCCESS) {
    std::cerr << "LMDB set mapsize failed: " << mdb_strerror(rc) << std::endl;
    return false;
  }
  rc = mdb_env_open(env, lmdb_path.c_str(), MDB_NOSUBDIR, 0664);
  if (rc != MDB_SUCCESS) {
    std::cerr << "LMDB env open failed: " << mdb_strerror(rc) << std::endl;
    return false;
  }
  MDB_txn *txn;
  rc = mdb_txn_begin(env, nullptr, 0, &txn);
  if (rc != MDB_SUCCESS) {
    std::cerr << "LMDB txn begin failed: " << mdb_strerror(rc) << std::endl;
    return false;
  }
  rc = mdb_dbi_open(txn, nullptr, MDB_CREATE, &dbi);
  if (rc != MDB_SUCCESS) {
    std::cerr << "LMDB dbi open failed: " << mdb_strerror(rc) << std::endl;
    mdb_txn_abort(txn);
    return false;
  }
  rc = mdb_txn_commit(txn);
  if (rc != MDB_SUCCESS) {
    std::cerr << "LMDB txn commit failed: " << mdb_strerror(rc) << std::endl;
    return false;
  }
  return true;
}

void RGWUsageCache::close_lmdb() {
  if (env) {
    mdb_dbi_close(env, dbi);
    mdb_env_close(env);
    env = nullptr;
  }
}

void RGWUsageCache::update_bucket_usage(const std::string &bucket, uint64_t bytes_used) {
  std::lock_guard<std::mutex> lock(cache_mutex);
  bucket_usage_cache[bucket] = bytes_used;
}

void RGWUsageCache::update_user_usage(const std::string &user, uint64_t bytes_used) {
  std::lock_guard<std::mutex> lock(cache_mutex);
  user_usage_cache[user] = bytes_used;
}

uint64_t RGWUsageCache::get_bucket_usage(const std::string &bucket) {
  std::lock_guard<std::mutex> lock(cache_mutex);
  auto it = bucket_usage_cache.find(bucket);
  return (it != bucket_usage_cache.end() ? it->second : 0);
}

uint64_t RGWUsageCache::get_user_usage(const std::string &user) {
  std::lock_guard<std::mutex> lock(cache_mutex);
  auto it = user_usage_cache.find(user);
  return (it != user_usage_cache.end() ? it->second : 0);
}

bool RGWUsageCache::flush_to_lmdb() {
  std::lock_guard<std::mutex> lock(cache_mutex);
  MDB_txn *txn;
  int rc = mdb_txn_begin(env, nullptr, 0, &txn);
  if (rc != MDB_SUCCESS) {
    std::cerr << "Flush: MDB txn begin failed: " << mdb_strerror(rc) << std::endl;
    return false;
  }
  // Flush bucket usage metrics
  for (const auto &entry : bucket_usage_cache) {
    MDB_val key, data;
    key.mv_size = entry.first.size();
    key.mv_data = const_cast<char*>(entry.first.c_str());
    data.mv_size = sizeof(uint64_t);
    data.mv_data = const_cast<uint64_t*>(&(entry.second));
    rc = mdb_put(txn, dbi, &key, &data, 0);
    if (rc != MDB_SUCCESS) {
      std::cerr << "Flush: MDB put failed for bucket " << entry.first << ": " << mdb_strerror(rc) << std::endl;
      mdb_txn_abort(txn);
      return false;
    }
  }
  // Flush user usage metrics
  for (const auto &entry : user_usage_cache) {
    MDB_val key, data;
    key.mv_size = entry.first.size();
    key.mv_data = const_cast<char*>(entry.first.c_str());
    data.mv_size = sizeof(uint64_t);
    data.mv_data = const_cast<uint64_t*>(&(entry.second));
    rc = mdb_put(txn, dbi, &key, &data, 0);
    if (rc != MDB_SUCCESS) {
      std::cerr << "Flush: MDB put failed for user " << entry.first << ": " << mdb_strerror(rc) << std::endl;
      mdb_txn_abort(txn);
      return false;
    }
  }
  rc = mdb_txn_commit(txn);
  if (rc != MDB_SUCCESS) {
    std::cerr << "Flush: MDB txn commit failed: " << mdb_strerror(rc) << std::endl;
    return false;
  }
  return true;
}
