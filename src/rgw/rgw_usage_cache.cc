#include "rgw_usage_cache.h"

#include <cstring>

RGWUsageCache::RGWUsageCache(const std::string& path) {
  mdb_env_create(&env);
  mdb_env_set_maxdbs(env, 2);
  mdb_env_open(env, path.c_str(), MDB_NOSUBDIR, 0664);
  MDB_txn* txn;
  mdb_txn_begin(env, nullptr, 0, &txn);
  mdb_dbi_open(txn, "user", MDB_CREATE, &user_dbi);
  mdb_dbi_open(txn, "bucket", MDB_CREATE, &bucket_dbi);
  mdb_txn_commit(txn);
}

RGWUsageCache::~RGWUsageCache() {
  if (env) {
    mdb_dbi_close(env, user_dbi);
    mdb_dbi_close(env, bucket_dbi);
    mdb_env_close(env);
  }
}

int RGWUsageCache::put_user(const std::string& user, const RGWUsageRecord& record) {
  MDB_txn* txn;
  int r = mdb_txn_begin(env, nullptr, 0, &txn);
  if (r != MDB_SUCCESS) return r;
  MDB_val key{user.size(), const_cast<char*>(user.data())};
  MDB_val val{sizeof(record), const_cast<RGWUsageRecord*>(&record)};
  r = mdb_put(txn, user_dbi, &key, &val, 0);
  if (r == MDB_SUCCESS) r = mdb_txn_commit(txn); else mdb_txn_abort(txn);
  return r;
}

int RGWUsageCache::get_user(const std::string& user, RGWUsageRecord* record) {
  MDB_txn* txn;
  int r = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
  if (r != MDB_SUCCESS) return r;
  MDB_val key{user.size(), const_cast<char*>(user.data())};
  MDB_val val;
  r = mdb_get(txn, user_dbi, &key, &val);
  if (r == MDB_SUCCESS && val.mv_size == sizeof(RGWUsageRecord)) {
    std::memcpy(record, val.mv_data, sizeof(RGWUsageRecord));
  }
  mdb_txn_abort(txn);
  return r;
}

int RGWUsageCache::put_bucket(const std::string& bucket, const RGWUsageRecord& record) {
  MDB_txn* txn;
  int r = mdb_txn_begin(env, nullptr, 0, &txn);
  if (r != MDB_SUCCESS) return r;
  MDB_val key{bucket.size(), const_cast<char*>(bucket.data())};
  MDB_val val{sizeof(record), const_cast<RGWUsageRecord*>(&record)};
  r = mdb_put(txn, bucket_dbi, &key, &val, 0);
  if (r == MDB_SUCCESS) r = mdb_txn_commit(txn); else mdb_txn_abort(txn);
  return r;
}

int RGWUsageCache::get_bucket(const std::string& bucket, RGWUsageRecord* record) {
  MDB_txn* txn;
  int r = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
  if (r != MDB_SUCCESS) return r;
  MDB_val key{bucket.size(), const_cast<char*>(bucket.data())};
  MDB_val val;
  r = mdb_get(txn, bucket_dbi, &key, &val);
  if (r == MDB_SUCCESS && val.mv_size == sizeof(RGWUsageRecord)) {
    std::memcpy(record, val.mv_data, sizeof(RGWUsageRecord));
  }
  mdb_txn_abort(txn);
  return r;
}

