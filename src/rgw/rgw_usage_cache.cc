#include "rgw_usage_cache.h"
#include "include/ceph_assert.h"

RGWUsageCache::RGWUsageCache(CephContext *cct_, const std::string& path)
    : cct(cct_), db_path(path), env(nullptr), dbi(0) {}

RGWUsageCache::~RGWUsageCache() {
    close();
}

int RGWUsageCache::init() {
    // Create LMDB environment
    int ret = mdb_env_create(&env);
    if (ret != 0) {
        ldout(cct, 0) << "ERROR: failed to create LMDB env for usage cache: " << mdb_strerror(ret) << dendl;
        return ret;
    }
    // Set a reasonable mapsize (e.g., 128 MB) for usage data
    mdb_env_set_mapsize(env, 128ULL * 1024 * 1024);
    // Open environment (create directory if needed)
    ret = mdb_env_open(env, db_path.c_str(), MDB_NOSUBDIR|MDB_NOSYNC, 0700);
    if (ret != 0) {
        ldout(cct, 0) << "ERROR: failed to open LMDB env at " << db_path << ": " << mdb_strerror(ret) << dendl;
        mdb_env_close(env);
        env = nullptr;
        return ret;
    }
    // Begin write txn to open database
    MDB_txn *txn;
    mdb_txn_begin(env, NULL, 0, &txn);
    ret = mdb_dbi_open(txn, NULL, MDB_CREATE, &dbi);
    if (ret != 0) {
        ldout(cct, 0) << "ERROR: mdb_dbi_open failed: " << mdb_strerror(ret) << dendl;
        mdb_txn_abort(txn);
        mdb_env_close(env);
        env = nullptr;
        return ret;
    }
    mdb_txn_commit(txn);
    ldout(cct, 20) << "RGWUsageCache: LMDB environment opened at " << db_path << dendl;
    return 0;
}

void RGWUsageCache::close() {
    if (env) {
        // ensure all transactions are finished (single writer pattern)
        mdb_dbi_close(env, dbi);
        mdb_env_close(env);
        env = nullptr;
    }
}

int RGWUsageCache::clear() {
    ceph_assert(env);
    MDB_txn *txn;
    int ret = mdb_txn_begin(env, NULL, 0, &txn);
    if (ret != 0) return ret;
    // Drop all entries from the database
    ret = mdb_drop(txn, dbi, /*del*/0);
    if (ret != 0) {
        mdb_txn_abort(txn);
        return ret;
    }
    ret = mdb_txn_commit(txn);
    return ret;
}

int RGWUsageCache::put_bucket_usage(const std::string& bucket_key, const RGWUsageStats& stats) {
    ceph_assert(env);
    MDB_val key, val;
    key.mv_data = (void*)bucket_key.c_str();
    key.mv_size = bucket_key.size();
    // Value is two 64-bit integers
    uint64_t data[2] = { stats.used_bytes, stats.num_objects };
    val.mv_data = data;
    val.mv_size = sizeof(data);
    MDB_txn *txn;
    int ret = mdb_txn_begin(env, NULL, 0, &txn);
    if (ret == 0) {
        ret = mdb_put(txn, dbi, &key, &val, 0);
        if (ret == 0) {
            ret = mdb_txn_commit(txn);
        } else {
            mdb_txn_abort(txn);
        }
    }
    return ret;
}

int RGWUsageCache::put_user_usage(const std::string& user_key, const RGWUsageStats& stats) {
    // Format user key with "user:" prefix for consistency
    std::string full_key = user_key.rfind("user:", 0) == 0 ? user_key : ("user:" + user_key);
    return put_bucket_usage(full_key, stats);
}

bool RGWUsageCache::get_bucket_usage(const std::string& bucket_key, RGWUsageStats *out) {
    ceph_assert(env);
    MDB_txn *txn;
    if (mdb_txn_begin(env, NULL, MDB_RDONLY, &txn) != 0) {
        return false;
    }
    MDB_val key, val;
    key.mv_data = (void*)bucket_key.c_str();
    key.mv_size = bucket_key.size();
    int ret = mdb_get(txn, dbi, &key, &val);
    if (ret == 0 && val.mv_size == 2 * sizeof(uint64_t)) {
        uint64_t *data = (uint64_t*)val.mv_data;
        out->used_bytes = data[0];
        out->num_objects = data[1];
        mdb_txn_abort(txn);
        return true;
    }
    mdb_txn_abort(txn);
    return false;
}

bool RGWUsageCache::get_user_usage(const std::string& user_key, RGWUsageStats *out) {
    std::string full_key = user_key.rfind("user:", 0) == 0 ? user_key : ("user:" + user_key);
    return get_bucket_usage(full_key, out);
}
