#include "rgw_usage_cache.h"
#include "include/ceph_assert.h"

namespace {
  std::string format_user_key(const std::string& key) {
    return key.rfind("user:", 0) == 0 ? key : "user:" + key;
  }
}

RGWUsageCache::RGWUsageCache(CephContext *cct_, const std::string& path)
    : cct(cct_), db_path(path), env(nullptr), dbi(0) {}

RGWUsageCache::~RGWUsageCache() {
    close();
}

int RGWUsageCache::init() {
    int ret = mdb_env_create(&env);
    if (ret != 0) {
        ldout(cct, 0) << "ERROR: failed to create LMDB env: " << mdb_strerror(ret) << dendl;
        return ret;
    }

    mdb_env_set_mapsize(env, 128ULL * 1024 * 1024);

    ret = mdb_env_open(env, db_path.c_str(), MDB_NOSUBDIR, 0700);
    if (ret != 0) {
        ldout(cct, 0) << "ERROR: failed to open LMDB env at " << db_path << ": " << mdb_strerror(ret) << dendl;
        mdb_env_close(env);
        env = nullptr;
        return ret;
    }

    MDB_txn *txn;
    ret = mdb_txn_begin(env, nullptr, 0, &txn);
    if (ret != 0) {
        ldout(cct, 0) << "ERROR: mdb_txn_begin failed: " << mdb_strerror(ret) << dendl;
        mdb_env_close(env);
        env = nullptr;
        return ret;
    }

    ret = mdb_dbi_open(txn, nullptr, MDB_CREATE, &dbi);
    if (ret != 0) {
        ldout(cct, 0) << "ERROR: mdb_dbi_open failed: " << mdb_strerror(ret) << dendl;
        mdb_txn_abort(txn);
        mdb_env_close(env);
        env = nullptr;
        return ret;
    }

    ret = mdb_txn_commit(txn);
    if (ret != 0) {
        ldout(cct, 0) << "ERROR: mdb_txn_commit failed: " << mdb_strerror(ret) << dendl;
        mdb_env_close(env);
        env = nullptr;
        return ret;
    }

    ldout(cct, 20) << "RGWUsageCache: LMDB environment initialized at " << db_path << dendl;
    return 0;
}

void RGWUsageCache::close() {
    if (env) {
        mdb_dbi_close(env, dbi);
        mdb_env_close(env);
        env = nullptr;
    }
}

int RGWUsageCache::clear() {
    ceph_assert(env);
    MDB_txn *txn;
    int ret = mdb_txn_begin(env, nullptr, 0, &txn);
    if (ret != 0) {
        ldout(cct, 0) << "ERROR: mdb_txn_begin (clear) failed: " << mdb_strerror(ret) << dendl;
        return ret;
    }

    ret = mdb_drop(txn, dbi, 0);
    if (ret != 0) {
        ldout(cct, 0) << "ERROR: mdb_drop failed: " << mdb_strerror(ret) << dendl;
        mdb_txn_abort(txn);
        return ret;
    }

    ret = mdb_txn_commit(txn);
    if (ret != 0) {
        ldout(cct, 0) << "ERROR: mdb_txn_commit (clear) failed: " << mdb_strerror(ret) << dendl;
    }
    return ret;
}

int RGWUsageCache::put_bucket_stats(const std::string& bucket_key, const RGWUsageStats& stats) {
    ceph_assert(env);
    MDB_val key, val;
    key.mv_data = (void*)bucket_key.c_str();
    key.mv_size = bucket_key.size();
    uint64_t data[2] = { stats.used_bytes, stats.num_objects };
    val.mv_data = data;
    val.mv_size = sizeof(data);

    std::lock_guard<std::mutex> lock(write_lock);
    MDB_txn *txn;
    int ret = mdb_txn_begin(env, nullptr, 0, &txn);
    if (ret != 0) {
        ldout(cct, 0) << "ERROR: mdb_txn_begin (put) failed: " << mdb_strerror(ret) << dendl;
        return ret;
    }

    ret = mdb_put(txn, dbi, &key, &val, 0);
    if (ret != 0) {
        ldout(cct, 0) << "ERROR: mdb_put failed for key " << bucket_key << ": " << mdb_strerror(ret) << dendl;
        mdb_txn_abort(txn);
        return ret;
    }

    ret = mdb_txn_commit(txn);
    if (ret != 0) {
        ldout(cct, 0) << "ERROR: mdb_txn_commit failed for key " << bucket_key << ": " << mdb_strerror(ret) << dendl;
    }

    return ret;
}

int RGWUsageCache::put_user_stats(const std::string& user_key, const RGWUsageStats& stats) {
    return put_bucket_stats(format_user_key(user_key), stats);
}

bool RGWUsageCache::get_bucket_stats(const std::string& bucket_key, RGWUsageStats *out) {
    ceph_assert(env);
    ceph_assert(out);
    MDB_txn *txn;

    int ret = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
    if (ret != 0) {
        ldout(cct, 0) << "ERROR: mdb_txn_begin (get) failed: " << mdb_strerror(ret) << dendl;
        return false;
    }

    MDB_val key, val;
    key.mv_data = (void*)bucket_key.c_str();
    key.mv_size = bucket_key.size();

    ret = mdb_get(txn, dbi, &key, &val);
    if (ret == 0 && val.mv_size == sizeof(uint64_t) * 2) {
        uint64_t *data = static_cast<uint64_t*>(val.mv_data);
        out->used_bytes = data[0];
        out->num_objects = data[1];
        mdb_txn_abort(txn);
        return true;
    } else if (ret != MDB_NOTFOUND) {
        ldout(cct, 0) << "ERROR: mdb_get failed for key " << bucket_key << ": " << mdb_strerror(ret) << dendl;
    }

    mdb_txn_abort(txn);
    return false;
}

bool RGWUsageCache::get_user_stats(const std::string& user_key, RGWUsageStats *out) {
    return get_bucket_stats(format_user_key(user_key), out);
}
