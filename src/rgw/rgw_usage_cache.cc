#include "rgw_usage_cache.h"
#include <sys/stat.h>
#include <cstring>

struct usage_record {
  uint64_t bytes;
  uint64_t objects;
};

RGWUsageCache::RGWUsageCache(const std::string& path)
{
  mkdir(path.c_str(), 0755);
  mdb_env_create(&env);
  mdb_env_set_maxdbs(env, 2);
  mdb_env_set_mapsize(env, 1024*1024*10);
  mdb_env_open(env, path.c_str(), 0, 0644);
  MDB_txn* txn;
  mdb_txn_begin(env, nullptr, 0, &txn);
  mdb_dbi_open(txn, "users", MDB_CREATE, &user_dbi);
  mdb_dbi_open(txn, "buckets", MDB_CREATE, &bucket_dbi);
  mdb_txn_commit(txn);
}

RGWUsageCache::~RGWUsageCache()
{
  if (env) {
    mdb_dbi_close(env, user_dbi);
    mdb_dbi_close(env, bucket_dbi);
    mdb_env_close(env);
  }
}

static int put(MDB_env* env, MDB_dbi dbi, const std::string& key, uint64_t bytes, uint64_t objects)
{
  MDB_txn* txn;
  int r = mdb_txn_begin(env, nullptr, 0, &txn);
  if (r != 0) return r;

  usage_record rec{bytes, objects};

  MDB_val k;
  k.mv_data = (void*)key.data();
  k.mv_size = key.size();

  MDB_val v;
  v.mv_data = &rec;
  v.mv_size = sizeof(rec);

  r = mdb_put(txn, dbi, &k, &v, 0);
  if (r == 0)
    r = mdb_txn_commit(txn);
  else
    mdb_txn_abort(txn);

  return r;
}

static bool get(MDB_env* env, MDB_dbi dbi, const std::string& key, uint64_t& bytes, uint64_t& objects)
{
  MDB_txn* txn;
  if (mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn) != 0)
    return false;

  MDB_val k;
  k.mv_data = (void*)key.data();
  k.mv_size = key.size();

  MDB_val v;
  int r = mdb_get(txn, dbi, &k, &v);
  if (r == 0 && v.mv_size == sizeof(usage_record)) {
    usage_record rec;
    std::memcpy(&rec, v.mv_data, sizeof(rec));
    bytes = rec.bytes;
    objects = rec.objects;
  }

  mdb_txn_abort(txn);
  return r == 0;
}

int RGWUsageCache::put_user(const std::string& id, uint64_t b, uint64_t o)
{
  return put(env, user_dbi, id, b, o);
}

int RGWUsageCache::put_bucket(const std::string& name, uint64_t b, uint64_t o)
{
  return put(env, bucket_dbi, name, b, o);
}

bool RGWUsageCache::get_user(const std::string& id, uint64_t& b, uint64_t& o)
{
  return get(env, user_dbi, id, b, o);
}

bool RGWUsageCache::get_bucket(const std::string& name, uint64_t& b, uint64_t& o)
{
  return get(env, bucket_dbi, name, b, o);
}

