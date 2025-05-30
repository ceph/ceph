#include "rgw_exporter.h"
#include "rgw_common.h"
#include "rgw_user.h"
#include "rgw_bucket.h"
#include "common/ceph_context.h"
#include "common/perf_counters.h"
#include <sys/stat.h>
#include <unistd.h>
#include <thread>
#include <chrono>
#include <lmdb.h>

RGWExporter *g_rgw_exporter = nullptr;

RGWExporter::RGWExporter(CephContext *cct_, RGWRados *store_)
  : cct(cct_), store(store_), env(nullptr), env_open(false), usage_updater(nullptr) {}

RGWExporter::~RGWExporter() {
  stop();
}

void RGWExporter::start() {
  std::string base_dir = cct->_conf->rgw_posix_data;
  if (base_dir.empty()) base_dir = "/var/lib/ceph/radosgw";
  std::string fsid = store->getRadosHandle() ? store->getRadosHandle()->get_fsid() : "default";
  std::string db_path = base_dir + "/" + fsid + "/rgw_usage";
  ::mkdir((base_dir + "/" + fsid).c_str(), 0755);
  ::mkdir(db_path.c_str(), 0755);

  int rc = mdb_env_create(&env);
  if (rc != 0) return;
  mdb_env_set_mapsize(env, 128 * 1024 * 1024);
  mdb_env_set_maxdbs(env, 2);
  rc = mdb_env_open(env, db_path.c_str(), 0, 0664);
  if (rc != 0) {
    mdb_env_close(env);
    env = nullptr;
    return;
  }
  MDB_txn *txn;
  rc = mdb_txn_begin(env, nullptr, 0, &txn);
  if (rc == 0) {
    mdb_dbi_open(txn, "user_usage", MDB_CREATE, &user_dbi);
    mdb_dbi_open(txn, "bucket_usage", MDB_CREATE, &bucket_dbi);
    mdb_txn_commit(txn);
    env_open = true;
  }

  uint64_t interval = cct->_conf->rgw_usage_update_interval;
  if (interval == 0) interval = 43200; // default 12h
  usage_updater = new std::thread(&RGWExporter::run_updater, this, interval);
}

void RGWExporter::stop() {
  if (usage_updater) {
    stop_flag.store(true);
    usage_updater->join();
    delete usage_updater;
    usage_updater = nullptr;
  }
  if (env) {
    mdb_dbi_close(env, user_dbi);
    mdb_dbi_close(env, bucket_dbi);
    mdb_env_close(env);
    env = nullptr;
    env_open = false;
  }
}

void RGWExporter::run_updater(uint64_t interval_sec) {
  while (!stop_flag.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(interval_sec));
    if (stop_flag.load()) break;
    update_usage();
  }
}

void RGWExporter::update_usage() {
  if (!env_open || !env) return;
  MDB_txn *txn;
  int rc = mdb_txn_begin(env, NULL, 0, &txn);
  if (rc != 0) return;

  std::vector<std::string> users;
  if (store->list_users(users) < 0) {
    mdb_txn_abort(txn);
    return;
  }

  for (const auto& uid : users) {
    RGWUserInfo user_info;
    if (store->get_user_info(uid, &user_info, true) < 0) continue;
    uint64_t num_objs = user_info.stats.num_objects;
    uint64_t num_bytes = user_info.stats.size_actual;
    MDB_val key, val;
    key.mv_data = (void*)uid.data(); key.mv_size = uid.size();
    uint64_t data[2] = {num_objs, num_bytes};
    val.mv_data = data; val.mv_size = sizeof(data);
    mdb_put(txn, user_dbi, &key, &val, 0);
    // update perf counter
    // perf->set(..., num_objs, {"user", uid}); etc.
  }

  std::vector<std::string> buckets;
  if (store->list_all_buckets(buckets) < 0) {
    mdb_txn_abort(txn);
    return;
  }

  for (const auto& bucket_name : buckets) {
    RGWBucketInfo info;
    if (store->get_bucket_info(bucket_name, &info, true) < 0) continue;
    std::string key_str = info.bucket.name;
    MDB_val key, val;
    key.mv_data = (void*)key_str.data(); key.mv_size = key_str.size();
    uint64_t data[2] = {info.stats.num_objects, info.stats.size_actual};
    val.mv_data = data; val.mv_size = sizeof(data);
    mdb_put(txn, bucket_dbi, &key, &val, 0);
    // update perf counter
  }

  mdb_txn_commit(txn);
}
