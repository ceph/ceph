#include "common/ceph_json.h"
#include "common/RWLock.h"
#include "common/RefCountedObj.h"
#include "common/WorkQueue.h"
#include "common/Throttle.h"

#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_data_sync.h"
#include "rgw_rest_conn.h"
#include "rgw_cr_rados.h"
#include "rgw_cr_rest.h"
#include "rgw_http_client.h"
#include "rgw_bucket.h"

#include "cls/lock/cls_lock_client.h"

#include <boost/asio/coroutine.hpp>
#include <boost/asio/yield.hpp>


#define dout_subsys ceph_subsys_rgw

static string datalog_sync_status_oid = "datalog.sync-status";
static string datalog_sync_status_shard_prefix = "datalog.sync-status.shard";
static string datalog_sync_full_sync_index_prefix = "data.full-sync.index";

void rgw_datalog_info::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("num_objects", num_shards, obj);
}

struct rgw_datalog_entry {
  string key;
  utime_t timestamp;

  void decode_json(JSONObj *obj);
};

struct rgw_datalog_shard_data {
  string marker;
  bool truncated;
  vector<rgw_datalog_entry> entries;

  void decode_json(JSONObj *obj);
};


void rgw_datalog_entry::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("key", key, obj);
  JSONDecoder::decode_json("timestamp", timestamp, obj);
}

void rgw_datalog_shard_data::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("marker", marker, obj);
  JSONDecoder::decode_json("truncated", truncated, obj);
  JSONDecoder::decode_json("entries", entries, obj);
};

class RGWReadDataSyncStatusCoroutine : public RGWSimpleRadosReadCR<rgw_data_sync_info> {
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;
  RGWObjectCtx& obj_ctx;

  string source_zone;

  rgw_data_sync_status *sync_status;

public:
  RGWReadDataSyncStatusCoroutine(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store,
		      RGWObjectCtx& _obj_ctx, const string& _source_zone,
		      rgw_data_sync_status *_status) : RGWSimpleRadosReadCR(_async_rados, _store, _obj_ctx,
									    _store->get_zone_params().log_pool,
									    datalog_sync_status_oid,
									    &_status->sync_info),
                                                                            async_rados(_async_rados), store(_store),
                                                                            obj_ctx(_obj_ctx), source_zone(_source_zone),
									    sync_status(_status) {}

  int handle_data(rgw_data_sync_info& data);
};

int RGWReadDataSyncStatusCoroutine::handle_data(rgw_data_sync_info& data)
{
  if (retcode == -ENOENT) {
    return retcode;
  }

  map<uint32_t, rgw_data_sync_marker>& markers = sync_status->sync_markers;
  for (int i = 0; i < (int)data.num_shards; i++) {
    spawn(new RGWSimpleRadosReadCR<rgw_data_sync_marker>(async_rados, store, obj_ctx, store->get_zone_params().log_pool,
				                    RGWDataSyncStatusManager::shard_obj_name(source_zone, i), &markers[i]), true);
  }
  return 0;
}

int RGWRemoteDataLog::read_log_info(rgw_datalog_info *log_info)
{
  rgw_http_param_pair pairs[] = { { "type", "data" },
                                  { NULL, NULL } };

  int ret = conn->get_json_resource("/admin/log", pairs, *log_info);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to fetch datalog info" << dendl;
    return ret;
  }

  ldout(store->ctx(), 20) << "remote datalog, num_shards=" << log_info->num_shards << dendl;

  return 0;
}

int RGWRemoteDataLog::init(const string& _source_zone, RGWRESTConn *_conn)
{
  CephContext *cct = store->ctx();
  async_rados = new RGWAsyncRadosProcessor(store, cct->_conf->rgw_num_async_rados_threads);
  async_rados->start();

  conn = _conn;
  source_zone = _source_zone;

  int ret = http_manager.set_threaded();
  if (ret < 0) {
    ldout(store->ctx(), 0) << "failed in http_manager.set_threaded() ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

void RGWRemoteDataLog::finish()
{
  stop();
  if (async_rados) {
    async_rados->stop();
  }
  delete async_rados;
}

int RGWRemoteDataLog::list_shards(int num_shards)
{
  for (int i = 0; i < (int)num_shards; i++) {
    int ret = list_shard(i);
    if (ret < 0) {
      ldout(store->ctx(), 10) << "failed to list shard: ret=" << ret << dendl;
    }
  }

  return 0;
}

int RGWRemoteDataLog::list_shard(int shard_id)
{
  conn = store->rest_master_conn;

  char buf[32];
  snprintf(buf, sizeof(buf), "%d", shard_id);

  rgw_http_param_pair pairs[] = { { "type", "data" },
                                  { "id", buf },
                                  { NULL, NULL } };

  rgw_datalog_shard_data data;
  int ret = conn->get_json_resource("/admin/log", pairs, data);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to fetch datalog data" << dendl;
    return ret;
  }

  ldout(store->ctx(), 20) << "remote datalog, shard_id=" << shard_id << " num of shard entries: " << data.entries.size() << dendl;

  vector<rgw_datalog_entry>::iterator iter;
  for (iter = data.entries.begin(); iter != data.entries.end(); ++iter) {
    rgw_datalog_entry& entry = *iter;
    ldout(store->ctx(), 20) << "entry: key=" << entry.key << dendl;
  }

  return 0;
}

int RGWRemoteDataLog::get_shard_info(int shard_id)
{
  conn = store->rest_master_conn;

  char buf[32];
  snprintf(buf, sizeof(buf), "%d", shard_id);

  rgw_http_param_pair pairs[] = { { "type", "data" },
                                  { "id", buf },
                                  { "info", NULL },
                                  { NULL, NULL } };

  RGWDataChangesLogInfo info;
  int ret = conn->get_json_resource("/admin/log", pairs, info);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to fetch datalog info" << dendl;
    return ret;
  }

  ldout(store->ctx(), 20) << "remote datalog, shard_id=" << shard_id << " marker=" << info.marker << dendl;

  return 0;
}

int RGWRemoteDataLog::read_sync_status(rgw_data_sync_status *sync_status)
{
  RGWObjectCtx obj_ctx(store, NULL);
  return run(new RGWReadDataSyncStatusCoroutine(async_rados, store, obj_ctx, source_zone, sync_status));
}

int RGWDataSyncStatusManager::init()
{
  map<string, RGWRESTConn *>::iterator iter = store->zone_conn_map.find(source_zone);
  if (iter == store->zone_conn_map.end()) {
    lderr(store->ctx()) << "no REST connection to master zone" << dendl;
    return -EIO;
  }

  conn = iter->second;

  const char *log_pool = store->get_zone_params().log_pool.name.c_str();
  librados::Rados *rados = store->get_rados_handle();
  int r = rados->ioctx_create(log_pool, ioctx);
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: failed to open log pool (" << store->get_zone_params().log_pool.name << " ret=" << r << dendl;
    return r;
  }

  source_status_obj = rgw_obj(store->get_zone_params().log_pool, datalog_sync_status_oid);

  r = source_log.init(source_zone, conn);
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: failed to init remote log, r=" << r << dendl;
    return r;
  }

  rgw_datalog_info datalog_info;
  r = source_log.read_log_info(&datalog_info);
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: master.read_log_info() returned r=" << r << dendl;
    return r;
  }

  num_shards = datalog_info.num_shards;

  for (int i = 0; i < num_shards; i++) {
    shard_objs[i] = rgw_obj(store->get_zone_params().log_pool, shard_obj_name(source_zone, i));
  }

  return 0;
}

string RGWDataSyncStatusManager::shard_obj_name(const string& source_zone, int shard_id)
{
  char buf[datalog_sync_status_shard_prefix.size() + source_zone.size() + 16];
  snprintf(buf, sizeof(buf), "%s.%s.%d", datalog_sync_status_shard_prefix.c_str(), source_zone.c_str(), shard_id);

  return string(buf);
}


