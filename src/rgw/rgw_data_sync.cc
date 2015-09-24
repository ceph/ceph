#include "common/ceph_json.h"
#include "common/RWLock.h"
#include "common/RefCountedObj.h"
#include "common/WorkQueue.h"
#include "common/Throttle.h"

#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_sync.h"
#include "rgw_data_sync.h"
#include "rgw_rest_conn.h"
#include "rgw_cr_rados.h"
#include "rgw_cr_rest.h"
#include "rgw_http_client.h"
#include "rgw_bucket.h"
#include "rgw_metadata.h"

#include "cls/lock/cls_lock_client.h"

#include <boost/asio/coroutine.hpp>
#include <boost/asio/yield.hpp>


#define dout_subsys ceph_subsys_rgw

static string datalog_sync_status_oid = "datalog.sync-status";
static string datalog_sync_status_shard_prefix = "datalog.sync-status.shard";
static string datalog_sync_full_sync_index_prefix = "data.full-sync.index";
static string bucket_status_oid_prefix = "bucket.sync-status";

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

class RGWReadRemoteDataLogShardInfoCR : public RGWCoroutine {
  RGWRados *store;
  RGWHTTPManager *http_manager;
  RGWAsyncRadosProcessor *async_rados;

  RGWRESTReadResource *http_op;

  int shard_id;
  RGWDataChangesLogInfo *shard_info;

public:
  RGWReadRemoteDataLogShardInfoCR(RGWRados *_store, RGWHTTPManager *_mgr, RGWAsyncRadosProcessor *_async_rados,
                                                      int _shard_id, RGWDataChangesLogInfo *_shard_info) : RGWCoroutine(_store->ctx()), store(_store),
                                                      http_manager(_mgr),
						      async_rados(_async_rados),
                                                      http_op(NULL),
                                                      shard_id(_shard_id),
                                                      shard_info(_shard_info) {
  }

  int operate() {
    RGWRESTConn *conn = store->rest_master_conn;
    reenter(this) {
      yield {
	char buf[16];
	snprintf(buf, sizeof(buf), "%d", shard_id);
        rgw_http_param_pair pairs[] = { { "type" , "data" },
	                                { "id", buf },
					{ "info" , NULL },
	                                { NULL, NULL } };

        string p = "/admin/log/";

        http_op = new RGWRESTReadResource(conn, p, pairs, NULL, http_manager);

        http_op->set_user_info((void *)stack);

        int ret = http_op->aio_read();
        if (ret < 0) {
          ldout(store->ctx(), 0) << "ERROR: failed to read from " << p << dendl;
          log_error() << "failed to send http operation: " << http_op->to_str() << " ret=" << ret << std::endl;
          http_op->put();
          return set_state(RGWCoroutine_Error, ret);
        }

        return io_block(0);
      }
      yield {
        int ret = http_op->wait(shard_info);
        if (ret < 0) {
          return set_state(RGWCoroutine_Error, ret);
        }
        return set_state(RGWCoroutine_Done, 0);
      }
    }
    return 0;
  }
};

class RGWInitDataSyncStatusCoroutine : public RGWCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;
  RGWHTTPManager *http_manager;
  RGWObjectCtx& obj_ctx;
  string source_zone;

  string lock_name;
  string cookie;
  rgw_data_sync_info status;
  map<int, RGWDataChangesLogInfo> shards_info;
public:
  RGWInitDataSyncStatusCoroutine(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store, RGWHTTPManager *_http_mgr,
		      RGWObjectCtx& _obj_ctx, const string& _source_zone, uint32_t _num_shards) : RGWCoroutine(_store->ctx()), async_rados(_async_rados), store(_store),
                                                http_manager(_http_mgr),
                                                obj_ctx(_obj_ctx), source_zone(_source_zone) {
    lock_name = "sync_lock";
    status.num_shards = _num_shards;

#define COOKIE_LEN 16
    char buf[COOKIE_LEN + 1];

    gen_rand_alphanumeric(cct, buf, sizeof(buf) - 1);
    string cookie = buf;
  }

  int operate() {
    int ret;
    reenter(this) {
      yield {
	uint32_t lock_duration = 30;
	call(new RGWSimpleRadosLockCR(async_rados, store, store->get_zone_params().log_pool, datalog_sync_status_oid,
			             lock_name, cookie, lock_duration));
	if (retcode < 0) {
	  ldout(cct, 0) << "ERROR: failed to take a lock on " << datalog_sync_status_oid << dendl;
	  return set_state(RGWCoroutine_Error, retcode);
	}
      }
      yield {
        call(new RGWSimpleRadosWriteCR<rgw_data_sync_info>(async_rados, store, store->get_zone_params().log_pool,
				 datalog_sync_status_oid, status));
      }
      yield { /* take lock again, we just recreated the object */
	uint32_t lock_duration = 30;
	call(new RGWSimpleRadosLockCR(async_rados, store, store->get_zone_params().log_pool, datalog_sync_status_oid,
			             lock_name, cookie, lock_duration));
	if (retcode < 0) {
	  ldout(cct, 0) << "ERROR: failed to take a lock on " << datalog_sync_status_oid << dendl;
	  return set_state(RGWCoroutine_Error, retcode);
	}
      }
      /* fetch current position in logs */
      yield {
        for (int i = 0; i < (int)status.num_shards; i++) {
          spawn(new RGWReadRemoteDataLogShardInfoCR(store, http_manager, async_rados, i, &shards_info[i]), false);
	}
      }
      while (collect(&ret)) {
	if (ret < 0) {
	  return set_state(RGWCoroutine_Error);
	}
        yield;
      }
      yield {
        for (int i = 0; i < (int)status.num_shards; i++) {
	  rgw_data_sync_marker marker;
	  marker.next_step_marker = shards_info[i].marker;
          spawn(new RGWSimpleRadosWriteCR<rgw_data_sync_marker>(async_rados, store, store->get_zone_params().log_pool,
				                          RGWDataSyncStatusManager::shard_obj_name(source_zone, i), marker), true);
        }
      }
      yield {
	status.state = rgw_data_sync_info::StateBuildingFullSyncMaps;
        call(new RGWSimpleRadosWriteCR<rgw_data_sync_info>(async_rados, store, store->get_zone_params().log_pool,
				 datalog_sync_status_oid, status));
      }
      yield { /* unlock */
	call(new RGWSimpleRadosUnlockCR(async_rados, store, store->get_zone_params().log_pool, datalog_sync_status_oid,
			             lock_name, cookie));
      }
      while (collect(&ret)) {
	if (ret < 0) {
	  return set_state(RGWCoroutine_Error);
	}
        yield;
      }
      return set_state(RGWCoroutine_Done);
    }
    return 0;
  }
};

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

int RGWRemoteDataLog::init_sync_status(int num_shards)
{
  RGWObjectCtx obj_ctx(store, NULL);
  return run(new RGWInitDataSyncStatusCoroutine(async_rados, store, &http_manager, obj_ctx, source_zone, num_shards));
}

int RGWRemoteDataLog::set_sync_info(const rgw_data_sync_info& sync_info)
{
  return run(new RGWSimpleRadosWriteCR<rgw_data_sync_info>(async_rados, store, store->get_zone_params().log_pool,
				 datalog_sync_status_oid, sync_info));
}

class RGWListBucketIndexesCR : public RGWCoroutine {
  RGWRados *store;
  RGWHTTPManager *http_manager;
  RGWAsyncRadosProcessor *async_rados;

  RGWRESTConn *conn;
  string source_zone;
  int num_shards;

  int req_ret;

  list<string> result;

  RGWShardedOmapCRManager *entries_index;

  string oid_prefix;

public:
  RGWListBucketIndexesCR(RGWRados *_store, RGWHTTPManager *_mgr, RGWAsyncRadosProcessor *_async_rados,
                         RGWRESTConn *_conn,
                         const string& _source_zone, int _num_shards) : RGWCoroutine(_store->ctx()), store(_store),
                                                      http_manager(_mgr),
						      async_rados(_async_rados),
						      conn(_conn), source_zone(_source_zone), num_shards(_num_shards),
						      req_ret(0), entries_index(NULL) {
    oid_prefix = datalog_sync_full_sync_index_prefix + "." + source_zone; 
  }
  ~RGWListBucketIndexesCR() {
    delete entries_index;
  }

  int operate() {
    reenter(this) {
      entries_index = new RGWShardedOmapCRManager(async_rados, store, this, num_shards,
						  store->get_zone_params().log_pool,
                                                  oid_prefix);
      yield {
        string entrypoint = string("/admin/metadata/bucket.instance");
#warning need a better scaling solution here, requires streaming output
        call(new RGWReadRESTResourceCR<list<string> >(store->ctx(), conn, http_manager,
                                                      entrypoint, NULL, &result));
      }
      yield {
        if (get_ret_status() < 0) {
          ldout(store->ctx(), 0) << "ERROR: failed to fetch metadata for section bucket.index" << dendl;
          return set_state(RGWCoroutine_Error);
        }
        for (list<string>::iterator iter = result.begin(); iter != result.end(); ++iter) {
          ldout(store->ctx(), 20) << "list metadata: section=bucket.index key=" << *iter << dendl;
          entries_index->append(*iter);
#warning error handling of shards
        }
      }
      yield entries_index->finish();
      int ret;
      while (collect(&ret)) {
	if (ret < 0) {
	  return set_state(RGWCoroutine_Error);
	}
        yield;
      }
      yield return set_state(RGWCoroutine_Done);
    }
    return 0;
  }
};

int RGWRemoteDataLog::run_sync(int num_shards, rgw_data_sync_status& sync_status)
{
  RGWObjectCtx obj_ctx(store, NULL);

  int r = run(new RGWReadDataSyncStatusCoroutine(async_rados, store, obj_ctx, source_zone, &sync_status));
  if (r < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to fetch sync status" << dendl;
    return r;
  }

  switch ((rgw_data_sync_info::SyncState)sync_status.sync_info.state) {
    case rgw_data_sync_info::StateInit:
      ldout(store->ctx(), 20) << __func__ << "(): init" << dendl;
      r = run(new RGWInitDataSyncStatusCoroutine(async_rados, store, &http_manager, obj_ctx, source_zone, num_shards));
      /* fall through */
    case rgw_data_sync_info::StateBuildingFullSyncMaps:
      ldout(store->ctx(), 20) << __func__ << "(): building full sync maps" << dendl;
      r = run(new RGWListBucketIndexesCR(store, &http_manager, async_rados, conn, source_zone, num_shards));
      sync_status.sync_info.state = rgw_data_sync_info::StateSync;
      r = set_sync_info(sync_status.sync_info);
      if (r < 0) {
        ldout(store->ctx(), 0) << "ERROR: failed to update sync status" << dendl;
        return r;
      }
      /* fall through */
    case rgw_data_sync_info::StateSync:
#warning FIXME
      break;
    default:
      ldout(store->ctx(), 0) << "ERROR: bad sync state!" << dendl;
      return -EIO;
  }

  return 0;
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

int RGWRemoteBucketLog::init(const string& _source_zone, RGWRESTConn *_conn, const string& _bucket_name,
                             const string& _bucket_id, int _shard_id)
{
  conn = _conn;
  source_zone = _source_zone;
  bucket_name = _bucket_name;
  bucket_id = _bucket_id;
  shard_id = _shard_id;

  return 0;
}

struct bucket_instance_meta_info {
  string key;
  obj_version ver;
  time_t mtime;
  RGWBucketInstanceMetadataObject data;

  bucket_instance_meta_info() : mtime(0) {}

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("key", key, obj);
    JSONDecoder::decode_json("ver", ver, obj);
    JSONDecoder::decode_json("mtime", mtime, obj);
    JSONDecoder::decode_json("data", data, obj);
  }
};

struct bucket_index_marker_info {
  string bucket_ver;
  string master_ver;
  string max_marker;

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("bucket_ver", bucket_ver, obj);
    JSONDecoder::decode_json("master_ver", master_ver, obj);
    JSONDecoder::decode_json("max_marker", max_marker, obj);
  }
};

class RGWReadRemoteBucketIndexLogInfoCR : public RGWCoroutine {
  RGWRados *store;
  RGWHTTPManager *http_manager;
  RGWAsyncRadosProcessor *async_rados;

  RGWRESTConn *conn;

  string bucket_name;
  string bucket_id;
  int shard_id;

  string instance_key;

  bucket_index_marker_info *info;

public:
  RGWReadRemoteBucketIndexLogInfoCR(RGWRados *_store, RGWHTTPManager *_mgr, RGWAsyncRadosProcessor *_async_rados,
                                  RGWRESTConn *_conn,
                                  const string& _bucket_name, const string& _bucket_id, int _shard_id,
                                  bucket_index_marker_info *_info) : RGWCoroutine(_store->ctx()), store(_store),
                                                      http_manager(_mgr),
						      async_rados(_async_rados),
                                                      conn(_conn),
                                                      bucket_name(_bucket_name), bucket_id(_bucket_id), shard_id(_shard_id),
                                                      info(_info) {
    instance_key = bucket_name + ":" + bucket_id;
    if (shard_id >= 0) {
      char buf[16];
      snprintf(buf, sizeof(buf), ":%d", shard_id);
      instance_key.append(buf);
    }
  }

  int operate() {
    int ret;
    reenter(this) {
      yield {
        rgw_http_param_pair pairs[] = { { "type" , "bucket-index" },
	                                { "bucket-instance", instance_key.c_str() },
					{ "info" , NULL },
	                                { NULL, NULL } };

        string p = "/admin/log/";
        ret = call(new RGWReadRESTResourceCR<bucket_index_marker_info>(store->ctx(), conn, http_manager, p, pairs, info));
        if (ret < 0) {
          return set_state(RGWCoroutine_Error, ret);
        }
      }
      if (retcode < 0) {
        return set_state(RGWCoroutine_Error, retcode);
      }
      return set_state(RGWCoroutine_Done, 0);
    }
    return 0;
  }
};

class RGWReadBucketShardSyncStatusCR : public RGWSimpleRadosReadCR<rgw_bucket_shard_sync_info> {
  map<string, bufferlist> attrs;
public:
  RGWReadBucketShardSyncStatusCR(RGWAsyncRadosProcessor *async_rados, RGWRados *store,
		      RGWObjectCtx& obj_ctx, const string& source_zone,
                      const string& bucket_name, const string bucket_id, int shard_id,
		      rgw_bucket_shard_sync_info *status) : RGWSimpleRadosReadCR(async_rados, store, obj_ctx,
									    store->get_zone_params().log_pool,
									    RGWBucketSyncStatusManager::status_oid(source_zone, bucket_name, bucket_id, shard_id),
                                                                            status) {}

};


class RGWInitBucketShardSyncStatusCoroutine : public RGWCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;
  RGWHTTPManager *http_manager;
  RGWObjectCtx& obj_ctx;
  string source_zone;
  RGWRESTConn *conn;
  string bucket_name;
  string bucket_id;
  int shard_id;

  string sync_status_oid;

  string lock_name;
  string cookie;
  rgw_bucket_shard_sync_info status;

  bucket_index_marker_info info;
public:
  RGWInitBucketShardSyncStatusCoroutine(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store, RGWHTTPManager *_http_mgr,
		      RGWObjectCtx& _obj_ctx, const string& _source_zone, RGWRESTConn *_conn,
                      const string& _bucket_name, const string& _bucket_id, int _shard_id) : RGWCoroutine(_store->ctx()), async_rados(_async_rados), store(_store),
                                                                                             http_manager(_http_mgr),
                                                                                             obj_ctx(_obj_ctx), source_zone(_source_zone), conn(_conn),
                                                                                             bucket_name(_bucket_name), bucket_id(_bucket_id), shard_id(_shard_id) {
    lock_name = "sync_lock";

#define COOKIE_LEN 16
    char buf[COOKIE_LEN + 1];

    gen_rand_alphanumeric(cct, buf, sizeof(buf) - 1);
    string cookie = buf;

    sync_status_oid = RGWBucketSyncStatusManager::status_oid(source_zone, bucket_name, bucket_id, shard_id);
  }

  int operate() {
    int ret;
    reenter(this) {
      yield {
	uint32_t lock_duration = 30;
	call(new RGWSimpleRadosLockCR(async_rados, store, store->get_zone_params().log_pool, sync_status_oid,
			             lock_name, cookie, lock_duration));
	if (retcode < 0) {
	  ldout(cct, 0) << "ERROR: failed to take a lock on " << sync_status_oid << dendl;
	  return set_state(RGWCoroutine_Error, retcode);
	}
      }
      yield {
        call(new RGWSimpleRadosWriteCR<rgw_bucket_shard_sync_info>(async_rados, store, store->get_zone_params().log_pool,
				 sync_status_oid, status));
      }
      yield { /* take lock again, we just recreated the object */
	uint32_t lock_duration = 30;
	call(new RGWSimpleRadosLockCR(async_rados, store, store->get_zone_params().log_pool, sync_status_oid,
			             lock_name, cookie, lock_duration));
	if (retcode < 0) {
	  ldout(cct, 0) << "ERROR: failed to take a lock on " << sync_status_oid << dendl;
	  return set_state(RGWCoroutine_Error, retcode);
	}
      }
      /* fetch current position in logs */
      yield {
        ret = call(new RGWReadRemoteBucketIndexLogInfoCR(store, http_manager, async_rados, conn, bucket_name, bucket_id, shard_id, &info));
        if (ret < 0) {
	  ldout(cct, 0) << "ERROR: failed to fetch bucket index status" << dendl;
          return set_state(RGWCoroutine_Error, ret);
        }
      }
      if (retcode < 0 && retcode != -ENOENT) {
        ldout(cct, 0) << "ERROR: failed to fetch bucket index status" << dendl;
        return set_state(RGWCoroutine_Error, retcode);
      }
      yield {
	status.state = rgw_bucket_shard_sync_info::StateFullSync;
        status.inc_marker.position = info.max_marker;
        map<string, bufferlist> attrs;
        status.encode_all_attrs(attrs);
        call(new RGWSimpleRadosWriteAttrsCR(async_rados, store, store->get_zone_params().log_pool,
                                            sync_status_oid, attrs));
      }
      yield { /* unlock */
	call(new RGWSimpleRadosUnlockCR(async_rados, store, store->get_zone_params().log_pool, sync_status_oid,
			             lock_name, cookie));
      }
      return set_state(RGWCoroutine_Done);
    }
    return 0;
  }
};

RGWCoroutine *RGWRemoteBucketLog::init_sync_status_cr(RGWObjectCtx& obj_ctx)
{
  return new RGWInitBucketShardSyncStatusCoroutine(async_rados, store, http_manager, obj_ctx, source_zone,
                                                   conn, bucket_name, bucket_id, shard_id);
}

template <class T>
static void decode_attr(CephContext *cct, map<string, bufferlist>& attrs, const string& attr_name, T *val)
{
  map<string, bufferlist>::iterator iter = attrs.find(attr_name);
  if (iter == attrs.end()) {
    *val = T();
    return;
  }

  bufferlist::iterator biter = iter->second.begin();
  try {
    ::decode(*val, biter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode attribute: " << attr_name << dendl;
  }
}

void rgw_bucket_shard_sync_info::decode_from_attrs(CephContext *cct, map<string, bufferlist>& attrs)
{
  decode_attr(cct, attrs, "state", &state);
  decode_attr(cct, attrs, "full_marker", &full_marker);
  decode_attr(cct, attrs, "inc_marker", &inc_marker);
}

void rgw_bucket_shard_sync_info::encode_all_attrs(map<string, bufferlist>& attrs)
{
  encode_state_attr(attrs);
  full_marker.encode_attr(attrs);
  inc_marker.encode_attr(attrs);
}

void rgw_bucket_shard_sync_info::encode_state_attr(map<string, bufferlist>& attrs)
{
  ::encode(state, attrs["state"]);
}

void rgw_bucket_shard_full_sync_marker::encode_attr(map<string, bufferlist>& attrs)
{
  ::encode(*this, attrs["full_marker"]);
}

void rgw_bucket_shard_inc_sync_marker::encode_attr(map<string, bufferlist>& attrs)
{
  ::encode(*this, attrs["inc_marker"]);
}

class RGWReadBucketSyncStatusCoroutine : public RGWCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;
  RGWObjectCtx& obj_ctx;
  string oid;
  rgw_bucket_shard_sync_info *status;

  map<string, bufferlist> attrs;
public:
  RGWReadBucketSyncStatusCoroutine(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store,
		      RGWObjectCtx& _obj_ctx, const string& _source_zone,
                      const string& _bucket_name, const string _bucket_id, int _shard_id,
		      rgw_bucket_shard_sync_info *_status) : RGWCoroutine(_store->ctx()),
                                                            async_rados(_async_rados),
                                                            store(_store),
                                                            obj_ctx(_obj_ctx),
                                                            oid(RGWBucketSyncStatusManager::status_oid(_source_zone, _bucket_name, _bucket_id, _shard_id)),
                                                            status(_status) {}
  int operate();
};

int RGWReadBucketSyncStatusCoroutine::operate()
{
  reenter(this) {
    yield {
      int ret = call(new RGWSimpleRadosReadAttrsCR(async_rados, store, obj_ctx,
                                                   store->get_zone_params().log_pool,
                                                   oid,
                                                   &attrs));
      if (ret < 0) {
        ldout(store->ctx(), 0) << "ERROR: failed to call new RGWSimpleRadosReadAttrsCR() ret=" << ret << dendl;
        return set_state(RGWCoroutine_Error, ret);
      }
    }
    if (retcode == -ENOENT) {
      *status = rgw_bucket_shard_sync_info();
      return set_state(RGWCoroutine_Done, 0);
    }
    if (retcode < 0) {
      ldout(store->ctx(), 0) << "ERROR: failed to call fetch bucket shard info oid=" << oid << " ret=" << retcode << dendl;
      return set_state(RGWCoroutine_Error, retcode);
    }
    status->decode_from_attrs(store->ctx(), attrs);
    return set_state(RGWCoroutine_Done, 0);
  }
  return 0;
}
RGWCoroutine *RGWRemoteBucketLog::read_sync_status_cr(RGWObjectCtx& obj_ctx, rgw_bucket_shard_sync_info *sync_status)
{
  return new RGWReadBucketSyncStatusCoroutine(async_rados, store, obj_ctx, source_zone,
                                              bucket_name, bucket_id, shard_id, sync_status);
}

RGWBucketSyncStatusManager::~RGWBucketSyncStatusManager() {
  for (map<int, RGWRemoteBucketLog *>::iterator iter = source_logs.begin(); iter != source_logs.end(); ++iter) {
    delete iter->second;
  }
}


struct bucket_entry_owner {
  string id;
  string display_name;

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("ID", id, obj);
    JSONDecoder::decode_json("DisplayName", display_name, obj);
  }
};

struct bucket_list_entry {
  bool delete_marker;
  rgw_obj_key key;
  bool is_latest;
  utime_t mtime;
  string etag;
  uint64_t size;
  string storage_class;
  bucket_entry_owner owner;
  uint64_t versioned_epoch;
  string rgw_tag;

  bucket_list_entry() : delete_marker(false), is_latest(false), size(0), versioned_epoch(0) {}

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("IsDeleteMarker", delete_marker, obj);
    JSONDecoder::decode_json("Key", key.name, obj);
    JSONDecoder::decode_json("VersionId", key.instance, obj);
    JSONDecoder::decode_json("IsLatest", is_latest, obj);
    string mtime_str;
    JSONDecoder::decode_json("LastModified", mtime_str, obj);

    struct tm t;
    if (parse_iso8601(mtime_str.c_str(), &t)) {
      time_t sec = timegm(&t);
#warning more high def clock?
      mtime = utime_t(sec, 0);
    }
    JSONDecoder::decode_json("ETag", etag, obj);
    JSONDecoder::decode_json("Size", size, obj);
    JSONDecoder::decode_json("StorageClass", storage_class, obj);
    JSONDecoder::decode_json("Owner", owner, obj);
    JSONDecoder::decode_json("VersionedEpoch", versioned_epoch, obj);
    JSONDecoder::decode_json("RgwxTag", rgw_tag, obj);
  }
};

struct bucket_list_result {
  string name;
  string prefix;
  string key_marker;
  string version_id_marker;
  int max_keys;
  bool is_truncated;
  list<bucket_list_entry> entries;

  bucket_list_result() : max_keys(0), is_truncated(false) {}

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("Name", name, obj);
    JSONDecoder::decode_json("Prefix", prefix, obj);
    JSONDecoder::decode_json("KeyMarker", key_marker, obj);
    JSONDecoder::decode_json("VersionIdMarker", version_id_marker, obj);
    JSONDecoder::decode_json("MaxKeys", max_keys, obj);
    JSONDecoder::decode_json("IsTruncated", is_truncated, obj);
    JSONDecoder::decode_json("Entries", entries, obj);
  }
};

class RGWListBucketShardCR: public RGWCoroutine {
  RGWRados *store;
  RGWHTTPManager *http_manager;
  RGWAsyncRadosProcessor *async_rados;

  RGWRESTConn *conn;

  string bucket_name;
  string bucket_id;
  int shard_id;

  string instance_key;
  rgw_obj_key marker_position;

  bucket_list_result *result;

public:
  RGWListBucketShardCR(RGWRados *_store, RGWHTTPManager *_mgr, RGWAsyncRadosProcessor *_async_rados,
                                  RGWRESTConn *_conn,
                                  const string& _bucket_name, const string& _bucket_id, int _shard_id,
                                  rgw_obj_key& _marker_position,
                                  bucket_list_result *_result) : RGWCoroutine(_store->ctx()), store(_store),
                                                      http_manager(_mgr),
						      async_rados(_async_rados),
                                                      conn(_conn),
                                                      bucket_name(_bucket_name), bucket_id(_bucket_id), shard_id(_shard_id),
                                                      marker_position(_marker_position),
                                                      result(_result) {
    instance_key = bucket_name + ":" + bucket_id;
    if (shard_id >= 0) {
      char buf[16];
      snprintf(buf, sizeof(buf), ":%d", shard_id);
      instance_key.append(buf);
    }
  }

  int operate() {
    int ret;
    reenter(this) {
      yield {
        rgw_http_param_pair pairs[] = { { "rgwx-bucket-instance", instance_key.c_str() },
					{ "versions" , NULL },
					{ "format" , "json" },
					{ "objs-container" , "true" },
					{ "key-marker" , marker_position.name.c_str() },
					{ "version-id-marker" , marker_position.instance.c_str() },
	                                { NULL, NULL } };

        string p = string("/") + bucket_name;
        ret = call(new RGWReadRESTResourceCR<bucket_list_result>(store->ctx(), conn, http_manager, p, pairs, result));
        if (ret < 0) {
          return set_state(RGWCoroutine_Error, ret);
        }
      }
      if (retcode < 0) {
        return set_state(RGWCoroutine_Error, retcode);
      }
      return set_state(RGWCoroutine_Done, 0);
    }
    return 0;
  }
};

class RGWListBucketIndexLogCR: public RGWCoroutine {
  RGWRados *store;
  RGWHTTPManager *http_manager;
  RGWAsyncRadosProcessor *async_rados;

  RGWRESTConn *conn;

  string bucket_name;
  string bucket_id;
  int shard_id;

  string instance_key;
  string marker;

  list<rgw_bi_log_entry> *result;

public:
  RGWListBucketIndexLogCR(RGWRados *_store, RGWHTTPManager *_mgr, RGWAsyncRadosProcessor *_async_rados,
                                  RGWRESTConn *_conn,
                                  const string& _bucket_name, const string& _bucket_id, int _shard_id,
                                  string& _marker,
                                  list<rgw_bi_log_entry> *_result) : RGWCoroutine(_store->ctx()), store(_store),
                                                      http_manager(_mgr),
						      async_rados(_async_rados),
                                                      conn(_conn),
                                                      bucket_name(_bucket_name), bucket_id(_bucket_id), shard_id(_shard_id),
                                                      marker(_marker),
                                                      result(_result) {
    instance_key = bucket_name + ":" + bucket_id;
    if (shard_id >= 0) {
      char buf[16];
      snprintf(buf, sizeof(buf), ":%d", shard_id);
      instance_key.append(buf);
    }
  }

  int operate() {
    int ret;
    reenter(this) {
      yield {
        rgw_http_param_pair pairs[] = { { "bucket-instance", instance_key.c_str() },
					{ "format" , "json" },
					{ "marker" , marker.c_str() },
					{ "type", "bucket-index" },
	                                { NULL, NULL } };

        ret = call(new RGWReadRESTResourceCR<list<rgw_bi_log_entry> >(store->ctx(), conn, http_manager, "/admin/log", pairs, result));
        if (ret < 0) {
          return set_state(RGWCoroutine_Error, ret);
        }
      }
      if (retcode < 0) {
        return set_state(RGWCoroutine_Error, retcode);
      }
      return set_state(RGWCoroutine_Done, 0);
    }
    return 0;
  }
};

#define BUCKET_SYNC_UPDATE_MARKER_WINDOW 10

class RGWBucketFullSyncShardMarkerTrack : public RGWSyncShardMarkerTrack<rgw_obj_key> {
  RGWRados *store;
  RGWAsyncRadosProcessor *async_rados;

  string marker_oid;
  rgw_bucket_shard_full_sync_marker sync_marker;


public:
  RGWBucketFullSyncShardMarkerTrack(RGWRados *_store, RGWAsyncRadosProcessor *_async_rados,
                         const string& _marker_oid,
                         const rgw_bucket_shard_full_sync_marker& _marker) : RGWSyncShardMarkerTrack(BUCKET_SYNC_UPDATE_MARKER_WINDOW),
                                                                store(_store),
                                                                async_rados(_async_rados),
                                                                marker_oid(_marker_oid),
                                                                sync_marker(_marker) {}

  RGWCoroutine *store_marker(const rgw_obj_key& new_marker) {
    sync_marker.position = new_marker;

    map<string, bufferlist> attrs;
    sync_marker.encode_attr(attrs);

    ldout(store->ctx(), 20) << __func__ << "(): updating marker marker_oid=" << marker_oid << " marker=" << new_marker << dendl;
    return new RGWSimpleRadosWriteAttrsCR(async_rados, store, store->get_zone_params().log_pool,
				 marker_oid, attrs);
  }
};

class RGWBucketIncSyncShardMarkerTrack : public RGWSyncShardMarkerTrack<string> {
  RGWRados *store;
  RGWAsyncRadosProcessor *async_rados;

  string marker_oid;
  rgw_bucket_shard_inc_sync_marker sync_marker;


public:
  RGWBucketIncSyncShardMarkerTrack(RGWRados *_store, RGWAsyncRadosProcessor *_async_rados,
                         const string& _marker_oid,
                         const rgw_bucket_shard_inc_sync_marker& _marker) : RGWSyncShardMarkerTrack(BUCKET_SYNC_UPDATE_MARKER_WINDOW),
                                                                store(_store),
                                                                async_rados(_async_rados),
                                                                marker_oid(_marker_oid),
                                                                sync_marker(_marker) {}

  RGWCoroutine *store_marker(const string& new_marker) {
    sync_marker.position = new_marker;

    map<string, bufferlist> attrs;
    sync_marker.encode_attr(attrs);

    ldout(store->ctx(), 20) << __func__ << "(): updating marker marker_oid=" << marker_oid << " marker=" << new_marker << dendl;
    return new RGWSimpleRadosWriteAttrsCR(async_rados, store, store->get_zone_params().log_pool,
				 marker_oid, attrs);
  }
};

template <class T>
class RGWBucketSyncSingleEntryCR : public RGWCoroutine {
  RGWRados *store;
  RGWAsyncRadosProcessor *async_rados;

  string source_zone;
  RGWBucketInfo *bucket_info;
  int shard_id;

  rgw_obj_key key;
  uint64_t versioned_epoch;

  T entry_marker;
  RGWSyncShardMarkerTrack<T> *marker_tracker;

  int sync_status;


public:
  RGWBucketSyncSingleEntryCR(RGWRados *_store, RGWAsyncRadosProcessor *_async_rados,
                             const string& _source_zone, RGWBucketInfo *_bucket_info, int _shard_id,
                             const rgw_obj_key& _key, uint64_t _versioned_epoch,
		             const T& _entry_marker, RGWSyncShardMarkerTrack<T> *_marker_tracker) : RGWCoroutine(_store->ctx()), store(_store),
						      async_rados(_async_rados),
						      source_zone(_source_zone),
                                                      bucket_info(_bucket_info), shard_id(_shard_id),
                                                      key(_key),
                                                      entry_marker(_entry_marker),
                                                      marker_tracker(_marker_tracker) {

  }

  int operate() {
    reenter(this) {
      yield {
        int r = call(new RGWFetchRemoteObjCR(async_rados, store, source_zone, *bucket_info,
                                             key, versioned_epoch,
                                             true));
        if (r < 0) {
          ldout(store->ctx(), 0) << "ERROR: failed to call RGWFetchRemoteObjCR()" << dendl;
          return r;
        }
      }
      if (retcode < 0 && retcode != -ENOENT) {
        rgw_bucket& bucket = bucket_info->bucket;
        ldout(store->ctx(), 0) << "ERROR: failed to sync object: " << bucket.name << ":" << bucket.bucket_id << ":" << shard_id << "/" << key << dendl;
        sync_status = retcode;
      }
      yield {
        /* update marker */
        int ret = call(marker_tracker->finish(entry_marker));
        if (ret < 0) {
          ldout(store->ctx(), 0) << "ERROR: marker_tracker->finish(" << entry_marker << ") returned ret=" << ret << dendl;
          return set_state(RGWCoroutine_Error, sync_status);
        }
      }
      if (sync_status == 0) {
        sync_status = retcode;
      }
      if (sync_status < 0) {
        return set_state(RGWCoroutine_Error, sync_status);
      }
      return set_state(RGWCoroutine_Done, 0);
    }
    return 0;
  }
};

#define BUCKET_SYNC_SPAWN_WINDOW 20

class RGWBucketShardFullSyncCR : public RGWCoroutine {
  RGWHTTPManager *http_manager;
  RGWAsyncRadosProcessor *async_rados;
  RGWRESTConn *conn;
  RGWRados *store;
  string source_zone;
  string bucket_name;
  string bucket_id;
  int shard_id;
  RGWBucketInfo *bucket_info;
  bucket_list_result list_result;
  list<bucket_list_entry>::iterator entries_iter;
  rgw_bucket_shard_full_sync_marker full_marker;
  RGWBucketFullSyncShardMarkerTrack *marker_tracker;
  int spawn_window;
  int pending;
  rgw_obj_key list_marker;

public:
  RGWBucketShardFullSyncCR(RGWHTTPManager *_mgr, RGWAsyncRadosProcessor *_async_rados,
                           RGWRESTConn *_conn, RGWRados *_store,
                           const string& _source_zone,
                           const string& _bucket_name, const string _bucket_id, int _shard_id,
                           RGWBucketInfo *_bucket_info,  rgw_bucket_shard_full_sync_marker& _full_marker) : RGWCoroutine(_store->ctx()),
                                                                            http_manager(_mgr), async_rados(_async_rados), conn(_conn),
                                                                            store(_store),
									    source_zone(_source_zone),
                                                                            bucket_name(_bucket_name),
									    bucket_id(_bucket_id), shard_id(_shard_id),
                                                                            bucket_info(_bucket_info),
                                                                            full_marker(_full_marker), marker_tracker(NULL),
                                                                            spawn_window(BUCKET_SYNC_SPAWN_WINDOW), pending(0) {}

  ~RGWBucketShardFullSyncCR() {
    delete marker_tracker;
  }
  int operate();
};

int RGWBucketShardFullSyncCR::operate()
{
  int ret;
  reenter(this) {
    list_marker = full_marker.position;
    marker_tracker = new RGWBucketFullSyncShardMarkerTrack(store, async_rados, 
                                                           RGWBucketSyncStatusManager::status_oid(source_zone, bucket_name, bucket_id, shard_id),
                                                           full_marker);
    do {
      yield {
        ldout(store->ctx(), 20) << __func__ << "(): listing bucket for full sync" << dendl;
        int r = call(new RGWListBucketShardCR(store, http_manager, async_rados, conn, bucket_name, bucket_id, shard_id,
                                              list_marker, &list_result));
        if (r < 0) {
          ldout(store->ctx(), 0) << "ERROR: failed to call new CR (RGWListBucketShardCR)" << dendl;
          return r;
        }
      }
      if (retcode < 0 && retcode != -ENOENT) {
        return set_state(RGWCoroutine_Error, retcode);
      }
      entries_iter = list_result.entries.begin();
      for (; entries_iter != list_result.entries.end(); ++entries_iter) {
        ldout(store->ctx(), 20) << "[full sync] syncing object: " << bucket_name << ":" << bucket_id << ":" << shard_id << "/" << entries_iter->key << dendl;
        yield {
          bucket_list_entry& entry = *entries_iter;
          ++pending;
          marker_tracker->start(entry.key);
          list_marker = entry.key;
          spawn(new RGWBucketSyncSingleEntryCR<rgw_obj_key>(store, async_rados, source_zone, bucket_info, shard_id,
                                               entry.key, entry.versioned_epoch, entry.key, marker_tracker), false);
        }
        while (pending > spawn_window &&
               collect(&ret)) {
          --pending;
          if (ret < 0) {
            ldout(store->ctx(), 0) << "ERROR: a sync operation returned error" << dendl;
            /* we should have reported this error */
#warning deal with error
          }
          yield;
        }
      }
    } while (list_result.is_truncated);
    /* wait for all operations to complete */
    while (collect(&ret)) {
      if (ret < 0) {
        ldout(store->ctx(), 0) << "ERROR: a sync operation returned error" << dendl;
        /* we should have reported this error */
#warning deal with error
      }
      yield;
    }
    /* update sync state to incremental */
    yield {
      rgw_bucket_shard_sync_info sync_status;
      sync_status.state = rgw_bucket_shard_sync_info::StateIncrementalSync;
      map<string, bufferlist> attrs;
      sync_status.encode_state_attr(attrs);
      string oid = RGWBucketSyncStatusManager::status_oid(source_zone, bucket_name, bucket_id, shard_id);
      int ret = call(new RGWSimpleRadosWriteAttrsCR(async_rados, store, store->get_zone_params().log_pool,
                                                    oid, attrs));
      if (ret < 0) {
        ldout(store->ctx(), 0) << "ERROR: failed to call RGWSimpleRadosWriteAttrsCR() oid=" << oid << dendl;
        return set_state(RGWCoroutine_Error, ret);
      }
    }
    if (retcode < 0) {
      ldout(store->ctx(), 0) << "ERROR: failed to set sync state on bucket " << bucket_name << ":" << bucket_id << ":" << shard_id
        << " retcode=" << retcode << dendl;
      return set_state(RGWCoroutine_Error, retcode);
    }
  }
  return 0;
}

class RGWBucketShardIncrementalSyncCR : public RGWCoroutine {
  RGWHTTPManager *http_manager;
  RGWAsyncRadosProcessor *async_rados;
  RGWRESTConn *conn;
  RGWRados *store;
  string source_zone;
  string bucket_name;
  string bucket_id;
  int shard_id;
  RGWBucketInfo *bucket_info;
  list<rgw_bi_log_entry> list_result;
  list<rgw_bi_log_entry>::iterator entries_iter;
  rgw_bucket_shard_inc_sync_marker inc_marker;
  RGWBucketIncSyncShardMarkerTrack *marker_tracker;
  int spawn_window;
  int pending;

public:
  RGWBucketShardIncrementalSyncCR(RGWHTTPManager *_mgr, RGWAsyncRadosProcessor *_async_rados,
                           RGWRESTConn *_conn, RGWRados *_store,
                           const string& _source_zone,
                           const string& _bucket_name, const string _bucket_id, int _shard_id,
                           RGWBucketInfo *_bucket_info, rgw_bucket_shard_inc_sync_marker& _inc_marker) : RGWCoroutine(_store->ctx()),
                                                                            http_manager(_mgr), async_rados(_async_rados), conn(_conn),
                                                                            store(_store),
									    source_zone(_source_zone),
                                                                            bucket_name(_bucket_name),
									    bucket_id(_bucket_id), shard_id(_shard_id),
                                                                            bucket_info(_bucket_info),
                                                                            inc_marker(_inc_marker), marker_tracker(NULL),
                                                                            spawn_window(BUCKET_SYNC_SPAWN_WINDOW), pending(0) {}

  ~RGWBucketShardIncrementalSyncCR() {
    delete marker_tracker;
  }
  int operate();
};

int RGWBucketShardIncrementalSyncCR::operate()
{
  int ret;
  reenter(this) {
    marker_tracker = new RGWBucketIncSyncShardMarkerTrack(store, async_rados, 
                                                          RGWBucketSyncStatusManager::status_oid(source_zone, bucket_name, bucket_id, shard_id),
                                                          inc_marker);
    do {
      yield {
        ldout(store->ctx(), 20) << __func__ << "(): listing bilog for incremental sync" << dendl;
        int r = call(new RGWListBucketIndexLogCR(store, http_manager, async_rados, conn, bucket_name, bucket_id, shard_id,
                                              inc_marker.position, &list_result));
        if (r < 0) {
          ldout(store->ctx(), 0) << "ERROR: failed to call new CR (RGWListBucketShardCR)" << dendl;
          return r;
        }
      }
      if (retcode < 0 && retcode != -ENOENT) {
        return set_state(RGWCoroutine_Error, retcode);
      }
      entries_iter = list_result.begin();
      for (; entries_iter != list_result.end(); ++entries_iter) {
        yield {
          rgw_obj_key key(entries_iter->object, entries_iter->instance);
          ldout(store->ctx(), 20) << "[inc sync] syncing object: " << bucket_name << ":" << bucket_id << ":" << shard_id << "/" << key << dendl;
          rgw_bi_log_entry& entry = *entries_iter;
          ++pending;
          marker_tracker->start(entry.id);
          inc_marker.position = entry.id;
          uint64_t versioned_epoch = 0;
          if (entry.ver.pool < 0) {
            versioned_epoch = entry.ver.epoch;
          }
          spawn(new RGWBucketSyncSingleEntryCR<string>(store, async_rados, source_zone, bucket_info, shard_id,
                                               key, versioned_epoch, entry.id, marker_tracker), false);
        }
        while (pending > spawn_window &&
               collect(&ret)) {
          --pending;
          if (ret < 0) {
            ldout(store->ctx(), 0) << "ERROR: a sync operation returned error" << dendl;
            /* we should have reported this error */
#warning deal with error
          }
          yield;
        }
      }
    } while (!list_result.empty());
    /* wait for all operations to complete */
    while (collect(&ret)) {
      if (ret < 0) {
        ldout(store->ctx(), 0) << "ERROR: a sync operation returned error" << dendl;
        /* we should have reported this error */
#warning deal with error
      }
      yield;
    }
  }
  return 0;
}

class RGWRunBucketSyncCoroutine : public RGWCoroutine {
  RGWHTTPManager *http_manager;
  RGWAsyncRadosProcessor *async_rados;
  RGWRESTConn *conn;
  RGWRados *store;
  RGWObjectCtx& obj_ctx;
  string source_zone;
  string bucket_name;
  string bucket_id;
  RGWBucketInfo bucket_info;
  int shard_id;
  rgw_bucket_shard_sync_info sync_status;

public:
  RGWRunBucketSyncCoroutine(RGWHTTPManager *_mgr, RGWAsyncRadosProcessor *_async_rados,
                            RGWRESTConn *_conn, RGWRados *_store,
                            RGWObjectCtx& _obj_ctx, const string& _source_zone,
                            const string& _bucket_name, const string _bucket_id, int _shard_id) : RGWCoroutine(_store->ctx()),
                                                                            http_manager(_mgr), async_rados(_async_rados), conn(_conn),
                                                                            store(_store),
									    obj_ctx(_obj_ctx), source_zone(_source_zone),
                                                                            bucket_name(_bucket_name),
									    bucket_id(_bucket_id), shard_id(_shard_id) {}

  int operate();
};


int RGWRunBucketSyncCoroutine::operate()
{
  reenter(this) {
    yield {
      int r = call(new RGWReadBucketSyncStatusCoroutine(async_rados, store, obj_ctx, source_zone, bucket_name, bucket_id, shard_id, &sync_status));
      if (r < 0) {
        ldout(store->ctx(), 0) << "ERROR: failed to fetch sync status" << dendl;
        return r;
      }
    }

    if (retcode < 0 && retcode != -ENOENT) {
      ldout(store->ctx(), 0) << "ERROR: failed to read sync status for bucket=" << bucket_name << " bucket_id=" << bucket_id << " shard_id=" << shard_id << dendl;
      return set_state(RGWCoroutine_Error, retcode);
    }

    yield {
      int r = call(new RGWGetBucketInstanceInfoCR(async_rados, store, bucket_name, bucket_id, &bucket_info));
      if (r < 0) {
        ldout(store->ctx(), 0) << "ERROR: failed to fetch sync status" << dendl;
        return r;
      }
    }

    if (retcode < 0) {
      ldout(store->ctx(), 0) << "ERROR: failed to retrieve bucket info for bucket=" << bucket_name << " bucket_id=" << bucket_id << dendl;
      return set_state(RGWCoroutine_Error, retcode);
    }

    if ((rgw_bucket_shard_sync_info::SyncState)sync_status.state == rgw_bucket_shard_sync_info::StateFullSync) {
      int r = call(new RGWBucketShardFullSyncCR(http_manager, async_rados, conn, store,
                                                source_zone, bucket_name, bucket_id, shard_id,
                                                &bucket_info, sync_status.full_marker));
      if (r < 0) {
        ldout(store->ctx(), 0) << "ERROR: failed to fetch sync status" << dendl;
        return r;
      }
    }

    if (retcode < 0) {
      ldout(store->ctx(), 0) << "ERROR: full sync on " << bucket_name << " bucket_id=" << bucket_id << " shard_id=" << shard_id << " failed, retcode=" << retcode << dendl;
      return set_state(RGWCoroutine_Error, retcode);
    }

    if ((rgw_bucket_shard_sync_info::SyncState)sync_status.state == rgw_bucket_shard_sync_info::StateIncrementalSync) {
      int r = call(new RGWBucketShardIncrementalSyncCR(http_manager, async_rados, conn, store,
                                                source_zone, bucket_name, bucket_id, shard_id,
                                                &bucket_info, sync_status.inc_marker));
      if (r < 0) {
        ldout(store->ctx(), 0) << "ERROR: failed to fetch sync status" << dendl;
        return r;
      }
    }

  }

  return 0;
}

RGWCoroutine *RGWRemoteBucketLog::run_sync_cr(RGWObjectCtx& obj_ctx)
{
  return new RGWRunBucketSyncCoroutine(http_manager, async_rados, conn, store, obj_ctx, source_zone, bucket_name, bucket_id, shard_id);
}

int RGWBucketSyncStatusManager::init()
{
  map<string, RGWRESTConn *>::iterator iter = store->zone_conn_map.find(source_zone);
  if (iter == store->zone_conn_map.end()) {
    lderr(store->ctx()) << "no REST connection to master zone" << dendl;
    return -EIO;
  }

  conn = iter->second;

  async_rados = new RGWAsyncRadosProcessor(store, store->ctx()->_conf->rgw_num_async_rados_threads);
  async_rados->start();

  int ret = http_manager.set_threaded();
  if (ret < 0) {
    ldout(store->ctx(), 0) << "failed in http_manager.set_threaded() ret=" << ret << dendl;
    return ret;
  }


  string key = bucket_name + ":" + bucket_id;

  rgw_http_param_pair pairs[] = { { "key", key.c_str() },
                                  { NULL, NULL } };

  string path = string("/admin/metadata/bucket.instance");

  bucket_instance_meta_info result;
  ret = cr_mgr.run(new RGWReadRESTResourceCR<bucket_instance_meta_info>(store->ctx(), conn, &http_manager, path, pairs, &result));
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to fetch bucket metadata info from zone=" << source_zone << " path=" << path << " key=" << key << " ret=" << ret << dendl;
    return ret;
  }

  RGWBucketInfo& bi = result.data.get_bucket_info();
  num_shards = bi.num_shards;


  int effective_num_shards = (num_shards ? num_shards : 1);

  for (int i = 0; i < effective_num_shards; i++) {
    RGWRemoteBucketLog *l = new RGWRemoteBucketLog(store, this, async_rados, &http_manager);
    ret = l->init(source_zone, conn, bucket_name, bucket_id, (num_shards ? i : -1));
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: failed to initialize RGWRemoteBucketLog object" << dendl;
      return ret;
    }
    source_logs[i] = l;
  }

  return 0;
}

int RGWBucketSyncStatusManager::init_sync_status()
{
  RGWObjectCtx obj_ctx(store);

  list<RGWCoroutinesStack *> stacks;

  for (map<int, RGWRemoteBucketLog *>::iterator iter = source_logs.begin(); iter != source_logs.end(); ++iter) {
    RGWCoroutinesStack *stack = new RGWCoroutinesStack(store->ctx(), &cr_mgr);
    RGWRemoteBucketLog *l = iter->second;
    int r = stack->call(l->init_sync_status_cr(obj_ctx));
    if (r < 0) {
      ldout(store->ctx(), 0) << "ERROR: failed to init sync status for " << bucket_name << ":" << bucket_id << ":" << iter->first << dendl;
    }

    stacks.push_back(stack);
  }

  return cr_mgr.run(stacks);
}

int RGWBucketSyncStatusManager::read_sync_status()
{
  RGWObjectCtx obj_ctx(store);

  list<RGWCoroutinesStack *> stacks;

  for (map<int, RGWRemoteBucketLog *>::iterator iter = source_logs.begin(); iter != source_logs.end(); ++iter) {
    RGWCoroutinesStack *stack = new RGWCoroutinesStack(store->ctx(), &cr_mgr);
    RGWRemoteBucketLog *l = iter->second;
    int r = stack->call(l->read_sync_status_cr(obj_ctx, &sync_status[iter->first]));
    if (r < 0) {
      ldout(store->ctx(), 0) << "ERROR: failed to read sync status for " << bucket_name << ":" << bucket_id << ":" << iter->first << dendl;
    }

    stacks.push_back(stack);
  }

  int ret = cr_mgr.run(stacks);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to read sync status for " << bucket_name << ":" << bucket_id << dendl;
    return ret;
  }

  return 0;
}

int RGWBucketSyncStatusManager::run()
{
  RGWObjectCtx obj_ctx(store);

  list<RGWCoroutinesStack *> stacks;

  for (map<int, RGWRemoteBucketLog *>::iterator iter = source_logs.begin(); iter != source_logs.end(); ++iter) {
    RGWCoroutinesStack *stack = new RGWCoroutinesStack(store->ctx(), &cr_mgr);
    RGWRemoteBucketLog *l = iter->second;
    int r = stack->call(l->run_sync_cr(obj_ctx));
    if (r < 0) {
      ldout(store->ctx(), 0) << "ERROR: failed to read sync status for " << bucket_name << ":" << bucket_id << ":" << iter->first << dendl;
    }

    stacks.push_back(stack);
  }

  int ret = cr_mgr.run(stacks);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to read sync status for " << bucket_name << ":" << bucket_id << dendl;
    return ret;
  }

  return 0;
}

string RGWBucketSyncStatusManager::status_oid(const string& source_zone, const string& bucket_name, const string& bucket_id, int shard_id)
{
  string oid = bucket_status_oid_prefix + "." + source_zone + ":" + bucket_name + ":" + bucket_id;
  if (shard_id >= 0) {
    char buf[16];
    snprintf(buf, sizeof(buf), ":%d", shard_id);
    oid.append(buf);
  }
  return oid;
}

