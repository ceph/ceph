#include "common/ceph_json.h"
#include "common/RWLock.h"
#include "common/RefCountedObj.h"
#include "common/WorkQueue.h"
#include "common/Throttle.h"
#include "common/errno.h"

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
#include "rgw_boost_asio_yield.h"

#include "cls/lock/cls_lock_client.h"

#define dout_subsys ceph_subsys_rgw

static string datalog_sync_status_oid_prefix = "datalog.sync-status";
static string datalog_sync_status_shard_prefix = "datalog.sync-status.shard";
static string datalog_sync_full_sync_index_prefix = "data.full-sync.index";
static string bucket_status_oid_prefix = "bucket.sync-status";

void rgw_datalog_info::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("num_objects", num_shards, obj);
}

void rgw_datalog_entry::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("key", key, obj);
  utime_t ut;
  JSONDecoder::decode_json("timestamp", ut, obj);
  timestamp = ut.to_real_time();
}

void rgw_datalog_shard_data::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("marker", marker, obj);
  JSONDecoder::decode_json("truncated", truncated, obj);
  JSONDecoder::decode_json("entries", entries, obj);
};

class RGWReadDataSyncStatusCoroutine : public RGWSimpleRadosReadCR<rgw_data_sync_info> {
  RGWDataSyncEnv *sync_env;

  RGWObjectCtx& obj_ctx;

  rgw_data_sync_status *sync_status;

public:
  RGWReadDataSyncStatusCoroutine(RGWDataSyncEnv *_sync_env, RGWObjectCtx& _obj_ctx,
		      rgw_data_sync_status *_status) : RGWSimpleRadosReadCR(_sync_env->async_rados, _sync_env->store, _obj_ctx,
									    _sync_env->store->get_zone_params().log_pool,
									    RGWDataSyncStatusManager::sync_status_oid(_sync_env->source_zone),
									    &_status->sync_info),
                                                                            sync_env(_sync_env),
                                                                            obj_ctx(_obj_ctx),
									    sync_status(_status) {}

  int handle_data(rgw_data_sync_info& data);
};

int RGWReadDataSyncStatusCoroutine::handle_data(rgw_data_sync_info& data)
{
  if (retcode == -ENOENT) {
    return retcode;
  }

  map<uint32_t, rgw_data_sync_marker>& markers = sync_status->sync_markers;
  RGWRados *store = sync_env->store;
  for (int i = 0; i < (int)data.num_shards; i++) {
    spawn(new RGWSimpleRadosReadCR<rgw_data_sync_marker>(sync_env->async_rados, store, obj_ctx, store->get_zone_params().log_pool,
                                                    RGWDataSyncStatusManager::shard_obj_name(sync_env->source_zone, i), &markers[i]), true);
  }
  return 0;
}

class RGWReadRemoteDataLogShardInfoCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;

  RGWRESTReadResource *http_op;

  int shard_id;
  RGWDataChangesLogInfo *shard_info;

public:
  RGWReadRemoteDataLogShardInfoCR(RGWDataSyncEnv *_sync_env,
                                                      int _shard_id, RGWDataChangesLogInfo *_shard_info) : RGWCoroutine(_sync_env->cct),
                                                      sync_env(_sync_env),
                                                      http_op(NULL),
                                                      shard_id(_shard_id),
                                                      shard_info(_shard_info) {
  }

  ~RGWReadRemoteDataLogShardInfoCR() {
    if (http_op) {
      http_op->put();
    }
  }

  int operate() {
    reenter(this) {
      yield {
	char buf[16];
	snprintf(buf, sizeof(buf), "%d", shard_id);
        rgw_http_param_pair pairs[] = { { "type" , "data" },
	                                { "id", buf },
					{ "info" , NULL },
	                                { NULL, NULL } };

        string p = "/admin/log/";

        http_op = new RGWRESTReadResource(sync_env->conn, p, pairs, NULL, sync_env->http_manager);

        http_op->set_user_info((void *)stack);

        int ret = http_op->aio_read();
        if (ret < 0) {
          ldout(sync_env->cct, 0) << "ERROR: failed to read from " << p << dendl;
          log_error() << "failed to send http operation: " << http_op->to_str() << " ret=" << ret << std::endl;
          return set_cr_error(ret);
        }

        return io_block(0);
      }
      yield {
        int ret = http_op->wait(shard_info);
        if (ret < 0) {
          return set_cr_error(ret);
        }
        return set_cr_done();
      }
    }
    return 0;
  }
};

struct read_remote_data_log_response {
  string marker;
  bool truncated;
  list<rgw_data_change_log_entry> entries;

  read_remote_data_log_response() : truncated(false) {}

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("marker", marker, obj);
    JSONDecoder::decode_json("truncated", truncated, obj);
    JSONDecoder::decode_json("entries", entries, obj);
  };
};

class RGWReadRemoteDataLogShardCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;

  RGWRESTReadResource *http_op;

  int shard_id;
  string *pmarker;
  list<rgw_data_change_log_entry> *entries;
  bool *truncated;

  read_remote_data_log_response response;

public:
  RGWReadRemoteDataLogShardCR(RGWDataSyncEnv *_sync_env,
                              int _shard_id, string *_pmarker, list<rgw_data_change_log_entry> *_entries, bool *_truncated) : RGWCoroutine(_sync_env->cct),
                                                      sync_env(_sync_env),
                                                      http_op(NULL),
                                                      shard_id(_shard_id),
                                                      pmarker(_pmarker),
                                                      entries(_entries),
                                                      truncated(_truncated) {
    if (http_op) {
      http_op->put();
    }
  }

  int operate() {
    reenter(this) {
      yield {
	char buf[16];
	snprintf(buf, sizeof(buf), "%d", shard_id);
        rgw_http_param_pair pairs[] = { { "type" , "data" },
	                                { "id", buf },
	                                { "marker", pmarker->c_str() },
	                                { "extra-info", "true" },
	                                { NULL, NULL } };

        string p = "/admin/log/";

        http_op = new RGWRESTReadResource(sync_env->conn, p, pairs, NULL, sync_env->http_manager);

        http_op->set_user_info((void *)stack);

        int ret = http_op->aio_read();
        if (ret < 0) {
          ldout(sync_env->cct, 0) << "ERROR: failed to read from " << p << dendl;
          log_error() << "failed to send http operation: " << http_op->to_str() << " ret=" << ret << std::endl;
          return set_cr_error(ret);
        }

        return io_block(0);
      }
      yield {
        int ret = http_op->wait(&response);
        if (ret < 0) {
          return set_cr_error(ret);
        }
        entries->clear();
        entries->swap(response.entries);
        *pmarker = response.marker;
        *truncated = response.truncated;
        return set_cr_done();
      }
    }
    return 0;
  }
};

class RGWReadRemoteDataLogInfoCR : public RGWShardCollectCR {
  RGWDataSyncEnv *sync_env;

  int num_shards;
  map<int, RGWDataChangesLogInfo> *datalog_info;

  int shard_id;
#define READ_DATALOG_MAX_CONCURRENT 10

public:
  RGWReadRemoteDataLogInfoCR(RGWDataSyncEnv *_sync_env,
                     int _num_shards,
                     map<int, RGWDataChangesLogInfo> *_datalog_info) : RGWShardCollectCR(_sync_env->cct, READ_DATALOG_MAX_CONCURRENT),
                                                                 sync_env(_sync_env), num_shards(_num_shards),
                                                                 datalog_info(_datalog_info), shard_id(0) {}
  bool spawn_next();
};

bool RGWReadRemoteDataLogInfoCR::spawn_next() {
  if (shard_id >= num_shards) {
    return false;
  }
  spawn(new RGWReadRemoteDataLogShardInfoCR(sync_env, shard_id, &(*datalog_info)[shard_id]), false);
  shard_id++;
  return true;
}

class RGWListRemoteDataLogShardCR : public RGWSimpleCoroutine {
  RGWDataSyncEnv *sync_env;
  RGWRESTReadResource *http_op;

  int shard_id;
  string marker;
  uint32_t max_entries;
  rgw_datalog_shard_data *result;

public:
  RGWListRemoteDataLogShardCR(RGWDataSyncEnv *env, int _shard_id,
                              const string& _marker, uint32_t _max_entries,
                              rgw_datalog_shard_data *_result)
    : RGWSimpleCoroutine(env->store->ctx()), sync_env(env), http_op(NULL),
      shard_id(_shard_id), marker(_marker), max_entries(_max_entries), result(_result) {}

  int send_request() {
    RGWRESTConn *conn = sync_env->conn;
    RGWRados *store = sync_env->store;

    char buf[32];
    snprintf(buf, sizeof(buf), "%d", shard_id);

    char max_entries_buf[32];
    snprintf(max_entries_buf, sizeof(max_entries_buf), "%d", (int)max_entries);

    const char *marker_key = (marker.empty() ? "" : "marker");

    rgw_http_param_pair pairs[] = { { "type", "data" },
      { "id", buf },
      { "max-entries", max_entries_buf },
      { marker_key, marker.c_str() },
      { NULL, NULL } };

    string p = "/admin/log/";

    http_op = new RGWRESTReadResource(conn, p, pairs, NULL, sync_env->http_manager);
    http_op->set_user_info((void *)stack);

    int ret = http_op->aio_read();
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: failed to read from " << p << dendl;
      log_error() << "failed to send http operation: " << http_op->to_str() << " ret=" << ret << std::endl;
      http_op->put();
      return ret;
    }

    return 0;
  }

  int request_complete() {
    int ret = http_op->wait(result);
    if (ret < 0 && ret != -ENOENT) {
      ldout(sync_env->store->ctx(), 0) << "ERROR: failed to list remote datalog shard, ret=" << ret << dendl;
      return ret;
    }
    return 0;
  }
};

class RGWListRemoteDataLogCR : public RGWShardCollectCR {
  RGWDataSyncEnv *sync_env;

  map<int, string> shards;
  int max_entries_per_shard;
  map<int, rgw_datalog_shard_data> *result;

  map<int, string>::iterator iter;
#define READ_DATALOG_MAX_CONCURRENT 10

public:
  RGWListRemoteDataLogCR(RGWDataSyncEnv *_sync_env,
                     map<int, string>& _shards,
                     int _max_entries_per_shard,
                     map<int, rgw_datalog_shard_data> *_result) : RGWShardCollectCR(_sync_env->cct, READ_DATALOG_MAX_CONCURRENT),
                                                                 sync_env(_sync_env), max_entries_per_shard(_max_entries_per_shard),
                                                                 result(_result) {
    shards.swap(_shards);
    iter = shards.begin();
  }
  bool spawn_next();
};

bool RGWListRemoteDataLogCR::spawn_next() {
  if (iter == shards.end()) {
    return false;
  }

  spawn(new RGWListRemoteDataLogShardCR(sync_env, iter->first, iter->second, max_entries_per_shard, &(*result)[iter->first]), false);
  ++iter;
  return true;
}

class RGWInitDataSyncStatusCoroutine : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;

  RGWRados *store;
  RGWObjectCtx& obj_ctx;

  string sync_status_oid;

  string lock_name;
  string cookie;
  rgw_data_sync_info status;
  map<int, RGWDataChangesLogInfo> shards_info;
public:
  RGWInitDataSyncStatusCoroutine(RGWDataSyncEnv *_sync_env,
		      RGWObjectCtx& _obj_ctx, uint32_t _num_shards) : RGWCoroutine(_sync_env->cct),
                                                sync_env(_sync_env), store(sync_env->store),
                                                obj_ctx(_obj_ctx) {
    lock_name = "sync_lock";
    status.num_shards = _num_shards;

#define COOKIE_LEN 16
    char buf[COOKIE_LEN + 1];

    gen_rand_alphanumeric(cct, buf, sizeof(buf) - 1);
    string cookie = buf;

    sync_status_oid = RGWDataSyncStatusManager::sync_status_oid(sync_env->source_zone);
  }

  int operate() {
    int ret;
    reenter(this) {
      yield {
	uint32_t lock_duration = 30;
	call(new RGWSimpleRadosLockCR(sync_env->async_rados, store, store->get_zone_params().log_pool, sync_status_oid,
			             lock_name, cookie, lock_duration));
	if (retcode < 0) {
	  ldout(cct, 0) << "ERROR: failed to take a lock on " << sync_status_oid << dendl;
	  return set_cr_error(retcode);
	}
      }
      yield {
        call(new RGWSimpleRadosWriteCR<rgw_data_sync_info>(sync_env->async_rados, store, store->get_zone_params().log_pool,
				 sync_status_oid, status));
      }
      yield { /* take lock again, we just recreated the object */
	uint32_t lock_duration = 30;
	call(new RGWSimpleRadosLockCR(sync_env->async_rados, store, store->get_zone_params().log_pool, sync_status_oid,
			             lock_name, cookie, lock_duration));
	if (retcode < 0) {
	  ldout(cct, 0) << "ERROR: failed to take a lock on " << sync_status_oid << dendl;
	  return set_cr_error(retcode);
	}
      }
      /* fetch current position in logs */
      yield {
        RGWRESTConn *conn = store->get_zone_conn_by_id(sync_env->source_zone);
        if (!conn) {
          ldout(cct, 0) << "ERROR: connection to zone " << sync_env->source_zone << " does not exist!" << dendl;
          return set_cr_error(-EIO);
        }
        for (int i = 0; i < (int)status.num_shards; i++) {
          spawn(new RGWReadRemoteDataLogShardInfoCR(sync_env, i, &shards_info[i]), true);
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
          RGWDataChangesLogInfo& info = shards_info[i];
	  marker.next_step_marker = info.marker;
	  marker.timestamp = info.last_update;
          spawn(new RGWSimpleRadosWriteCR<rgw_data_sync_marker>(sync_env->async_rados, store, store->get_zone_params().log_pool,
				                          RGWDataSyncStatusManager::shard_obj_name(sync_env->source_zone, i), marker), true);
        }
      }
      yield {
	status.state = rgw_data_sync_info::StateBuildingFullSyncMaps;
        call(new RGWSimpleRadosWriteCR<rgw_data_sync_info>(sync_env->async_rados, store, store->get_zone_params().log_pool,
				 sync_status_oid, status));
      }
      yield { /* unlock */
	call(new RGWSimpleRadosUnlockCR(sync_env->async_rados, store, store->get_zone_params().log_pool, sync_status_oid,
			             lock_name, cookie));
      }
      while (collect(&ret)) {
	if (ret < 0) {
	  return set_state(RGWCoroutine_Error);
	}
        yield;
      }
      drain_all();
      return set_cr_done();
    }
    return 0;
  }
};

int RGWRemoteDataLog::read_log_info(rgw_datalog_info *log_info)
{
  rgw_http_param_pair pairs[] = { { "type", "data" },
                                  { NULL, NULL } };

  int ret = sync_env.conn->get_json_resource("/admin/log", pairs, *log_info);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to fetch datalog info" << dendl;
    return ret;
  }

  ldout(store->ctx(), 20) << "remote datalog, num_shards=" << log_info->num_shards << dendl;

  return 0;
}

int RGWRemoteDataLog::read_source_log_shards_info(map<int, RGWDataChangesLogInfo> *shards_info)
{
  rgw_datalog_info log_info;
  int ret = read_log_info(&log_info);
  if (ret < 0) {
    return ret;
  }

  return run(new RGWReadRemoteDataLogInfoCR(&sync_env, log_info.num_shards, shards_info));
}

int RGWRemoteDataLog::read_source_log_shards_next(map<int, string> shard_markers, map<int, rgw_datalog_shard_data> *result)
{
  if (store->is_meta_master()) {
    return 0;
  }

  return run(new RGWListRemoteDataLogCR(&sync_env, shard_markers, 1, result));
}

int RGWRemoteDataLog::init(const string& _source_zone, RGWRESTConn *_conn, RGWSyncErrorLogger *_error_logger)
{
  if (initialized) {
    return 0;
  }

  sync_env.init(store->ctx(), store, _conn, async_rados, &http_manager, _error_logger, _source_zone);

  int ret = http_manager.set_threaded();
  if (ret < 0) {
    ldout(store->ctx(), 0) << "failed in http_manager.set_threaded() ret=" << ret << dendl;
    return ret;
  }

  initialized = true;

  return 0;
}

void RGWRemoteDataLog::finish()
{
  stop();
}

int RGWRemoteDataLog::get_shard_info(int shard_id)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%d", shard_id);

  rgw_http_param_pair pairs[] = { { "type", "data" },
                                  { "id", buf },
                                  { "info", NULL },
                                  { NULL, NULL } };

  RGWDataChangesLogInfo info;
  int ret = sync_env.conn->get_json_resource("/admin/log", pairs, info);
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
  int r = run(new RGWReadDataSyncStatusCoroutine(&sync_env, obj_ctx, sync_status));
  if (r == -ENOENT) {
    r = 0;
  }
  return r;
}

int RGWRemoteDataLog::init_sync_status(int num_shards)
{
  RGWObjectCtx obj_ctx(store, NULL);
  return run(new RGWInitDataSyncStatusCoroutine(&sync_env, obj_ctx, num_shards));
}

static string full_data_sync_index_shard_oid(const string& source_zone, int shard_id)
{
  char buf[datalog_sync_full_sync_index_prefix.size() + 1 + source_zone.size() + 1 + 16];
  snprintf(buf, sizeof(buf), "%s.%s.%d", datalog_sync_full_sync_index_prefix.c_str(), source_zone.c_str(), shard_id);
  return string(buf);
}

struct bucket_instance_meta_info {
  string key;
  obj_version ver;
  utime_t mtime;
  RGWBucketInstanceMetadataObject data;

  bucket_instance_meta_info() {}

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("key", key, obj);
    JSONDecoder::decode_json("ver", ver, obj);
    JSONDecoder::decode_json("mtime", mtime, obj);
    JSONDecoder::decode_json("data", data, obj);
  }
};

class RGWListBucketIndexesCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;

  RGWRados *store;

  rgw_data_sync_status *sync_status;
  int num_shards;

  int req_ret;
  int ret;

  list<string> result;
  list<string>::iterator iter;

  RGWShardedOmapCRManager *entries_index;

  string oid_prefix;

  string path;
  bucket_instance_meta_info meta_info;
  string key;
  string s;
  int i;

  bool failed;

public:
  RGWListBucketIndexesCR(RGWDataSyncEnv *_sync_env,
                         rgw_data_sync_status *_sync_status) : RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
                                                      store(sync_env->store), sync_status(_sync_status),
						      req_ret(0), ret(0), entries_index(NULL), i(0), failed(false) {
    oid_prefix = datalog_sync_full_sync_index_prefix + "." + sync_env->source_zone; 
    path = "/admin/metadata/bucket.instance";
    num_shards = sync_status->sync_info.num_shards;
  }
  ~RGWListBucketIndexesCR() {
    delete entries_index;
  }

  int operate() {
    reenter(this) {
      entries_index = new RGWShardedOmapCRManager(sync_env->async_rados, store, this, num_shards,
						  store->get_zone_params().log_pool,
                                                  oid_prefix);
      yield {
        string entrypoint = string("/admin/metadata/bucket.instance");
        /* FIXME: need a better scaling solution here, requires streaming output */
        call(new RGWReadRESTResourceCR<list<string> >(store->ctx(), sync_env->conn, sync_env->http_manager,
                                                      entrypoint, NULL, &result));
      }
      if (get_ret_status() < 0) {
        ldout(sync_env->cct, 0) << "ERROR: failed to fetch metadata for section bucket.index" << dendl;
        return set_state(RGWCoroutine_Error);
      }
      for (iter = result.begin(); iter != result.end(); ++iter) {
        ldout(sync_env->cct, 20) << "list metadata: section=bucket.index key=" << *iter << dendl;

        key = *iter;

        yield {
          rgw_http_param_pair pairs[] = { { "key", key.c_str() },
                                          { NULL, NULL } };

          call(new RGWReadRESTResourceCR<bucket_instance_meta_info>(store->ctx(), sync_env->conn, sync_env->http_manager, path, pairs, &meta_info));
        }

        num_shards = meta_info.data.get_bucket_info().num_shards;
        if (num_shards > 0) {
          for (i = 0; i < num_shards; i++) {
            char buf[16];
            snprintf(buf, sizeof(buf), ":%d", i);
            s = key + buf;
            yield entries_index->append(s, store->data_log->get_log_shard_id(meta_info.data.get_bucket_info().bucket, i));
          }
        } else {
          yield entries_index->append(key, store->data_log->get_log_shard_id(meta_info.data.get_bucket_info().bucket, -1));
        }
      }
      yield {
        if (!entries_index->finish()) {
          failed = true;
        }
      }
      if (!failed) {
        for (map<uint32_t, rgw_data_sync_marker>::iterator iter = sync_status->sync_markers.begin(); iter != sync_status->sync_markers.end(); ++iter) {
          int shard_id = (int)iter->first;
          rgw_data_sync_marker& marker = iter->second;
          marker.total_entries = entries_index->get_total_entries(shard_id);
          spawn(new RGWSimpleRadosWriteCR<rgw_data_sync_marker>(sync_env->async_rados, store, store->get_zone_params().log_pool,
                                                                RGWDataSyncStatusManager::shard_obj_name(sync_env->source_zone, shard_id), marker), true);
        }
      } else {
          yield call(sync_env->error_logger->log_error_cr(sync_env->conn->get_remote_id(), "data.init", "",
                                                          EIO, string("failed to build bucket instances map")));
      }
      while (collect(&ret)) {
	if (ret < 0) {
          yield call(sync_env->error_logger->log_error_cr(sync_env->conn->get_remote_id(), "data.init", "",
                                                          -ret, string("failed to store sync status: ") + cpp_strerror(-ret)));
	  req_ret = ret;
	}
        yield;
      }
      drain_all();
      if (req_ret < 0) {
        yield return set_cr_error(req_ret);
      }
      yield return set_cr_done();
    }
    return 0;
  }
};

#define DATA_SYNC_UPDATE_MARKER_WINDOW 1

class RGWDataSyncShardMarkerTrack : public RGWSyncShardMarkerTrack<string, string> {
  RGWDataSyncEnv *sync_env;

  string marker_oid;
  rgw_data_sync_marker sync_marker;

  map<string, string> key_to_marker;
  map<string, string> marker_to_key;

  void handle_finish(const string& marker) {
    map<string, string>::iterator iter = marker_to_key.find(marker);
    if (iter == marker_to_key.end()) {
      return;
    }
    key_to_marker.erase(iter->second);
    reset_need_retry(iter->second);
    marker_to_key.erase(iter);
  }

public:
  RGWDataSyncShardMarkerTrack(RGWDataSyncEnv *_sync_env,
                         const string& _marker_oid,
                         const rgw_data_sync_marker& _marker) : RGWSyncShardMarkerTrack(DATA_SYNC_UPDATE_MARKER_WINDOW),
                                                                sync_env(_sync_env),
                                                                marker_oid(_marker_oid),
                                                                sync_marker(_marker) {}

  RGWCoroutine *store_marker(const string& new_marker, uint64_t index_pos, const real_time& timestamp) {
    sync_marker.marker = new_marker;
    sync_marker.pos = index_pos;

    ldout(sync_env->cct, 20) << __func__ << "(): updating marker marker_oid=" << marker_oid << " marker=" << new_marker << dendl;
    RGWRados *store = sync_env->store;

    return new RGWSimpleRadosWriteCR<rgw_data_sync_marker>(sync_env->async_rados, store, store->get_zone_params().log_pool,
				 marker_oid, sync_marker);
  }

  /*
   * create index from key -> marker, and from marker -> key
   * this is useful so that we can insure that we only have one
   * entry for any key that is used. This is needed when doing
   * incremenatl sync of data, and we don't want to run multiple
   * concurrent sync operations for the same bucket shard 
   */
  bool index_key_to_marker(const string& key, const string& marker) {
    if (key_to_marker.find(key) != key_to_marker.end()) {
      set_need_retry(key);
      return false;
    }
    key_to_marker[key] = marker;
    marker_to_key[marker] = key;
    return true;
  }
};

class RGWRunBucketSyncCoroutine : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  string bucket_name;
  string bucket_id;
  RGWBucketInfo bucket_info;
  int shard_id;
  rgw_bucket_shard_sync_info sync_status;
  RGWMetaSyncEnv meta_sync_env;

public:
  RGWRunBucketSyncCoroutine(RGWDataSyncEnv *_sync_env,
                            const string& _bucket_name, const string _bucket_id, int _shard_id) : RGWCoroutine(_sync_env->cct),
                                                                            sync_env(_sync_env),
                                                                            bucket_name(_bucket_name),
									    bucket_id(_bucket_id), shard_id(_shard_id) {}

  int operate();
};

static int parse_bucket_shard(CephContext *cct, const string& raw_key, string *bucket_name, string *bucket_instance, int *shard_id)
{
  ssize_t pos = raw_key.find(':');
  *bucket_name = raw_key.substr(0, pos);
  *bucket_instance = raw_key.substr(pos + 1);
  pos = bucket_instance->find(':');
  *shard_id = -1;
  if (pos >= 0) {
    string err;
    string s = bucket_instance->substr(pos + 1);
    *shard_id = strict_strtol(s.c_str(), 10, &err);
    if (!err.empty()) {
      ldout(cct, 0) << "ERROR: failed to parse bucket instance key: " << *bucket_instance << dendl;
      return -EINVAL;
    }

    *bucket_instance = bucket_instance->substr(0, pos);
  }
  return 0;
}

class RGWDataSyncSingleEntryCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;

  string raw_key;
  string entry_marker;

  string bucket_name;
  string bucket_instance;

  int sync_status;

  bufferlist md_bl;

  RGWDataSyncShardMarkerTrack *marker_tracker;

public:
  RGWDataSyncSingleEntryCR(RGWDataSyncEnv *_sync_env,
		           const string& _raw_key, const string& _entry_marker, RGWDataSyncShardMarkerTrack *_marker_tracker) : RGWCoroutine(_sync_env->cct),
                                                      sync_env(_sync_env),
						      raw_key(_raw_key), entry_marker(_entry_marker),
                                                      sync_status(0),
                                                      marker_tracker(_marker_tracker) {
    set_description() << "data sync single entry (source_zone=" << sync_env->source_zone << ") key=" <<_raw_key << " entry=" << entry_marker;
  }

  int operate() {
    reenter(this) {
      do {
        yield {
          int shard_id;
          int ret = parse_bucket_shard(sync_env->cct, raw_key, &bucket_name, &bucket_instance, &shard_id);
          if (ret < 0) {
            return set_cr_error(-EIO);
          }
          marker_tracker->reset_need_retry(raw_key);
          call(new RGWRunBucketSyncCoroutine(sync_env, bucket_name, bucket_instance, shard_id));
        }
      } while (marker_tracker->need_retry(raw_key));

      sync_status = retcode;

      if (sync_status < 0) {
        yield call(sync_env->error_logger->log_error_cr(sync_env->conn->get_remote_id(), "data", bucket_name + ":" + bucket_instance,
                                                        -sync_status, string("failed to sync bucket instance: ") + cpp_strerror(-sync_status)));
      }
      /* FIXME: what do do in case of error */
      if (!entry_marker.empty()) {
        /* update marker */
        yield call(marker_tracker->finish(entry_marker));
      }
      if (sync_status == 0) {
        sync_status = retcode;
      }
      if (sync_status < 0) {
        return set_cr_error(sync_status);
      }
      return set_cr_done();
    }
    return 0;
  }
};

#define BUCKET_SHARD_SYNC_SPAWN_WINDOW 20

class RGWDataSyncShardCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;

  rgw_bucket pool;

  uint32_t shard_id;
  rgw_data_sync_marker sync_marker;

  map<string, bufferlist> entries;
  map<string, bufferlist>::iterator iter;

  string oid;

  RGWDataSyncShardMarkerTrack *marker_tracker;

  list<rgw_data_change_log_entry> log_entries;
  list<rgw_data_change_log_entry>::iterator log_iter;
  bool truncated;

  RGWDataChangesLogInfo shard_info;
  string datalog_marker;

  Mutex inc_lock;
  Cond inc_cond;

  boost::asio::coroutine incremental_cr;
  boost::asio::coroutine full_cr;


  set<string> modified_shards;
  set<string> current_modified;

  set<string>::iterator modified_iter;

  int total_entries;

  int spawn_window;

  bool *reset_backoff;

  set<string> spawned_keys;

  RGWContinuousLeaseCR *lease_cr;
  string status_oid;
public:
  RGWDataSyncShardCR(RGWDataSyncEnv *_sync_env,
                     rgw_bucket& _pool,
		     uint32_t _shard_id, rgw_data_sync_marker& _marker, bool *_reset_backoff) : RGWCoroutine(_sync_env->cct),
                                                      sync_env(_sync_env),
						      pool(_pool),
						      shard_id(_shard_id),
						      sync_marker(_marker),
                                                      marker_tracker(NULL), truncated(false), inc_lock("RGWDataSyncShardCR::inc_lock"),
                                                      total_entries(0), spawn_window(BUCKET_SHARD_SYNC_SPAWN_WINDOW), reset_backoff(NULL),
                                                      lease_cr(NULL) {
    set_description() << "data sync shard source_zone=" << sync_env->source_zone << " shard_id=" << shard_id;
    status_oid = RGWDataSyncStatusManager::shard_obj_name(sync_env->source_zone, shard_id);
  }

  ~RGWDataSyncShardCR() {
    delete marker_tracker;
    if (lease_cr) {
      lease_cr->abort();
      lease_cr->put();
    }
  }

  void append_modified_shards(set<string>& keys) {
    Mutex::Locker l(inc_lock);
    modified_shards.insert(keys.begin(), keys.end());
  }

  void set_marker_tracker(RGWDataSyncShardMarkerTrack *mt) {
    delete marker_tracker;
    marker_tracker = mt;
  }

  int operate() {
    int r;
    while (true) {
      switch (sync_marker.state) {
      case rgw_data_sync_marker::FullSync:
        r = full_sync();
        if (r < 0) {
          ldout(cct, 10) << "sync: full_sync: shard_id=" << shard_id << " r=" << r << dendl;
          return set_cr_error(r);
        }
        return 0;
      case rgw_data_sync_marker::IncrementalSync:
        r  = incremental_sync();
        if (r < 0) {
          ldout(cct, 10) << "sync: incremental_sync: shard_id=" << shard_id << " r=" << r << dendl;
          return set_cr_error(r);
        }
        return 0;
      default:
        return set_cr_error(-EIO);
      }
    }
    return 0;
  }

  void init_lease_cr() {
    set_status("acquiring sync lock");
    uint32_t lock_duration = cct->_conf->rgw_sync_lease_period;
    string lock_name = "sync_lock";
    if (lease_cr) {
      lease_cr->abort();
      lease_cr->put();
    }
    RGWRados *store = sync_env->store;
    lease_cr = new RGWContinuousLeaseCR(sync_env->async_rados, store, store->get_zone_params().log_pool, status_oid,
                                        lock_name, lock_duration, this);
    lease_cr->get();
    spawn(lease_cr, false);
  }

  int full_sync() {
#define OMAP_GET_MAX_ENTRIES 100
    int max_entries = OMAP_GET_MAX_ENTRIES;
    reenter(&full_cr) {
      yield init_lease_cr();
      while (!lease_cr->is_locked()) {
        if (lease_cr->is_done()) {
          ldout(cct, 0) << "ERROR: lease cr failed, done early " << dendl;
          set_status("lease lock failed, early abort");
          return set_cr_error(lease_cr->get_ret_status());
        }
        set_sleeping(true);
        yield;
      }
      oid = full_data_sync_index_shard_oid(sync_env->source_zone, shard_id);
      set_marker_tracker(new RGWDataSyncShardMarkerTrack(sync_env, status_oid, sync_marker));
      total_entries = sync_marker.pos;
      do {
        yield call(new RGWRadosGetOmapKeysCR(sync_env->store, pool, oid, sync_marker.marker, &entries, max_entries));
        if (retcode < 0) {
          ldout(sync_env->cct, 0) << "ERROR: " << __func__ << "(): RGWRadosGetOmapKeysCR() returned ret=" << retcode << dendl;
          lease_cr->go_down();
          drain_all();
          return set_cr_error(retcode);
        }
        iter = entries.begin();
        for (; iter != entries.end(); ++iter) {
          ldout(sync_env->cct, 20) << __func__ << ": full sync: " << iter->first << dendl;
          total_entries++;
          if (!marker_tracker->start(iter->first, total_entries, real_time())) {
            ldout(sync_env->cct, 0) << "ERROR: cannot start syncing " << iter->first << ". Duplicate entry?" << dendl;
          } else {
            // fetch remote and write locally
            yield spawn(new RGWDataSyncSingleEntryCR(sync_env, iter->first, iter->first, marker_tracker), false);
            if (retcode < 0) {
              lease_cr->go_down();
              drain_all();
              return set_cr_error(retcode);
            }
          }
          sync_marker.marker = iter->first;
        }
      } while ((int)entries.size() == max_entries);

      lease_cr->go_down();
      drain_all();

      yield {
        /* update marker to reflect we're done with full sync */
        sync_marker.state = rgw_data_sync_marker::IncrementalSync;
        sync_marker.marker = sync_marker.next_step_marker;
        sync_marker.next_step_marker.clear();
        RGWRados *store = sync_env->store;
        call(new RGWSimpleRadosWriteCR<rgw_data_sync_marker>(sync_env->async_rados, store, store->get_zone_params().log_pool,
                                                             status_oid, sync_marker));
      }
      if (retcode < 0) {
        ldout(sync_env->cct, 0) << "ERROR: failed to set sync marker: retcode=" << retcode << dendl;
        lease_cr->go_down();
        return set_cr_error(retcode);
      }
    }
    return 0;
  }

  int incremental_sync() {
    reenter(&incremental_cr) {
      yield init_lease_cr();
      while (!lease_cr->is_locked()) {
        if (lease_cr->is_done()) {
          ldout(cct, 0) << "ERROR: lease cr failed, done early " << dendl;
          set_status("lease lock failed, early abort");
          return set_cr_error(lease_cr->get_ret_status());
        }
        set_sleeping(true);
        yield;
      }
      set_status("lease acquired");
      set_marker_tracker(new RGWDataSyncShardMarkerTrack(sync_env, status_oid, sync_marker));
      do {
        current_modified.clear();
        inc_lock.Lock();
        current_modified.swap(modified_shards);
        inc_lock.Unlock();

        /* process out of band updates */
        for (modified_iter = current_modified.begin(); modified_iter != current_modified.end(); ++modified_iter) {
          yield {
            ldout(sync_env->cct, 20) << __func__ << "(): async update notification: " << *modified_iter << dendl;
            spawn(new RGWDataSyncSingleEntryCR(sync_env, *modified_iter, string(), marker_tracker), false);
          }
        }

        yield call(new RGWReadRemoteDataLogShardInfoCR(sync_env, shard_id, &shard_info));
        if (retcode < 0) {
          ldout(sync_env->cct, 0) << "ERROR: failed to fetch remote data log info: ret=" << retcode << dendl;
          lease_cr->go_down();
          drain_all();
          return set_cr_error(retcode);
        }
        datalog_marker = shard_info.marker;
#define INCREMENTAL_MAX_ENTRIES 100
	ldout(sync_env->cct, 20) << __func__ << ":" << __LINE__ << ": shard_id=" << shard_id << " datalog_marker=" << datalog_marker << " sync_marker.marker=" << sync_marker.marker << dendl;
	if (datalog_marker > sync_marker.marker) {
          spawned_keys.clear();
          yield call(new RGWReadRemoteDataLogShardCR(sync_env, shard_id, &sync_marker.marker, &log_entries, &truncated));
          if (retcode < 0) {
            ldout(sync_env->cct, 0) << "ERROR: failed to read remote data log info: ret=" << retcode << dendl;
            lease_cr->go_down();
            drain_all();
            return set_cr_error(retcode);
          }
          for (log_iter = log_entries.begin(); log_iter != log_entries.end(); ++log_iter) {
            ldout(sync_env->cct, 20) << __func__ << ":" << __LINE__ << ": shard_id=" << shard_id << " log_entry: " << log_iter->log_id << ":" << log_iter->log_timestamp << ":" << log_iter->entry.key << dendl;
            if (!marker_tracker->index_key_to_marker(log_iter->entry.key, log_iter->log_id)) {
              ldout(sync_env->cct, 20) << __func__ << ": skipping sync of entry: " << log_iter->log_id << ":" << log_iter->entry.key << " sync already in progress for bucket shard" << dendl;
              marker_tracker->try_update_high_marker(log_iter->log_id, 0, log_iter->log_timestamp);
              continue;
            }
            if (!marker_tracker->start(log_iter->log_id, 0, log_iter->log_timestamp)) {
              ldout(sync_env->cct, 0) << "ERROR: cannot start syncing " << log_iter->log_id << ". Duplicate entry?" << dendl;
            } else {
              /*
               * don't spawn the same key more than once. We can do that as long as we don't yield
               */
              if (spawned_keys.find(log_iter->entry.key) == spawned_keys.end()) {
                spawned_keys.insert(log_iter->entry.key);
                spawn(new RGWDataSyncSingleEntryCR(sync_env, log_iter->entry.key, log_iter->log_id, marker_tracker), false);
                if (retcode < 0) {
                  lease_cr->go_down();
                  drain_all();
                  return set_cr_error(retcode);
                }
              }
            }
	  }
          while ((int)num_spawned() > spawn_window) {
            set_status() << "num_spawned() > spawn_window";
            yield wait_for_child();
            int ret;
            while (collect(&ret)) {
              if (ret < 0) {
                ldout(sync_env->cct, 0) << "ERROR: a sync operation returned error" << dendl;
                /* we have reported this error */
              }
              /* not waiting for child here */
            }
          }
	}
	ldout(sync_env->cct, 20) << __func__ << ":" << __LINE__ << ": shard_id=" << shard_id << " datalog_marker=" << datalog_marker << " sync_marker.marker=" << sync_marker.marker << dendl;
	if (datalog_marker == sync_marker.marker) {
#define INCREMENTAL_INTERVAL 20
	  yield wait(utime_t(INCREMENTAL_INTERVAL, 0));
	}
      } while (true);
    }
    return 0;
  }
};

class RGWDataSyncShardControlCR : public RGWBackoffControlCR {
  RGWDataSyncEnv *sync_env;

  rgw_bucket pool;

  uint32_t shard_id;
  rgw_data_sync_marker sync_marker;

public:
  RGWDataSyncShardControlCR(RGWDataSyncEnv *_sync_env, rgw_bucket& _pool,
		     uint32_t _shard_id, rgw_data_sync_marker& _marker) : RGWBackoffControlCR(_sync_env->cct, false),
                                                      sync_env(_sync_env),
						      pool(_pool),
						      shard_id(_shard_id),
						      sync_marker(_marker) {
  }

  RGWCoroutine *alloc_cr() {
    return new RGWDataSyncShardCR(sync_env, pool, shard_id, sync_marker, backoff_ptr());
  }

  RGWCoroutine *alloc_finisher_cr() {
    RGWRados *store = sync_env->store;
    RGWObjectCtx obj_ctx(store, NULL);
    return new RGWSimpleRadosReadCR<rgw_data_sync_marker>(sync_env->async_rados, store, obj_ctx, store->get_zone_params().log_pool,
                                                    RGWDataSyncStatusManager::shard_obj_name(sync_env->source_zone, shard_id), &sync_marker);
  }

  void append_modified_shards(set<string>& keys) {
    Mutex::Locker l(cr_lock());

    RGWDataSyncShardCR *cr = static_cast<RGWDataSyncShardCR *>(get_cr());
    if (!cr) {
      return;
    }

    cr->append_modified_shards(keys);
  }
};

class RGWDataSyncCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  uint32_t num_shards;

  RGWObjectCtx obj_ctx;

  rgw_data_sync_status sync_status;

  RGWDataSyncShardMarkerTrack *marker_tracker;

  Mutex shard_crs_lock;
  map<int, RGWDataSyncShardControlCR *> shard_crs;

  bool *reset_backoff;

public:
  RGWDataSyncCR(RGWDataSyncEnv *_sync_env, uint32_t _num_shards, bool *_reset_backoff) : RGWCoroutine(_sync_env->cct),
                                                      sync_env(_sync_env),
                                                      num_shards(_num_shards),
                                                      obj_ctx(sync_env->store),
                                                      marker_tracker(NULL),
                                                      shard_crs_lock("RGWDataSyncCR::shard_crs_lock"),
                                                      reset_backoff(_reset_backoff) {
  }

  ~RGWDataSyncCR() {
    for (auto iter : shard_crs) {
      iter.second->put();
    }
  }

  int operate() {
    reenter(this) {

      /* read sync status */
      yield call(new RGWReadDataSyncStatusCoroutine(sync_env, obj_ctx, &sync_status));

      if (retcode == -ENOENT) {
        sync_status.sync_info.num_shards = num_shards;
      } else if (retcode < 0 && retcode != -ENOENT) {
        ldout(sync_env->cct, 0) << "ERROR: failed to fetch sync status, retcode=" << retcode << dendl;
        return set_cr_error(retcode);
      }

      /* state: init status */
      if ((rgw_data_sync_info::SyncState)sync_status.sync_info.state == rgw_data_sync_info::StateInit) {
        ldout(sync_env->cct, 20) << __func__ << "(): init" << dendl;
        yield call(new RGWInitDataSyncStatusCoroutine(sync_env, obj_ctx, sync_status.sync_info.num_shards));
        if (retcode < 0) {
          ldout(sync_env->cct, 0) << "ERROR: failed to init sync, retcode=" << retcode << dendl;
          return set_cr_error(retcode);
        }
        sync_status.sync_info.num_shards = num_shards;
        sync_status.sync_info.state = rgw_data_sync_info::StateBuildingFullSyncMaps;
        /* update new state */
        yield call(set_sync_info_cr());

        if (retcode < 0) {
          ldout(sync_env->cct, 0) << "ERROR: failed to write sync status, retcode=" << retcode << dendl;
          return set_cr_error(retcode);
        }

        *reset_backoff = true;
      }

      if  ((rgw_data_sync_info::SyncState)sync_status.sync_info.state == rgw_data_sync_info::StateBuildingFullSyncMaps) {
        /* state: building full sync maps */
        ldout(sync_env->cct, 20) << __func__ << "(): building full sync maps" << dendl;
        yield call(new RGWListBucketIndexesCR(sync_env, &sync_status));
        sync_status.sync_info.state = rgw_data_sync_info::StateSync;

        /* update new state */
        yield call(set_sync_info_cr());
        if (retcode < 0) {
          ldout(sync_env->cct, 0) << "ERROR: failed to write sync status, retcode=" << retcode << dendl;
          return set_cr_error(retcode);
        }

        *reset_backoff = true;
      }

      yield {
        if  ((rgw_data_sync_info::SyncState)sync_status.sync_info.state == rgw_data_sync_info::StateSync) {
          case rgw_data_sync_info::StateSync:
            for (map<uint32_t, rgw_data_sync_marker>::iterator iter = sync_status.sync_markers.begin();
                 iter != sync_status.sync_markers.end(); ++iter) {
              RGWDataSyncShardControlCR *cr = new RGWDataSyncShardControlCR(sync_env, sync_env->store->get_zone_params().log_pool,
                                                        iter->first, iter->second);
              cr->get();
              shard_crs_lock.Lock();
              shard_crs[iter->first] = cr;
              shard_crs_lock.Unlock();
              spawn(cr, true);
            }
        }
      }

      return set_cr_done();
    }
    return 0;
  }

  RGWCoroutine *set_sync_info_cr() {
    RGWRados *store = sync_env->store;
    return new RGWSimpleRadosWriteCR<rgw_data_sync_info>(sync_env->async_rados, store, store->get_zone_params().log_pool,
                                                         RGWDataSyncStatusManager::sync_status_oid(sync_env->source_zone),
                                                         sync_status.sync_info);
  }

  void wakeup(int shard_id, set<string>& keys) {
    Mutex::Locker l(shard_crs_lock);
    map<int, RGWDataSyncShardControlCR *>::iterator iter = shard_crs.find(shard_id);
    if (iter == shard_crs.end()) {
      return;
    }
    iter->second->append_modified_shards(keys);
    iter->second->wakeup();
  }
};

class RGWDataSyncControlCR : public RGWBackoffControlCR
{
  RGWDataSyncEnv *sync_env;
  uint32_t num_shards;

public:
  RGWDataSyncControlCR(RGWDataSyncEnv *_sync_env, uint32_t _num_shards) : RGWBackoffControlCR(_sync_env->cct, true),
                                                      sync_env(_sync_env), num_shards(_num_shards) {
  }

  RGWCoroutine *alloc_cr() {
    return new RGWDataSyncCR(sync_env, num_shards, backoff_ptr());
  }

  void wakeup(int shard_id, set<string>& keys) {
    Mutex& m = cr_lock();

    m.Lock();
    RGWDataSyncCR *cr = static_cast<RGWDataSyncCR *>(get_cr());
    if (!cr) {
      m.Unlock();
      return;
    }

    cr->get();
    m.Unlock();

    if (cr) {
      cr->wakeup(shard_id, keys);
    }

    cr->put();
  }
};

void RGWRemoteDataLog::wakeup(int shard_id, set<string>& keys) {
  RWLock::RLocker rl(lock);
  if (!data_sync_cr) {
    return;
  }
  data_sync_cr->wakeup(shard_id, keys);
}

int RGWRemoteDataLog::run_sync(int num_shards, rgw_data_sync_status& sync_status)
{
  RGWObjectCtx obj_ctx(store, NULL);

  int r = run(new RGWReadDataSyncStatusCoroutine(&sync_env, obj_ctx, &sync_status));
  if (r < 0 && r != -ENOENT) {
    ldout(store->ctx(), 0) << "ERROR: failed to read sync status from source_zone=" << sync_env.source_zone << " r=" << r << dendl;
    return r;
  }
  
  lock.get_write();
  data_sync_cr = new RGWDataSyncControlCR(&sync_env, num_shards);
  lock.unlock();
  r = run(data_sync_cr);
  if (r < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to run sync" << dendl;
    return r;
  }

  lock.get_write();
  data_sync_cr = NULL;
  lock.unlock();

  return 0;
}

int RGWDataSyncStatusManager::init()
{
  conn = store->get_zone_conn_by_id(source_zone);
  if (!conn) {
    ldout(store->ctx(), 0) << "connection object to zone " << source_zone << " does not exist" << dendl;
    return -EINVAL;
  }

  const char *log_pool = store->get_zone_params().log_pool.name.c_str();
  librados::Rados *rados = store->get_rados_handle();
  int r = rados->ioctx_create(log_pool, ioctx);
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: failed to open log pool (" << store->get_zone_params().log_pool.name << " ret=" << r << dendl;
    return r;
  }

  source_status_obj = rgw_obj(store->get_zone_params().log_pool, RGWDataSyncStatusManager::sync_status_oid(source_zone));

  error_logger = new RGWSyncErrorLogger(store, RGW_SYNC_ERROR_LOG_SHARD_PREFIX, ERROR_LOGGER_SHARDS);

  r = source_log.init(source_zone, conn, error_logger);
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: failed to init remote log, r=" << r << dendl;
    return r;
  }

  rgw_datalog_info datalog_info;
  r = source_log.read_log_info(&datalog_info);
  if (r < 0) {
    ldout(store->ctx(), 5) << "ERROR: master.read_log_info() returned r=" << r << dendl;
    return r;
  }

  num_shards = datalog_info.num_shards;

  for (int i = 0; i < num_shards; i++) {
    shard_objs[i] = rgw_obj(store->get_zone_params().log_pool, shard_obj_name(source_zone, i));
  }

  return 0;
}

string RGWDataSyncStatusManager::sync_status_oid(const string& source_zone)
{
  char buf[datalog_sync_status_oid_prefix.size() + source_zone.size() + 16];
  snprintf(buf, sizeof(buf), "%s.%s", datalog_sync_status_oid_prefix.c_str(), source_zone.c_str());

  return string(buf);
}

string RGWDataSyncStatusManager::shard_obj_name(const string& source_zone, int shard_id)
{
  char buf[datalog_sync_status_shard_prefix.size() + source_zone.size() + 16];
  snprintf(buf, sizeof(buf), "%s.%s.%d", datalog_sync_status_shard_prefix.c_str(), source_zone.c_str(), shard_id);

  return string(buf);
}

int RGWRemoteBucketLog::init(const string& _source_zone, RGWRESTConn *_conn, const string& _bucket_name,
                             const string& _bucket_id, int _shard_id, RGWSyncErrorLogger *_error_logger)
{
  conn = _conn;
  source_zone = _source_zone;
  bucket_name = _bucket_name;
  bucket_id = _bucket_id;
  shard_id = _shard_id;

  sync_env.init(store->ctx(), store, conn, async_rados, http_manager, _error_logger, source_zone);

  return 0;
}

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
  RGWDataSyncEnv *sync_env;

  string bucket_name;
  string bucket_id;
  int shard_id;

  string instance_key;

  bucket_index_marker_info *info;

public:
  RGWReadRemoteBucketIndexLogInfoCR(RGWDataSyncEnv *_sync_env,
                                  const string& _bucket_name, const string& _bucket_id, int _shard_id,
                                  bucket_index_marker_info *_info) : RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
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
    reenter(this) {
      yield {
        rgw_http_param_pair pairs[] = { { "type" , "bucket-index" },
	                                { "bucket-instance", instance_key.c_str() },
					{ "info" , NULL },
	                                { NULL, NULL } };

        string p = "/admin/log/";
        call(new RGWReadRESTResourceCR<bucket_index_marker_info>(sync_env->cct, sync_env->conn, sync_env->http_manager, p, pairs, info));
      }
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }
};

class RGWInitBucketShardSyncStatusCoroutine : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  RGWRados *store;

  string bucket_name;
  string bucket_id;
  int shard_id;

  string sync_status_oid;

  string lock_name;
  string cookie;
  rgw_bucket_shard_sync_info status;

  bucket_index_marker_info info;
public:
  RGWInitBucketShardSyncStatusCoroutine(RGWDataSyncEnv *_sync_env,
                      const string& _bucket_name, const string& _bucket_id, int _shard_id) : RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
                                                                                             bucket_name(_bucket_name), bucket_id(_bucket_id), shard_id(_shard_id) {
    store = sync_env->store;
    lock_name = "sync_lock";

#define COOKIE_LEN 16
    char buf[COOKIE_LEN + 1];

    gen_rand_alphanumeric(cct, buf, sizeof(buf) - 1);
    string cookie = buf;

    sync_status_oid = RGWBucketSyncStatusManager::status_oid(sync_env->source_zone, bucket_name, bucket_id, shard_id);
  }

  int operate() {
    reenter(this) {
      yield {
	uint32_t lock_duration = 30;
	call(new RGWSimpleRadosLockCR(sync_env->async_rados, store, store->get_zone_params().log_pool, sync_status_oid,
			             lock_name, cookie, lock_duration));
	if (retcode < 0) {
	  ldout(cct, 0) << "ERROR: failed to take a lock on " << sync_status_oid << dendl;
	  return set_cr_error(retcode);
	}
      }
      yield call(new RGWSimpleRadosWriteCR<rgw_bucket_shard_sync_info>(sync_env->async_rados, store, store->get_zone_params().log_pool,
				 sync_status_oid, status));
      yield { /* take lock again, we just recreated the object */
	uint32_t lock_duration = 30;
	call(new RGWSimpleRadosLockCR(sync_env->async_rados, store, store->get_zone_params().log_pool, sync_status_oid,
			             lock_name, cookie, lock_duration));
	if (retcode < 0) {
	  ldout(cct, 0) << "ERROR: failed to take a lock on " << sync_status_oid << dendl;
	  return set_cr_error(retcode);
	}
      }
      /* fetch current position in logs */
      yield call(new RGWReadRemoteBucketIndexLogInfoCR(sync_env, bucket_name, bucket_id, shard_id, &info));
      if (retcode < 0 && retcode != -ENOENT) {
        ldout(cct, 0) << "ERROR: failed to fetch bucket index status" << dendl;
        return set_cr_error(retcode);
      }
      yield {
	status.state = rgw_bucket_shard_sync_info::StateFullSync;
        status.inc_marker.position = info.max_marker;
        map<string, bufferlist> attrs;
        status.encode_all_attrs(attrs);
        call(new RGWSimpleRadosWriteAttrsCR(sync_env->async_rados, store, store->get_zone_params().log_pool,
                                            sync_status_oid, attrs));
      }
      yield { /* unlock */
	call(new RGWSimpleRadosUnlockCR(sync_env->async_rados, store, store->get_zone_params().log_pool, sync_status_oid,
			             lock_name, cookie));
      }
      return set_cr_done();
    }
    return 0;
  }
};

RGWCoroutine *RGWRemoteBucketLog::init_sync_status_cr()
{
  return new RGWInitBucketShardSyncStatusCoroutine(&sync_env,
                                                   bucket_name, bucket_id, shard_id);
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
  RGWDataSyncEnv *sync_env;
  RGWObjectCtx obj_ctx;
  string oid;
  rgw_bucket_shard_sync_info *status;

  map<string, bufferlist> attrs;
public:
  RGWReadBucketSyncStatusCoroutine(RGWDataSyncEnv *_sync_env,
                      const string& _bucket_name, const string _bucket_id, int _shard_id,
		      rgw_bucket_shard_sync_info *_status) : RGWCoroutine(_sync_env->cct),
                                                            sync_env(_sync_env),
                                                            obj_ctx(sync_env->store),
                                                            oid(RGWBucketSyncStatusManager::status_oid(sync_env->source_zone, _bucket_name, _bucket_id, _shard_id)),
                                                            status(_status) {}
  int operate();
};

int RGWReadBucketSyncStatusCoroutine::operate()
{
  reenter(this) {
    yield call(new RGWSimpleRadosReadAttrsCR(sync_env->async_rados, sync_env->store, obj_ctx,
                                                   sync_env->store->get_zone_params().log_pool,
                                                   oid,
                                                   &attrs));
    if (retcode == -ENOENT) {
      *status = rgw_bucket_shard_sync_info();
      return set_cr_done();
    }
    if (retcode < 0) {
      ldout(sync_env->cct, 0) << "ERROR: failed to call fetch bucket shard info oid=" << oid << " ret=" << retcode << dendl;
      return set_cr_error(retcode);
    }
    status->decode_from_attrs(sync_env->cct, attrs);
    return set_cr_done();
  }
  return 0;
}
RGWCoroutine *RGWRemoteBucketLog::read_sync_status_cr(rgw_bucket_shard_sync_info *sync_status)
{
  return new RGWReadBucketSyncStatusCoroutine(&sync_env, bucket_name, bucket_id, shard_id, sync_status);
}

RGWBucketSyncStatusManager::~RGWBucketSyncStatusManager() {
  for (map<int, RGWRemoteBucketLog *>::iterator iter = source_logs.begin(); iter != source_logs.end(); ++iter) {
    delete iter->second;
  }
  delete error_logger;
}


struct bucket_entry_owner {
  string id;
  string display_name;

  bucket_entry_owner() {}
  bucket_entry_owner(const string& _id, const string& _display_name) : id(_id), display_name(_display_name) {}

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("ID", id, obj);
    JSONDecoder::decode_json("DisplayName", display_name, obj);
  }
};

struct bucket_list_entry {
  bool delete_marker;
  rgw_obj_key key;
  bool is_latest;
  real_time mtime;
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
    JSONDecoder::decode_json("RgwxMtime", mtime_str, obj);

    struct tm t;
    uint32_t nsec;
    if (parse_iso8601(mtime_str.c_str(), &t, &nsec)) {
      ceph_timespec ts;
      ts.tv_sec = (uint64_t)timegm(&t);
      ts.tv_nsec = nsec;
      mtime = real_clock::from_ceph_timespec(ts);
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
  RGWDataSyncEnv *sync_env;

  string bucket_name;
  string bucket_id;
  int shard_id;

  string instance_key;
  rgw_obj_key marker_position;

  bucket_list_result *result;

public:
  RGWListBucketShardCR(RGWDataSyncEnv *_sync_env,
                                  const string& _bucket_name, const string& _bucket_id, int _shard_id,
                                  rgw_obj_key& _marker_position,
                                  bucket_list_result *_result) : RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
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
        call(new RGWReadRESTResourceCR<bucket_list_result>(sync_env->cct, sync_env->conn, sync_env->http_manager, p, pairs, result));
      }
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }
};

class RGWListBucketIndexLogCR: public RGWCoroutine {
  RGWDataSyncEnv *sync_env;

  string bucket_name;
  string bucket_id;
  int shard_id;

  string instance_key;
  string marker;

  list<rgw_bi_log_entry> *result;

public:
  RGWListBucketIndexLogCR(RGWDataSyncEnv *_sync_env,
                          const string& _bucket_name, const string& _bucket_id, int _shard_id,
                          string& _marker,
                          list<rgw_bi_log_entry> *_result) : RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
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
    reenter(this) {
      yield {
        rgw_http_param_pair pairs[] = { { "bucket-instance", instance_key.c_str() },
					{ "format" , "json" },
					{ "marker" , marker.c_str() },
					{ "type", "bucket-index" },
	                                { NULL, NULL } };

        call(new RGWReadRESTResourceCR<list<rgw_bi_log_entry> >(sync_env->cct, sync_env->conn, sync_env->http_manager, "/admin/log", pairs, result));
      }
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }
};

#define BUCKET_SYNC_UPDATE_MARKER_WINDOW 10

class RGWBucketFullSyncShardMarkerTrack : public RGWSyncShardMarkerTrack<rgw_obj_key, rgw_obj_key> {
  RGWDataSyncEnv *sync_env;

  string marker_oid;
  rgw_bucket_shard_full_sync_marker sync_marker;

public:
  RGWBucketFullSyncShardMarkerTrack(RGWDataSyncEnv *_sync_env,
                         const string& _marker_oid,
                         const rgw_bucket_shard_full_sync_marker& _marker) : RGWSyncShardMarkerTrack(BUCKET_SYNC_UPDATE_MARKER_WINDOW),
                                                                sync_env(_sync_env),
                                                                marker_oid(_marker_oid),
                                                                sync_marker(_marker) {}

  RGWCoroutine *store_marker(const rgw_obj_key& new_marker, uint64_t index_pos, const real_time& timestamp) {
    sync_marker.position = new_marker;
    sync_marker.count = index_pos;

    map<string, bufferlist> attrs;
    sync_marker.encode_attr(attrs);

    RGWRados *store = sync_env->store;

    ldout(sync_env->cct, 20) << __func__ << "(): updating marker marker_oid=" << marker_oid << " marker=" << new_marker << dendl;
    return new RGWSimpleRadosWriteAttrsCR(sync_env->async_rados, store, store->get_zone_params().log_pool,
				 marker_oid, attrs);
  }
};

class RGWBucketIncSyncShardMarkerTrack : public RGWSyncShardMarkerTrack<string, rgw_obj_key> {
  RGWDataSyncEnv *sync_env;

  string marker_oid;
  rgw_bucket_shard_inc_sync_marker sync_marker;

  map<rgw_obj_key, pair<RGWModifyOp, string> > key_to_marker;
  map<string, rgw_obj_key> marker_to_key;

  void handle_finish(const string& marker) {
    map<string, rgw_obj_key>::iterator iter = marker_to_key.find(marker);
    if (iter == marker_to_key.end()) {
      return;
    }
    key_to_marker.erase(iter->second);
    reset_need_retry(iter->second);
    marker_to_key.erase(iter);
  }

public:
  RGWBucketIncSyncShardMarkerTrack(RGWDataSyncEnv *_sync_env,
                         const string& _marker_oid,
                         const rgw_bucket_shard_inc_sync_marker& _marker) : RGWSyncShardMarkerTrack(BUCKET_SYNC_UPDATE_MARKER_WINDOW),
                                                                sync_env(_sync_env),
                                                                marker_oid(_marker_oid),
                                                                sync_marker(_marker) {}

  RGWCoroutine *store_marker(const string& new_marker, uint64_t index_pos, const real_time& timestamp) {
    sync_marker.position = new_marker;

    map<string, bufferlist> attrs;
    sync_marker.encode_attr(attrs);

    RGWRados *store = sync_env->store;

    ldout(sync_env->cct, 20) << __func__ << "(): updating marker marker_oid=" << marker_oid << " marker=" << new_marker << dendl;
    return new RGWSimpleRadosWriteAttrsCR(sync_env->async_rados, store, store->get_zone_params().log_pool,
				 marker_oid, attrs);
  }

  /*
   * create index from key -> <op, marker>, and from marker -> key
   * this is useful so that we can insure that we only have one
   * entry for any key that is used. This is needed when doing
   * incremenatl sync of data, and we don't want to run multiple
   * concurrent sync operations for the same bucket shard 
   * Also, we should make sure that we don't run concurrent operations on the same key with
   * different ops.
   */
  bool index_key_to_marker(const rgw_obj_key& key, RGWModifyOp op, const string& marker) {
    if (key_to_marker.find(key) != key_to_marker.end()) {
      set_need_retry(key);
      return false;
    }
    key_to_marker[key] = make_pair<>(op, marker);
    marker_to_key[marker] = key;
    return true;
  }

  bool can_do_op(const rgw_obj_key& key, RGWModifyOp op) {
    auto i = key_to_marker.find(key);
    if (i == key_to_marker.end()) {
      return true;
    }

    return (i->second.first == op);
  }
};

template <class T, class K>
class RGWBucketSyncSingleEntryCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;

  RGWBucketInfo *bucket_info;
  int shard_id;

  rgw_obj_key key;
  bool versioned;
  uint64_t versioned_epoch;
  bucket_entry_owner owner;
  real_time timestamp;
  RGWModifyOp op;
  RGWPendingState op_state;

  T entry_marker;
  RGWSyncShardMarkerTrack<T, K> *marker_tracker;

  int sync_status;

  stringstream error_ss;


public:
  RGWBucketSyncSingleEntryCR(RGWDataSyncEnv *_sync_env,
                             RGWBucketInfo *_bucket_info, int _shard_id,
                             const rgw_obj_key& _key, bool _versioned, uint64_t _versioned_epoch,
                             real_time& _timestamp,
                             const bucket_entry_owner& _owner,
                             RGWModifyOp _op, RGWPendingState _op_state,
		             const T& _entry_marker, RGWSyncShardMarkerTrack<T, K> *_marker_tracker) : RGWCoroutine(_sync_env->cct),
						      sync_env(_sync_env),
                                                      bucket_info(_bucket_info), shard_id(_shard_id),
                                                      key(_key), versioned(_versioned), versioned_epoch(_versioned_epoch),
                                                      owner(_owner),
                                                      timestamp(_timestamp), op(_op),
                                                      op_state(_op_state),
                                                      entry_marker(_entry_marker),
                                                      marker_tracker(_marker_tracker),
                                                      sync_status(0) {
    set_description() << "bucket sync single entry (source_zone=" << sync_env->source_zone << ") b=" << bucket_info->bucket << ":" << shard_id <<"/" << key << "[" << versioned_epoch << "] log_entry=" << entry_marker << " op=" << (int)op << " op_state=" << (int)op_state;
    ldout(sync_env->cct, 20) << "bucket sync single entry (source_zone=" << sync_env->source_zone << ") b=" << bucket_info->bucket << ":" << shard_id <<"/" << key << "[" << versioned_epoch << "] log_entry=" << entry_marker << " op=" << (int)op << " op_state=" << (int)op_state << dendl;
    set_status("init");
  }

  int operate() {
    reenter(this) {
      /* skip entries that are not complete */
      if (op_state != CLS_RGW_STATE_COMPLETE) {
        goto done;
      }
      do {
        yield {
          marker_tracker->reset_need_retry(key);
          if (key.name.empty()) {
            /* shouldn't happen */
            set_status("skipping empty entry");
            ldout(sync_env->cct, 0) << "ERROR: " << __func__ << "(): entry with empty obj name, skipping" << dendl;
            goto done;
          }
          if (op == CLS_RGW_OP_ADD ||
              op == CLS_RGW_OP_LINK_OLH) {
            if (op == CLS_RGW_OP_ADD && !key.instance.empty() && key.instance != "null") {
              set_status("skipping entry");
              ldout(sync_env->cct, 10) << "bucket skipping sync obj: " << sync_env->source_zone << "/" << bucket_info->bucket << "/" << key << "[" << versioned_epoch << "]: versioned object will be synced on link_olh" << dendl;
              goto done;

            }
            set_status("syncing obj");
            ldout(sync_env->cct, 5) << "bucket sync: sync obj: " << sync_env->source_zone << "/" << bucket_info->bucket << "/" << key << "[" << versioned_epoch << "]" << dendl;
            call(new RGWFetchRemoteObjCR(sync_env->async_rados, sync_env->store, sync_env->source_zone, *bucket_info,
                                         key, versioned_epoch,
                                         true));
          } else if (op == CLS_RGW_OP_DEL || op == CLS_RGW_OP_UNLINK_INSTANCE) {
            set_status("removing obj");
            if (op == CLS_RGW_OP_UNLINK_INSTANCE) {
              versioned = true;
            }
            call(new RGWRemoveObjCR(sync_env->async_rados, sync_env->store, sync_env->source_zone, *bucket_info, key, versioned, versioned_epoch, NULL, NULL, false, &timestamp));
          } else if (op == CLS_RGW_OP_LINK_OLH_DM) {
            set_status("creating delete marker");
            ldout(sync_env->cct, 10) << "creating delete marker: obj: " << sync_env->source_zone << "/" << bucket_info->bucket << "/" << key << "[" << versioned_epoch << "]" << dendl;
            call(new RGWRemoveObjCR(sync_env->async_rados, sync_env->store, sync_env->source_zone, *bucket_info, key, versioned, versioned_epoch, &owner.id, &owner.display_name, true, &timestamp));
          }
        }
      } while (marker_tracker->need_retry(key));
      if (retcode < 0 && retcode != -ENOENT) {
        set_status() << "failed to sync obj; retcode=" << retcode;
        rgw_bucket& bucket = bucket_info->bucket;
        ldout(sync_env->cct, 0) << "ERROR: failed to sync object: " << bucket.name << ":" << bucket.bucket_id << ":" << shard_id << "/" << key.name << dendl;
        error_ss << bucket.name << ":" << bucket.bucket_id << ":" << shard_id << "/" << key.name;
        sync_status = retcode;
      }
      if (!error_ss.str().empty()) {
        yield call(sync_env->error_logger->log_error_cr(sync_env->conn->get_remote_id(), "data", error_ss.str(), retcode, "failed to sync object"));
      }
done:
      /* update marker */
      set_status() << "calling marker_tracker->finish(" << entry_marker << ")";
      yield call(marker_tracker->finish(entry_marker));
      if (sync_status == 0) {
        sync_status = retcode;
      }
      if (sync_status < 0) {
        return set_cr_error(sync_status);
      }
      return set_cr_done();
    }
    return 0;
  }
};

#define BUCKET_SYNC_SPAWN_WINDOW 20

class RGWBucketShardFullSyncCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  string bucket_name;
  string bucket_id;
  int shard_id;
  RGWBucketInfo *bucket_info;
  bucket_list_result list_result;
  list<bucket_list_entry>::iterator entries_iter;
  rgw_bucket_shard_full_sync_marker full_marker;
  RGWBucketFullSyncShardMarkerTrack *marker_tracker;
  int spawn_window;
  rgw_obj_key list_marker;
  bucket_list_entry *entry;
  RGWModifyOp op;

  int total_entries;

  RGWContinuousLeaseCR *lease_cr;

  string status_oid;
public:
  RGWBucketShardFullSyncCR(RGWDataSyncEnv *_sync_env,
                           const string& _bucket_name, const string _bucket_id, int _shard_id,
                           RGWBucketInfo *_bucket_info,  rgw_bucket_shard_full_sync_marker& _full_marker) : RGWCoroutine(_sync_env->cct),
									    sync_env(_sync_env),
                                                                            bucket_name(_bucket_name),
									    bucket_id(_bucket_id), shard_id(_shard_id),
                                                                            bucket_info(_bucket_info),
                                                                            full_marker(_full_marker), marker_tracker(NULL),
                                                                            spawn_window(BUCKET_SYNC_SPAWN_WINDOW), entry(NULL),
                                                                            op(CLS_RGW_OP_ADD),
                                                                            total_entries(0), lease_cr(NULL) {
    status_oid = RGWBucketSyncStatusManager::status_oid(sync_env->source_zone, bucket_name, bucket_id, shard_id);
  }

  ~RGWBucketShardFullSyncCR() {
    delete marker_tracker;
    if (lease_cr) {
      lease_cr->abort();
      lease_cr->put();
    }
  }
  int operate();
};

int RGWBucketShardFullSyncCR::operate()
{
  int ret;
  reenter(this) {
    yield {
      set_status("acquiring sync lock");
      uint32_t lock_duration = cct->_conf->rgw_sync_lease_period;
      string lock_name = "sync_lock";
      RGWRados *store = sync_env->store;
      lease_cr = new RGWContinuousLeaseCR(sync_env->async_rados, store, store->get_zone_params().log_pool, status_oid,
                                          lock_name, lock_duration, this);
      lease_cr->get();
      spawn(lease_cr, false);
    }
    while (!lease_cr->is_locked()) {
      if (lease_cr->is_done()) {
        ldout(cct, 0) << "ERROR: lease cr failed, done early " << dendl;
        set_status("lease lock failed, early abort");
        return set_cr_error(lease_cr->get_ret_status());
      }
      set_sleeping(true);
      yield;
    }
    set_status("lock acquired");
    list_marker = full_marker.position;
    marker_tracker = new RGWBucketFullSyncShardMarkerTrack(sync_env, status_oid, full_marker);

    total_entries = full_marker.count;
    do {
      set_status("listing remote bucket");
      ldout(sync_env->cct, 20) << __func__ << "(): listing bucket for full sync" << dendl;
      yield call(new RGWListBucketShardCR(sync_env, bucket_name, bucket_id, shard_id,
                                          list_marker, &list_result));
      if (retcode < 0 && retcode != -ENOENT) {
        set_status("failed bucket listing, going down");
        yield lease_cr->go_down();
        drain_all();
        return set_cr_error(retcode);
      }
      entries_iter = list_result.entries.begin();
      for (; entries_iter != list_result.entries.end(); ++entries_iter) {
        ldout(sync_env->cct, 20) << "[full sync] syncing object: " << bucket_name << ":" << bucket_id << ":" << shard_id << "/" << entries_iter->key << dendl;
        entry = &(*entries_iter);
        total_entries++;
        list_marker = entries_iter->key;
        if (!marker_tracker->start(entry->key, total_entries, real_time())) {
          ldout(sync_env->cct, 0) << "ERROR: cannot start syncing " << entry->key << ". Duplicate entry?" << dendl;
        } else {
          op = (entry->key.instance.empty() || entry->key.instance == "null" ? CLS_RGW_OP_ADD : CLS_RGW_OP_LINK_OLH);

          yield {
            spawn(new RGWBucketSyncSingleEntryCR<rgw_obj_key, rgw_obj_key>(sync_env, bucket_info, shard_id,
                                                                           entry->key,
                                                                           false, /* versioned, only matters for object removal */
                                                                           entry->versioned_epoch, entry->mtime,
                                                                           entry->owner, op, CLS_RGW_STATE_COMPLETE, entry->key, marker_tracker), false);
          }
        }
        while ((int)num_spawned() > spawn_window) {
          yield wait_for_child();
          while (collect(&ret)) {
            if (ret < 0) {
              ldout(sync_env->cct, 0) << "ERROR: a sync operation returned error" << dendl;
              /* we have reported this error */
            }
          }
        }
      }
    } while (list_result.is_truncated);
    set_status("done iterating over all objects");
    /* wait for all operations to complete */
    drain_all_but(1); /* still need to hold lease cr */
    /* update sync state to incremental */
    yield {
      rgw_bucket_shard_sync_info sync_status;
      sync_status.state = rgw_bucket_shard_sync_info::StateIncrementalSync;
      map<string, bufferlist> attrs;
      sync_status.encode_state_attr(attrs);
      string oid = RGWBucketSyncStatusManager::status_oid(sync_env->source_zone, bucket_name, bucket_id, shard_id);
      RGWRados *store = sync_env->store;
      call(new RGWSimpleRadosWriteAttrsCR(sync_env->async_rados, store, store->get_zone_params().log_pool,
                                          oid, attrs));
    }
    yield lease_cr->go_down();
    drain_all();
    if (retcode < 0) {
      ldout(sync_env->cct, 0) << "ERROR: failed to set sync state on bucket " << bucket_name << ":" << bucket_id << ":" << shard_id
        << " retcode=" << retcode << dendl;
      return set_cr_error(retcode);
    }
    return set_cr_done();
  }
  return 0;
}

class RGWBucketShardIncrementalSyncCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  string bucket_name;
  string bucket_id;
  int shard_id;
  RGWBucketInfo *bucket_info;
  list<rgw_bi_log_entry> list_result;
  list<rgw_bi_log_entry>::iterator entries_iter;
  rgw_bucket_shard_inc_sync_marker inc_marker;
  rgw_obj_key key;
  rgw_bi_log_entry *entry;
  RGWBucketIncSyncShardMarkerTrack *marker_tracker;
  int spawn_window;
  bool updated_status;
  RGWContinuousLeaseCR *lease_cr;
  string status_oid;

  string name;
  string instance;
  string ns;

  string cur_id;



public:
  RGWBucketShardIncrementalSyncCR(RGWDataSyncEnv *_sync_env,
                           const string& _bucket_name, const string _bucket_id, int _shard_id,
                           RGWBucketInfo *_bucket_info, rgw_bucket_shard_inc_sync_marker& _inc_marker) : RGWCoroutine(_sync_env->cct),
                                                                            sync_env(_sync_env),
                                                                            bucket_name(_bucket_name),
									    bucket_id(_bucket_id), shard_id(_shard_id),
                                                                            bucket_info(_bucket_info),
                                                                            inc_marker(_inc_marker), entry(NULL), marker_tracker(NULL),
                                                                            spawn_window(BUCKET_SYNC_SPAWN_WINDOW), updated_status(false),
                                                                            lease_cr(NULL) {
    status_oid = RGWBucketSyncStatusManager::status_oid(sync_env->source_zone, bucket_name, bucket_id, shard_id);
    set_description() << "bucket shard incremental sync bucket=" << _bucket_name << ":" << _bucket_id << ":" << _shard_id;
    set_status("init");
  }

  ~RGWBucketShardIncrementalSyncCR() {
    if (lease_cr) {
      lease_cr->abort();
      lease_cr->put();
    }
    delete marker_tracker;
  }
  int operate();
};

int RGWBucketShardIncrementalSyncCR::operate()
{
  int ret;
  reenter(this) {
    yield {
      set_status("acquiring sync lock");
      uint32_t lock_duration = cct->_conf->rgw_sync_lease_period;
      string lock_name = "sync_lock";
      RGWRados *store = sync_env->store;
      lease_cr = new RGWContinuousLeaseCR(sync_env->async_rados, store, store->get_zone_params().log_pool, status_oid,
                                          lock_name, lock_duration, this);
      lease_cr->get();
      spawn(lease_cr, false);
    }
    while (!lease_cr->is_locked()) {
      if (lease_cr->is_done()) {
        ldout(cct, 0) << "ERROR: lease cr failed, done early " << dendl;
        set_status("lease lock failed, early abort");
        return set_cr_error(lease_cr->get_ret_status());
      }
      set_sleeping(true);
      yield;
    }
    marker_tracker = new RGWBucketIncSyncShardMarkerTrack(sync_env,
                                                          status_oid,
                                                          inc_marker);
    do {
      ldout(sync_env->cct, 20) << __func__ << "(): listing bilog for incremental sync" << dendl;
      set_status() << "listing bilog; position=" << inc_marker.position;
      yield call(new RGWListBucketIndexLogCR(sync_env, bucket_name, bucket_id, shard_id,
                                         inc_marker.position, &list_result));
      if (retcode < 0 && retcode != -ENOENT) {
        /* wait for all operations to complete */
        lease_cr->go_down();
        drain_all();
        return set_cr_error(retcode);
      }
      entries_iter = list_result.begin();
      for (; entries_iter != list_result.end(); ++entries_iter) {
        entry = &(*entries_iter);
        {
          ssize_t p = entry->id.find('#'); /* entries might have explicit shard info in them, e.g., 6#00000000004.94.3 */
          if (p < 0) {
            cur_id = entry->id;
          } else {
            cur_id = entry->id.substr(p + 1);
          }
        }
        inc_marker.position = cur_id;

        if (!rgw_obj::parse_raw_oid(entries_iter->object, &name, &instance, &ns)) {
          set_status() << "parse_raw_oid() on " << entries_iter->object << " returned false, skipping entry";
          ldout(sync_env->cct, 20) << "parse_raw_oid() on " << entries_iter->object << " returned false, skipping entry" << dendl;
          marker_tracker->try_update_high_marker(cur_id, 0, entries_iter->timestamp);
          continue;
        }

        ldout(sync_env->cct, 20) << "parsed entry: id=" << cur_id << " iter->object=" << entry->object << " iter->instance=" << entry->instance << " name=" << name << " instance=" << instance << " ns=" << ns << dendl;

        if (!ns.empty()) {
          set_status() << "skipping entry in namespace: " << entry->object;
          ldout(sync_env->cct, 20) << "skipping entry in namespace: " << entry->object << dendl;
          marker_tracker->try_update_high_marker(cur_id, 0, entry->timestamp);
          continue;
        }

        key = rgw_obj_key(name, entry->instance);
        set_status() << "got entry.id=" << cur_id << " key=" << key << " op=" << (int)entry->op;
        if (entry->op == CLS_RGW_OP_CANCEL) {
          set_status() << "canceled operation, skipping";
          ldout(sync_env->cct, 20) << "[inc sync] skipping object: " << bucket_name << ":" << bucket_id << ":" << shard_id << "/" << key << ": canceled operation" << dendl;
          marker_tracker->try_update_high_marker(cur_id, 0, entry->timestamp);
          continue;
        }
        if (entry->state != CLS_RGW_STATE_COMPLETE) {
          set_status() << "non-complete operation, skipping";
          ldout(sync_env->cct, 20) << "[inc sync] skipping object: " << bucket_name << ":" << bucket_id << ":" << shard_id << "/" << key << ": non-complete operation" << dendl;
          marker_tracker->try_update_high_marker(cur_id, 0, entry->timestamp);
          continue;
        }
        ldout(sync_env->cct, 20) << "[inc sync] syncing object: " << bucket_name << ":" << bucket_id << ":" << shard_id << "/" << key << dendl;
        updated_status = false;
        while (!marker_tracker->can_do_op(key, entry->op)) {
          if (!updated_status) {
            set_status() << "can't do op, conflicting inflight operation";
            updated_status = true;
          }
          ldout(sync_env->cct, 5) << *this << ": [inc sync] can't do op on key=" << key << " need to wait for conflicting operation to complete" << dendl;
          yield wait_for_child();
          
        }
        if (!marker_tracker->index_key_to_marker(key, entry->op, cur_id)) {
          set_status() << "can't do op, sync already in progress for object";
          ldout(sync_env->cct, 20) << __func__ << ": skipping sync of entry: " << cur_id << ":" << key << " sync already in progress for object" << dendl;
          marker_tracker->try_update_high_marker(cur_id, 0, entry->timestamp);
          continue;
        }
        // yield {
          set_status() << "start object sync";
          if (!marker_tracker->start(cur_id, 0, entry->timestamp)) {
            ldout(sync_env->cct, 0) << "ERROR: cannot start syncing " << cur_id << ". Duplicate entry?" << dendl;
          } else {
            uint64_t versioned_epoch = 0;
            bucket_entry_owner owner(entry->owner, entry->owner_display_name);
            if (entry->ver.pool < 0) {
              versioned_epoch = entry->ver.epoch;
            }
            ldout(sync_env->cct, 20) << __func__ << "(): entry->timestamp=" << entry->timestamp << dendl;
            spawn(new RGWBucketSyncSingleEntryCR<string, rgw_obj_key>(sync_env, bucket_info, shard_id,
                                                         key, entry->is_versioned(), versioned_epoch, entry->timestamp, owner, entry->op,
                                                         entry->state, cur_id, marker_tracker), false);
          }
        // }
        while ((int)num_spawned() > spawn_window) {
          set_status() << "num_spawned() > spawn_window";
          yield wait_for_child();
          while (collect(&ret)) {
            if (ret < 0) {
              ldout(sync_env->cct, 0) << "ERROR: a sync operation returned error" << dendl;
              /* we have reported this error */
            }
            /* not waiting for child here */
          }
        }
      }
    } while (!list_result.empty());

    yield {
      call(marker_tracker->flush());
    }
    if (retcode < 0) {
      ldout(sync_env->cct, 0) << "ERROR: marker_tracker->flush() returned retcode=" << retcode << dendl;
      lease_cr->go_down();
      drain_all();
      return set_cr_error(retcode);
    }

    lease_cr->go_down();
    /* wait for all operations to complete */
    drain_all();

    return set_cr_done();
  }
  return 0;
}

int RGWRunBucketSyncCoroutine::operate()
{
  reenter(this) {
    yield call(new RGWReadBucketSyncStatusCoroutine(sync_env, bucket_name, bucket_id, shard_id, &sync_status));
    if (retcode < 0 && retcode != -ENOENT) {
      ldout(sync_env->cct, 0) << "ERROR: failed to read sync status for bucket=" << bucket_name << " bucket_id=" << bucket_id << " shard_id=" << shard_id << dendl;
      return set_cr_error(retcode);
    }

    ldout(sync_env->cct, 20) << __func__ << "(): sync status for bucket " << bucket_name << ":" << bucket_id << ":" << shard_id << ": " << sync_status.state << dendl;

    yield call(new RGWGetBucketInstanceInfoCR(sync_env->async_rados, sync_env->store, bucket_name, bucket_id, &bucket_info));
    if (retcode == -ENOENT) {
      /* bucket instance info has not been synced in yet, fetch it now */
      yield {
        ldout(sync_env->cct, 10) << "no local info for bucket " << bucket_name << ":" << bucket_id << ": fetching metadata" << dendl;
        string raw_key = string("bucket.instance:") + bucket_name + ":" + bucket_id;

        meta_sync_env.init(cct, sync_env->store, sync_env->store->rest_master_conn, sync_env->async_rados, sync_env->http_manager, sync_env->error_logger);

        call(new RGWMetaSyncSingleEntryCR(&meta_sync_env, raw_key,
                                          string() /* no marker */,
                                          MDLOG_STATUS_COMPLETE,
                                          NULL /* no marker tracker */));
      }
      if (retcode < 0) {
        ldout(sync_env->cct, 0) << "ERROR: failed to fetch bucket instance info for " << bucket_name << ":" << bucket_id << dendl;
        return set_cr_error(retcode);
      }

      yield call(new RGWGetBucketInstanceInfoCR(sync_env->async_rados, sync_env->store, bucket_name, bucket_id, &bucket_info));
    }
    if (retcode < 0) {
      ldout(sync_env->cct, 0) << "ERROR: failed to retrieve bucket info for bucket=" << bucket_name << " bucket_id=" << bucket_id << dendl;
      return set_cr_error(retcode);
    }

    yield {
      if ((rgw_bucket_shard_sync_info::SyncState)sync_status.state == rgw_bucket_shard_sync_info::StateInit) {
        call(new RGWInitBucketShardSyncStatusCoroutine(sync_env, bucket_name, bucket_id, shard_id));
        sync_status.state = rgw_bucket_shard_sync_info::StateFullSync;
      }
    }

    if (retcode < 0) {
      ldout(sync_env->cct, 0) << "ERROR: init sync on " << bucket_name << " bucket_id=" << bucket_id << " shard_id=" << shard_id << " failed, retcode=" << retcode << dendl;
      return set_cr_error(retcode);
    }
    yield {
      if ((rgw_bucket_shard_sync_info::SyncState)sync_status.state == rgw_bucket_shard_sync_info::StateFullSync) {
        call(new RGWBucketShardFullSyncCR(sync_env, bucket_name, bucket_id, shard_id,
                                          &bucket_info, sync_status.full_marker));
        sync_status.state = rgw_bucket_shard_sync_info::StateIncrementalSync;
      }
    }
    if (retcode < 0) {
      ldout(sync_env->cct, 0) << "ERROR: full sync on " << bucket_name << " bucket_id=" << bucket_id << " shard_id=" << shard_id << " failed, retcode=" << retcode << dendl;
      return set_cr_error(retcode);
    }

    yield {
      if ((rgw_bucket_shard_sync_info::SyncState)sync_status.state == rgw_bucket_shard_sync_info::StateIncrementalSync) {
        call(new RGWBucketShardIncrementalSyncCR(sync_env, bucket_name, bucket_id, shard_id,
                                                 &bucket_info, sync_status.inc_marker));
      }
    }
    if (retcode < 0) {
      ldout(sync_env->cct, 0) << "ERROR: incremental sync on " << bucket_name << " bucket_id=" << bucket_id << " shard_id=" << shard_id << " failed, retcode=" << retcode << dendl;
      return set_cr_error(retcode);
    }

    return set_cr_done();
  }

  return 0;
}

RGWCoroutine *RGWRemoteBucketLog::run_sync_cr()
{
  return new RGWRunBucketSyncCoroutine(&sync_env, bucket_name, bucket_id, shard_id);
}

int RGWBucketSyncStatusManager::init()
{
  conn = store->get_zone_conn_by_id(source_zone);
  if (!conn) {
    ldout(store->ctx(), 0) << "connection object to zone " << source_zone << " does not exist" << dendl;
    return -EINVAL;
  }

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

  error_logger = new RGWSyncErrorLogger(store, RGW_SYNC_ERROR_LOG_SHARD_PREFIX, ERROR_LOGGER_SHARDS);

  int effective_num_shards = (num_shards ? num_shards : 1);

  for (int i = 0; i < effective_num_shards; i++) {
    RGWRemoteBucketLog *l = new RGWRemoteBucketLog(store, this, async_rados, &http_manager);
    ret = l->init(source_zone, conn, bucket_name, bucket_id, (num_shards ? i : -1), error_logger);
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
  list<RGWCoroutinesStack *> stacks;

  for (map<int, RGWRemoteBucketLog *>::iterator iter = source_logs.begin(); iter != source_logs.end(); ++iter) {
    RGWCoroutinesStack *stack = new RGWCoroutinesStack(store->ctx(), &cr_mgr);
    RGWRemoteBucketLog *l = iter->second;
    stack->call(l->init_sync_status_cr());

    stacks.push_back(stack);
  }

  return cr_mgr.run(stacks);
}

int RGWBucketSyncStatusManager::read_sync_status()
{
  list<RGWCoroutinesStack *> stacks;

  for (map<int, RGWRemoteBucketLog *>::iterator iter = source_logs.begin(); iter != source_logs.end(); ++iter) {
    RGWCoroutinesStack *stack = new RGWCoroutinesStack(store->ctx(), &cr_mgr);
    RGWRemoteBucketLog *l = iter->second;
    stack->call(l->read_sync_status_cr(&sync_status[iter->first]));

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
  list<RGWCoroutinesStack *> stacks;

  for (map<int, RGWRemoteBucketLog *>::iterator iter = source_logs.begin(); iter != source_logs.end(); ++iter) {
    RGWCoroutinesStack *stack = new RGWCoroutinesStack(store->ctx(), &cr_mgr);
    RGWRemoteBucketLog *l = iter->second;
    stack->call(l->run_sync_cr());

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

