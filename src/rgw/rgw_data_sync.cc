// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "common/ceph_json.h"
#include "common/RWLock.h"
#include "common/RefCountedObj.h"
#include "common/WorkQueue.h"
#include "common/Throttle.h"
#include "common/errno.h"

#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_zone.h"
#include "rgw_sync.h"
#include "rgw_data_sync.h"
#include "rgw_rest_conn.h"
#include "rgw_cr_rados.h"
#include "rgw_cr_rest.h"
#include "rgw_cr_tools.h"
#include "rgw_http_client.h"
#include "rgw_bucket.h"
#include "rgw_bucket_sync.h"
#include "rgw_bucket_sync_cache.h"
#include "rgw_metadata.h"
#include "rgw_sync_counters.h"
#include "rgw_sync_error_repo.h"
#include "rgw_sync_module.h"
#include "rgw_sal.h"

#include "cls/lock/cls_lock_client.h"

#include "services/svc_zone.h"
#include "services/svc_sync_modules.h"
#include "services/svc_datalog_rados.h"

#include "include/common_fwd.h"
#include "include/random.h"

#include <boost/asio/yield.hpp>
#include <string_view>

#define dout_subsys ceph_subsys_rgw

#undef dout_prefix
#define dout_prefix (*_dout << "data sync: ")

static string datalog_sync_status_oid_prefix = "datalog.sync-status";
static string datalog_sync_status_shard_prefix = "datalog.sync-status.shard";
static string datalog_sync_full_sync_index_prefix = "data.full-sync.index";
static string bucket_status_oid_prefix = "bucket.sync-status";
static string object_status_oid_prefix = "bucket.sync-status";


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


class RGWReadDataSyncStatusMarkersCR : public RGWShardCollectCR {
  static constexpr int MAX_CONCURRENT_SHARDS = 16;

  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *env;
  const int num_shards;
  int shard_id{0};;

  map<uint32_t, rgw_data_sync_marker>& markers;

 public:
  RGWReadDataSyncStatusMarkersCR(RGWDataSyncCtx *sc, int num_shards,
                                 map<uint32_t, rgw_data_sync_marker>& markers)
    : RGWShardCollectCR(sc->cct, MAX_CONCURRENT_SHARDS),
      sc(sc), env(sc->env), num_shards(num_shards), markers(markers)
  {}
  bool spawn_next() override;
};

bool RGWReadDataSyncStatusMarkersCR::spawn_next()
{
  if (shard_id >= num_shards) {
    return false;
  }
  using CR = RGWSimpleRadosReadCR<rgw_data_sync_marker>;
  spawn(new CR(env->async_rados, env->svc->sysobj,
               rgw_raw_obj(env->svc->zone->get_zone_params().log_pool, RGWDataSyncStatusManager::shard_obj_name(sc->source_zone, shard_id)),
               &markers[shard_id]),
        false);
  shard_id++;
  return true;
}

class RGWReadDataSyncRecoveringShardsCR : public RGWShardCollectCR {
  static constexpr int MAX_CONCURRENT_SHARDS = 16;

  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *env;

  uint64_t max_entries;
  int num_shards;
  int shard_id{0};

  string marker;
  std::vector<RGWRadosGetOmapKeysCR::ResultPtr>& omapkeys;

 public:
  RGWReadDataSyncRecoveringShardsCR(RGWDataSyncCtx *sc, uint64_t _max_entries, int _num_shards,
                                    std::vector<RGWRadosGetOmapKeysCR::ResultPtr>& omapkeys)
    : RGWShardCollectCR(sc->cct, MAX_CONCURRENT_SHARDS), sc(sc), env(sc->env),
      max_entries(_max_entries), num_shards(_num_shards), omapkeys(omapkeys)
  {}
  bool spawn_next() override;
};

bool RGWReadDataSyncRecoveringShardsCR::spawn_next()
{
  if (shard_id >= num_shards)
    return false;
 
  string error_oid = RGWDataSyncStatusManager::shard_obj_name(sc->source_zone, shard_id) + ".retry";
  auto& shard_keys = omapkeys[shard_id];
  shard_keys = std::make_shared<RGWRadosGetOmapKeysCR::Result>();
  spawn(new RGWRadosGetOmapKeysCR(env->store, rgw_raw_obj(env->svc->zone->get_zone_params().log_pool, error_oid),
                                  marker, max_entries, shard_keys), false);

  ++shard_id;
  return true;
}

class RGWReadDataSyncStatusCoroutine : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  rgw_data_sync_status *sync_status;

public:
  RGWReadDataSyncStatusCoroutine(RGWDataSyncCtx *_sc,
                                 rgw_data_sync_status *_status)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(sc->env), sync_status(_status)
  {}
  int operate() override;
};

int RGWReadDataSyncStatusCoroutine::operate()
{
  reenter(this) {
    // read sync info
    using ReadInfoCR = RGWSimpleRadosReadCR<rgw_data_sync_info>;
    yield {
      bool empty_on_enoent = false; // fail on ENOENT
      call(new ReadInfoCR(sync_env->async_rados, sync_env->svc->sysobj,
                          rgw_raw_obj(sync_env->svc->zone->get_zone_params().log_pool, RGWDataSyncStatusManager::sync_status_oid(sc->source_zone)),
                          &sync_status->sync_info, empty_on_enoent));
    }
    if (retcode < 0) {
      ldout(sync_env->cct, 4) << "failed to read sync status info with "
          << cpp_strerror(retcode) << dendl;
      return set_cr_error(retcode);
    }
    // read shard markers
    using ReadMarkersCR = RGWReadDataSyncStatusMarkersCR;
    yield call(new ReadMarkersCR(sc, sync_status->sync_info.num_shards,
                                 sync_status->sync_markers));
    if (retcode < 0) {
      ldout(sync_env->cct, 4) << "failed to read sync status markers with "
          << cpp_strerror(retcode) << dendl;
      return set_cr_error(retcode);
    }
    return set_cr_done();
  }
  return 0;
}

class RGWReadRemoteDataLogShardInfoCR : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;

  RGWRESTReadResource *http_op;

  int shard_id;
  RGWDataChangesLogInfo *shard_info;

public:
  RGWReadRemoteDataLogShardInfoCR(RGWDataSyncCtx *_sc,
                                  int _shard_id, RGWDataChangesLogInfo *_shard_info) : RGWCoroutine(_sc->cct),
                                                      sc(_sc),
                                                      sync_env(_sc->env),
                                                      http_op(NULL),
                                                      shard_id(_shard_id),
                                                      shard_info(_shard_info) {
  }

  ~RGWReadRemoteDataLogShardInfoCR() override {
    if (http_op) {
      http_op->put();
    }
  }

  int operate() override {
    reenter(this) {
      yield {
	char buf[16];
	snprintf(buf, sizeof(buf), "%d", shard_id);
        rgw_http_param_pair pairs[] = { { "type" , "data" },
	                                { "id", buf },
					{ "info" , NULL },
	                                { NULL, NULL } };

        string p = "/admin/log/";

        http_op = new RGWRESTReadResource(sc->conn, p, pairs, NULL, sync_env->http_manager);

        init_new_io(http_op);

        int ret = http_op->aio_read();
        if (ret < 0) {
          ldout(sync_env->cct, 0) << "ERROR: failed to read from " << p << dendl;
          log_error() << "failed to send http operation: " << http_op->to_str() << " ret=" << ret << std::endl;
          return set_cr_error(ret);
        }

        return io_block(0);
      }
      yield {
        int ret = http_op->wait(shard_info, null_yield);
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
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;

  RGWRESTReadResource *http_op = nullptr;

  int shard_id;
  const std::string& marker;
  string *pnext_marker;
  list<rgw_data_change_log_entry> *entries;
  bool *truncated;

  read_remote_data_log_response response;
  std::optional<TOPNSPC::common::PerfGuard> timer;

public:
  RGWReadRemoteDataLogShardCR(RGWDataSyncCtx *_sc, int _shard_id,
                              const std::string& marker, string *pnext_marker,
                              list<rgw_data_change_log_entry> *_entries,
                              bool *_truncated)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
      shard_id(_shard_id), marker(marker), pnext_marker(pnext_marker),
      entries(_entries), truncated(_truncated) {
  }
  ~RGWReadRemoteDataLogShardCR() override {
    if (http_op) {
      http_op->put();
    }
  }

  int operate() override {
    reenter(this) {
      yield {
	char buf[16];
	snprintf(buf, sizeof(buf), "%d", shard_id);
        rgw_http_param_pair pairs[] = { { "type" , "data" },
	                                { "id", buf },
	                                { "marker", marker.c_str() },
	                                { "extra-info", "true" },
	                                { NULL, NULL } };

        string p = "/admin/log/";

        http_op = new RGWRESTReadResource(sc->conn, p, pairs, NULL, sync_env->http_manager);

        init_new_io(http_op);

        if (sync_env->counters) {
          timer.emplace(sync_env->counters, sync_counters::l_poll);
        }
        int ret = http_op->aio_read();
        if (ret < 0) {
          ldout(sync_env->cct, 0) << "ERROR: failed to read from " << p << dendl;
          log_error() << "failed to send http operation: " << http_op->to_str() << " ret=" << ret << std::endl;
          if (sync_env->counters) {
            sync_env->counters->inc(sync_counters::l_poll_err);
          }
          return set_cr_error(ret);
        }

        return io_block(0);
      }
      yield {
        timer.reset();
        int ret = http_op->wait(&response, null_yield);
        if (ret < 0) {
          if (sync_env->counters && ret != -ENOENT) {
            sync_env->counters->inc(sync_counters::l_poll_err);
          }
          return set_cr_error(ret);
        }
        entries->clear();
        entries->swap(response.entries);
        *pnext_marker = response.marker;
        *truncated = response.truncated;
        return set_cr_done();
      }
    }
    return 0;
  }
};

class RGWReadRemoteDataLogInfoCR : public RGWShardCollectCR {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;

  int num_shards;
  map<int, RGWDataChangesLogInfo> *datalog_info;

  int shard_id;
#define READ_DATALOG_MAX_CONCURRENT 10

public:
  RGWReadRemoteDataLogInfoCR(RGWDataSyncCtx *_sc,
                     int _num_shards,
                     map<int, RGWDataChangesLogInfo> *_datalog_info) : RGWShardCollectCR(_sc->cct, READ_DATALOG_MAX_CONCURRENT),
                                                                 sc(_sc), sync_env(_sc->env), num_shards(_num_shards),
                                                                 datalog_info(_datalog_info), shard_id(0) {}
  bool spawn_next() override;
};

bool RGWReadRemoteDataLogInfoCR::spawn_next() {
  if (shard_id >= num_shards) {
    return false;
  }
  spawn(new RGWReadRemoteDataLogShardInfoCR(sc, shard_id, &(*datalog_info)[shard_id]), false);
  shard_id++;
  return true;
}

class RGWListRemoteDataLogShardCR : public RGWSimpleCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  RGWRESTReadResource *http_op;

  int shard_id;
  string marker;
  uint32_t max_entries;
  rgw_datalog_shard_data *result;

public:
  RGWListRemoteDataLogShardCR(RGWDataSyncCtx *sc, int _shard_id,
                              const string& _marker, uint32_t _max_entries,
                              rgw_datalog_shard_data *_result)
    : RGWSimpleCoroutine(sc->cct), sc(sc), sync_env(sc->env), http_op(NULL),
      shard_id(_shard_id), marker(_marker), max_entries(_max_entries), result(_result) {}

  int send_request() override {
    RGWRESTConn *conn = sc->conn;

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
    init_new_io(http_op);

    int ret = http_op->aio_read();
    if (ret < 0) {
      ldout(sync_env->cct, 0) << "ERROR: failed to read from " << p << dendl;
      log_error() << "failed to send http operation: " << http_op->to_str() << " ret=" << ret << std::endl;
      http_op->put();
      return ret;
    }

    return 0;
  }

  int request_complete() override {
    int ret = http_op->wait(result, null_yield);
    http_op->put();
    if (ret < 0 && ret != -ENOENT) {
      ldout(sync_env->cct, 0) << "ERROR: failed to list remote datalog shard, ret=" << ret << dendl;
      return ret;
    }
    return 0;
  }
};

class RGWListRemoteDataLogCR : public RGWShardCollectCR {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;

  map<int, string> shards;
  int max_entries_per_shard;
  map<int, rgw_datalog_shard_data> *result;

  map<int, string>::iterator iter;
#define READ_DATALOG_MAX_CONCURRENT 10

public:
  RGWListRemoteDataLogCR(RGWDataSyncCtx *_sc,
                     map<int, string>& _shards,
                     int _max_entries_per_shard,
                     map<int, rgw_datalog_shard_data> *_result) : RGWShardCollectCR(_sc->cct, READ_DATALOG_MAX_CONCURRENT),
                                                                 sc(_sc), sync_env(_sc->env), max_entries_per_shard(_max_entries_per_shard),
                                                                 result(_result) {
    shards.swap(_shards);
    iter = shards.begin();
  }
  bool spawn_next() override;
};

bool RGWListRemoteDataLogCR::spawn_next() {
  if (iter == shards.end()) {
    return false;
  }

  spawn(new RGWListRemoteDataLogShardCR(sc, iter->first, iter->second, max_entries_per_shard, &(*result)[iter->first]), false);
  ++iter;
  return true;
}

class RGWInitDataSyncStatusCoroutine : public RGWCoroutine {
  static constexpr uint32_t lock_duration = 30;
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  rgw::sal::RGWRadosStore *store;
  const rgw_pool& pool;
  const uint32_t num_shards;

  string sync_status_oid;

  string lock_name;
  string cookie;
  rgw_data_sync_status *status;
  map<int, RGWDataChangesLogInfo> shards_info;

  RGWSyncTraceNodeRef tn;
public:
  RGWInitDataSyncStatusCoroutine(RGWDataSyncCtx *_sc, uint32_t num_shards,
                                 uint64_t instance_id,
                                 RGWSyncTraceNodeRef& _tn_parent,
                                 rgw_data_sync_status *status)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env), store(sync_env->store),
      pool(sync_env->svc->zone->get_zone_params().log_pool),
      num_shards(num_shards), status(status),
      tn(sync_env->sync_tracer->add_node(_tn_parent, "init_data_sync_status")) {
    lock_name = "sync_lock";

    status->sync_info.instance_id = instance_id;

#define COOKIE_LEN 16
    char buf[COOKIE_LEN + 1];

    gen_rand_alphanumeric(cct, buf, sizeof(buf) - 1);
    cookie = buf;

    sync_status_oid = RGWDataSyncStatusManager::sync_status_oid(sc->source_zone);

  }

  int operate() override {
    int ret;
    reenter(this) {
      using LockCR = RGWSimpleRadosLockCR;
      yield call(new LockCR(sync_env->async_rados, store,
                            rgw_raw_obj{pool, sync_status_oid},
                            lock_name, cookie, lock_duration));
      if (retcode < 0) {
        tn->log(0, SSTR("ERROR: failed to take a lock on " << sync_status_oid));
        return set_cr_error(retcode);
      }
      using WriteInfoCR = RGWSimpleRadosWriteCR<rgw_data_sync_info>;
      yield call(new WriteInfoCR(sync_env->async_rados, sync_env->svc->sysobj,
                                 rgw_raw_obj{pool, sync_status_oid},
                                 status->sync_info));
      if (retcode < 0) {
        tn->log(0, SSTR("ERROR: failed to write sync status info with " << retcode));
        return set_cr_error(retcode);
      }

      /* take lock again, we just recreated the object */
      yield call(new LockCR(sync_env->async_rados, store,
                            rgw_raw_obj{pool, sync_status_oid},
                            lock_name, cookie, lock_duration));
      if (retcode < 0) {
        tn->log(0, SSTR("ERROR: failed to take a lock on " << sync_status_oid));
        return set_cr_error(retcode);
      }

      tn->log(10, "took lease");

      /* fetch current position in logs */
      yield {
        RGWRESTConn *conn = sync_env->svc->zone->get_zone_conn(sc->source_zone);
        if (!conn) {
          tn->log(0, SSTR("ERROR: connection to zone " << sc->source_zone << " does not exist!"));
          return set_cr_error(-EIO);
        }
        for (uint32_t i = 0; i < num_shards; i++) {
          spawn(new RGWReadRemoteDataLogShardInfoCR(sc, i, &shards_info[i]), true);
        }
      }
      while (collect(&ret, NULL)) {
        if (ret < 0) {
          tn->log(0, SSTR("ERROR: failed to read remote data log shards"));
          return set_state(RGWCoroutine_Error);
        }
        yield;
      }
      yield {
        for (uint32_t i = 0; i < num_shards; i++) {
          RGWDataChangesLogInfo& info = shards_info[i];
          auto& marker = status->sync_markers[i];
          marker.next_step_marker = info.marker;
          marker.timestamp = info.last_update;
          const auto& oid = RGWDataSyncStatusManager::shard_obj_name(sc->source_zone, i);
          using WriteMarkerCR = RGWSimpleRadosWriteCR<rgw_data_sync_marker>;
          spawn(new WriteMarkerCR(sync_env->async_rados, sync_env->svc->sysobj,
                                  rgw_raw_obj{pool, oid}, marker), true);
        }
      }
      while (collect(&ret, NULL)) {
        if (ret < 0) {
          tn->log(0, SSTR("ERROR: failed to write data sync status markers"));
          return set_state(RGWCoroutine_Error);
        }
        yield;
      }

      status->sync_info.state = rgw_data_sync_info::StateBuildingFullSyncMaps;
      yield call(new WriteInfoCR(sync_env->async_rados, sync_env->svc->sysobj,
                                 rgw_raw_obj{pool, sync_status_oid},
                                 status->sync_info));
      if (retcode < 0) {
        tn->log(0, SSTR("ERROR: failed to write sync status info with " << retcode));
        return set_cr_error(retcode);
      }
      yield call(new RGWSimpleRadosUnlockCR(sync_env->async_rados, store,
                                            rgw_raw_obj{pool, sync_status_oid},
                                            lock_name, cookie));
      return set_cr_done();
    }
    return 0;
  }
};

RGWRemoteDataLog::RGWRemoteDataLog(const DoutPrefixProvider *dpp,
                                   rgw::sal::RGWRadosStore *store,
                                   RGWAsyncRadosProcessor *async_rados)
  : RGWCoroutinesManager(store->ctx(), store->getRados()->get_cr_registry()),
      dpp(dpp), store(store),
      cct(store->ctx()), cr_registry(store->getRados()->get_cr_registry()),
      async_rados(async_rados),
      http_manager(store->ctx(), completion_mgr),
      data_sync_cr(NULL),
      initialized(false)
{
}

int RGWRemoteDataLog::read_log_info(rgw_datalog_info *log_info)
{
  rgw_http_param_pair pairs[] = { { "type", "data" },
                                  { NULL, NULL } };

  int ret = sc.conn->get_json_resource("/admin/log", pairs, *log_info);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to fetch datalog info" << dendl;
    return ret;
  }

  ldpp_dout(dpp, 20) << "remote datalog, num_shards=" << log_info->num_shards << dendl;

  return 0;
}

int RGWRemoteDataLog::read_source_log_shards_info(map<int, RGWDataChangesLogInfo> *shards_info)
{
  rgw_datalog_info log_info;
  int ret = read_log_info(&log_info);
  if (ret < 0) {
    return ret;
  }

  return run(new RGWReadRemoteDataLogInfoCR(&sc, log_info.num_shards, shards_info));
}

int RGWRemoteDataLog::read_source_log_shards_next(map<int, string> shard_markers, map<int, rgw_datalog_shard_data> *result)
{
  return run(new RGWListRemoteDataLogCR(&sc, shard_markers, 1, result));
}

int RGWRemoteDataLog::init(const rgw_zone_id& _source_zone, RGWRESTConn *_conn, RGWSyncErrorLogger *_error_logger,
                           RGWSyncTraceManager *_sync_tracer, RGWSyncModuleInstanceRef& _sync_module,
                           PerfCounters* counters)
{
  sync_env.init(dpp, cct, store, store->svc(), async_rados, &http_manager, _error_logger,
                _sync_tracer, _sync_module, counters);
  sc.init(&sync_env, _conn, _source_zone);

  if (initialized) {
    return 0;
  }

  int ret = http_manager.start();
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "failed in http_manager.start() ret=" << ret << dendl;
    return ret;
  }

  tn = sync_env.sync_tracer->add_node(sync_env.sync_tracer->root_node, "data");

  initialized = true;

  return 0;
}

void RGWRemoteDataLog::finish()
{
  stop();
}

int RGWRemoteDataLog::read_sync_status(rgw_data_sync_status *sync_status)
{
  // cannot run concurrently with run_sync(), so run in a separate manager
  RGWCoroutinesManager crs(cct, cr_registry);
  RGWHTTPManager http_manager(cct, crs.get_completion_mgr());
  int ret = http_manager.start();
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "failed in http_manager.start() ret=" << ret << dendl;
    return ret;
  }
  RGWDataSyncEnv sync_env_local = sync_env;
  sync_env_local.http_manager = &http_manager;

  RGWDataSyncCtx sc_local = sc;
  sc_local.env = &sync_env_local;

  ret = crs.run(new RGWReadDataSyncStatusCoroutine(&sc_local, sync_status));
  http_manager.stop();
  return ret;
}

int RGWRemoteDataLog::read_recovering_shards(const int num_shards, set<int>& recovering_shards)
{
  // cannot run concurrently with run_sync(), so run in a separate manager
  RGWCoroutinesManager crs(cct, cr_registry);
  RGWHTTPManager http_manager(cct, crs.get_completion_mgr());
  int ret = http_manager.start();
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "failed in http_manager.start() ret=" << ret << dendl;
    return ret;
  }
  RGWDataSyncEnv sync_env_local = sync_env;
  sync_env_local.http_manager = &http_manager;

  RGWDataSyncCtx sc_local = sc;
  sc_local.env = &sync_env_local;

  std::vector<RGWRadosGetOmapKeysCR::ResultPtr> omapkeys;
  omapkeys.resize(num_shards);
  uint64_t max_entries{1};

  ret = crs.run(new RGWReadDataSyncRecoveringShardsCR(&sc_local, max_entries, num_shards, omapkeys));
  http_manager.stop();

  if (ret == 0) {
    for (int i = 0; i < num_shards; i++) {
      if (omapkeys[i]->entries.size() != 0) {
        recovering_shards.insert(i);
      }
    }
  }

  return ret;
}

int RGWRemoteDataLog::init_sync_status(int num_shards)
{
  rgw_data_sync_status sync_status;
  sync_status.sync_info.num_shards = num_shards;

  RGWCoroutinesManager crs(cct, cr_registry);
  RGWHTTPManager http_manager(cct, crs.get_completion_mgr());
  int ret = http_manager.start();
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "failed in http_manager.start() ret=" << ret << dendl;
    return ret;
  }
  RGWDataSyncEnv sync_env_local = sync_env;
  sync_env_local.http_manager = &http_manager;
  auto instance_id = ceph::util::generate_random_number<uint64_t>();
  RGWDataSyncCtx sc_local = sc;
  sc_local.env = &sync_env_local;
  ret = crs.run(new RGWInitDataSyncStatusCoroutine(&sc_local, num_shards, instance_id, tn, &sync_status));
  http_manager.stop();
  return ret;
}

static string full_data_sync_index_shard_oid(const rgw_zone_id& source_zone, int shard_id)
{
  char buf[datalog_sync_full_sync_index_prefix.size() + 1 + source_zone.id.size() + 1 + 16];
  snprintf(buf, sizeof(buf), "%s.%s.%d", datalog_sync_full_sync_index_prefix.c_str(), source_zone.id.c_str(), shard_id);
  return string(buf);
}

struct read_metadata_list {
  string marker;
  bool truncated;
  list<string> keys;
  int count;

  read_metadata_list() : truncated(false), count(0) {}

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("marker", marker, obj);
    JSONDecoder::decode_json("truncated", truncated, obj);
    JSONDecoder::decode_json("keys", keys, obj);
    JSONDecoder::decode_json("count", count, obj);
  }
};

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
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;

  rgw::sal::RGWRadosStore *store;

  rgw_data_sync_status *sync_status;
  int num_shards;

  int req_ret;
  int ret;

  list<string>::iterator iter;

  RGWShardedOmapCRManager *entries_index;

  string oid_prefix;

  string path;
  bucket_instance_meta_info meta_info;
  string key;
  string s;
  int i;

  bool failed;
  bool truncated;
  read_metadata_list result;

public:
  RGWListBucketIndexesCR(RGWDataSyncCtx *_sc,
                         rgw_data_sync_status *_sync_status) : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
                                                      store(sync_env->store), sync_status(_sync_status),
						      req_ret(0), ret(0), entries_index(NULL), i(0), failed(false), truncated(false) {
    oid_prefix = datalog_sync_full_sync_index_prefix + "." + sc->source_zone.id; 
    path = "/admin/metadata/bucket.instance";
    num_shards = sync_status->sync_info.num_shards;
  }
  ~RGWListBucketIndexesCR() override {
    delete entries_index;
  }

  int operate() override {
    reenter(this) {
      entries_index = new RGWShardedOmapCRManager(sync_env->async_rados, store, this, num_shards,
						  sync_env->svc->zone->get_zone_params().log_pool,
                                                  oid_prefix);
      yield; // yield so OmapAppendCRs can start

      do {
        yield {
          string entrypoint = string("/admin/metadata/bucket.instance");

          rgw_http_param_pair pairs[] = {{"max-entries", "1000"},
                                         {"marker", result.marker.c_str()},
                                         {NULL, NULL}};

          call(new RGWReadRESTResourceCR<read_metadata_list>(sync_env->cct, sc->conn, sync_env->http_manager,
                                                             entrypoint, pairs, &result));
        }
        if (retcode < 0) {
          ldout(sync_env->cct, 0) << "ERROR: failed to fetch metadata for section bucket.instance" << dendl;
          return set_cr_error(retcode);
        }

        for (iter = result.keys.begin(); iter != result.keys.end(); ++iter) {
          ldout(sync_env->cct, 20) << "list metadata: section=bucket.instance key=" << *iter << dendl;
          key = *iter;

          yield {
            rgw_http_param_pair pairs[] = {{"key", key.c_str()},
                                           {NULL, NULL}};

            call(new RGWReadRESTResourceCR<bucket_instance_meta_info>(sync_env->cct, sc->conn, sync_env->http_manager, path, pairs, &meta_info));
          }

          num_shards = meta_info.data.get_bucket_info().layout.current_index.layout.normal.num_shards;
          if (num_shards > 0) {
            for (i = 0; i < num_shards; i++) {
              char buf[16];
              snprintf(buf, sizeof(buf), ":%d", i);
              s = key + buf;
              yield entries_index->append(s, sync_env->svc->datalog_rados->get_log_shard_id(meta_info.data.get_bucket_info().bucket, i));
            }
          } else {
            yield entries_index->append(key, sync_env->svc->datalog_rados->get_log_shard_id(meta_info.data.get_bucket_info().bucket, -1));
          }
        }
        truncated = result.truncated;
      } while (truncated);

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
          spawn(new RGWSimpleRadosWriteCR<rgw_data_sync_marker>(sync_env->async_rados, sync_env->svc->sysobj,
                                                                rgw_raw_obj(sync_env->svc->zone->get_zone_params().log_pool, RGWDataSyncStatusManager::shard_obj_name(sc->source_zone, shard_id)),
                                                                marker),
                true);
        }
      } else {
        yield call(sync_env->error_logger->log_error_cr(sc->conn->get_remote_id(), "data.init", "",
                                                        EIO, string("failed to build bucket instances map")));
      }
      while (collect(&ret, NULL)) {
        if (ret < 0) {
          yield call(sync_env->error_logger->log_error_cr(sc->conn->get_remote_id(), "data.init", "",
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
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  string marker_oid;
  rgw_data_sync_marker sync_marker;
  RGWSyncTraceNodeRef tn;

public:
  RGWDataSyncShardMarkerTrack(RGWDataSyncCtx *_sc,
                         const string& _marker_oid,
                         const rgw_data_sync_marker& _marker,
                         RGWSyncTraceNodeRef& _tn) : RGWSyncShardMarkerTrack(DATA_SYNC_UPDATE_MARKER_WINDOW),
                                                                sc(_sc), sync_env(_sc->env),
                                                                marker_oid(_marker_oid),
                                                                sync_marker(_marker),
                                                                tn(_tn) {}

  RGWCoroutine *store_marker(const string& new_marker, uint64_t index_pos, const real_time& timestamp) override {
    sync_marker.marker = new_marker;
    sync_marker.pos = index_pos;
    sync_marker.timestamp = timestamp;

    tn->log(20, SSTR("updating marker marker_oid=" << marker_oid << " marker=" << new_marker));

    return new RGWSimpleRadosWriteCR<rgw_data_sync_marker>(sync_env->async_rados, sync_env->svc->sysobj,
                                                           rgw_raw_obj(sync_env->svc->zone->get_zone_params().log_pool, marker_oid),
                                                           sync_marker);
  }

  RGWOrderCallCR *allocate_order_control_cr() override {
    return new RGWLastCallerWinsCR(sync_env->cct);
  }
};

// ostream wrappers to print buckets without copying strings
struct bucket_str {
  const rgw_bucket& b;
  explicit bucket_str(const rgw_bucket& b) : b(b) {}
};
std::ostream& operator<<(std::ostream& out, const bucket_str& rhs) {
  auto& b = rhs.b;
  if (!b.tenant.empty()) {
    out << b.tenant << '/';
  }
  out << b.name;
  if (!b.bucket_id.empty()) {
    out << ':' << b.bucket_id;
  }
  return out;
}

struct bucket_str_noinstance {
  const rgw_bucket& b;
  explicit bucket_str_noinstance(const rgw_bucket& b) : b(b) {}
};
std::ostream& operator<<(std::ostream& out, const bucket_str_noinstance& rhs) {
  auto& b = rhs.b;
  if (!b.tenant.empty()) {
    out << b.tenant << '/';
  }
  out << b.name;
  return out;
}

struct bucket_shard_str {
  const rgw_bucket_shard& bs;
  explicit bucket_shard_str(const rgw_bucket_shard& bs) : bs(bs) {}
};
std::ostream& operator<<(std::ostream& out, const bucket_shard_str& rhs) {
  auto& bs = rhs.bs;
  out << bucket_str{bs.bucket};
  if (bs.shard_id >= 0) {
    out << ':' << bs.shard_id;
  }
  return out;
}

class RGWRunBucketSyncCoroutine : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr;
  rgw_bucket_sync_pair_info sync_pair;
  rgw_bucket_sync_pipe sync_pipe;
  rgw_bucket_shard_sync_info sync_status;
  RGWMetaSyncEnv meta_sync_env;
  RGWObjVersionTracker objv_tracker;
  ceph::real_time* progress;

  const std::string status_oid;

  RGWSyncTraceNodeRef tn;

public:
  RGWRunBucketSyncCoroutine(RGWDataSyncCtx *_sc,
                            boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr,
                            const rgw_bucket_sync_pair_info& _sync_pair,
                            const RGWSyncTraceNodeRef& _tn_parent,
                            ceph::real_time* progress)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
      lease_cr(std::move(lease_cr)), sync_pair(_sync_pair), progress(progress),
      status_oid(RGWBucketPipeSyncStatusManager::status_oid(sc->source_zone, sync_pair)),
      tn(sync_env->sync_tracer->add_node(_tn_parent, "bucket",
                                         SSTR(bucket_shard_str{_sync_pair.dest_bs} << "<-" << bucket_shard_str{_sync_pair.source_bs} ))) {
  }

  int operate() override;
};

struct all_bucket_info {
  RGWBucketInfo bucket_info;
  map<string, bufferlist> attrs;
};

struct rgw_sync_pipe_info_entity
{
private:
  RGWBucketInfo bucket_info;
  map<string, bufferlist> bucket_attrs;
  bool _has_bucket_info{false};

public:
  rgw_zone_id zone;

  rgw_sync_pipe_info_entity() {}
  rgw_sync_pipe_info_entity(const rgw_sync_bucket_entity& e,
                            std::optional<all_bucket_info>& binfo) {
    if (e.zone) {
      zone = *e.zone;
    }
    if (!e.bucket) {
      return;
    }
    if (!binfo ||
        binfo->bucket_info.bucket != *e.bucket) {
      bucket_info.bucket = *e.bucket;
    } else {
      set_bucket_info(*binfo);
    }
  }

  void update_empty_bucket_info(const std::map<rgw_bucket, all_bucket_info>& buckets_info) {
    if (_has_bucket_info) {
      return;
    }
    if (bucket_info.bucket.name.empty()) {
      return;
    }

    auto iter = buckets_info.find(bucket_info.bucket);
    if (iter == buckets_info.end()) {
      return;
    }

    set_bucket_info(iter->second);
  }

  bool has_bucket_info() const {
    return _has_bucket_info;
  }

  void set_bucket_info(const all_bucket_info& all_info) {
    bucket_info = all_info.bucket_info;
    bucket_attrs = all_info.attrs;
    _has_bucket_info = true;
  }

  const RGWBucketInfo& get_bucket_info() const {
    return bucket_info;
  }

  const rgw_bucket& get_bucket() const {
    return bucket_info.bucket;
  }

  bool operator<(const rgw_sync_pipe_info_entity& e) const {
    if (zone < e.zone) {
      return false;
    }
    if (zone > e.zone) {
      return true;
    }
    return (bucket_info.bucket < e.bucket_info.bucket);
  }
};

std::ostream& operator<<(std::ostream& out, const rgw_sync_pipe_info_entity& e) {
  auto& bucket = e.get_bucket_info().bucket;

  out << e.zone << ":" << bucket.get_key();
  return out;
}

struct rgw_sync_pipe_handler_info {
  RGWBucketSyncFlowManager::pipe_handler handler;
  rgw_sync_pipe_info_entity source;
  rgw_sync_pipe_info_entity target;

  rgw_sync_pipe_handler_info() {}
  rgw_sync_pipe_handler_info(const RGWBucketSyncFlowManager::pipe_handler& _handler,
                     std::optional<all_bucket_info> source_bucket_info,
                     std::optional<all_bucket_info> target_bucket_info) : handler(_handler),
                                                                          source(handler.source, source_bucket_info),
                                                                          target(handler.dest, target_bucket_info) {
  }

  bool operator<(const rgw_sync_pipe_handler_info& p) const {
    if (source < p.source) {
      return true;
    }
    if (p.source < source) {
      return false;
    }
    return (target < p.target);
  }

  void update_empty_bucket_info(const std::map<rgw_bucket, all_bucket_info>& buckets_info) {
    source.update_empty_bucket_info(buckets_info);
    target.update_empty_bucket_info(buckets_info);
  }
};

std::ostream& operator<<(std::ostream& out, const rgw_sync_pipe_handler_info& p) {
  out << p.source << ">" << p.target;
  return out;
}

struct rgw_sync_pipe_info_set {
  std::set<rgw_sync_pipe_handler_info> handlers;

  using iterator = std::set<rgw_sync_pipe_handler_info>::iterator;

  void clear() {
    handlers.clear();
  }

  void insert(const RGWBucketSyncFlowManager::pipe_handler& handler,
              std::optional<all_bucket_info>& source_bucket_info,
              std::optional<all_bucket_info>& target_bucket_info) {
    rgw_sync_pipe_handler_info p(handler, source_bucket_info, target_bucket_info);
    handlers.insert(p);
  }

  iterator begin() {
    return handlers.begin();
  }

  iterator end() {
    return handlers.end();
  }

  bool empty() const {
    return handlers.empty();
  }

  void update_empty_bucket_info(const std::map<rgw_bucket, all_bucket_info>& buckets_info) {
    if (buckets_info.empty()) {
      return;
    }

    std::set<rgw_sync_pipe_handler_info> p;

    for (auto pipe : handlers) {
      pipe.update_empty_bucket_info(buckets_info);
      p.insert(pipe);
    }

    handlers = std::move(p);
  }
};

class RGWRunBucketsSyncBySourceCR : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;

  rgw_bucket_shard source;

  RGWSyncTraceNodeRef tn;

public:
  RGWRunBucketsSyncBySourceCR(RGWDataSyncCtx *_sc, const rgw_bucket_shard& _source, const RGWSyncTraceNodeRef& _tn_parent)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env), source(_source),
      tn(sync_env->sync_tracer->add_node(_tn_parent, "source",
                                         SSTR(bucket_shard_str{_source} ))) {
  }
  ~RGWRunBucketsSyncBySourceCR() override {
  }

  int operate() override;
};

class RGWRunBucketSourcesSyncCR : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr;

  std::optional<rgw_bucket_shard> target_bs;
  std::optional<rgw_bucket_shard> source_bs;

  std::optional<rgw_bucket> target_bucket;
  std::optional<rgw_bucket> source_bucket;

  rgw_sync_pipe_info_set pipes;
  rgw_sync_pipe_info_set::iterator siter;

  rgw_bucket_sync_pair_info sync_pair;

  RGWSyncTraceNodeRef tn;
  ceph::real_time* progress;
  std::vector<ceph::real_time> shard_progress;
  std::vector<ceph::real_time>::iterator cur_shard_progress;

  RGWRESTConn *conn{nullptr};
  rgw_zone_id last_zone;

  int ret{0};

  int source_num_shards{0};
  int target_num_shards{0};

  int num_shards{0};
  int cur_shard{0};
  bool again = false;

public:
  RGWRunBucketSourcesSyncCR(RGWDataSyncCtx *_sc,
                            boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr,
                            std::optional<rgw_bucket_shard> _target_bs,
                            std::optional<rgw_bucket_shard> _source_bs,
                            const RGWSyncTraceNodeRef& _tn_parent,
                            ceph::real_time* progress);

  int operate() override;
};

class RGWDataSyncSingleEntryCR : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  rgw::bucket_sync::Handle state; // cached bucket-shard state
  rgw_data_sync_obligation obligation; // input obligation
  std::optional<rgw_data_sync_obligation> complete; // obligation to complete
  uint32_t obligation_counter = 0;
  RGWDataSyncShardMarkerTrack *marker_tracker;
  const rgw_raw_obj& error_repo;
  boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr;
  RGWSyncTraceNodeRef tn;

  ceph::real_time progress;
  int sync_status = 0;
public:
  RGWDataSyncSingleEntryCR(RGWDataSyncCtx *_sc, rgw::bucket_sync::Handle state,
                           rgw_data_sync_obligation obligation,
                           RGWDataSyncShardMarkerTrack *_marker_tracker,
                           const rgw_raw_obj& error_repo,
                           boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr,
                           const RGWSyncTraceNodeRef& _tn_parent)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
      state(std::move(state)), obligation(std::move(obligation)),
      marker_tracker(_marker_tracker), error_repo(error_repo),
      lease_cr(std::move(lease_cr)) {
    set_description() << "data sync single entry (source_zone=" << sc->source_zone << ") " << obligation;
    tn = sync_env->sync_tracer->add_node(_tn_parent, "entry", obligation.key);
  }

  int operate() override {
    reenter(this) {
      if (state->obligation) {
        // this is already syncing in another DataSyncSingleEntryCR
        if (state->obligation->timestamp < obligation.timestamp) {
          // cancel existing obligation and overwrite it
          tn->log(10, SSTR("canceling existing obligation " << *state->obligation));
          complete = std::move(*state->obligation);
          *state->obligation = std::move(obligation);
          state->counter++;
        } else {
          // cancel new obligation
          tn->log(10, SSTR("canceling new obligation " << obligation));
          complete = std::move(obligation);
        }
      } else {
        // start syncing a new obligation
        state->obligation = obligation;
        obligation_counter = state->counter;
        state->counter++;

        // loop until the latest obligation is satisfied, because other callers
        // may update the obligation while we're syncing
        while (state->progress_timestamp < state->obligation->timestamp &&
               obligation_counter != state->counter) {
          obligation_counter = state->counter;
          progress = ceph::real_time{};

          ldout(cct, 4) << "starting sync on " << bucket_shard_str{state->key}
              << ' ' << *state->obligation << dendl;
          yield call(new RGWRunBucketSourcesSyncCR(sc, lease_cr,
                                                   std::nullopt, /* target_bs */
                                                   state->key, tn, &progress));
          if (retcode < 0) {
            break;
          }
          state->progress_timestamp = std::max(progress, state->progress_timestamp);
        }
        // any new obligations will process themselves
        complete = std::move(*state->obligation);
        state->obligation.reset();

        tn->log(10, SSTR("sync finished on " << bucket_shard_str{state->key}
                         << " progress=" << progress << ' ' << complete << " r=" << retcode));
      }
      sync_status = retcode;

      if (sync_status == -ENOENT) {
        // this was added when 'tenant/' was added to datalog entries, because
        // preexisting tenant buckets could never sync and would stay in the
        // error_repo forever
        tn->log(0, SSTR("WARNING: skipping data log entry for missing bucket " << complete->key));
        sync_status = 0;
      }

      if (sync_status < 0) {
        // write actual sync failures for 'radosgw-admin sync error list'
        if (sync_status != -EBUSY && sync_status != -EAGAIN) {
          yield call(sync_env->error_logger->log_error_cr(sc->conn->get_remote_id(), "data", complete->key,
                                                          -sync_status, string("failed to sync bucket instance: ") + cpp_strerror(-sync_status)));
          if (retcode < 0) {
            tn->log(0, SSTR("ERROR: failed to log sync failure: retcode=" << retcode));
          }
        }
        if (complete->timestamp != ceph::real_time{}) {
          tn->log(10, SSTR("writing " << *complete << " to error repo for retry"));
          yield call(rgw_error_repo_write_cr(sync_env->store->svc()->rados, error_repo,
                                            complete->key, complete->timestamp));
          if (retcode < 0) {
            tn->log(0, SSTR("ERROR: failed to log sync failure in error repo: retcode=" << retcode));
          }
        }
      } else if (complete->retry) {
        yield call(rgw_error_repo_remove_cr(sync_env->store->svc()->rados, error_repo,
                                            complete->key, complete->timestamp));
        if (retcode < 0) {
          tn->log(0, SSTR("ERROR: failed to remove omap key from error repo ("
             << error_repo << " retcode=" << retcode));
        }
      }
      /* FIXME: what do do in case of error */
      if (marker_tracker && !complete->marker.empty()) {
        /* update marker */
        yield call(marker_tracker->finish(complete->marker));
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
#define DATA_SYNC_MAX_ERR_ENTRIES 10

class RGWDataSyncShardCR : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;

  rgw_pool pool;

  uint32_t shard_id;
  rgw_data_sync_marker& sync_marker;

  RGWRadosGetOmapValsCR::ResultPtr omapvals;
  std::map<std::string, bufferlist> entries;
  std::map<std::string, bufferlist>::iterator iter;

  string oid;

  std::optional<RGWDataSyncShardMarkerTrack> marker_tracker;

  std::string next_marker;
  list<rgw_data_change_log_entry> log_entries;
  list<rgw_data_change_log_entry>::iterator log_iter;
  bool truncated = false;

  ceph::mutex inc_lock = ceph::make_mutex("RGWDataSyncShardCR::inc_lock");
  ceph::condition_variable inc_cond;

  boost::asio::coroutine incremental_cr;
  boost::asio::coroutine full_cr;


  set<string> modified_shards;
  set<string> current_modified;

  set<string>::iterator modified_iter;

  uint64_t total_entries = 0;
  static constexpr int spawn_window = BUCKET_SHARD_SYNC_SPAWN_WINDOW;
  bool *reset_backoff = nullptr;

  boost::intrusive_ptr<RGWContinuousLeaseCR> lease_cr;
  boost::intrusive_ptr<RGWCoroutinesStack> lease_stack;
  string status_oid;

  rgw_raw_obj error_repo;
  std::map<std::string, bufferlist> error_entries;
  string error_marker;
  ceph::real_time entry_timestamp;
  static constexpr int max_error_entries = DATA_SYNC_MAX_ERR_ENTRIES;

  ceph::coarse_real_time error_retry_time;

#define RETRY_BACKOFF_SECS_MIN 60
#define RETRY_BACKOFF_SECS_DEFAULT 60
#define RETRY_BACKOFF_SECS_MAX 600
  uint32_t retry_backoff_secs = RETRY_BACKOFF_SECS_DEFAULT;

  RGWSyncTraceNodeRef tn;

  rgw_bucket_shard source_bs;

  // target number of entries to cache before recycling idle ones
  static constexpr size_t target_cache_size = 256;
  boost::intrusive_ptr<rgw::bucket_sync::Cache> bucket_shard_cache;

  int parse_bucket_key(const std::string& key, rgw_bucket_shard& bs) const {
    return rgw_bucket_parse_bucket_key(sync_env->cct, key,
                                       &bs.bucket, &bs.shard_id);
  }
  RGWCoroutine* sync_single_entry(const rgw_bucket_shard& src,
                                  const std::string& key,
                                  const std::string& marker,
                                  ceph::real_time timestamp, bool retry) {
    auto state = bucket_shard_cache->get(src);
    auto obligation = rgw_data_sync_obligation{key, marker, timestamp, retry};
    return new RGWDataSyncSingleEntryCR(sc, std::move(state), std::move(obligation),
                                        &*marker_tracker, error_repo,
                                        lease_cr.get(), tn);
  }
public:
  RGWDataSyncShardCR(RGWDataSyncCtx *_sc, rgw_pool& _pool,
                     uint32_t _shard_id, rgw_data_sync_marker& _marker,
                     RGWSyncTraceNodeRef& _tn, bool *_reset_backoff)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
      pool(_pool), shard_id(_shard_id), sync_marker(_marker),
      status_oid(RGWDataSyncStatusManager::shard_obj_name(sc->source_zone, shard_id)),
      error_repo(pool, status_oid + ".retry"), tn(_tn),
      bucket_shard_cache(rgw::bucket_sync::Cache::create(target_cache_size))
  {
    set_description() << "data sync shard source_zone=" << sc->source_zone << " shard_id=" << shard_id;
  }

  ~RGWDataSyncShardCR() override {
    if (lease_cr) {
      lease_cr->abort();
    }
  }

  void append_modified_shards(set<string>& keys) {
    std::lock_guard l{inc_lock};
    modified_shards.insert(keys.begin(), keys.end());
  }

  int operate() override {
    int r;
    while (true) {
      switch (sync_marker.state) {
      case rgw_data_sync_marker::FullSync:
        r = full_sync();
        if (r < 0) {
          if (r != -EBUSY) {
            tn->log(10, SSTR("full sync failed (r=" << r << ")"));
          }
          return set_cr_error(r);
        }
        return 0;
      case rgw_data_sync_marker::IncrementalSync:
        r  = incremental_sync();
        if (r < 0) {
          if (r != -EBUSY) {
            tn->log(10, SSTR("incremental sync failed (r=" << r << ")"));
          }
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
    }
    auto store = sync_env->store;
    lease_cr.reset(new RGWContinuousLeaseCR(sync_env->async_rados, store,
                                            rgw_raw_obj(pool, status_oid),
                                            lock_name, lock_duration, this));
    lease_stack.reset(spawn(lease_cr.get(), false));
  }

  int full_sync() {
#define OMAP_GET_MAX_ENTRIES 100
    int max_entries = OMAP_GET_MAX_ENTRIES;
    reenter(&full_cr) {
      tn->log(10, "start full sync");
      yield init_lease_cr();
      while (!lease_cr->is_locked()) {
        if (lease_cr->is_done()) {
          tn->log(5, "failed to take lease");
          set_status("lease lock failed, early abort");
          drain_all();
          return set_cr_error(lease_cr->get_ret_status());
        }
        set_sleeping(true);
        yield;
      }
      tn->log(10, "took lease");
      oid = full_data_sync_index_shard_oid(sc->source_zone, shard_id);
      marker_tracker.emplace(sc, status_oid, sync_marker, tn);
      total_entries = sync_marker.pos;
      entry_timestamp = sync_marker.timestamp; // time when full sync started
      do {
        if (!lease_cr->is_locked()) {
          lease_cr->go_down();
          drain_all();
          return set_cr_error(-ECANCELED);
        }
        omapvals = std::make_shared<RGWRadosGetOmapValsCR::Result>();
        yield call(new RGWRadosGetOmapValsCR(sync_env->store, rgw_raw_obj(pool, oid),
                                             sync_marker.marker, max_entries, omapvals));
        if (retcode < 0) {
          lease_cr->go_down();
          drain_all();
          return set_cr_error(retcode);
        }
        entries = std::move(omapvals->entries);
        if (entries.size() > 0) {
          tn->set_flag(RGW_SNS_FLAG_ACTIVE); /* actually have entries to sync */
        }
        tn->log(20, SSTR("retrieved " << entries.size() << " entries to sync"));
        iter = entries.begin();
        for (; iter != entries.end(); ++iter) {
          retcode = parse_bucket_key(iter->first, source_bs);
          if (retcode < 0) {
            tn->log(1, SSTR("failed to parse bucket shard: " << iter->first));
            marker_tracker->try_update_high_marker(iter->first, 0, entry_timestamp);
            continue;
          }
          tn->log(20, SSTR("full sync: " << iter->first));
          total_entries++;
          if (!marker_tracker->start(iter->first, total_entries, entry_timestamp)) {
            tn->log(0, SSTR("ERROR: cannot start syncing " << iter->first << ". Duplicate entry?"));
          } else {
            // fetch remote and write locally
            spawn(sync_single_entry(source_bs, iter->first, iter->first,
                                    entry_timestamp, false), false);
          }
          sync_marker.marker = iter->first;

          while ((int)num_spawned() > spawn_window) {
            set_status() << "num_spawned() > spawn_window";
            yield wait_for_child();
            int ret;
            while (collect(&ret, lease_stack.get())) {
              if (ret < 0) {
                tn->log(10, "a sync operation returned error");
              }
            }
          }
        }
      } while (omapvals->more);
      omapvals.reset();

      drain_all_but_stack(lease_stack.get());

      tn->unset_flag(RGW_SNS_FLAG_ACTIVE);

      yield {
        /* update marker to reflect we're done with full sync */
        sync_marker.state = rgw_data_sync_marker::IncrementalSync;
        sync_marker.marker = sync_marker.next_step_marker;
        sync_marker.next_step_marker.clear();
        call(new RGWSimpleRadosWriteCR<rgw_data_sync_marker>(sync_env->async_rados, sync_env->svc->sysobj,
                                                             rgw_raw_obj(pool, status_oid),
                                                             sync_marker));
      }
      if (retcode < 0) {
        tn->log(0, SSTR("ERROR: failed to set sync marker: retcode=" << retcode));
        lease_cr->go_down();
        drain_all();
        return set_cr_error(retcode);
      }
      // keep lease and transition to incremental_sync()
    }
    return 0;
  }

  int incremental_sync() {
    reenter(&incremental_cr) {
      tn->log(10, "start incremental sync");
      if (lease_cr) {
        tn->log(10, "lease already held from full sync");
      } else {
        yield init_lease_cr();
        while (!lease_cr->is_locked()) {
          if (lease_cr->is_done()) {
            tn->log(5, "failed to take lease");
            set_status("lease lock failed, early abort");
            drain_all();
            return set_cr_error(lease_cr->get_ret_status());
          }
          set_sleeping(true);
          yield;
        }
        set_status("lease acquired");
        tn->log(10, "took lease");
      }
      marker_tracker.emplace(sc, status_oid, sync_marker, tn);
      do {
        if (!lease_cr->is_locked()) {
          lease_cr->go_down();
          drain_all();
          return set_cr_error(-ECANCELED);
        }
        current_modified.clear();
        inc_lock.lock();
        current_modified.swap(modified_shards);
        inc_lock.unlock();

        if (current_modified.size() > 0) {
          tn->set_flag(RGW_SNS_FLAG_ACTIVE); /* actually have entries to sync */
        }
        /* process out of band updates */
        for (modified_iter = current_modified.begin(); modified_iter != current_modified.end(); ++modified_iter) {
          retcode = parse_bucket_key(*modified_iter, source_bs);
          if (retcode < 0) {
            tn->log(1, SSTR("failed to parse bucket shard: " << *modified_iter));
            continue;
          }
          tn->log(20, SSTR("received async update notification: " << *modified_iter));
          spawn(sync_single_entry(source_bs, *modified_iter, string(),
                                  ceph::real_time{}, false), false);
        }

        if (error_retry_time <= ceph::coarse_real_clock::now()) {
          /* process bucket shards that previously failed */
          omapvals = std::make_shared<RGWRadosGetOmapValsCR::Result>();
          yield call(new RGWRadosGetOmapValsCR(sync_env->store, error_repo,
                                               error_marker, max_error_entries, omapvals));
          error_entries = std::move(omapvals->entries);
          tn->log(20, SSTR("read error repo, got " << error_entries.size() << " entries"));
          iter = error_entries.begin();
          for (; iter != error_entries.end(); ++iter) {
            error_marker = iter->first;
            entry_timestamp = rgw_error_repo_decode_value(iter->second);
            retcode = parse_bucket_key(error_marker, source_bs);
            if (retcode < 0) {
              tn->log(1, SSTR("failed to parse bucket shard: " << error_marker));
              spawn(rgw_error_repo_remove_cr(sync_env->store->svc()->rados, error_repo,
                                             error_marker, entry_timestamp), false);
              continue;
            }
            tn->log(20, SSTR("handle error entry key=" << error_marker << " timestamp=" << entry_timestamp));
            spawn(sync_single_entry(source_bs, error_marker, "",
                                    entry_timestamp, true), false);
          }
          if (!omapvals->more) {
            if (error_marker.empty() && error_entries.empty()) {
              /* the retry repo is empty, we back off a bit before calling it again */
              retry_backoff_secs *= 2;
              if (retry_backoff_secs > RETRY_BACKOFF_SECS_MAX) {
                retry_backoff_secs = RETRY_BACKOFF_SECS_MAX;
              }
            } else {
              retry_backoff_secs = RETRY_BACKOFF_SECS_DEFAULT;
            }
            error_retry_time = ceph::coarse_real_clock::now() + make_timespan(retry_backoff_secs);
            error_marker.clear();
          }
        }
        omapvals.reset();

        tn->log(20, SSTR("shard_id=" << shard_id << " sync_marker=" << sync_marker.marker));
        yield call(new RGWReadRemoteDataLogShardCR(sc, shard_id, sync_marker.marker,
                                                   &next_marker, &log_entries, &truncated));
        if (retcode < 0 && retcode != -ENOENT) {
          tn->log(0, SSTR("ERROR: failed to read remote data log info: ret=" << retcode));
          lease_cr->go_down();
          drain_all();
          return set_cr_error(retcode);
        }

        if (log_entries.size() > 0) {
          tn->set_flag(RGW_SNS_FLAG_ACTIVE); /* actually have entries to sync */
        }

        for (log_iter = log_entries.begin(); log_iter != log_entries.end(); ++log_iter) {
          tn->log(20, SSTR("shard_id=" << shard_id << " log_entry: " << log_iter->log_id << ":" << log_iter->log_timestamp << ":" << log_iter->entry.key));
          retcode = parse_bucket_key(log_iter->entry.key, source_bs);
          if (retcode < 0) {
            tn->log(1, SSTR("failed to parse bucket shard: " << log_iter->entry.key));
            marker_tracker->try_update_high_marker(log_iter->log_id, 0, log_iter->log_timestamp);
            continue;
          }
          if (!marker_tracker->start(log_iter->log_id, 0, log_iter->log_timestamp)) {
            tn->log(0, SSTR("ERROR: cannot start syncing " << log_iter->log_id << ". Duplicate entry?"));
          } else {
            spawn(sync_single_entry(source_bs, log_iter->entry.key, log_iter->log_id,
                                    log_iter->log_timestamp, false), false);
          }
          while ((int)num_spawned() > spawn_window) {
            set_status() << "num_spawned() > spawn_window";
            yield wait_for_child();
            int ret;
            while (collect(&ret, lease_stack.get())) {
              if (ret < 0) {
                tn->log(10, "a sync operation returned error");
                /* we have reported this error */
              }
              /* not waiting for child here */
            }
          }
        }

        tn->log(20, SSTR("shard_id=" << shard_id << " sync_marker=" << sync_marker.marker
                         << " next_marker=" << next_marker << " truncated=" << truncated));
        if (!next_marker.empty()) {
          sync_marker.marker = next_marker;
        } else if (!log_entries.empty()) {
          sync_marker.marker = log_entries.back().log_id;
        }
        if (!truncated) {
          // we reached the end, wait a while before checking for more
          tn->unset_flag(RGW_SNS_FLAG_ACTIVE);
	  yield wait(get_idle_interval());
	}
      } while (true);
    }
    return 0;
  }

  utime_t get_idle_interval() const {
#define INCREMENTAL_INTERVAL 20
    ceph::timespan interval = std::chrono::seconds(INCREMENTAL_INTERVAL);
    if (!ceph::coarse_real_clock::is_zero(error_retry_time)) {
      auto now = ceph::coarse_real_clock::now();
      if (error_retry_time > now) {
        auto d = error_retry_time - now;
        if (interval > d) {
          interval = d;
        }
      }
    }
    // convert timespan -> time_point -> utime_t
    return utime_t(ceph::coarse_real_clock::zero() + interval);
  }
};

class RGWDataSyncShardControlCR : public RGWBackoffControlCR {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;

  rgw_pool pool;

  uint32_t shard_id;
  rgw_data_sync_marker sync_marker;

  RGWSyncTraceNodeRef tn;
public:
  RGWDataSyncShardControlCR(RGWDataSyncCtx *_sc, const rgw_pool& _pool,
		     uint32_t _shard_id, rgw_data_sync_marker& _marker,
                     RGWSyncTraceNodeRef& _tn_parent) : RGWBackoffControlCR(_sc->cct, false),
                                                      sc(_sc), sync_env(_sc->env),
						      pool(_pool),
						      shard_id(_shard_id),
						      sync_marker(_marker) {
    tn = sync_env->sync_tracer->add_node(_tn_parent, "shard", std::to_string(shard_id));
  }

  RGWCoroutine *alloc_cr() override {
    return new RGWDataSyncShardCR(sc, pool, shard_id, sync_marker, tn, backoff_ptr());
  }

  RGWCoroutine *alloc_finisher_cr() override {
    return new RGWSimpleRadosReadCR<rgw_data_sync_marker>(sync_env->async_rados, sync_env->svc->sysobj,
                                                          rgw_raw_obj(sync_env->svc->zone->get_zone_params().log_pool, RGWDataSyncStatusManager::shard_obj_name(sc->source_zone, shard_id)),
                                                          &sync_marker);
  }

  void append_modified_shards(set<string>& keys) {
    std::lock_guard l{cr_lock()};

    RGWDataSyncShardCR *cr = static_cast<RGWDataSyncShardCR *>(get_cr());
    if (!cr) {
      return;
    }

    cr->append_modified_shards(keys);
  }
};

class RGWDataSyncCR : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  uint32_t num_shards;

  rgw_data_sync_status sync_status;

  ceph::mutex shard_crs_lock =
    ceph::make_mutex("RGWDataSyncCR::shard_crs_lock");
  map<int, RGWDataSyncShardControlCR *> shard_crs;

  bool *reset_backoff;

  RGWSyncTraceNodeRef tn;

  RGWDataSyncModule *data_sync_module{nullptr};
public:
  RGWDataSyncCR(RGWDataSyncCtx *_sc, uint32_t _num_shards, RGWSyncTraceNodeRef& _tn, bool *_reset_backoff) : RGWCoroutine(_sc->cct),
                                                      sc(_sc), sync_env(_sc->env),
                                                      num_shards(_num_shards),
                                                      reset_backoff(_reset_backoff), tn(_tn) {

  }

  ~RGWDataSyncCR() override {
    for (auto iter : shard_crs) {
      iter.second->put();
    }
  }

  int operate() override {
    reenter(this) {

      /* read sync status */
      yield call(new RGWReadDataSyncStatusCoroutine(sc, &sync_status));

      data_sync_module = sync_env->sync_module->get_data_handler();

      if (retcode < 0 && retcode != -ENOENT) {
        tn->log(0, SSTR("ERROR: failed to fetch sync status, retcode=" << retcode));
        return set_cr_error(retcode);
      }

      /* state: init status */
      if ((rgw_data_sync_info::SyncState)sync_status.sync_info.state == rgw_data_sync_info::StateInit) {
        tn->log(20, SSTR("init"));
        sync_status.sync_info.num_shards = num_shards;
        uint64_t instance_id;
        instance_id = ceph::util::generate_random_number<uint64_t>();
        yield call(new RGWInitDataSyncStatusCoroutine(sc, num_shards, instance_id, tn, &sync_status));
        if (retcode < 0) {
          tn->log(0, SSTR("ERROR: failed to init sync, retcode=" << retcode));
          return set_cr_error(retcode);
        }
        // sets state = StateBuildingFullSyncMaps

        *reset_backoff = true;
      }

      data_sync_module->init(sc, sync_status.sync_info.instance_id);

      if  ((rgw_data_sync_info::SyncState)sync_status.sync_info.state == rgw_data_sync_info::StateBuildingFullSyncMaps) {
        tn->log(10, SSTR("building full sync maps"));
        /* call sync module init here */
        sync_status.sync_info.num_shards = num_shards;
        yield call(data_sync_module->init_sync(sc));
        if (retcode < 0) {
          tn->log(0, SSTR("ERROR: sync module init_sync() failed, retcode=" << retcode));
          return set_cr_error(retcode);
        }
        /* state: building full sync maps */
        yield call(new RGWListBucketIndexesCR(sc, &sync_status));
        if (retcode < 0) {
          tn->log(0, SSTR("ERROR: failed to build full sync maps, retcode=" << retcode));
          return set_cr_error(retcode);
        }
        sync_status.sync_info.state = rgw_data_sync_info::StateSync;

        /* update new state */
        yield call(set_sync_info_cr());
        if (retcode < 0) {
          tn->log(0, SSTR("ERROR: failed to write sync status, retcode=" << retcode));
          return set_cr_error(retcode);
        }

        *reset_backoff = true;
      }

      yield call(data_sync_module->start_sync(sc));
      if (retcode < 0) {
        tn->log(0, SSTR("ERROR: failed to start sync, retcode=" << retcode));
        return set_cr_error(retcode);
      }
      
      yield {
        if  ((rgw_data_sync_info::SyncState)sync_status.sync_info.state == rgw_data_sync_info::StateSync) {
          tn->log(10, SSTR("spawning " << num_shards << " shards sync"));
          for (map<uint32_t, rgw_data_sync_marker>::iterator iter = sync_status.sync_markers.begin();
               iter != sync_status.sync_markers.end(); ++iter) {
            RGWDataSyncShardControlCR *cr = new RGWDataSyncShardControlCR(sc, sync_env->svc->zone->get_zone_params().log_pool,
                                                                          iter->first, iter->second, tn);
            cr->get();
            shard_crs_lock.lock();
            shard_crs[iter->first] = cr;
            shard_crs_lock.unlock();
            spawn(cr, true);
          }
        }
      }

      return set_cr_done();
    }
    return 0;
  }

  RGWCoroutine *set_sync_info_cr() {
    return new RGWSimpleRadosWriteCR<rgw_data_sync_info>(sync_env->async_rados, sync_env->svc->sysobj,
                                                         rgw_raw_obj(sync_env->svc->zone->get_zone_params().log_pool, RGWDataSyncStatusManager::sync_status_oid(sc->source_zone)),
                                                         sync_status.sync_info);
  }

  void wakeup(int shard_id, set<string>& keys) {
    std::lock_guard l{shard_crs_lock};
    map<int, RGWDataSyncShardControlCR *>::iterator iter = shard_crs.find(shard_id);
    if (iter == shard_crs.end()) {
      return;
    }
    iter->second->append_modified_shards(keys);
    iter->second->wakeup();
  }
};

class RGWDefaultDataSyncModule : public RGWDataSyncModule {
public:
  RGWDefaultDataSyncModule() {}

  RGWCoroutine *sync_object(RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key, std::optional<uint64_t> versioned_epoch, rgw_zone_set *zones_trace) override;
  RGWCoroutine *remove_object(RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key, real_time& mtime, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override;
  RGWCoroutine *create_delete_marker(RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key, real_time& mtime,
                                     rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override;
};

class RGWDefaultSyncModuleInstance : public RGWSyncModuleInstance {
  RGWDefaultDataSyncModule data_handler;
public:
  RGWDefaultSyncModuleInstance() {}
  RGWDataSyncModule *get_data_handler() override {
    return &data_handler;
  }
  bool supports_user_writes() override {
    return true;
  }
};

int RGWDefaultSyncModule::create_instance(CephContext *cct, const JSONFormattable& config, RGWSyncModuleInstanceRef *instance)
{
  instance->reset(new RGWDefaultSyncModuleInstance());
  return 0;
}

class RGWUserPermHandler {
  friend struct Init;
  friend class Bucket;

  RGWDataSyncEnv *sync_env;
  rgw_user uid;

  struct _info {
    RGWUserInfo user_info;
    rgw::IAM::Environment env;
    std::unique_ptr<rgw::auth::Identity> identity;
    RGWAccessControlPolicy user_acl;
  };

  std::shared_ptr<_info> info;

  struct Init;

  std::shared_ptr<Init> init_action;

  struct Init : public RGWGenericAsyncCR::Action {
    RGWDataSyncEnv *sync_env;

    rgw_user uid;
    std::shared_ptr<RGWUserPermHandler::_info> info;

    int ret{0};
    
    Init(RGWUserPermHandler *handler) : sync_env(handler->sync_env),
                                        uid(handler->uid),
                                        info(handler->info) {}
    int operate() override {
      auto user_ctl = sync_env->store->getRados()->ctl.user;

      ret = user_ctl->get_info_by_uid(uid, &info->user_info, null_yield);
      if (ret < 0) {
        return ret;
      }

      info->identity = rgw::auth::transform_old_authinfo(sync_env->cct,
                                                         uid,
                                                         RGW_PERM_FULL_CONTROL,
                                                         false, /* system_request? */
                                                         TYPE_RGW);

      map<string, bufferlist> uattrs;

      ret = user_ctl->get_attrs_by_uid(uid, &uattrs, null_yield);
      if (ret == 0) {
        ret = RGWUserPermHandler::policy_from_attrs(sync_env->cct, uattrs, &info->user_acl);
      }
      if (ret == -ENOENT) {
        info->user_acl.create_default(uid, info->user_info.display_name);
      }

      return 0;
    }
  };

public:
  RGWUserPermHandler(RGWDataSyncEnv *_sync_env,
                     const rgw_user& _uid) : sync_env(_sync_env),
                                             uid(_uid) {}

  RGWCoroutine *init_cr() {
    info = make_shared<_info>();
    init_action = make_shared<Init>(this);

    return new RGWGenericAsyncCR(sync_env->cct,
                                 sync_env->async_rados,
                                 init_action);
  }

  class Bucket {
    RGWDataSyncEnv *sync_env;
    std::shared_ptr<_info> info;
    RGWAccessControlPolicy bucket_acl;
    std::optional<perm_state> ps;
  public:
    Bucket() {}

    int init(RGWUserPermHandler *handler,
             const RGWBucketInfo& bucket_info,
             const map<string, bufferlist>& bucket_attrs);

    bool verify_bucket_permission(int perm);
    bool verify_object_permission(const map<string, bufferlist>& obj_attrs,
                                  int perm);
  };

  static int policy_from_attrs(CephContext *cct,
                               const map<string, bufferlist>& attrs,
                               RGWAccessControlPolicy *acl) {
    acl->set_ctx(cct);

    auto aiter = attrs.find(RGW_ATTR_ACL);
    if (aiter == attrs.end()) {
      return -ENOENT;
    }
    auto iter = aiter->second.begin();
    try {
      acl->decode(iter);
    } catch (buffer::error& err) {
      ldout(cct, 0) << "ERROR: " << __func__ << "(): could not decode policy, caught buffer::error" << dendl;
      return -EIO;
    }

    return 0;
  }

  int init_bucket(const RGWBucketInfo& bucket_info,
                  const map<string, bufferlist>& bucket_attrs,
                  Bucket *bs) {
    return bs->init(this, bucket_info, bucket_attrs);
  }
};

int RGWUserPermHandler::Bucket::init(RGWUserPermHandler *handler,
                                     const RGWBucketInfo& bucket_info,
                                     const map<string, bufferlist>& bucket_attrs)
{
  sync_env = handler->sync_env;
  info = handler->info;

  int r = RGWUserPermHandler::policy_from_attrs(sync_env->cct, bucket_attrs, &bucket_acl);
  if (r < 0) {
    return r;
  }

  ps.emplace(sync_env->cct,
             info->env,
             info->identity.get(),
             bucket_info,
             info->identity->get_perm_mask(),
             false, /* defer to bucket acls */
             nullptr, /* referer */
             false); /* request_payer */

  return 0;
}

bool RGWUserPermHandler::Bucket::verify_bucket_permission(int perm)
{
  return verify_bucket_permission_no_policy(sync_env->dpp,
                                            &(*ps),
                                            &info->user_acl,
                                            &bucket_acl,
                                            perm);
}

bool RGWUserPermHandler::Bucket::verify_object_permission(const map<string, bufferlist>& obj_attrs,
                                                          int perm)
{
  RGWAccessControlPolicy obj_acl;

  int r = policy_from_attrs(sync_env->cct, obj_attrs, &obj_acl);
  if (r < 0) {
    return r;
  }

  return verify_bucket_permission_no_policy(sync_env->dpp,
                                            &(*ps),
                                            &bucket_acl,
                                            &obj_acl,
                                            perm);
}

class RGWFetchObjFilter_Sync : public RGWFetchObjFilter_Default {
  rgw_bucket_sync_pipe sync_pipe;

  std::shared_ptr<RGWUserPermHandler::Bucket> bucket_perms;
  std::optional<rgw_sync_pipe_dest_params> verify_dest_params;

  std::optional<ceph::real_time> mtime;
  std::optional<string> etag;
  std::optional<uint64_t> obj_size;

  std::unique_ptr<rgw::auth::Identity> identity;

  std::shared_ptr<bool> need_retry;

public:
  RGWFetchObjFilter_Sync(rgw_bucket_sync_pipe& _sync_pipe,
                         std::shared_ptr<RGWUserPermHandler::Bucket>& _bucket_perms,
                         std::optional<rgw_sync_pipe_dest_params>&& _verify_dest_params,
                         std::shared_ptr<bool>& _need_retry) : sync_pipe(_sync_pipe),
                                         bucket_perms(_bucket_perms),
                                         verify_dest_params(std::move(_verify_dest_params)),
                                         need_retry(_need_retry) {
    *need_retry = false;
  }

  int filter(CephContext *cct,
             const rgw_obj_key& source_key,
             const RGWBucketInfo& dest_bucket_info,
             std::optional<rgw_placement_rule> dest_placement_rule,
             const map<string, bufferlist>& obj_attrs,
             std::optional<rgw_user> *poverride_owner,
             const rgw_placement_rule **prule) override;
};

int RGWFetchObjFilter_Sync::filter(CephContext *cct,
                                   const rgw_obj_key& source_key,
                                   const RGWBucketInfo& dest_bucket_info,
                                   std::optional<rgw_placement_rule> dest_placement_rule,
                                   const map<string, bufferlist>& obj_attrs,
                                   std::optional<rgw_user> *poverride_owner,
                                   const rgw_placement_rule **prule)
{
  int abort_err = -ERR_PRECONDITION_FAILED;

  rgw_sync_pipe_params params;

  RGWObjTags obj_tags;

  auto iter = obj_attrs.find(RGW_ATTR_TAGS);
  if (iter != obj_attrs.end()) {
    try {
      auto it = iter->second.cbegin();
      obj_tags.decode(it);
    } catch (buffer::error &err) {
      ldout(cct, 0) << "ERROR: " << __func__ << ": caught buffer::error couldn't decode TagSet " << dendl;
    }
  }

  if (!sync_pipe.info.handler.find_obj_params(source_key,
                                              obj_tags.get_tags(),
                                              &params)) {
    return abort_err;
  }

  if (verify_dest_params &&
      !(*verify_dest_params == params.dest)) {
    /* raced! original dest params were different, will need to retry */
    ldout(cct, 0) << "WARNING: " << __func__ << ": pipe dest params are different than original params, must have raced with object rewrite, retrying" << dendl;
    *need_retry = true;
    return -ECANCELED;
  }

  std::optional<std::map<string, bufferlist> > new_attrs;

  if (params.dest.acl_translation) {
    rgw_user& acl_translation_owner = params.dest.acl_translation->owner;
    if (!acl_translation_owner.empty()) {
      if (params.mode == rgw_sync_pipe_params::MODE_USER &&
          acl_translation_owner != dest_bucket_info.owner) {
        ldout(cct, 0) << "ERROR: " << __func__ << ": acl translation was requested, but user (" << acl_translation_owner
          << ") is not dest bucket owner (" << dest_bucket_info.owner << ")" << dendl;
        return -EPERM;
      }
      *poverride_owner = acl_translation_owner;
    }
  }
  if (params.mode == rgw_sync_pipe_params::MODE_USER) {
    if (!bucket_perms->verify_object_permission(obj_attrs, RGW_PERM_READ)) {
      ldout(cct, 0) << "ERROR: " << __func__ << ": permission check failed: user not allowed to fetch object" << dendl;
      return -EPERM;
    }
  }

  if (!dest_placement_rule &&
      params.dest.storage_class) {
    dest_rule.storage_class = *params.dest.storage_class;
    dest_rule.inherit_from(dest_bucket_info.placement_rule);
    dest_placement_rule = dest_rule;
    *prule = &dest_rule;
  }

  return RGWFetchObjFilter_Default::filter(cct,
                                           source_key,
                                           dest_bucket_info,
                                           dest_placement_rule,
                                           obj_attrs,
                                           poverride_owner,
                                           prule);
}

class RGWObjFetchCR : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  rgw_bucket_sync_pipe& sync_pipe;
  rgw_obj_key& key;
  std::optional<rgw_obj_key> dest_key;
  std::optional<uint64_t> versioned_epoch;
  rgw_zone_set *zones_trace;

  bool need_more_info{false};
  bool check_change{false};

  ceph::real_time src_mtime;
  uint64_t src_size;
  string src_etag;
  map<string, bufferlist> src_attrs;
  map<string, string> src_headers;

  std::optional<rgw_user> param_user;
  rgw_sync_pipe_params::Mode param_mode;

  std::optional<RGWUserPermHandler> user_perms;
  std::shared_ptr<RGWUserPermHandler::Bucket> source_bucket_perms;
  RGWUserPermHandler::Bucket dest_bucket_perms;

  std::optional<rgw_sync_pipe_dest_params> dest_params;

  int try_num{0};
  std::shared_ptr<bool> need_retry;
public:
  RGWObjFetchCR(RGWDataSyncCtx *_sc,
                rgw_bucket_sync_pipe& _sync_pipe,
                rgw_obj_key& _key,
                std::optional<rgw_obj_key> _dest_key,
                std::optional<uint64_t> _versioned_epoch,
                rgw_zone_set *_zones_trace) : RGWCoroutine(_sc->cct),
                                              sc(_sc), sync_env(_sc->env),
                                              sync_pipe(_sync_pipe),
                                              key(_key),
                                              dest_key(_dest_key),
                                              versioned_epoch(_versioned_epoch),
                                              zones_trace(_zones_trace) {
  }


  int operate() override {
    reenter(this) {

#define MAX_RACE_RETRIES_OBJ_FETCH 10
      for (try_num = 0; try_num < MAX_RACE_RETRIES_OBJ_FETCH; ++try_num) {

        {
          std::optional<rgw_user> param_acl_translation;
          std::optional<string> param_storage_class;

          if (!sync_pipe.info.handler.find_basic_info_without_tags(key,
                                                                   &param_user,
                                                                   &param_acl_translation,
                                                                   &param_storage_class,
                                                                   &param_mode,
                                                                   &need_more_info)) {
            if (!need_more_info) {
              return set_cr_error(-ERR_PRECONDITION_FAILED);
            }
          }
        }

        if (need_more_info) {
          ldout(cct, 20) << "Could not determine exact policy rule for obj=" << key << ", will read source object attributes" << dendl;
          /*
           * we need to fetch info about source object, so that we can determine
           * the correct policy configuration. This can happen if there are multiple
           * policy rules, and some depend on the object tagging */
          yield call(new RGWStatRemoteObjCR(sync_env->async_rados,
                                            sync_env->store,
                                            sc->source_zone,
                                            sync_pipe.info.source_bs.bucket,
                                            key,
                                            &src_mtime,
                                            &src_size,
                                            &src_etag,
                                            &src_attrs,
                                            &src_headers));
          if (retcode < 0) {
            return set_cr_error(retcode);
          }

          RGWObjTags obj_tags;

          auto iter = src_attrs.find(RGW_ATTR_TAGS);
          if (iter != src_attrs.end()) {
            try {
              auto it = iter->second.cbegin();
              obj_tags.decode(it);
            } catch (buffer::error &err) {
              ldout(cct, 0) << "ERROR: " << __func__ << ": caught buffer::error couldn't decode TagSet " << dendl;
            }
          }

          rgw_sync_pipe_params params;
          if (!sync_pipe.info.handler.find_obj_params(key,
                                                      obj_tags.get_tags(),
                                                      &params)) {
            return set_cr_error(-ERR_PRECONDITION_FAILED);
          }

          param_user = params.user;
          param_mode = params.mode;

          dest_params = params.dest;
        }

        if (param_mode == rgw_sync_pipe_params::MODE_USER) {
          if (!param_user) {
            ldout(cct, 20) << "ERROR: " << __func__ << ": user level sync but user param not set" << dendl;
            return set_cr_error(-EPERM);
          }
          user_perms.emplace(sync_env, *param_user);

          yield call(user_perms->init_cr());
          if (retcode < 0) {
            ldout(cct, 20) << "ERROR: " << __func__ << ": failed to init user perms manager for uid=" << *param_user << dendl;
            return set_cr_error(retcode);
          }

          /* verify that user is allowed to write at the target bucket */
          int r = user_perms->init_bucket(sync_pipe.dest_bucket_info,
                                          sync_pipe.dest_bucket_attrs,
                                          &dest_bucket_perms);
          if (r < 0) {
            ldout(cct, 20) << "ERROR: " << __func__ << ": failed to init bucket perms manager for uid=" << *param_user << " bucket=" << sync_pipe.source_bucket_info.bucket.get_key() << dendl;
            return set_cr_error(retcode);
          }

          if (!dest_bucket_perms.verify_bucket_permission(RGW_PERM_WRITE)) {
            ldout(cct, 0) << "ERROR: " << __func__ << ": permission check failed: user not allowed to write into bucket (bucket=" << sync_pipe.info.dest_bs.bucket.get_key() << ")" << dendl;
            return -EPERM;
          }

          /* init source bucket permission structure */
          source_bucket_perms = make_shared<RGWUserPermHandler::Bucket>();
          r = user_perms->init_bucket(sync_pipe.source_bucket_info,
                                      sync_pipe.source_bucket_attrs,
                                      source_bucket_perms.get());
          if (r < 0) {
            ldout(cct, 20) << "ERROR: " << __func__ << ": failed to init bucket perms manager for uid=" << *param_user << " bucket=" << sync_pipe.source_bucket_info.bucket.get_key() << dendl;
            return set_cr_error(retcode);
          }
        }

        yield {
          if (!need_retry) {
            need_retry = make_shared<bool>();
          }
          auto filter = make_shared<RGWFetchObjFilter_Sync>(sync_pipe,
                                                            source_bucket_perms,
                                                            std::move(dest_params),
                                                            need_retry);

          call(new RGWFetchRemoteObjCR(sync_env->async_rados, sync_env->store, sc->source_zone,
                                       nullopt,
                                       sync_pipe.info.source_bs.bucket,
                                       std::nullopt, sync_pipe.dest_bucket_info,
                                       key, dest_key, versioned_epoch,
                                       true,
                                       std::static_pointer_cast<RGWFetchObjFilter>(filter),
                                       zones_trace, sync_env->counters, sync_env->dpp));
        }
        if (retcode < 0) {
          if (*need_retry) {
            continue;
          }
          return set_cr_error(retcode);
        }

        return set_cr_done();
      }

      ldout(cct, 0) << "ERROR: " << __func__ << ": Too many retries trying to fetch object, possibly a bug: bucket=" << sync_pipe.source_bucket_info.bucket.get_key() << " key=" << key << dendl;

      return set_cr_error(-EIO);
    }
    return 0;
  }
};

RGWCoroutine *RGWDefaultDataSyncModule::sync_object(RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key, std::optional<uint64_t> versioned_epoch, rgw_zone_set *zones_trace)
{
  return new RGWObjFetchCR(sc, sync_pipe, key, std::nullopt, versioned_epoch, zones_trace);
}

RGWCoroutine *RGWDefaultDataSyncModule::remove_object(RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key,
                                                      real_time& mtime, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace)
{
  auto sync_env = sc->env;
  return new RGWRemoveObjCR(sync_env->async_rados, sync_env->store, sc->source_zone,
                            sync_pipe.dest_bucket_info, key, versioned, versioned_epoch,
                            NULL, NULL, false, &mtime, zones_trace);
}

RGWCoroutine *RGWDefaultDataSyncModule::create_delete_marker(RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key, real_time& mtime,
                                                             rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace)
{
  auto sync_env = sc->env;
  return new RGWRemoveObjCR(sync_env->async_rados, sync_env->store, sc->source_zone,
                            sync_pipe.dest_bucket_info, key, versioned, versioned_epoch,
                            &owner.id, &owner.display_name, true, &mtime, zones_trace);
}

class RGWArchiveDataSyncModule : public RGWDefaultDataSyncModule {
public:
  RGWArchiveDataSyncModule() {}

  RGWCoroutine *sync_object(RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key, std::optional<uint64_t> versioned_epoch, rgw_zone_set *zones_trace) override;
  RGWCoroutine *remove_object(RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key, real_time& mtime, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override;
  RGWCoroutine *create_delete_marker(RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key, real_time& mtime,
                                     rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override;
};

class RGWArchiveSyncModuleInstance : public RGWDefaultSyncModuleInstance {
  RGWArchiveDataSyncModule data_handler;
public:
  RGWArchiveSyncModuleInstance() {}
  RGWDataSyncModule *get_data_handler() override {
    return &data_handler;
  }
  RGWMetadataHandler *alloc_bucket_meta_handler() override {
    return RGWArchiveBucketMetaHandlerAllocator::alloc();
  }
  RGWBucketInstanceMetadataHandlerBase *alloc_bucket_instance_meta_handler() override {
    return RGWArchiveBucketInstanceMetaHandlerAllocator::alloc();
  }
};

int RGWArchiveSyncModule::create_instance(CephContext *cct, const JSONFormattable& config, RGWSyncModuleInstanceRef *instance)
{
  instance->reset(new RGWArchiveSyncModuleInstance());
  return 0;
}

RGWCoroutine *RGWArchiveDataSyncModule::sync_object(RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key, std::optional<uint64_t> versioned_epoch, rgw_zone_set *zones_trace)
{
  auto sync_env = sc->env;
  ldout(sc->cct, 5) << "SYNC_ARCHIVE: sync_object: b=" << sync_pipe.info.source_bs.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch.value_or(0) << dendl;
  if (!sync_pipe.dest_bucket_info.versioned() ||
     (sync_pipe.dest_bucket_info.flags & BUCKET_VERSIONS_SUSPENDED)) {
      ldout(sc->cct, 0) << "SYNC_ARCHIVE: sync_object: enabling object versioning for archive bucket" << dendl;
      sync_pipe.dest_bucket_info.flags = (sync_pipe.dest_bucket_info.flags & ~BUCKET_VERSIONS_SUSPENDED) | BUCKET_VERSIONED;
      int op_ret = sync_env->store->getRados()->put_bucket_instance_info(sync_pipe.dest_bucket_info, false, real_time(), NULL);
      if (op_ret < 0) {
         ldout(sc->cct, 0) << "SYNC_ARCHIVE: sync_object: error versioning archive bucket" << dendl;
         return NULL;
      }
  }

  std::optional<rgw_obj_key> dest_key;

  if (versioned_epoch.value_or(0) == 0) { /* force version if not set */
    versioned_epoch = 0;
    dest_key = key;
    if (key.instance.empty()) {
      sync_env->store->getRados()->gen_rand_obj_instance_name(&(*dest_key));
    }
  }

  return new RGWObjFetchCR(sc, sync_pipe, key, dest_key, versioned_epoch, zones_trace);
}

RGWCoroutine *RGWArchiveDataSyncModule::remove_object(RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key,
                                                     real_time& mtime, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace)
{
  ldout(sc->cct, 0) << "SYNC_ARCHIVE: remove_object: b=" << sync_pipe.info.source_bs.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch << dendl;
  return NULL;
}

RGWCoroutine *RGWArchiveDataSyncModule::create_delete_marker(RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key, real_time& mtime,
                                                            rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace)
{
  ldout(sc->cct, 0) << "SYNC_ARCHIVE: create_delete_marker: b=" << sync_pipe.info.source_bs.bucket << " k=" << key << " mtime=" << mtime
	                            << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
  auto sync_env = sc->env;
  return new RGWRemoveObjCR(sync_env->async_rados, sync_env->store, sc->source_zone,
                            sync_pipe.dest_bucket_info, key, versioned, versioned_epoch,
                            &owner.id, &owner.display_name, true, &mtime, zones_trace);
}

class RGWDataSyncControlCR : public RGWBackoffControlCR
{
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  uint32_t num_shards;

  RGWSyncTraceNodeRef tn;

  static constexpr bool exit_on_error = false; // retry on all errors
public:
  RGWDataSyncControlCR(RGWDataSyncCtx *_sc, uint32_t _num_shards,
                       RGWSyncTraceNodeRef& _tn_parent) : RGWBackoffControlCR(_sc->cct, exit_on_error),
                                                          sc(_sc), sync_env(_sc->env), num_shards(_num_shards) {
    tn = sync_env->sync_tracer->add_node(_tn_parent, "sync");
  }

  RGWCoroutine *alloc_cr() override {
    return new RGWDataSyncCR(sc, num_shards, tn, backoff_ptr());
  }

  void wakeup(int shard_id, set<string>& keys) {
    ceph::mutex& m = cr_lock();

    m.lock();
    RGWDataSyncCR *cr = static_cast<RGWDataSyncCR *>(get_cr());
    if (!cr) {
      m.unlock();
      return;
    }

    cr->get();
    m.unlock();

    if (cr) {
      tn->log(20, SSTR("notify shard=" << shard_id << " keys=" << keys));
      cr->wakeup(shard_id, keys);
    }

    cr->put();
  }
};

void RGWRemoteDataLog::wakeup(int shard_id, set<string>& keys) {
  std::shared_lock rl{lock};
  if (!data_sync_cr) {
    return;
  }
  data_sync_cr->wakeup(shard_id, keys);
}

int RGWRemoteDataLog::run_sync(int num_shards)
{
  lock.lock();
  data_sync_cr = new RGWDataSyncControlCR(&sc, num_shards, tn);
  data_sync_cr->get(); // run() will drop a ref, so take another
  lock.unlock();

  int r = run(data_sync_cr);

  lock.lock();
  data_sync_cr->put();
  data_sync_cr = NULL;
  lock.unlock();

  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to run sync" << dendl;
    return r;
  }
  return 0;
}

CephContext *RGWDataSyncStatusManager::get_cct() const
{
  return store->ctx();
}

int RGWDataSyncStatusManager::init()
{
  RGWZone *zone_def;

  if (!store->svc()->zone->find_zone(source_zone, &zone_def)) {
    ldpp_dout(this, 0) << "ERROR: failed to find zone config info for zone=" << source_zone << dendl;
    return -EIO;
  }

  if (!store->svc()->sync_modules->get_manager()->supports_data_export(zone_def->tier_type)) {
    return -ENOTSUP;
  }

  const RGWZoneParams& zone_params = store->svc()->zone->get_zone_params();

  if (sync_module == nullptr) { 
    sync_module = store->getRados()->get_sync_module();
  }

  conn = store->svc()->zone->get_zone_conn(source_zone);
  if (!conn) {
    ldpp_dout(this, 0) << "connection object to zone " << source_zone << " does not exist" << dendl;
    return -EINVAL;
  }

  error_logger = new RGWSyncErrorLogger(store, RGW_SYNC_ERROR_LOG_SHARD_PREFIX, ERROR_LOGGER_SHARDS);

  int r = source_log.init(source_zone, conn, error_logger, store->getRados()->get_sync_tracer(),
                          sync_module, counters);
  if (r < 0) {
    ldpp_dout(this, 0) << "ERROR: failed to init remote log, r=" << r << dendl;
    finalize();
    return r;
  }

  rgw_datalog_info datalog_info;
  r = source_log.read_log_info(&datalog_info);
  if (r < 0) {
    ldpp_dout(this, 5) << "ERROR: master.read_log_info() returned r=" << r << dendl;
    finalize();
    return r;
  }

  num_shards = datalog_info.num_shards;

  for (int i = 0; i < num_shards; i++) {
    shard_objs[i] = rgw_raw_obj(zone_params.log_pool, shard_obj_name(source_zone, i));
  }

  return 0;
}

void RGWDataSyncStatusManager::finalize()
{
  delete error_logger;
  error_logger = nullptr;
}

unsigned RGWDataSyncStatusManager::get_subsys() const
{
  return dout_subsys;
}

std::ostream& RGWDataSyncStatusManager::gen_prefix(std::ostream& out) const
{
  auto zone = std::string_view{source_zone.id};
  return out << "data sync zone:" << zone.substr(0, 8) << ' ';
}

string RGWDataSyncStatusManager::sync_status_oid(const rgw_zone_id& source_zone)
{
  char buf[datalog_sync_status_oid_prefix.size() + source_zone.id.size() + 16];
  snprintf(buf, sizeof(buf), "%s.%s", datalog_sync_status_oid_prefix.c_str(), source_zone.id.c_str());

  return string(buf);
}

string RGWDataSyncStatusManager::shard_obj_name(const rgw_zone_id& source_zone, int shard_id)
{
  char buf[datalog_sync_status_shard_prefix.size() + source_zone.id.size() + 16];
  snprintf(buf, sizeof(buf), "%s.%s.%d", datalog_sync_status_shard_prefix.c_str(), source_zone.id.c_str(), shard_id);

  return string(buf);
}

class RGWReadRemoteBucketIndexLogInfoCR : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  const string instance_key;

  rgw_bucket_index_marker_info *info;

public:
  RGWReadRemoteBucketIndexLogInfoCR(RGWDataSyncCtx *_sc,
                                  const rgw_bucket_shard& bs,
                                  rgw_bucket_index_marker_info *_info)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
      instance_key(bs.get_key()), info(_info) {}

  int operate() override {
    reenter(this) {
      yield {
        rgw_http_param_pair pairs[] = { { "type" , "bucket-index" },
	                                { "bucket-instance", instance_key.c_str() },
					{ "info" , NULL },
	                                { NULL, NULL } };

        string p = "/admin/log/";
        call(new RGWReadRESTResourceCR<rgw_bucket_index_marker_info>(sync_env->cct, sc->conn, sync_env->http_manager, p, pairs, info));
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
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;

  const rgw_bucket_sync_pair_info& sync_pair;
  const string sync_status_oid;

  rgw_bucket_shard_sync_info& status;
  RGWObjVersionTracker& objv_tracker;
  rgw_bucket_index_marker_info info;
public:
  RGWInitBucketShardSyncStatusCoroutine(RGWDataSyncCtx *_sc,
                                        const rgw_bucket_sync_pair_info& _sync_pair,
                                        rgw_bucket_shard_sync_info& _status,
                                        RGWObjVersionTracker& objv_tracker)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
      sync_pair(_sync_pair),
      sync_status_oid(RGWBucketPipeSyncStatusManager::status_oid(sc->source_zone, _sync_pair)),
      status(_status), objv_tracker(objv_tracker)
  {}

  int operate() override {
    reenter(this) {
      /* fetch current position in logs */
      yield call(new RGWReadRemoteBucketIndexLogInfoCR(sc, sync_pair.source_bs, &info));
      if (retcode < 0 && retcode != -ENOENT) {
        ldout(cct, 0) << "ERROR: failed to fetch bucket index status" << dendl;
        return set_cr_error(retcode);
      }
      yield {
        auto store = sync_env->store;
        rgw_raw_obj obj(sync_env->svc->zone->get_zone_params().log_pool, sync_status_oid);
        const bool stopped = status.state == rgw_bucket_shard_sync_info::StateStopped;
        bool write_status = false;

        if (info.syncstopped) {
          if (stopped && !sync_env->sync_module->should_full_sync()) {
            // preserve our current incremental marker position
            write_status = true;
          }
        } else {
          // whether or not to do full sync, incremental sync will follow anyway
          if (sync_env->sync_module->should_full_sync()) {
            status.state = rgw_bucket_shard_sync_info::StateFullSync;
            status.inc_marker.position = info.max_marker;
          } else {
            // clear the marker position unless we're resuming from SYNCSTOP
            if (!stopped) {
              status.inc_marker.position = "";
            }
            status.state = rgw_bucket_shard_sync_info::StateIncrementalSync;
          }
          write_status = true;
          status.inc_marker.timestamp = ceph::real_clock::now();
        }

        if (write_status) {
          map<string, bufferlist> attrs;
          status.encode_all_attrs(attrs);
          call(new RGWSimpleRadosWriteAttrsCR(sync_env->async_rados, sync_env->svc->sysobj, obj, attrs, &objv_tracker));
        } else {
          call(new RGWRadosRemoveCR(store, obj, &objv_tracker));
        }
      }
      if (info.syncstopped) {
        retcode = -ENOENT;
      }
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }
};

RGWRemoteBucketManager::RGWRemoteBucketManager(const DoutPrefixProvider *_dpp,
                                               RGWDataSyncEnv *_sync_env,
                                               const rgw_zone_id& _source_zone,
                                               RGWRESTConn *_conn,
                                               const RGWBucketInfo& source_bucket_info,
                                               const rgw_bucket& dest_bucket) : dpp(_dpp), sync_env(_sync_env)
{
  conn = _conn;
  source_zone = _source_zone;

  int num_shards = (source_bucket_info.layout.current_index.layout.normal.num_shards <= 0 ? 
                    1 : source_bucket_info.layout.current_index.layout.normal.num_shards);

  sync_pairs.resize(num_shards);

  int cur_shard = std::min<int>(source_bucket_info.layout.current_index.layout.normal.num_shards, 0);

  for (int i = 0; i < num_shards; ++i, ++cur_shard) {
    auto& sync_pair = sync_pairs[i];

    sync_pair.source_bs.bucket = source_bucket_info.bucket;
    sync_pair.dest_bs.bucket = dest_bucket;

    sync_pair.source_bs.shard_id = (source_bucket_info.layout.current_index.layout.normal.num_shards > 0 ? cur_shard : -1);

    if (dest_bucket == source_bucket_info.bucket) {
      sync_pair.dest_bs.shard_id = sync_pair.source_bs.shard_id;
    } else {
      sync_pair.dest_bs.shard_id = -1;
    }
  }

  sc.init(sync_env, conn, source_zone);
}

RGWCoroutine *RGWRemoteBucketManager::init_sync_status_cr(int num, RGWObjVersionTracker& objv_tracker)
{
  if ((size_t)num >= sync_pairs.size()) {
    return nullptr;
  }
  return new RGWInitBucketShardSyncStatusCoroutine(&sc, sync_pairs[num], init_status, objv_tracker);
}

#define BUCKET_SYNC_ATTR_PREFIX RGW_ATTR_PREFIX "bucket-sync."

template <class T>
static bool decode_attr(CephContext *cct, map<string, bufferlist>& attrs, const string& attr_name, T *val)
{
  map<string, bufferlist>::iterator iter = attrs.find(attr_name);
  if (iter == attrs.end()) {
    *val = T();
    return false;
  }

  auto biter = iter->second.cbegin();
  try {
    decode(*val, biter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode attribute: " << attr_name << dendl;
    return false;
  }
  return true;
}

void rgw_bucket_shard_sync_info::decode_from_attrs(CephContext *cct, map<string, bufferlist>& attrs)
{
  if (!decode_attr(cct, attrs, BUCKET_SYNC_ATTR_PREFIX "state", &state)) {
    decode_attr(cct, attrs, "state", &state);
  }
  if (!decode_attr(cct, attrs, BUCKET_SYNC_ATTR_PREFIX "full_marker", &full_marker)) {
    decode_attr(cct, attrs, "full_marker", &full_marker);
  }
  if (!decode_attr(cct, attrs, BUCKET_SYNC_ATTR_PREFIX "inc_marker", &inc_marker)) {
    decode_attr(cct, attrs, "inc_marker", &inc_marker);
  }
}

void rgw_bucket_shard_sync_info::encode_all_attrs(map<string, bufferlist>& attrs)
{
  encode_state_attr(attrs);
  full_marker.encode_attr(attrs);
  inc_marker.encode_attr(attrs);
}

void rgw_bucket_shard_sync_info::encode_state_attr(map<string, bufferlist>& attrs)
{
  using ceph::encode;
  encode(state, attrs[BUCKET_SYNC_ATTR_PREFIX "state"]);
}

void rgw_bucket_shard_full_sync_marker::encode_attr(map<string, bufferlist>& attrs)
{
  using ceph::encode;
  encode(*this, attrs[BUCKET_SYNC_ATTR_PREFIX "full_marker"]);
}

void rgw_bucket_shard_inc_sync_marker::encode_attr(map<string, bufferlist>& attrs)
{
  using ceph::encode;
  encode(*this, attrs[BUCKET_SYNC_ATTR_PREFIX "inc_marker"]);
}

class RGWReadBucketPipeSyncStatusCoroutine : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  string oid;
  rgw_bucket_shard_sync_info *status;
  RGWObjVersionTracker* objv_tracker;
  map<string, bufferlist> attrs;
public:
  RGWReadBucketPipeSyncStatusCoroutine(RGWDataSyncCtx *_sc,
                                   const rgw_bucket_sync_pair_info& sync_pair,
                                   rgw_bucket_shard_sync_info *_status,
                                   RGWObjVersionTracker* objv_tracker)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
      oid(RGWBucketPipeSyncStatusManager::status_oid(sc->source_zone, sync_pair)),
      status(_status), objv_tracker(objv_tracker)
  {}
  int operate() override;
};

int RGWReadBucketPipeSyncStatusCoroutine::operate()
{
  reenter(this) {
    yield call(new RGWSimpleRadosReadAttrsCR(sync_env->async_rados, sync_env->svc->sysobj,
                                             rgw_raw_obj(sync_env->svc->zone->get_zone_params().log_pool, oid),
                                             &attrs, true, objv_tracker));
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

#define OMAP_READ_MAX_ENTRIES 10
class RGWReadRecoveringBucketShardsCoroutine : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  rgw::sal::RGWRadosStore *store;
  
  const int shard_id;
  int max_entries;

  set<string>& recovering_buckets;
  string marker;
  string error_oid;

  RGWRadosGetOmapKeysCR::ResultPtr omapkeys;
  set<string> error_entries;
  int max_omap_entries;
  int count;

public:
  RGWReadRecoveringBucketShardsCoroutine(RGWDataSyncCtx *_sc, const int _shard_id,
                                      set<string>& _recovering_buckets, const int _max_entries) 
  : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
  store(sync_env->store), shard_id(_shard_id), max_entries(_max_entries),
  recovering_buckets(_recovering_buckets), max_omap_entries(OMAP_READ_MAX_ENTRIES)
  {
    error_oid = RGWDataSyncStatusManager::shard_obj_name(sc->source_zone, shard_id) + ".retry";
  }

  int operate() override;
};

int RGWReadRecoveringBucketShardsCoroutine::operate()
{
  reenter(this){
    //read recovering bucket shards
    count = 0;
    do {
      omapkeys = std::make_shared<RGWRadosGetOmapKeysCR::Result>();
      yield call(new RGWRadosGetOmapKeysCR(store, rgw_raw_obj(sync_env->svc->zone->get_zone_params().log_pool, error_oid), 
            marker, max_omap_entries, omapkeys));

      if (retcode == -ENOENT) {
        break;
      }

      if (retcode < 0) {
        ldout(sync_env->cct, 0) << "failed to read recovering bucket shards with " 
          << cpp_strerror(retcode) << dendl;
        return set_cr_error(retcode);
      }

      error_entries = std::move(omapkeys->entries);
      if (error_entries.empty()) {
        break;
      }

      count += error_entries.size();
      marker = *error_entries.rbegin();
      recovering_buckets.insert(std::make_move_iterator(error_entries.begin()),
                                std::make_move_iterator(error_entries.end()));
    } while (omapkeys->more && count < max_entries);
  
    return set_cr_done();
  }

  return 0;
}

class RGWReadPendingBucketShardsCoroutine : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  rgw::sal::RGWRadosStore *store;
  
  const int shard_id;
  int max_entries;

  set<string>& pending_buckets;
  string marker;
  string status_oid;

  rgw_data_sync_marker* sync_marker;
  int count;

  std::string next_marker;
  list<rgw_data_change_log_entry> log_entries;
  bool truncated;

public:
  RGWReadPendingBucketShardsCoroutine(RGWDataSyncCtx *_sc, const int _shard_id,
                                      set<string>& _pending_buckets,
                                      rgw_data_sync_marker* _sync_marker, const int _max_entries) 
  : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
  store(sync_env->store), shard_id(_shard_id), max_entries(_max_entries),
  pending_buckets(_pending_buckets), sync_marker(_sync_marker)
  {
    status_oid = RGWDataSyncStatusManager::shard_obj_name(sc->source_zone, shard_id);
  }

  int operate() override;
};

int RGWReadPendingBucketShardsCoroutine::operate()
{
  reenter(this){
    //read sync status marker
    using CR = RGWSimpleRadosReadCR<rgw_data_sync_marker>;
    yield call(new CR(sync_env->async_rados, sync_env->svc->sysobj,
                      rgw_raw_obj(sync_env->svc->zone->get_zone_params().log_pool, status_oid),
                      sync_marker));
    if (retcode < 0) {
      ldout(sync_env->cct,0) << "failed to read sync status marker with " 
        << cpp_strerror(retcode) << dendl;
      return set_cr_error(retcode);
    }

    //read pending bucket shards
    marker = sync_marker->marker;
    count = 0;
    do{
      yield call(new RGWReadRemoteDataLogShardCR(sc, shard_id, marker,
                                                 &next_marker, &log_entries, &truncated));

      if (retcode == -ENOENT) {
        break;
      }

      if (retcode < 0) {
        ldout(sync_env->cct,0) << "failed to read remote data log info with " 
          << cpp_strerror(retcode) << dendl;
        return set_cr_error(retcode);
      }

      if (log_entries.empty()) {
        break;
      }

      count += log_entries.size();
      for (const auto& entry : log_entries) {
        pending_buckets.insert(entry.entry.key);
      }
    }while(truncated && count < max_entries);

    return set_cr_done();
  }

  return 0;
}

int RGWRemoteDataLog::read_shard_status(int shard_id, set<string>& pending_buckets, set<string>& recovering_buckets, rgw_data_sync_marker *sync_marker, const int max_entries)
{
  // cannot run concurrently with run_sync(), so run in a separate manager
  RGWCoroutinesManager crs(store->ctx(), store->getRados()->get_cr_registry());
  RGWHTTPManager http_manager(store->ctx(), crs.get_completion_mgr());
  int ret = http_manager.start();
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "failed in http_manager.start() ret=" << ret << dendl;
    return ret;
  }
  RGWDataSyncEnv sync_env_local = sync_env;
  sync_env_local.http_manager = &http_manager;
  RGWDataSyncCtx sc_local = sc;
  sc_local.env = &sync_env_local;
  list<RGWCoroutinesStack *> stacks;
  RGWCoroutinesStack* recovering_stack = new RGWCoroutinesStack(store->ctx(), &crs);
  recovering_stack->call(new RGWReadRecoveringBucketShardsCoroutine(&sc_local, shard_id, recovering_buckets, max_entries));
  stacks.push_back(recovering_stack);
  RGWCoroutinesStack* pending_stack = new RGWCoroutinesStack(store->ctx(), &crs);
  pending_stack->call(new RGWReadPendingBucketShardsCoroutine(&sc_local, shard_id, pending_buckets, sync_marker, max_entries));
  stacks.push_back(pending_stack);
  ret = crs.run(stacks);
  http_manager.stop();
  return ret;
}

RGWCoroutine *RGWRemoteBucketManager::read_sync_status_cr(int num, rgw_bucket_shard_sync_info *sync_status)
{
  if ((size_t)num >= sync_pairs.size()) {
    return nullptr;
  }

  return new RGWReadBucketPipeSyncStatusCoroutine(&sc, sync_pairs[num], sync_status, nullptr);
}

RGWBucketPipeSyncStatusManager::RGWBucketPipeSyncStatusManager(rgw::sal::RGWRadosStore *_store,
                                                               std::optional<rgw_zone_id> _source_zone,
                                                               std::optional<rgw_bucket> _source_bucket,
                                                               const rgw_bucket& _dest_bucket) : store(_store),
                                                                                   cr_mgr(_store->ctx(), _store->getRados()->get_cr_registry()),
                                                                                   http_manager(store->ctx(), cr_mgr.get_completion_mgr()),
                                                                                   source_zone(_source_zone), source_bucket(_source_bucket),
                                                                                   conn(NULL), error_logger(NULL),
                                                                                   dest_bucket(_dest_bucket),
                                                                                   num_shards(0)
{
}

RGWBucketPipeSyncStatusManager::~RGWBucketPipeSyncStatusManager()
{
  for (vector<RGWRemoteBucketManager *>::iterator iter = source_mgrs.begin(); iter != source_mgrs.end(); ++iter) {
    delete *iter;
  }
  delete error_logger;
}

CephContext *RGWBucketPipeSyncStatusManager::get_cct() const
{
  return store->ctx();
}

void rgw_bucket_entry_owner::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("ID", id, obj);
  JSONDecoder::decode_json("DisplayName", display_name, obj);
}

struct bucket_list_entry {
  bool delete_marker;
  rgw_obj_key key;
  bool is_latest;
  real_time mtime;
  string etag;
  uint64_t size;
  string storage_class;
  rgw_bucket_entry_owner owner;
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
      ts.tv_sec = (uint64_t)internal_timegm(&t);
      ts.tv_nsec = nsec;
      mtime = real_clock::from_ceph_timespec(ts);
    }
    JSONDecoder::decode_json("ETag", etag, obj);
    JSONDecoder::decode_json("Size", size, obj);
    JSONDecoder::decode_json("StorageClass", storage_class, obj);
    JSONDecoder::decode_json("Owner", owner, obj);
    JSONDecoder::decode_json("VersionedEpoch", versioned_epoch, obj);
    JSONDecoder::decode_json("RgwxTag", rgw_tag, obj);
    if (key.instance == "null" && !versioned_epoch) {
      key.instance.clear();
    }
  }

  RGWModifyOp get_modify_op() const {
    if (delete_marker) {
      return CLS_RGW_OP_LINK_OLH_DM;
    } else if (!key.instance.empty() && key.instance != "null") {
      return CLS_RGW_OP_LINK_OLH;
    } else {
      return CLS_RGW_OP_ADD;
    }
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
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  const rgw_bucket_shard& bs;
  const string instance_key;
  rgw_obj_key marker_position;

  bucket_list_result *result;

public:
  RGWListBucketShardCR(RGWDataSyncCtx *_sc, const rgw_bucket_shard& bs,
                       rgw_obj_key& _marker_position, bucket_list_result *_result)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env), bs(bs),
      instance_key(bs.get_key()), marker_position(_marker_position),
      result(_result) {}

  int operate() override {
    reenter(this) {
      yield {
        rgw_http_param_pair pairs[] = { { "rgwx-bucket-instance", instance_key.c_str() },
					{ "versions" , NULL },
					{ "format" , "json" },
					{ "objs-container" , "true" },
					{ "key-marker" , marker_position.name.c_str() },
					{ "version-id-marker" , marker_position.instance.c_str() },
	                                { NULL, NULL } };
        // don't include tenant in the url, it's already part of instance_key
        string p = string("/") + bs.bucket.name;
        call(new RGWReadRESTResourceCR<bucket_list_result>(sync_env->cct, sc->conn, sync_env->http_manager, p, pairs, result));
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
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  const string instance_key;
  string marker;

  list<rgw_bi_log_entry> *result;
  std::optional<PerfGuard> timer;

public:
  RGWListBucketIndexLogCR(RGWDataSyncCtx *_sc, const rgw_bucket_shard& bs,
                          string& _marker, list<rgw_bi_log_entry> *_result)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
      instance_key(bs.get_key()), marker(_marker), result(_result) {}

  int operate() override {
    reenter(this) {
      if (sync_env->counters) {
        timer.emplace(sync_env->counters, sync_counters::l_poll);
      }
      yield {
        rgw_http_param_pair pairs[] = { { "bucket-instance", instance_key.c_str() },
					{ "format" , "json" },
					{ "marker" , marker.c_str() },
					{ "type", "bucket-index" },
	                                { NULL, NULL } };

        call(new RGWReadRESTResourceCR<list<rgw_bi_log_entry> >(sync_env->cct, sc->conn, sync_env->http_manager, "/admin/log", pairs, result));
      }
      timer.reset();
      if (retcode < 0) {
        if (sync_env->counters) {
          sync_env->counters->inc(sync_counters::l_poll_err);
        }
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }
};

#define BUCKET_SYNC_UPDATE_MARKER_WINDOW 10

class RGWBucketFullSyncShardMarkerTrack : public RGWSyncShardMarkerTrack<rgw_obj_key, rgw_obj_key> {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;

  string marker_oid;
  rgw_bucket_shard_full_sync_marker sync_marker;
  RGWSyncTraceNodeRef tn;
  RGWObjVersionTracker& objv_tracker;

public:
  RGWBucketFullSyncShardMarkerTrack(RGWDataSyncCtx *_sc,
                                    const string& _marker_oid,
                                    const rgw_bucket_shard_full_sync_marker& _marker,
                                    RGWSyncTraceNodeRef tn,
                                    RGWObjVersionTracker& objv_tracker)
    : RGWSyncShardMarkerTrack(BUCKET_SYNC_UPDATE_MARKER_WINDOW),
      sc(_sc), sync_env(_sc->env), marker_oid(_marker_oid),
      sync_marker(_marker), tn(std::move(tn)), objv_tracker(objv_tracker)
  {}

  RGWCoroutine *store_marker(const rgw_obj_key& new_marker, uint64_t index_pos, const real_time& timestamp) override {
    sync_marker.position = new_marker;
    sync_marker.count = index_pos;

    map<string, bufferlist> attrs;
    sync_marker.encode_attr(attrs);

    tn->log(20, SSTR("updating marker marker_oid=" << marker_oid << " marker=" << new_marker));
    return new RGWSimpleRadosWriteAttrsCR(sync_env->async_rados, sync_env->svc->sysobj,
                                          rgw_raw_obj(sync_env->svc->zone->get_zone_params().log_pool, marker_oid),
                                          attrs, &objv_tracker);
  }

  RGWOrderCallCR *allocate_order_control_cr() override {
    return new RGWLastCallerWinsCR(sync_env->cct);
  }
};

// write the incremental sync status and update 'stable_timestamp' on success
class RGWWriteBucketShardIncSyncStatus : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  rgw_raw_obj obj;
  rgw_bucket_shard_inc_sync_marker sync_marker;
  ceph::real_time* stable_timestamp;
  RGWObjVersionTracker& objv_tracker;
  std::map<std::string, bufferlist> attrs;
 public:
  RGWWriteBucketShardIncSyncStatus(RGWDataSyncEnv *sync_env,
                                   const rgw_raw_obj& obj,
                                   const rgw_bucket_shard_inc_sync_marker& sync_marker,
                                   ceph::real_time* stable_timestamp,
                                   RGWObjVersionTracker& objv_tracker)
    : RGWCoroutine(sync_env->cct), sync_env(sync_env), obj(obj),
      sync_marker(sync_marker), stable_timestamp(stable_timestamp),
      objv_tracker(objv_tracker)
  {}
  int operate() {
    reenter(this) {
      sync_marker.encode_attr(attrs);

      yield call(new RGWSimpleRadosWriteAttrsCR(sync_env->async_rados, sync_env->svc->sysobj,
                                                obj, attrs, &objv_tracker));
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      if (stable_timestamp) {
        *stable_timestamp = sync_marker.timestamp;
      }
      return set_cr_done();
    }
    return 0;
  }
};

class RGWBucketIncSyncShardMarkerTrack : public RGWSyncShardMarkerTrack<string, rgw_obj_key> {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;

  rgw_raw_obj obj;
  rgw_bucket_shard_inc_sync_marker sync_marker;

  map<rgw_obj_key, string> key_to_marker;

  struct operation {
    rgw_obj_key key;
    bool is_olh;
  };
  map<string, operation> marker_to_op;
  std::set<std::string> pending_olh; // object names with pending olh operations

  RGWSyncTraceNodeRef tn;
  RGWObjVersionTracker& objv_tracker;
  ceph::real_time* stable_timestamp;

  void handle_finish(const string& marker) override {
    auto iter = marker_to_op.find(marker);
    if (iter == marker_to_op.end()) {
      return;
    }
    auto& op = iter->second;
    key_to_marker.erase(op.key);
    reset_need_retry(op.key);
    if (op.is_olh) {
      pending_olh.erase(op.key.name);
    }
    marker_to_op.erase(iter);
  }

public:
  RGWBucketIncSyncShardMarkerTrack(RGWDataSyncCtx *_sc,
                         const string& _marker_oid,
                         const rgw_bucket_shard_inc_sync_marker& _marker,
                         RGWSyncTraceNodeRef tn,
                         RGWObjVersionTracker& objv_tracker,
                         ceph::real_time* stable_timestamp)
    : RGWSyncShardMarkerTrack(BUCKET_SYNC_UPDATE_MARKER_WINDOW),
      sc(_sc), sync_env(_sc->env),
      obj(sync_env->svc->zone->get_zone_params().log_pool, _marker_oid),
      sync_marker(_marker), tn(std::move(tn)), objv_tracker(objv_tracker),
      stable_timestamp(stable_timestamp)
  {}

  RGWCoroutine *store_marker(const string& new_marker, uint64_t index_pos, const real_time& timestamp) override {
    sync_marker.position = new_marker;
    sync_marker.timestamp = timestamp;

    tn->log(20, SSTR("updating marker marker_oid=" << obj.oid << " marker=" << new_marker << " timestamp=" << timestamp));
    return new RGWWriteBucketShardIncSyncStatus(sync_env, obj, sync_marker,
                                                stable_timestamp, objv_tracker);
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
  bool index_key_to_marker(const rgw_obj_key& key, const string& marker, bool is_olh) {
    auto result = key_to_marker.emplace(key, marker);
    if (!result.second) { // exists
      set_need_retry(key);
      return false;
    }
    marker_to_op[marker] = operation{key, is_olh};
    if (is_olh) {
      // prevent other olh ops from starting on this object name
      pending_olh.insert(key.name);
    }
    return true;
  }

  bool can_do_op(const rgw_obj_key& key, bool is_olh) {
    // serialize olh ops on the same object name
    if (is_olh && pending_olh.count(key.name)) {
      tn->log(20, SSTR("sync of " << key << " waiting for pending olh op"));
      return false;
    }
    return (key_to_marker.find(key) == key_to_marker.end());
  }

  RGWOrderCallCR *allocate_order_control_cr() override {
    return new RGWLastCallerWinsCR(sync_env->cct);
  }
};

static bool ignore_sync_error(int err) {
  switch (err) {
    case -ENOENT:
    case -EPERM:
      return true;
    default:
      break;
  }
  return false;
}

template <class T, class K>
class RGWBucketSyncSingleEntryCR : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;

  rgw_bucket_sync_pipe& sync_pipe;
  rgw_bucket_shard& bs;

  rgw_obj_key key;
  bool versioned;
  std::optional<uint64_t> versioned_epoch;
  rgw_bucket_entry_owner owner;
  real_time timestamp;
  RGWModifyOp op;
  RGWPendingState op_state;

  T entry_marker;
  RGWSyncShardMarkerTrack<T, K> *marker_tracker;

  int sync_status;

  stringstream error_ss;

  bool error_injection;

  RGWDataSyncModule *data_sync_module;
  
  rgw_zone_set zones_trace;

  RGWSyncTraceNodeRef tn;
public:
  RGWBucketSyncSingleEntryCR(RGWDataSyncCtx *_sc,
                             rgw_bucket_sync_pipe& _sync_pipe,
                             const rgw_obj_key& _key, bool _versioned,
                             std::optional<uint64_t> _versioned_epoch,
                             real_time& _timestamp,
                             const rgw_bucket_entry_owner& _owner,
                             RGWModifyOp _op, RGWPendingState _op_state,
		             const T& _entry_marker, RGWSyncShardMarkerTrack<T, K> *_marker_tracker, rgw_zone_set& _zones_trace,
                             RGWSyncTraceNodeRef& _tn_parent) : RGWCoroutine(_sc->cct),
						      sc(_sc), sync_env(_sc->env),
                                                      sync_pipe(_sync_pipe), bs(_sync_pipe.info.source_bs),
                                                      key(_key), versioned(_versioned), versioned_epoch(_versioned_epoch),
                                                      owner(_owner),
                                                      timestamp(_timestamp), op(_op),
                                                      op_state(_op_state),
                                                      entry_marker(_entry_marker),
                                                      marker_tracker(_marker_tracker),
                                                      sync_status(0){
    stringstream ss;
    ss << bucket_shard_str{bs} << "/" << key << "[" << versioned_epoch.value_or(0) << "]";
    set_description() << "bucket sync single entry (source_zone=" << sc->source_zone << ") b=" << ss.str() << " log_entry=" << entry_marker << " op=" << (int)op << " op_state=" << (int)op_state;
    set_status("init");

    tn = sync_env->sync_tracer->add_node(_tn_parent, "entry", SSTR(key));

    tn->log(20, SSTR("bucket sync single entry (source_zone=" << sc->source_zone << ") b=" << ss.str() << " log_entry=" << entry_marker << " op=" << (int)op << " op_state=" << (int)op_state));
    error_injection = (sync_env->cct->_conf->rgw_sync_data_inject_err_probability > 0);

    data_sync_module = sync_env->sync_module->get_data_handler();
    
    zones_trace = _zones_trace;
    zones_trace.insert(sync_env->svc->zone->get_zone().id, _sync_pipe.info.dest_bs.get_key());
  }

  int operate() override {
    reenter(this) {
      /* skip entries that are not complete */
      if (op_state != CLS_RGW_STATE_COMPLETE) {
        goto done;
      }
      tn->set_flag(RGW_SNS_FLAG_ACTIVE);
      do {
        yield {
          marker_tracker->reset_need_retry(key);
          if (key.name.empty()) {
            /* shouldn't happen */
            set_status("skipping empty entry");
            tn->log(0, "entry with empty obj name, skipping");
            goto done;
          }
          if (error_injection &&
              rand() % 10000 < cct->_conf->rgw_sync_data_inject_err_probability * 10000.0) {
            tn->log(0, SSTR(": injecting data sync error on key=" << key.name));
            retcode = -EIO;
          } else if (op == CLS_RGW_OP_ADD ||
                     op == CLS_RGW_OP_LINK_OLH) {
            set_status("syncing obj");
            tn->log(5, SSTR("bucket sync: sync obj: " << sc->source_zone << "/" << bs.bucket << "/" << key << "[" << versioned_epoch.value_or(0) << "]"));
            call(data_sync_module->sync_object(sc, sync_pipe, key, versioned_epoch, &zones_trace));
          } else if (op == CLS_RGW_OP_DEL || op == CLS_RGW_OP_UNLINK_INSTANCE) {
            set_status("removing obj");
            if (op == CLS_RGW_OP_UNLINK_INSTANCE) {
              versioned = true;
            }
            tn->log(10, SSTR("removing obj: " << sc->source_zone << "/" << bs.bucket << "/" << key << "[" << versioned_epoch.value_or(0) << "]"));
            call(data_sync_module->remove_object(sc, sync_pipe, key, timestamp, versioned, versioned_epoch.value_or(0), &zones_trace));
            // our copy of the object is more recent, continue as if it succeeded
          } else if (op == CLS_RGW_OP_LINK_OLH_DM) {
            set_status("creating delete marker");
            tn->log(10, SSTR("creating delete marker: obj: " << sc->source_zone << "/" << bs.bucket << "/" << key << "[" << versioned_epoch.value_or(0) << "]"));
            call(data_sync_module->create_delete_marker(sc, sync_pipe, key, timestamp, owner, versioned, versioned_epoch.value_or(0), &zones_trace));
          }
          tn->set_resource_name(SSTR(bucket_str_noinstance(bs.bucket) << "/" << key));
        }
        if (retcode == -ERR_PRECONDITION_FAILED) {
          set_status("Skipping object sync: precondition failed (object contains newer change or policy doesn't allow sync)");
          tn->log(0, "Skipping object sync: precondition failed (object contains newer change or policy doesn't allow sync)");
          retcode = 0;
        }
      } while (marker_tracker->need_retry(key));
      {
        tn->unset_flag(RGW_SNS_FLAG_ACTIVE);
        if (retcode >= 0) {
          tn->log(10, "success");
        } else {
          tn->log(10, SSTR("failed, retcode=" << retcode << " (" << cpp_strerror(-retcode) << ")"));
        }
      }

      if (retcode < 0 && retcode != -ENOENT) {
        set_status() << "failed to sync obj; retcode=" << retcode;
        tn->log(0, SSTR("ERROR: failed to sync object: "
            << bucket_shard_str{bs} << "/" << key.name));
        if (!ignore_sync_error(retcode)) {
          error_ss << bucket_shard_str{bs} << "/" << key.name;
          sync_status = retcode;
        }
      }
      if (!error_ss.str().empty()) {
        yield call(sync_env->error_logger->log_error_cr(sc->conn->get_remote_id(), "data", error_ss.str(), -retcode, string("failed to sync object") + cpp_strerror(-sync_status)));
      }
done:
      if (sync_status == 0) {
        /* update marker */
        set_status() << "calling marker_tracker->finish(" << entry_marker << ")";
        yield call(marker_tracker->finish(entry_marker));
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
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  rgw_bucket_sync_pipe& sync_pipe;
  rgw_bucket_shard& bs;
  boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr;
  bucket_list_result list_result;
  list<bucket_list_entry>::iterator entries_iter;
  rgw_bucket_shard_sync_info& sync_info;
  rgw_obj_key list_marker;
  bucket_list_entry *entry{nullptr};

  int total_entries{0};

  int sync_status{0};

  const string& status_oid;

  rgw_zone_set zones_trace;

  RGWSyncTraceNodeRef tn;
  RGWBucketFullSyncShardMarkerTrack marker_tracker;

  struct _prefix_handler {
    RGWBucketSyncFlowManager::pipe_rules_ref rules;
    RGWBucketSyncFlowManager::pipe_rules::prefix_map_t::const_iterator iter;
    std::optional<string> cur_prefix;

    void set_rules(RGWBucketSyncFlowManager::pipe_rules_ref& _rules) {
      rules = _rules;
    }

    bool revalidate_marker(rgw_obj_key *marker) {
      if (cur_prefix &&
          boost::starts_with(marker->name, *cur_prefix)) {
        return true;
      }
      if (!rules) {
        return false;
      }
      iter = rules->prefix_search(marker->name);
      if (iter == rules->prefix_end()) {
        return false;
      }
      cur_prefix = iter->first;
      marker->name = *cur_prefix;
      marker->instance.clear();
      return true;
    }

    bool check_key_handled(const rgw_obj_key& key) {
      if (!rules) {
        return false;
      }
      if (cur_prefix &&
          boost::starts_with(key.name, *cur_prefix)) {
        return true;
      }
      iter = rules->prefix_search(key.name);
      if (iter == rules->prefix_end()) {
        return false;
      }
      cur_prefix = iter->first;
      return boost::starts_with(key.name, iter->first);
    }
  } prefix_handler;

public:
  RGWBucketShardFullSyncCR(RGWDataSyncCtx *_sc,
                           rgw_bucket_sync_pipe& _sync_pipe,
                           const std::string& status_oid,
                           boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr,
                           rgw_bucket_shard_sync_info& sync_info,
                           RGWSyncTraceNodeRef tn_parent,
                           RGWObjVersionTracker& objv_tracker)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
      sync_pipe(_sync_pipe), bs(_sync_pipe.info.source_bs),
      lease_cr(std::move(lease_cr)), sync_info(sync_info),
      status_oid(status_oid),
      tn(sync_env->sync_tracer->add_node(tn_parent, "full_sync",
                                         SSTR(bucket_shard_str{bs}))),
      marker_tracker(sc, status_oid, sync_info.full_marker, tn, objv_tracker)
  {
    zones_trace.insert(sc->source_zone.id, sync_pipe.info.dest_bs.bucket.get_key());
    prefix_handler.set_rules(sync_pipe.get_rules());
  }

  int operate() override;
};

int RGWBucketShardFullSyncCR::operate()
{
  int ret;
  reenter(this) {
    list_marker = sync_info.full_marker.position;

    total_entries = sync_info.full_marker.count;
    do {
      if (lease_cr && !lease_cr->is_locked()) {
        drain_all();
        return set_cr_error(-ECANCELED);
      }
      set_status("listing remote bucket");
      tn->log(20, "listing bucket for full sync");

      if (!prefix_handler.revalidate_marker(&list_marker)) {
        set_status() << "finished iterating over all available prefixes: last marker=" << list_marker;
        tn->log(20, SSTR("finished iterating over all available prefixes: last marker=" << list_marker));
        break;
      }

      yield call(new RGWListBucketShardCR(sc, bs, list_marker,
                                          &list_result));
      if (retcode < 0 && retcode != -ENOENT) {
        set_status("failed bucket listing, going down");
        drain_all();
        return set_cr_error(retcode);
      }
      if (list_result.entries.size() > 0) {
        tn->set_flag(RGW_SNS_FLAG_ACTIVE); /* actually have entries to sync */
      }
      entries_iter = list_result.entries.begin();
      for (; entries_iter != list_result.entries.end(); ++entries_iter) {
        if (lease_cr && !lease_cr->is_locked()) {
          drain_all();
          return set_cr_error(-ECANCELED);
        }
        tn->log(20, SSTR("[full sync] syncing object: "
            << bucket_shard_str{bs} << "/" << entries_iter->key));
        entry = &(*entries_iter);
        list_marker = entries_iter->key;
        if (!prefix_handler.check_key_handled(entries_iter->key)) {
          set_status() << "skipping entry due to policy rules: " << entries_iter->key;
          tn->log(20, SSTR("skipping entry due to policy rules: " << entries_iter->key));
          continue;
        }
        total_entries++;
        if (!marker_tracker.start(entry->key, total_entries, real_time())) {
          tn->log(0, SSTR("ERROR: cannot start syncing " << entry->key << ". Duplicate entry?"));
        } else {
          using SyncCR = RGWBucketSyncSingleEntryCR<rgw_obj_key, rgw_obj_key>;
          yield spawn(new SyncCR(sc, sync_pipe, entry->key,
                                 false, /* versioned, only matters for object removal */
                                 entry->versioned_epoch, entry->mtime,
                                 entry->owner, entry->get_modify_op(), CLS_RGW_STATE_COMPLETE,
                                 entry->key, &marker_tracker, zones_trace, tn),
                      false);
        }
        while (num_spawned() > BUCKET_SYNC_SPAWN_WINDOW) {
          yield wait_for_child();
          bool again = true;
          while (again) {
            again = collect(&ret, nullptr);
            if (ret < 0) {
              tn->log(10, "a sync operation returned error");
              sync_status = ret;
              /* we have reported this error */
            }
          }
        }
      }
    } while (list_result.is_truncated && sync_status == 0);
    set_status("done iterating over all objects");
    /* wait for all operations to complete */
    while (num_spawned()) {
      yield wait_for_child();
      bool again = true;
      while (again) {
        again = collect(&ret, nullptr);
        if (ret < 0) {
          tn->log(10, "a sync operation returned error");
          sync_status = ret;
          /* we have reported this error */
        }
      }
    }
    tn->unset_flag(RGW_SNS_FLAG_ACTIVE);
    if (lease_cr && !lease_cr->is_locked()) {
      return set_cr_error(-ECANCELED);
    }
    /* update sync state to incremental */
    if (sync_status == 0) {
      yield {
        sync_info.state = rgw_bucket_shard_sync_info::StateIncrementalSync;
        map<string, bufferlist> attrs;
        sync_info.encode_state_attr(attrs);
        call(new RGWSimpleRadosWriteAttrsCR(sync_env->async_rados, sync_env->svc->sysobj,
                                            rgw_raw_obj(sync_env->svc->zone->get_zone_params().log_pool, status_oid),
                                            attrs));
      }
    } else {
      tn->log(10, SSTR("backing out with sync_status=" << sync_status));
    }
    if (retcode < 0 && sync_status == 0) { /* actually tried to set incremental state and failed */
      tn->log(0, SSTR("ERROR: failed to set sync state on bucket "
          << bucket_shard_str{bs} << " retcode=" << retcode));
      return set_cr_error(retcode);
    }
    if (sync_status < 0) {
      return set_cr_error(sync_status);
    }
    return set_cr_done();
  }
  return 0;
}

static bool has_olh_epoch(RGWModifyOp op) {
  return op == CLS_RGW_OP_LINK_OLH || op == CLS_RGW_OP_UNLINK_INSTANCE;
}

class RGWBucketShardIncrementalSyncCR : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  rgw_bucket_sync_pipe& sync_pipe;
  RGWBucketSyncFlowManager::pipe_rules_ref rules;
  rgw_bucket_shard& bs;
  boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr;
  list<rgw_bi_log_entry> list_result;
  list<rgw_bi_log_entry>::iterator entries_iter, entries_end;
  map<pair<string, string>, pair<real_time, RGWModifyOp> > squash_map;
  rgw_bucket_shard_sync_info& sync_info;
  rgw_obj_key key;
  rgw_bi_log_entry *entry{nullptr};
  bool updated_status{false};
  rgw_zone_id zone_id;
  string target_location_key;

  string cur_id;

  int sync_status{0};
  bool syncstopped{false};

  RGWSyncTraceNodeRef tn;
  RGWBucketIncSyncShardMarkerTrack marker_tracker;

public:
  RGWBucketShardIncrementalSyncCR(RGWDataSyncCtx *_sc,
                                  rgw_bucket_sync_pipe& _sync_pipe,
                                  const std::string& status_oid,
                                  boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr,
                                  rgw_bucket_shard_sync_info& sync_info,
                                  RGWSyncTraceNodeRef& _tn_parent,
                                  RGWObjVersionTracker& objv_tracker,
                                  ceph::real_time* stable_timestamp)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
      sync_pipe(_sync_pipe), bs(_sync_pipe.info.source_bs),
      lease_cr(std::move(lease_cr)), sync_info(sync_info),
      zone_id(sync_env->svc->zone->get_zone().id),
      tn(sync_env->sync_tracer->add_node(_tn_parent, "inc_sync",
                                         SSTR(bucket_shard_str{bs}))),
      marker_tracker(sc, status_oid, sync_info.inc_marker, tn,
                     objv_tracker, stable_timestamp)
  {
    set_description() << "bucket shard incremental sync bucket="
        << bucket_shard_str{bs};
    set_status("init");
    rules = sync_pipe.get_rules();
    target_location_key = sync_pipe.info.dest_bs.bucket.get_key();
  }

  bool check_key_handled(const rgw_obj_key& key) {
    if (!rules) {
      return false;
    }
    auto iter = rules->prefix_search(key.name);
    if (iter == rules->prefix_end()) {
      return false;
    }
    return boost::starts_with(key.name, iter->first);
  }

  int operate() override;
};

int RGWBucketShardIncrementalSyncCR::operate()
{
  int ret;
  reenter(this) {
    do {
      if (lease_cr && !lease_cr->is_locked()) {
        drain_all();
        tn->log(0, "ERROR: lease is not taken, abort");
        return set_cr_error(-ECANCELED);
      }
      tn->log(20, SSTR("listing bilog for incremental sync" << sync_info.inc_marker.position));
      set_status() << "listing bilog; position=" << sync_info.inc_marker.position;
      yield call(new RGWListBucketIndexLogCR(sc, bs, sync_info.inc_marker.position,
                                             &list_result));
      if (retcode < 0 && retcode != -ENOENT) {
        /* wait for all operations to complete */
        drain_all();
        return set_cr_error(retcode);
      }
      squash_map.clear();
      entries_iter = list_result.begin();
      entries_end = list_result.end();
      for (; entries_iter != entries_end; ++entries_iter) {
        auto e = *entries_iter;
        if (e.op == RGWModifyOp::CLS_RGW_OP_SYNCSTOP) {
          ldout(sync_env->cct, 20) << "syncstop on " << e.timestamp << dendl;
          syncstopped = true;
          entries_end = std::next(entries_iter); // stop after this entry
          break;
        }
        if (e.op == RGWModifyOp::CLS_RGW_OP_RESYNC) {
          continue;
        }
        if (e.op == CLS_RGW_OP_CANCEL) {
          continue;
        }
        if (e.state != CLS_RGW_STATE_COMPLETE) {
          continue;
        }
        if (e.zones_trace.exists(zone_id.id, target_location_key)) {
          continue;
        }
        auto& squash_entry = squash_map[make_pair(e.object, e.instance)];
        // don't squash over olh entries - we need to apply their olh_epoch
        if (has_olh_epoch(squash_entry.second) && !has_olh_epoch(e.op)) {
          continue;
        }
        if (squash_entry.first <= e.timestamp) {
          squash_entry = make_pair<>(e.timestamp, e.op);
        }
      }

      entries_iter = list_result.begin();
      for (; entries_iter != entries_end; ++entries_iter) {
        if (lease_cr && !lease_cr->is_locked()) {
          drain_all();
          return set_cr_error(-ECANCELED);
        }
        entry = &(*entries_iter);
        {
          ssize_t p = entry->id.find('#'); /* entries might have explicit shard info in them, e.g., 6#00000000004.94.3 */
          if (p < 0) {
            cur_id = entry->id;
          } else {
            cur_id = entry->id.substr(p + 1);
          }
        }
        sync_info.inc_marker.position = cur_id;

        if (entry->op == RGWModifyOp::CLS_RGW_OP_SYNCSTOP || entry->op == RGWModifyOp::CLS_RGW_OP_RESYNC) {
          ldout(sync_env->cct, 20) << "detected syncstop or resync on " << entries_iter->timestamp << ", skipping entry" << dendl;
          marker_tracker.try_update_high_marker(cur_id, 0, entry->timestamp);
          continue;
        }

        if (!key.set(rgw_obj_index_key{entry->object, entry->instance})) {
          set_status() << "parse_raw_oid() on " << entry->object << " returned false, skipping entry";
          tn->log(20, SSTR("parse_raw_oid() on " << entry->object << " returned false, skipping entry"));
          marker_tracker.try_update_high_marker(cur_id, 0, entry->timestamp);
          continue;
        }

        tn->log(20, SSTR("parsed entry: id=" << cur_id << " iter->object=" << entry->object << " iter->instance=" << entry->instance << " name=" << key.name << " instance=" << key.instance << " ns=" << key.ns));

        if (!key.ns.empty()) {
          set_status() << "skipping entry in namespace: " << entry->object;
          tn->log(20, SSTR("skipping entry in namespace: " << entry->object));
          marker_tracker.try_update_high_marker(cur_id, 0, entry->timestamp);
          continue;
        }

        if (!check_key_handled(key)) {
          set_status() << "skipping entry due to policy rules: " << entry->object;
          tn->log(20, SSTR("skipping entry due to policy rules: " << entry->object));
          marker_tracker.try_update_high_marker(cur_id, 0, entry->timestamp);
          continue;
        }

        set_status() << "got entry.id=" << cur_id << " key=" << key << " op=" << (int)entry->op;
        if (entry->op == CLS_RGW_OP_CANCEL) {
          set_status() << "canceled operation, skipping";
          tn->log(20, SSTR("skipping object: "
              << bucket_shard_str{bs} << "/" << key << ": canceled operation"));
          marker_tracker.try_update_high_marker(cur_id, 0, entry->timestamp);
          continue;
        }
        if (entry->state != CLS_RGW_STATE_COMPLETE) {
          set_status() << "non-complete operation, skipping";
          tn->log(20, SSTR("skipping object: "
              << bucket_shard_str{bs} << "/" << key << ": non-complete operation"));
          marker_tracker.try_update_high_marker(cur_id, 0, entry->timestamp);
          continue;
        }
        if (entry->zones_trace.exists(zone_id.id, target_location_key)) {
          set_status() << "redundant operation, skipping";
          tn->log(20, SSTR("skipping object: "
              <<bucket_shard_str{bs} <<"/"<<key<<": redundant operation"));
          marker_tracker.try_update_high_marker(cur_id, 0, entry->timestamp);
          continue;
        }
        if (make_pair<>(entry->timestamp, entry->op) != squash_map[make_pair(entry->object, entry->instance)]) {
          set_status() << "squashed operation, skipping";
          tn->log(20, SSTR("skipping object: "
              << bucket_shard_str{bs} << "/" << key << ": squashed operation"));
          marker_tracker.try_update_high_marker(cur_id, 0, entry->timestamp);
          continue;
        }
        tn->set_flag(RGW_SNS_FLAG_ACTIVE);
        tn->log(20, SSTR("syncing object: "
            << bucket_shard_str{bs} << "/" << key));
        updated_status = false;
        while (!marker_tracker.can_do_op(key, has_olh_epoch(entry->op))) {
          if (!updated_status) {
            set_status() << "can't do op, conflicting inflight operation";
            updated_status = true;
          }
          tn->log(5, SSTR("can't do op on key=" << key << " need to wait for conflicting operation to complete"));
          yield wait_for_child();
          bool again = true;
          while (again) {
            again = collect(&ret, nullptr);
            if (ret < 0) {
              tn->log(0, SSTR("ERROR: a child operation returned error (ret=" << ret << ")"));
              sync_status = ret;
              /* we have reported this error */
            }
          }
          if (sync_status != 0)
            break;
        }
        if (sync_status != 0) {
          /* get error, stop */
          break;
        }
        if (!marker_tracker.index_key_to_marker(key, cur_id, has_olh_epoch(entry->op))) {
          set_status() << "can't do op, sync already in progress for object";
          tn->log(20, SSTR("skipping sync of entry: " << cur_id << ":" << key << " sync already in progress for object"));
          marker_tracker.try_update_high_marker(cur_id, 0, entry->timestamp);
          continue;
        }
        // yield {
          set_status() << "start object sync";
          if (!marker_tracker.start(cur_id, 0, entry->timestamp)) {
            tn->log(0, SSTR("ERROR: cannot start syncing " << cur_id << ". Duplicate entry?"));
          } else {
            std::optional<uint64_t> versioned_epoch;
            rgw_bucket_entry_owner owner(entry->owner, entry->owner_display_name);
            if (entry->ver.pool < 0) {
              versioned_epoch = entry->ver.epoch;
            }
            tn->log(20, SSTR("entry->timestamp=" << entry->timestamp));
            using SyncCR = RGWBucketSyncSingleEntryCR<string, rgw_obj_key>;
            spawn(new SyncCR(sc, sync_pipe, key,
                             entry->is_versioned(), versioned_epoch,
                             entry->timestamp, owner, entry->op, entry->state,
                             cur_id, &marker_tracker, entry->zones_trace, tn),
                  false);
          }
        // }
        while (num_spawned() > BUCKET_SYNC_SPAWN_WINDOW) {
          set_status() << "num_spawned() > spawn_window";
          yield wait_for_child();
          bool again = true;
          while (again) {
            again = collect(&ret, nullptr);
            if (ret < 0) {
              tn->log(10, "a sync operation returned error");
              sync_status = ret;
              /* we have reported this error */
            }
            /* not waiting for child here */
          }
        }
      }
    } while (!list_result.empty() && sync_status == 0 && !syncstopped);

    while (num_spawned()) {
      yield wait_for_child();
      bool again = true;
      while (again) {
        again = collect(&ret, nullptr);
        if (ret < 0) {
          tn->log(10, "a sync operation returned error");
          sync_status = ret;
          /* we have reported this error */
        }
        /* not waiting for child here */
      }
    }
    tn->unset_flag(RGW_SNS_FLAG_ACTIVE);

    if (syncstopped) {
      // transition to StateStopped in RGWRunBucketSyncCoroutine. if sync is
      // still disabled, we'll delete the sync status object. otherwise we'll
      // restart full sync to catch any changes that happened while sync was
      // disabled
      sync_info.state = rgw_bucket_shard_sync_info::StateStopped;
      return set_cr_done();
    }

    yield call(marker_tracker.flush());
    if (retcode < 0) {
      tn->log(0, SSTR("ERROR: marker_tracker.flush() returned retcode=" << retcode));
      return set_cr_error(retcode);
    }
    if (sync_status < 0) {
      tn->log(10, SSTR("backing out with sync_status=" << sync_status));
      return set_cr_error(sync_status);
    }
    return set_cr_done();
  }
  return 0;
}

class RGWGetBucketPeersCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;

  std::optional<rgw_bucket> target_bucket;
  std::optional<rgw_zone_id> source_zone;
  std::optional<rgw_bucket> source_bucket;

  rgw_sync_pipe_info_set *pipes;
  map<rgw_bucket, all_bucket_info> buckets_info;
  map<rgw_bucket, all_bucket_info>::iterator siiter;
  std::optional<all_bucket_info> target_bucket_info;
  std::optional<all_bucket_info> source_bucket_info;

  rgw_sync_pipe_info_set::iterator siter;

  std::shared_ptr<rgw_bucket_get_sync_policy_result> source_policy;
  std::shared_ptr<rgw_bucket_get_sync_policy_result> target_policy;

  RGWSyncTraceNodeRef tn;

  using pipe_const_iter = map<rgw_zone_id, RGWBucketSyncFlowManager::pipe_set>::const_iterator;

  static pair<pipe_const_iter, pipe_const_iter> get_pipe_iters(const map<rgw_zone_id, RGWBucketSyncFlowManager::pipe_set>& m, std::optional<rgw_zone_id> zone) {
    if (!zone) {
      return { m.begin(), m.end() };
    }

    auto b = m.find(*zone);
    if (b == m.end()) {
      return { b, b };
    }
    return { b, std::next(b) };
  }

  void filter_sources(std::optional<rgw_zone_id> source_zone,
                      std::optional<rgw_bucket> source_bucket,
                      const map<rgw_zone_id, RGWBucketSyncFlowManager::pipe_set>& all_sources,
                      rgw_sync_pipe_info_set *result) {
    ldpp_dout(sync_env->dpp, 20) << __func__ << ": source_zone=" << source_zone.value_or(rgw_zone_id("*")).id
                                << " source_bucket=" << source_bucket.value_or(rgw_bucket())
                                << " all_sources.size()=" << all_sources.size() << dendl;
    auto iters = get_pipe_iters(all_sources, source_zone);
    for (auto i = iters.first; i != iters.second; ++i) {
      for (auto& handler : i->second) {
        if (!handler.specific()) {
          ldpp_dout(sync_env->dpp, 20) << __func__ << ": pipe_handler=" << handler << ": skipping" << dendl;
          continue;
        }
        if (source_bucket &&
            !source_bucket->match(*handler.source.bucket)) {
          continue;
        }
        ldpp_dout(sync_env->dpp, 20) << __func__ << ": pipe_handler=" << handler << ": adding" << dendl;
        result->insert(handler, source_bucket_info, target_bucket_info);
      }
    }
  }

  void filter_targets(std::optional<rgw_zone_id> target_zone,
                      std::optional<rgw_bucket> target_bucket,
                      const map<rgw_zone_id, RGWBucketSyncFlowManager::pipe_set>& all_targets,
                      rgw_sync_pipe_info_set *result) {
    ldpp_dout(sync_env->dpp, 20) << __func__ << ": target_zone=" << source_zone.value_or(rgw_zone_id("*")).id
                                << " target_bucket=" << source_bucket.value_or(rgw_bucket())
                                << " all_targets.size()=" << all_targets.size() << dendl;
    auto iters = get_pipe_iters(all_targets, target_zone);
    for (auto i = iters.first; i != iters.second; ++i) {
      for (auto& handler : i->second) {
        if (target_bucket &&
            handler.dest.bucket &&
            !target_bucket->match(*handler.dest.bucket)) {
          ldpp_dout(sync_env->dpp, 20) << __func__ << ": pipe_handler=" << handler << ": skipping" << dendl;
          continue;
        }
        ldpp_dout(sync_env->dpp, 20) << __func__ << ": pipe_handler=" << handler << ": adding" << dendl;
        result->insert(handler, source_bucket_info, target_bucket_info);
      }
    }
  }

  void update_from_target_bucket_policy();
  void update_from_source_bucket_policy();

  struct GetHintTargets : public RGWGenericAsyncCR::Action {
    RGWDataSyncEnv *sync_env;
    rgw_bucket source_bucket;
    std::set<rgw_bucket> targets;
    
    GetHintTargets(RGWDataSyncEnv *_sync_env,
                   const rgw_bucket& _source_bucket) : sync_env(_sync_env),
                                                       source_bucket(_source_bucket) {}
    int operate() override {
      int r = sync_env->svc->bucket_sync->get_bucket_sync_hints(source_bucket,
                                                                nullptr,
                                                                &targets,
                                                                null_yield);
      if (r < 0) {
        ldout(sync_env->cct, 0) << "ERROR: " << __func__ << "(): failed to fetch bucket sync hints for bucket=" << source_bucket << dendl;
        return r;
      }

      return 0;
    }
  };

  std::shared_ptr<GetHintTargets> get_hint_targets_action;
  std::set<rgw_bucket>::iterator hiter;

public:
  RGWGetBucketPeersCR(RGWDataSyncEnv *_sync_env,
                      std::optional<rgw_bucket> _target_bucket,
                      std::optional<rgw_zone_id> _source_zone,
                      std::optional<rgw_bucket> _source_bucket,
                      rgw_sync_pipe_info_set *_pipes,
                      const RGWSyncTraceNodeRef& _tn_parent)
    : RGWCoroutine(_sync_env->cct),
      sync_env(_sync_env),
      target_bucket(_target_bucket),
      source_zone(_source_zone),
      source_bucket(_source_bucket),
      pipes(_pipes),
      tn(sync_env->sync_tracer->add_node(_tn_parent, "get_bucket_peers",
                                         SSTR( "target=" << target_bucket.value_or(rgw_bucket())
                                               << ":source=" << target_bucket.value_or(rgw_bucket())
                                               << ":source_zone=" << source_zone.value_or(rgw_zone_id("*")).id))) {
      }

  int operate() override;
};

std::ostream& operator<<(std::ostream& out, std::optional<rgw_bucket_shard>& bs) {
  if (!bs) {
    out << "*";
  } else {
    out << *bs;
  }
  return out;
}

RGWRunBucketSourcesSyncCR::RGWRunBucketSourcesSyncCR(RGWDataSyncCtx *_sc,
                                                     boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr,
                                                     std::optional<rgw_bucket_shard> _target_bs,
                                                     std::optional<rgw_bucket_shard> _source_bs,
                                                     const RGWSyncTraceNodeRef& _tn_parent,
                                                     ceph::real_time* progress)
  : RGWCoroutine(_sc->env->cct), sc(_sc), sync_env(_sc->env),
    lease_cr(std::move(lease_cr)), target_bs(_target_bs), source_bs(_source_bs),
    tn(sync_env->sync_tracer->add_node(_tn_parent, "bucket_sync_sources",
                                       SSTR( "target=" << target_bucket.value_or(rgw_bucket()) << ":source_bucket=" << source_bucket.value_or(rgw_bucket()) << ":source_zone=" << sc->source_zone))),
    progress(progress)
{
  if (target_bs) {
    target_bucket = target_bs->bucket;
  }
  if (source_bs) {
    source_bucket = source_bs->bucket;
  }
}

int RGWRunBucketSourcesSyncCR::operate()
{
  reenter(this) {
    yield call(new RGWGetBucketPeersCR(sync_env, target_bucket, sc->source_zone, source_bucket, &pipes, tn));
    if (retcode < 0 && retcode != -ENOENT) {
      tn->log(0, "ERROR: failed to read sync status for bucket");
      return set_cr_error(retcode);
    }

    ldpp_dout(sync_env->dpp, 20) << __func__ << "(): requested source_bs=" << source_bs << " target_bs=" << target_bs << dendl;

    if (pipes.empty()) {
      ldpp_dout(sync_env->dpp, 20) << __func__ << "(): no relevant sync pipes found" << dendl;
      return set_cr_done();
    }

    for (siter = pipes.begin(); siter != pipes.end(); ++siter) {
      {
        ldpp_dout(sync_env->dpp, 20) << __func__ << "(): sync pipe=" << *siter << dendl;

        source_num_shards = siter->source.get_bucket_info().layout.current_index.layout.normal.num_shards;
        target_num_shards = siter->target.get_bucket_info().layout.current_index.layout.normal.num_shards;
        if (source_bs) {
          sync_pair.source_bs = *source_bs;
        } else {
          sync_pair.source_bs.bucket = siter->source.get_bucket();
        }
        sync_pair.dest_bs.bucket = siter->target.get_bucket();

        sync_pair.handler = siter->handler;

        if (sync_pair.source_bs.shard_id >= 0) {
          num_shards = 1;
          cur_shard = sync_pair.source_bs.shard_id;
        } else {
          num_shards = std::max<int>(1, source_num_shards);
          cur_shard = std::min<int>(0, source_num_shards);
        }
      }

      ldpp_dout(sync_env->dpp, 20) << __func__ << "(): num shards=" << num_shards << " cur_shard=" << cur_shard << dendl;

      shard_progress.resize(num_shards);
      cur_shard_progress = shard_progress.begin();
      for (; num_shards > 0; --num_shards, ++cur_shard, ++cur_shard_progress) {
        /*
         * use a negatvie shard_id for backward compatibility,
         * this affects the crafted status oid
         */
        sync_pair.source_bs.shard_id = (source_num_shards > 0 ? cur_shard : -1);
        if (source_num_shards == target_num_shards) {
          sync_pair.dest_bs.shard_id = sync_pair.source_bs.shard_id;
        } else {
          sync_pair.dest_bs.shard_id = -1;
        }

        ldpp_dout(sync_env->dpp, 20) << __func__ << "(): sync_pair=" << sync_pair << dendl;

        yield spawn(new RGWRunBucketSyncCoroutine(sc, lease_cr, sync_pair, tn,
                                                  &*cur_shard_progress), false);
        while (num_spawned() > BUCKET_SYNC_SPAWN_WINDOW) {
          set_status() << "num_spawned() > spawn_window";
          yield wait_for_child();
          again = true;
          while (again) {
            again = collect(&ret, nullptr);
            if (ret < 0) {
              tn->log(10, "a sync operation returned error");
              drain_all();
              return set_cr_error(ret);
            }
          }
        }
      }
    }
    while (num_spawned()) {
      set_status() << "draining";
      yield wait_for_child();
      again = true;
      while (again) {
        again = collect(&ret, nullptr);
        if (ret < 0) {
          tn->log(10, "a sync operation returned error");
          drain_all();
          return set_cr_error(ret);
        }
      }
    }
    if (progress) {
      *progress = *std::min_element(shard_progress.begin(), shard_progress.end());
    }
    return set_cr_done();
  }

  return 0;
}

class RGWSyncGetBucketInfoCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  rgw_bucket bucket;
  RGWBucketInfo *pbucket_info;
  map<string, bufferlist> *pattrs;
  RGWMetaSyncEnv meta_sync_env;

  RGWSyncTraceNodeRef tn;

public:
  RGWSyncGetBucketInfoCR(RGWDataSyncEnv *_sync_env,
                         const rgw_bucket& _bucket,
                         RGWBucketInfo *_pbucket_info,
                         map<string, bufferlist> *_pattrs,
                         const RGWSyncTraceNodeRef& _tn_parent)
    : RGWCoroutine(_sync_env->cct),
      sync_env(_sync_env),
      bucket(_bucket),
      pbucket_info(_pbucket_info),
      pattrs(_pattrs),
      tn(sync_env->sync_tracer->add_node(_tn_parent, "get_bucket_info",
                                         SSTR(bucket))) {
  }

  int operate() override;
};

int RGWSyncGetBucketInfoCR::operate()
{
  reenter(this) {
    yield call(new RGWGetBucketInstanceInfoCR(sync_env->async_rados, sync_env->store, bucket, pbucket_info, pattrs));
    if (retcode == -ENOENT) {
      /* bucket instance info has not been synced in yet, fetch it now */
      yield {
        tn->log(10, SSTR("no local info for bucket:" << ": fetching metadata"));
        string raw_key = string("bucket.instance:") + bucket.get_key();

        meta_sync_env.init(sync_env->dpp, cct, sync_env->store, sync_env->svc->zone->get_master_conn(), sync_env->async_rados,
                           sync_env->http_manager, sync_env->error_logger, sync_env->sync_tracer);

        call(new RGWMetaSyncSingleEntryCR(&meta_sync_env, raw_key,
                                          string() /* no marker */,
                                          MDLOG_STATUS_COMPLETE,
                                          NULL /* no marker tracker */,
                                          tn));
      }
      if (retcode < 0) {
        tn->log(0, SSTR("ERROR: failed to fetch bucket instance info for " << bucket_str{bucket}));
        return set_cr_error(retcode);
      }

      yield call(new RGWGetBucketInstanceInfoCR(sync_env->async_rados, sync_env->store, bucket, pbucket_info, pattrs));
    }
    if (retcode < 0) {
      tn->log(0, SSTR("ERROR: failed to retrieve bucket info for bucket=" << bucket_str{bucket}));
      return set_cr_error(retcode);
    }

    return set_cr_done();
  }

  return 0;
}

void RGWGetBucketPeersCR::update_from_target_bucket_policy()
{
  if (!target_policy ||
      !target_policy->policy_handler ||
      !pipes) {
    return;
  }

  auto handler = target_policy->policy_handler.get();

  filter_sources(source_zone,
                 source_bucket,
                 handler->get_sources(),
                 pipes);

  for (siter = pipes->begin(); siter != pipes->end(); ++siter) {
    if (!siter->source.has_bucket_info()) {
      buckets_info.emplace(siter->source.get_bucket(), all_bucket_info());
    }
    if (!siter->target.has_bucket_info()) {
      buckets_info.emplace(siter->target.get_bucket(), all_bucket_info());
    }
  }
}

void RGWGetBucketPeersCR::update_from_source_bucket_policy()
{
  if (!source_policy ||
      !source_policy->policy_handler ||
      !pipes) {
    return;
  }

  auto handler = source_policy->policy_handler.get();

  filter_targets(sync_env->svc->zone->get_zone().id,
                 target_bucket,
                 handler->get_targets(),
                 pipes);

  for (siter = pipes->begin(); siter != pipes->end(); ++siter) {
    if (!siter->source.has_bucket_info()) {
      buckets_info.emplace(siter->source.get_bucket(), all_bucket_info());
    }
    if (!siter->target.has_bucket_info()) {
      buckets_info.emplace(siter->target.get_bucket(), all_bucket_info());
    }
  }
}


class RGWSyncGetBucketSyncPolicyHandlerCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  rgw_bucket bucket;
  rgw_bucket_get_sync_policy_params get_policy_params;

  std::shared_ptr<rgw_bucket_get_sync_policy_result> policy;

  RGWSyncTraceNodeRef tn;

  int i;

public:
  RGWSyncGetBucketSyncPolicyHandlerCR(RGWDataSyncEnv *_sync_env,
                         std::optional<rgw_zone_id> zone,
                         const rgw_bucket& _bucket,
                         std::shared_ptr<rgw_bucket_get_sync_policy_result>& _policy,
                         const RGWSyncTraceNodeRef& _tn_parent)
    : RGWCoroutine(_sync_env->cct),
      sync_env(_sync_env),
      bucket(_bucket),
      policy(_policy),
      tn(sync_env->sync_tracer->add_node(_tn_parent, "get_sync_policy_handler",
                                         SSTR(bucket))) {
    get_policy_params.zone = zone;
    get_policy_params.bucket = bucket;
  }

  int operate() override {
    reenter(this) {
      for (i = 0; i < 2; ++i) {
        yield call(new RGWBucketGetSyncPolicyHandlerCR(sync_env->async_rados,
                                                       sync_env->store,
                                                       get_policy_params,
                                                       policy));
        if (retcode < 0 &&
            retcode != -ENOENT) {
          return set_cr_error(retcode);
        }

        if (retcode == 0) {
          return set_cr_done();
        }

        /* bucket instance was not found,
         * try to get bucket instance info, can trigger
         * metadata sync of bucket instance
         */
        yield call(new RGWSyncGetBucketInfoCR(sync_env, 
                                              bucket, 
                                              nullptr,
                                              nullptr,
                                              tn));
        if (retcode < 0) {
          return set_cr_error(retcode);
        }
      }
    }

    return 0;
  }
};


int RGWGetBucketPeersCR::operate()
{
  reenter(this) {
    if (pipes) {
      pipes->clear();
    }
    if (target_bucket) {
      target_policy = make_shared<rgw_bucket_get_sync_policy_result>();
      yield call(new RGWSyncGetBucketSyncPolicyHandlerCR(sync_env,
                                                         nullopt,
                                                         *target_bucket,
                                                         target_policy,
                                                         tn));
      if (retcode < 0 &&
          retcode != -ENOENT) {
        return set_cr_error(retcode);
      }

      update_from_target_bucket_policy();
    }

    if (source_bucket && source_zone) {
      source_policy = make_shared<rgw_bucket_get_sync_policy_result>();
      yield call(new RGWSyncGetBucketSyncPolicyHandlerCR(sync_env,
                                                         source_zone,
                                                         *source_bucket,
                                                         source_policy,
                                                         tn));
      if (retcode < 0 &&
          retcode != -ENOENT) {
        return set_cr_error(retcode);
      }

      if (source_policy->policy_handler) {
        auto& opt_bucket_info = source_policy->policy_handler->get_bucket_info();
        auto& opt_attrs = source_policy->policy_handler->get_bucket_attrs();
        if (opt_bucket_info && opt_attrs) {
          source_bucket_info.emplace();
          source_bucket_info->bucket_info = *opt_bucket_info;
          source_bucket_info->attrs = *opt_attrs;
        }
      }

      if (!target_bucket) {
        get_hint_targets_action = make_shared<GetHintTargets>(sync_env, *source_bucket);

        yield call(new RGWGenericAsyncCR(cct, sync_env->async_rados,
                                         get_hint_targets_action));
        if (retcode < 0) {
          return set_cr_error(retcode);
        }

        /* hints might have incomplete bucket ids,
         * in which case we need to figure out the current
         * bucket_id
         */
        for (hiter = get_hint_targets_action->targets.begin();
             hiter != get_hint_targets_action->targets.end();
             ++hiter) {
          ldpp_dout(sync_env->dpp, 20) << "Got sync hint for bucket=" << *source_bucket << ": " << hiter->get_key() << dendl;

          target_policy = make_shared<rgw_bucket_get_sync_policy_result>();
          yield call(new RGWSyncGetBucketSyncPolicyHandlerCR(sync_env,
                                                             nullopt,
                                                             *hiter,
                                                             target_policy,
                                                             tn));
          if (retcode < 0 &&
              retcode != -ENOENT) {
            return set_cr_error(retcode);
          }
          update_from_target_bucket_policy();
        }
      }
    }

    update_from_source_bucket_policy();

    for (siiter = buckets_info.begin(); siiter != buckets_info.end(); ++siiter) {
      if (siiter->second.bucket_info.bucket.name.empty()) {
        yield call(new RGWSyncGetBucketInfoCR(sync_env, siiter->first,
                                              &siiter->second.bucket_info,
                                              &siiter->second.attrs,
                                              tn));
      }
    }

    if (pipes) {
      pipes->update_empty_bucket_info(buckets_info);
    }

    return set_cr_done();
  }

  return 0;
}

int RGWRunBucketsSyncBySourceCR::operate()
{
  reenter(this) {
    return set_cr_done();
  }

  return 0;
}

int RGWRunBucketSyncCoroutine::operate()
{
  reenter(this) {
    yield call(new RGWReadBucketPipeSyncStatusCoroutine(sc, sync_pair, &sync_status, &objv_tracker));
    if (retcode < 0 && retcode != -ENOENT) {
      tn->log(0, "ERROR: failed to read sync status for bucket");
      drain_all();
      return set_cr_error(retcode);
    }

    tn->log(20, SSTR("sync status for source bucket: " << sync_status.state));

    yield call(new RGWSyncGetBucketInfoCR(sync_env, sync_pair.source_bs.bucket, &sync_pipe.source_bucket_info,
                                          &sync_pipe.source_bucket_attrs, tn));
    if (retcode < 0) {
      tn->log(0, SSTR("ERROR: failed to retrieve bucket info for bucket=" << bucket_str{sync_pair.source_bs.bucket}));
      drain_all();
      return set_cr_error(retcode);
    }

    yield call(new RGWSyncGetBucketInfoCR(sync_env, sync_pair.dest_bs.bucket, &sync_pipe.dest_bucket_info, 
                                          &sync_pipe.dest_bucket_attrs, tn));
    if (retcode < 0) {
      tn->log(0, SSTR("ERROR: failed to retrieve bucket info for bucket=" << bucket_str{sync_pair.source_bs.bucket}));
      drain_all();
      return set_cr_error(retcode);
    }

    sync_pipe.info = sync_pair;

    do {
      if (sync_status.state == rgw_bucket_shard_sync_info::StateInit ||
          sync_status.state == rgw_bucket_shard_sync_info::StateStopped) {
        yield call(new RGWInitBucketShardSyncStatusCoroutine(sc, sync_pair, sync_status, objv_tracker));
        if (retcode == -ENOENT) {
          tn->log(0, "bucket sync disabled");
          drain_all();
          return set_cr_done();
        }
        if (retcode < 0) {
          tn->log(0, SSTR("ERROR: init sync on bucket failed, retcode=" << retcode));
          drain_all();
          return set_cr_error(retcode);
        }
      }
      if (progress) {
        *progress = sync_status.inc_marker.timestamp;
      }

      if (sync_status.state == rgw_bucket_shard_sync_info::StateFullSync) {
        yield call(new RGWBucketShardFullSyncCR(sc, sync_pipe,
                                                status_oid, lease_cr,
                                                sync_status, tn, objv_tracker));
        if (retcode < 0) {
          tn->log(5, SSTR("full sync on bucket failed, retcode=" << retcode));
          drain_all();
          return set_cr_error(retcode);
        }
      }

      if (sync_status.state == rgw_bucket_shard_sync_info::StateIncrementalSync) {
        yield call(new RGWBucketShardIncrementalSyncCR(sc, sync_pipe,
                                                       status_oid, lease_cr,
                                                       sync_status, tn,
                                                       objv_tracker, progress));
        if (retcode < 0) {
          tn->log(5, SSTR("incremental sync on bucket failed, retcode=" << retcode));
          drain_all();
          return set_cr_error(retcode);
        }
      }
      // loop back to previous states unless incremental sync returns normally
    } while (sync_status.state != rgw_bucket_shard_sync_info::StateIncrementalSync);

    drain_all();
    return set_cr_done();
  }

  return 0;
}

RGWCoroutine *RGWRemoteBucketManager::run_sync_cr(int num)
{
  if ((size_t)num >= sync_pairs.size()) {
    return nullptr;
  }

  return new RGWRunBucketSyncCoroutine(&sc, nullptr, sync_pairs[num], sync_env->sync_tracer->root_node, nullptr);
}

int RGWBucketPipeSyncStatusManager::init()
{
  int ret = http_manager.start();
  if (ret < 0) {
    ldpp_dout(this, 0) << "failed in http_manager.start() ret=" << ret << dendl;
    return ret;
  }

  error_logger = new RGWSyncErrorLogger(store, RGW_SYNC_ERROR_LOG_SHARD_PREFIX, ERROR_LOGGER_SHARDS);

  sync_module.reset(new RGWDefaultSyncModuleInstance());
  auto async_rados = store->svc()->rados->get_async_processor();

  sync_env.init(this, store->ctx(), store,
                store->svc(), async_rados, &http_manager,
                error_logger, store->getRados()->get_sync_tracer(),
                sync_module, nullptr);

  rgw_sync_pipe_info_set pipes;

  ret = cr_mgr.run(new RGWGetBucketPeersCR(&sync_env,
                                           dest_bucket,
                                           source_zone,
                                           source_bucket,
                                           &pipes,
                                           sync_env.sync_tracer->root_node));
  if (ret < 0) {
    ldpp_dout(this, 0) << "failed to get bucket source peers info: (ret=" << ret << "): " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  rgw_zone_id last_zone;

  for (auto& pipe : pipes) {
    auto& szone = pipe.source.zone;

    if (last_zone != szone) {
      conn = store->svc()->zone->get_zone_conn(szone);
      if (!conn) {
        ldpp_dout(this, 0) << "connection object to zone " << szone << " does not exist" << dendl;
        return -EINVAL;
      }
      last_zone = szone;
    }

    source_mgrs.push_back(new RGWRemoteBucketManager(this, &sync_env,
                                                     szone, conn,
                                                     pipe.source.get_bucket_info(),
                                                     pipe.target.get_bucket()));
  }

  return 0;
}

int RGWBucketPipeSyncStatusManager::init_sync_status()
{
  list<RGWCoroutinesStack *> stacks;
  // pass an empty objv tracker to each so that the version gets incremented
  std::list<RGWObjVersionTracker> objvs;

  for (auto& mgr : source_mgrs) {
    RGWCoroutinesStack *stack = new RGWCoroutinesStack(store->ctx(), &cr_mgr);

    for (int i = 0; i < mgr->num_pipes(); ++i) {
      objvs.emplace_back();
      stack->call(mgr->init_sync_status_cr(i, objvs.back()));
    }

    stacks.push_back(stack);
  }

  return cr_mgr.run(stacks);
}

int RGWBucketPipeSyncStatusManager::read_sync_status()
{
  list<RGWCoroutinesStack *> stacks;

  for (auto& mgr : source_mgrs) {
    RGWCoroutinesStack *stack = new RGWCoroutinesStack(store->ctx(), &cr_mgr);
    for (int i = 0; i < mgr->num_pipes(); ++i) {
      stack->call(mgr->read_sync_status_cr(i, &sync_status[i]));
    }

    stacks.push_back(stack);
  }

  int ret = cr_mgr.run(stacks);
  if (ret < 0) {
    ldpp_dout(this, 0) << "ERROR: failed to read sync status for "
        << bucket_str{dest_bucket} << dendl;
    return ret;
  }

  return 0;
}

int RGWBucketPipeSyncStatusManager::run()
{
  list<RGWCoroutinesStack *> stacks;

  for (auto& mgr : source_mgrs) {
    RGWCoroutinesStack *stack = new RGWCoroutinesStack(store->ctx(), &cr_mgr);
    for (int i = 0; i < mgr->num_pipes(); ++i) {
      stack->call(mgr->run_sync_cr(i));
    }

    stacks.push_back(stack);
  }

  int ret = cr_mgr.run(stacks);
  if (ret < 0) {
    ldpp_dout(this, 0) << "ERROR: failed to read sync status for "
        << bucket_str{dest_bucket} << dendl;
    return ret;
  }

  return 0;
}

unsigned RGWBucketPipeSyncStatusManager::get_subsys() const
{
  return dout_subsys;
}

std::ostream& RGWBucketPipeSyncStatusManager::gen_prefix(std::ostream& out) const
{
  auto zone = std::string_view{source_zone.value_or(rgw_zone_id("*")).id};
  return out << "bucket sync zone:" << zone.substr(0, 8)
    << " bucket:" << dest_bucket << ' ';
}

string RGWBucketPipeSyncStatusManager::status_oid(const rgw_zone_id& source_zone,
                                              const rgw_bucket_sync_pair_info& sync_pair)
{
  if (sync_pair.source_bs == sync_pair.dest_bs) {
    return bucket_status_oid_prefix + "." + source_zone.id + ":" + sync_pair.dest_bs.get_key();
  } else {
    return bucket_status_oid_prefix + "." + source_zone.id + ":" + sync_pair.dest_bs.get_key() + ":" + sync_pair.source_bs.get_key();
  }
}

string RGWBucketPipeSyncStatusManager::obj_status_oid(const rgw_bucket_sync_pipe& sync_pipe,
                                                      const rgw_zone_id& source_zone,
                                                      const rgw::sal::RGWObject* obj)
{
  string prefix = object_status_oid_prefix + "." + source_zone.id + ":" + obj->get_bucket()->get_key();
  if (sync_pipe.source_bucket_info.bucket !=
      sync_pipe.dest_bucket_info.bucket) {
    prefix += string("/") + sync_pipe.dest_bucket_info.bucket.get_key();
  }
  return prefix + ":" + obj->get_name() + ":" + obj->get_instance();
}

class RGWCollectBucketSyncStatusCR : public RGWShardCollectCR {
  static constexpr int max_concurrent_shards = 16;
  rgw::sal::RGWRadosStore *const store;
  RGWDataSyncCtx *const sc;
  RGWDataSyncEnv *const env;
  RGWBucketInfo source_bucket_info;
  RGWBucketInfo dest_bucket_info;
  rgw_bucket_shard source_bs;
  rgw_bucket_shard dest_bs;

  rgw_bucket_sync_pair_info sync_pair;

  bool shard_to_shard_sync;

  using Vector = std::vector<rgw_bucket_shard_sync_info>;
  Vector::iterator i, end;

 public:
  RGWCollectBucketSyncStatusCR(rgw::sal::RGWRadosStore *store, RGWDataSyncCtx *sc,
                               const RGWBucketInfo& source_bucket_info,
                               const RGWBucketInfo& dest_bucket_info,
                               Vector *status)
    : RGWShardCollectCR(sc->cct, max_concurrent_shards),
      store(store), sc(sc), env(sc->env),
      source_bucket_info(source_bucket_info),
      dest_bucket_info(dest_bucket_info),
      i(status->begin()), end(status->end())
  {
    shard_to_shard_sync = (source_bucket_info.layout.current_index.layout.normal.num_shards == dest_bucket_info.layout.current_index.layout.normal.num_shards);

    source_bs = rgw_bucket_shard(source_bucket_info.bucket, source_bucket_info.layout.current_index.layout.normal.num_shards > 0 ? 0 : -1);
    dest_bs = rgw_bucket_shard(dest_bucket_info.bucket, dest_bucket_info.layout.current_index.layout.normal.num_shards > 0 ? 0 : -1);

    status->clear();
    status->resize(std::max<size_t>(1, source_bucket_info.layout.current_index.layout.normal.num_shards));

    i = status->begin();
    end = status->end();
  }

  bool spawn_next() override {
    if (i == end) {
      return false;
    }
    sync_pair.source_bs = source_bs;
    sync_pair.dest_bs = dest_bs;
    spawn(new RGWReadBucketPipeSyncStatusCoroutine(sc, sync_pair, &*i, nullptr), false);
    ++i;
    ++source_bs.shard_id;
    if (shard_to_shard_sync) {
      dest_bs.shard_id = source_bs.shard_id;
    }
    return true;
  }
};

int rgw_bucket_sync_status(const DoutPrefixProvider *dpp,
                           rgw::sal::RGWRadosStore *store,
                           const rgw_sync_bucket_pipe& pipe,
                           const RGWBucketInfo& dest_bucket_info,
                           const RGWBucketInfo *psource_bucket_info,
                           std::vector<rgw_bucket_shard_sync_info> *status)
{
  if (!pipe.source.zone ||
      !pipe.source.bucket ||
      !pipe.dest.zone ||
      !pipe.dest.bucket) {
    return -EINVAL;
  }

  if (*pipe.dest.bucket !=
      dest_bucket_info.bucket) {
    return -EINVAL;
  }

  const rgw_bucket& source_bucket = *pipe.source.bucket;

  RGWBucketInfo source_bucket_info;

  if (!psource_bucket_info) {
    auto& bucket_ctl = store->getRados()->ctl.bucket;

    int ret = bucket_ctl->read_bucket_info(source_bucket, &source_bucket_info, null_yield);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to get bucket instance info: bucket=" << source_bucket << ": " << cpp_strerror(-ret) << dendl;
      return ret;
    }

    psource_bucket_info = &source_bucket_info;
  }


  RGWDataSyncEnv env;
  RGWSyncModuleInstanceRef module; // null sync module
  env.init(dpp, store->ctx(), store, store->svc(), store->svc()->rados->get_async_processor(),
           nullptr, nullptr, nullptr, module, nullptr);

  RGWDataSyncCtx sc;
  sc.init(&env, nullptr, *pipe.source.zone);

  RGWCoroutinesManager crs(store->ctx(), store->getRados()->get_cr_registry());
  return crs.run(new RGWCollectBucketSyncStatusCR(store, &sc,
                                                  *psource_bucket_info,
                                                  dest_bucket_info,
                                                  status));
}
