// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "common/ceph_json.h"
#include "common/RefCountedObj.h"
#include "common/WorkQueue.h"
#include "common/Throttle.h"
#include "common/errno.h"

#include "rgw_common.h"
#include "rgw_zone.h"
#include "rgw_sync.h"
#include "rgw_data_sync.h"
#include "rgw_rest_conn.h"
#include "rgw_cr_rados.h"
#include "rgw_cr_sip.h"
#include "rgw_cr_rest.h"
#include "rgw_cr_tools.h"
#include "rgw_http_client.h"
#include "rgw_bucket.h"
#include "rgw_bucket_sync.h"
#include "rgw_bucket_sync_cache.h"
#include "rgw_datalog.h"
#include "rgw_metadata.h"
#include "rgw_sip_data.h"
#include "rgw_sip_bucket.h"
#include "rgw_sync_counters.h"
#include "rgw_sync_error_repo.h"
#include "rgw_sync_module.h"
#include "rgw_sync_info.h"
#include "rgw_sal.h"
#include "rgw_b64.h"

#include "cls/lock/cls_lock_client.h"
#include "cls/rgw/cls_rgw_client.h"

#include "services/svc_zone.h"
#include "services/svc_sync_modules.h"
#include "rgw_bucket.h"

#include "include/common_fwd.h"
#include "include/random.h"

#include <boost/asio/yield.hpp>
#include <string_view>

#define dout_subsys ceph_subsys_rgw

#undef dout_prefix
#define dout_prefix (*_dout << "data sync: ")

using namespace std;

static string datalog_sync_status_oid_prefix = "datalog.sync-status";
static string datalog_sync_status_shard_prefix = "datalog.sync-status.shard";
static string datalog_sync_full_sync_index_prefix = "data.full-sync.index";
static string bucket_status_oid_prefix = "bucket.sync-status";
static string object_status_oid_prefix = "bucket.sync-status";


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
  spawn(new CR(env->dpp, env->async_rados, env->svc->sysobj,
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
  int operate(const DoutPrefixProvider *dpp) override;
};

int RGWReadDataSyncStatusCoroutine::operate(const DoutPrefixProvider *dpp)
{
  reenter(this) {
    // read sync info
    using ReadInfoCR = RGWSimpleRadosReadCR<rgw_data_sync_info>;
    yield {
      bool empty_on_enoent = false; // fail on ENOENT
      call(new ReadInfoCR(dpp, sync_env->async_rados, sync_env->svc->sysobj,
                          rgw_raw_obj(sync_env->svc->zone->get_zone_params().log_pool, RGWDataSyncStatusManager::sync_status_oid(sc->source_zone)),
                          &sync_status->sync_info, empty_on_enoent));
    }
    if (retcode < 0) {
      ldpp_dout(dpp, 4) << "failed to read sync status info with "
          << cpp_strerror(retcode) << dendl;
      return set_cr_error(retcode);
    }
    // read shard markers
    using ReadMarkersCR = RGWReadDataSyncStatusMarkersCR;
    yield call(new ReadMarkersCR(sc, sync_status->sync_info.num_shards,
                                 sync_status->sync_markers));
    if (retcode < 0) {
      ldpp_dout(dpp, 4) << "failed to read sync status markers with "
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

  int operate(const DoutPrefixProvider *dpp) override {
    reenter(this) {
      yield {
	char buf[16];
	snprintf(buf, sizeof(buf), "%d", shard_id);
        rgw_http_param_pair pairs[] = { { "type" , "data" },
	                                { "id", buf },
					{ "info" , NULL },
	                                { NULL, NULL } };

        string p = "/admin/log/";

        http_op = new RGWRESTReadResource(sc->conns.data, p, pairs, NULL, sync_env->http_manager);

        init_new_io(http_op);

        int ret = http_op->aio_read(dpp);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "ERROR: failed to read from " << p << dendl;
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

  int operate(const DoutPrefixProvider *dpp) override {
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

        http_op = new RGWRESTReadResource(sc->conns.data, p, pairs, NULL, sync_env->http_manager);

        init_new_io(http_op);

        if (sync_env->counters) {
          timer.emplace(sync_env->counters, sync_counters::l_poll);
        }
        int ret = http_op->aio_read(dpp);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "ERROR: failed to read from " << p << dendl;
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

  int send_request(const DoutPrefixProvider *dpp) override {
    RGWRESTConn *conn = sc->conns.data;

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

    int ret = http_op->aio_read(dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to read from " << p << dendl;
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
      ldpp_dout(sync_env->dpp, 0) << "ERROR: failed to list remote datalog shard, ret=" << ret << dendl;
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

struct bucket_instance_meta_info;
struct read_metadata_list;

struct sip_data_list_result {
  string marker;
  bool truncated{false};

  struct entry {
    string entry_id;
    string key;
    rgw_bucket bucket;
    int shard_id{-1};
    int num_shards{0};
    ceph::real_time timestamp;
  };

  vector<entry> entries;
};

using sip_data_inc_pos  = rgw_sip_pos;

template <class T, class M>
class RGWSyncInfoCRHandler {
public:
  virtual ~RGWSyncInfoCRHandler() {}

  virtual string get_sip_name() const = 0;

  virtual int num_shards() const = 0;

  virtual RGWCoroutine *init_cr(RGWSyncTraceNodeRef& tn) = 0;

  virtual RGWCoroutine *get_pos_cr(int shard_id, M *pos, bool *disabled) = 0;
  virtual RGWCoroutine *fetch_cr(int shard_id,
                                 const string& marker,
                                 T *result) = 0;

  virtual RGWCoroutine *update_marker_cr(int shard_id,
                                         const RGWSI_SIP_Marker::SetParams& params) {
    return nullptr;
  }
};

class RGWDataSyncInfoCRHandler : virtual public RGWSyncInfoCRHandler<sip_data_list_result, sip_data_inc_pos> {
protected:
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;

public:
  RGWDataSyncInfoCRHandler(RGWDataSyncCtx *_sc) : sc(_sc),
                                                  sync_env(_sc->env) {}

  virtual ~RGWDataSyncInfoCRHandler() {}
};

using RGWDataSyncInfoCRHandlerRef = std::shared_ptr<RGWDataSyncInfoCRHandler>;

class RGWInitDataSyncStatusCoroutine : public RGWCoroutine {
  static constexpr uint32_t lock_duration = 30;
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  RGWDataSyncInfoCRHandlerPair dsi;
  rgw::sal::RGWRadosStore* store;
  const rgw_pool& pool;
  uint32_t num_shards;

  string sync_status_oid;

  string lock_name;
  string cookie;
  rgw_data_sync_status *status;

  vector<sip_data_inc_pos> shards_info;

  RGWSyncTraceNodeRef tn;

  int i;
  RGWCoroutine *cr;

public:
  RGWInitDataSyncStatusCoroutine(RGWDataSyncCtx *_sc,
                                 uint64_t instance_id,
                                 RGWSyncTraceNodeRef& _tn_parent,
                                 rgw_data_sync_status *status)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
      dsi(_sc->dsi), store(sync_env->store),
      pool(sync_env->svc->zone->get_zone_params().log_pool),
      status(status),
      tn(sync_env->sync_tracer->add_node(_tn_parent, "init_data_sync_status")) {
    lock_name = "sync_lock";

    status->sync_info.instance_id = instance_id;

    num_shards = sc->dsi.inc->num_shards();
    status->sync_info.num_shards = num_shards;

#define COOKIE_LEN 16
    char buf[COOKIE_LEN + 1];

    gen_rand_alphanumeric(cct, buf, sizeof(buf) - 1);
    cookie = buf;

    sync_status_oid = RGWDataSyncStatusManager::sync_status_oid(sc->source_zone);
  }

  int operate(const DoutPrefixProvider *dpp) override {
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
      yield call(new WriteInfoCR(dpp, sync_env->async_rados, sync_env->svc->sysobj,
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

#define DATA_INIT_SPAWN_WINDOW 16
      /* fetch current position in logs */
      shards_info.resize(num_shards);
      for (i = 0; i < (int)num_shards; i++) {
        yield_spawn_window(dsi.inc->get_pos_cr(i, &shards_info[i], nullptr),
                           DATA_INIT_SPAWN_WINDOW,
                           [&](uint64_t stack_id, int ret) {
                           if (ret < 0) {
                             tn->log(0, SSTR("ERROR: failed to read remote data log shards"));
                             return ret;
                           }
                           return 0;
                         });

      }

      drain_all_cb([&](uint64_t stack_id, int ret) {
        if (ret < 0) {
          tn->log(0, SSTR("ERROR: failed to read remote data log shards"));
          return ret;
        }
        return 0;
      });

      /* init remote status markers */
      for (i = 0; i < (int)num_shards; i++) {

        {
          auto& info = shards_info[i];
          cr = dsi.inc->update_marker_cr(i, { RGWSI_SIP_Marker::create_target_id(sync_env->svc->zone->get_zone().id, nullopt),
                                               info.marker,
                                               info.timestamp,
                                               false });
        }

        yield_spawn_window(cr,
                           DATA_INIT_SPAWN_WINDOW,
                           [&](uint64_t stack_id, int ret) {
                             if (ret < 0) {
                               tn->log(0, SSTR("WARNING: failed to update remote sync status markers"));

                               /* not erroring out, should we? */
                             }
                             return 0;
                           });

      }

      drain_all_cb([&](uint64_t stack_id, int ret) {
        if (ret < 0) {
          tn->log(0, SSTR("ERROR: failed to read remote data log shards"));

          /* not erroring out, should we? */
        }
        return 0;
      });

      /* init local status */
      yield {
        for (uint32_t i = 0; i < num_shards; i++) {
          auto& info = shards_info[i];
          auto& marker = status->sync_markers[i];
          marker.next_step_marker = info.marker;
          marker.next_sip_name = dsi.inc->get_sip_name();
          marker.timestamp = info.timestamp;
          const auto& oid = RGWDataSyncStatusManager::shard_obj_name(sc->source_zone, i);
          using WriteMarkerCR = RGWSimpleRadosWriteCR<rgw_data_sync_marker>;
          spawn(new WriteMarkerCR(dpp, sync_env->async_rados, sync_env->svc->sysobj,
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
      yield call(new WriteInfoCR(dpp, sync_env->async_rados, sync_env->svc->sysobj,
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
                                   rgw::sal::RadosStore* store,
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

int RGWRemoteDataLog::read_source_log_shards_info(const DoutPrefixProvider *dpp, map<int, RGWDataChangesLogInfo> *shards_info)
{
  return run(dpp, new RGWReadRemoteDataLogInfoCR(&sc, sc.dsi.inc->num_shards(), shards_info));
}

int RGWRemoteDataLog::read_source_log_shards_next(const DoutPrefixProvider *dpp, map<int, string> shard_markers, map<int, rgw_datalog_shard_data> *result)
{
  return run(dpp, new RGWListRemoteDataLogCR(&sc, shard_markers, 1, result));
}

int RGWRemoteDataLog::init(const rgw_zone_id& _source_zone, const RGWRemoteCtl::Conns& _conns, RGWSyncErrorLogger *_error_logger,
                           RGWSyncTraceManager *_sync_tracer, RGWSyncModuleInstanceRef& _sync_module,
                           PerfCounters* counters)
{
  sync_env.init(dpp, cct, store, store->svc(), async_rados, &http_manager, _error_logger,
                _sync_tracer, _sync_module, counters);

  sc.init(&sync_env, _conns, _source_zone);

  if (initialized) {
    return 0;
  }

  tn = sync_env.sync_tracer->add_node(sync_env.sync_tracer->root_node, "data");

  int ret;

  /* init() might be called multiple times, if it failed to complete initialization */
  if (!http_manager.is_started()) {
    ret = http_manager.start();
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "failed in http_manager.start() ret=" << ret << dendl;
      return ret;
    }
  }

  ret = run(sc.dsi.full->init_cr(tn));
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "failed to initialize datalog full sync handler: " << ret << dendl;
    return ret;
  }

  ret = run(sc.dsi.inc->init_cr(tn));
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "failed to initialize datalog incrementa sync handler: " << ret << dendl;
    return ret;
  }

  initialized = true;

  return 0;
}

void RGWRemoteDataLog::finish()
{
  stop();
}

int RGWRemoteDataLog::local_call(const DoutPrefixProvider *dpp, std::function<int(RGWCoroutinesManager&, RGWDataSyncCtx&)> f)
{
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
  sc_local.reset_env(&sync_env_local);

  ret = f(crs, sc_local);

  http_manager.stop();

  return ret;
}

int RGWRemoteDataLog::read_sync_status(const DoutPrefixProvider *dpp, rgw_data_sync_status *sync_status)
{
  // cannot run concurrently with run_sync(), so run in a separate manager
  int ret = local_call(dpp, [&](RGWCoroutinesManager& crs, RGWDataSyncCtx& sc_local) {

    return crs.run(new RGWReadDataSyncStatusCoroutine(&sc, sync_status));
  });

  return ret;
}

int RGWRemoteDataLog::read_recovering_shards(const DoutPrefixProvider *dpp, const int num_shards, set<int>& recovering_shards)
{
  std::vector<RGWRadosGetOmapKeysCR::ResultPtr> omapkeys;

  // cannot run concurrently with run_sync(), so run in a separate manager
  int ret = local_call(dpp, [&](RGWCoroutinesManager& crs, RGWDataSyncCtx& sc_local) {

    omapkeys.resize(num_shards);
    uint64_t max_entries{1};

    return crs.run(dpp, new RGWReadDataSyncRecoveringShardsCR(&sc, max_entries, num_shards, omapkeys));
  });

  if (ret == 0) {
    for (int i = 0; i < num_shards; i++) {
      if (omapkeys[i]->entries.size() != 0) {
        recovering_shards.insert(i);
      }
    }
  }

  return ret;
}

int RGWRemoteDataLog::init_sync_status(const DoutPrefixProvider *dpp)
{
  int ret = local_call(dpp, [&](RGWCoroutinesManager& crs, RGWDataSyncCtx& sc_local) {
    rgw_data_sync_status sync_status;
    auto instance_id = ceph::util::generate_random_number<uint64_t>();

    return crs.run(dpp, new RGWInitDataSyncStatusCoroutine(&sc_local, instance_id, tn, &sync_status));
  });

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

class RGWDataFullSyncInfoCRHandler_Legacy : public RGWDataSyncInfoCRHandler {
  string path;

  class FetchFullCR : public RGWCoroutine {
    RGWDataFullSyncInfoCRHandler_Legacy *handler;
    RGWDataSyncCtx *sc;
    string marker;
    sip_data_list_result *result;

    read_metadata_list list_result;
    list<string>::iterator iter;
    bucket_instance_meta_info meta_info;
  public:
    FetchFullCR(RGWDataFullSyncInfoCRHandler_Legacy *_handler,
                RGWDataSyncCtx *_sc,
                const string& _marker,
                sip_data_list_result *_result) : RGWCoroutine(_sc->cct),
                                                 handler(_handler),
                                                 sc(_sc),
                                                 marker(_marker),
                                                 result(_result) {}

    int operate() {
      reenter(this) {
        yield {
          string entrypoint = string("/admin/metadata/bucket.instance");

          rgw_http_param_pair pairs[] = {{"max-entries", "1000"},
                                         {"marker", marker.c_str()},
                                         {NULL, NULL}};
          call(new RGWReadRESTResourceCR<read_metadata_list>(sc->env->cct, sc->conns.data,
                                                             sc->env->http_manager,
                                                             entrypoint, pairs, &list_result));
        }
        if (retcode < 0) {
          return set_cr_error(retcode);
        }

        result->truncated = list_result.truncated;
        result->marker = list_result.marker;
        result->entries.clear();

        for (iter = list_result.keys.begin(); iter != list_result.keys.end(); ++iter) {
          yield call(handler->get_source_bucket_info_cr(*iter, &meta_info));

          auto& bucket_info = meta_info.data.get_bucket_info();

          sip_data_list_result::entry e;
          e.entry_id = *iter;
          e.key = *iter;
          e.bucket = bucket_info.bucket;
          e.shard_id = -1;
          e.num_shards = bucket_info.layout.current_index.layout.normal.num_shards;
          e.timestamp = bucket_info.creation_time;

          result->entries.emplace_back(std::move(e));
        }
        return set_cr_done();
      }
      return 0;
    }
  };

  RGWCoroutine *get_source_bucket_info_cr(const string& key,
                                          bucket_instance_meta_info *result) {
    rgw_http_param_pair pairs[] = {{"key", key.c_str()},
                                   {NULL, NULL}};

    return new RGWReadRESTResourceCR<bucket_instance_meta_info>(sync_env->cct, sc->conns.data,
                                                                sync_env->http_manager, path, pairs, result);
  }

public:

  RGWDataFullSyncInfoCRHandler_Legacy(RGWDataSyncCtx *_sc) : RGWDataSyncInfoCRHandler(_sc) {
    path = "/admin/metadata/bucket.instance";
  }

  string get_sip_name() const override {
    return "legacy/data.full";
  }

  int num_shards() const override {
    return 1;
  }

  RGWCoroutine *init_cr(RGWSyncTraceNodeRef& tn) override {
    /* nothing to init */
    return nullptr;
  }

  RGWCoroutine *fetch_cr(int shard_id,
                         const string& marker,
                         sip_data_list_result *result) override {
    return new FetchFullCR(this, sc, marker, result);

  }

  RGWCoroutine *get_pos_cr(int shard_id, sip_data_inc_pos *pos, bool *disabled) override {
    pos->marker.clear();
    pos->timestamp = ceph::real_time();
    if (*disabled) {
      *disabled = false;
    }
    return nullptr;
  }
};

struct rgw_datalog_info {
  uint32_t num_shards;

  rgw_datalog_info() : num_shards(0) {}

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("num_objects", num_shards, obj);
  }
};

class RGWDataIncSyncInfoCRHandler_Legacy : public RGWDataSyncInfoCRHandler {
  friend class InitCR;

  rgw_datalog_info remote_datalog_info;

  class ReadDatalogStatusCR : public RGWCoroutine {
    RGWDataSyncCtx *sc;
    int shard_id;
    string *marker;
    ceph::real_time *timestamp;

    RGWDataChangesLogInfo info;

  public:
    ReadDatalogStatusCR(RGWDataSyncCtx *_sc,
                        int _shard_id,
                        string *_marker,
                        ceph::real_time *_timestamp) : RGWCoroutine(_sc->cct),
                                                       sc(_sc),
                                                       shard_id(_shard_id),
                                                       marker(_marker),
                                                       timestamp(_timestamp) {}

    int operate() {
      reenter(this) {
        yield call(new RGWReadRemoteDataLogShardInfoCR(sc, shard_id, &info));
        if (retcode < 0) {
          return set_cr_error(retcode);
        }
        *marker = info.marker;
        *timestamp = info.last_update;
        return set_cr_done();
      }
      return 0;
    }
  };

  class InitCR : public RGWCoroutine {
    RGWDataIncSyncInfoCRHandler_Legacy *handler;
    RGWDataSyncCtx *sc;

  public:
    InitCR(RGWDataIncSyncInfoCRHandler_Legacy *_handler,
           RGWDataSyncCtx *_sc) : RGWCoroutine(_sc->cct),
                                                handler(_handler),
                                                sc(_sc) {}

    int operate() {
      reenter(this) {
        yield {
          string entrypoint = string("/admin/log");

          rgw_http_param_pair pairs[] = { { "type", "data" },
                                          { nullptr, nullptr } };

          call(new RGWReadRESTResourceCR<rgw_datalog_info>(sc->env->cct, sc->conns.data,
                                                           sc->env->http_manager,
                                                           entrypoint, pairs, &handler->remote_datalog_info));
        }
        if (retcode < 0) {
          ldout(cct, 0) << "ERROR: failed to fetch remote datalog info: retcode=" << retcode << dendl;
          return set_cr_error(retcode);
        }

        return set_cr_done();
      }
      return 0;
    }
  };

  class FetchIncCR : public RGWCoroutine {
    RGWDataIncSyncInfoCRHandler_Legacy *handler;
    RGWDataSyncCtx *sc;
    int shard_id;
    string marker;
    sip_data_list_result *result;

    list<rgw_data_change_log_entry> fetch_result;
    list<rgw_data_change_log_entry>::iterator iter;
  public:
    FetchIncCR(RGWDataIncSyncInfoCRHandler_Legacy *_handler,
               RGWDataSyncCtx *_sc,
               int _shard_id,
               const string& _marker,
               sip_data_list_result *_result) : RGWCoroutine(_sc->cct),
                                                handler(_handler),
                                                sc(_sc),
                                                shard_id(_shard_id),
                                                marker(_marker),
                                                result(_result) {}

    int operate() {
      reenter(this) {
        yield call(new RGWReadRemoteDataLogShardCR(sc, shard_id,
                                                   marker,
                                                   &result->marker,
                                                   &fetch_result,
                                                   &result->truncated));
        if (retcode < 0) {
          return set_cr_error(retcode);
        }

        result->entries.clear();

        for (iter = fetch_result.begin(); iter != fetch_result.end(); ++iter) {
          auto& log_entry = *iter;

          sip_data_list_result::entry e;
          e.key = log_entry.entry.key;

          int r = rgw_bucket_parse_bucket_key(sc->cct, e.key,
                                              &e.bucket, &e.shard_id);
          if (r < 0) {
            ldout(sc->cct, 0) << "ERROR: " << __func__ << "(): failed to parse bucket key: " << e.key << ", r=" << r << ", skipping entry" << dendl;
            continue;
          }

          e.entry_id = log_entry.log_id;
          e.num_shards = -1; /* unknown */
          e.timestamp = log_entry.log_timestamp;

          result->entries.emplace_back(std::move(e));
        }
        return set_cr_done();
      }
      return 0;
    }
  };

public:

  RGWDataIncSyncInfoCRHandler_Legacy(RGWDataSyncCtx *_sc) : RGWDataSyncInfoCRHandler(_sc) {
  }

  string get_sip_name() const override {
    return "legacy/data.inc";
  }

  int num_shards() const override {
    return remote_datalog_info.num_shards;
  }

  RGWCoroutine *init_cr(RGWSyncTraceNodeRef& tn) override {
    return new InitCR(this, sc);
  }

  RGWCoroutine *get_pos_cr(int shard_id, sip_data_inc_pos *pos, bool *disabled) override {
    if (disabled) {
      *disabled = false;
    }
    return new ReadDatalogStatusCR(sc, shard_id, &pos->marker, &pos->timestamp);
  }

  RGWCoroutine *fetch_cr(int shard_id,
                         const string& marker,
                         sip_data_list_result *result) override {
    return new FetchIncCR(this, sc, shard_id, marker, result);
  }
};

template <class T, class M>
class RGWSyncInfoCRHandler_SIP : virtual public RGWSyncInfoCRHandler<T, M> {
  friend class InitCR;

protected:
  static constexpr int max_entries = 1000;

  CephContext *cct;
  string data_type;
  SIProvider::StageType stage_type;

  std::unique_ptr<SIProviderCRMgr_REST> sip;

  SIProvider::Info info;
  SIProvider::stage_id_t sid;
  SIProvider::StageInfo stage_info;

  SIProvider::TypeHandler *type_handler;

  void copy_state(RGWSyncInfoCRHandler_SIP *dest) {
    dest->info = info;
    dest->sid = sid;
    dest->stage_info = stage_info;
  }

  string sip_id() const {
    return data_type + ":" + SIProvider::stage_type_to_str(stage_type);
  }

  class InitCR : public RGWCoroutine {
    RGWSyncInfoCRHandler_SIP *siph;
    bool found_sid{false};

  public:
    InitCR(RGWSyncInfoCRHandler_SIP *_siph) : RGWCoroutine(_siph->ctx()),
                                              siph(_siph) {}

    int operate() {
      reenter(this) {
        yield call(siph->sip->init_cr());
        if (retcode < 0) {
          ldout(cct, 0) << "ERROR: failed to initialize sip (" << siph->sip_id() << "): retcode=" << retcode << dendl;
          return set_cr_error(retcode);
        }
        siph->info = siph->sip->get_info();
        if (siph->info.stages.empty()) {
          ldout(cct, 0) << "ERROR: sip (" << siph->sip_id() << ") has no stages, likely a bug!" << dendl;
          return set_cr_error(-EIO);
        }
        {
          for (auto& stage : siph->info.stages) {
            if (stage.type == siph->stage_type) {
              siph->sid = stage.sid;
              found_sid = true;
            }
          }
        }
        if (!found_sid) {
          ldout(cct, 0) << "ERROR: sip (" << siph->sip_id() << ") failed to find an appropirate stage" << dendl;
          return set_cr_error(-EIO);
        }
        yield call(siph->sip->get_stage_info_cr(siph->sid, &siph->stage_info));
        if (retcode < 0) {
          ldout(cct, 0) << "ERROR: sip (" << siph->sip_id() << ") failed to fetch stage info for sid " << siph->sid << ", retcode=" << retcode << dendl;
          return set_cr_error(retcode);
        }
        return set_cr_done();
      }
      return 0;
    }
  };

  class FetchCR : public RGWCoroutine {
    RGWSyncInfoCRHandler_SIP *handler;

    SIProviderCRMgr_REST *provider;
    SIProvider::stage_id_t sid;
    int shard_id;
    string marker;
    T *result;

    SIProvider::fetch_result provider_result;
    vector<SIProvider::Entry>::iterator iter;
  public:
    FetchCR(RGWSyncInfoCRHandler_SIP *_handler,
            SIProviderCRMgr_REST *_provider,
            const SIProvider::stage_id_t& _sid,
            int _shard_id,
            const string& _marker,
            T *_result) : RGWCoroutine(_handler->ctx()),
                          handler(_handler),
                          provider(_provider),
                          sid(_sid),
                          shard_id(_shard_id),
                          marker(_marker),
                          result(_result) {}

    int operate() {
      reenter(this) {
        yield call(provider->fetch_cr(sid, shard_id, marker, handler->max_entries, &provider_result));
        if (retcode < 0) {
          return set_cr_error(retcode);
        }

        result->truncated = provider_result.more;
        if (!provider_result.entries.empty()) {
          result->marker = provider_result.entries.back().key;
        } else {
          result->marker.clear();
        }
        result->entries.clear();

        for (iter = provider_result.entries.begin(); iter != provider_result.entries.end(); ++iter) {

          typename T::entry e;

          int r = handler->handle_fetched_entry(sid, *iter, &e);
          if (r < 0) {
            continue;
          }
          result->entries.emplace_back(std::move(e));
        }
        return set_cr_done();
      }
      return 0;
    }
  };

  virtual int handle_fetched_entry(const SIProvider::stage_id_t& sid, SIProvider::Entry& fetched_entry, typename T::entry *pe) = 0;

  void sip_init(std::unique_ptr<SIProviderCRMgr_REST>&& _sip) {
    sip = std::move(_sip);
    type_handler = sip->get_type_handler();
  }

public:
  RGWSyncInfoCRHandler_SIP(CephContext *_cct,
                           string _data_type,
                           SIProvider::StageType _stage_type) : cct(_cct),
                                                                data_type(_data_type),
                                                                stage_type(_stage_type) {
  }

  CephContext *ctx() {
    return cct;
  }

  string get_sip_name() const override {
    return info.name;
  }

  RGWCoroutine *init_cr(RGWSyncTraceNodeRef& tn) override {
    return new InitCR(this);
  }

  int num_shards() const override {
    return stage_info.num_shards;
  }

  RGWCoroutine *fetch_cr(int shard_id,
                         const string& marker,
                         T *result) override {
    return new FetchCR(this, sip.get(), sid, shard_id, marker, result);
  }
};

class RGWDataSyncInfoCRHandler_SIP_Base : public RGWSyncInfoCRHandler_SIP<sip_data_list_result, sip_data_inc_pos>,
                                          public RGWDataSyncInfoCRHandler
{
  SIProvider::TypeHandlerProviderRef type_provider;

protected:
  virtual RGWDataSyncInfoCRHandler_SIP_Base *alloc(RGWDataSyncCtx *sc) = 0;

  int handle_fetched_entry(const SIProvider::stage_id_t& sid, SIProvider::Entry& fetched_entry, sip_data_list_result::entry *pe) override {
    sip_data_list_result::entry& e = *pe;

    e.entry_id = fetched_entry.key;

    int r = type_handler->handle_entry(sid, fetched_entry, [&](SIProvider::EntryInfoBase& _info) {
      auto& info = static_cast<siprovider_data_info&>(_info);

      e.key = info.key;

      int r = rgw_bucket_parse_bucket_key(sc->cct, info.key,
                                          &e.bucket, &e.shard_id);
      if (r < 0) {
        ldout(sc->cct, 0) << "ERROR: " << __func__ << "(): failed to parse bucket key: " << info.key << ", r=" << r << ", skipping entry" << dendl;
        return r;
      }

      e.shard_id = info.shard_id;
      e.num_shards = info.num_shards;
      if (info.timestamp) {
        e.timestamp = *info.timestamp;
      }

      return 0;
    });

    return r;
  }

public:
  RGWDataSyncInfoCRHandler_SIP_Base(RGWDataSyncCtx *_sc,
                                    SIProvider::StageType _stage_type) : RGWSyncInfoCRHandler_SIP(_sc->cct,
                                                                                                  "data",
                                                                                                  _stage_type),
                                                                         RGWDataSyncInfoCRHandler(_sc) {
    type_provider = std::make_shared<SITypeHandlerProvider_Default<siprovider_data_info> >();
    sip_init(std::make_unique<SIProviderCRMgr_REST>(_sc->cct,
                                                    _sc->conns.sip,
                                                    _sc->env->http_manager,
                                                    data_type,
                                                    stage_type,
                                                    type_provider.get(),
                                                    nullopt));
  }

  /* create a clone object with new data sync ctx,
   * needed so that caller could use a private http manager
   * and propagate it to our sip manager
   */
  RGWDataSyncInfoCRHandler_SIP_Base *clone(RGWDataSyncCtx *new_sc) {
    auto myclone = alloc(new_sc);
    copy_state(myclone);
    return myclone;
  }
};

class RGWDataFullSyncInfoCRHandler_SIP : public RGWDataSyncInfoCRHandler_SIP_Base {

protected:
  RGWDataSyncInfoCRHandler_SIP_Base *alloc(RGWDataSyncCtx *new_sc) override {
    return new RGWDataFullSyncInfoCRHandler_SIP(new_sc);
  }

public:
  RGWDataFullSyncInfoCRHandler_SIP(RGWDataSyncCtx *_sc) : RGWDataSyncInfoCRHandler_SIP_Base(_sc, SIProvider::StageType::FULL) {
  }

  RGWCoroutine *get_pos_cr(int shard_id, sip_data_inc_pos *pos, bool *disabled) override {
    pos->marker.clear();
    pos->timestamp = ceph::real_time();
    if (disabled) {
      *disabled = false;
    }
    return nullptr;
  }

};

class RGWDataIncSyncInfoCRHandler_SIP : public RGWDataSyncInfoCRHandler_SIP_Base {

protected:
  RGWDataSyncInfoCRHandler_SIP_Base *alloc(RGWDataSyncCtx *new_sc) override {
    return new RGWDataIncSyncInfoCRHandler_SIP(new_sc);
  }

public:
  RGWDataIncSyncInfoCRHandler_SIP(RGWDataSyncCtx *_sc) : RGWDataSyncInfoCRHandler_SIP_Base(_sc, SIProvider::StageType::INC) {
  }

  RGWCoroutine *get_pos_cr(int shard_id, sip_data_inc_pos *pos, bool *disabled) override {
    return sip->get_cur_state_cr(sid, shard_id, pos, disabled);
  }

  RGWCoroutine *update_marker_cr(int shard_id,
                                 const RGWSI_SIP_Marker::SetParams& params) override {
    return sip->update_marker_cr(sid, shard_id, params);
  }
};

class RGWListBucketIndexesCR : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;

  rgw::sal::RadosStore* store;

  rgw_data_sync_status *sync_status;
  int num_shards;

  int req_ret;
  int ret;


  RGWShardedOmapCRManager *entries_index;

  string oid_prefix;

  string path;
  string key;
  string s;
  int i;

  bool failed;
  bool truncated;
  sip_data_list_result result;
  vector<sip_data_list_result::entry>::iterator iter;

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

  int operate(const DoutPrefixProvider *dpp) override {
    reenter(this) {
      entries_index = new RGWShardedOmapCRManager(sync_env->async_rados, store, this, num_shards,
						  sync_env->svc->zone->get_zone_params().log_pool,
                                                  oid_prefix);
      yield; // yield so OmapAppendCRs can start

      do {
        yield call(sc->dsi.full->fetch_cr(0, result.marker, &result));
        if (retcode < 0) {
          ldpp_dout(dpp, 0) << "ERROR: failed to fetch metadata for section bucket.instance" << dendl;
          return set_cr_error(retcode);
        }

        for (iter = result.entries.begin(); iter != result.entries.end(); ++iter) {
          ldout_dpp(dpp, 20) << "fetch full: bucket=" << iter->bucket << dendl;

          if (iter->num_shards > 0) {
            for (i = 0; i < iter->num_shards; i++) {
              char buf[16];
              snprintf(buf, sizeof(buf), ":%d", i);
              s = iter->key + buf;
              yield entries_index->append(s, sync_env->svc->datalog_rados->calc_shard(rgw_bucket_shard{iter->bucket, i}, num_shards));
            }
          } else {
            yield entries_index->append(iter->key, sync_env->svc->datalog_rados->calc_shard(rgw_bucket_shard{iter->bucket, -1}, num_shards));
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
          spawn(new RGWSimpleRadosWriteCR<rgw_data_sync_marker>(dpp, sync_env->async_rados, sync_env->svc->sysobj,
                                                                rgw_raw_obj(sync_env->svc->zone->get_zone_params().log_pool, RGWDataSyncStatusManager::shard_obj_name(sc->source_zone, shard_id)),
                                                                marker),
                true);
        }
      } else {
        yield call(sync_env->error_logger->log_error_cr(dpp, sc->conns.data->get_remote_id(), "data.init", "",
                                                        EIO, string("failed to build bucket instances map")));
      }
      while (collect(&ret, NULL)) {
        if (ret < 0) {
          yield call(sync_env->error_logger->log_error_cr(dpp, sc->conns.data->get_remote_id(), "data.init", "",
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

  RGWCoroutine *store_marker(const DoutPrefixProvider *dpp, const string& new_marker, const string& key, uint64_t index_pos, const real_time& timestamp) override {
    sync_marker.marker = new_marker;
    sync_marker.pos = index_pos;
    sync_marker.timestamp = timestamp;

    tn->log(20, SSTR("updating marker marker_oid=" << marker_oid << " marker=" << new_marker));

    return new RGWSimpleRadosWriteCR<rgw_data_sync_marker>(dpp, sync_env->async_rados, sync_env->svc->sysobj,
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

struct sip_bucket_fetch_result {
  string marker;
  bool truncated{false};

  struct entry {
    string id;
    rgw_obj_key key;
    ceph::real_time mtime;
    std::optional<uint64_t> versioned_epoch;
    RGWModifyOp op;
    RGWPendingState state;
    std::string tag;
    bool is_versioned{false};
    rgw_bucket_entry_owner owner;
    rgw_zone_set zones_trace;
  };

  vector<entry> entries;
};

class RGWBucketPipeSyncInfoCRHandler : virtual public RGWSyncInfoCRHandler<sip_bucket_fetch_result, rgw_bucket_index_marker_info> {
protected:
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;

public:

  RGWBucketPipeSyncInfoCRHandler(RGWDataSyncCtx *_sc) : sc(_sc),
                                                        sync_env(_sc->env) {}
  virtual ~RGWBucketPipeSyncInfoCRHandler() {}
};

class RGWBucketPipeSyncStatusCRHandler {
protected:
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  rgw_bucket_sync_pair_info sync_pair;

public:

  RGWBucketPipeSyncStatusCRHandler(RGWDataSyncCtx *_sc,
                                   const rgw_bucket_sync_pair_info& _sync_pair) : sc(_sc),
                                                                                  sync_env(_sc->env),
                                                                                  sync_pair(_sync_pair) {}

  virtual ~RGWBucketPipeSyncStatusCRHandler() {}

  virtual RGWCoroutine *read_status_cr(rgw_bucket_shard_sync_info *status) = 0;
  virtual RGWCoroutine *write_status_cr(const rgw_bucket_shard_sync_info& status) = 0;
  virtual RGWCoroutine *remove_status_cr() = 0;
};

using RGWBucketPipeInfoHandlerRef = std::shared_ptr<RGWBucketPipeSyncInfoCRHandler>;
using RGWBucketPipeStatusHandlerRef = std::shared_ptr<RGWBucketPipeSyncStatusCRHandler>;

class RGWRunBucketSyncCoroutine : public RGWCoroutine {
  RGWBucketSyncCtx *bsc;
  RGWDataSyncEnv *sync_env;
  boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr;
  rgw_bucket_sync_pair_info sync_pair;
  rgw_bucket_sync_pipe sync_pipe;
  rgw_bucket_shard_sync_info sync_status;
  RGWMetaSyncEnv meta_sync_env;
  ceph::real_time* progress;

  const std::string status_oid;

  RGWSyncTraceNodeRef tn;

public:
  RGWRunBucketSyncCoroutine(RGWBucketSyncCtx *_bsc,
                            boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr,
                            const RGWSyncTraceNodeRef& _tn_parent,
                            ceph::real_time* progress)
    : RGWCoroutine(_bsc->sc->cct), bsc(_bsc), sync_env(_bsc->sc->env),
      lease_cr(std::move(lease_cr)),
      sync_pair(_bsc->sync_pair), progress(progress),
      tn(sync_env->sync_tracer->add_node(_tn_parent, "bucket",
                                         SSTR(bucket_shard_str{sync_pair.dest_bs} << "<-" << bucket_shard_str{sync_pair.source_bs} ))) {
  }

  int operate(const DoutPrefixProvider *dpp) override;
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

  int operate(const DoutPrefixProvider *dpp) override;
};

class RGWBucketShardSIPCRHandlersRepo;
class RGWBucketShardSIPCRWrapperCore;

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
  
  struct stack_info {
    ceph::real_time progress;
    struct {
      std::shared_ptr<RGWBucketShardSIPCRWrapper> full;
      std::shared_ptr<RGWBucketShardSIPCRWrapper> inc;
    } bsi;
    RGWBucketPipeStatusHandlerRef bst;
    RGWBucketSyncCtx bsc;
  };

  std::map<uint64_t, stack_info> stacks_info;

  stack_info *cur_stack{nullptr};
  std::optional<ceph::real_time> min_progress;

  RGWRESTConn *conn{nullptr};
  rgw_zone_id last_zone;

  int source_num_shards{0};
  int target_num_shards{0};

  int num_shards{0};
  int cur_shard{0};
  bool again = false;

  std::shared_ptr<RGWBucketShardSIPCRHandlersRepo> handlers_repo;

  std::shared_ptr<RGWBucketShardSIPCRWrapperCore> wrapper_core_full;
  std::shared_ptr<RGWBucketShardSIPCRWrapperCore> wrapper_core_inc;

  RGWBucketInfo bucket_info;

public:
  RGWRunBucketSourcesSyncCR(RGWDataSyncCtx *_sc,
                            boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr,
                            std::optional<rgw_bucket_shard> _target_bs,
                            std::optional<rgw_bucket_shard> _source_bs,
                            const RGWSyncTraceNodeRef& _tn_parent,
                            ceph::real_time* progress);

  int operate(const DoutPrefixProvider *dpp) override;

  void handle_complete_stack(uint64_t stack_id) {
    auto iter = stacks_info.find(stack_id);
    if (iter == stacks_info.end()) {
      lderr(cct) << "ERROR: RGWRunBucketSourcesSyncCR::handle_complete_stack(): stack_id=" << stack_id << " not found! Likely a bug" << dendl;
      return;
    }
    auto& sinfo = iter->second;
    if (progress) {
      if (!min_progress) {
        min_progress = sinfo.progress;
      } else {
        if (sinfo.progress < *min_progress) {
          min_progress = sinfo.progress;
        }
      }
    }

    stacks_info.erase(stack_id);
  }
};

class RGWDataSyncSingleEntryCR : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  std::shared_ptr<RGWDataSyncInfoCRHandler> sip;
  int shard_id;
  rgw::bucket_sync::Handle state; // cached bucket-shard state
  rgw_data_sync_obligation obligation; // input obligation
  std::optional<rgw_data_sync_obligation> complete; // obligation to complete
  uint32_t obligation_counter = 0;
  RGWDataSyncShardMarkerTrack *marker_tracker;
  const rgw_raw_obj& error_repo;
  boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr;
  RGWSyncTraceNodeRef tn;

  bool has_lowerbound_marker{false};
  RGWDataSyncShardMarkerTrack::marker_entry_type lowerbound_marker;

  ceph::real_time progress;
  int sync_status = 0;
public:
  RGWDataSyncSingleEntryCR(RGWDataSyncCtx *_sc,
                           std::shared_ptr<RGWDataSyncInfoCRHandler>& _sip,
                           int _shard_id,
                           rgw::bucket_sync::Handle state,
                           rgw_data_sync_obligation obligation,
                           RGWDataSyncShardMarkerTrack *_marker_tracker,
                           const rgw_raw_obj& error_repo,
                           boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr,
                           const RGWSyncTraceNodeRef& _tn_parent)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
      sip(_sip), shard_id(_shard_id),
      state(std::move(state)), obligation(std::move(obligation)),
      marker_tracker(_marker_tracker), error_repo(error_repo),
      lease_cr(std::move(lease_cr)) {
    set_description() << "data sync single entry (source_zone=" << sc->source_zone << ") " << obligation;
    tn = sync_env->sync_tracer->add_node(_tn_parent, "entry", obligation.key);
  }

  int operate(const DoutPrefixProvider *dpp) override {
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
        while ((state->obligation->timestamp == ceph::real_time() ||
                state->progress_timestamp < state->obligation->timestamp) &&
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
          yield call(sync_env->error_logger->log_error_cr(dpp, sc->conns.data->get_remote_id(), "data", complete->key,
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
        yield call(marker_tracker->finish(dpp, complete->marker));

        has_lowerbound_marker = marker_tracker->get_lowerbound(&lowerbound_marker);
      }
      if (sync_status == 0) {
        sync_status = retcode;
      }
      if (sync_status < 0) {
        return set_cr_error(sync_status);
      }
      if (sip && has_lowerbound_marker) {
        yield call(sip->update_marker_cr(shard_id, { RGWSI_SIP_Marker::create_target_id(sc->env->svc->zone->get_zone().id, nullopt),
                                                     lowerbound_marker.marker,
                                                     lowerbound_marker.timestamp,
                                                     false }));
      }
      if (retcode < 0) {
        tn->log(0, SSTR("ERROR: failed to update source with minimu marker: retcode=" << retcode));
        /* not much to do about it now, assuming this is transient issue and will be fixed
         * next time */
      }
      return set_cr_done();
    }
    return 0;
  }
};

void RGWDataSyncCtx::init(RGWDataSyncEnv *_env,
                          const RGWRemoteCtl::Conns& _conns,
                          const rgw_zone_id& _source_zone) {
  cct = _env->cct;
  env = _env;
  conns = _conns;
  source_zone = _source_zone;

  dsi.full.reset(new RGWDataFullSyncInfoCRHandler_SIP(this));
  dsi.inc.reset(new RGWDataIncSyncInfoCRHandler_SIP(this));
}

void RGWDataSyncCtx::reset_env(RGWDataSyncEnv *new_env)
{
  env = new_env;
  cct = new_env->cct;

  auto dsi_full = static_cast<RGWDataFullSyncInfoCRHandler_SIP *>(dsi.full.get());
  auto dsi_inc = static_cast<RGWDataIncSyncInfoCRHandler_SIP *>(dsi.inc.get());

  dsi.full.reset(dsi_full->clone(this));
  dsi.inc.reset(dsi_inc->clone(this));
}

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

  sip_data_list_result fetch_result;
  vector<sip_data_list_result::entry>::iterator fetch_iter;

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
  RGWCoroutine* sync_single_entry(std::shared_ptr<RGWDataSyncInfoCRHandler>& sip,
                                  const rgw_bucket_shard& src,
                                  const std::string& key,
                                  const std::string& marker,
                                  ceph::real_time timestamp, bool retry) {
    auto state = bucket_shard_cache->get(src);
    auto obligation = rgw_data_sync_obligation{key, marker, timestamp, retry};
    return new RGWDataSyncSingleEntryCR(sc, sip, shard_id, std::move(state), std::move(obligation),
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

  int operate(const DoutPrefixProvider *dpp) override {
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
            marker_tracker->try_update_high_marker(iter->first, std::nullopt, 0, entry_timestamp);
            continue;
          }
          tn->log(20, SSTR("full sync: " << iter->first));
          total_entries++;
          if (!marker_tracker->start(iter->first, std::nullopt, total_entries, entry_timestamp)) {
            tn->log(0, SSTR("ERROR: cannot start syncing " << iter->first << ". Duplicate entry?"));
          } else {
            // fetch remote and write locally
            std::shared_ptr<RGWDataSyncInfoCRHandler> no_sip; /* nullptr */
            spawn(sync_single_entry(no_sip,
                                    source_bs, iter->first, iter->first,
                                    entry_timestamp, false), false);
          }
          sync_marker.marker = iter->first;

          drain_all_but_stack_cb(lease_stack.get(),
                                 [&](uint64_t stack_id, int ret) {
                                   if (ret < 0) {
                                     tn->log(10, "a sync operation returned error");
                                   }
                                 });
        }
      } while (omapvals->more);
      omapvals.reset();

      drain_all_but_stack(lease_stack.get());

      tn->unset_flag(RGW_SNS_FLAG_ACTIVE);

      yield {
        /* update marker to reflect we're done with full sync */
        sync_marker.state = rgw_data_sync_marker::IncrementalSync;
        sync_marker.marker = sync_marker.next_step_marker;
        sync_marker.sip_name = sync_marker.next_sip_name;
        sync_marker.next_step_marker.clear();
        sync_marker.next_sip_name.clear();
        call(new RGWSimpleRadosWriteCR<rgw_data_sync_marker>(dpp, sync_env->async_rados, sync_env->svc->sysobj,
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
          spawn(sync_single_entry(sc->dsi.inc,
                                  source_bs, *modified_iter, string(),
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
            spawn(sync_single_entry(sc->dsi.inc,
                                    source_bs, error_marker, "",
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
        yield call(sc->dsi.inc->fetch_cr(shard_id, sync_marker.marker,
                                         &fetch_result));
        if (retcode < 0 && retcode != -ENOENT) {
          tn->log(0, SSTR("ERROR: failed to read remote data log info: ret=" << retcode));
          lease_cr->go_down();
          drain_all();
          return set_cr_error(retcode);
        }

        if (fetch_result.entries.size() > 0) {
          tn->set_flag(RGW_SNS_FLAG_ACTIVE); /* actually have entries to sync */
        }

        for (fetch_iter = fetch_result.entries.begin(); fetch_iter != fetch_result.entries.end(); ++fetch_iter) {
          tn->log(20, SSTR("shard_id=" << shard_id << " log_entry: " << fetch_iter->entry_id << ":" << fetch_iter->timestamp << ":" << fetch_iter->key));
          if (!marker_tracker->start(fetch_iter->entry_id, std::nullopt, 0, fetch_iter->timestamp)) {
            tn->log(0, SSTR("ERROR: cannot start syncing " << fetch_iter->entry_id << ". Duplicate entry?"));
          } else {
            source_bs = rgw_bucket_shard{fetch_iter->bucket, fetch_iter->shard_id};
            spawn(sync_single_entry(sc->dsi.inc,
                                    source_bs, fetch_iter->key, fetch_iter->entry_id,
                                    fetch_iter->timestamp, false), false);
          }

          drain_all_but_stack_cb(lease_stack.get(),
                                 [&](uint64_t stack_id, int ret) {
                                   if (ret < 0) {
                                     tn->log(10, "a sync operation returned error");
                                   }
                                 });
        }

        tn->log(20, SSTR("shard_id=" << shard_id << " sync_marker=" << sync_marker.marker
                         << " fetch_result.marker=" << fetch_result.marker << " truncated=" << fetch_result.truncated));
        if (!fetch_result.marker.empty()) {
          sync_marker.marker = fetch_result.marker;
        } else if (!fetch_result.entries.empty()) {
          sync_marker.marker = fetch_result.entries.back().entry_id;
        }
        sync_marker.sip_name = sc->dsi.inc->get_sip_name();
        if (!fetch_result.truncated) {
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
    return new RGWSimpleRadosReadCR<rgw_data_sync_marker>(sync_env->dpp, sync_env->async_rados, sync_env->svc->sysobj,
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
  uint32_t num_shards{0};

  rgw_data_sync_status sync_status;

  ceph::mutex shard_crs_lock =
    ceph::make_mutex("RGWDataSyncCR::shard_crs_lock");
  map<int, RGWDataSyncShardControlCR *> shard_crs;

  bool *reset_backoff;

  RGWSyncTraceNodeRef tn;

  RGWDataSyncModule *data_sync_module{nullptr};
public:
  RGWDataSyncCR(RGWDataSyncCtx *_sc, RGWSyncTraceNodeRef& _tn, bool *_reset_backoff) : RGWCoroutine(_sc->cct),
                                                      sc(_sc), sync_env(_sc->env),
                                                      reset_backoff(_reset_backoff), tn(_tn) {

  }

  ~RGWDataSyncCR() override {
    for (auto iter : shard_crs) {
      iter.second->put();
    }
  }

  int operate(const DoutPrefixProvider *dpp) override {
    reenter(this) {

      /* read sync status */
      yield call(new RGWReadDataSyncStatusCoroutine(sc, &sync_status));

      data_sync_module = sync_env->sync_module->get_data_handler();

      if (retcode < 0 && retcode != -ENOENT) {
        tn->log(0, SSTR("ERROR: failed to fetch sync status, retcode=" << retcode));
        return set_cr_error(retcode);
      }

      yield call(sc->dsi.full->init_cr(tn));
      if (retcode < 0) {
        tn->log(0, SSTR("ERROR: failed to init full sync dsi, retcode=" << retcode));
        return set_cr_error(retcode);
      }

      yield call(sc->dsi.inc->init_cr(tn));
      if (retcode < 0) {
        tn->log(0, SSTR("ERROR: failed to init inc sync dsi, retcode=" << retcode));
        return set_cr_error(retcode);
      }

      num_shards = sc->dsi.inc->num_shards();

      /* state: init status */
      if ((rgw_data_sync_info::SyncState)sync_status.sync_info.state == rgw_data_sync_info::StateInit) {
        tn->log(20, SSTR("init"));
        uint64_t instance_id;
        instance_id = ceph::util::generate_random_number<uint64_t>();
        yield call(new RGWInitDataSyncStatusCoroutine(sc, instance_id, tn, &sync_status));
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
        yield call(data_sync_module->init_sync(dpp, sc));
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

      yield call(data_sync_module->start_sync(dpp, sc));
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
    return new RGWSimpleRadosWriteCR<rgw_data_sync_info>(sync_env->dpp, sync_env->async_rados, sync_env->svc->sysobj,
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

  RGWCoroutine *sync_object(const DoutPrefixProvider *dpp, RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key, std::optional<uint64_t> versioned_epoch, rgw_zone_set *zones_trace) override;
  RGWCoroutine *remove_object(const DoutPrefixProvider *dpp, RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key, real_time& mtime, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override;
  RGWCoroutine *create_delete_marker(const DoutPrefixProvider *dpp, RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key, real_time& mtime,
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

int RGWDefaultSyncModule::create_instance(const DoutPrefixProvider *dpp, CephContext *cct, const JSONFormattable& config, RGWSyncModuleInstanceRef *instance)
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
    int operate(const DoutPrefixProvider *dpp) override {
      auto user_ctl = sync_env->store->getRados()->ctl.user;

      ret = user_ctl->get_info_by_uid(sync_env->dpp, uid, &info->user_info, null_yield);
      if (ret < 0) {
        return ret;
      }

      info->identity = rgw::auth::transform_old_authinfo(sync_env->cct,
                                                         uid,
                                                         RGW_PERM_FULL_CONTROL,
                                                         false, /* system_request? */
                                                         TYPE_RGW);

      map<string, bufferlist> uattrs;

      ret = user_ctl->get_attrs_by_uid(sync_env->dpp, uid, &uattrs, null_yield);
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

  /* no acls to object, so set owner as the destination bucket owner */
  if (obj_attrs.find(RGW_ATTR_ACL) == obj_attrs.end()) {
    *poverride_owner = dest_bucket_info.owner;
  }

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


  int operate(const DoutPrefixProvider *dpp) override {
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
                                       zones_trace, sync_env->counters, dpp));
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

RGWCoroutine *RGWDefaultDataSyncModule::sync_object(const DoutPrefixProvider *dpp, RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key, std::optional<uint64_t> versioned_epoch, rgw_zone_set *zones_trace)
{
  return new RGWObjFetchCR(sc, sync_pipe, key, std::nullopt, versioned_epoch, zones_trace);
}

RGWCoroutine *RGWDefaultDataSyncModule::remove_object(const DoutPrefixProvider *dpp, RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key,
                                                      real_time& mtime, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace)
{
  auto sync_env = sc->env;
  return new RGWRemoveObjCR(sync_env->dpp, sync_env->async_rados, sync_env->store, sc->source_zone,
                            sync_pipe.dest_bucket_info, key, versioned, versioned_epoch,
                            NULL, NULL, false, &mtime, zones_trace);
}

RGWCoroutine *RGWDefaultDataSyncModule::create_delete_marker(const DoutPrefixProvider *dpp, RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key, real_time& mtime,
                                                             rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace)
{
  auto sync_env = sc->env;
  return new RGWRemoveObjCR(sync_env->dpp, sync_env->async_rados, sync_env->store, sc->source_zone,
                            sync_pipe.dest_bucket_info, key, versioned, versioned_epoch,
                            &owner.id, &owner.display_name, true, &mtime, zones_trace);
}

class RGWArchiveDataSyncModule : public RGWDefaultDataSyncModule {
public:
  RGWArchiveDataSyncModule() {}

  RGWCoroutine *sync_object(const DoutPrefixProvider *dpp, RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key, std::optional<uint64_t> versioned_epoch, rgw_zone_set *zones_trace) override;
  RGWCoroutine *remove_object(const DoutPrefixProvider *dpp, RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key, real_time& mtime, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override;
  RGWCoroutine *create_delete_marker(const DoutPrefixProvider *dpp, RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key, real_time& mtime,
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

int RGWArchiveSyncModule::create_instance(const DoutPrefixProvider *dpp, CephContext *cct, const JSONFormattable& config, RGWSyncModuleInstanceRef *instance)
{
  instance->reset(new RGWArchiveSyncModuleInstance());
  return 0;
}

RGWCoroutine *RGWArchiveDataSyncModule::sync_object(const DoutPrefixProvider *dpp, RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key, std::optional<uint64_t> versioned_epoch, rgw_zone_set *zones_trace)
{
  auto sync_env = sc->env;
  ldout(sc->cct, 5) << "SYNC_ARCHIVE: sync_object: b=" << sync_pipe.info.source_bs.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch.value_or(0) << dendl;
  if (!sync_pipe.dest_bucket_info.versioned() ||
     (sync_pipe.dest_bucket_info.flags & BUCKET_VERSIONS_SUSPENDED)) {
      ldout(sc->cct, 0) << "SYNC_ARCHIVE: sync_object: enabling object versioning for archive bucket" << dendl;
      sync_pipe.dest_bucket_info.flags = (sync_pipe.dest_bucket_info.flags & ~BUCKET_VERSIONS_SUSPENDED) | BUCKET_VERSIONED;
      int op_ret = sync_env->store->getRados()->put_bucket_instance_info(sync_pipe.dest_bucket_info, false, real_time(), NULL, sync_env->dpp);
      if (op_ret < 0) {
         ldpp_dout(sync_env->dpp, 0) << "SYNC_ARCHIVE: sync_object: error versioning archive bucket" << dendl;
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

RGWCoroutine *RGWArchiveDataSyncModule::remove_object(const DoutPrefixProvider *dpp, RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key,
                                                     real_time& mtime, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace)
{
  ldout(sc->cct, 0) << "SYNC_ARCHIVE: remove_object: b=" << sync_pipe.info.source_bs.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch << dendl;
  return NULL;
}

RGWCoroutine *RGWArchiveDataSyncModule::create_delete_marker(const DoutPrefixProvider *dpp, RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key, real_time& mtime,
                                                            rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace)
{
  ldout(sc->cct, 0) << "SYNC_ARCHIVE: create_delete_marker: b=" << sync_pipe.info.source_bs.bucket << " k=" << key << " mtime=" << mtime
	                            << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
  auto sync_env = sc->env;
  return new RGWRemoveObjCR(sync_env->dpp, sync_env->async_rados, sync_env->store, sc->source_zone,
                            sync_pipe.dest_bucket_info, key, versioned, versioned_epoch,
                            &owner.id, &owner.display_name, true, &mtime, zones_trace);
}

class RGWDataSyncControlCR : public RGWBackoffControlCR
{
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;

  RGWSyncTraceNodeRef tn;

  static constexpr bool exit_on_error = false; // retry on all errors
public:
  RGWDataSyncControlCR(RGWDataSyncCtx *_sc,
                       RGWSyncTraceNodeRef& _tn_parent) : RGWBackoffControlCR(_sc->cct, exit_on_error),
                                                          sc(_sc), sync_env(_sc->env) {
    tn = sync_env->sync_tracer->add_node(_tn_parent, "sync");
  }

  RGWCoroutine *alloc_cr() override {
    return new RGWDataSyncCR(sc, tn, backoff_ptr());
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

int RGWRemoteDataLog::run_sync(const DoutPrefixProvider *dpp)
{
  lock.lock();
  data_sync_cr = new RGWDataSyncControlCR(&sc, tn);
  data_sync_cr->get(); // run() will drop a ref, so take another
  lock.unlock();

  int r = run(dpp, data_sync_cr);

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

int RGWDataSyncStatusManager::init(const DoutPrefixProvider *dpp)
{
  RGWZone *zone_def{nullptr};

  if (!store->svc()->zone->find_zone(source_zone, &zone_def)) {
    /* zone not found, let's make sure it's a data provider */
    RGWDataProvider *dp;
    if (!store->svc()->zone->find_data_provider(source_zone, &dp)) {
      ldpp_dout(this, 0) << "ERROR: failed to find zone config info for zone=" << source_zone << dendl;
      return -EIO;
    }
  } else { /* it is a zone */
    if (!store->svc()->sync_modules->get_manager()->supports_data_export(zone_def->tier_type)) {
      return -ENOTSUP;
    }
  }

  if (sync_module == nullptr) { 
    sync_module = store->get_sync_module();
  }

  auto opt_conns = store->ctl()->remote->zone_conns(source_zone);
  if (!opt_conns) {
    ldpp_dout(this, 0) << "connection object to zone " << source_zone << " does not exist" << dendl;
    return -EINVAL;
  }
  conns = *opt_conns;

  error_logger = new RGWSyncErrorLogger(store, RGW_SYNC_ERROR_LOG_SHARD_PREFIX, ERROR_LOGGER_SHARDS);

  int r = source_log.init(source_zone, conns, error_logger, store->getRados()->get_sync_tracer(),
                          sync_module, counters);
  if (r < 0) {
    ldpp_dout(this, 0) << "ERROR: failed to init remote log, r=" << r << dendl;
    finalize();
    return r;
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

  rgw_bilog_marker_info bilog_info;
  rgw_bucket_index_marker_info *info;
  bool *disabled;

public:
  RGWReadRemoteBucketIndexLogInfoCR(RGWDataSyncCtx *_sc,
                                  const rgw_bucket_shard& bs,
                                  rgw_bucket_index_marker_info *_info,
                                  bool *_disabled)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
      instance_key(bs.get_key()), info(_info),
      disabled(_disabled) {}

  int operate(const DoutPrefixProvider *dpp) override {
    reenter(this) {
      yield {
        rgw_http_param_pair pairs[] = { { "type" , "bucket-index" },
	                                { "bucket-instance", instance_key.c_str() },
					{ "info" , NULL },
	                                { NULL, NULL } };

        string p = "/admin/log/";
        call(new RGWReadRESTResourceCR<rgw_bilog_marker_info>(sync_env->cct, sc->conns.data, sync_env->http_manager, p, pairs, &bilog_info));
      }
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      info->marker = bilog_info.max_marker;
      info->timestamp = ceph::real_time(); /* we don't get this info */
      *disabled = bilog_info.syncstopped;
      return set_cr_done();
    }
    return 0;
  }
};


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

class RGWBucketPipeSyncInfoCRHandler_Legacy_Base : public RGWBucketPipeSyncInfoCRHandler {
  friend class InitCR;

  rgw_bucket source_bucket;
  RGWBucketInfo source_bucket_info; /* legacy only supports buckets with bucket info,
                                       and that's where we get num_shards from */
  class InitCR : public RGWCoroutine {
    RGWBucketPipeSyncInfoCRHandler_Legacy_Base *caller;
    RGWDataSyncCtx *sc;
    RGWSyncTraceNodeRef tn;

  public:
    InitCR(RGWBucketPipeSyncInfoCRHandler_Legacy_Base *_caller,
           RGWDataSyncCtx *_sc,
           RGWSyncTraceNodeRef& _tn) : RGWCoroutine(_sc->cct),
                                       caller(_caller),
                                       sc(_sc),
                                       tn(_tn) {}

    int operate() {
      reenter(this) {
        yield call(new RGWSyncGetBucketInfoCR(sc->env,
                                              caller->source_bucket,
                                              &caller->source_bucket_info,
                                              nullptr,
                                              tn));
        if (retcode < 0) {
          ldout(cct, 0) << "ERROR: RGWSyncGetBucketInfoCR: failed to get bucket info for bucket=" << caller->source_bucket <<": ret=" << retcode << dendl;
          return set_cr_error(retcode);
        }
        return set_cr_done();
      }
      return 0;
    }
  };

public:
  RGWBucketPipeSyncInfoCRHandler_Legacy_Base(RGWDataSyncCtx *_sc, const rgw_bucket& _source_bucket) : RGWBucketPipeSyncInfoCRHandler(_sc),
                                                                                                      source_bucket(_source_bucket) {}
  RGWCoroutine *init_cr(RGWSyncTraceNodeRef& tn) override {
    return new InitCR(this, sc, tn);
  }

  int num_shards() const override {
    return source_bucket_info.layout.current_index.layout.normal.num_shards;
  }
};

class RGWBucketFullSyncInfoCRHandler_Legacy : public RGWBucketPipeSyncInfoCRHandler_Legacy_Base {
  rgw_bucket source_bucket;
public:
  RGWBucketFullSyncInfoCRHandler_Legacy(RGWDataSyncCtx *_sc, const rgw_bucket& _source_bucket) : RGWBucketPipeSyncInfoCRHandler_Legacy_Base(_sc, _source_bucket),
                                                                                                 source_bucket(_source_bucket) {}

  string get_sip_name() const override {
    return "legacy/bucket.full";
  }

  RGWCoroutine *get_pos_cr(int shard_id, rgw_bucket_index_marker_info *info, bool *disabled) override {
    return nullptr;
  }

  RGWCoroutine *fetch_cr(int shard_id,
                         const string& list_marker,
                         sip_bucket_fetch_result *result) override;
};

class RGWBucketIncSyncInfoCRHandler_Legacy : public RGWBucketPipeSyncInfoCRHandler_Legacy_Base {
  rgw_bucket source_bucket;
public:
  RGWBucketIncSyncInfoCRHandler_Legacy(RGWDataSyncCtx *_sc,
                                       const rgw_bucket& _source_bucket) : RGWBucketPipeSyncInfoCRHandler_Legacy_Base(_sc, _source_bucket),
                                                                           source_bucket(_source_bucket) {}

  string get_sip_name() const override {
    return "legacy/bucket.inc";
  }

  RGWCoroutine *get_pos_cr(int shard_id, rgw_bucket_index_marker_info *info, bool *disabled) override {
    rgw_bucket_shard source_bs(source_bucket, shard_id);
    return new RGWReadRemoteBucketIndexLogInfoCR(sc, source_bs, info, disabled);
  }

  RGWCoroutine *fetch_cr(int shard_id,
                         const string& marker,
                         sip_bucket_fetch_result *result) override;
};

class RGWBucketSyncInfoCRHandler_SIP_Base : public RGWSyncInfoCRHandler_SIP<sip_bucket_fetch_result, rgw_bucket_index_marker_info>,
                                            public RGWBucketPipeSyncInfoCRHandler
{
  SIProvider::TypeHandlerProviderRef type_provider;

protected:
  int handle_fetched_entry(const SIProvider::stage_id_t& sid, SIProvider::Entry& fetched_entry, sip_bucket_fetch_result::entry *pe) override {
    sip_bucket_fetch_result::entry& e = *pe;

    int r = type_handler->handle_entry(sid, fetched_entry, [&](SIProvider::EntryInfoBase& _info) {
      auto& info = static_cast<siprovider_bucket_entry_info&>(_info);
      auto& le = info.info;

      e.id = fetched_entry.key;

      if (!e.key.set(rgw_obj_index_key{le.object, le.instance})) {
        ldout(cct, 0) << "ERROR: parse_raw_oid() on " << le.object << " returned false, skipping entry" << dendl;
        return -EINVAL;
      }
      e.mtime = le.timestamp;
      e.versioned_epoch = le.versioned_epoch;
      e.is_versioned = (!!e.versioned_epoch) || (!e.key.instance.empty());
      e.op = siprovider_bucket_entry_info::Info::from_sip_op(le.op, e.versioned_epoch);
      e.state = (le.complete ? CLS_RGW_STATE_COMPLETE : CLS_RGW_STATE_PENDING_MODIFY);
      e.tag = le.instance_tag;
      e.owner = rgw_bucket_entry_owner(le.owner, le.owner_display_name);

      for (auto& ste : le.sync_trace) {
        rgw_zone_set_entry zse(ste);
        e.zones_trace.insert(std::move(zse));
      }

      return 0;
    });

    return r;
  }

public:
  RGWBucketSyncInfoCRHandler_SIP_Base(RGWDataSyncCtx *_sc,
                                      const rgw_bucket& source_bucket,
                                      SIProvider::StageType _stage_type) : RGWSyncInfoCRHandler_SIP(_sc->cct,
                                                                                                    "bucket",
                                                                                                    _stage_type),
                                                                           RGWBucketPipeSyncInfoCRHandler(_sc) {
    type_provider = std::make_shared<SITypeHandlerProvider_Default<siprovider_bucket_entry_info> >();
    sip_init(std::make_unique<SIProviderCRMgr_REST>(_sc->cct,
                                                    _sc->conns.sip,
                                                    _sc->env->http_manager,
                                                    data_type,
                                                    stage_type,
                                                    type_provider.get(),
                                                    source_bucket.get_key()));
  }
};

class RGWBucketSyncInfoCRHandler_SIP : public RGWBucketSyncInfoCRHandler_SIP_Base {
public:
  RGWBucketSyncInfoCRHandler_SIP(RGWDataSyncCtx *_sc,
                                 const rgw_bucket& _source_bucket,
                                 SIProvider::StageType _stage_type) : RGWBucketSyncInfoCRHandler_SIP_Base(_sc, _source_bucket, _stage_type) {
  }

  RGWCoroutine *get_pos_cr(int shard_id, rgw_bucket_index_marker_info *pos, bool *disabled) override {
    return sip->get_cur_state_cr(sid, shard_id, pos, disabled);
  }

  RGWCoroutine *update_marker_cr(int shard_id,
                                 const RGWSI_SIP_Marker::SetParams& params) override {
    return sip->update_marker_cr(sid, shard_id, params);
  }
};

class RGWBucketSyncStatusCRHandler_Legacy : public RGWBucketPipeSyncStatusCRHandler {
  string sync_status_oid;
  rgw_raw_obj status_obj;

  RGWObjVersionTracker objv_tracker;

  class ReadSyncStatusCR : public RGWCoroutine {
    RGWDataSyncCtx *sc;
    RGWDataSyncEnv *sync_env;
    rgw_raw_obj& status_obj;
    rgw_bucket_shard_sync_info *status;
    RGWObjVersionTracker* objv_tracker;
    map<string, bufferlist> attrs;
  public:
    ReadSyncStatusCR(RGWDataSyncCtx *_sc,
                     rgw_raw_obj& _status_obj,
                     rgw_bucket_shard_sync_info *_status,
                     RGWObjVersionTracker* objv_tracker) : RGWCoroutine(_sc->cct),
                                                          sc(_sc),
                                                          sync_env(_sc->env),
                                                          status_obj(_status_obj),
                                                          status(_status),
                                                          objv_tracker(objv_tracker) {}
    int operate(const DoutPrefixProvider *dpp) {
      reenter(this) {
        yield call(new RGWSimpleRadosReadAttrsCR(dpp, sync_env->async_rados, sync_env->svc->sysobj,
                                                 status_obj,
                                                 &attrs, true, objv_tracker));
        if (retcode == -ENOENT) {
          *status = rgw_bucket_shard_sync_info();
          return set_cr_done();
        }
        if (retcode < 0) {
          ldpp_dout(dpp, 0) << "ERROR: failed to call fetch bucket shard info obj=" << status_obj << " ret=" << retcode << dendl;
          return set_cr_error(retcode);
        }
        status->decode_from_attrs(sync_env->cct, attrs);
        return set_cr_done();
      }
      return 0;
    }
  };

public:

  RGWBucketSyncStatusCRHandler_Legacy(RGWDataSyncCtx *_sc,
                                  const rgw_bucket_sync_pair_info& _sync_pair) : RGWBucketPipeSyncStatusCRHandler(_sc, _sync_pair),
    sync_status_oid(RGWBucketPipeSyncStatusManager::status_oid(sc->source_zone, _sync_pair)),
    status_obj(sync_env->svc->zone->get_zone_params().log_pool, sync_status_oid) {}

  RGWCoroutine *read_status_cr(rgw_bucket_shard_sync_info *status) override {
    return new ReadSyncStatusCR(sc, status_obj, status, &objv_tracker);
  }

  RGWCoroutine *write_status_cr(const DoutPrefixProvider *dpp, const rgw_bucket_shard_sync_info& status) override {
    map<string, bufferlist> attrs;
    status.encode_dirty_attrs(attrs);
    return new RGWSimpleRadosWriteAttrsCR(dpp, sync_env->async_rados, sync_env->svc->sysobj, status_obj, attrs, &objv_tracker);
  }

  RGWCoroutine *remove_status_cr() override {
    return new RGWRadosRemoveCR(sync_env->store, status_obj, &objv_tracker);
  }
};

class RGWBucketShardSIPCRHandlersRepo {
  RGWDataSyncCtx *sc;
  rgw_bucket bucket;

  using SIPType = std::pair<SIProvider::StageType, bool>; /* stage type + legacy flag */

  map<SIPType, std::shared_ptr<RGWBucketPipeSyncInfoCRHandler> > handlers;

  std::shared_ptr<RGWBucketPipeSyncInfoCRHandler> create_handler(SIProvider::StageType stage_type,
                                                                 bool legacy) {

    std::shared_ptr<RGWBucketPipeSyncInfoCRHandler> result;
    if (!legacy) {
      result.reset(new RGWBucketSyncInfoCRHandler_SIP(sc, bucket, stage_type));
    } else {
      if (stage_type == SIProvider::StageType::FULL) {
        result.reset(new RGWBucketFullSyncInfoCRHandler_Legacy(sc, bucket));
      } else { /* only incremental is left */
        result.reset(new RGWBucketIncSyncInfoCRHandler_Legacy(sc, bucket));
      }
    }

    return result;
  }

public:
  RGWBucketShardSIPCRHandlersRepo(RGWDataSyncCtx *_sc,
                                  const rgw_bucket& _bucket) : sc(_sc),
                                                               bucket(_bucket) {}

  std::shared_ptr<RGWBucketPipeSyncInfoCRHandler> get(SIProvider::StageType stage_type, bool legacy) {
    auto sip_type = SIPType(stage_type, legacy);
    auto iter = handlers.find(sip_type);
    if (iter != handlers.end()) {
      return iter->second;
    }

    auto result = create_handler(stage_type, legacy);

    handlers[sip_type] = result;

    return result;
  }
};


class RGWGroupCallCR : public RGWCoroutine
{
public:
  struct State {
    enum {
      STATE_INIT = 0,
      STATE_RUNNING = 1,
      STATE_COMPLETE = 2,
    } state{STATE_INIT};

    set<RGWCoroutine *> waiters;
    RGWCoroutine *cr{nullptr};

    ~State() {
      if (cr) {
        cr->put();
      }
    }

    int result{0};

    template <class T>
    void maybe_init(T alloc_cr) {
      if (state == STATE_INIT) {
        cr = alloc_cr();
        if (cr) {
          cr->get();
        }
      }
    }

    void start() {
      state = State::STATE_RUNNING;
    }

    void complete(int retcode) {
      state = State::STATE_COMPLETE;
      result = retcode;
      if (cr) {
        cr->put();
        cr = nullptr;
      }
      for (auto& w : waiters) {
        w->wakeup();
      }
    }

    bool is_init() const {
      return state == STATE_INIT;
    }

    bool is_running() const {
      return state == STATE_RUNNING;
    }

    bool is_complete(int *retcode) const {
      *retcode = result;
      return state == STATE_COMPLETE;
    }
  };


private:
  RGWCoroutine *cr;

  State& group;

public:
  RGWGroupCallCR(CephContext *cct,
                 State& _group) : RGWCoroutine(cct),
                                  group(_group) {}
  int operate() override {
    reenter(this) {
      if (group.is_init()) {
        group.start();
        yield call(group.cr);
        group.complete(retcode);

        if (retcode < 0) {
          return set_cr_error(retcode);
        }
        return set_cr_done();
      }

      while (group.is_running()) {
        group.waiters.insert(this);
        set_sleeping(true);
        yield;
      }

      if (!group.is_complete(&retcode)) {
        /* shouldn't happen! */
        ldout(cct, 0) << "ERROR: " << __func__ << "(): group is not complete, likely a bug" << dendl;
        return set_cr_error(-EIO);
      }

      if (retcode < 0) {
        return set_cr_error(retcode);
      }

      return set_cr_done();
    }

    return 0;
  }
};

class RGWBucketShardSIPCRWrapperCore
{
  friend class InitCR;

  RGWDataSyncCtx *sc;
  rgw_bucket source_bucket;

  std::shared_ptr<RGWBucketShardSIPCRHandlersRepo> handlers_repo;

  std::shared_ptr<RGWBucketPipeSyncInfoCRHandler> handler;

  using SIPType = std::pair<SIProvider::StageType, bool>; /* stage type + legacy flag */

  std::map<SIPType, RGWGroupCallCR::State> groups;

  class InitCR : public RGWCoroutine {
    RGWBucketShardSIPCRWrapperCore *caller;
    SIProvider::StageType stage_type;
    bool legacy_fallback;
    bool legacy{false};
    RGWSyncTraceNodeRef tn;

    RGWGroupCallCR::State *group{nullptr};

    RGWCoroutine *cr;

    int i;

  public:
    InitCR(RGWBucketShardSIPCRWrapperCore *_caller,
           SIProvider::StageType _stage_type,
           bool _legacy_fallback,
           RGWSyncTraceNodeRef& _tn) : RGWCoroutine(_caller->sc->cct),
                                       caller(_caller),
                                       stage_type(_stage_type),
                                       legacy_fallback(_legacy_fallback),
                                       tn(_tn) {}

    int operate() {
      reenter(this) {

        for (i = 0; i < 2; ++i) {
          caller->handler = caller->handlers_repo->get(stage_type, legacy);
          if (!caller->handler) {
            return set_cr_error(-ENOENT);
          }

          group = &caller->groups[SIPType(stage_type, legacy)];

          if (!group->is_complete(&retcode)) {
            group->maybe_init([&]{
              return caller->handler->init_cr(tn);
            });
            yield call(new RGWGroupCallCR(cct,
                                          *group));
          }
          if (retcode >= 0) {
            return set_cr_done();
          }

          if (retcode != -ENOTSUP || !legacy_fallback) {
            ldout(cct, 0) << "ERROR: failed to initialize bucket sip (stage_type=" << SIProvider::stage_type_to_str(stage_type) << (legacy ? " [legacy]" : "") << " : ret=" << retcode << dendl;
            return set_cr_error(retcode);
          }
          ldout(cct, 0) << "WARNING: failed to initialize bucket sip (stage_type=" << SIProvider::stage_type_to_str(stage_type) << (legacy ? " [legacy]" : "") << " : ret=" << retcode << ", trying fallback to legacy sip" << dendl;

          legacy = true;
        }

        return set_cr_done();
      }
      return 0;
    }
  };
  

public:
  RGWBucketShardSIPCRWrapperCore(RGWDataSyncCtx *_sc,
                                 const rgw_bucket& _source_bucket,
                                 std::shared_ptr<RGWBucketShardSIPCRHandlersRepo> _handlers_repo) : sc(_sc),
                                                                                                    source_bucket(_source_bucket),
                                                                                                    handlers_repo(_handlers_repo) {}

  RGWCoroutine *init_cr(SIProvider::StageType stage_type, bool legacy_fallback,
                        RGWSyncTraceNodeRef& tn) {
    return new InitCR(this, stage_type, legacy_fallback, tn);
  }

  int num_shards() const {
    return handler->num_shards();
  }

  RGWCoroutine *get_pos_cr(int shard_id, rgw_bucket_index_marker_info *info, bool *disabled) {
    return handler->get_pos_cr(shard_id, info, disabled);
  }

  RGWCoroutine *fetch_cr(int shard_id,
                         const string& list_marker,
                         sip_bucket_fetch_result *result) {
    return handler->fetch_cr(shard_id, list_marker, result);
  }

  RGWCoroutine *update_marker_cr(int shard_id, const RGWSI_SIP_Marker::SetParams& params) {
    return handler->update_marker_cr(shard_id, params);
  }
};

class RGWBucketShardSIPCRWrapper
{
  std::shared_ptr<RGWBucketShardSIPCRWrapperCore> wrapper_core;
  int shard_id;

public:
  RGWBucketShardSIPCRWrapper(std::shared_ptr<RGWBucketShardSIPCRWrapperCore>& _wrapper_core,
                             int _shard_id) : wrapper_core(_wrapper_core),
                                              shard_id(_shard_id) {}

  RGWCoroutine *get_pos_cr(rgw_bucket_index_marker_info *info, bool *disabled) {
    return wrapper_core->get_pos_cr(shard_id, info, disabled);
  }

  RGWCoroutine *fetch_cr(const string& list_marker,
                         sip_bucket_fetch_result *result) {
    return wrapper_core->fetch_cr(shard_id, list_marker, result);
  }

  RGWCoroutine *update_marker_cr(const RGWSI_SIP_Marker::SetParams& params) {
    return wrapper_core->update_marker_cr(shard_id, params);
  }
};

class RGWInitBucketShardSyncStatusCoroutine : public RGWCoroutine {
  RGWBucketSyncCtx *bsc;

  rgw_bucket_shard_sync_info& status;
  rgw_bucket_index_marker_info info;
  bool syncstopped{false};

  string target_id;

public:
  RGWInitBucketShardSyncStatusCoroutine(RGWBucketSyncCtx *_bsc,
                                        rgw_bucket_shard_sync_info& _status)
    : RGWCoroutine(_bsc->cct), bsc(_bsc), status(_status)
  {
    target_id = RGWSI_SIP_Marker::create_target_id(bsc->env->svc->zone->get_zone().id,
                                                   bsc->sync_pair.dest_bs.bucket.get_key());
  }

  int operate(const DoutPrefixProvider *dpp) override {
    reenter(this) {
      /* fetch current position in logs */
      yield call(bsc->hsi.inc->get_pos_cr(&info, &syncstopped));
      if (retcode < 0 && retcode != -ENOENT) {
        ldout(cct, 0) << "ERROR: failed to fetch bucket index status" << dendl;
        return set_cr_error(retcode);
      }

      if (!syncstopped) {
        /* init remote status markers */
        yield call(bsc->hsi.inc->update_marker_cr({ target_id,
                                                    info.marker,
                                                    info.timestamp,
                                                    false }));
        if (retcode < 0) {
          ldout(cct, 0) << "WARNING: failed to update remote sync status markers" << dendl;

          /* not erroring out, should we? */
        }
      }

      yield {
        const bool stopped = status.state == rgw_bucket_shard_sync_info::StateStopped;
        bool write_status = false;

        if (syncstopped) {
          if (stopped && !bsc->env->sync_module->should_full_sync()) {
            // preserve our current incremental marker position
            write_status = true;
          }
        } else {
          // whether or not to do full sync, incremental sync will follow anyway
          if (bsc->env->sync_module->should_full_sync()) {
            status.state = rgw_bucket_shard_sync_info::StateFullSync;
            status.inc_marker.modify().position = info.marker;
          } else {
            // clear the marker position unless we're resuming from SYNCSTOP
            if (!stopped) {
              status.inc_marker.modify().position = "";
            }
            status.state = rgw_bucket_shard_sync_info::StateIncrementalSync;
          }
          write_status = true;
          status.inc_marker.modify().timestamp = ceph::real_clock::now();
        }

        if (write_status) {
          call(bsc->hst->write_status_cr(dpp, status));
        } else {
          call(bsc->hst->remove_status_cr());
        }
      }
      if (syncstopped) {
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
                                               const RGWRemoteCtl::Conns& _conns,
                                               const RGWBucketSyncFlowManager::pipe_handler& _flow_handler,
                                               const rgw_bucket& _source_bucket,
                                               const RGWBucketInfo& _dest_bucket_info) : dpp(_dpp),
                                                                                 sync_env(_sync_env),
                                                                                 source_zone(_source_zone),
                                                                                 conns(_conns),
                                                                                 flow_handler(_flow_handler),
                                                                                 source_bucket(_source_bucket),
                                                                                 dest_bucket_info(_dest_bucket_info),
                                                                                 dest_bucket(dest_bucket_info.bucket) {}

int RGWRemoteBucketManager::init(RGWCoroutinesManager *cr_mgr)
{
  auto& sc = _ctxs.sc;
  auto& bh = _ctxs.handlers;

  sc.init(sync_env, conns, source_zone);

  auto handlers_repo = std::make_shared<RGWBucketShardSIPCRHandlersRepo>(&sc, source_bucket);
  auto wrapper_core_full = std::make_shared<RGWBucketShardSIPCRWrapperCore>(&sc, source_bucket, handlers_repo);
  auto wrapper_core_inc = std::make_shared<RGWBucketShardSIPCRWrapperCore>(&sc, source_bucket, handlers_repo);

  auto tn = sync_env->sync_tracer->add_node(sync_env->sync_tracer->root_node, "bucket_manager.init", SSTR(source_bucket));

  int ret = cr_mgr->run(wrapper_core_full->init_cr(SIProvider::StageType::FULL, true, tn));
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "(): failed to initialize SIP core for full bucket sync: ret=" << ret << dendl;
    return ret;
  }

  ret = cr_mgr->run(wrapper_core_inc->init_cr(SIProvider::StageType::INC, true, tn));
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "(): failed to initialize SIP core for incremental bucket sync: ret=" << ret << dendl;
    return ret;
  }

  int source_num_shards = wrapper_core_inc->num_shards();

  int num_shards = source_num_shards;
  if (num_shards <= 0) {
    num_shards = 1;
  }

  bool shard_to_shard_sync = (source_num_shards == (int)dest_bucket_info.layout.current_index.layout.normal.num_shards);

  sync_pairs.resize(source_num_shards);

  int cur_shard = std::min<int>(source_num_shards, 0);

  for (int i = 0; i < num_shards; ++i, ++cur_shard) {
    auto& sync_pair = sync_pairs[i];

    sync_pair.source_bs.bucket = source_bucket;
    sync_pair.dest_bs.bucket = dest_bucket;
    sync_pair.handler = flow_handler;

    sync_pair.source_bs.shard_id = (source_num_shards > 0 ? cur_shard : -1);

    if (shard_to_shard_sync) {
      sync_pair.dest_bs.shard_id = sync_pair.source_bs.shard_id;
    } else {
      sync_pair.dest_bs.shard_id = -1;
    }
  }

  bh.resize(num_shards);
  bscs.resize(num_shards);


  for (int i = 0; i < num_shards; ++i) {
    auto& handlers = bh[i];
    handlers.info.full = std::make_shared<RGWBucketShardSIPCRWrapper>(wrapper_core_full, i);
    handlers.info.inc = std::make_shared<RGWBucketShardSIPCRWrapper>(wrapper_core_inc, i);
    handlers.status.reset(new RGWBucketSyncStatusCRHandler_Legacy(&sc, sync_pairs[i]));
    bscs[i].init(&sc, sync_pairs[i], handlers.info.full.get(), handlers.info.inc.get(), handlers.status.get());
  }

  return 0;
}

RGWRemoteBucketManager::~RGWRemoteBucketManager() {}

RGWCoroutine *RGWRemoteBucketManager::init_sync_status_cr(int num)
{
  if ((size_t)num >= sync_pairs.size()) {
    return nullptr;
  }
  return new RGWInitBucketShardSyncStatusCoroutine(&bscs[num], init_status);
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

void rgw_bucket_shard_sync_info::encode_dirty_attrs(map<string, bufferlist>& attrs) const
{
  if (state) {
    encode_state_attr(attrs);
  }
  if (full_marker) {
    full_marker->encode_attr(attrs);
  }
  if (inc_marker) {
    inc_marker->encode_attr(attrs);
  }
}

void rgw_bucket_shard_sync_info::encode_state_attr(map<string, bufferlist>& attrs) const
{
  using ceph::encode;
  encode(state, attrs[BUCKET_SYNC_ATTR_PREFIX "state"]);
}

void rgw_bucket_shard_full_sync_marker::encode_attr(map<string, bufferlist>& attrs) const
{
  using ceph::encode;
  encode(*this, attrs[BUCKET_SYNC_ATTR_PREFIX "full_marker"]);
}

void rgw_bucket_shard_inc_sync_marker::encode_attr(map<string, bufferlist>& attrs) const
{
  using ceph::encode;
  encode(*this, attrs[BUCKET_SYNC_ATTR_PREFIX "inc_marker"]);
}

#define OMAP_READ_MAX_ENTRIES 10
class RGWReadRecoveringBucketShardsCoroutine : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  rgw::sal::RadosStore* store;
  
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

  int operate(const DoutPrefixProvider *dpp) override;
};

int RGWReadRecoveringBucketShardsCoroutine::operate(const DoutPrefixProvider *dpp)
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
        ldpp_dout(dpp, 0) << "failed to read recovering bucket shards with " 
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
  rgw::sal::RadosStore* store;
  
  const int shard_id;
  int max_entries;

  set<string>& pending_buckets;
  string marker;
  string status_oid;

  rgw_data_sync_marker* sync_marker;
  int count;

  sip_data_list_result fetch_result;

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

  int operate(const DoutPrefixProvider *dpp) override;
};

int RGWReadPendingBucketShardsCoroutine::operate(const DoutPrefixProvider *dpp)
{
  reenter(this){
    //read sync status marker
    using CR = RGWSimpleRadosReadCR<rgw_data_sync_marker>;
    yield call(new CR(dpp, sync_env->async_rados, sync_env->svc->sysobj,
                      rgw_raw_obj(sync_env->svc->zone->get_zone_params().log_pool, status_oid),
                      sync_marker));
    if (retcode < 0) {
      ldpp_dout(dpp, 0) << "failed to read sync status marker with " 
        << cpp_strerror(retcode) << dendl;
      return set_cr_error(retcode);
    }

    //read pending bucket shards
    marker = sync_marker->marker;
    count = 0;
    do{
      yield call(sc->dsi.inc->fetch_cr(shard_id, marker,
                                       &fetch_result));

      if (retcode == -ENOENT) {
        break;
      }

      if (retcode < 0) {
        ldpp_dout(dpp, 0) << "failed to read remote data log info with " 
          << cpp_strerror(retcode) << dendl;
        return set_cr_error(retcode);
      }

      if (fetch_result.entries.empty()) {
        break;
      }

      count += fetch_result.entries.size();
      for (const auto& entry : fetch_result.entries) {
        pending_buckets.insert(entry.key);
      }
    } while (fetch_result.truncated && count < max_entries);

    return set_cr_done();
  }

  return 0;
}

int RGWRemoteDataLog::read_shard_status(const DoutPrefixProvider *dpp, int shard_id, set<string>& pending_buckets, set<string>& recovering_buckets, rgw_data_sync_marker *sync_marker, const int max_entries)
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
  ret = crs.run(dpp, stacks);
  http_manager.stop();
  return ret;
}

RGWCoroutine *RGWRemoteBucketManager::read_sync_status_cr(int num, rgw_bucket_shard_sync_info *sync_status)
{
  if ((size_t)num >= sync_pairs.size()) {
    return nullptr;
  }

  return bscs[num].hst->read_status_cr(sync_status);
}

RGWBucketPipeSyncStatusManager::RGWBucketPipeSyncStatusManager(rgw::sal::RadosStore* _store,
                                                               std::optional<rgw_zone_id> _source_zone,
                                                               std::optional<rgw_bucket> _source_bucket,
                                                               const rgw_bucket& _dest_bucket) : store(_store),
                                                                                   cr_mgr(_store->ctx(), _store->getRados()->get_cr_registry()),
                                                                                   http_manager(store->ctx(), cr_mgr.get_completion_mgr()),
                                                                                   source_zone(_source_zone), source_bucket(_source_bucket),
                                                                                   error_logger(NULL),
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
  std::optional<uint64_t> versioned_epoch;
  string rgw_tag;

  bucket_list_entry() : delete_marker(false), is_latest(false), size(0) {}

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
    uint64_t ve;
    JSONDecoder::decode_json("VersionedEpoch", ve, obj);
    if (ve > 0) {
      versioned_epoch = ve;
      if (key.instance.empty()) {
        key.instance = "null";
      }
    } else if (key.instance == "null") {
      key.instance.clear();
    }
    JSONDecoder::decode_json("RgwxTag", rgw_tag, obj);
  }

  RGWModifyOp get_modify_op() const {
    if (delete_marker) {
      return CLS_RGW_OP_LINK_OLH_DM;
    } else if (versioned_epoch) {
      return CLS_RGW_OP_LINK_OLH;
    } else {
      return CLS_RGW_OP_ADD;
    }
  }

  bool is_versioned() const {
    return !!versioned_epoch;
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
  string instance_key;
  string path;
  rgw_obj_key marker_key;

  sip_bucket_fetch_result *result;

  bucket_list_result list_result;

public:
  RGWListBucketShardCR(RGWDataSyncCtx *_sc, const rgw_bucket_shard& bs,
                       const string& _marker, sip_bucket_fetch_result *_result)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
      instance_key(bs.get_key()), marker_key(rgw_obj_key::from_escaped_str(_marker)),
      result(_result) {
    path = string("/") + bs.bucket.name;
  }

  int operate(const DoutPrefixProvider *dpp) override {
    reenter(this) {
      yield {
        rgw_http_param_pair pairs[] = { { "rgwx-bucket-instance", instance_key.c_str() },
					{ "versions" , NULL },
					{ "format" , "json" },
					{ "objs-container" , "true" },
					{ "key-marker" , marker_key.name.c_str() },
					{ "version-id-marker" , marker_key.instance.c_str() },
	                                { NULL, NULL } };
        // don't include tenant in the url, it's already part of instance_key
        call(new RGWReadRESTResourceCR<bucket_list_result>(sync_env->cct, sc->conns.data, sync_env->http_manager, path, pairs, &list_result));
      }
      if (retcode < 0) {
        return set_cr_error(retcode);
      }

      result->entries.clear();
      result->entries.reserve(list_result.entries.size());

      for (auto& be : list_result.entries) {

        sip_bucket_fetch_result::entry e;
        e.id = be.key.to_escaped_str();

        e.key = be.key;

        e.mtime = be.mtime;

        e.op = be.get_modify_op(); /* CLS_RGW_OP_ADD or CLS_RGW_OP_LINK_OLH or CLS_RGW_OP_LINK_OLH_DM,
                                      depending on be */
        e.is_versioned = be.is_versioned();
        e.owner = { be.owner.id, be.owner.display_name };

        e.versioned_epoch = be.versioned_epoch;

        result->entries.emplace_back(std::move(e));
      }

      if (!result->entries.empty()) {
        result->marker = result->entries.back().id;
      } else {
        result->marker.clear();
      }

      result->truncated = list_result.is_truncated;

      return set_cr_done();
    }
    return 0;
  }
};

RGWCoroutine *RGWBucketFullSyncInfoCRHandler_Legacy::fetch_cr(int shard_id,
                                                              const string& list_marker,
                                                              sip_bucket_fetch_result *result)
{
  rgw_bucket_shard source_bs(source_bucket, shard_id);
  return new RGWListBucketShardCR(sc, source_bs, list_marker, result);
}

class RGWListBucketIndexLogCR: public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  const string instance_key;
  string marker;

  sip_bucket_fetch_result *result;

  list<rgw_bi_log_entry> list_result;
  std::optional<PerfGuard> timer;

public:
  RGWListBucketIndexLogCR(RGWDataSyncCtx *_sc, const rgw_bucket_shard& bs,
                          const string& _marker, sip_bucket_fetch_result *_result)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
      instance_key(bs.get_key()), marker(_marker), result(_result) {}

  int operate(const DoutPrefixProvider *dpp) override {
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

        call(new RGWReadRESTResourceCR<list<rgw_bi_log_entry> >(sync_env->cct, sc->conns.data, sync_env->http_manager, "/admin/log", pairs, &list_result));
      }
      timer.reset();
      if (retcode < 0) {
        if (sync_env->counters) {
          sync_env->counters->inc(sync_counters::l_poll_err);
        }
        return set_cr_error(retcode);
      }

      result->entries.reserve(list_result.size());


      for (auto& le : list_result) {
        sip_bucket_fetch_result::entry e;

        e.id = le.id;
        if (!e.key.set(rgw_obj_index_key{le.object, le.instance})) {
          ldout(cct, 0) << "ERROR: parse_raw_oid() on " << le.object << " returned false, skipping entry" << dendl;
          continue;
        }
        e.mtime = le.timestamp;
        if (le.ver.pool < 0) {
          e.versioned_epoch = le.ver.epoch;
        }
        e.op = le.op;
        e.state = le.state;
        e.tag = le.tag;
        e.is_versioned = le.is_versioned();
        e.owner = rgw_bucket_entry_owner(le.owner, le.owner_display_name);
        e.zones_trace = le.zones_trace;

        result->entries.emplace_back(e);
      }

      if (!result->entries.empty()) {
        result->marker = result->entries.back().id;
      }
      result->truncated = true;

      return set_cr_done();
    }
    return 0;
  }
};

RGWCoroutine *RGWBucketIncSyncInfoCRHandler_Legacy::fetch_cr(int shard_id,
                                                             const string& marker,
                                                             sip_bucket_fetch_result *result)
{
  rgw_bucket_shard source_bs(source_bucket, shard_id);
  return new RGWListBucketIndexLogCR(sc, source_bs,
                                     marker, result);
}

#define BUCKET_SYNC_UPDATE_MARKER_WINDOW 10

class RGWBucketFullSyncShardMarkerTrack : public RGWSyncShardMarkerTrack<std::string, rgw_obj_key> {
  RGWBucketSyncCtx *bsc;

  rgw_bucket_shard_sync_info status;
  RGWSyncTraceNodeRef tn;

public:
  RGWBucketFullSyncShardMarkerTrack(RGWBucketSyncCtx *_bsc,
                                    const rgw_bucket_shard_full_sync_marker& _marker,
                                    RGWSyncTraceNodeRef tn)
    : RGWSyncShardMarkerTrack(BUCKET_SYNC_UPDATE_MARKER_WINDOW),
      bsc(_bsc), tn(std::move(tn))
  {
    status.full_marker = _marker;
  }

  RGWCoroutine *store_marker(const DoutPrefixProvider *dpp, const std::string& new_marker, const rgw_obj_key& key, uint64_t index_pos, const real_time& timestamp) override {
    auto& sync_marker = status.full_marker.modify();
    sync_marker.position = key;
    sync_marker.position_id = new_marker;
    sync_marker.count = index_pos;

    tn->log(20, SSTR("updating marker=" << new_marker));

    auto cr = bsc->hst->write_status_cr(dpp, status);

    status.full_marker.clean();

    return cr;
  }

  RGWOrderCallCR *allocate_order_control_cr() override {
    return new RGWLastCallerWinsCR(bsc->cct);
  }
};

// write the incremental sync status and update 'stable_timestamp' on success
class RGWWriteBucketShardIncSyncStatus : public RGWCoroutine {
  RGWBucketSyncCtx *bsc;
  rgw_bucket_shard_sync_info status;
  ceph::real_time* stable_timestamp;
  std::map<std::string, bufferlist> attrs;
 public:
  RGWWriteBucketShardIncSyncStatus(RGWBucketSyncCtx *bsc,
                                   const rgw_bucket_shard_inc_sync_marker& sync_marker,
                                   ceph::real_time* stable_timestamp)
    : RGWCoroutine(bsc->cct),
      bsc(bsc),
      stable_timestamp(stable_timestamp)
  {
    status.inc_marker = sync_marker;
  }

  int operate(const DoutPrefixProvider *dpp) {
    reenter(this) {
      yield call(bsc->hst->write_status_cr(dpp, status));
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      if (stable_timestamp) {
        *stable_timestamp = status.inc_marker->timestamp;
      }
      return set_cr_done();
    }
    return 0;
  }
};

class RGWBucketIncSyncShardMarkerTrack : public RGWSyncShardMarkerTrack<string, rgw_obj_key> {
  RGWBucketSyncCtx *bsc;

  rgw_bucket_shard_inc_sync_marker sync_marker;
  map<rgw_obj_key, string> key_to_marker;

  struct operation {
    rgw_obj_key key;
    bool is_olh;
  };
  map<string, operation> marker_to_op;
  std::set<std::string> pending_olh; // object names with pending olh operations

  RGWSyncTraceNodeRef tn;
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

  RGWBucketIncSyncShardMarkerTrack(RGWBucketSyncCtx *_bsc,
                         const rgw_bucket_shard_inc_sync_marker& _marker,
                         RGWSyncTraceNodeRef tn,
                         ceph::real_time* stable_timestamp)
    : RGWSyncShardMarkerTrack(BUCKET_SYNC_UPDATE_MARKER_WINDOW),
      bsc(_bsc), sync_marker(_marker), tn(std::move(tn)),
      stable_timestamp(stable_timestamp)
  {}

  RGWCoroutine *store_marker(const DoutPrefixProvider *dpp, const string& new_marker, const rgw_obj_key& key, uint64_t index_pos, const real_time& timestamp) override {
    sync_marker.position = new_marker;
    sync_marker.timestamp = timestamp;

    tn->log(20, SSTR("updating marker=" << new_marker << " timestamp=" << timestamp));
    return new RGWWriteBucketShardIncSyncStatus(bsc, sync_marker, stable_timestamp);

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
    return new RGWLastCallerWinsCR(bsc->cct);
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

  int operate(const DoutPrefixProvider *dpp) override {
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
            call(data_sync_module->sync_object(dpp, sc, sync_pipe, key, versioned_epoch, &zones_trace));
          } else if (op == CLS_RGW_OP_DEL || op == CLS_RGW_OP_UNLINK_INSTANCE) {
            set_status("removing obj");
            tn->log(10, SSTR("removing obj: " << sc->source_zone << "/" << bs.bucket << "/" << key << "[" << versioned_epoch.value_or(0) << "]"));
            call(data_sync_module->remove_object(dpp, sc, sync_pipe, key, timestamp, versioned, versioned_epoch.value_or(0), &zones_trace));
            // our copy of the object is more recent, continue as if it succeeded
          } else if (op == CLS_RGW_OP_LINK_OLH_DM) {
            set_status("creating delete marker");
            tn->log(10, SSTR("creating delete marker: obj: " << sc->source_zone << "/" << bs.bucket << "/" << key << "[" << versioned_epoch.value_or(0) << "]"));
            call(data_sync_module->create_delete_marker(dpp, sc, sync_pipe, key, timestamp, owner, versioned, versioned_epoch.value_or(0), &zones_trace));
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
        yield call(sync_env->error_logger->log_error_cr(dpp, sc->conns.data->get_remote_id(), "data", error_ss.str(), -retcode, string("failed to sync object") + cpp_strerror(-sync_status)));
      }
done:
      if (sync_status == 0) {
        /* update marker */
        set_status() << "calling marker_tracker->finish(" << entry_marker << ")";
        yield call(marker_tracker->finish(dpp, entry_marker));
        sync_status = retcode;
      }
      if (sync_status < 0) {
        return set_cr_error(sync_status);
      }
      if (retcode < 0) {
        tn->log(0, SSTR("ERROR: failed to update source with minimu marker: retcode=" << retcode));
        /* not much to do about it now, assuming this is transient issue and will be fixed
         * next time */
      }
      return set_cr_done();
    }
    return 0;
  }
};


class SIPClientCRHandler {
  CephContext *cct;
  RGWAsyncRadosProcessor *async_rados;

  SIClientRef sipc;

  struct InitMarkersAction : public RGWGenericAsyncCR::Action {
    CephContext *cct;
    SIClientRef sipc;

    InitMarkersAction(CephContext *_cct, SIClientRef _sipc) : cct(_cct),
                                                              sipc(_sipc) {}
    int operate(const DoutPrefixProvider *dpp) override {
      int r = sipc->init_markers(null_yield);
      if (r < 0) {
        ldout(cct, 0) << "ERROR: " << __func__ << "(): init_markers() failed (r=" << r << ")" << dendl;
        return r;
      }

      return 0;
    }
  };

  struct FetchAction : public RGWGenericAsyncCR::Action {
    CephContext *cct;
    SIClientRef sipc;

    int shard_id;
    int max;

    std::shared_ptr<SIProvider::fetch_result> result;


    FetchAction(CephContext *_cct,
                SIClientRef _sipc,
                int _shard_id,
                int _max,
                std::shared_ptr<SIProvider::fetch_result>& _result) : cct(_cct),
                                                                      sipc(_sipc),
                                                                      shard_id(_shard_id),
                                                                      max(_max),
                                                                      result(_result) {}
    int operate(const DoutPrefixProvider *dpp) override {
      int r = sipc->fetch(shard_id, max, result.get());
      if (r < 0) {
        ldout(cct, 0) << "ERROR: " << __func__ << "(): failed to fetch bucket sync info (r=" << r << ")" << dendl;
        return r;
      }

      return 0;
    }
  };

public:
  SIPClientCRHandler(CephContext *_cct,
                     RGWAsyncRadosProcessor *_async_rados,
                     SIClientRef& _sipc) : cct(_cct),
                                           async_rados(_async_rados),
                                           sipc(_sipc) {}

  RGWCoroutine *init_markers_cr() {
    auto init_action = make_shared<InitMarkersAction>(cct, sipc);

    return new RGWGenericAsyncCR(cct,
                                 async_rados,
                                 init_action);
  }


  RGWCoroutine *fetch_cr(int shard_id,
                         int max,
                         std::shared_ptr<SIProvider::fetch_result>& result) {
    auto fetch_action = make_shared<FetchAction>(cct, sipc, shard_id, max, result);

    return new RGWGenericAsyncCR(cct,
                                 async_rados,
                                 fetch_action);
  }
};


#define BUCKET_SYNC_SPAWN_WINDOW 20

class RGWBucketShardFullSyncCR : public RGWCoroutine {
  RGWBucketSyncCtx *bsc;
  rgw_bucket_sync_pipe& sync_pipe;
  rgw_bucket_shard& bs;
  boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr;
  sip_bucket_fetch_result list_result;
  vector<sip_bucket_fetch_result::entry>::iterator entries_iter;
  rgw_bucket_shard_sync_info& sync_info;
  string list_marker;
  rgw_obj_key list_marker_key;
  sip_bucket_fetch_result::entry *entry{nullptr};

  int total_entries{0};

  int sync_status{0};

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
  RGWBucketShardFullSyncCR(RGWBucketSyncCtx *_bsc,
                           rgw_bucket_sync_pipe& _sync_pipe,
                           boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr,
                           rgw_bucket_shard_sync_info& sync_info,
                           RGWSyncTraceNodeRef tn_parent)
    : RGWCoroutine(_bsc->sc->cct), bsc(_bsc),
      sync_pipe(_sync_pipe), bs(_sync_pipe.info.source_bs),
      lease_cr(std::move(lease_cr)), sync_info(sync_info),
      tn(bsc->env->sync_tracer->add_node(tn_parent, "full_sync",
                                         SSTR(bucket_shard_str{bs}))),
      marker_tracker(bsc, *sync_info.full_marker, tn)
  {
    zones_trace.insert(bsc->sc->source_zone.id, sync_pipe.info.dest_bs.bucket.get_key());
    prefix_handler.set_rules(sync_pipe.get_rules());
  }

  int operate(const DoutPrefixProvider *dpp) override;
};

int RGWBucketShardFullSyncCR::operate(const DoutPrefixProvider *dpp)
{
  reenter(this) {
    list_marker_key = sync_info.full_marker->position;
    list_marker = sync_info.full_marker->position_id;

    total_entries = sync_info.full_marker->count;
    do {
      if (lease_cr && !lease_cr->is_locked()) {
        drain_all();
        return set_cr_error(-ECANCELED);
      }
      set_status("listing remote bucket");
      tn->log(20, "listing bucket for full sync");

      if (!prefix_handler.revalidate_marker(&list_marker_key)) {
        set_status() << "finished iterating over all available prefixes: last marker=" << list_marker;
        tn->log(20, SSTR("finished iterating over all available prefixes: last marker=" << list_marker));
        break;
      }

      yield call(bsc->hsi.full->fetch_cr(list_marker, &list_result));
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
        list_marker = entry->id;
        list_marker_key = entry->key;
        if (!prefix_handler.check_key_handled(entry->key)) {
          set_status() << "skipping entry due to policy rules: " << entries_iter->key;
          tn->log(20, SSTR("skipping entry due to policy rules: " << entries_iter->key));
          continue;
        }
        total_entries++;
        if (!marker_tracker.start(entry->id, entry->key, total_entries, real_time())) {
          tn->log(0, SSTR("ERROR: cannot start syncing " << entry->key << ". Duplicate entry?"));
        } else {
          using SyncCR = RGWBucketSyncSingleEntryCR<string, rgw_obj_key>;
          yield spawn(new SyncCR(bsc->sc, sync_pipe, entry->key,
                                 false, /* versioned, only matters for object removal */
                                 entry->versioned_epoch, entry->mtime,
                                 entry->owner, entry->op, CLS_RGW_STATE_COMPLETE,
                                 entry->id, &marker_tracker, zones_trace, tn),
                      false);
        }
        drain_with_cb(BUCKET_SYNC_SPAWN_WINDOW,
                      [&](uint64_t stack_id, int ret) {
                if (ret < 0) {
                  tn->log(10, "a sync operation returned error");
                  sync_status = ret;
                }
                return 0;
              });
      }
    } while (list_result.truncated && sync_status == 0);
    set_status("done iterating over all objects");
    /* wait for all operations to complete */

    drain_all_cb([&](uint64_t stack_id, int ret) {
      if (ret < 0) {
        tn->log(10, "a sync operation returned error");
        sync_status = ret;
      }
      return 0;
    });
    tn->unset_flag(RGW_SNS_FLAG_ACTIVE);
    if (lease_cr && !lease_cr->is_locked()) {
      return set_cr_error(-ECANCELED);
    }
    /* update sync state to incremental */
    if (sync_status == 0) {
      yield {
        sync_info.state = rgw_bucket_shard_sync_info::StateIncrementalSync;
        call(bsc->hst->write_status_cr(dpp, sync_info));
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
  RGWBucketSyncCtx *bsc;
  rgw_bucket_sync_pipe& sync_pipe;
  RGWBucketSyncFlowManager::pipe_rules_ref rules;
  rgw_bucket_shard& bs;
  boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr;
  sip_bucket_fetch_result list_result;
  vector<sip_bucket_fetch_result::entry>::iterator entries_iter, entries_end;
  map<pair<string, string>, pair<real_time, RGWModifyOp> > squash_map;
  rgw_bucket_shard_sync_info& sync_info;
  rgw_obj_key key;
  sip_bucket_fetch_result::entry *entry{nullptr};
  bool updated_status{false};
  rgw_zone_id zone_id;
  string target_location_key;
  string target_id;

  string cur_id;

  int sync_status{0};
  bool syncstopped{false};

  RGWSyncTraceNodeRef tn;
  RGWBucketIncSyncShardMarkerTrack marker_tracker;
  RGWBucketIncSyncShardMarkerTrack::marker_entry_type lowerbound_marker;

public:
  RGWBucketShardIncrementalSyncCR(RGWBucketSyncCtx *_bsc,
                                  rgw_bucket_sync_pipe& _sync_pipe,
                                  boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr,
                                  rgw_bucket_shard_sync_info& sync_info,
                                  RGWSyncTraceNodeRef& _tn_parent,
                                  ceph::real_time* stable_timestamp)
    : RGWCoroutine(_bsc->cct), bsc(_bsc),
      sync_pipe(_sync_pipe),
      bs(_sync_pipe.info.source_bs),
      lease_cr(std::move(lease_cr)), sync_info(sync_info),
      zone_id(bsc->env->svc->zone->get_zone().id),
      tn(bsc->env->sync_tracer->add_node(_tn_parent, "inc_sync",
                                         SSTR(bucket_shard_str{bs}))),
      marker_tracker(bsc, *sync_info.inc_marker, tn,
                     stable_timestamp)
  {
    set_description() << "bucket shard incremental sync bucket="
        << bucket_shard_str{bs};
    set_status("init");
    rules = sync_pipe.get_rules();
    target_location_key = sync_pipe.info.dest_bs.bucket.get_key();
    target_id = RGWSI_SIP_Marker::create_target_id(zone_id, target_location_key);
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

  int operate(const DoutPrefixProvider *dpp) override;
};

int RGWBucketShardIncrementalSyncCR::operate(const DoutPrefixProvider *dpp)
{
  int ret;
  reenter(this) {
    do {
      if (lease_cr && !lease_cr->is_locked()) {
        drain_all();
        tn->log(0, "ERROR: lease is not taken, abort");
        return set_cr_error(-ECANCELED);
      }
      tn->log(20, SSTR("listing bilog for incremental sync position=" << sync_info.inc_marker->position));
      set_status() << "listing bilog; position=" << sync_info.inc_marker->position;
      yield call(bsc->hsi.inc->fetch_cr(sync_info.inc_marker->position,
                                        &list_result));
      if (retcode < 0 && retcode != -ENOENT) {
        /* wait for all operations to complete */
        drain_all();
        return set_cr_error(retcode);
      }
      squash_map.clear();
      entries_iter = list_result.entries.begin();
      entries_end = list_result.entries.end();
      for (; entries_iter != entries_end; ++entries_iter) {
        auto e = *entries_iter;
        if (e.op == RGWModifyOp::CLS_RGW_OP_SYNCSTOP) {
          ldout(dpp, 20) << "syncstop on " << e.mtime << dendl;
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
        auto& squash_entry = squash_map[make_pair(e.key.name, e.key.instance)];
        // don't squash over olh entries - we need to apply their olh_epoch
        if (has_olh_epoch(squash_entry.second) && !has_olh_epoch(e.op)) {
          continue;
        }
        if (squash_entry.first <= e.mtime) {
          squash_entry = make_pair<>(e.mtime, e.op);
        }
      }

      entries_iter = list_result.entries.begin();
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
        sync_info.inc_marker.modify().position = cur_id;

        if (entry->op == RGWModifyOp::CLS_RGW_OP_SYNCSTOP || entry->op == RGWModifyOp::CLS_RGW_OP_RESYNC) {
          ldpp_dout(dpp, 20) << "detected syncstop or resync on " << entries_iter->mtime << ", skipping entry" << dendl;
          marker_tracker.try_update_high_marker(cur_id, std::nullopt, 0, entry->mtime);
          continue;
        }

        key = entry->key;

        if (!key.ns.empty()) {
          set_status() << "skipping entry in namespace: " << key;
          tn->log(20, SSTR("skipping entry in namespace: " << key));
          marker_tracker.try_update_high_marker(cur_id, std::nullopt, 0, entry->mtime);
          continue;
        }

        if (!check_key_handled(key)) {
          set_status() << "skipping entry due to policy rules: " << key;
          tn->log(20, SSTR("skipping entry due to policy rules: " << key));
          marker_tracker.try_update_high_marker(cur_id, std::nullopt, 0, entry->mtime);
          continue;
        }

        set_status() << "got entry.id=" << cur_id << " key=" << key << " op=" << (int)entry->op;
        if (entry->op == CLS_RGW_OP_CANCEL) {
          set_status() << "canceled operation, skipping";
          tn->log(20, SSTR("skipping object: "
              << bucket_shard_str{bs} << "/" << key << ": canceled operation"));
          marker_tracker.try_update_high_marker(cur_id, std::nullopt, 0, entry->mtime);
          continue;
        }
        if (entry->state != CLS_RGW_STATE_COMPLETE) {
          set_status() << "non-complete operation, skipping";
          tn->log(20, SSTR("skipping object: "
              << bucket_shard_str{bs} << "/" << key << ": non-complete operation"));
          marker_tracker.try_update_high_marker(cur_id, std::nullopt, 0, entry->mtime);
          continue;
        }
        if (entry->zones_trace.exists(zone_id.id, target_location_key)) {
          set_status() << "redundant operation, skipping";
          tn->log(20, SSTR("skipping object: "
              <<bucket_shard_str{bs} <<"/"<<key<<": redundant operation"));
          marker_tracker.try_update_high_marker(cur_id, std::nullopt, 0, entry->mtime);
          continue;
        }
        if (make_pair<>(entry->mtime, entry->op) != squash_map[make_pair(entry->key.name, entry->key.instance)]) {
          set_status() << "squashed operation, skipping";
          tn->log(20, SSTR("skipping object: "
              << bucket_shard_str{bs} << "/" << key << ": squashed operation"));
          marker_tracker.try_update_high_marker(cur_id, std::nullopt, 0, entry->mtime);
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
          marker_tracker.try_update_high_marker(cur_id, key, 0, entry->mtime);
          continue;
        }
        // yield {
          set_status() << "start object sync";
          if (!marker_tracker.start(cur_id, key, 0, entry->mtime)) {
            tn->log(0, SSTR("ERROR: cannot start syncing " << cur_id << ". Duplicate entry?"));
          } else {
            tn->log(20, SSTR("entry->timestamp=" << entry->mtime));
            using SyncCR = RGWBucketSyncSingleEntryCR<string, rgw_obj_key>;
            spawn(new SyncCR(bsc->sc, sync_pipe, key,
                             entry->is_versioned, entry->versioned_epoch,
                             entry->mtime, entry->owner, entry->op, entry->state,
                             cur_id, &marker_tracker, entry->zones_trace, tn),
                  false);
          }
        // }
        drain_with_cb(BUCKET_SYNC_SPAWN_WINDOW,
                      [&](uint64_t stack_id, int ret) {
                if (ret < 0) {
                  tn->log(10, "a sync operation returned error");
                  sync_status = ret;
                }
                return 0;
              });
      }
    } while (!list_result.entries.empty() && sync_status == 0 && !syncstopped);

    drain_all_cb([&](uint64_t stack_id, int ret) {
      if (ret < 0) {
        tn->log(10, "a sync operation returned error");
        sync_status = ret;
      }
      return 0;
    });

    if (marker_tracker.get_lowerbound(&lowerbound_marker)) {
      yield call(bsc->hsi.inc->update_marker_cr({ target_id,
                                                  lowerbound_marker.marker,
                                                  lowerbound_marker.timestamp,
                                                  false }));
      if (retcode < 0) {
        tn->log(0, SSTR("ERROR: failed to update source with minimu marker: retcode=" << retcode));
        /* not much to do about it now, assuming this is transient issue and will be fixed
         * next time */
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

    yield call(marker_tracker.flush(dpp));
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
    int operate(const DoutPrefixProvider *dpp) override {
      int r = sync_env->svc->bucket_sync->get_bucket_sync_hints(sync_env->dpp, 
                                                                source_bucket,
                                                                nullptr,
                                                                &targets,
                                                                null_yield);
      if (r < 0) {
        ldpp_dout(sync_env->dpp, 0) << "ERROR: " << __func__ << "(): failed to fetch bucket sync hints for bucket=" << source_bucket << dendl;
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

  int operate(const DoutPrefixProvider *dpp) override;
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

int RGWRunBucketSourcesSyncCR::operate(const DoutPrefixProvider *dpp)
{
  reenter(this) {
    if (source_bucket &&
        source_bucket->bucket_id.empty()) {
      /* source bucket id is empty, use latest bucket id if exists */

      yield call(new RGWSyncGetBucketInfoCR(sync_env,
                                            *source_bucket,
                                            &bucket_info,
                                            nullptr,
                                            tn));
      if (retcode < 0 &&
          retcode != -ENOENT) {
        return set_cr_error(retcode);
      } else {
        *source_bucket = bucket_info.bucket;
      }
    }
    yield call(new RGWGetBucketPeersCR(sync_env, target_bucket, sc->source_zone, source_bucket, &pipes, tn));
    if (retcode < 0 && retcode != -ENOENT) {
      tn->log(0, "ERROR: failed to read sync status for bucket");
      return set_cr_error(retcode);
    }

    ldpp_dout(dpp, 20) << __func__ << "(): requested source_bs=" << source_bs << " target_bs=" << target_bs << dendl;

    if (pipes.empty()) {
      ldpp_dout(dpp, 20) << __func__ << "(): no relevant sync pipes found" << dendl;
      return set_cr_done();
    }

    for (siter = pipes.begin(); siter != pipes.end(); ++siter) {
      {
        ldpp_dout(dpp, 20) << __func__ << "(): sync pipe=" << *siter << dendl;

        if (source_bs) {
          sync_pair.source_bs = *source_bs;
        } else {
          sync_pair.source_bs.bucket = siter->source.get_bucket();
        }
        sync_pair.dest_bs.bucket = siter->target.get_bucket();

        sync_pair.handler = siter->handler;

        handlers_repo = std::make_shared<RGWBucketShardSIPCRHandlersRepo>(sc, sync_pair.source_bs.bucket);
        wrapper_core_full = std::make_shared<RGWBucketShardSIPCRWrapperCore>(sc, sync_pair.source_bs.bucket, handlers_repo);
        wrapper_core_inc = std::make_shared<RGWBucketShardSIPCRWrapperCore>(sc, sync_pair.source_bs.bucket, handlers_repo);

        yield call(wrapper_core_full->init_cr(SIProvider::StageType::FULL, true, tn));
        if (retcode < 0) {
          tn->log(0, SSTR("ERROR: failed to initialize sip for full bucket sync: " << retcode));
          drain_all();
          return set_cr_error(retcode);
        }

        yield call(wrapper_core_inc->init_cr(SIProvider::StageType::INC, true, tn));
        if (retcode < 0) {
          tn->log(0, SSTR("ERROR: failed to initialize sip for incremental bucket sync: " << retcode));
          drain_all();
          return set_cr_error(retcode);
        }

        source_num_shards = wrapper_core_inc->num_shards();
        target_num_shards = siter->target.get_bucket_info().layout.current_index.layout.normal.num_shards;

        if (sync_pair.source_bs.shard_id >= 0) {
          num_shards = 1;
          cur_shard = sync_pair.source_bs.shard_id;
        } else {
          num_shards = std::max<int>(1, source_num_shards);
          cur_shard = std::min<int>(0, source_num_shards);
        }
      }

      ldpp_dout(dpp, 20) << __func__ << "(): num shards=" << num_shards << " cur_shard=" << cur_shard << dendl;

      for (; num_shards > 0; --num_shards, ++cur_shard) {
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

        ldpp_dout(dpp, 20) << __func__ << "(): sync_pair=" << sync_pair << dendl;

        cur_stack = &stacks_info[prealloc_stack_id()];

        cur_stack->bsi.full = std::make_shared<RGWBucketShardSIPCRWrapper>(wrapper_core_full, sync_pair.source_bs.shard_id);
        cur_stack->bsi.inc = std::make_shared<RGWBucketShardSIPCRWrapper>(wrapper_core_inc, sync_pair.source_bs.shard_id);
        cur_stack->bst = std::make_shared<RGWBucketSyncStatusCRHandler_Legacy>(sc, sync_pair);
        cur_stack->bsc.init(sc, sync_pair, cur_stack->bsi.full.get(), cur_stack->bsi.inc.get(), cur_stack->bst.get());

        yield_spawn_window(new RGWRunBucketSyncCoroutine(&cur_stack->bsc, lease_cr, tn,
                                                         &cur_stack->progress),
                           BUCKET_SYNC_SPAWN_WINDOW,
                           [&](uint64_t stack_id, int ret) {
                             handle_complete_stack(stack_id);
                             if (ret < 0) {
                               tn->log(10, "a sync operation returned error");
                             }
                             return ret;
                           });
      }
    }
    drain_all_cb([&](uint64_t stack_id, int ret) {
                   handle_complete_stack(stack_id);
                   if (ret < 0) {
                     tn->log(10, "a sync operation returned error");
                   }
                   return ret;
                 });
    if (progress && min_progress) {
      *progress = *min_progress;
    }
    return set_cr_done();
  }

  return 0;
}

int RGWSyncGetBucketInfoCR::operate(const DoutPrefixProvider *dpp)
{
  reenter(this) {
    yield call(new RGWGetBucketInstanceInfoCR(sync_env->async_rados, sync_env->store, bucket, pbucket_info, pattrs, dpp));
    if (retcode == -ENOENT) {
      /* bucket instance info has not been synced in yet, fetch it now */
      yield {
        tn->log(10, SSTR("no local info for bucket:" << ": fetching metadata"));
        string raw_key = string("bucket.instance:") + bucket.get_key();

        meta_sync_env.init(dpp, cct, sync_env->store, sync_env->svc->zone->get_master_conn(), sync_env->async_rados,
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

      yield call(new RGWGetBucketInstanceInfoCR(sync_env->async_rados, sync_env->store, bucket, pbucket_info, pattrs, dpp));
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
#if 0
    if (!siter->source.has_bucket_info()) {
      buckets_info.emplace(siter->source.get_bucket(), all_bucket_info());
    }
#endif
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

  int operate(const DoutPrefixProvider *dpp) override {
    reenter(this) {
      for (i = 0; i < 2; ++i) {
        yield call(new RGWBucketGetSyncPolicyHandlerCR(sync_env->async_rados,
                                                       sync_env->store,
                                                       get_policy_params,
                                                       policy,
                                                       dpp));
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

      return set_cr_error(-EIO);
    }

    return 0;
  }
};


int RGWGetBucketPeersCR::operate(const DoutPrefixProvider *dpp)
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
          ldpp_dout(dpp, 20) << "Got sync hint for bucket=" << *source_bucket << ": " << hiter->get_key() << dendl;

          target_policy = make_shared<rgw_bucket_get_sync_policy_result>();
          yield {
            rgw_bucket hiter_bucket;
            int r = rgw_bucket_parse_bucket_key(cct, hiter->get_key(),
                                                &hiter_bucket, nullptr);
            if (r < 0) {
              ldpp_dout(sync_env->dpp, 0) << "ERROR: failed to parse bucket key: " << hiter->get_key() << ": r=" << r << dendl;
              return set_cr_error(r);
            }
            call(new RGWSyncGetBucketSyncPolicyHandlerCR(sync_env,
                                                         nullopt,
                                                         hiter_bucket,
                                                         target_policy,
                                                         tn));
          }
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

int RGWRunBucketsSyncBySourceCR::operate(const DoutPrefixProvider *dpp)
{
  reenter(this) {
    return set_cr_done();
  }

  return 0;
}

int RGWRunBucketSyncCoroutine::operate(const DoutPrefixProvider *dpp)
{
  reenter(this) {
    yield call(bsc->hst->read_status_cr(&sync_status));
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
        yield call(new RGWInitBucketShardSyncStatusCoroutine(bsc, sync_status));
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
        *progress = sync_status.inc_marker->timestamp;
      }

      if (sync_status.state == rgw_bucket_shard_sync_info::StateFullSync) {
        yield call(new RGWBucketShardFullSyncCR(bsc, sync_pipe,
                                                lease_cr,
                                                sync_status, tn));
        if (retcode < 0) {
          tn->log(5, SSTR("full sync on bucket failed, retcode=" << retcode));
          drain_all();
          return set_cr_error(retcode);
        }
      }

      if (sync_status.state == rgw_bucket_shard_sync_info::StateIncrementalSync) {
        yield call(new RGWBucketShardIncrementalSyncCR(bsc, sync_pipe,
                                                       lease_cr,
                                                       sync_status, tn,
                                                       progress));
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

  return new RGWRunBucketSyncCoroutine(&bscs[num], nullptr, sync_env->sync_tracer->root_node, nullptr);
}

int RGWBucketPipeSyncStatusManager::init(const DoutPrefixProvider *dpp)
{
  int ret;

  if (!http_manager.is_started()) {
    ret = http_manager.start();
    if (ret < 0) {
      ldpp_dout(this, 0) << "failed in http_manager.start() ret=" << ret << dendl;
      return ret;
    }
  }

  error_logger = new RGWSyncErrorLogger(store, RGW_SYNC_ERROR_LOG_SHARD_PREFIX, ERROR_LOGGER_SHARDS);

  sync_module.reset(new RGWDefaultSyncModuleInstance());
  auto async_rados = store->svc()->rados->get_async_processor();

  sync_env.init(this, store->ctx(), store,
                store->svc(), async_rados, &http_manager,
                error_logger, store->getRados()->get_sync_tracer(),
                sync_module, nullptr);

  rgw_sync_pipe_info_set pipes;

  ret = cr_mgr.run(dpp, new RGWGetBucketPeersCR(&sync_env,
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
  RGWRemoteCtl::Conns conns;

  for (auto& pipe : pipes) {
    auto& szone = pipe.source.zone;

    if (last_zone != szone) {
      auto opt_conns = store->ctl()->remote->zone_conns(szone);
      if (!opt_conns) {
        ldpp_dout(this, 0) << "connection object to zone " << szone << " does not exist" << dendl;
        return -EINVAL;
      }
      conns = *opt_conns;
      last_zone = szone;
    }

    auto bucket_mgr = new RGWRemoteBucketManager(this, &sync_env,
                                                 szone, conns,
                                                 pipe.handler,
                                                 pipe.source.get_bucket(),
                                                 pipe.target.get_bucket_info());

    ret = bucket_mgr->init(&cr_mgr);
    if (ret < 0) {
      ldpp_dout(this, 0) << "ERROR: failed to init bucket sync manager for pipe=" << pipe << "ret=" << ret << dendl;
      return ret;
    }

    source_mgrs.push_back(bucket_mgr);
  }

  return 0;
}

int RGWBucketPipeSyncStatusManager::init_sync_status(const DoutPrefixProvider *dpp)
{
  list<RGWCoroutinesStack *> stacks;
  // pass an empty objv tracker to each so that the version gets incremented

  for (auto& mgr : source_mgrs) {
    RGWCoroutinesStack *stack = new RGWCoroutinesStack(store->ctx(), &cr_mgr);

    for (int i = 0; i < mgr->num_pipes(); ++i) {
      stack->call(mgr->init_sync_status_cr(i));
    }

    stacks.push_back(stack);
  }

  return cr_mgr.run(dpp, stacks);
}

int RGWBucketPipeSyncStatusManager::read_sync_status(const DoutPrefixProvider *dpp)
{
  list<RGWCoroutinesStack *> stacks;

  for (auto& mgr : source_mgrs) {
    RGWCoroutinesStack *stack = new RGWCoroutinesStack(store->ctx(), &cr_mgr);
    for (int i = 0; i < mgr->num_pipes(); ++i) {
      stack->call(mgr->read_sync_status_cr(i, &sync_status[i]));
    }

    stacks.push_back(stack);
  }

  int ret = cr_mgr.run(dpp, stacks);
  if (ret < 0) {
    ldpp_dout(this, 0) << "ERROR: failed to read sync status for "
        << bucket_str{dest_bucket} << dendl;
    return ret;
  }

  return 0;
}

int RGWBucketPipeSyncStatusManager::run(const DoutPrefixProvider *dpp)
{
  list<RGWCoroutinesStack *> stacks;

  for (auto& mgr : source_mgrs) {
    RGWCoroutinesStack *stack = new RGWCoroutinesStack(store->ctx(), &cr_mgr);
    for (int i = 0; i < mgr->num_pipes(); ++i) {
      stack->call(mgr->run_sync_cr(i));
    }

    stacks.push_back(stack);
  }

  int ret = cr_mgr.run(dpp, stacks);
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
  auto zone = source_zone.value_or(rgw_zone_id("*")).id;
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
                                                      const rgw::sal::Object* obj)
{
  string prefix = object_status_oid_prefix + "." + source_zone.id + ":" + obj->get_bucket()->get_key().get_key();
  if (sync_pipe.source_bucket_info.bucket !=
      sync_pipe.dest_bucket_info.bucket) {
    prefix += string("/") + sync_pipe.dest_bucket_info.bucket.get_key();
  }
  return prefix + ":" + obj->get_name() + ":" + obj->get_instance();
}

int rgw_read_remote_bilog_info(const DoutPrefixProvider *dpp,
                               RGWRESTConn* conn,
                               const rgw_bucket& bucket,
                               BucketIndexShardsManager& markers,
                               optional_yield y)
{
  const auto instance_key = bucket.get_key();
  const rgw_http_param_pair params[] = {
    { "type" , "bucket-index" },
    { "bucket-instance", instance_key.c_str() },
    { "info" , nullptr },
    { nullptr, nullptr }
  };
  rgw_bilog_marker_info result;
  int r = conn->get_json_resource(dpp, "/admin/log/", params, y, result);
  if (r < 0) {
    ldpp_dout(dpp, -1) << "failed to fetch remote log markers: " << cpp_strerror(r) << dendl;
    return r;
  }
  r = markers.from_string(result.max_marker, -1);
  if (r < 0) {
    lderr(conn->get_ctx()) << "failed to decode remote log markers" << dendl;
    return r;
  }
  return 0;
}

class RGWCollectBucketSyncStatusCR : public RGWShardCollectCR {
  static constexpr int max_concurrent_shards = 16;
  rgw::sal::RadosStore* const store;
  RGWDataSyncCtx *const sc;
  RGWDataSyncEnv *const env;
  rgw_bucket source_bucket;
  RGWBucketInfo dest_bucket_info;
  rgw_bucket_shard source_bs;
  rgw_bucket_shard dest_bs;

  rgw_bucket_sync_pair_info sync_pair;
  std::vector<unique_ptr<RGWBucketSyncStatusCRHandler_Legacy> > bst;

  bool shard_to_shard_sync;

  using Vector = std::vector<rgw_bucket_shard_sync_info>;
  Vector::iterator i, end;

 public:
  RGWCollectBucketSyncStatusCR(rgw::sal::RadosStore *store, RGWDataSyncCtx *sc,
                               const rgw_bucket& source_bucket,
                               int num_source_shards,
                               const RGWBucketInfo& dest_bucket_info,
                               Vector *status)
    : RGWShardCollectCR(sc->cct, max_concurrent_shards),
      store(store), sc(sc), env(sc->env),
      source_bucket(source_bucket),
      dest_bucket_info(dest_bucket_info),
      i(status->begin()), end(status->end())
  {
    shard_to_shard_sync = (num_source_shards == (int)dest_bucket_info.layout.current_index.layout.normal.num_shards);

    source_bs = rgw_bucket_shard(source_bucket, num_source_shards > 0 ? 0 : -1);
    dest_bs = rgw_bucket_shard(dest_bucket_info.bucket, shard_to_shard_sync ? source_bs.shard_id : -1);

    status->clear();

    auto num = std::max<size_t>(1, num_source_shards);
    status->resize(num);

    bst.reserve(num);

    i = status->begin();
    end = status->end();
  }

  bool spawn_next() override {
    if (i == end) {
      return false;
    }
    sync_pair.source_bs = source_bs;
    sync_pair.dest_bs = dest_bs;
    bst.emplace_back(new RGWBucketSyncStatusCRHandler_Legacy(sc, sync_pair));
    spawn(bst.back()->read_status_cr(&*i), false);
    ++i;
    ++source_bs.shard_id;
    if (shard_to_shard_sync) {
      dest_bs.shard_id = source_bs.shard_id;
    }
    return true;
  }
};

class RGWCollectBucketSourceShardStateCR : public RGWShardCollectCR {
  static constexpr int max_concurrent_shards = 16;
  rgw::sal::RGWRadosStore *const store;
  RGWDataSyncCtx *const sc;
  RGWDataSyncEnv *const env;

  std::shared_ptr<RGWBucketShardSIPCRWrapperCore> wrapper_core_inc;

  std::vector<unique_ptr<RGWBucketShardSIPCRWrapper> > sips;

  using Vector = std::vector<rgw_bucket_index_marker_info>;
  Vector::iterator i, end;

  int cur_shard{0};

 public:
  RGWCollectBucketSourceShardStateCR(rgw::sal::RGWRadosStore *store,
                                     RGWDataSyncCtx *sc,
                                     std::shared_ptr<RGWBucketShardSIPCRWrapperCore> wrapper_core_inc,
                                     Vector *status)
    : RGWShardCollectCR(sc->cct, max_concurrent_shards),
      store(store), sc(sc), env(sc->env),
      wrapper_core_inc(wrapper_core_inc),
      i(status->begin()), end(status->end())
  {
    status->clear();

    auto num = std::max<size_t>(1, wrapper_core_inc->num_shards());
    status->resize(num);

    sips.reserve(num);

    i = status->begin();
    end = status->end();
  }

  bool spawn_next() override {
    if (i == end) {
      return false;
    }
    sips.emplace_back(new RGWBucketShardSIPCRWrapper(wrapper_core_inc, cur_shard));
    spawn(sips.back()->get_pos_cr(&*i, nullptr), false);
    ++i;
    ++cur_shard;
    return true;
  }
};

int rgw_bucket_sync_status(const DoutPrefixProvider *dpp,
                           rgw::sal::RadosStore* store,
                           const rgw_sync_bucket_pipe& pipe,
                           const RGWBucketInfo& dest_bucket_info,
                           std::vector<rgw_bucket_shard_sync_info> *status,
                           std::vector<rgw_bucket_index_marker_info> *opt_source_markers)
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

  RGWCoroutinesManager crs(store->ctx(), store->getRados()->get_cr_registry());
  RGWHTTPManager http_manager(store->ctx(), crs.get_completion_mgr());
  int ret = http_manager.start();
  if (ret < 0) {
    ldout(store->ctx(), 0) << "failed in http_manager.start() ret=" << ret << dendl;
    return ret;
  }

  RGWDataSyncEnv env;
  RGWSyncModuleInstanceRef module; // null sync module
  env.init(dpp, store->ctx(), store, store->svc(), store->svc()->rados->get_async_processor(),
           &http_manager, nullptr, store->getRados()->get_sync_tracer(),
           module, nullptr);

  RGWDataSyncCtx sc;
  auto conns = store->ctl()->remote->zone_conns(*pipe.source.zone);
  if (!conns) {
    ldpp_dout(dpp, 0) << "connection object to zone " << *pipe.source.zone << " does not exist" << dendl;
    return -ENOENT;
  }

  sc.init(&env, *conns, *pipe.source.zone);

  auto handlers_repo = std::make_shared<RGWBucketShardSIPCRHandlersRepo>(&sc, source_bucket);
  auto wrapper_core_inc = std::make_shared<RGWBucketShardSIPCRWrapperCore>(&sc, source_bucket, handlers_repo);

  auto tn = env.sync_tracer->add_node(env.sync_tracer->root_node, "rgw_bucket_sync_status", SSTR(source_bucket));

  ret = crs.run(dpp, wrapper_core_inc->init_cr(SIProvider::StageType::INC, true, tn));
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "(): failed to initialize SIP core for incremental bucket sync: ret=" << ret << dendl;
    return ret;
  }

  ret = crs.run(dpp, new RGWCollectBucketSyncStatusCR(store, &sc,
                                                 source_bucket,
                                                 wrapper_core_inc->num_shards(),
                                                 dest_bucket_info,
                                                 status));
  if (ret < 0) {
    return ret;
  }

  if (!opt_source_markers) {
    return 0;
  }

  return crs.run(dpp, new RGWCollectBucketSourceShardStateCR(store, &sc,
                                                        wrapper_core_inc,
                                                        opt_source_markers));
}

void rgw_data_sync_info::generate_test_instances(list<rgw_data_sync_info*>& o)
{
  auto info = new rgw_data_sync_info;
  info->state = rgw_data_sync_info::StateBuildingFullSyncMaps;
  info->num_shards = 8;
  o.push_back(info);
  o.push_back(new rgw_data_sync_info);
}

void rgw_data_sync_marker::generate_test_instances(list<rgw_data_sync_marker*>& o)
{
  auto marker = new rgw_data_sync_marker;
  marker->state = rgw_data_sync_marker::IncrementalSync;
  marker->marker = "01234";
  marker->pos = 5;
  o.push_back(marker);
  o.push_back(new rgw_data_sync_marker);
}

void rgw_data_sync_status::generate_test_instances(list<rgw_data_sync_status*>& o)
{
  o.push_back(new rgw_data_sync_status);
}

void rgw_bucket_shard_full_sync_marker::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("position", position, obj);
  JSONDecoder::decode_json("position_id", position_id, obj);
  JSONDecoder::decode_json("count", count, obj);
}

void rgw_bucket_shard_full_sync_marker::dump(Formatter *f) const
{
  encode_json("position", position, f);
  encode_json("position_id", position_id, f);
  encode_json("count", count, f);
}

void rgw_bucket_shard_inc_sync_marker::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("position", position, obj);
  JSONDecoder::decode_json("timestamp", timestamp, obj);
}

void rgw_bucket_shard_inc_sync_marker::dump(Formatter *f) const
{
  encode_json("position", position, f);
  encode_json("timestamp", timestamp, f);
}

void rgw_bucket_shard_sync_info::decode_json(JSONObj *obj)
{
  std::string s;
  JSONDecoder::decode_json("status", s, obj);

  auto& _state = state.raw();
  
  if (s == "full-sync") {
    _state = StateFullSync;
  } else if (s == "incremental-sync") {
    _state = StateIncrementalSync;
  } else if (s == "stopped") {
    _state = StateStopped;
  } else {
    _state = StateInit;
  }
  JSONDecoder::decode_json("full_marker", full_marker.raw(), obj);
  JSONDecoder::decode_json("inc_marker", inc_marker.raw(), obj);
}

void rgw_bucket_shard_sync_info::dump(Formatter *f) const
{
  const char *s{nullptr};
  switch ((SyncState)*state) {
    case StateInit:
    s = "init";
    break;
  case StateFullSync:
    s = "full-sync";
    break;
  case StateIncrementalSync:
    s = "incremental-sync";
    break;
  case StateStopped:
    s = "stopped";
    break;
  default:
    s = "unknown";
    break;
  }
  encode_json("status", s, f);
  encode_json("full_marker", *full_marker, f);
  encode_json("inc_marker", *inc_marker, f);
}

