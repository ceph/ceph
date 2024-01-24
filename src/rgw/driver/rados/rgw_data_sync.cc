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
#include "rgw_cr_rest.h"
#include "rgw_cr_tools.h"
#include "rgw_http_client.h"
#include "rgw_bucket.h"
#include "rgw_bucket_sync.h"
#include "rgw_bucket_sync_cache.h"
#include "rgw_datalog.h"
#include "rgw_metadata.h"
#include "rgw_sync_counters.h"
#include "rgw_sync_error_repo.h"
#include "rgw_sync_module.h"
#include "rgw_sal.h"

#include "cls/lock/cls_lock_client.h"
#include "cls/rgw/cls_rgw_client.h"

#include "services/svc_zone.h"
#include "services/svc_sync_modules.h"

#include "include/common_fwd.h"
#include "include/random.h"

#include <boost/asio/yield.hpp>
#include <string_view>

#define dout_subsys ceph_subsys_rgw

#undef dout_prefix
#define dout_prefix (*_dout << "data sync: ")

using namespace std;

static const string datalog_sync_status_oid_prefix = "datalog.sync-status";
static const string datalog_sync_status_shard_prefix = "datalog.sync-status.shard";
static const string datalog_sync_full_sync_index_prefix = "data.full-sync.index";
static const string bucket_full_status_oid_prefix = "bucket.full-sync-status";
static const string bucket_status_oid_prefix = "bucket.sync-status";
static const string object_status_oid_prefix = "bucket.sync-status";

static const string data_sync_bids_oid = "data-sync-bids";

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

// print a bucket shard with [gen]
std::string to_string(const rgw_bucket_shard& bs, std::optional<uint64_t> gen)
{
  constexpr auto digits10 = std::numeric_limits<uint64_t>::digits10;
  constexpr auto reserve = 2 + digits10; // [value]
  auto str = bs.get_key('/', ':', ':', reserve);
  str.append(1, '[');
  str.append(std::to_string(gen.value_or(0)));
  str.append(1, ']');
  return str;
}

class RGWReadDataSyncStatusMarkersCR : public RGWShardCollectCR {
  static constexpr int MAX_CONCURRENT_SHARDS = 16;

  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *env;
  const int num_shards;
  int shard_id{0};;

  map<uint32_t, rgw_data_sync_marker>& markers;
  std::vector<RGWObjVersionTracker>& objvs;

  int handle_result(int r) override {
    if (r == -ENOENT) { // ENOENT is not a fatal error
      return 0;
    }
    if (r < 0) {
      ldout(cct, 4) << "failed to read data sync status: "
          << cpp_strerror(r) << dendl;
    }
    return r;
  }
 public:
  RGWReadDataSyncStatusMarkersCR(RGWDataSyncCtx *sc, int num_shards,
                                 map<uint32_t, rgw_data_sync_marker>& markers,
                                 std::vector<RGWObjVersionTracker>& objvs)
    : RGWShardCollectCR(sc->cct, MAX_CONCURRENT_SHARDS),
      sc(sc), env(sc->env), num_shards(num_shards), markers(markers), objvs(objvs)
  {}
  bool spawn_next() override;
};

bool RGWReadDataSyncStatusMarkersCR::spawn_next()
{
  if (shard_id >= num_shards) {
    return false;
  }
  using CR = RGWSimpleRadosReadCR<rgw_data_sync_marker>;
  spawn(new CR(env->dpp, env->driver,
               rgw_raw_obj(env->svc->zone->get_zone_params().log_pool, RGWDataSyncStatusManager::shard_obj_name(sc->source_zone, shard_id)),
               &markers[shard_id], true, &objvs[shard_id]),
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

  int handle_result(int r) override {
    if (r == -ENOENT) { // ENOENT is not a fatal error
      return 0;
    }
    if (r < 0) {
      ldout(cct, 4) << "failed to list recovering data sync: "
          << cpp_strerror(r) << dendl;
    }
    return r;
  }
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
  spawn(new RGWRadosGetOmapKeysCR(env->driver, rgw_raw_obj(env->svc->zone->get_zone_params().log_pool, error_oid),
                                  marker, max_entries, shard_keys), false);

  ++shard_id;
  return true;
}

class RGWReadDataSyncStatusCoroutine : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  rgw_data_sync_status *sync_status;
  RGWObjVersionTracker* objv_tracker;
  std::vector<RGWObjVersionTracker>& objvs;

public:
  RGWReadDataSyncStatusCoroutine(RGWDataSyncCtx *_sc,
                                 rgw_data_sync_status *_status,
                                 RGWObjVersionTracker* objv_tracker,
                                 std::vector<RGWObjVersionTracker>& objvs)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(sc->env), sync_status(_status),
      objv_tracker(objv_tracker), objvs(objvs)
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
      call(new ReadInfoCR(dpp, sync_env->driver,
                          rgw_raw_obj(sync_env->svc->zone->get_zone_params().log_pool, RGWDataSyncStatusManager::sync_status_oid(sc->source_zone)),
                          &sync_status->sync_info, empty_on_enoent, objv_tracker));
    }
    if (retcode < 0) {
      ldpp_dout(dpp, 4) << "failed to read sync status info with "
          << cpp_strerror(retcode) << dendl;
      return set_cr_error(retcode);
    }
    // read shard markers
    objvs.resize(sync_status->sync_info.num_shards);
    using ReadMarkersCR = RGWReadDataSyncStatusMarkersCR;
    yield call(new ReadMarkersCR(sc, sync_status->sync_info.num_shards,
                                 sync_status->sync_markers, objvs));
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

  int tries{0};
  int op_ret{0};

public:
  RGWReadRemoteDataLogShardInfoCR(RGWDataSyncCtx *_sc,
                                  int _shard_id, RGWDataChangesLogInfo *_shard_info) : RGWCoroutine(_sc->cct),
                                                      sc(_sc),
                                                      sync_env(_sc->env),
                                                      http_op(NULL),
                                                      shard_id(_shard_id),
                                                      shard_info(_shard_info) {
  }

  int operate(const DoutPrefixProvider *dpp) override {
    reenter(this) {
      static constexpr int NUM_ENPOINT_IOERROR_RETRIES = 20;
      for (tries = 0; tries < NUM_ENPOINT_IOERROR_RETRIES; tries++) {
        ldpp_dout(dpp, 20) << "read remote datalog shard info. shard_id=" << shard_id << " retries=" << tries << dendl;

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

          int ret = http_op->aio_read(dpp);
          if (ret < 0) {
            ldpp_dout(dpp, 0) << "ERROR: failed to read from " << p << dendl;
            log_error() << "failed to send http operation: " << http_op->to_str() << " ret=" << ret << std::endl;
            http_op->put();
            return set_cr_error(ret);
          }

          return io_block(0);
        }
        yield {
          op_ret = http_op->wait(shard_info, null_yield);
          http_op->put();
        }

        if (op_ret < 0) {
          if (op_ret == -EIO && tries < NUM_ENPOINT_IOERROR_RETRIES - 1) {
            ldpp_dout(dpp, 20) << "failed to fetch remote datalog shard info. retry. shard_id=" << shard_id << dendl;
            continue;
          } else {
            return set_cr_error(op_ret);
          }
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
  vector<rgw_data_change_log_entry> entries;

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
  vector<rgw_data_change_log_entry> *entries;
  bool *truncated;

  read_remote_data_log_response response;
  std::optional<TOPNSPC::common::PerfGuard> timer;

  int tries{0};
  int op_ret{0};

public:
  RGWReadRemoteDataLogShardCR(RGWDataSyncCtx *_sc, int _shard_id,
                              const std::string& marker, string *pnext_marker,
                              vector<rgw_data_change_log_entry> *_entries,
                              bool *_truncated)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
      shard_id(_shard_id), marker(marker), pnext_marker(pnext_marker),
      entries(_entries), truncated(_truncated) {
  }

  int operate(const DoutPrefixProvider *dpp) override {
    reenter(this) {
      static constexpr int NUM_ENPOINT_IOERROR_RETRIES = 20;
      for (tries = 0; tries < NUM_ENPOINT_IOERROR_RETRIES; tries++) {
        ldpp_dout(dpp, 20) << "read remote datalog shard. shard_id=" << shard_id << " retries=" << tries << dendl;

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
          int ret = http_op->aio_read(dpp);
          if (ret < 0) {
            ldpp_dout(dpp, 0) << "ERROR: failed to read from " << p << dendl;
            log_error() << "failed to send http operation: " << http_op->to_str() << " ret=" << ret << std::endl;
            if (sync_env->counters) {
              sync_env->counters->inc(sync_counters::l_poll_err);
            }
            http_op->put();
            return set_cr_error(ret);
          }

          return io_block(0);
        }
        yield {
          timer.reset();
          op_ret = http_op->wait(&response, null_yield);
          http_op->put();
        }

        if (op_ret < 0) {
          if (op_ret == -EIO && tries < NUM_ENPOINT_IOERROR_RETRIES - 1) {
            ldpp_dout(dpp, 20) << "failed to read remote datalog shard. retry. shard_id=" << shard_id << dendl;
            continue;
          } else {
            if (sync_env->counters && op_ret != -ENOENT) {
              sync_env->counters->inc(sync_counters::l_poll_err);
            }
            return set_cr_error(op_ret);
          }
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

  int handle_result(int r) override {
    if (r == -ENOENT) { // ENOENT is not a fatal error
      return 0;
    }
    if (r < 0) {
      ldout(cct, 4) << "failed to fetch remote datalog info: "
          << cpp_strerror(r) << dendl;
    }
    return r;
  }
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
  static constexpr int NUM_ENPOINT_IOERROR_RETRIES = 20;

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
    : RGWSimpleCoroutine(sc->cct, NUM_ENPOINT_IOERROR_RETRIES), sc(sc), sync_env(sc->env), http_op(NULL),
      shard_id(_shard_id), marker(_marker), max_entries(_max_entries), result(_result) {}

  int send_request(const DoutPrefixProvider *dpp) override {
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
      ldpp_dout(sync_env->dpp, 5) << "ERROR: failed to list remote datalog shard, ret=" << ret << dendl;
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

  int handle_result(int r) override {
    if (r == -ENOENT) { // ENOENT is not a fatal error
      return 0;
    }
    if (r < 0) {
      ldout(cct, 4) << "failed to list remote datalog: "
          << cpp_strerror(r) << dendl;
    }
    return r;
  }
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
  static constexpr auto lock_name{ "sync_lock"sv };
  RGWDataSyncCtx* const sc;
  RGWDataSyncEnv* const sync_env{ sc->env };
  const uint32_t num_shards;
  rgw_data_sync_status* const status;
  RGWSyncTraceNodeRef tn;
  boost::intrusive_ptr<RGWContinuousLeaseCR> lease_cr;
  RGWObjVersionTracker& objv_tracker;
  std::vector<RGWObjVersionTracker>& objvs;

  const rgw_pool& pool{ sync_env->svc->zone->get_zone_params().log_pool };
  const string sync_status_oid{
    RGWDataSyncStatusManager::sync_status_oid(sc->source_zone) };

  map<int, RGWDataChangesLogInfo> shards_info;


public:
  RGWInitDataSyncStatusCoroutine(
    RGWDataSyncCtx* _sc, uint32_t num_shards, uint64_t instance_id,
    const RGWSyncTraceNodeRef& tn_parent, rgw_data_sync_status* status,
    boost::intrusive_ptr<RGWContinuousLeaseCR> lease_cr,
    RGWObjVersionTracker& objv_tracker,
    std::vector<RGWObjVersionTracker>& objvs)
    : RGWCoroutine(_sc->cct), sc(_sc), num_shards(num_shards), status(status),
      tn(sync_env->sync_tracer->add_node(tn_parent, "init_data_sync_status")),
      lease_cr(std::move(lease_cr)), objv_tracker(objv_tracker), objvs(objvs) {
    status->sync_info.instance_id = instance_id;
  }

  static auto continuous_lease_cr(RGWDataSyncCtx* const sc,
				  RGWCoroutine* const caller) {
    auto lock_duration = sc->cct->_conf->rgw_sync_lease_period;
    return new RGWContinuousLeaseCR(
      sc->env->async_rados, sc->env->driver,
      { sc->env->svc->zone->get_zone_params().log_pool,
	RGWDataSyncStatusManager::sync_status_oid(sc->source_zone) },
      string(lock_name), lock_duration, caller, &sc->lcc);
  }

  int operate(const DoutPrefixProvider *dpp) override {
    int ret;
    reenter(this) {
      if (!lease_cr->is_locked()) {
	drain_all();
	return set_cr_error(-ECANCELED);
      }

      using WriteInfoCR = RGWSimpleRadosWriteCR<rgw_data_sync_info>;
      yield call(new WriteInfoCR(dpp, sync_env->driver,
                                 rgw_raw_obj{pool, sync_status_oid},
                                 status->sync_info, &objv_tracker));
      if (retcode < 0) {
        tn->log(0, SSTR("ERROR: failed to write sync status info with " << retcode));
        return set_cr_error(retcode);
      }

      // In the original code we reacquired the lock. Since
      // RGWSimpleRadosWriteCR doesn't appear to touch the attributes
      // and cls_version works across it, this should be unnecessary.
      // Putting a note here just in case. If we see ECANCELED where
      // we expect EBUSY, we can revisit this.

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
        objvs.resize(num_shards);
        for (uint32_t i = 0; i < num_shards; i++) {
          RGWDataChangesLogInfo& info = shards_info[i];
          auto& marker = status->sync_markers[i];
          marker.next_step_marker = info.marker;
          marker.timestamp = info.last_update;
          const auto& oid = RGWDataSyncStatusManager::shard_obj_name(sc->source_zone, i);
          auto& objv = objvs[i];
          objv.generate_new_write_ver(cct);
          using WriteMarkerCR = RGWSimpleRadosWriteCR<rgw_data_sync_marker>;
          spawn(new WriteMarkerCR(dpp, sync_env->driver,
                                  rgw_raw_obj{pool, oid}, marker, &objv), true);
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
      yield call(new WriteInfoCR(dpp, sync_env->driver,
                                 rgw_raw_obj{pool, sync_status_oid},
                                 status->sync_info, &objv_tracker));
      if (retcode < 0) {
        tn->log(0, SSTR("ERROR: failed to write sync status info with " << retcode));
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }
};

RGWRemoteDataLog::RGWRemoteDataLog(const DoutPrefixProvider *dpp,
                                   rgw::sal::RadosStore* driver,
                                   RGWAsyncRadosProcessor *async_rados)
  : RGWCoroutinesManager(driver->ctx(), driver->getRados()->get_cr_registry()),
      dpp(dpp), driver(driver),
      cct(driver->ctx()), cr_registry(driver->getRados()->get_cr_registry()),
      async_rados(async_rados),
      http_manager(driver->ctx(), completion_mgr),
      data_sync_cr(NULL),
      initialized(false)
{
}

int RGWRemoteDataLog::read_log_info(const DoutPrefixProvider *dpp, rgw_datalog_info *log_info)
{
  rgw_http_param_pair pairs[] = { { "type", "data" },
                                  { NULL, NULL } };

  int ret = sc.conn->get_json_resource(dpp, "/admin/log", pairs, null_yield, *log_info);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to fetch datalog info" << dendl;
    return ret;
  }

  ldpp_dout(dpp, 20) << "remote datalog, num_shards=" << log_info->num_shards << dendl;

  return 0;
}

int RGWRemoteDataLog::read_source_log_shards_info(const DoutPrefixProvider *dpp, map<int, RGWDataChangesLogInfo> *shards_info)
{
  rgw_datalog_info log_info;
  int ret = read_log_info(dpp, &log_info);
  if (ret < 0) {
    return ret;
  }

  return run(dpp, new RGWReadRemoteDataLogInfoCR(&sc, log_info.num_shards, shards_info));
}

int RGWRemoteDataLog::read_source_log_shards_next(const DoutPrefixProvider *dpp, map<int, string> shard_markers, map<int, rgw_datalog_shard_data> *result)
{
  return run(dpp, new RGWListRemoteDataLogCR(&sc, shard_markers, 1, result));
}

int RGWRemoteDataLog::init(const rgw_zone_id& _source_zone, RGWRESTConn *_conn, RGWSyncErrorLogger *_error_logger,
                           RGWSyncTraceManager *_sync_tracer, RGWSyncModuleInstanceRef& _sync_module,
                           PerfCounters* counters)
{
  sync_env.init(dpp, cct, driver, driver->svc(), async_rados, &http_manager, _error_logger,
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

int RGWRemoteDataLog::read_sync_status(const DoutPrefixProvider *dpp, rgw_data_sync_status *sync_status)
{
  // cannot run concurrently with run_sync(), so run in a separate manager
  RGWObjVersionTracker objv;
  std::vector<RGWObjVersionTracker> shard_objvs;
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

  ret = crs.run(dpp, new RGWReadDataSyncStatusCoroutine(&sc_local, sync_status,
                                                        &objv, shard_objvs));
  http_manager.stop();
  return ret;
}

int RGWRemoteDataLog::read_recovering_shards(const DoutPrefixProvider *dpp, const int num_shards, set<int>& recovering_shards)
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

  ret = crs.run(dpp, new RGWReadDataSyncRecoveringShardsCR(&sc_local, max_entries, num_shards, omapkeys));
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

namespace RGWRDL {
class DataSyncInitCR : public RGWCoroutine {
  RGWDataSyncCtx* const sc;
  const uint32_t num_shards;
  uint64_t instance_id;
  const RGWSyncTraceNodeRef& tn;
  rgw_data_sync_status* const sync_status;
  std::vector<RGWObjVersionTracker>& objvs;

  boost::intrusive_ptr<RGWContinuousLeaseCR> lease_cr;

  RGWObjVersionTracker objv_tracker;

public:

  DataSyncInitCR(RGWDataSyncCtx* sc, uint32_t num_shards, uint64_t instance_id,
		 const RGWSyncTraceNodeRef& tn,
		 rgw_data_sync_status* sync_status,
		 std::vector<RGWObjVersionTracker>& objvs)
    : RGWCoroutine(sc->cct), sc(sc), num_shards(num_shards),
      instance_id(instance_id), tn(tn),
      sync_status(sync_status), objvs(objvs) {}

  ~DataSyncInitCR() override {
    if (lease_cr) {
      lease_cr->abort();
    }
  }

  int operate(const DoutPrefixProvider *dpp) override {
    reenter(this) {
      lease_cr.reset(
	RGWInitDataSyncStatusCoroutine::continuous_lease_cr(sc, this));

      yield spawn(lease_cr.get(), false);
      while (!lease_cr->is_locked()) {
	if (lease_cr->is_done()) {
	  tn->log(5, "ERROR: failed to take data sync status lease");
	  set_status("lease lock failed, early abort");
	  drain_all();
	  return set_cr_error(lease_cr->get_ret_status());
	}
	tn->log(5, "waiting on data sync status lease");
	yield set_sleeping(true);
      }
      tn->log(5, "acquired data sync status lease");
      objv_tracker.generate_new_write_ver(sc->cct);
      yield call(new RGWInitDataSyncStatusCoroutine(sc, num_shards, instance_id,
						    tn, sync_status, lease_cr,
						    objv_tracker, objvs));
      lease_cr->go_down();
      lease_cr.reset();
      drain_all();
      if (retcode < 0) {
	set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }
};
}

int RGWRemoteDataLog::init_sync_status(const DoutPrefixProvider *dpp, int num_shards)
{
  rgw_data_sync_status sync_status;
  std::vector<RGWObjVersionTracker> objvs;
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
  ret = crs.run(dpp, new RGWRDL::DataSyncInitCR(&sc_local, num_shards,
						instance_id, tn, &sync_status, objvs));
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

class RGWReadRemoteBucketIndexLogInfoCR : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  const string instance_key;

  rgw_bucket_index_marker_info *info;

public:
  RGWReadRemoteBucketIndexLogInfoCR(RGWDataSyncCtx *_sc,
				    const rgw_bucket& bucket,
				    rgw_bucket_index_marker_info *_info)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
      instance_key(bucket.get_key()), info(_info) {}

  int operate(const DoutPrefixProvider *dpp) override {
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


class RGWListBucketIndexesCR : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env = sc->env;

  rgw::sal::RadosStore* driver = sync_env->driver;

  rgw_data_sync_status *sync_status;
  std::vector<RGWObjVersionTracker>& objvs;

  int req_ret = 0;
  int ret = 0;

  list<string>::iterator iter;

  unique_ptr<RGWShardedOmapCRManager> entries_index;
  string oid_prefix =
    datalog_sync_full_sync_index_prefix + "." + sc->source_zone.id;

  string path = "/admin/metadata/bucket.instance";
  bucket_instance_meta_info meta_info;
  string key;

  bool failed = false;
  bool truncated = false;
  read_metadata_list result;

public:
  RGWListBucketIndexesCR(RGWDataSyncCtx* sc,
                         rgw_data_sync_status* sync_status, std::vector<RGWObjVersionTracker>& objvs)
    : RGWCoroutine(sc->cct), sc(sc), sync_status(sync_status), objvs(objvs) {}
  ~RGWListBucketIndexesCR() override { }

  int operate(const DoutPrefixProvider *dpp) override {
    reenter(this) {
      entries_index = std::make_unique<RGWShardedOmapCRManager>(
	sync_env->async_rados, driver, this,
	cct->_conf->rgw_data_log_num_shards,
	sync_env->svc->zone->get_zone_params().log_pool,
	oid_prefix);
      yield; // yield so OmapAppendCRs can start

      do {
        yield {
          string entrypoint = "/admin/metadata/bucket.instance"s;

          rgw_http_param_pair pairs[] = {{"max-entries", "1000"},
                                         {"marker", result.marker.c_str()},
                                         {NULL, NULL}};

          call(new RGWReadRESTResourceCR<read_metadata_list>(
		 sync_env->cct, sc->conn, sync_env->http_manager,
		 entrypoint, pairs, &result));
	}
	if (retcode < 0) {
	  ldpp_dout(dpp, 0)
	    << "ERROR: failed to fetch metadata for section bucket.instance"
	    << dendl;
          return set_cr_error(retcode);
        }

        for (iter = result.keys.begin(); iter != result.keys.end(); ++iter) {
          ldpp_dout(dpp, 20) << "list metadata: section=bucket.instance key="
			     << *iter << dendl;
          key = *iter;

          yield {
            rgw_http_param_pair pairs[] = {{"key", key.c_str()},
                                           {NULL, NULL}};

            call(new RGWReadRESTResourceCR<bucket_instance_meta_info>(
		   sync_env->cct, sc->conn, sync_env->http_manager, path, pairs,
		   &meta_info));
          }
	  if (retcode < 0) {
	    ldpp_dout(dpp, 0) << "ERROR: failed to fetch metadata for key: "
			      << key << dendl;
	    return set_cr_error(retcode);
	  }
	  // Now that bucket full sync is bucket-wide instead of
	  // per-shard, we only need to register a single shard of
	  // each bucket to guarantee that sync will see everything
	  // that happened before data full sync starts. This also
	  // means we don't have to care about the bucket's current
	  // shard count.
	  yield entries_index->append(
	    fmt::format("{}:{}", key, 0),
	    sync_env->svc->datalog_rados->get_log_shard_id(
	      meta_info.data.get_bucket_info().bucket, 0));
	}
	truncated = result.truncated;
      } while (truncated);

      yield {
        if (!entries_index->finish()) {
          failed = true;
        }
      }
      if (!failed) {
        for (auto iter = sync_status->sync_markers.begin();
	     iter != sync_status->sync_markers.end();
	     ++iter) {
          int shard_id = (int)iter->first;
          rgw_data_sync_marker& marker = iter->second;
          marker.total_entries = entries_index->get_total_entries(shard_id);
          spawn(new RGWSimpleRadosWriteCR<rgw_data_sync_marker>(
		  dpp, sync_env->driver,
		  rgw_raw_obj(sync_env->svc->zone->get_zone_params().log_pool,
			      RGWDataSyncStatusManager::shard_obj_name(
				sc->source_zone, shard_id)),
		  marker, &objvs[shard_id]),
		true);
	}
      } else {
        yield call(sync_env->error_logger->log_error_cr(
		     dpp, sc->conn->get_remote_id(), "data.init", "",
		     EIO, string("failed to build bucket instances map")));
      }
      while (collect(&ret, NULL)) {
	if (ret < 0) {
          yield call(sync_env->error_logger->log_error_cr(
		       dpp, sc->conn->get_remote_id(), "data.init", "",
		       -ret, string("failed to driver sync status: ") +
		       cpp_strerror(-ret)));
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
  RGWObjVersionTracker& objv;

public:
  RGWDataSyncShardMarkerTrack(RGWDataSyncCtx *_sc,
                         const string& _marker_oid,
                         const rgw_data_sync_marker& _marker,
                         RGWSyncTraceNodeRef& _tn, RGWObjVersionTracker& objv) : RGWSyncShardMarkerTrack(DATA_SYNC_UPDATE_MARKER_WINDOW),
                                                                sc(_sc), sync_env(_sc->env),
                                                                marker_oid(_marker_oid),
                                                                sync_marker(_marker),
                                                                tn(_tn), objv(objv) {}

  RGWCoroutine* store_marker(const string& new_marker, uint64_t index_pos, const real_time& timestamp) override {
    sync_marker.marker = new_marker;
    sync_marker.pos = index_pos;
    sync_marker.timestamp = timestamp;

    tn->log(20, SSTR("updating marker marker_oid=" << marker_oid << " marker=" << new_marker));

    return new RGWSimpleRadosWriteCR<rgw_data_sync_marker>(sync_env->dpp, sync_env->driver,
                                                           rgw_raw_obj(sync_env->svc->zone->get_zone_params().log_pool, marker_oid),
                                                           sync_marker, &objv);
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
#if FMT_VERSION >= 90000
template <> struct fmt::formatter<bucket_shard_str> : fmt::ostream_formatter {};
#endif

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

  size_t size() const {
    return handlers.size();
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

class RGWRunBucketSourcesSyncCR : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr;

  rgw_sync_pipe_info_set pipes;
  rgw_sync_pipe_info_set::iterator siter;

  rgw_bucket_sync_pair_info sync_pair;

  RGWSyncTraceNodeRef tn;
  ceph::real_time* progress;
  std::vector<ceph::real_time> shard_progress;
  std::vector<ceph::real_time>::iterator cur_shard_progress;

  RGWRESTConn *conn{nullptr};
  rgw_zone_id last_zone;

  std::optional<uint64_t> gen;
  rgw_bucket_index_marker_info marker_info;
  BucketIndexShardsManager marker_mgr;

public:
  RGWRunBucketSourcesSyncCR(RGWDataSyncCtx *_sc,
                            boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr,
                            const rgw_bucket_shard& source_bs,
                            const RGWSyncTraceNodeRef& _tn_parent,
			    std::optional<uint64_t> gen,
                            ceph::real_time* progress);

  int operate(const DoutPrefixProvider *dpp) override;
};

class RGWDataSyncSingleEntryCR : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  rgw::bucket_sync::Handle state; // cached bucket-shard state
  rgw_data_sync_obligation obligation; // input obligation
  std::optional<rgw_data_sync_obligation> complete; // obligation to complete
  uint32_t obligation_counter = 0;
  RGWDataSyncShardMarkerTrack *marker_tracker;
  rgw_raw_obj error_repo;
  boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr;
  RGWSyncTraceNodeRef tn;

  ceph::real_time progress;
  int sync_status = 0;
public:
  RGWDataSyncSingleEntryCR(RGWDataSyncCtx *_sc, rgw::bucket_sync::Handle state,
                           rgw_data_sync_obligation _obligation,
                           RGWDataSyncShardMarkerTrack *_marker_tracker,
                           const rgw_raw_obj& error_repo,
                           boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr,
                           const RGWSyncTraceNodeRef& _tn_parent)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
      state(std::move(state)), obligation(std::move(_obligation)),
      marker_tracker(_marker_tracker), error_repo(error_repo),
      lease_cr(std::move(lease_cr)) {
    set_description() << "data sync single entry (source_zone=" << sc->source_zone << ") " << obligation;
    tn = sync_env->sync_tracer->add_node(_tn_parent, "entry", to_string(obligation.bs, obligation.gen));
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

          ldout(cct, 4) << "starting sync on " << bucket_shard_str{state->key.first}
              << ' ' << *state->obligation << " progress timestamp " << state->progress_timestamp
              << " progress " << progress << dendl;
          yield call(new RGWRunBucketSourcesSyncCR(sc, lease_cr,
                                                   state->key.first, tn,
                                                   state->obligation->gen,
						   &progress));
          if (retcode < 0) {
            break;
          }
          state->progress_timestamp = std::max(progress, state->progress_timestamp);
        }
        // any new obligations will process themselves
        complete = std::move(*state->obligation);
        state->obligation.reset();

        tn->log(10, SSTR("sync finished on " << bucket_shard_str{state->key.first}
                         << " progress=" << progress << ' ' << complete << " r=" << retcode));
      }
      sync_status = retcode;

      if (sync_status == -ENOENT) {
        // this was added when 'tenant/' was added to datalog entries, because
        // preexisting tenant buckets could never sync and would stay in the
        // error_repo forever
        tn->log(0, SSTR("WARNING: skipping data log entry for missing bucket " << complete->bs));
        sync_status = 0;
      }

      if (sync_status < 0) {
        // write actual sync failures for 'radosgw-admin sync error list'
        if (sync_status != -EBUSY && sync_status != -EAGAIN) {
          yield call(sync_env->error_logger->log_error_cr(dpp, sc->conn->get_remote_id(), "data",
                                                          to_string(complete->bs, complete->gen),
                                                          -sync_status, string("failed to sync bucket instance: ") + cpp_strerror(-sync_status)));
          if (retcode < 0) {
            tn->log(0, SSTR("ERROR: failed to log sync failure: retcode=" << retcode));
          }
        }
        if (complete->timestamp != ceph::real_time{}) {
          tn->log(10, SSTR("writing " << *complete << " to error repo for retry"));
          yield call(rgw::error_repo::write_cr(sync_env->driver->getRados()->get_rados_handle(), error_repo,
                                              rgw::error_repo::encode_key(complete->bs, complete->gen),
                                              complete->timestamp));
          if (retcode < 0) {
            tn->log(0, SSTR("ERROR: failed to log sync failure in error repo: retcode=" << retcode));
          }
        }
      } else if (complete->retry) {
        yield call(rgw::error_repo::remove_cr(sync_env->driver->getRados()->get_rados_handle(), error_repo,
                                              rgw::error_repo::encode_key(complete->bs, complete->gen),
                                              complete->timestamp));
        if (retcode < 0) {
          tn->log(0, SSTR("ERROR: failed to remove omap key from error repo ("
             << error_repo << " retcode=" << retcode));
        }
      }
      /* FIXME: what do do in case of error */
      if (marker_tracker && !complete->marker.empty()) {
        /* update marker */
        yield call(marker_tracker->finish(complete->marker));
        if (retcode < 0) {
          return set_cr_error(retcode);
        }
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

rgw_raw_obj datalog_oid_for_error_repo(RGWDataSyncCtx *sc, rgw::sal::RadosStore* driver,
                                      rgw_pool& pool, rgw_bucket_shard& bs) {
  int datalog_shard = driver->svc()->datalog_rados->choose_oid(bs);
  string oid = RGWDataSyncStatusManager::shard_obj_name(sc->source_zone, datalog_shard);
  return rgw_raw_obj(pool, oid + ".retry");
  }

class RGWDataIncrementalSyncFullObligationCR: public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  rgw_bucket_shard source_bs;
  rgw_raw_obj error_repo;
  std::string error_marker;
  ceph::real_time timestamp;
  RGWSyncTraceNodeRef tn;
  rgw_bucket_index_marker_info remote_info;
  rgw_pool pool;
  uint32_t sid;
  rgw_bucket_shard bs;
  std::vector<store_gen_shards>::const_iterator each;

public:
  RGWDataIncrementalSyncFullObligationCR(RGWDataSyncCtx *_sc, rgw_bucket_shard& _source_bs,
                                         const rgw_raw_obj& error_repo, const std::string& _error_marker,
                                         ceph::real_time& _timestamp, RGWSyncTraceNodeRef& _tn)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env), source_bs(_source_bs),
      error_repo(error_repo), error_marker(_error_marker), timestamp(_timestamp),
      tn(sync_env->sync_tracer->add_node(_tn, "error_repo", SSTR(bucket_shard_str(source_bs))))
  {}

  int operate(const DoutPrefixProvider *dpp) override {
    reenter(this) {
      yield call(new RGWReadRemoteBucketIndexLogInfoCR(sc, source_bs.bucket, &remote_info));
      if (retcode == -ENOENT) {
        // don't retry if bucket instance does not exist
        tn->log(10, SSTR("bucket instance or log layout does not exist on source for bucket " << source_bs.bucket));
        yield call(rgw::error_repo::remove_cr(sync_env->driver->getRados()->get_rados_handle(), error_repo,
                                            error_marker, timestamp));
        return set_cr_done();
      } else if (retcode < 0) {
        return set_cr_error(retcode);
      }

      each = remote_info.generations.cbegin();
      for (; each != remote_info.generations.cend(); each++) {
        for (sid = 0; sid < each->num_shards; sid++) {
          bs.bucket = source_bs.bucket;
          bs.shard_id = sid;
	  pool = sync_env->svc->zone->get_zone_params().log_pool;
          error_repo = datalog_oid_for_error_repo(sc, sync_env->driver, pool, source_bs);
          tn->log(10, SSTR("writing shard_id " << sid << " of gen " << each->gen << " to error repo for retry"));
          yield_spawn_window(rgw::error_repo::write_cr(sync_env->driver->getRados()->get_rados_handle(), error_repo,
                            rgw::error_repo::encode_key(bs, each->gen),
			    timestamp), sc->lcc.adj_concurrency(cct->_conf->rgw_data_sync_spawn_window),
                            [&](uint64_t stack_id, int ret) {
                              if (ret < 0) {
                                retcode = ret;
                              }
                              return 0;
                            });
        }
      }
      drain_all_cb([&](uint64_t stack_id, int ret) {
                   if (ret < 0) {
                     tn->log(10, SSTR("writing to error repo returned error: " << ret));
                   }
                   return ret;
                 });

      // once everything succeeds, remove the full sync obligation from the error repo
      yield call(rgw::error_repo::remove_cr(sync_env->driver->getRados()->get_rados_handle(), error_repo,
                                            error_marker, timestamp));
      return set_cr_done();
    }
    return 0;
  }
};

RGWCoroutine* data_sync_single_entry(RGWDataSyncCtx *sc, const rgw_bucket_shard& src,
                                std::optional<uint64_t> gen,
                                const std::string marker,
                                ceph::real_time timestamp,
                                boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr,
                                boost::intrusive_ptr<rgw::bucket_sync::Cache> bucket_shard_cache,
                                RGWDataSyncShardMarkerTrack* marker_tracker,
                                rgw_raw_obj error_repo,
                                RGWSyncTraceNodeRef& tn,
                                bool retry) {
  auto state = bucket_shard_cache->get(src, gen);
  auto obligation = rgw_data_sync_obligation{src, gen, marker, timestamp, retry};
  return new RGWDataSyncSingleEntryCR(sc, std::move(state), std::move(obligation),
                                      &*marker_tracker, error_repo,
                                      lease_cr.get(), tn);
}

static ceph::real_time timestamp_for_bucket_shard(rgw::sal::RadosStore* driver,
                                                const rgw_data_sync_status& sync_status,
                                                const rgw_bucket_shard& bs) {
  int datalog_shard = driver->svc()->datalog_rados->choose_oid(bs);
  auto status = sync_status.sync_markers.find(datalog_shard);
  if (status == sync_status.sync_markers.end()) {
    return ceph::real_clock::zero();
  }
  return status->second.timestamp;
}

class RGWDataFullSyncSingleEntryCR : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  rgw_pool pool;
  rgw_bucket_shard source_bs;
  const std::string key;
  rgw_data_sync_status sync_status;
  rgw_raw_obj error_repo;
  ceph::real_time timestamp;
  boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr;
  boost::intrusive_ptr<rgw::bucket_sync::Cache> bucket_shard_cache;
  RGWDataSyncShardMarkerTrack* marker_tracker;
  RGWSyncTraceNodeRef tn;
  rgw_bucket_index_marker_info remote_info;
  uint32_t sid;
  std::vector<store_gen_shards>::iterator each;
  uint64_t i{0};
  RGWCoroutine* shard_cr = nullptr;
  bool first_shard = true;
  bool error_inject;

public:
  RGWDataFullSyncSingleEntryCR(RGWDataSyncCtx *_sc, const rgw_pool& _pool, const rgw_bucket_shard& _source_bs,
                      const std::string& _key, const rgw_data_sync_status& sync_status, const rgw_raw_obj& _error_repo,
                      ceph::real_time _timestamp, boost::intrusive_ptr<const RGWContinuousLeaseCR> _lease_cr,
                      boost::intrusive_ptr<rgw::bucket_sync::Cache> _bucket_shard_cache,
                      RGWDataSyncShardMarkerTrack* _marker_tracker,
                      RGWSyncTraceNodeRef& _tn)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env), pool(_pool), source_bs(_source_bs), key(_key),
      error_repo(_error_repo), timestamp(_timestamp), lease_cr(std::move(_lease_cr)),
      bucket_shard_cache(_bucket_shard_cache), marker_tracker(_marker_tracker), tn(_tn) {
        error_inject = (sync_env->cct->_conf->rgw_sync_data_full_inject_err_probability > 0);
      }


  int operate(const DoutPrefixProvider *dpp) override {
    reenter(this) {
      if (error_inject &&
          rand() % 10000 < cct->_conf->rgw_sync_data_full_inject_err_probability * 10000.0) {
        tn->log(0, SSTR("injecting read bilog info error on key=" << key));
        retcode = -ENOENT;
      } else {
        tn->log(0, SSTR("read bilog info key=" << key));
        yield call(new RGWReadRemoteBucketIndexLogInfoCR(sc, source_bs.bucket, &remote_info));
      }

      if (retcode < 0) {
        tn->log(10, SSTR("full sync: failed to read remote bucket info. Writing "
                        << source_bs.shard_id << " to error repo for retry"));
        yield call(rgw::error_repo::write_cr(sync_env->driver->getRados()->get_rados_handle(), error_repo,
                                            rgw::error_repo::encode_key(source_bs, std::nullopt),
                                            timestamp));
        if (retcode < 0) {
          tn->log(0, SSTR("ERROR: failed to log " << source_bs.shard_id << " in error repo: retcode=" << retcode));
        }
        yield call(marker_tracker->finish(key));
        return set_cr_error(retcode);
      }

      //wait to sync the first shard of the oldest generation and then sync all other shards.
      //if any of the operations fail at any time, write them into error repo for later retry.

      each = remote_info.generations.begin();
      for (; each != remote_info.generations.end(); each++) {
        for (sid = 0; sid < each->num_shards; sid++) {
          source_bs.shard_id = sid;
          // use the error repo and sync status timestamp from the datalog shard corresponding to source_bs
          error_repo = datalog_oid_for_error_repo(sc, sync_env->driver, pool, source_bs);
          timestamp = timestamp_for_bucket_shard(sync_env->driver, sync_status, source_bs);
          if (retcode < 0) {
            tn->log(10, SSTR("Write " << source_bs.shard_id << " to error repo for retry"));
            yield_spawn_window(rgw::error_repo::write_cr(sync_env->driver->getRados()->get_rados_handle(), error_repo,
                rgw::error_repo::encode_key(source_bs, each->gen),
		timestamp), sc->lcc.adj_concurrency(cct->_conf->rgw_data_sync_spawn_window), std::nullopt);
          } else {
          shard_cr = data_sync_single_entry(sc, source_bs, each->gen, key, timestamp,
                      lease_cr, bucket_shard_cache, nullptr, error_repo, tn, false);
          tn->log(10, SSTR("full sync: syncing shard_id " << sid << " of gen " << each->gen));
          if (first_shard) {
            yield call(shard_cr);
            first_shard = false;
          } else {
            yield_spawn_window(shard_cr, sc->lcc.adj_concurrency(cct->_conf->rgw_data_sync_spawn_window),
                              [&](uint64_t stack_id, int ret) {
                                if (ret < 0) {
                                  retcode = ret;
                                }
                                return retcode;
                                });
            }
          }
        }
        drain_all_cb([&](uint64_t stack_id, int ret) {
                if (ret < 0) {
                  retcode = ret;
                }
                return retcode;
              });
      }

      yield call(marker_tracker->finish(key));
      if (retcode < 0) {
          return set_cr_error(retcode);
        }

      return set_cr_done();
    }
    return 0;
  }
};

class RGWDataBaseSyncShardCR : public RGWCoroutine {
protected:
  RGWDataSyncCtx *const sc;
  const rgw_pool& pool;
  const uint32_t shard_id;
  rgw_data_sync_marker& sync_marker;
  RGWSyncTraceNodeRef tn;
  const string& status_oid;
  const rgw_raw_obj& error_repo;
  boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr;
  const rgw_data_sync_status& sync_status;
  RGWObjVersionTracker& objv;
  boost::intrusive_ptr<rgw::bucket_sync::Cache> bucket_shard_cache;

  std::optional<RGWDataSyncShardMarkerTrack> marker_tracker;
  RGWRadosGetOmapValsCR::ResultPtr omapvals;
  rgw_bucket_shard source_bs;

  int parse_bucket_key(const std::string& key, rgw_bucket_shard& bs) const {
    int ret = rgw_bucket_parse_bucket_key(sc->env->cct, key,
                                       &bs.bucket, &bs.shard_id);
    //for the case of num_shards 0, shard_id gets a value of -1
    //because of the way bucket instance gets parsed in the absence of shard_id delimiter.
    //interpret it as a non-negative value.
    if (ret == 0) {
      if (bs.shard_id < 0) {
        bs.shard_id = 0;
      }
    }
    return ret;
  }

  RGWDataBaseSyncShardCR(
    RGWDataSyncCtx *const _sc, const rgw_pool& pool, const uint32_t shard_id,
    rgw_data_sync_marker& sync_marker, RGWSyncTraceNodeRef tn,
    const string& status_oid, const rgw_raw_obj& error_repo,
    boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr,
    const rgw_data_sync_status& sync_status,
    RGWObjVersionTracker& objv,
    const boost::intrusive_ptr<rgw::bucket_sync::Cache>& bucket_shard_cache)
    : RGWCoroutine(_sc->cct), sc(_sc), pool(pool), shard_id(shard_id),
      sync_marker(sync_marker), tn(tn), status_oid(status_oid),
      error_repo(error_repo), lease_cr(std::move(lease_cr)),
      sync_status(sync_status), objv(objv),
      bucket_shard_cache(bucket_shard_cache) {}
};

class RGWDataFullSyncShardCR : public RGWDataBaseSyncShardCR {
  static constexpr auto OMAP_GET_MAX_ENTRIES = 100;

  string oid;
  uint64_t total_entries = 0;
  ceph::real_time entry_timestamp;
  std::map<std::string, bufferlist> entries;
  std::map<std::string, bufferlist>::iterator iter;
  string error_marker;
  bool lost_lock = false;
  bool lost_bid = false;

public:

  RGWDataFullSyncShardCR(
    RGWDataSyncCtx *const sc, const rgw_pool& pool, const uint32_t shard_id,
    rgw_data_sync_marker& sync_marker, RGWSyncTraceNodeRef tn,
    const string& status_oid, const rgw_raw_obj& error_repo,
    boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr,
    const rgw_data_sync_status& sync_status, RGWObjVersionTracker& objv,
    const boost::intrusive_ptr<rgw::bucket_sync::Cache>& bucket_shard_cache)
    : RGWDataBaseSyncShardCR(sc, pool, shard_id, sync_marker, tn,
			     status_oid, error_repo, std::move(lease_cr),
			     sync_status, objv, bucket_shard_cache) {}

  int operate(const DoutPrefixProvider *dpp) override {
    reenter(this) {
      tn->log(10, "start full sync");
      oid = full_data_sync_index_shard_oid(sc->source_zone, shard_id);
      marker_tracker.emplace(sc, status_oid, sync_marker, tn, objv);
      total_entries = sync_marker.pos;
      entry_timestamp = sync_marker.timestamp; // time when full sync started
      do {
        if (!lease_cr->is_locked()) {
          tn->log(1, "lease is lost, abort");
          lost_lock = true;
          break;
          }

        if (!sc->env->bid_manager->is_highest_bidder(shard_id)) {
          tn->log(1, "lost bid");
          lost_bid = true;
          break;
        }

        omapvals = std::make_shared<RGWRadosGetOmapValsCR::Result>();
        yield call(new RGWRadosGetOmapValsCR(sc->env->driver,
					     rgw_raw_obj(pool, oid),
                                             sync_marker.marker,
					     OMAP_GET_MAX_ENTRIES, omapvals));
        if (retcode < 0) {
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
            marker_tracker->try_update_high_marker(iter->first, 0,
						   entry_timestamp);
            continue;
          }
          tn->log(20, SSTR("full sync: " << iter->first));
          total_entries++;
          if (!marker_tracker->start(iter->first, total_entries,
				     entry_timestamp)) {
            tn->log(0, SSTR("ERROR: cannot start syncing " << iter->first
			    << ". Duplicate entry?"));
          } else {
            tn->log(10, SSTR("timestamp for " << iter->first << " is :" << entry_timestamp));
            yield_spawn_window(new RGWDataFullSyncSingleEntryCR(
				 sc, pool, source_bs, iter->first, sync_status,
				 error_repo, entry_timestamp, lease_cr,
				 bucket_shard_cache, &*marker_tracker, tn),
			       sc->lcc.adj_concurrency(cct->_conf->rgw_data_sync_spawn_window),
			       [&](uint64_t stack_id, int ret) {
                                if (ret < 0) {
                                  retcode = ret;
                                }
                                return retcode;
                                });
          }
          sync_marker.marker = iter->first;
        }

      } while (omapvals->more);
      omapvals.reset();

      drain_all();

      tn->unset_flag(RGW_SNS_FLAG_ACTIVE);

      if (lost_bid) {
        yield call(marker_tracker->flush());
      } else if (!lost_lock) {
        /* update marker to reflect we're done with full sync */
        sync_marker.state = rgw_data_sync_marker::IncrementalSync;
        sync_marker.marker = sync_marker.next_step_marker;
        sync_marker.next_step_marker.clear();
        yield call(new RGWSimpleRadosWriteCR<rgw_data_sync_marker>(
              sc->env->dpp, sc->env->driver,
              rgw_raw_obj(pool, status_oid), sync_marker, &objv));
        if (retcode < 0) {
          tn->log(0, SSTR("ERROR: failed to set sync marker: retcode=" << retcode));
          return set_cr_error(retcode);
        }

        // clean up full sync index, ignoring errors
        yield call(new RGWRadosRemoveCR(sc->env->driver, {pool, oid}));

        // transition to incremental sync
        return set_cr_done();
      }

      if (lost_lock || lost_bid) {
        return set_cr_error(-EBUSY);
      }

    }  return 0;
  }
};

class RGWDataIncSyncShardCR : public RGWDataBaseSyncShardCR {
  static constexpr int max_error_entries = 10;
  static constexpr uint32_t retry_backoff_secs = 60;

  ceph::mutex& inc_lock;
  bc::flat_set<rgw_data_notify_entry>& modified_shards;

  bc::flat_set<rgw_data_notify_entry> current_modified;
  decltype(current_modified)::iterator modified_iter;

  ceph::coarse_real_time error_retry_time;
  string error_marker;
  std::map<std::string, bufferlist> error_entries;
  decltype(error_entries)::iterator iter;
  ceph::real_time entry_timestamp;
  std::optional<uint64_t> gen;

  string next_marker;
  vector<rgw_data_change_log_entry> log_entries;
  decltype(log_entries)::iterator log_iter;
  bool truncated = false;
  int cbret = 0;
  bool lost_lock = false;
  bool lost_bid = false;

  utime_t get_idle_interval() const {
    ceph::timespan interval = std::chrono::seconds(cct->_conf->rgw_data_sync_poll_interval);
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


public:

  RGWDataIncSyncShardCR(
    RGWDataSyncCtx *const sc, const rgw_pool& pool, const uint32_t shard_id,
    rgw_data_sync_marker& sync_marker, RGWSyncTraceNodeRef tn,
    const string& status_oid, const rgw_raw_obj& error_repo,
    boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr,
    const rgw_data_sync_status& sync_status, RGWObjVersionTracker& objv,
    const boost::intrusive_ptr<rgw::bucket_sync::Cache>& bucket_shard_cache,
    ceph::mutex& inc_lock,
    bc::flat_set<rgw_data_notify_entry>& modified_shards)
    : RGWDataBaseSyncShardCR(sc, pool, shard_id, sync_marker, tn,
			     status_oid, error_repo, std::move(lease_cr),
			     sync_status, objv, bucket_shard_cache),
      inc_lock(inc_lock), modified_shards(modified_shards) {}

  int operate(const DoutPrefixProvider *dpp) override {
    reenter(this) {
      tn->log(10, "start incremental sync");
      marker_tracker.emplace(sc, status_oid, sync_marker, tn, objv);
      do {
        if (!lease_cr->is_locked()) {
          lost_lock = true;
          tn->log(1, "lease is lost, abort");
          break;
        }

        if (!sc->env->bid_manager->is_highest_bidder(shard_id)) {
          tn->log(1, "lost bid");
          lost_bid = true;
          break;
        }
	{
	  current_modified.clear();
	  std::unique_lock il(inc_lock);
	  current_modified.swap(modified_shards);
	  il.unlock();
	}

        if (current_modified.size() > 0) {
          tn->set_flag(RGW_SNS_FLAG_ACTIVE); /* actually have entries to sync */
        }
        /* process out of band updates */
        for (modified_iter = current_modified.begin();
	     modified_iter != current_modified.end();
	     ++modified_iter) {
	  if (!lease_cr->is_locked()) {
          tn->log(1, "lease is lost, abort");
          lost_lock = true;
          break;
	  }
          retcode = parse_bucket_key(modified_iter->key, source_bs);
          if (retcode < 0) {
            tn->log(1, SSTR("failed to parse bucket shard: "
			    << modified_iter->key));
	    continue;
          }
          tn->log(20, SSTR("received async update notification: "
			   << modified_iter->key));
          spawn(data_sync_single_entry(sc, source_bs, modified_iter->gen, {},
				       ceph::real_time{}, lease_cr,
				       bucket_shard_cache, &*marker_tracker,
				       error_repo, tn, false), false);
	}

        if (error_retry_time <= ceph::coarse_real_clock::now()) {
          /* process bucket shards that previously failed */
          omapvals = std::make_shared<RGWRadosGetOmapValsCR::Result>();
          yield call(new RGWRadosGetOmapValsCR(sc->env->driver, error_repo,
                                               error_marker, max_error_entries,
					       omapvals));
          error_entries = std::move(omapvals->entries);
          tn->log(20, SSTR("read error repo, got " << error_entries.size()
			   << " entries"));
          iter = error_entries.begin();
          for (; iter != error_entries.end(); ++iter) {
	    if (!lease_cr->is_locked()) {
          tn->log(1, "lease is lost, abort");
          lost_lock = true;
          break;
	    }
            error_marker = iter->first;
            entry_timestamp = rgw::error_repo::decode_value(iter->second);
            retcode = rgw::error_repo::decode_key(iter->first, source_bs, gen);
            if (retcode == -EINVAL) {
              // backward compatibility for string keys that don't encode a gen
              retcode = parse_bucket_key(error_marker, source_bs);
            }
            if (retcode < 0) {
              tn->log(1, SSTR("failed to parse bucket shard: " << error_marker));
              spawn(rgw::error_repo::remove_cr(sc->env->driver->getRados()->get_rados_handle(),
					       error_repo, error_marker,
					       entry_timestamp),
		    false);
              continue;
            }
            tn->log(10, SSTR("gen is " << gen));
            if (!gen) {
              // write all full sync obligations for the bucket to error repo
              spawn(new RGWDataIncrementalSyncFullObligationCR(sc, source_bs,
                     error_repo, error_marker, entry_timestamp, tn), false);
            } else {
              tn->log(20, SSTR("handle error entry key="
			       << to_string(source_bs, gen)
			       << " timestamp=" << entry_timestamp));
              spawn(data_sync_single_entry(sc, source_bs, gen, "",
					   entry_timestamp, lease_cr,
					   bucket_shard_cache, &*marker_tracker,
					   error_repo, tn, true), false);
            }
          }
          if (!omapvals->more) {
            error_retry_time = ceph::coarse_real_clock::now() +
	      make_timespan(retry_backoff_secs);
            error_marker.clear();
          }
        }
        omapvals.reset();

        tn->log(20, SSTR("shard_id=" << shard_id << " sync_marker="
			 << sync_marker.marker));
        yield call(new RGWReadRemoteDataLogShardCR(sc, shard_id,
						   sync_marker.marker,
                                                   &next_marker, &log_entries,
						   &truncated));
        if (retcode < 0 && retcode != -ENOENT) {
          tn->log(0, SSTR("ERROR: failed to read remote data log info: ret="
			  << retcode));
          drain_all();
          return set_cr_error(retcode);
        }

        if (log_entries.size() > 0) {
          tn->set_flag(RGW_SNS_FLAG_ACTIVE); /* actually have entries to sync */
        }

        for (log_iter = log_entries.begin();
	     log_iter != log_entries.end();
	     ++log_iter) {
	  if (!lease_cr->is_locked()) {
          tn->log(1, "lease is lost, abort");
          lost_lock = true;
          break;
	  }

          tn->log(20, SSTR("shard_id=" << shard_id << " log_entry: " << log_iter->log_id << ":" << log_iter->log_timestamp << ":" << log_iter->entry.key));
          retcode = parse_bucket_key(log_iter->entry.key, source_bs);
          if (retcode < 0) {
            tn->log(1, SSTR("failed to parse bucket shard: "
			    << log_iter->entry.key));
            marker_tracker->try_update_high_marker(log_iter->log_id, 0,
						   log_iter->log_timestamp);
            continue;
          }
          if (!marker_tracker->start(log_iter->log_id, 0,
				     log_iter->log_timestamp)) {
            tn->log(0, SSTR("ERROR: cannot start syncing " << log_iter->log_id
			    << ". Duplicate entry?"));
          } else {
            tn->log(1, SSTR("incremental sync on " << log_iter->entry.key  << "shard: " << shard_id << "on gen " << log_iter->entry.gen));
            yield_spawn_window(data_sync_single_entry(sc, source_bs, log_iter->entry.gen, log_iter->log_id,
                                                 log_iter->log_timestamp, lease_cr,bucket_shard_cache,
                                                 &*marker_tracker, error_repo, tn, false),
                               sc->lcc.adj_concurrency(cct->_conf->rgw_data_sync_spawn_window),
                               [&](uint64_t stack_id, int ret) {
                                 if (ret < 0) {
                                   tn->log(10, SSTR("data_sync_single_entry returned error: " << ret));
                                   cbret = ret;
                                 }
                                 return 0;
                                });
          }
        }
        if (cbret < 0 ) {
          retcode = cbret;
          drain_all();
          return set_cr_error(retcode);
        }

        tn->log(20, SSTR("shard_id=" << shard_id <<
			 " sync_marker="<< sync_marker.marker
			 << " next_marker=" << next_marker
			 << " truncated=" << truncated));
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

      drain_all();

      if (lost_bid) {
        yield call(marker_tracker->flush());
        return set_cr_error(-EBUSY);
      } else if (lost_lock) {
        return set_cr_error(-ECANCELED);
      }

    }
    return 0;
  }
};

class RGWDataSyncShardCR : public RGWCoroutine {
  RGWDataSyncCtx *const sc;
  const rgw_pool pool;
  const uint32_t shard_id;
  rgw_data_sync_marker& sync_marker;
  rgw_data_sync_status sync_status;
  const RGWSyncTraceNodeRef tn;
  RGWObjVersionTracker& objv;
  bool *reset_backoff;

  ceph::mutex inc_lock = ceph::make_mutex("RGWDataSyncShardCR::inc_lock");
  ceph::condition_variable inc_cond;

  RGWDataSyncEnv *const sync_env{ sc->env };

  const string status_oid{ RGWDataSyncStatusManager::shard_obj_name(
      sc->source_zone, shard_id) };
  const rgw_raw_obj error_repo{ pool, status_oid + ".retry" };

  // target number of entries to cache before recycling idle ones
  static constexpr size_t target_cache_size = 256;
  boost::intrusive_ptr<rgw::bucket_sync::Cache> bucket_shard_cache {
    rgw::bucket_sync::Cache::create(target_cache_size) };

  boost::intrusive_ptr<RGWContinuousLeaseCR> lease_cr;
  boost::intrusive_ptr<RGWCoroutinesStack> lease_stack;

  bc::flat_set<rgw_data_notify_entry> modified_shards;

public:
  RGWDataSyncShardCR(RGWDataSyncCtx* const _sc, const rgw_pool& pool,
                     const uint32_t shard_id, rgw_data_sync_marker& marker,
                     const rgw_data_sync_status& sync_status,
                     RGWSyncTraceNodeRef& tn, RGWObjVersionTracker& objv, bool *reset_backoff)
    : RGWCoroutine(_sc->cct), sc(_sc), pool(pool), shard_id(shard_id),
      sync_marker(marker), sync_status(sync_status), tn(tn),
      objv(objv), reset_backoff(reset_backoff) {
    set_description() << "data sync shard source_zone=" << sc->source_zone
		      << " shard_id=" << shard_id;
  }

  ~RGWDataSyncShardCR() override {
    if (lease_cr) {
      lease_cr->abort();
    }
  }

  void append_modified_shards(bc::flat_set<rgw_data_notify_entry>& entries) {
    std::lock_guard l{inc_lock};
    modified_shards.insert(entries.begin(), entries.end());
  }

  int operate(const DoutPrefixProvider *dpp) override {
    reenter(this) {

      if (!sc->env->bid_manager->is_highest_bidder(shard_id)) {
        tn->log(10, "not the highest bidder");
        return set_cr_error(-EBUSY);
      }

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
      *reset_backoff = true;
      tn->log(10, "took lease");
      /* Reread data sync status to fetch latest marker and objv */
      objv.clear();
      yield call(new RGWSimpleRadosReadCR<rgw_data_sync_marker>(sync_env->dpp, sync_env->driver,
                                                             rgw_raw_obj(pool, status_oid),
                                                             &sync_marker, true, &objv));
      if (retcode < 0) {
        lease_cr->go_down();
        drain_all();
        return set_cr_error(retcode);
      }

      while (true) {
	if (sync_marker.state == rgw_data_sync_marker::FullSync) {
	  yield call(new RGWDataFullSyncShardCR(sc, pool, shard_id,
						sync_marker, tn,
						status_oid, error_repo,
						lease_cr, sync_status,
            objv, bucket_shard_cache));
	  if (retcode < 0) {
	    if (retcode != -EBUSY) {
	      tn->log(10, SSTR("full sync failed (retcode=" << retcode << ")"));
	    }
	    lease_cr->go_down();
	    drain_all();
	    return set_cr_error(retcode);
	  }
	} else if (sync_marker.state == rgw_data_sync_marker::IncrementalSync) {
	  yield call(new RGWDataIncSyncShardCR(sc, pool, shard_id,
					       sync_marker, tn,
					       status_oid, error_repo,
					       lease_cr, sync_status,
					       objv, bucket_shard_cache,
					       inc_lock, modified_shards));
	  if (retcode < 0) {
	    if (retcode != -EBUSY) {
	      tn->log(10, SSTR("incremental sync failed (retcode=" << retcode
			       << ")"));
	    }
	    lease_cr->go_down();
	    drain_all();
	    return set_cr_error(retcode);
	  }
	} else {
	  lease_cr->go_down();
	  drain_all();
	  return set_cr_error(-EIO);
	}
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
    auto driver = sync_env->driver;
    lease_cr.reset(new RGWContinuousLeaseCR(sync_env->async_rados, driver,
                                            rgw_raw_obj(pool, status_oid),
                                            lock_name, lock_duration, this,
					    &sc->lcc));
    lease_stack.reset(spawn(lease_cr.get(), false));
  }
};

class RGWDataSyncShardControlCR : public RGWBackoffControlCR {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;

  rgw_pool pool;

  uint32_t shard_id;
  rgw_data_sync_marker sync_marker;
  rgw_data_sync_status sync_status;

  RGWSyncTraceNodeRef tn;
  RGWObjVersionTracker& objv;
public:
  RGWDataSyncShardControlCR(RGWDataSyncCtx *_sc, const rgw_pool& _pool,
                           uint32_t _shard_id, rgw_data_sync_marker& _marker,
                           const rgw_data_sync_status& sync_status,
                           RGWObjVersionTracker& objv,
                           RGWSyncTraceNodeRef& _tn_parent)
          : RGWBackoffControlCR(_sc->cct, false),
          sc(_sc), sync_env(_sc->env),
          pool(_pool),
          shard_id(_shard_id),
          sync_marker(_marker), objv(objv) {
    tn = sync_env->sync_tracer->add_node(_tn_parent, "shard", std::to_string(shard_id));
  }

  RGWCoroutine *alloc_cr() override {
    return new RGWDataSyncShardCR(sc, pool, shard_id, sync_marker, sync_status, tn, objv, backoff_ptr());
  }

  RGWCoroutine *alloc_finisher_cr() override {
    return new RGWSimpleRadosReadCR<rgw_data_sync_marker>(sync_env->dpp, sync_env->driver,
                                                          rgw_raw_obj(sync_env->svc->zone->get_zone_params().log_pool, RGWDataSyncStatusManager::shard_obj_name(sc->source_zone, shard_id)),
                                                          &sync_marker, true, &objv);
  }

  void append_modified_shards(bc::flat_set<rgw_data_notify_entry>& keys) {
    std::lock_guard l{cr_lock()};

    RGWDataSyncShardCR *cr = static_cast<RGWDataSyncShardCR *>(get_cr());
    if (!cr) {
      return;
    }

    cr->append_modified_shards(keys);
  }
};

class RGWDataSyncShardNotifyCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  RGWSyncTraceNodeRef tn;

public:
  RGWDataSyncShardNotifyCR(RGWDataSyncEnv *_sync_env, RGWSyncTraceNodeRef& _tn)
    : RGWCoroutine(_sync_env->cct),
      sync_env(_sync_env), tn(_tn) {}

  int operate(const DoutPrefixProvider* dpp) override
  {
    reenter(this) {
      for (;;) {
        set_status("sync lock notification");
        yield call(sync_env->bid_manager->notify_cr());
        if (retcode < 0) {
          tn->log(5, SSTR("ERROR: failed to notify bidding information" << retcode));
          return set_cr_error(retcode);
        }

        set_status("sleeping");
        yield wait(utime_t(cct->_conf->rgw_sync_lease_period, 0));
      }

    }
    return 0;
  }
};

class RGWDataSyncCR : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  uint32_t num_shards;

  rgw_data_sync_status sync_status;
  std::vector<RGWObjVersionTracker> objvs;

  ceph::mutex shard_crs_lock =
    ceph::make_mutex("RGWDataSyncCR::shard_crs_lock");
  map<int, RGWDataSyncShardControlCR *> shard_crs;

  bool *reset_backoff;

  RGWSyncTraceNodeRef tn;

  RGWDataSyncModule *data_sync_module{nullptr};

  boost::intrusive_ptr<RGWContinuousLeaseCR> init_lease;
  boost::intrusive_ptr<RGWCoroutinesStack> lease_stack;
  boost::intrusive_ptr<RGWCoroutinesStack> notify_stack;

  RGWObjVersionTracker obj_version;
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
    if (init_lease) {
      init_lease->abort();
    }
  }

  int operate(const DoutPrefixProvider *dpp) override {
    reenter(this) {

      yield {
        ldpp_dout(dpp, 10) << "broadcast sync lock notify" << dendl;
        notify_stack.reset(spawn(new RGWDataSyncShardNotifyCR(sync_env, tn), false));
      }

      /* read sync status */
      yield call(new RGWReadDataSyncStatusCoroutine(sc, &sync_status,
                                                    &obj_version, objvs));

      data_sync_module = sync_env->sync_module->get_data_handler();

      if (retcode < 0 && retcode != -ENOENT) {
        tn->log(0, SSTR("ERROR: failed to fetch sync status, retcode=" << retcode));
        return set_cr_error(retcode);
      }

      if ((rgw_data_sync_info::SyncState)sync_status.sync_info.state !=
	  rgw_data_sync_info::StateSync) {
	init_lease.reset(
	  RGWInitDataSyncStatusCoroutine::continuous_lease_cr(sc, this));
	yield lease_stack.reset(spawn(init_lease.get(), false));

	while (!init_lease->is_locked()) {
	  if (init_lease->is_done()) {
	    tn->log(5, "ERROR: failed to take data sync status lease");
	    set_status("lease lock failed, early abort");
	    drain_all_but_stack(notify_stack.get());
	    return set_cr_error(init_lease->get_ret_status());
	  }
	  tn->log(5, "waiting on data sync status lease");
	  yield set_sleeping(true);
	}
	tn->log(5, "acquired data sync status lease");

	// Reread sync status now that we've acquired the lock!
	obj_version.clear();
	yield call(new RGWReadDataSyncStatusCoroutine(sc, &sync_status, &obj_version, objvs));
	if (retcode < 0) {
	  tn->log(0, SSTR("ERROR: failed to fetch sync status, retcode=" << retcode));
	  return set_cr_error(retcode);
	}
      }

      /* state: init status */
      if ((rgw_data_sync_info::SyncState)sync_status.sync_info.state == rgw_data_sync_info::StateInit) {
        tn->log(20, SSTR("init"));
        sync_status.sync_info.num_shards = num_shards;
        uint64_t instance_id;
        instance_id = ceph::util::generate_random_number<uint64_t>();
        yield call(new RGWInitDataSyncStatusCoroutine(sc, num_shards, instance_id, tn,
                                                      &sync_status, init_lease, obj_version, objvs));
        if (retcode < 0) {
          tn->log(0, SSTR("ERROR: failed to init sync, retcode=" << retcode));
	  init_lease->go_down();
	  drain_all_but_stack(notify_stack.get());
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

        if (!init_lease->is_locked()) {
          init_lease->go_down();
          drain_all_but_stack(notify_stack.get());
          return set_cr_error(-ECANCELED);
        }
        /* state: building full sync maps */
        yield call(new RGWListBucketIndexesCR(sc, &sync_status, objvs));
        if (retcode < 0) {
          tn->log(0, SSTR("ERROR: failed to build full sync maps, retcode=" << retcode));
          return set_cr_error(retcode);
        }
        sync_status.sync_info.state = rgw_data_sync_info::StateSync;

        if (!init_lease->is_locked()) {
          init_lease->go_down();
          drain_all_but_stack(notify_stack.get());
          return set_cr_error(-ECANCELED);
        }
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

      if ((rgw_data_sync_info::SyncState)sync_status.sync_info.state == rgw_data_sync_info::StateSync) {
        if (init_lease) {
          init_lease->go_down();
          drain_all_but_stack(notify_stack.get());
          init_lease.reset();
          lease_stack.reset();
        }
        yield {
          tn->log(10, SSTR("spawning " << num_shards << " shards sync"));
          for (map<uint32_t, rgw_data_sync_marker>::iterator iter = sync_status.sync_markers.begin();
               iter != sync_status.sync_markers.end(); ++iter) {
            RGWDataSyncShardControlCR *cr = new RGWDataSyncShardControlCR(sc, sync_env->svc->zone->get_zone_params().log_pool,
                                                                          iter->first, iter->second, sync_status, objvs[iter->first], tn);
            cr->get();
            shard_crs_lock.lock();
            shard_crs[iter->first] = cr;
            shard_crs_lock.unlock();
            spawn(cr, true);
          }
        }
      }

      notify_stack->cancel();

      return set_cr_done();
    }
    return 0;
  }

  RGWCoroutine *set_sync_info_cr() {
    return new RGWSimpleRadosWriteCR<rgw_data_sync_info>(sync_env->dpp, sync_env->driver,
                                                         rgw_raw_obj(sync_env->svc->zone->get_zone_params().log_pool, RGWDataSyncStatusManager::sync_status_oid(sc->source_zone)),
                                                         sync_status.sync_info, &obj_version);
  }

  void wakeup(int shard_id, bc::flat_set<rgw_data_notify_entry>& entries) {
    std::lock_guard l{shard_crs_lock};
    map<int, RGWDataSyncShardControlCR *>::iterator iter = shard_crs.find(shard_id);
    if (iter == shard_crs.end()) {
      return;
    }
    iter->second->append_modified_shards(entries);
    iter->second->wakeup();
  }
};

class RGWDefaultDataSyncModule : public RGWDataSyncModule {
public:
  RGWDefaultDataSyncModule() {}

  RGWCoroutine *sync_object(const DoutPrefixProvider *dpp, RGWDataSyncCtx *sc,
                            rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key,
                            std::optional<uint64_t> versioned_epoch,
                            const rgw_zone_set_entry& source_trace_entry,
                            rgw_zone_set *zones_trace) override;
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
    int operate() override {
      auto user_ctl = sync_env->driver->getRados()->ctl.user;

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
                                            info->user_acl,
                                            bucket_acl,
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
                                            bucket_acl,
                                            obj_acl,
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
  bool stat_follow_olh;
  const rgw_zone_set_entry& source_trace_entry;
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
                bool _stat_follow_olh,
                const rgw_zone_set_entry& source_trace_entry,
                rgw_zone_set *_zones_trace) : RGWCoroutine(_sc->cct),
                                              sc(_sc), sync_env(_sc->env),
                                              sync_pipe(_sync_pipe),
                                              key(_key),
                                              dest_key(_dest_key),
                                              versioned_epoch(_versioned_epoch),
                                              stat_follow_olh(_stat_follow_olh),
                                              source_trace_entry(source_trace_entry),
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
                                            sync_env->driver,
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
            ldout(cct, 0) << "ERROR: " << __func__ << ": permission check failed: user not allowed to write into bucket (bucket=" << sync_pipe.info.dest_bucket.get_key() << ")" << dendl;
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

          call(new RGWFetchRemoteObjCR(sync_env->async_rados, sync_env->driver, sc->source_zone,
                                       nullopt,
                                       sync_pipe.source_bucket_info.bucket,
                                       std::nullopt, sync_pipe.dest_bucket_info,
                                       key, dest_key, versioned_epoch,
                                       true,
                                       std::static_pointer_cast<RGWFetchObjFilter>(filter),
                                       stat_follow_olh,
                                       source_trace_entry, zones_trace,
                                       sync_env->counters, dpp));
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

RGWCoroutine *RGWDefaultDataSyncModule::sync_object(const DoutPrefixProvider *dpp, RGWDataSyncCtx *sc,
                                                    rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key,
                                                    std::optional<uint64_t> versioned_epoch,
                                                    const rgw_zone_set_entry& source_trace_entry,
                                                    rgw_zone_set *zones_trace)
{
  bool stat_follow_olh = false;
  return new RGWObjFetchCR(sc, sync_pipe, key, std::nullopt, versioned_epoch, stat_follow_olh,
                           source_trace_entry, zones_trace);
}

RGWCoroutine *RGWDefaultDataSyncModule::remove_object(const DoutPrefixProvider *dpp, RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key,
                                                      real_time& mtime, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace)
{
  auto sync_env = sc->env;
  return new RGWRemoveObjCR(sync_env->dpp, sync_env->async_rados, sync_env->driver, sc->source_zone,
                            sync_pipe.dest_bucket_info, key, versioned, versioned_epoch,
                            NULL, NULL, false, &mtime, zones_trace);
}

RGWCoroutine *RGWDefaultDataSyncModule::create_delete_marker(const DoutPrefixProvider *dpp, RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key, real_time& mtime,
                                                             rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace)
{
  auto sync_env = sc->env;
  return new RGWRemoveObjCR(sync_env->dpp, sync_env->async_rados, sync_env->driver, sc->source_zone,
                            sync_pipe.dest_bucket_info, key, versioned, versioned_epoch,
                            &owner.id, &owner.display_name, true, &mtime, zones_trace);
}

class RGWArchiveDataSyncModule : public RGWDefaultDataSyncModule {
public:
  RGWArchiveDataSyncModule() {}

  RGWCoroutine *sync_object(const DoutPrefixProvider *dpp, RGWDataSyncCtx *sc,
                            rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key,
                            std::optional<uint64_t> versioned_epoch,
                            const rgw_zone_set_entry& source_trace_entry,
                            rgw_zone_set *zones_trace) override;
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
  RGWBucketInstanceMetadataHandlerBase *alloc_bucket_instance_meta_handler(rgw::sal::Driver* driver) override {
    return RGWArchiveBucketInstanceMetaHandlerAllocator::alloc(driver);
  }
};

int RGWArchiveSyncModule::create_instance(const DoutPrefixProvider *dpp, CephContext *cct, const JSONFormattable& config, RGWSyncModuleInstanceRef *instance)
{
  instance->reset(new RGWArchiveSyncModuleInstance());
  return 0;
}

RGWCoroutine *RGWArchiveDataSyncModule::sync_object(const DoutPrefixProvider *dpp, RGWDataSyncCtx *sc,
                                                    rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key,
                                                    std::optional<uint64_t> versioned_epoch,
                                                    const rgw_zone_set_entry& source_trace_entry,
                                                    rgw_zone_set *zones_trace)
{
  auto sync_env = sc->env;
  ldout(sc->cct, 5) << "SYNC_ARCHIVE: sync_object: b=" << sync_pipe.info.source_bs.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch.value_or(0) << dendl;

  std::optional<rgw_obj_key> dest_key;
  bool stat_follow_olh = false;


  if (versioned_epoch.value_or(0) == 0) { /* force version if not set */
    stat_follow_olh = true;
    versioned_epoch = 0;
    dest_key = key;
    if (key.instance.empty()) {
      sync_env->driver->getRados()->gen_rand_obj_instance_name(&(*dest_key));
    }
  }

  if (key.instance.empty()) {
    dest_key = key;
    sync_env->driver->getRados()->gen_rand_obj_instance_name(&(*dest_key));
  }

  return new RGWObjFetchCR(sc, sync_pipe, key, dest_key, versioned_epoch,
                           stat_follow_olh, source_trace_entry, zones_trace);
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
  return new RGWRemoveObjCR(sync_env->dpp, sync_env->async_rados, sync_env->driver, sc->source_zone,
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

  void wakeup(int shard_id, bc::flat_set<rgw_data_notify_entry>& entries) {
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
      cr->wakeup(shard_id, entries);
    }

    cr->put();
  }
};

void RGWRemoteDataLog::wakeup(int shard_id, bc::flat_set<rgw_data_notify_entry>& entries) {
  std::shared_lock rl{lock};
  if (!data_sync_cr) {
    return;
  }
  data_sync_cr->wakeup(shard_id, entries);
}

int RGWRemoteDataLog::run_sync(const DoutPrefixProvider *dpp, int num_shards)
{
  // construct and start bid manager for data sync fairness
  const auto& control_pool = sc.env->driver->svc()->zone->get_zone_params().control_pool;
  char buf[data_sync_bids_oid.size() + sc.source_zone.id.size() + 16];
  snprintf(buf, sizeof(buf), "%s.%s", data_sync_bids_oid.c_str(), sc.source_zone.id.c_str());
  auto control_obj = rgw_raw_obj{control_pool, string(buf)};

  auto bid_manager = rgw::sync_fairness::create_rados_bid_manager(
      driver, control_obj, num_shards);
  int r = bid_manager->start();
  if (r < 0) {
    return r;
  }
  sc.env->bid_manager = bid_manager.get();

  lock.lock();
  data_sync_cr = new RGWDataSyncControlCR(&sc, num_shards, tn);
  data_sync_cr->get(); // run() will drop a ref, so take another
  lock.unlock();

  r = run(dpp, data_sync_cr);

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
  return driver->ctx();
}

int RGWDataSyncStatusManager::init(const DoutPrefixProvider *dpp)
{
  RGWZone *zone_def;

  if (!(zone_def = driver->svc()->zone->find_zone(source_zone))) {
    ldpp_dout(this, 0) << "ERROR: failed to find zone config info for zone=" << source_zone << dendl;
    return -EIO;
  }

  if (!driver->svc()->sync_modules->get_manager()->supports_data_export(zone_def->tier_type)) {
    return -ENOTSUP;
  }

  const RGWZoneParams& zone_params = driver->svc()->zone->get_zone_params();

  if (sync_module == nullptr) {
    sync_module = driver->get_sync_module();
  }

  conn = driver->svc()->zone->get_zone_conn(source_zone);
  if (!conn) {
    ldpp_dout(this, 0) << "connection object to zone " << source_zone << " does not exist" << dendl;
    return -EINVAL;
  }

  error_logger = new RGWSyncErrorLogger(driver, RGW_SYNC_ERROR_LOG_SHARD_PREFIX, ERROR_LOGGER_SHARDS);

  int r = source_log.init(source_zone, conn, error_logger, driver->getRados()->get_sync_tracer(),
                          sync_module, counters);
  if (r < 0) {
    ldpp_dout(this, 0) << "ERROR: failed to init remote log, r=" << r << dendl;
    finalize();
    return r;
  }

  rgw_datalog_info datalog_info;
  r = source_log.read_log_info(dpp, &datalog_info);
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

class RGWInitBucketShardSyncStatusCoroutine : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;

  const rgw_bucket_sync_pair_info& sync_pair;
  const string sync_status_oid;

  rgw_bucket_shard_sync_info& status;
  RGWObjVersionTracker& objv_tracker;
  const BucketIndexShardsManager& marker_mgr;
  bool exclusive;
public:
  RGWInitBucketShardSyncStatusCoroutine(RGWDataSyncCtx *_sc,
                                        const rgw_bucket_sync_pair_info& _sync_pair,
                                        rgw_bucket_shard_sync_info& _status,
                                        uint64_t gen,
                                        const BucketIndexShardsManager& _marker_mgr,
                                        RGWObjVersionTracker& objv_tracker,
                                        bool exclusive)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
      sync_pair(_sync_pair),
      sync_status_oid(RGWBucketPipeSyncStatusManager::inc_status_oid(sc->source_zone, _sync_pair, gen)),
      status(_status), objv_tracker(objv_tracker), marker_mgr(_marker_mgr), exclusive(exclusive)
  {}

  int operate(const DoutPrefixProvider *dpp) override {
    reenter(this) {
      yield {
        rgw_raw_obj obj(sync_env->svc->zone->get_zone_params().log_pool, sync_status_oid);

        // whether or not to do full sync, incremental sync will follow anyway
        if (sync_env->sync_module->should_full_sync()) {
          const auto max_marker = marker_mgr.get(sync_pair.source_bs.shard_id, "");
          status.inc_marker.position = max_marker;
        }
        status.inc_marker.timestamp = ceph::real_clock::now();
        status.state = rgw_bucket_shard_sync_info::StateIncrementalSync;

        map<string, bufferlist> attrs;
        status.encode_all_attrs(attrs);
        call(new RGWSimpleRadosWriteAttrsCR(dpp, sync_env->driver,
                                            obj, attrs, &objv_tracker, exclusive));
      }

      if (retcode < 0) {
        ldout(cct, 20) << "ERROR: init marker position failed. error: " << retcode << dendl;
        return set_cr_error(retcode);
      }
      ldout(cct, 20) << "init marker position: " << status.inc_marker.position << 
        ". written to shard status object: " << sync_status_oid << dendl;
      return set_cr_done();
    }
    return 0;
  }
};

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
  if (!decode_attr(cct, attrs, BUCKET_SYNC_ATTR_PREFIX "inc_marker", &inc_marker)) {
    decode_attr(cct, attrs, "inc_marker", &inc_marker);
  }
}

void rgw_bucket_shard_sync_info::encode_all_attrs(map<string, bufferlist>& attrs)
{
  encode_state_attr(attrs);
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
                                   RGWObjVersionTracker* objv_tracker,
                                   uint64_t gen)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
      oid(RGWBucketPipeSyncStatusManager::inc_status_oid(sc->source_zone, sync_pair, gen)),
      status(_status), objv_tracker(objv_tracker)
  {}
  int operate(const DoutPrefixProvider *dpp) override;
};

int RGWReadBucketPipeSyncStatusCoroutine::operate(const DoutPrefixProvider *dpp)
{
  reenter(this) {
    yield call(new RGWSimpleRadosReadAttrsCR(dpp, sync_env->driver,
                                             rgw_raw_obj(sync_env->svc->zone->get_zone_params().log_pool, oid),
                                             &attrs, true, objv_tracker));
    if (retcode == -ENOENT) {
      *status = rgw_bucket_shard_sync_info();
      return set_cr_done();
    }
    if (retcode < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to call fetch bucket shard info oid=" << oid << " ret=" << retcode << dendl;
      return set_cr_error(retcode);
    }
    status->decode_from_attrs(sync_env->cct, attrs);
    return set_cr_done();
  }
  return 0;
}

// wrap ReadSyncStatus and set a flag if it's not in incremental
class CheckBucketShardStatusIsIncremental : public RGWReadBucketPipeSyncStatusCoroutine {
  bool* result;
  rgw_bucket_shard_sync_info status;
 public:
  CheckBucketShardStatusIsIncremental(RGWDataSyncCtx* sc,
                                      const rgw_bucket_sync_pair_info& sync_pair,
                                      bool* result)
    : RGWReadBucketPipeSyncStatusCoroutine(sc, sync_pair, &status, nullptr, 0 /*no gen in compat mode*/),
      result(result)
  {}

  int operate(const DoutPrefixProvider *dpp) override {
    int r = RGWReadBucketPipeSyncStatusCoroutine::operate(dpp);
    if (state == RGWCoroutine_Done &&
        status.state != rgw_bucket_shard_sync_info::StateIncrementalSync) {
      *result = false;
    }
    return r;
  }
};

class CheckAllBucketShardStatusIsIncremental : public RGWShardCollectCR {
  // start with 1 shard, and only spawn more if we detect an existing shard.
  // this makes the backward compatibility check far less expensive in the
  // general case where no shards exist
  static constexpr int initial_concurrent_shards = 1;
  static constexpr int max_concurrent_shards = 16;

  RGWDataSyncCtx* sc;
  rgw_bucket_sync_pair_info sync_pair;
  const int num_shards;
  bool* result;
  int shard = 0;
 public:
  CheckAllBucketShardStatusIsIncremental(RGWDataSyncCtx* sc,
                                         const rgw_bucket_sync_pair_info& sync_pair,
                                         int num_shards, bool* result)
    : RGWShardCollectCR(sc->cct, initial_concurrent_shards),
      sc(sc), sync_pair(sync_pair), num_shards(num_shards), result(result)
  {}

  bool spawn_next() override {
    // stop spawning if we saw any errors or non-incremental shards
    if (shard >= num_shards || status < 0 || !*result) {
      return false;
    }
    sync_pair.source_bs.shard_id = shard++;
    spawn(new CheckBucketShardStatusIsIncremental(sc, sync_pair, result), false);
    return true;
  }

 private:
  int handle_result(int r) override {
    if (r < 0) {
      ldout(cct, 4) << "failed to read bucket shard status: "
          << cpp_strerror(r) << dendl;
    } else if (shard == 0) {
      // enable concurrency once the first shard succeeds
      max_concurrent = max_concurrent_shards;
    }
    return r;
  }
};

// wrap InitBucketShardSyncStatus with local storage for 'status' and 'objv'
// and a loop to retry on racing writes
class InitBucketShardStatusCR : public RGWCoroutine {
  RGWDataSyncCtx* sc;
  rgw_bucket_sync_pair_info pair;
  rgw_bucket_shard_sync_info status;
  RGWObjVersionTracker objv;
  const uint64_t gen;
  const BucketIndexShardsManager& marker_mgr;

 public:
  InitBucketShardStatusCR(RGWDataSyncCtx* sc,
                         const rgw_bucket_sync_pair_info& pair,
                         uint64_t gen,
                         const BucketIndexShardsManager& marker_mgr)
    : RGWCoroutine(sc->cct), sc(sc), pair(pair), gen(gen), marker_mgr(marker_mgr)
  {}
  int operate(const DoutPrefixProvider *dpp) {
    reenter(this) {
      // non exclusive create with empty status
      objv.generate_new_write_ver(cct);
      yield call(new RGWInitBucketShardSyncStatusCoroutine(sc, pair, status, gen, marker_mgr, objv, false));
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }
};

class InitBucketShardStatusCollectCR : public RGWShardCollectCR {
  static constexpr int max_concurrent_shards = 16;
  RGWDataSyncCtx* sc;
  rgw_bucket_sync_pair_info sync_pair;
  const uint64_t gen;
  const BucketIndexShardsManager& marker_mgr;

  const int num_shards;
  int shard = 0;

  int handle_result(int r) override {
    if (r < 0) {
      ldout(cct, 4) << "failed to init bucket shard status: "
          << cpp_strerror(r) << dendl;
    }
    return r;
  }
 public:
  InitBucketShardStatusCollectCR(RGWDataSyncCtx* sc,
                                 const rgw_bucket_sync_pair_info& sync_pair,
                                 uint64_t gen,
                                 const BucketIndexShardsManager& marker_mgr,
                                 int num_shards)
    : RGWShardCollectCR(sc->cct, max_concurrent_shards),
      sc(sc), sync_pair(sync_pair), gen(gen), marker_mgr(marker_mgr), num_shards(num_shards)
  {}

  bool spawn_next() override {
    if (shard >= num_shards || status < 0) { // stop spawning on any errors
      return false;
    }
    sync_pair.source_bs.shard_id = shard++;
    spawn(new InitBucketShardStatusCR(sc, sync_pair, gen, marker_mgr), false);
    return true;
  }
};

class RemoveBucketShardStatusCR : public RGWCoroutine {
  RGWDataSyncCtx* const sc;
  RGWDataSyncEnv* const sync_env;

  rgw_bucket_sync_pair_info sync_pair;
  rgw_raw_obj obj;
  RGWObjVersionTracker objv;

public:
  RemoveBucketShardStatusCR(RGWDataSyncCtx* sc,
                             const rgw_bucket_sync_pair_info& sync_pair, uint64_t gen)
    : RGWCoroutine(sc->cct), sc(sc), sync_env(sc->env),
      sync_pair(sync_pair),
      obj(sync_env->svc->zone->get_zone_params().log_pool, 
          RGWBucketPipeSyncStatusManager::inc_status_oid(sc->source_zone, sync_pair, gen))
  {}

  int operate(const DoutPrefixProvider *dpp) override {
    reenter(this) {
      yield call(new RGWRadosRemoveCR(sync_env->driver, obj, &objv));
 			if (retcode < 0 && retcode != -ENOENT) {
        ldout(cct, 20) << "ERROR: failed to remove bucket shard status for: " << sync_pair << 
          ". with error: " << retcode << dendl;
        return set_cr_error(retcode);
      }
      ldout(cct, 20) << "removed bucket shard status object: " << obj.oid << dendl;
      return set_cr_done();
    }
    return 0;
  }
};

class RemoveBucketShardStatusCollectCR : public RGWShardCollectCR {
  static constexpr int max_concurrent_shards = 16;
  RGWDataSyncCtx* const sc;
  RGWDataSyncEnv* const sync_env;
  rgw_bucket_sync_pair_info sync_pair;
  const uint64_t gen;

  const int num_shards;
  int shard = 0;

  int handle_result(int r) override {
    if (r < 0) {
      ldout(cct, 4) << "failed to remove bucket shard status object: "
          << cpp_strerror(r) << dendl;
    }
    return r;
  }
 public:
  RemoveBucketShardStatusCollectCR(RGWDataSyncCtx* sc,
                                 const rgw_bucket_sync_pair_info& sync_pair,
                                 uint64_t gen,
                                 int num_shards)
    : RGWShardCollectCR(sc->cct, max_concurrent_shards),
      sc(sc), sync_env(sc->env), sync_pair(sync_pair), gen(gen), num_shards(num_shards)
  {}

  bool spawn_next() override {
    if (shard >= num_shards) {
      return false;
    }
    sync_pair.source_bs.shard_id = shard++;
    spawn(new RemoveBucketShardStatusCR(sc, sync_pair, gen),  false);
    return true;
  }
};

class InitBucketFullSyncStatusCR : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;

  const rgw_bucket_sync_pair_info& sync_pair;
  const rgw_raw_obj& status_obj;
  rgw_bucket_sync_status& status;
  RGWObjVersionTracker& objv;
  const RGWBucketInfo& source_info;
  const bool check_compat;

  const rgw_bucket_index_marker_info& info;
  BucketIndexShardsManager marker_mgr;

  bool all_incremental = true;
  bool no_zero = false;

public:
  InitBucketFullSyncStatusCR(RGWDataSyncCtx* sc,
                             const rgw_bucket_sync_pair_info& sync_pair,
                             const rgw_raw_obj& status_obj,
                             rgw_bucket_sync_status& status,
                             RGWObjVersionTracker& objv,
			     const RGWBucketInfo& source_info,
                             bool check_compat,
                             const rgw_bucket_index_marker_info& info)
    : RGWCoroutine(sc->cct), sc(sc), sync_env(sc->env),
      sync_pair(sync_pair), status_obj(status_obj),
      status(status), objv(objv), source_info(source_info),
      check_compat(check_compat), info(info)
  {}

  int operate(const DoutPrefixProvider *dpp) override {
    reenter(this) {
      retcode = marker_mgr.from_string(info.max_marker, -1);
      if (retcode < 0) {
        lderr(cct) << "failed to parse bilog shard markers: "
            << cpp_strerror(retcode) << dendl;
        return set_cr_error(retcode);
      }

      status.state = BucketSyncState::Init;

      if (info.oldest_gen == 0) {
	if (check_compat) {
	  // use shard count from our log gen=0
	  // try to convert existing per-shard incremental status for backward compatibility
	  if (source_info.layout.logs.empty() ||
	      source_info.layout.logs.front().gen > 0) {
	    ldpp_dout(dpp, 20) << "no generation zero when checking compatibility" << dendl;
	    no_zero = true;
	  } else if (auto& log = source_info.layout.logs.front();
                     log.layout.type != rgw::BucketLogType::InIndex) {
	    ldpp_dout(dpp, 20) << "unrecognized log layout type when checking compatibility " << log.layout.type << dendl;
	    no_zero = true;
	  }
	  if (!no_zero) {
	    yield {
	      const int num_shards0 = rgw::num_shards(
		source_info.layout.logs.front().layout.in_index.layout);
	      call(new CheckAllBucketShardStatusIsIncremental(sc, sync_pair,
							      num_shards0,
							      &all_incremental));
	    }
	    if (retcode < 0) {
	      return set_cr_error(retcode);
	    }
	    if (all_incremental) {
	      // we can use existing status and resume incremental sync
	      status.state = BucketSyncState::Incremental;
	    }
	  } else {
	    all_incremental = false;
	  }
	}
      }

      if (status.state != BucketSyncState::Incremental) {
	// initialize all shard sync status. this will populate the log marker
        // positions where incremental sync will resume after full sync
	yield {
	  const int num_shards = marker_mgr.get().size();
	  call(new InitBucketShardStatusCollectCR(sc, sync_pair, info.latest_gen, marker_mgr, num_shards));
	}
	if (retcode < 0) {
          ldout(cct, 20) << "failed to init bucket shard status: "
			 << cpp_strerror(retcode) << dendl;
	  return set_cr_error(retcode);
        }

        if (sync_env->sync_module->should_full_sync()) {
          status.state = BucketSyncState::Full;
        } else {
          status.state = BucketSyncState::Incremental;
        }
      }

      status.shards_done_with_gen.resize(marker_mgr.get().size());
      status.incremental_gen = info.latest_gen;

      ldout(cct, 20) << "writing bucket sync status during init. state=" << status.state << ". marker=" << status.full.position << dendl;

      // write bucket sync status
      using CR = RGWSimpleRadosWriteCR<rgw_bucket_sync_status>;
      yield call(new CR(dpp, sync_env->driver,
			status_obj, status, &objv, false));
      if (retcode < 0) {
        ldout(cct, 20) << "failed to write bucket shard status: "
            << cpp_strerror(retcode) << dendl;
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }
};

#define OMAP_READ_MAX_ENTRIES 10
class RGWReadRecoveringBucketShardsCoroutine : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  rgw::sal::RadosStore* driver;
  
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
  driver(sync_env->driver), shard_id(_shard_id), max_entries(_max_entries),
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
      yield call(new RGWRadosGetOmapKeysCR(driver, rgw_raw_obj(sync_env->svc->zone->get_zone_params().log_pool, error_oid),
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
      for (const std::string& key : error_entries) {
        rgw_bucket_shard bs;
        std::optional<uint64_t> gen;
        if (int r = rgw::error_repo::decode_key(key, bs, gen); r < 0) {
          // insert the key as-is
          recovering_buckets.insert(std::move(key));
        } else if (gen) {
          recovering_buckets.insert(fmt::format("{}[{}]", bucket_shard_str{bs}, *gen));
        } else {
          recovering_buckets.insert(fmt::format("{}[full]", bucket_shard_str{bs}));
        }
      }
    } while (omapkeys->more && count < max_entries);
  
    return set_cr_done();
  }

  return 0;
}

class RGWReadPendingBucketShardsCoroutine : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  rgw::sal::RadosStore* driver;

  const int shard_id;
  int max_entries;

  set<string>& pending_buckets;
  string marker;
  string status_oid;

  rgw_data_sync_marker* sync_marker;
  int count;

  std::string next_marker;
  vector<rgw_data_change_log_entry> log_entries;
  bool truncated;

public:
  RGWReadPendingBucketShardsCoroutine(RGWDataSyncCtx *_sc, const int _shard_id,
                                      set<string>& _pending_buckets,
                                      rgw_data_sync_marker* _sync_marker, const int _max_entries) 
  : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
  driver(sync_env->driver), shard_id(_shard_id), max_entries(_max_entries),
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
    yield call(new CR(dpp, sync_env->driver,
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
      yield call(new RGWReadRemoteDataLogShardCR(sc, shard_id, marker,
                                                 &next_marker, &log_entries, &truncated));

      if (retcode == -ENOENT) {
        break;
      }

      if (retcode < 0) {
        ldpp_dout(dpp, 0) << "failed to read remote data log info with " 
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

int RGWRemoteDataLog::read_shard_status(const DoutPrefixProvider *dpp, int shard_id, set<string>& pending_buckets, set<string>& recovering_buckets, rgw_data_sync_marker *sync_marker, const int max_entries)
{
  // cannot run concurrently with run_sync(), so run in a separate manager
  RGWCoroutinesManager crs(driver->ctx(), driver->getRados()->get_cr_registry());
  RGWHTTPManager http_manager(driver->ctx(), crs.get_completion_mgr());
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
  RGWCoroutinesStack* recovering_stack = new RGWCoroutinesStack(driver->ctx(), &crs);
  recovering_stack->call(new RGWReadRecoveringBucketShardsCoroutine(&sc_local, shard_id, recovering_buckets, max_entries));
  stacks.push_back(recovering_stack);
  RGWCoroutinesStack* pending_stack = new RGWCoroutinesStack(driver->ctx(), &crs);
  pending_stack->call(new RGWReadPendingBucketShardsCoroutine(&sc_local, shard_id, pending_buckets, sync_marker, max_entries));
  stacks.push_back(pending_stack);
  ret = crs.run(dpp, stacks);
  http_manager.stop();
  return ret;
}

CephContext *RGWBucketPipeSyncStatusManager::get_cct() const
{
  return driver->ctx();
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

class RGWListRemoteBucketCR: public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  const rgw_bucket_shard& bs;
  rgw_obj_key marker_position;

  bucket_list_result *result;

public:
  RGWListRemoteBucketCR(RGWDataSyncCtx *_sc, const rgw_bucket_shard& bs,
                        rgw_obj_key& _marker_position, bucket_list_result *_result)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env), bs(bs),
      marker_position(_marker_position), result(_result) {}

  int operate(const DoutPrefixProvider *dpp) override {
    reenter(this) {
      yield {
        rgw_http_param_pair pairs[] = { { "versions" , NULL },
					{ "format" , "json" },
					{ "objs-container" , "true" },
					{ "key-marker" , marker_position.name.c_str() },
					{ "version-id-marker" , marker_position.instance.c_str() },
	                                { NULL, NULL } };
        string p = string("/") + bs.bucket.get_key(':', 0);
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

struct next_bilog_result {
  uint64_t generation = 0;
  int num_shards = 0;

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("generation", generation, obj);
    JSONDecoder::decode_json("num_shards", num_shards, obj);
  }
};

struct bilog_list_result {
  list<rgw_bi_log_entry> entries;
  bool truncated{false};
  std::optional<next_bilog_result> next_log;

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("entries", entries, obj);
    JSONDecoder::decode_json("truncated", truncated, obj);
    JSONDecoder::decode_json("next_log", next_log, obj);
  }
};

class RGWListBucketIndexLogCR: public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  const string instance_key;
  string marker;

  bilog_list_result *result;
  std::optional<PerfGuard> timer;
  uint64_t generation;
  std::string gen_str = std::to_string(generation);
  uint32_t format_ver{1};

public:
  RGWListBucketIndexLogCR(RGWDataSyncCtx *_sc, const rgw_bucket_shard& bs, string& _marker,
                          uint64_t _generation, bilog_list_result *_result)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
      instance_key(bs.get_key()), marker(_marker), result(_result), generation(_generation) {}

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
					{ "generation", gen_str.c_str() },
					{ "format-ver", "2"},
	                                { NULL, NULL } };

        call(new RGWReadRESTResourceCR<bilog_list_result>(sync_env->cct, sc->conn, sync_env->http_manager,
                                                      "/admin/log", pairs, result));
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

class RGWBucketFullSyncMarkerTrack : public RGWSyncShardMarkerTrack<rgw_obj_key, rgw_obj_key> {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;

  const rgw_raw_obj& status_obj;
  rgw_bucket_sync_status& sync_status;
  RGWSyncTraceNodeRef tn;
  RGWObjVersionTracker& objv_tracker;

public:
  RGWBucketFullSyncMarkerTrack(RGWDataSyncCtx *_sc,
                               const rgw_raw_obj& status_obj,
                               rgw_bucket_sync_status& sync_status,
                               RGWSyncTraceNodeRef tn,
                               RGWObjVersionTracker& objv_tracker)
    : RGWSyncShardMarkerTrack(BUCKET_SYNC_UPDATE_MARKER_WINDOW),
      sc(_sc), sync_env(_sc->env), status_obj(status_obj),
      sync_status(sync_status), tn(std::move(tn)), objv_tracker(objv_tracker)
  {}


  RGWCoroutine *store_marker(const rgw_obj_key& new_marker, uint64_t index_pos, const real_time& timestamp) override {
    sync_status.full.position = new_marker;
    sync_status.full.count = index_pos;

    tn->log(20, SSTR("updating marker oid=" << status_obj.oid << " marker=" << new_marker));
    return new RGWSimpleRadosWriteCR<rgw_bucket_sync_status>(
        sync_env->dpp, sync_env->driver,
	status_obj, sync_status, &objv_tracker);
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
  int operate(const DoutPrefixProvider *dpp) {
    reenter(this) {
      sync_marker.encode_attr(attrs);

      yield call(new RGWSimpleRadosWriteAttrsCR(sync_env->dpp, sync_env->driver,
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

  const rgw_raw_obj& get_obj() const { return obj; }

  RGWCoroutine* store_marker(const string& new_marker, uint64_t index_pos, const real_time& timestamp) override {
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
   * incremental sync of data, and we don't want to run multiple
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

  rgw_zone_set_entry source_trace_entry;
  rgw_zone_set zones_trace;

  RGWSyncTraceNodeRef tn;
  std::string zone_name;

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

    source_trace_entry.zone = sc->source_zone.id;
    source_trace_entry.location_key = _sync_pipe.info.source_bs.bucket.get_key();

    zones_trace = _zones_trace;
    zones_trace.insert(sync_env->svc->zone->get_zone().id, _sync_pipe.info.dest_bucket.get_key());

    if (sc->env->ostr) {
      RGWZone* z;
      if ((z = sc->env->driver->svc()->zone->find_zone(sc->source_zone))) {
	zone_name = z->name;
      }
    }
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
	    if (versioned_epoch) {
	      pretty_print(sc->env, "Syncing object s3://{}/{} version {} in sync from zone {}\n", 
			   bs.bucket.name, key, *versioned_epoch, zone_name);
	    } else {
	      pretty_print(sc->env, "Syncing object s3://{}/{} in sync from zone {}\n",
			   bs.bucket.name, key, zone_name);
	    }
            call(data_sync_module->sync_object(dpp, sc, sync_pipe, key, versioned_epoch,
                                               source_trace_entry, &zones_trace));
          } else if (op == CLS_RGW_OP_DEL || op == CLS_RGW_OP_UNLINK_INSTANCE) {
            set_status("removing obj");
	    if (versioned_epoch) {
	      pretty_print(sc->env, "Deleting object s3://{}/{} version {} in sync from zone {}\n",
			   bs.bucket.name, key, *versioned_epoch, zone_name);
	    } else {
	      pretty_print(sc->env, "Deleting object s3://{}/{} in sync from zone {}\n",
			   bs.bucket.name, key, zone_name);
	    }
            if (op == CLS_RGW_OP_UNLINK_INSTANCE) {
              versioned = true;
            }
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
	  pretty_print(sc->env, "Skipping object s3://{}/{} in sync from zone {}\n",
		       bs.bucket.name, key, zone_name);
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
        yield call(sync_env->error_logger->log_error_cr(dpp, sc->conn->get_remote_id(), "data", error_ss.str(), -retcode, string("failed to sync object") + cpp_strerror(-sync_status)));
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

class RGWBucketFullSyncCR : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  rgw_bucket_sync_pipe& sync_pipe;
  rgw_bucket_sync_status& sync_status;
  rgw_bucket_shard& bs;
  boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr;
  bucket_list_result list_result;
  list<bucket_list_entry>::iterator entries_iter;
  rgw_obj_key list_marker;
  bucket_list_entry *entry{nullptr};

  int total_entries{0};

  int sync_result{0};

  const rgw_raw_obj& status_obj;
  RGWObjVersionTracker& objv;

  rgw_zone_set zones_trace;

  RGWSyncTraceNodeRef tn;
  RGWBucketFullSyncMarkerTrack marker_tracker;

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
  RGWBucketFullSyncCR(RGWDataSyncCtx *_sc,
                      rgw_bucket_sync_pipe& _sync_pipe,
                      const rgw_raw_obj& status_obj,
                      boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr,
                      rgw_bucket_sync_status& sync_status,
                      RGWSyncTraceNodeRef tn_parent,
                      RGWObjVersionTracker& objv_tracker)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
      sync_pipe(_sync_pipe), sync_status(sync_status),
      bs(_sync_pipe.info.source_bs),
      lease_cr(std::move(lease_cr)), status_obj(status_obj), objv(objv_tracker),
      tn(sync_env->sync_tracer->add_node(tn_parent, "full_sync",
                                         SSTR(bucket_shard_str{bs}))),
      marker_tracker(sc, status_obj, sync_status, tn, objv_tracker)
  {
    zones_trace.insert(sc->source_zone.id, sync_pipe.info.dest_bucket.get_key());
    prefix_handler.set_rules(sync_pipe.get_rules());
  }

  int operate(const DoutPrefixProvider *dpp) override;
};

int RGWBucketFullSyncCR::operate(const DoutPrefixProvider *dpp)
{
  reenter(this) {
    list_marker = sync_status.full.position;

    total_entries = sync_status.full.count;
    do {
      if (lease_cr && !lease_cr->is_locked()) {
        tn->log(1, "no lease or lease is lost, abort");
        drain_all();
	yield call(marker_tracker.flush());
        if (retcode < 0) {
          tn->log(0, SSTR("ERROR: bucket full sync marker_tracker.flush() returned retcode=" << retcode));
          return set_cr_error(retcode);
	}
        return set_cr_error(-ECANCELED);
      }
      set_status("listing remote bucket");
      tn->log(20, "listing bucket for full sync");

      if (!prefix_handler.revalidate_marker(&list_marker)) {
        set_status() << "finished iterating over all available prefixes: last marker=" << list_marker;
        tn->log(20, SSTR("finished iterating over all available prefixes: last marker=" << list_marker));
        break;
      }

      yield call(new RGWListRemoteBucketCR(sc, bs, list_marker, &list_result));
      if (retcode < 0 && retcode != -ENOENT) {
        set_status("failed bucket listing, going down");
        drain_all();
        yield spawn(marker_tracker.flush(), true);
        return set_cr_error(retcode);
      }
      if (list_result.entries.size() > 0) {
        tn->set_flag(RGW_SNS_FLAG_ACTIVE); /* actually have entries to sync */
      }
      entries_iter = list_result.entries.begin();
      for (; entries_iter != list_result.entries.end(); ++entries_iter) {
        if (lease_cr && !lease_cr->is_locked()) {
          drain_all();
          yield call(marker_tracker.flush());
          tn->log(1, "no lease or lease is lost, abort");
          if (retcode < 0) {
            tn->log(0, SSTR("ERROR: bucket full sync marker_tracker.flush() returned retcode=" << retcode));
            return set_cr_error(retcode);
          }
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
        drain_with_cb(sc->lcc.adj_concurrency(cct->_conf->rgw_bucket_sync_spawn_window),
                      [&](uint64_t stack_id, int ret) {
                if (ret < 0) {
                  tn->log(10, "a sync operation returned error");
                  sync_result = ret;
                }
                return 0;
              });
      }
    } while (list_result.is_truncated && sync_result == 0);
    set_status("done iterating over all objects");

    /* wait for all operations to complete */
    drain_all_cb([&](uint64_t stack_id, int ret) {
      if (ret < 0) {
        tn->log(10, "a sync operation returned error");
        sync_result = ret;
      }
      return 0;
    });
    tn->unset_flag(RGW_SNS_FLAG_ACTIVE);
    if (lease_cr && !lease_cr->is_locked()) {
      tn->log(1, "no lease or lease is lost, abort");
      yield call(marker_tracker.flush());
      if (retcode < 0) {
        tn->log(0, SSTR("ERROR: bucket full sync marker_tracker.flush() returned retcode=" << retcode));
        return set_cr_error(retcode);
      }
      return set_cr_error(-ECANCELED);
    }
    yield call(marker_tracker.flush());
    if (retcode < 0) {
      tn->log(0, SSTR("ERROR: bucket full sync marker_tracker.flush() returned retcode=" << retcode));
      return set_cr_error(retcode);
    }
    /* update sync state to incremental */
    if (sync_result == 0) {
      sync_status.state = BucketSyncState::Incremental;
      tn->log(5, SSTR("set bucket state=" << sync_status.state));
      yield call(new RGWSimpleRadosWriteCR<rgw_bucket_sync_status>(
	      dpp, sync_env->driver, status_obj, sync_status, &objv));
      tn->log(5, SSTR("bucket status objv=" << objv));
    } else {
      tn->log(10, SSTR("backing out with sync_status=" << sync_result));
    }
    if (retcode < 0 && sync_result == 0) { /* actually tried to set incremental state and failed */
      tn->log(0, SSTR("ERROR: failed to set sync state on bucket "
          << bucket_shard_str{bs} << " retcode=" << retcode));
      return set_cr_error(retcode);
    }
    if (sync_result < 0) {
      return set_cr_error(sync_result);
    }
    return set_cr_done();
  }
  return 0;
}

static bool has_olh_epoch(RGWModifyOp op) {
  return op == CLS_RGW_OP_LINK_OLH || op == CLS_RGW_OP_UNLINK_INSTANCE;
}

class RGWBucketShardIsDoneCR : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  rgw_bucket_sync_status bucket_status;
  const rgw_raw_obj& bucket_status_obj;
  const int shard_id;
  RGWObjVersionTracker objv_tracker;
  const next_bilog_result& next_log;
  const uint64_t generation;

public:
  RGWBucketShardIsDoneCR(RGWDataSyncCtx *_sc, const rgw_raw_obj& _bucket_status_obj,
                         int _shard_id, const next_bilog_result& _next_log, const uint64_t _gen)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
      bucket_status_obj(_bucket_status_obj),
      shard_id(_shard_id), next_log(_next_log), generation(_gen) {}

  int operate(const DoutPrefixProvider* dpp) override
  {
    reenter(this) {
      do {
        // read bucket sync status
        objv_tracker.clear();
        using ReadCR = RGWSimpleRadosReadCR<rgw_bucket_sync_status>;
        yield call(new ReadCR(dpp, sync_env->driver,
                              bucket_status_obj, &bucket_status, false, &objv_tracker));
        if (retcode < 0) {
          ldpp_dout(dpp, 20) << "failed to read bucket shard status: "
              << cpp_strerror(retcode) << dendl;
          return set_cr_error(retcode);
        }

        if (bucket_status.state != BucketSyncState::Incremental) {
          // exit with success to avoid stale shard being
          // retried in error repo if we lost a race
          ldpp_dout(dpp, 20) << "RGWBucketShardIsDoneCR found sync state = " << bucket_status.state << dendl;
          return set_cr_done();
        }

        if (bucket_status.incremental_gen != generation) {
          // exit with success to avoid stale shard being
          // retried in error repo if we lost a race
          ldpp_dout(dpp, 20) << "RGWBucketShardIsDoneCR expected gen: " << generation
              << ", got: " << bucket_status.incremental_gen << dendl;
          return set_cr_done();
        }

        yield {
          // update bucket_status after a shard is done with current gen
          auto& done = bucket_status.shards_done_with_gen;
          done[shard_id] = true;

          // increment gen if all shards are already done with current gen
          if (std::all_of(done.begin(), done.end(),
            [] (const bool done){return done; } )) {
            bucket_status.incremental_gen = next_log.generation;
            done.clear();
            done.resize(next_log.num_shards, false);
          }
          ldpp_dout(dpp, 20) << "bucket status incremental gen is " << bucket_status.incremental_gen << dendl;
          using WriteCR = RGWSimpleRadosWriteCR<rgw_bucket_sync_status>;
          call(new WriteCR(dpp, sync_env->driver,
                            bucket_status_obj, bucket_status, &objv_tracker, false));
        }
        if (retcode < 0 && retcode != -ECANCELED) {
          ldpp_dout(dpp, 20) << "failed to write bucket sync status: " << cpp_strerror(retcode) << dendl;
          return set_cr_error(retcode);
        } else if (retcode >= 0) {
          return set_cr_done();
        }
      } while (retcode == -ECANCELED);
    }
    return 0;
  }
};

class RGWBucketShardIncrementalSyncCR : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  rgw_bucket_sync_pipe& sync_pipe;
  RGWBucketSyncFlowManager::pipe_rules_ref rules;
  rgw_bucket_shard& bs;
  const rgw_raw_obj& bucket_status_obj;
  boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr;
  bilog_list_result extended_result;
  list<rgw_bi_log_entry> list_result;
  int next_num_shards;
  uint64_t next_gen;
  bool truncated;

  list<rgw_bi_log_entry>::iterator entries_iter, entries_end;
  map<pair<string, string>, pair<real_time, RGWModifyOp> > squash_map;
  rgw_bucket_shard_sync_info& sync_info;
  uint64_t generation;
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
                                  const std::string& shard_status_oid,
                                  const rgw_raw_obj& _bucket_status_obj,
                                  boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr,
                                  rgw_bucket_shard_sync_info& sync_info,
                                  uint64_t generation,
                                  RGWSyncTraceNodeRef& _tn_parent,
                                  RGWObjVersionTracker& objv_tracker,
                                  ceph::real_time* stable_timestamp)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
      sync_pipe(_sync_pipe), bs(_sync_pipe.info.source_bs),
      bucket_status_obj(_bucket_status_obj), lease_cr(std::move(lease_cr)),
      sync_info(sync_info), generation(generation), zone_id(sync_env->svc->zone->get_zone().id),
      tn(sync_env->sync_tracer->add_node(_tn_parent, "inc_sync",
                                         SSTR(bucket_shard_str{bs}))),
      marker_tracker(sc, shard_status_oid, sync_info.inc_marker, tn,
                     objv_tracker, stable_timestamp)
  {
    set_description() << "bucket shard incremental sync bucket="
        << bucket_shard_str{bs};
    set_status("init");
    rules = sync_pipe.get_rules();
    target_location_key = sync_pipe.info.dest_bucket.get_key();
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
        tn->log(1, "no lease or lease is lost, abort");
        drain_all();
        yield call(marker_tracker.flush());
        if (retcode < 0) {
          tn->log(0, SSTR("ERROR: incremental sync marker_tracker.flush() returned retcode=" << retcode));
          return set_cr_error(retcode);
        }
        return set_cr_error(-ECANCELED);
      }
      tn->log(20, SSTR("listing bilog for incremental sync; position=" << sync_info.inc_marker.position));
      set_status() << "listing bilog; position=" << sync_info.inc_marker.position;
      yield call(new RGWListBucketIndexLogCR(sc, bs, sync_info.inc_marker.position, generation, &extended_result));
      if (retcode < 0 && retcode != -ENOENT) {
        /* wait for all operations to complete */
        drain_all();
        yield spawn(marker_tracker.flush(), true);
        return set_cr_error(retcode);
      }
      list_result = std::move(extended_result.entries);
      truncated = extended_result.truncated;
      if (extended_result.next_log) {
        next_gen = extended_result.next_log->generation;
        next_num_shards = extended_result.next_log->num_shards;
      }

      squash_map.clear();
      entries_iter = list_result.begin();
      entries_end = list_result.end();
      for (; entries_iter != entries_end; ++entries_iter) {
        auto e = *entries_iter;
        if (e.op == RGWModifyOp::CLS_RGW_OP_SYNCSTOP) {
          ldpp_dout(dpp, 20) << "syncstop at: " << e.timestamp << ". marker: " << e.id << dendl;
          syncstopped = true;
          entries_end = std::next(entries_iter); // stop after this entry
          break;
        }
        if (e.op == RGWModifyOp::CLS_RGW_OP_RESYNC) {
          ldpp_dout(dpp, 20) << "syncstart at: " << e.timestamp << ". marker: " << e.id << dendl;
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
          tn->log(1, "no lease or lease is lost, abort");
          drain_all();
	  yield call(marker_tracker.flush());
          if (retcode < 0) {
            tn->log(0, SSTR("ERROR: incremental sync marker_tracker.flush() returned retcode=" << retcode));
            return set_cr_error(retcode);
          }
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
          ldpp_dout(dpp, 20) << "detected syncstop or resync on " << entries_iter->timestamp << ", skipping entry" << dendl;
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
	  drain_with_cb(sc->lcc.adj_concurrency(cct->_conf->rgw_bucket_sync_spawn_window),
                      [&](uint64_t stack_id, int ret) {
                if (ret < 0) {
                  tn->log(10, "a sync operation returned error");
                  sync_status = ret;
                }
                return 0;
              });
      }

    } while (!list_result.empty() && sync_status == 0 && !syncstopped);

    drain_all_cb([&](uint64_t stack_id, int ret) {
      if (ret < 0) {
        tn->log(10, "a sync operation returned error");
        sync_status = ret;
      }
      return 0;
    });
    tn->unset_flag(RGW_SNS_FLAG_ACTIVE);

    if (syncstopped) {
      // transition to StateStopped in RGWSyncBucketShardCR. if sync is
      // still disabled, we'll delete the sync status object. otherwise we'll
      // restart full sync to catch any changes that happened while sync was
      // disabled
      sync_info.state = rgw_bucket_shard_sync_info::StateStopped;
      return set_cr_done();
    }

    yield call(marker_tracker.flush());
    if (retcode < 0) {
      tn->log(0, SSTR("ERROR: incremental sync marker_tracker.flush() returned retcode=" << retcode));
      return set_cr_error(retcode);
    }
    if (sync_status < 0) {
      tn->log(10, SSTR("backing out with sync_status=" << sync_status));
      return set_cr_error(sync_status);
    }

    if (!truncated && extended_result.next_log) {
      yield call(new RGWBucketShardIsDoneCR(sc, bucket_status_obj, bs.shard_id, *extended_result.next_log, generation));
      if (retcode < 0) {
        ldout(cct, 20) << "failed to update bucket sync status: "
            << cpp_strerror(retcode) << dendl;
        return set_cr_error(retcode);
      }
      yield {
        // delete the shard status object
        rgw_rados_ref status_obj;
        retcode = rgw_get_rados_ref(dpp,
				    sync_env->driver->getRados()->get_rados_handle(),
				    marker_tracker.get_obj(),
				    &status_obj);
        if (retcode < 0) {
          return set_cr_error(retcode);
        }
        call(new RGWRadosRemoveOidCR(sync_env->driver, std::move(status_obj)));
        if (retcode < 0) {
          ldpp_dout(dpp, 20) << "failed to remove shard status object: " << cpp_strerror(retcode) << dendl;
          return set_cr_error(retcode);
        }
      }
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

static RGWCoroutine* sync_bucket_shard_cr(RGWDataSyncCtx* sc,
                                          boost::intrusive_ptr<const RGWContinuousLeaseCR> lease,
                                          const rgw_bucket_sync_pair_info& sync_pair,
                                          std::optional<uint64_t> gen,
                                          const RGWSyncTraceNodeRef& tn,
                                          ceph::real_time* progress);

RGWRunBucketSourcesSyncCR::RGWRunBucketSourcesSyncCR(RGWDataSyncCtx *_sc,
                                                     boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr,
                                                     const rgw_bucket_shard& source_bs,
                                                     const RGWSyncTraceNodeRef& _tn_parent,
						     std::optional<uint64_t> gen,
                                                     ceph::real_time* progress)
  : RGWCoroutine(_sc->env->cct), sc(_sc), sync_env(_sc->env),
    lease_cr(std::move(lease_cr)),
    tn(sync_env->sync_tracer->add_node(
	 _tn_parent, "bucket_sync_sources",
	 SSTR( "source=" << source_bs << ":source_zone=" << sc->source_zone))),
    progress(progress),
    gen(gen)
{
  sync_pair.source_bs = source_bs;
}

int RGWRunBucketSourcesSyncCR::operate(const DoutPrefixProvider *dpp)
{
  reenter(this) {
    yield call(new RGWGetBucketPeersCR(sync_env, std::nullopt, sc->source_zone,
                                       sync_pair.source_bs.bucket, &pipes, tn));
    if (retcode < 0 && retcode != -ENOENT) {
      tn->log(0, SSTR("ERROR: failed to read sync status for bucket. error: " << retcode));
      return set_cr_error(retcode);
    }

    ldpp_dout(dpp, 20) << __func__ << "(): requested source_bs=" << sync_pair.source_bs << dendl;

    if (pipes.empty()) {
      ldpp_dout(dpp, 20) << __func__ << "(): no relevant sync pipes found" << dendl;
      return set_cr_done();
    }

    shard_progress.resize(pipes.size());
    cur_shard_progress = shard_progress.begin();

    for (siter = pipes.begin(); siter != pipes.end(); ++siter, ++cur_shard_progress) {
      ldpp_dout(dpp, 20) << __func__ << "(): sync pipe=" << *siter << dendl;

      sync_pair.dest_bucket = siter->target.get_bucket();
      sync_pair.handler = siter->handler;

      ldpp_dout(dpp, 20) << __func__ << "(): sync_pair=" << sync_pair << dendl;

      yield_spawn_window(sync_bucket_shard_cr(sc, lease_cr, sync_pair,
                                              gen, tn, &*cur_shard_progress),
                         sc->lcc.adj_concurrency(cct->_conf->rgw_bucket_sync_spawn_window),
                         [&](uint64_t stack_id, int ret) {
                           if (ret < 0) {
                             tn->log(10, SSTR("ERROR: a sync operation returned error: " << ret));
                           }
                           return ret;
                         });
    }
    drain_all_cb([&](uint64_t stack_id, int ret) {
                   if (ret < 0) {
                     tn->log(10, SSTR("a sync operation returned error: " << ret));
                   }
                   return ret;
                 });
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

  int operate(const DoutPrefixProvider *dpp) override;
};

int RGWSyncGetBucketInfoCR::operate(const DoutPrefixProvider *dpp)
{
  reenter(this) {
    yield call(new RGWGetBucketInstanceInfoCR(sync_env->async_rados, sync_env->driver, bucket, pbucket_info, pattrs, dpp));
    if (retcode == -ENOENT) {
      /* bucket instance info has not been synced in yet, fetch it now */
      yield {
        tn->log(10, SSTR("no local info for bucket:" << ": fetching metadata"));
        string raw_key = string("bucket.instance:") + bucket.get_key();

        meta_sync_env.init(dpp, cct, sync_env->driver, sync_env->svc->zone->get_master_conn(), sync_env->async_rados,
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

      yield call(new RGWGetBucketInstanceInfoCR(sync_env->async_rados, sync_env->driver, bucket, pbucket_info, pattrs, dpp));
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

  int operate(const DoutPrefixProvider *dpp) override {
    reenter(this) {
      for (i = 0; i < 2; ++i) {
        yield call(new RGWBucketGetSyncPolicyHandlerCR(sync_env->async_rados,
                                                       sync_env->driver,
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

class RGWSyncBucketShardCR : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;
  boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr;
  rgw_bucket_sync_pair_info sync_pair;
  rgw_bucket_sync_pipe& sync_pipe;
  bool& bucket_stopped;
  uint64_t generation;
  ceph::real_time* progress;

  const std::string shard_status_oid;
  const rgw_raw_obj bucket_status_obj;
  rgw_bucket_shard_sync_info sync_status;
  RGWObjVersionTracker objv_tracker;

  RGWSyncTraceNodeRef tn;

public:
  RGWSyncBucketShardCR(RGWDataSyncCtx *_sc,
                       boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr,
                       const rgw_bucket_sync_pair_info& _sync_pair,
                       rgw_bucket_sync_pipe& sync_pipe,
                       bool& bucket_stopped,
                       uint64_t generation,
                       const RGWSyncTraceNodeRef& tn,
                       ceph::real_time* progress)
    : RGWCoroutine(_sc->cct), sc(_sc), sync_env(_sc->env),
      lease_cr(std::move(lease_cr)), sync_pair(_sync_pair),
      sync_pipe(sync_pipe), bucket_stopped(bucket_stopped), generation(generation), progress(progress),
      shard_status_oid(RGWBucketPipeSyncStatusManager::inc_status_oid(sc->source_zone, sync_pair, generation)),
      bucket_status_obj(sc->env->svc->zone->get_zone_params().log_pool,
                 RGWBucketPipeSyncStatusManager::full_status_oid(sc->source_zone,
                                                                 sync_pair.source_bs.bucket,
                                                                 sync_pair.dest_bucket)),
      tn(tn) {
  }

  int operate(const DoutPrefixProvider *dpp) override;
};

int RGWSyncBucketShardCR::operate(const DoutPrefixProvider *dpp)
{
  reenter(this) {
    objv_tracker.clear();
    yield call(new RGWReadBucketPipeSyncStatusCoroutine(sc, sync_pair, &sync_status, &objv_tracker, generation));
    if (retcode < 0 && retcode != -ENOENT) {
      tn->log(0, SSTR("ERROR: failed to read sync status for bucket. error: " << retcode));
      return set_cr_error(retcode);
    }

    tn->log(20, SSTR("sync status for source bucket shard: " << sync_status.state));
    sync_status.state = rgw_bucket_shard_sync_info::StateIncrementalSync;
    if (progress) {
      *progress = sync_status.inc_marker.timestamp;
    }

    yield call(new RGWBucketShardIncrementalSyncCR(sc, sync_pipe,
                                                   shard_status_oid, bucket_status_obj, lease_cr,
                                                   sync_status, generation, tn,
                                                   objv_tracker, progress));
    if (retcode < 0) {
      tn->log(5, SSTR("incremental sync on bucket failed, retcode=" << retcode));
      return set_cr_error(retcode);
    }

    if (sync_status.state == rgw_bucket_shard_sync_info::StateStopped) {
      tn->log(20, SSTR("syncstopped indication for source bucket shard"));
      bucket_stopped = true;
    }

    return set_cr_done();
  }

  return 0;
}

class RGWSyncBucketCR : public RGWCoroutine {
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *env;
  boost::intrusive_ptr<const RGWContinuousLeaseCR> data_lease_cr;
  boost::intrusive_ptr<RGWContinuousLeaseCR> bucket_lease_cr;
  rgw_bucket_sync_pair_info sync_pair;
  rgw_bucket_sync_pipe sync_pipe;
  std::optional<uint64_t> gen;
  ceph::real_time* progress;

  const std::string lock_name = "bucket sync";
  const uint32_t lock_duration;
  const rgw_raw_obj status_obj;
  rgw_bucket_sync_status bucket_status;
  bool bucket_stopped = false;
  RGWObjVersionTracker objv;
  bool init_check_compat = false;
  rgw_bucket_index_marker_info info;
  rgw_raw_obj error_repo;
  rgw_bucket_shard source_bs;
  rgw_pool pool;
  uint64_t current_gen = 0;

  RGWSyncTraceNodeRef tn;

public:
  RGWSyncBucketCR(RGWDataSyncCtx *_sc,
                  boost::intrusive_ptr<const RGWContinuousLeaseCR> lease_cr,
                  const rgw_bucket_sync_pair_info& _sync_pair,
                  std::optional<uint64_t> gen,
                  const RGWSyncTraceNodeRef& _tn_parent,
                  ceph::real_time* progress)
    : RGWCoroutine(_sc->cct), sc(_sc), env(_sc->env),
      data_lease_cr(std::move(lease_cr)), sync_pair(_sync_pair),
      gen(gen), progress(progress),
      lock_duration(cct->_conf->rgw_sync_lease_period),
      status_obj(env->svc->zone->get_zone_params().log_pool,
                 RGWBucketPipeSyncStatusManager::full_status_oid(sc->source_zone,
                                                                 sync_pair.source_bs.bucket,
                                                                 sync_pair.dest_bucket)),
      tn(env->sync_tracer->add_node(_tn_parent, "bucket",
                                    SSTR(bucket_str{_sync_pair.dest_bucket} << "<-" << bucket_shard_str{_sync_pair.source_bs} ))) {
  }

  int operate(const DoutPrefixProvider *dpp) override;
};

static RGWCoroutine* sync_bucket_shard_cr(RGWDataSyncCtx* sc,
                                          boost::intrusive_ptr<const RGWContinuousLeaseCR> lease,
                                          const rgw_bucket_sync_pair_info& sync_pair,
                                          std::optional<uint64_t> gen,
                                          const RGWSyncTraceNodeRef& tn,
                                          ceph::real_time* progress)
{
  return new RGWSyncBucketCR(sc, std::move(lease), sync_pair,
                             gen, tn, progress);
}

#define RELEASE_LOCK(cr) \
	if (cr) {cr->go_down(); drain_all(); cr.reset();}

int RGWSyncBucketCR::operate(const DoutPrefixProvider *dpp)
{
  reenter(this) {
    // read source/destination bucket info
    yield call(new RGWSyncGetBucketInfoCR(env, sync_pair.source_bs.bucket, &sync_pipe.source_bucket_info,
                                          &sync_pipe.source_bucket_attrs, tn));
    if (retcode < 0) {
      tn->log(0, SSTR("ERROR: failed to retrieve bucket info for bucket=" << bucket_str{sync_pair.source_bs.bucket}));
      return set_cr_error(retcode);
    }

    yield call(new RGWSyncGetBucketInfoCR(env, sync_pair.dest_bucket, &sync_pipe.dest_bucket_info,
                                          &sync_pipe.dest_bucket_attrs, tn));
    if (retcode < 0) {
      tn->log(0, SSTR("ERROR: failed to retrieve bucket info for bucket=" << bucket_str{sync_pair.source_bs.bucket}));
      return set_cr_error(retcode);
    }

    sync_pipe.info = sync_pair;

    // read bucket sync status
    using ReadCR = RGWSimpleRadosReadCR<rgw_bucket_sync_status>;
    using WriteCR = RGWSimpleRadosWriteCR<rgw_bucket_sync_status>;

    objv.clear();
    yield call(new ReadCR(dpp, env->driver,
                          status_obj, &bucket_status, false, &objv));
    if (retcode == -ENOENT) {
      // if the full sync status object didn't exist yet, run the backward
      // compatability logic in InitBucketFullSyncStatusCR below. if it did
      // exist, a `bucket sync init` probably requested its re-initialization,
      // and shouldn't try to resume incremental sync
      init_check_compat = true;

      // use exclusive create to set state=Init
      objv.generate_new_write_ver(cct);
      yield call(new WriteCR(dpp, env->driver, status_obj, bucket_status, &objv, true));
      tn->log(20, "bucket status object does not exist, create a new one");
      if (retcode == -EEXIST) {
        // raced with another create, read its status
        tn->log(20, "raced with another create, read its status");
        objv.clear();
        yield call(new ReadCR(dpp, env->driver,
                              status_obj, &bucket_status, false, &objv));
      }
    }
    if (retcode < 0) {
      tn->log(20, SSTR("ERROR: failed to read bucket status object. error: " << retcode));
      return set_cr_error(retcode);
    }

    do {
      tn->log(20, SSTR("sync status for source bucket: " << bucket_status.state << 
            ". lease is: " << (bucket_lease_cr ? "taken" : "not taken") << ". stop indications is: " << bucket_stopped));

      if (bucket_status.state != BucketSyncState::Incremental ||
          bucket_stopped) {

        if (!bucket_lease_cr) {
          bucket_lease_cr.reset(new RGWContinuousLeaseCR(env->async_rados, env->driver, status_obj,
                lock_name, lock_duration, this, &sc->lcc));
          yield spawn(bucket_lease_cr.get(), false);
          while (!bucket_lease_cr->is_locked()) {
            if (bucket_lease_cr->is_done()) {
              tn->log(5, "failed to take lease");
              set_status("lease lock failed, early abort");
              drain_all();
              return set_cr_error(bucket_lease_cr->get_ret_status());
            }
            tn->log(5, "waiting on bucket lease");
            yield set_sleeping(true);
          }
        }

        // if state is Init or Stopped, we query the remote RGW for ther state
        yield call(new RGWReadRemoteBucketIndexLogInfoCR(sc, sync_pair.source_bs.bucket, &info));
        if (retcode < 0) {
          RELEASE_LOCK(bucket_lease_cr);
          return set_cr_error(retcode);
        }
        if (info.syncstopped) {
          // remote indicates stopped state
          tn->log(20, "remote bilog indicates that sync was stopped");

          // if state was incremental, remove all per-shard status objects
          if (bucket_status.state == BucketSyncState::Incremental) {
            yield {
              const auto num_shards = bucket_status.shards_done_with_gen.size();
              const auto gen = bucket_status.incremental_gen;
              call(new RemoveBucketShardStatusCollectCR(sc, sync_pair, gen, num_shards));
            }
          }

          // check if local state is "stopped"
          objv.clear();
          yield call(new ReadCR(dpp, env->driver,
                status_obj, &bucket_status, false, &objv));
          if (retcode < 0) {
            tn->log(20, SSTR("ERROR: failed to read status before writing 'stopped'. error: " << retcode));
            RELEASE_LOCK(bucket_lease_cr);
            return set_cr_error(retcode);
          }
          if (bucket_status.state != BucketSyncState::Stopped) {
            // make sure that state is changed to stopped locally
            bucket_status.state = BucketSyncState::Stopped;
            yield call(new WriteCR(dpp, env->driver, status_obj, bucket_status,
				   &objv, false));
            if (retcode < 0) {
              tn->log(20, SSTR("ERROR: failed to write 'stopped' status. error: " << retcode));
              RELEASE_LOCK(bucket_lease_cr);
              return set_cr_error(retcode);
            }
          }
          RELEASE_LOCK(bucket_lease_cr);
          return set_cr_done();
        }
        if (bucket_stopped) {
          tn->log(20, SSTR("ERROR: switched from 'stop' to 'start' sync. while state is: " << bucket_status.state));
          bucket_stopped = false;
          bucket_status.state = BucketSyncState::Init;
        }
      }

      if (bucket_status.state != BucketSyncState::Incremental) {
        // if the state wasn't Incremental, take a bucket-wide lease to prevent
        // different shards from duplicating the init and full sync
        if (!bucket_lease_cr) {
          bucket_lease_cr.reset(new RGWContinuousLeaseCR(env->async_rados, env->driver, status_obj,
							 lock_name, lock_duration, this, &sc->lcc));
          yield spawn(bucket_lease_cr.get(), false);
          while (!bucket_lease_cr->is_locked()) {
            if (bucket_lease_cr->is_done()) {
              tn->log(5, "failed to take lease");
              set_status("lease lock failed, early abort");
              drain_all();
              return set_cr_error(bucket_lease_cr->get_ret_status());
            }
            tn->log(5, "waiting on bucket lease");
            yield set_sleeping(true);
          }
        }

        // reread the status after acquiring the lock
        objv.clear();
        yield call(new ReadCR(dpp, env->driver, status_obj,
                              &bucket_status, false, &objv));
        if (retcode < 0) {
          RELEASE_LOCK(bucket_lease_cr);
          tn->log(20, SSTR("ERROR: reading the status after acquiring the lock failed. error: " << retcode));
          return set_cr_error(retcode);
        }
        tn->log(20, SSTR("status after acquiring the lock is: " << bucket_status.state));

	yield call(new InitBucketFullSyncStatusCR(sc, sync_pair, status_obj,
						  bucket_status, objv,
						  sync_pipe.source_bucket_info,
						  init_check_compat, info));

        if (retcode < 0) {
          tn->log(20, SSTR("ERROR: init full sync failed. error: " << retcode));
          RELEASE_LOCK(bucket_lease_cr);
          return set_cr_error(retcode);
        }
      }

      assert(bucket_status.state == BucketSyncState::Incremental || 
          bucket_status.state == BucketSyncState::Full);

      if (bucket_status.state == BucketSyncState::Full) {
        assert(bucket_lease_cr);
        yield call(new RGWBucketFullSyncCR(sc, sync_pipe, status_obj,
                                           bucket_lease_cr, bucket_status,
                                           tn, objv));
        if (retcode < 0) {
          tn->log(20, SSTR("ERROR: full sync failed. error: " << retcode));
          RELEASE_LOCK(bucket_lease_cr);
          return set_cr_error(retcode);
        }
      }

      if (bucket_status.state == BucketSyncState::Incremental) {
        // lease not required for incremental sync
        RELEASE_LOCK(bucket_lease_cr);

        assert(sync_pair.source_bs.shard_id >= 0);
        // if a specific gen was requested, compare that to the sync status
        if (gen) {
          current_gen = bucket_status.incremental_gen;
	  source_bs = sync_pair.source_bs;
          if (*gen > current_gen) {
	    /* In case the data log entry is missing for previous gen, it may
	     * not be marked complete and the sync can get stuck. To avoid it,
	     * may be we can add this (shardid, gen) to error repo to force
	     * sync and mark that shard as completed.
	     */
	    pool = sc->env->svc->zone->get_zone_params().log_pool;
            if ((static_cast<std::size_t>(source_bs.shard_id) < bucket_status.shards_done_with_gen.size()) &&
	       !bucket_status.shards_done_with_gen[source_bs.shard_id]) {
	      // use the error repo and sync status timestamp from the datalog shard corresponding to source_bs
              error_repo = datalog_oid_for_error_repo(sc, sc->env->driver,
			   pool, source_bs);
              yield call(rgw::error_repo::write_cr(sc->env->driver->getRados()->get_rados_handle(), error_repo,
                                              rgw::error_repo::encode_key(source_bs, current_gen),
                                              ceph::real_clock::zero()));
              if (retcode < 0) {
                tn->log(0, SSTR("ERROR: failed to log prev gen entry (bucket=" << source_bs.bucket << ", shard_id=" << source_bs.shard_id << ", gen=" << current_gen << " in error repo: retcode=" << retcode));
              } else {
                tn->log(20, SSTR("logged prev gen entry (bucket=" << source_bs.bucket << ", shard_id=" << source_bs.shard_id << ", gen=" << current_gen << " in error repo: retcode=" << retcode));
	      }
	    }
            retcode = -EAGAIN;
            tn->log(10, SSTR("ERROR: requested sync of future generation "
                             << *gen << " > " << current_gen
                             << ", returning " << retcode << " for later retry"));
            return set_cr_error(retcode);
          } else if (*gen < current_gen) {
            tn->log(10, SSTR("WARNING: requested sync of past generation "
                             << *gen << " < " << current_gen
                             << ", returning success"));
            return set_cr_done();
          }
        }

        if (static_cast<std::size_t>(sync_pair.source_bs.shard_id) >= bucket_status.shards_done_with_gen.size()) {
          tn->log(1, SSTR("bucket shard " << sync_pair.source_bs << " index out of bounds"));
          return set_cr_done(); // return success so we don't retry
        }
        if (bucket_status.shards_done_with_gen[sync_pair.source_bs.shard_id]) {
          tn->log(10, SSTR("bucket shard " << sync_pair.source_bs << " of gen " <<
                          gen << " already synced."));
          return set_cr_done();
        }

        yield call(new RGWSyncBucketShardCR(sc, data_lease_cr, sync_pair,
                                            sync_pipe, bucket_stopped,
                                            bucket_status.incremental_gen, tn, progress));
        if (retcode < 0) {
          tn->log(20, SSTR("ERROR: incremental sync failed. error: " << retcode));
          return set_cr_error(retcode);
        }
      }
      // loop back to previous states unless incremental sync returns normally
    } while (bucket_status.state != BucketSyncState::Incremental || bucket_stopped);

    return set_cr_done();
  }

  return 0;
}

int RGWBucketPipeSyncStatusManager::do_init(const DoutPrefixProvider *dpp,
					    std::ostream* ostr)
{
  int ret = http_manager.start();
  if (ret < 0) {
    ldpp_dout(this, 0) << "failed in http_manager.start() ret=" << ret << dendl;
    return ret;
  }

  sync_module.reset(new RGWDefaultSyncModuleInstance());
  auto async_rados = driver->svc()->async_processor;

  sync_env.init(this, driver->ctx(), driver,
                driver->svc(), async_rados, &http_manager,
                error_logger.get(), driver->getRados()->get_sync_tracer(),
                sync_module, nullptr);

  sync_env.ostr = ostr;

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

  if (pipes.empty()) {
    ldpp_dout(this, 0) << "No peers. This is not a valid multisite configuration." << dendl;
    return -EINVAL;
  }

  for (auto& pipe : pipes) {
    auto& szone = pipe.source.zone;

    auto conn = driver->svc()->zone->get_zone_conn(szone);
    if (!conn) {
      ldpp_dout(this, 0) << "connection object to zone " << szone << " does not exist" << dendl;
      return -EINVAL;
    }

    RGWZone* z;
    if (!(z = driver->svc()->zone->find_zone(szone))) {
      ldpp_dout(this, 0) << "zone " << szone << " does not exist" << dendl;
      return -EINVAL;
    }
    sources.emplace_back(&sync_env, szone, conn,
			 pipe.source.get_bucket_info(),
			 pipe.target.get_bucket(),
			 pipe.handler, z->name);
  }

  return 0;
}

int RGWBucketPipeSyncStatusManager::remote_info(const DoutPrefixProvider *dpp,
						source& s,
						uint64_t* oldest_gen,
						uint64_t* latest_gen,
						uint64_t* num_shards)
{
  rgw_bucket_index_marker_info remote_info;
  BucketIndexShardsManager remote_markers;
  auto r = rgw_read_remote_bilog_info(dpp, s.sc.conn, s.info.bucket,
				      remote_info, remote_markers,
				      null_yield);

  if (r < 0) {
    ldpp_dout(dpp, 0) << __PRETTY_FUNCTION__ << ":" << __LINE__
		      << " rgw_read_remote_bilog_info: r="
		      << r << dendl;
    return r;
  }
  if (oldest_gen)
    *oldest_gen = remote_info.oldest_gen;

  if (latest_gen)
    *latest_gen = remote_info.latest_gen;

  if (num_shards)
    *num_shards = remote_markers.get().size();

  return 0;
}

tl::expected<std::unique_ptr<RGWBucketPipeSyncStatusManager>, int>
RGWBucketPipeSyncStatusManager::construct(
  const DoutPrefixProvider* dpp,
  rgw::sal::RadosStore* driver,
  std::optional<rgw_zone_id> source_zone,
  std::optional<rgw_bucket> source_bucket,
  const rgw_bucket& dest_bucket,
  std::ostream* ostr)
{
  std::unique_ptr<RGWBucketPipeSyncStatusManager> self{
    new RGWBucketPipeSyncStatusManager(driver, source_zone, source_bucket,
				       dest_bucket)};
  auto r = self->do_init(dpp, ostr);
  if (r < 0) {
    return tl::unexpected(r);
  }
  return self;
}

int RGWBucketPipeSyncStatusManager::init_sync_status(
  const DoutPrefixProvider *dpp)
{
  // Just running one at a time saves us from buildup/teardown and in
  // practice we only do one zone at a time.
  for (auto& source : sources) {
    list<RGWCoroutinesStack*> stacks;
    RGWCoroutinesStack *stack = new RGWCoroutinesStack(driver->ctx(), &cr_mgr);
    pretty_print(source.sc.env, "Initializing sync state of bucket {} with zone {}.\n",
		 source.info.bucket.name, source.zone_name);
    stack->call(new RGWSimpleRadosWriteCR<rgw_bucket_sync_status>(
		  dpp, source.sc.env->driver,
		  {sync_env.svc->zone->get_zone_params().log_pool,
                   full_status_oid(source.sc.source_zone,
				   source.info.bucket,
				   source.dest)},
		  rgw_bucket_sync_status{}));
    stacks.push_back(stack);
    auto r = cr_mgr.run(dpp, stacks);
    if (r < 0) {
      pretty_print(source.sc.env,
		   "Initialization of sync state for bucket {} with zone {} "
		   "failed with error {}\n",
		   source.info.bucket.name, source.zone_name, cpp_strerror(r));
    }
  }
  return 0;
}

tl::expected<std::map<int, rgw_bucket_shard_sync_info>, int>
RGWBucketPipeSyncStatusManager::read_sync_status(
  const DoutPrefixProvider *dpp)
{
  std::map<int, rgw_bucket_shard_sync_info> sync_status;
  list<RGWCoroutinesStack *> stacks;

  auto sz = sources.begin();

  if (source_zone) {
    sz = std::find_if(sources.begin(), sources.end(),
		      [this](const source& s) {
			return s.sc.source_zone == *source_zone;
		      }
      );
    if (sz == sources.end()) {
      ldpp_dout(this, 0) << "ERROR: failed to find source zone: "
			 << *source_zone << dendl;
      return tl::unexpected(-ENOENT);
    }
  } else {
    ldpp_dout(this, 5) << "No source zone specified, using source zone: "
		       << sz->sc.source_zone << dendl;
    return tl::unexpected(-ENOENT);
  }
  uint64_t num_shards, latest_gen;
  auto ret = remote_info(dpp, *sz, nullptr, &latest_gen, &num_shards);
  if (ret < 0) {
    ldpp_dout(this, 5) << "Unable to get remote info: "
		       << ret << dendl;
    return tl::unexpected(ret);
  }
  auto stack = new RGWCoroutinesStack(driver->ctx(), &cr_mgr);
  std::vector<rgw_bucket_sync_pair_info> pairs(num_shards);
  for (auto shard = 0u; shard < num_shards; ++shard) {
    auto& pair = pairs[shard];
    pair.source_bs.bucket = sz->info.bucket;
    pair.dest_bucket = sz->dest;
    pair.source_bs.shard_id = shard;
    stack->call(new RGWReadBucketPipeSyncStatusCoroutine(
		  &sz->sc, pair, &sync_status[shard],
		  nullptr, latest_gen));
  }

  stacks.push_back(stack);

  ret = cr_mgr.run(dpp, stacks);
  if (ret < 0) {
    ldpp_dout(this, 0) << "ERROR: failed to read sync status for "
		       << bucket_str{dest_bucket} << dendl;
    return tl::unexpected(ret);
  }

  return sync_status;
}

namespace rgw::bucket_sync_run {
// Retry-loop over calls to sync_bucket_shard_cr
class ShardCR : public RGWCoroutine {
  static constexpr auto allowed_retries = 10u;

  RGWDataSyncCtx& sc;
  const rgw_bucket_sync_pair_info& pair;
  const uint64_t gen;
  unsigned retries = 0;

  ceph::real_time prev_progress;
  ceph::real_time progress;

public:

  ShardCR(RGWDataSyncCtx& sc, const rgw_bucket_sync_pair_info& pair,
	  const uint64_t gen)
    : RGWCoroutine(sc.cct), sc(sc), pair(pair), gen(gen) {}

  int operate(const DoutPrefixProvider *dpp) override {
    reenter(this) {
      // Since all errors (except ECANCELED) are considered retryable,
      // retry other errors so long as we're making progress.
      for (retries = 0u, retcode = -EDOM;
	   (retries < allowed_retries) && (retcode != 0);
	   ++retries) {
	ldpp_dout(dpp, 5) << "ShardCR: syncing bucket shard on: "
			  << "zone=" << sc.source_zone
			  << ", bucket=" << pair.source_bs.bucket.name
			  << ", shard=" << pair.source_bs.shard_id
			  << ", gen=" << gen
			  << dendl;
	yield call(sync_bucket_shard_cr(&sc, nullptr, pair, gen,
					sc.env->sync_tracer->root_node,
					&progress));

	if (retcode == -ECANCELED) {
	  ldpp_dout(dpp, -1) << "ERROR: Got -ECANCELED for "
			     << pair.source_bs << dendl;
	  drain_all();
	  return set_cr_error(retcode);
	} else if (retcode < 0) {
	  ldpp_dout(dpp, 5) << "WARNING: Got error, retcode=" << retcode << " for "
			    << pair.source_bs << "on retry "
			    << retries + 1 << " of " << allowed_retries
			    << " allowed" << dendl;
	  // Reset the retry counter if we made any progress
	  if (progress != prev_progress) {
	    retries = 0;
	  }
	  prev_progress = progress;
	}
      }
      if (retcode < 0) {
	ldpp_dout(dpp, -1) << "ERROR: Exhausted retries for "
			   << pair.source_bs << " retcode="
			   << retcode << dendl;
	drain_all();
	return set_cr_error(retcode);
      }

      drain_all();
      return set_cr_done();
    }
    return 0;
  }
};

// Loop over calls to ShardCR with limited concurrency
class GenCR : public RGWShardCollectCR {
  static constexpr auto MAX_CONCURRENT_SHARDS = 64;

  RGWDataSyncCtx& sc;
  const uint64_t gen;

  std::vector<rgw_bucket_sync_pair_info> pairs;
  decltype(pairs)::const_iterator iter;

public:
  GenCR(RGWDataSyncCtx& sc, const rgw_bucket& source, const rgw_bucket& dest,
	const uint64_t gen, const uint64_t shards,
	const RGWBucketSyncFlowManager::pipe_handler& handler)
    : RGWShardCollectCR(sc.cct, MAX_CONCURRENT_SHARDS),
      sc(sc), gen(gen) {
    pairs.resize(shards);
    for (auto shard = 0u; shard < shards; ++shard) {
      auto& pair = pairs[shard];
      pair.handler = handler;
      pair.source_bs.bucket = source;
      pair.dest_bucket = dest;
      pair.source_bs.shard_id = shard;
    }
    iter = pairs.cbegin();
    assert(pairs.size() == shards);
  }

  virtual bool spawn_next() override {
    if (iter == pairs.cend()) {
      return false;
    }
    spawn(new ShardCR(sc, *iter, gen), false);
    ++iter;
    return true;
  }

  int handle_result(int r) override {
    if (r < 0) {
      ldpp_dout(sc.env->dpp, 4) << "ERROR: Error syncing shard: "
				<< cpp_strerror(r) << dendl;
    }
    return r;
  }
};

// Read sync status, loop over calls to GenCR
class SourceCR : public RGWCoroutine {
  RGWDataSyncCtx& sc;
  const RGWBucketInfo& info;
  const rgw_bucket& dest;
  const RGWBucketSyncFlowManager::pipe_handler& handler;
  const rgw_raw_obj status_obj{
    sc.env->svc->zone->get_zone_params().log_pool,
    RGWBucketPipeSyncStatusManager::full_status_oid(sc.source_zone, info.bucket,
						    dest)};

  BucketSyncState state = BucketSyncState::Incremental;
  uint64_t gen = 0;
  uint64_t num_shards = 0;
  rgw_bucket_sync_status status;
  std::string zone_name;

public:

  SourceCR(RGWDataSyncCtx& sc, const RGWBucketInfo& info,
	   const rgw_bucket& dest,
	   const RGWBucketSyncFlowManager::pipe_handler& handler,
	   const std::string& zone_name)
    : RGWCoroutine(sc.cct), sc(sc), info(info), dest(dest), handler(handler),
      zone_name(zone_name) {}

  int operate(const DoutPrefixProvider *dpp) override {
    reenter(this) {
      // Get the source's status. In incremental sync, this gives us
      // the generation and shard count that is next needed to be run.
      yield call(new RGWSimpleRadosReadCR<rgw_bucket_sync_status>(
		   dpp, sc.env->driver, status_obj, &status));
      if (retcode < 0) {
	ldpp_dout(dpp, -1) << "ERROR: Unable to fetch status for zone="
			   << sc.source_zone << " retcode="
			   << retcode << dendl;
	drain_all();
	return set_cr_error(retcode);
      }

      if (status.state == BucketSyncState::Stopped) {
	// Nothing to do.
	pretty_print(sc.env, "Sync of bucket {} from source zone {} is in state Stopped. "
		     "Nothing to do.\n", dest.name, zone_name);
	ldpp_dout(dpp, 5) << "SourceCR: Bucket is in state Stopped, returning."
			  << dendl;
	drain_all();
	return set_cr_done();
      }

      do {
	state = status.state;
	gen = status.incremental_gen;
	num_shards = status.shards_done_with_gen.size();

	ldpp_dout(dpp, 5) << "SourceCR: "
			  << "state=" << state
			  << ", gen=" << gen
			  << ", num_shards=" << num_shards
			  << dendl;

	// Special case to handle full sync. Since full sync no longer
	// uses shards and has no generations, we sync shard zero,
	// though use the current generation so a following
	// incremental sync can carry on.
	if (state != BucketSyncState::Incremental) {
	  pretty_print(sc.env, "Beginning full sync of bucket {} from source zone {}.\n",
		       dest.name, zone_name);
	  ldpp_dout(dpp, 5)  << "SourceCR: Calling GenCR with "
			     << "gen=" << gen
			     << ", num_shards=" << 1
			     << dendl;
	  yield call(new GenCR(sc, info.bucket, dest, gen, 1, handler));
	} else {
	  pretty_print(sc.env, "Beginning incremental sync of bucket {}, generation {} from source zone {}.\n",
		       dest.name, gen, zone_name);
	  ldpp_dout(dpp, 5) << "SourceCR: Calling GenCR with "
			    << "gen=" << gen
			    << ", num_shards=" << num_shards
			    << dendl;
	  yield call(new GenCR(sc, info.bucket, dest, gen, num_shards,
			       handler));
	}
	if (retcode < 0) {
	  ldpp_dout(dpp, -1) << "ERROR: Giving up syncing from "
			     << sc.source_zone << " retcode="
			     << retcode << dendl;
	  drain_all();
	  return set_cr_error(retcode);
	}

	pretty_print(sc.env, "Completed.\n");

	yield call(new RGWSimpleRadosReadCR<rgw_bucket_sync_status>(
		     dpp, sc.env->driver, status_obj, &status));
	if (retcode < 0) {
	  ldpp_dout(dpp, -1) << "ERROR: Unable to fetch status for zone="
			     << sc.source_zone << " retcode="
			     << retcode << dendl;
	  drain_all();
	  return set_cr_error(retcode);
	}
	// Repeat until we have done an incremental run and the
	// generation remains unchanged.
	ldpp_dout(dpp, 5) << "SourceCR: "
			  << "state=" << state
			  << ", gen=" << gen
			  << ", num_shards=" << num_shards
			  << ", status.state=" << status.state
			  << ", status.incremental_gen=" << status.incremental_gen
			  << ", status.shards_done_with_gen.size()=" << status.shards_done_with_gen.size()
			  << dendl;
      } while (state != BucketSyncState::Incremental ||
	       gen != status.incremental_gen);
      drain_all();
      return set_cr_done();
    }
    return 0;
  }
};
} // namespace rgw::bucket_sync_run

int RGWBucketPipeSyncStatusManager::run(const DoutPrefixProvider *dpp)
{
  list<RGWCoroutinesStack *> stacks;
  for (auto& source : sources) {
    auto stack = new RGWCoroutinesStack(driver->ctx(), &cr_mgr);
    stack->call(new rgw::bucket_sync_run::SourceCR(
		  source.sc, source.info, source.dest, source.handler,
		  source.zone_name));
    stacks.push_back(stack);
  }
  auto ret = cr_mgr.run(dpp, stacks);
  if (ret < 0) {
    ldpp_dout(this, 0) << "ERROR: Sync unsuccessful on bucket "
		       << bucket_str{dest_bucket} << dendl;
  }
  return ret;
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

string RGWBucketPipeSyncStatusManager::full_status_oid(const rgw_zone_id& source_zone,
                                                       const rgw_bucket& source_bucket,
                                                       const rgw_bucket& dest_bucket)
{
  if (source_bucket == dest_bucket) {
    return bucket_full_status_oid_prefix + "." + source_zone.id + ":"
        + dest_bucket.get_key();
  } else {
    return bucket_full_status_oid_prefix + "." + source_zone.id + ":"
        + dest_bucket.get_key() + ":" + source_bucket.get_key();
  }
}

inline std::string generation_token(uint64_t gen) {
  return (gen == 0) ? "" : (":" + std::to_string(gen));
}

string RGWBucketPipeSyncStatusManager::inc_status_oid(const rgw_zone_id& source_zone,
                                                      const rgw_bucket_sync_pair_info& sync_pair,
                                                      uint64_t gen)
{
  if (sync_pair.source_bs.bucket == sync_pair.dest_bucket) {
    return bucket_status_oid_prefix + "." + source_zone.id + ":" + sync_pair.source_bs.get_key() + 
      generation_token(gen);
  } else {
    return bucket_status_oid_prefix + "." + source_zone.id + ":" + sync_pair.dest_bucket.get_key() + ":" + sync_pair.source_bs.get_key() +
      generation_token(gen);
  }
}

string RGWBucketPipeSyncStatusManager::obj_status_oid(const rgw_bucket_sync_pipe& sync_pipe,
                                                      const rgw_zone_id& source_zone,
                                                      const rgw_obj& obj)
{
  string prefix = object_status_oid_prefix + "." + source_zone.id + ":" + obj.bucket.get_key();
  if (sync_pipe.source_bucket_info.bucket !=
      sync_pipe.dest_bucket_info.bucket) {
    prefix += string("/") + sync_pipe.dest_bucket_info.bucket.get_key();
  }
  return prefix + ":" + obj.key.name + ":" + obj.key.instance;
}

int rgw_read_remote_bilog_info(const DoutPrefixProvider *dpp,
                               RGWRESTConn* conn,
                               const rgw_bucket& bucket,
                               rgw_bucket_index_marker_info& info,
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
  int r = conn->get_json_resource(dpp, "/admin/log/", params, y, info);
  if (r < 0) {
    ldpp_dout(dpp, -1) << "failed to fetch remote log markers: " << cpp_strerror(r) << dendl;
    return r;
  }
  // parse shard markers
  r = markers.from_string(info.max_marker, -1);
  if (r < 0) {
    ldpp_dout(dpp, -1) << "failed to decode remote log markers" << dendl;
    return r;
  }
  return 0;
}

class RGWCollectBucketSyncStatusCR : public RGWShardCollectCR {
  static constexpr int max_concurrent_shards = 16;
  rgw::sal::RadosStore* const driver;
  RGWDataSyncCtx *const sc;
  RGWDataSyncEnv *const env;
  const uint64_t gen;

  rgw_bucket_sync_pair_info sync_pair;
  using Vector = std::vector<rgw_bucket_shard_sync_info>;
  Vector::iterator i, end;

  int handle_result(int r) override {
    if (r == -ENOENT) { // ENOENT is not a fatal error
      return 0;
    }
    if (r < 0) {
      ldout(cct, 4) << "failed to read bucket shard sync status: "
          << cpp_strerror(r) << dendl;
    }
    return r;
  }
 public:
  RGWCollectBucketSyncStatusCR(rgw::sal::RadosStore* driver, RGWDataSyncCtx *sc,
                               const rgw_bucket_sync_pair_info& sync_pair,
                               uint64_t gen,
                               Vector *status)
    : RGWShardCollectCR(sc->cct, max_concurrent_shards),
      driver(driver), sc(sc), env(sc->env), gen(gen), sync_pair(sync_pair),
      i(status->begin()), end(status->end())
  {}

  bool spawn_next() override {
    if (i == end) {
      return false;
    }
    spawn(new RGWReadBucketPipeSyncStatusCoroutine(sc, sync_pair, &*i, nullptr, gen), false);
    ++i;
    ++sync_pair.source_bs.shard_id;
    return true;
  }
};

int rgw_read_bucket_full_sync_status(const DoutPrefixProvider *dpp,
                                     rgw::sal::RadosStore *driver,
                                     const rgw_sync_bucket_pipe& pipe,
                                     rgw_bucket_sync_status *status,
                                     optional_yield y)
{
  auto get_oid = RGWBucketPipeSyncStatusManager::full_status_oid;
  const rgw_raw_obj obj{driver->svc()->zone->get_zone_params().log_pool,
                        get_oid(*pipe.source.zone, *pipe.source.bucket, *pipe.dest.bucket)};

  auto svc = driver->svc()->sysobj;
  auto sysobj = svc->get_obj(obj);
  bufferlist bl;
  int ret = sysobj.rop().read(dpp, &bl, y);
  if (ret < 0)
    return ret;

  try {
    auto iter = bl.cbegin();
    using ceph::decode;
    rgw_bucket_sync_status result;
    decode(result, iter);
    *status = result;
    return 0;
  } catch (const buffer::error& err) {
    lderr(svc->ctx()) << "error decoding " << obj << ": " << err.what() << dendl;
    return -EIO;
  }
}

int rgw_read_bucket_inc_sync_status(const DoutPrefixProvider *dpp,
                                    rgw::sal::RadosStore *driver,
                                    const rgw_sync_bucket_pipe& pipe,
                                    uint64_t gen,
                                    std::vector<rgw_bucket_shard_sync_info> *status)
{
  if (!pipe.source.zone ||
      !pipe.source.bucket ||
      !pipe.dest.zone ||
      !pipe.dest.bucket) {
    return -EINVAL;
  }

  rgw_bucket_sync_pair_info sync_pair;
  sync_pair.source_bs.bucket = *pipe.source.bucket;
  sync_pair.source_bs.shard_id = 0;
  sync_pair.dest_bucket = *pipe.dest.bucket;

  RGWDataSyncEnv env;
  RGWSyncModuleInstanceRef module; // null sync module
  env.init(dpp, driver->ctx(), driver, driver->svc(), driver->svc()->async_processor,
           nullptr, nullptr, nullptr, module, nullptr);

  RGWDataSyncCtx sc;
  sc.init(&env, nullptr, *pipe.source.zone);

  RGWCoroutinesManager crs(driver->ctx(), driver->getRados()->get_cr_registry());
  return crs.run(dpp, new RGWCollectBucketSyncStatusCR(driver, &sc,
                                                  sync_pair,
                                                  gen,
                                                  status));
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

void rgw_bucket_shard_full_sync_marker::dump(Formatter *f) const
{
  encode_json("position", position, f);
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
  if (s == "full-sync") {
    state = StateFullSync;
  } else if (s == "incremental-sync") {
    state = StateIncrementalSync;
  } else if (s == "stopped") {
    state = StateStopped;
  } else {
    state = StateInit;
  }
  JSONDecoder::decode_json("inc_marker", inc_marker, obj);
}

void rgw_bucket_shard_full_sync_marker::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("position", position, obj);
  JSONDecoder::decode_json("count", count, obj);
}

void rgw_bucket_shard_sync_info::dump(Formatter *f) const
{
  const char *s{nullptr};
  switch ((SyncState)state) {
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
  encode_json("inc_marker", inc_marker, f);
}

void rgw_bucket_full_sync_status::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("position", position, obj);
  JSONDecoder::decode_json("count", count, obj);
}

void rgw_bucket_full_sync_status::dump(Formatter *f) const
{
  encode_json("position", position, f);
  encode_json("count", count, f);
}

void encode_json(const char *name, BucketSyncState state, Formatter *f)
{
  switch (state) {
  case BucketSyncState::Init:
    encode_json(name, "init", f);
    break;
  case BucketSyncState::Full:
    encode_json(name, "full-sync", f);
    break;
  case BucketSyncState::Incremental:
    encode_json(name, "incremental-sync", f);
    break;
  case BucketSyncState::Stopped:
    encode_json(name, "stopped", f);
    break;
  default:
    encode_json(name, "unknown", f);
    break;
  }
}

void decode_json_obj(BucketSyncState& state, JSONObj *obj)
{
  std::string s;
  decode_json_obj(s, obj);
  if (s == "full-sync") {
    state = BucketSyncState::Full;
  } else if (s == "incremental-sync") {
    state = BucketSyncState::Incremental;
  } else if (s == "stopped") {
    state = BucketSyncState::Stopped;
  } else {
    state = BucketSyncState::Init;
  }
}

void rgw_bucket_sync_status::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("state", state, obj);
  JSONDecoder::decode_json("full", full, obj);
  JSONDecoder::decode_json("incremental_gen", incremental_gen, obj);
}

void rgw_bucket_sync_status::dump(Formatter *f) const
{
  encode_json("state", state, f);
  encode_json("full", full, f);
  encode_json("incremental_gen", incremental_gen, f);
}


void bilog_status_v2::dump(Formatter *f) const
{
  encode_json("sync_status", sync_status, f);
  encode_json("inc_status", inc_status, f);
}

void bilog_status_v2::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("sync_status", sync_status, obj);
  JSONDecoder::decode_json("inc_status", inc_status, obj);
}
