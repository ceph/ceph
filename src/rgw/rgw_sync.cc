// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <boost/optional.hpp>

#include "common/ceph_json.h"
#include "common/RWLock.h"
#include "common/RefCountedObj.h"
#include "common/WorkQueue.h"
#include "common/Throttle.h"
#include "common/admin_socket.h"
#include "common/errno.h"

#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_zone.h"
#include "rgw_sync.h"
#include "rgw_metadata.h"
#include "rgw_mdlog_types.h"
#include "rgw_rest_conn.h"
#include "rgw_tools.h"
#include "rgw_cr_rados.h"
#include "rgw_cr_rest.h"
#include "rgw_http_client.h"
#include "rgw_sync_trace.h"

#include "cls/lock/cls_lock_client.h"

#include "services/svc_zone.h"
#include "services/svc_mdlog.h"
#include "services/svc_meta.h"
#include "services/svc_cls.h"

#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw

#undef dout_prefix
#define dout_prefix (*_dout << "meta sync: ")

static string mdlog_sync_status_oid = "mdlog.sync-status";
static string mdlog_sync_status_shard_prefix = "mdlog.sync-status.shard";
static string mdlog_sync_full_sync_index_prefix = "meta.full-sync.index";

RGWSyncErrorLogger::RGWSyncErrorLogger(rgw::sal::RGWRadosStore *_store, const string &oid_prefix, int _num_shards) : store(_store), num_shards(_num_shards) {
  for (int i = 0; i < num_shards; i++) {
    oids.push_back(get_shard_oid(oid_prefix, i));
  }
}
string RGWSyncErrorLogger::get_shard_oid(const string& oid_prefix, int shard_id) {
  char buf[oid_prefix.size() + 16];
  snprintf(buf, sizeof(buf), "%s.%d", oid_prefix.c_str(), shard_id);
  return string(buf);
}

RGWCoroutine *RGWSyncErrorLogger::log_error_cr(const string& source_zone, const string& section, const string& name, uint32_t error_code, const string& message) {
  cls_log_entry entry;

  rgw_sync_error_info info(source_zone, error_code, message);
  bufferlist bl;
  encode(info, bl);
  store->svc()->cls->timelog.prepare_entry(entry, real_clock::now(), section, name, bl);

  uint32_t shard_id = ++counter % num_shards;


  return new RGWRadosTimelogAddCR(store, oids[shard_id], entry);
}

void RGWSyncBackoff::update_wait_time()
{
  if (cur_wait == 0) {
    cur_wait = 1;
  } else {
    cur_wait = (cur_wait << 1);
  }
  if (cur_wait >= max_secs) {
    cur_wait = max_secs;
  }
}

void RGWSyncBackoff::backoff_sleep()
{
  update_wait_time();
  sleep(cur_wait);
}

void RGWSyncBackoff::backoff(RGWCoroutine *op)
{
  update_wait_time();
  op->wait(utime_t(cur_wait, 0));
}

int RGWBackoffControlCR::operate() {
  reenter(this) {
    // retry the operation until it succeeds
    while (true) {
      yield {
	std::lock_guard l{lock};
        cr = alloc_cr();
        cr->get();
        call(cr);
      }
      {
	std::lock_guard l{lock};
        cr->put();
        cr = NULL;
      }
      if (retcode >= 0) {
        break;
      }
      if (retcode != -EBUSY && retcode != -EAGAIN) {
        ldout(cct, 0) << "ERROR: RGWBackoffControlCR called coroutine returned " << retcode << dendl;
        if (exit_on_error) {
          return set_cr_error(retcode);
        }
      }
      if (reset_backoff) {
        backoff.reset();
      }
      yield backoff.backoff(this);
    }

    // run an optional finisher
    yield call(alloc_finisher_cr());
    if (retcode < 0) {
      ldout(cct, 0) << "ERROR: call to finisher_cr() failed: retcode=" << retcode << dendl;
      return set_cr_error(retcode);
    }
    return set_cr_done();
  }
  return 0;
}

void rgw_mdlog_info::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("num_objects", num_shards, obj);
  JSONDecoder::decode_json("period", period, obj);
  JSONDecoder::decode_json("realm_epoch", realm_epoch, obj);
}

void rgw_mdlog_entry::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("section", section, obj);
  JSONDecoder::decode_json("name", name, obj);
  utime_t ut;
  JSONDecoder::decode_json("timestamp", ut, obj);
  timestamp = ut.to_real_time();
  JSONDecoder::decode_json("data", log_data, obj);
}

void rgw_mdlog_shard_data::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("marker", marker, obj);
  JSONDecoder::decode_json("truncated", truncated, obj);
  JSONDecoder::decode_json("entries", entries, obj);
};

int RGWShardCollectCR::operate() {
  reenter(this) {
    while (spawn_next()) {
      current_running++;

      while (current_running >= max_concurrent) {
        int child_ret;
        yield wait_for_child();
        if (collect_next(&child_ret)) {
          current_running--;
          if (child_ret < 0 && child_ret != -ENOENT) {
            ldout(cct, 10) << __func__ << ": failed to fetch log status, ret=" << child_ret << dendl;
            status = child_ret;
          }
        }
      }
    }
    while (current_running > 0) {
      int child_ret;
      yield wait_for_child();
      if (collect_next(&child_ret)) {
        current_running--;
        if (child_ret < 0 && child_ret != -ENOENT) {
          ldout(cct, 10) << __func__ << ": failed to fetch log status, ret=" << child_ret << dendl;
          status = child_ret;
        }
      }
    }
    if (status < 0) {
      return set_cr_error(status);
    }
    return set_cr_done();
  }
  return 0;
}

class RGWReadRemoteMDLogInfoCR : public RGWShardCollectCR {
  RGWMetaSyncEnv *sync_env;

  const std::string& period;
  int num_shards;
  map<int, RGWMetadataLogInfo> *mdlog_info;

  int shard_id;
#define READ_MDLOG_MAX_CONCURRENT 10

public:
  RGWReadRemoteMDLogInfoCR(RGWMetaSyncEnv *_sync_env,
                     const std::string& period, int _num_shards,
                     map<int, RGWMetadataLogInfo> *_mdlog_info) : RGWShardCollectCR(_sync_env->cct, READ_MDLOG_MAX_CONCURRENT),
                                                                 sync_env(_sync_env),
                                                                 period(period), num_shards(_num_shards),
                                                                 mdlog_info(_mdlog_info), shard_id(0) {}
  bool spawn_next() override;
};

class RGWListRemoteMDLogCR : public RGWShardCollectCR {
  RGWMetaSyncEnv *sync_env;

  const std::string& period;
  map<int, string> shards;
  int max_entries_per_shard;
  map<int, rgw_mdlog_shard_data> *result;

  map<int, string>::iterator iter;
#define READ_MDLOG_MAX_CONCURRENT 10

public:
  RGWListRemoteMDLogCR(RGWMetaSyncEnv *_sync_env,
                     const std::string& period, map<int, string>& _shards,
                     int _max_entries_per_shard,
                     map<int, rgw_mdlog_shard_data> *_result) : RGWShardCollectCR(_sync_env->cct, READ_MDLOG_MAX_CONCURRENT),
                                                                 sync_env(_sync_env), period(period),
                                                                 max_entries_per_shard(_max_entries_per_shard),
                                                                 result(_result) {
    shards.swap(_shards);
    iter = shards.begin();
  }
  bool spawn_next() override;
};

RGWRemoteMetaLog::~RGWRemoteMetaLog()
{
  delete error_logger;
}

int RGWRemoteMetaLog::read_log_info(rgw_mdlog_info *log_info)
{
  rgw_http_param_pair pairs[] = { { "type", "metadata" },
                                  { NULL, NULL } };

  int ret = conn->get_json_resource("/admin/log", pairs, *log_info);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to fetch mdlog info" << dendl;
    return ret;
  }

  ldpp_dout(dpp, 20) << "remote mdlog, num_shards=" << log_info->num_shards << dendl;

  return 0;
}

int RGWRemoteMetaLog::read_master_log_shards_info(const string &master_period, map<int, RGWMetadataLogInfo> *shards_info)
{
  if (store->svc()->zone->is_meta_master()) {
    return 0;
  }

  rgw_mdlog_info log_info;
  int ret = read_log_info(&log_info);
  if (ret < 0) {
    return ret;
  }

  return run(new RGWReadRemoteMDLogInfoCR(&sync_env, master_period, log_info.num_shards, shards_info));
}

int RGWRemoteMetaLog::read_master_log_shards_next(const string& period, map<int, string> shard_markers, map<int, rgw_mdlog_shard_data> *result)
{
  if (store->svc()->zone->is_meta_master()) {
    return 0;
  }

  return run(new RGWListRemoteMDLogCR(&sync_env, period, shard_markers, 1, result));
}

int RGWRemoteMetaLog::init()
{
  conn = store->svc()->zone->get_master_conn();

  int ret = http_manager.start();
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "failed in http_manager.start() ret=" << ret << dendl;
    return ret;
  }

  error_logger = new RGWSyncErrorLogger(store, RGW_SYNC_ERROR_LOG_SHARD_PREFIX, ERROR_LOGGER_SHARDS);

  init_sync_env(&sync_env);

  tn = sync_env.sync_tracer->add_node(sync_env.sync_tracer->root_node, "meta");

  return 0;
}

void RGWRemoteMetaLog::finish()
{
  going_down = true;
  stop();
}

#define CLONE_MAX_ENTRIES 100

int RGWMetaSyncStatusManager::init()
{
  if (store->svc()->zone->is_meta_master()) {
    return 0;
  }

  if (!store->svc()->zone->get_master_conn()) {
    lderr(store->ctx()) << "no REST connection to master zone" << dendl;
    return -EIO;
  }

  int r = rgw_init_ioctx(store->getRados()->get_rados_handle(), store->svc()->zone->get_zone_params().log_pool, ioctx, true);
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: failed to open log pool (" << store->svc()->zone->get_zone_params().log_pool << " ret=" << r << dendl;
    return r;
  }

  r = master_log.init();
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: failed to init remote log, r=" << r << dendl;
    return r;
  }

  RGWMetaSyncEnv& sync_env = master_log.get_sync_env();

  rgw_meta_sync_status sync_status;
  r = read_sync_status(&sync_status);
  if (r < 0 && r != -ENOENT) {
    lderr(store->ctx()) << "ERROR: failed to read sync status, r=" << r << dendl;
    return r;
  }

  int num_shards = sync_status.sync_info.num_shards;

  for (int i = 0; i < num_shards; i++) {
    shard_objs[i] = rgw_raw_obj(store->svc()->zone->get_zone_params().log_pool, sync_env.shard_obj_name(i));
  }

  std::unique_lock wl{ts_to_shard_lock};
  for (int i = 0; i < num_shards; i++) {
    clone_markers.push_back(string());
    utime_shard ut;
    ut.shard_id = i;
    ts_to_shard[ut] = i;
  }

  return 0;
}

unsigned RGWMetaSyncStatusManager::get_subsys() const
{
  return dout_subsys;
}

std::ostream&  RGWMetaSyncStatusManager::gen_prefix(std::ostream& out) const
{
  return out << "meta sync: ";
}

void RGWMetaSyncEnv::init(const DoutPrefixProvider *_dpp, CephContext *_cct, rgw::sal::RGWRadosStore *_store, RGWRESTConn *_conn,
                          RGWAsyncRadosProcessor *_async_rados, RGWHTTPManager *_http_manager,
                          RGWSyncErrorLogger *_error_logger, RGWSyncTraceManager *_sync_tracer) {
  dpp = _dpp;
  cct = _cct;
  store = _store;
  conn = _conn;
  async_rados = _async_rados;
  http_manager = _http_manager;
  error_logger = _error_logger;
  sync_tracer = _sync_tracer;
}

string RGWMetaSyncEnv::status_oid()
{
  return mdlog_sync_status_oid;
}

string RGWMetaSyncEnv::shard_obj_name(int shard_id)
{
  char buf[mdlog_sync_status_shard_prefix.size() + 16];
  snprintf(buf, sizeof(buf), "%s.%d", mdlog_sync_status_shard_prefix.c_str(), shard_id);

  return string(buf);
}

class RGWAsyncReadMDLogEntries : public RGWAsyncRadosRequest {
  rgw::sal::RGWRadosStore *store;
  RGWMetadataLog *mdlog;
  int shard_id;
  int max_entries;

protected:
  int _send_request() override {
    real_time from_time;
    real_time end_time;

    void *handle;

    mdlog->init_list_entries(shard_id, from_time, end_time, marker, &handle);

    int ret = mdlog->list_entries(handle, max_entries, entries, &marker, &truncated);

    mdlog->complete_list_entries(handle);

    return ret;
  }
public:
  string marker;
  list<cls_log_entry> entries;
  bool truncated;

  RGWAsyncReadMDLogEntries(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, rgw::sal::RGWRadosStore *_store,
                           RGWMetadataLog* mdlog, int _shard_id,
                           std::string _marker, int _max_entries)
    : RGWAsyncRadosRequest(caller, cn), store(_store), mdlog(mdlog),
      shard_id(_shard_id), max_entries(_max_entries), marker(std::move(_marker)) {}
};

class RGWReadMDLogEntriesCR : public RGWSimpleCoroutine {
  RGWMetaSyncEnv *sync_env;
  RGWMetadataLog *const mdlog;
  int shard_id;
  string marker;
  string *pmarker;
  int max_entries;
  list<cls_log_entry> *entries;
  bool *truncated;

  RGWAsyncReadMDLogEntries *req{nullptr};

public:
  RGWReadMDLogEntriesCR(RGWMetaSyncEnv *_sync_env, RGWMetadataLog* mdlog,
                        int _shard_id, string*_marker, int _max_entries,
                        list<cls_log_entry> *_entries, bool *_truncated)
    : RGWSimpleCoroutine(_sync_env->cct), sync_env(_sync_env), mdlog(mdlog),
      shard_id(_shard_id), pmarker(_marker), max_entries(_max_entries),
      entries(_entries), truncated(_truncated) {}

  ~RGWReadMDLogEntriesCR() override {
    if (req) {
      req->finish();
    }
  }

  int send_request() override {
    marker = *pmarker;
    req = new RGWAsyncReadMDLogEntries(this, stack->create_completion_notifier(),
                                       sync_env->store, mdlog, shard_id, marker,
                                       max_entries);
    sync_env->async_rados->queue(req);
    return 0;
  }

  int request_complete() override {
    *pmarker = std::move(req->marker);
    *entries = std::move(req->entries);
    *truncated = req->truncated;
    return req->get_ret_status();
  }
};


class RGWReadRemoteMDLogShardInfoCR : public RGWCoroutine {
  RGWMetaSyncEnv *env;
  RGWRESTReadResource *http_op;

  const std::string& period;
  int shard_id;
  RGWMetadataLogInfo *shard_info;

public:
  RGWReadRemoteMDLogShardInfoCR(RGWMetaSyncEnv *env, const std::string& period,
                                int _shard_id, RGWMetadataLogInfo *_shard_info)
    : RGWCoroutine(env->store->ctx()), env(env), http_op(NULL),
      period(period), shard_id(_shard_id), shard_info(_shard_info) {}

  int operate() override {
    auto store = env->store;
    RGWRESTConn *conn = store->svc()->zone->get_master_conn();
    reenter(this) {
      yield {
	char buf[16];
	snprintf(buf, sizeof(buf), "%d", shard_id);
        rgw_http_param_pair pairs[] = { { "type" , "metadata" },
	                                { "id", buf },
	                                { "period", period.c_str() },
					{ "info" , NULL },
	                                { NULL, NULL } };

        string p = "/admin/log/";

        http_op = new RGWRESTReadResource(conn, p, pairs, NULL,
                                          env->http_manager);

        init_new_io(http_op);

        int ret = http_op->aio_read();
        if (ret < 0) {
          ldpp_dout(env->dpp, 0) << "ERROR: failed to read from " << p << dendl;
          log_error() << "failed to send http operation: " << http_op->to_str() << " ret=" << ret << std::endl;
          http_op->put();
          return set_cr_error(ret);
        }

        return io_block(0);
      }
      yield {
        int ret = http_op->wait(shard_info, null_yield);
        http_op->put();
        if (ret < 0) {
          return set_cr_error(ret);
        }
        return set_cr_done();
      }
    }
    return 0;
  }
};

RGWCoroutine* create_read_remote_mdlog_shard_info_cr(RGWMetaSyncEnv *env,
                                                     const std::string& period,
                                                     int shard_id,
                                                     RGWMetadataLogInfo* info)
{
  return new RGWReadRemoteMDLogShardInfoCR(env, period, shard_id, info);
}

class RGWListRemoteMDLogShardCR : public RGWSimpleCoroutine {
  RGWMetaSyncEnv *sync_env;
  RGWRESTReadResource *http_op;

  const std::string& period;
  int shard_id;
  string marker;
  uint32_t max_entries;
  rgw_mdlog_shard_data *result;

public:
  RGWListRemoteMDLogShardCR(RGWMetaSyncEnv *env, const std::string& period,
                            int _shard_id, const string& _marker, uint32_t _max_entries,
                            rgw_mdlog_shard_data *_result)
    : RGWSimpleCoroutine(env->store->ctx()), sync_env(env), http_op(NULL),
      period(period), shard_id(_shard_id), marker(_marker), max_entries(_max_entries), result(_result) {}

  int send_request() override {
    RGWRESTConn *conn = sync_env->conn;

    char buf[32];
    snprintf(buf, sizeof(buf), "%d", shard_id);

    char max_entries_buf[32];
    snprintf(max_entries_buf, sizeof(max_entries_buf), "%d", (int)max_entries);

    const char *marker_key = (marker.empty() ? "" : "marker");

    rgw_http_param_pair pairs[] = { { "type", "metadata" },
      { "id", buf },
      { "period", period.c_str() },
      { "max-entries", max_entries_buf },
      { marker_key, marker.c_str() },
      { NULL, NULL } };

    string p = "/admin/log/";

    http_op = new RGWRESTReadResource(conn, p, pairs, NULL, sync_env->http_manager);
    init_new_io(http_op);

    int ret = http_op->aio_read();
    if (ret < 0) {
      ldpp_dout(sync_env->dpp, 0) << "ERROR: failed to read from " << p << dendl;
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
      ldpp_dout(sync_env->dpp, 0) << "ERROR: failed to list remote mdlog shard, ret=" << ret << dendl;
      return ret;
    }
    return 0;
  }
};

RGWCoroutine* create_list_remote_mdlog_shard_cr(RGWMetaSyncEnv *env,
                                                const std::string& period,
                                                int shard_id,
                                                const std::string& marker,
                                                uint32_t max_entries,
                                                rgw_mdlog_shard_data *result)
{
  return new RGWListRemoteMDLogShardCR(env, period, shard_id, marker,
                                       max_entries, result);
}

bool RGWReadRemoteMDLogInfoCR::spawn_next() {
  if (shard_id >= num_shards) {
    return false;
  }
  spawn(new RGWReadRemoteMDLogShardInfoCR(sync_env, period, shard_id, &(*mdlog_info)[shard_id]), false);
  shard_id++;
  return true;
}

bool RGWListRemoteMDLogCR::spawn_next() {
  if (iter == shards.end()) {
    return false;
  }

  spawn(new RGWListRemoteMDLogShardCR(sync_env, period, iter->first, iter->second, max_entries_per_shard, &(*result)[iter->first]), false);
  ++iter;
  return true;
}

class RGWInitSyncStatusCoroutine : public RGWCoroutine {
  RGWMetaSyncEnv *sync_env;

  rgw_meta_sync_info status;
  vector<RGWMetadataLogInfo> shards_info;
  boost::intrusive_ptr<RGWContinuousLeaseCR> lease_cr;
  boost::intrusive_ptr<RGWCoroutinesStack> lease_stack;
public:
  RGWInitSyncStatusCoroutine(RGWMetaSyncEnv *_sync_env,
                             const rgw_meta_sync_info &status)
    : RGWCoroutine(_sync_env->store->ctx()), sync_env(_sync_env),
      status(status), shards_info(status.num_shards),
      lease_cr(nullptr), lease_stack(nullptr) {}

  ~RGWInitSyncStatusCoroutine() override {
    if (lease_cr) {
      lease_cr->abort();
    }
  }

  int operate() override {
    int ret;
    reenter(this) {
      yield {
        set_status("acquiring sync lock");
	uint32_t lock_duration = cct->_conf->rgw_sync_lease_period;
        string lock_name = "sync_lock";
	rgw::sal::RGWRadosStore *store = sync_env->store;
        lease_cr.reset(new RGWContinuousLeaseCR(sync_env->async_rados, store,
                                                rgw_raw_obj(store->svc()->zone->get_zone_params().log_pool, sync_env->status_oid()),
                                                lock_name, lock_duration, this));
        lease_stack.reset(spawn(lease_cr.get(), false));
      }
      while (!lease_cr->is_locked()) {
        if (lease_cr->is_done()) {
          ldpp_dout(sync_env->dpp, 5) << "lease cr failed, done early " << dendl;
          set_status("lease lock failed, early abort");
          return set_cr_error(lease_cr->get_ret_status());
        }
        set_sleeping(true);
        yield;
      }
      yield {
        set_status("writing sync status");
	rgw::sal::RGWRadosStore *store = sync_env->store;
        call(new RGWSimpleRadosWriteCR<rgw_meta_sync_info>(sync_env->async_rados, store->svc()->sysobj,
                                                           rgw_raw_obj(store->svc()->zone->get_zone_params().log_pool, sync_env->status_oid()),
                                                           status));
      }

      if (retcode < 0) {
        set_status("failed to write sync status");
        ldpp_dout(sync_env->dpp, 0) << "ERROR: failed to write sync status, retcode=" << retcode << dendl;
        yield lease_cr->go_down();
        return set_cr_error(retcode);
      }
      /* fetch current position in logs */
      set_status("fetching remote log position");
      yield {
        for (int i = 0; i < (int)status.num_shards; i++) {
          spawn(new RGWReadRemoteMDLogShardInfoCR(sync_env, status.period, i,
                                                  &shards_info[i]), false);
	}
      }

      drain_all_but_stack(lease_stack.get()); /* the lease cr still needs to run */

      yield {
        set_status("updating sync status");
        for (int i = 0; i < (int)status.num_shards; i++) {
	  rgw_meta_sync_marker marker;
          RGWMetadataLogInfo& info = shards_info[i];
	  marker.next_step_marker = info.marker;
	  marker.timestamp = info.last_update;
	  rgw::sal::RGWRadosStore *store = sync_env->store;
          spawn(new RGWSimpleRadosWriteCR<rgw_meta_sync_marker>(sync_env->async_rados,
                                                                store->svc()->sysobj,
                                                                rgw_raw_obj(store->svc()->zone->get_zone_params().log_pool, sync_env->shard_obj_name(i)),
                                                                marker), true);
        }
      }
      yield {
        set_status("changing sync state: build full sync maps");
	status.state = rgw_meta_sync_info::StateBuildingFullSyncMaps;
	rgw::sal::RGWRadosStore *store = sync_env->store;
        call(new RGWSimpleRadosWriteCR<rgw_meta_sync_info>(sync_env->async_rados, store->svc()->sysobj,
                                                           rgw_raw_obj(store->svc()->zone->get_zone_params().log_pool, sync_env->status_oid()),
                                                           status));
      }
      set_status("drop lock lease");
      yield lease_cr->go_down();
      while (collect(&ret, NULL)) {
	if (ret < 0) {
	  return set_cr_error(ret);
	}
        yield;
      }
      drain_all();
      return set_cr_done();
    }
    return 0;
  }
};

class RGWReadSyncStatusMarkersCR : public RGWShardCollectCR {
  static constexpr int MAX_CONCURRENT_SHARDS = 16;

  RGWMetaSyncEnv *env;
  const int num_shards;
  int shard_id{0};
  map<uint32_t, rgw_meta_sync_marker>& markers;

 public:
  RGWReadSyncStatusMarkersCR(RGWMetaSyncEnv *env, int num_shards,
                             map<uint32_t, rgw_meta_sync_marker>& markers)
    : RGWShardCollectCR(env->cct, MAX_CONCURRENT_SHARDS),
      env(env), num_shards(num_shards), markers(markers)
  {}
  bool spawn_next() override;
};

bool RGWReadSyncStatusMarkersCR::spawn_next()
{
  if (shard_id >= num_shards) {
    return false;
  }
  using CR = RGWSimpleRadosReadCR<rgw_meta_sync_marker>;
  rgw_raw_obj obj{env->store->svc()->zone->get_zone_params().log_pool,
                  env->shard_obj_name(shard_id)};
  spawn(new CR(env->async_rados, env->store->svc()->sysobj, obj, &markers[shard_id]), false);
  shard_id++;
  return true;
}

class RGWReadSyncStatusCoroutine : public RGWCoroutine {
  RGWMetaSyncEnv *sync_env;
  rgw_meta_sync_status *sync_status;

public:
  RGWReadSyncStatusCoroutine(RGWMetaSyncEnv *_sync_env,
                             rgw_meta_sync_status *_status)
    : RGWCoroutine(_sync_env->cct), sync_env(_sync_env), sync_status(_status)
  {}
  int operate() override;
};

int RGWReadSyncStatusCoroutine::operate()
{
  reenter(this) {
    // read sync info
    using ReadInfoCR = RGWSimpleRadosReadCR<rgw_meta_sync_info>;
    yield {
      bool empty_on_enoent = false; // fail on ENOENT
      rgw_raw_obj obj{sync_env->store->svc()->zone->get_zone_params().log_pool,
                      sync_env->status_oid()};
      call(new ReadInfoCR(sync_env->async_rados, sync_env->store->svc()->sysobj, obj,
                          &sync_status->sync_info, empty_on_enoent));
    }
    if (retcode < 0) {
      ldpp_dout(sync_env->dpp, 4) << "failed to read sync status info with "
          << cpp_strerror(retcode) << dendl;
      return set_cr_error(retcode);
    }
    // read shard markers
    using ReadMarkersCR = RGWReadSyncStatusMarkersCR;
    yield call(new ReadMarkersCR(sync_env, sync_status->sync_info.num_shards,
                                 sync_status->sync_markers));
    if (retcode < 0) {
      ldpp_dout(sync_env->dpp, 4) << "failed to read sync status markers with "
          << cpp_strerror(retcode) << dendl;
      return set_cr_error(retcode);
    }
    return set_cr_done();
  }
  return 0;
}

class RGWFetchAllMetaCR : public RGWCoroutine {
  RGWMetaSyncEnv *sync_env;

  int num_shards;


  int ret_status;

  list<string> sections;
  list<string>::iterator sections_iter;

  struct meta_list_result {
    list<string> keys;
    string marker;
    uint64_t count{0};
    bool truncated{false};

    void decode_json(JSONObj *obj) {
      JSONDecoder::decode_json("keys", keys, obj);
      JSONDecoder::decode_json("marker", marker, obj);
      JSONDecoder::decode_json("count", count, obj);
      JSONDecoder::decode_json("truncated", truncated, obj);
    }
  } result;
  list<string>::iterator iter;

  std::unique_ptr<RGWShardedOmapCRManager> entries_index;

  boost::intrusive_ptr<RGWContinuousLeaseCR> lease_cr;
  boost::intrusive_ptr<RGWCoroutinesStack> lease_stack;
  bool lost_lock;
  bool failed;

  string marker;

  map<uint32_t, rgw_meta_sync_marker>& markers;

  RGWSyncTraceNodeRef tn;

public:
  RGWFetchAllMetaCR(RGWMetaSyncEnv *_sync_env, int _num_shards,
                    map<uint32_t, rgw_meta_sync_marker>& _markers,
                    RGWSyncTraceNodeRef& _tn_parent) : RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
						      num_shards(_num_shards),
						      ret_status(0), lease_cr(nullptr), lease_stack(nullptr),
                                                      lost_lock(false), failed(false), markers(_markers) {
    tn = sync_env->sync_tracer->add_node(_tn_parent, "fetch_all_meta");
  }

  ~RGWFetchAllMetaCR() override {
  }

  void append_section_from_set(set<string>& all_sections, const string& name) {
    set<string>::iterator iter = all_sections.find(name);
    if (iter != all_sections.end()) {
      sections.emplace_back(std::move(*iter));
      all_sections.erase(iter);
    }
  }
  /*
   * meta sync should go in the following order: user, bucket.instance, bucket
   * then whatever other sections exist (if any)
   */
  void rearrange_sections() {
    set<string> all_sections;
    std::move(sections.begin(), sections.end(),
              std::inserter(all_sections, all_sections.end()));
    sections.clear();

    append_section_from_set(all_sections, "user");
    append_section_from_set(all_sections, "bucket.instance");
    append_section_from_set(all_sections, "bucket");

    std::move(all_sections.begin(), all_sections.end(),
              std::back_inserter(sections));
  }

  int operate() override {
    RGWRESTConn *conn = sync_env->conn;

    reenter(this) {
      yield {
        set_status(string("acquiring lock (") + sync_env->status_oid() + ")");
	uint32_t lock_duration = cct->_conf->rgw_sync_lease_period;
        string lock_name = "sync_lock";
        lease_cr.reset(new RGWContinuousLeaseCR(sync_env->async_rados,
                                                sync_env->store,
                                                rgw_raw_obj(sync_env->store->svc()->zone->get_zone_params().log_pool, sync_env->status_oid()),
                                                lock_name, lock_duration, this));
        lease_stack.reset(spawn(lease_cr.get(), false));
      }
      while (!lease_cr->is_locked()) {
        if (lease_cr->is_done()) {
          ldpp_dout(sync_env->dpp, 5) << "lease cr failed, done early " << dendl;
          set_status("failed acquiring lock");
          return set_cr_error(lease_cr->get_ret_status());
        }
        set_sleeping(true);
        yield;
      }
      entries_index.reset(new RGWShardedOmapCRManager(sync_env->async_rados, sync_env->store, this, num_shards,
                                                      sync_env->store->svc()->zone->get_zone_params().log_pool,
                                                      mdlog_sync_full_sync_index_prefix));
      yield {
	call(new RGWReadRESTResourceCR<list<string> >(cct, conn, sync_env->http_manager,
				       "/admin/metadata", NULL, &sections));
      }
      if (get_ret_status() < 0) {
        ldpp_dout(sync_env->dpp, 0) << "ERROR: failed to fetch metadata sections" << dendl;
        yield entries_index->finish();
        yield lease_cr->go_down();
        drain_all();
	return set_cr_error(get_ret_status());
      }
      rearrange_sections();
      sections_iter = sections.begin();
      for (; sections_iter != sections.end(); ++sections_iter) {
        do {
          yield {
#define META_FULL_SYNC_CHUNK_SIZE "1000"
            string entrypoint = string("/admin/metadata/") + *sections_iter;
            rgw_http_param_pair pairs[] = { { "max-entries", META_FULL_SYNC_CHUNK_SIZE },
              { "marker", result.marker.c_str() },
              { NULL, NULL } };
            result.keys.clear();
            call(new RGWReadRESTResourceCR<meta_list_result >(cct, conn, sync_env->http_manager,
                                                              entrypoint, pairs, &result));
          }
          ret_status = get_ret_status();
          if (ret_status == -ENOENT) {
            set_retcode(0); /* reset coroutine status so that we don't return it */
            ret_status = 0;
          }
          if (ret_status < 0) {
            tn->log(0, SSTR("ERROR: failed to fetch metadata section: " << *sections_iter));
            yield entries_index->finish();
            yield lease_cr->go_down();
            drain_all();
            return set_cr_error(ret_status);
          }
          iter = result.keys.begin();
          for (; iter != result.keys.end(); ++iter) {
            if (!lease_cr->is_locked()) {
              lost_lock = true;
              break;
            }
            yield; // allow entries_index consumer to make progress

            tn->log(20, SSTR("list metadata: section=" << *sections_iter << " key=" << *iter));
            string s = *sections_iter + ":" + *iter;
            int shard_id;
	    rgw::sal::RGWRadosStore *store = sync_env->store;
            int ret = store->ctl()->meta.mgr->get_shard_id(*sections_iter, *iter, &shard_id);
            if (ret < 0) {
              tn->log(0, SSTR("ERROR: could not determine shard id for " << *sections_iter << ":" << *iter));
              ret_status = ret;
              break;
            }
            if (!entries_index->append(s, shard_id)) {
              break;
            }
          }
        } while (result.truncated);
      }
      yield {
        if (!entries_index->finish()) {
          failed = true;
        }
      }
      if (!failed) {
        for (map<uint32_t, rgw_meta_sync_marker>::iterator iter = markers.begin(); iter != markers.end(); ++iter) {
          int shard_id = (int)iter->first;
          rgw_meta_sync_marker& marker = iter->second;
          marker.total_entries = entries_index->get_total_entries(shard_id);
          spawn(new RGWSimpleRadosWriteCR<rgw_meta_sync_marker>(sync_env->async_rados, sync_env->store->svc()->sysobj,
                                                                rgw_raw_obj(sync_env->store->svc()->zone->get_zone_params().log_pool, sync_env->shard_obj_name(shard_id)),
                                                                marker), true);
        }
      }

      drain_all_but_stack(lease_stack.get()); /* the lease cr still needs to run */

      yield lease_cr->go_down();

      int ret;
      while (collect(&ret, NULL)) {
	if (ret < 0) {
	  return set_cr_error(ret);
	}
        yield;
      }
      drain_all();
      if (failed) {
        yield return set_cr_error(-EIO);
      }
      if (lost_lock) {
        yield return set_cr_error(-EBUSY);
      }

      if (ret_status < 0) {
        yield return set_cr_error(ret_status);
      }

      yield return set_cr_done();
    }
    return 0;
  }
};

static string full_sync_index_shard_oid(int shard_id)
{
  char buf[mdlog_sync_full_sync_index_prefix.size() + 16];
  snprintf(buf, sizeof(buf), "%s.%d", mdlog_sync_full_sync_index_prefix.c_str(), shard_id);
  return string(buf);
}

class RGWReadRemoteMetadataCR : public RGWCoroutine {
  RGWMetaSyncEnv *sync_env;

  RGWRESTReadResource *http_op;

  string section;
  string key;

  bufferlist *pbl;

  RGWSyncTraceNodeRef tn;

public:
  RGWReadRemoteMetadataCR(RGWMetaSyncEnv *_sync_env,
                                                      const string& _section, const string& _key, bufferlist *_pbl,
                                                      const RGWSyncTraceNodeRef& _tn_parent) : RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
                                                      http_op(NULL),
                                                      section(_section),
                                                      key(_key),
						      pbl(_pbl) {
    tn = sync_env->sync_tracer->add_node(_tn_parent, "read_remote_meta",
                                         section + ":" + key);
  }

  int operate() override {
    RGWRESTConn *conn = sync_env->conn;
    reenter(this) {
      yield {
        rgw_http_param_pair pairs[] = { { "key" , key.c_str()},
	                                { NULL, NULL } };

        string p = string("/admin/metadata/") + section + "/" + key;

        http_op = new RGWRESTReadResource(conn, p, pairs, NULL, sync_env->http_manager);

        init_new_io(http_op);

        int ret = http_op->aio_read();
        if (ret < 0) {
          ldpp_dout(sync_env->dpp, 0) << "ERROR: failed to fetch mdlog data" << dendl;
          log_error() << "failed to send http operation: " << http_op->to_str() << " ret=" << ret << std::endl;
          http_op->put();
          return set_cr_error(ret);
        }

        return io_block(0);
      }
      yield {
        int ret = http_op->wait(pbl, null_yield);
        http_op->put();
        if (ret < 0) {
          return set_cr_error(ret);
        }
        return set_cr_done();
      }
    }
    return 0;
  }
};

class RGWAsyncMetaStoreEntry : public RGWAsyncRadosRequest {
  rgw::sal::RGWRadosStore *store;
  string raw_key;
  bufferlist bl;
protected:
  int _send_request() override {
    int ret = store->ctl()->meta.mgr->put(raw_key, bl, null_yield, RGWMDLogSyncType::APPLY_ALWAYS);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: can't store key: " << raw_key << " ret=" << ret << dendl;
      return ret;
    }
    return 0;
  }
public:
  RGWAsyncMetaStoreEntry(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, rgw::sal::RGWRadosStore *_store,
                       const string& _raw_key,
                       bufferlist& _bl) : RGWAsyncRadosRequest(caller, cn), store(_store),
                                          raw_key(_raw_key), bl(_bl) {}
};


class RGWMetaStoreEntryCR : public RGWSimpleCoroutine {
  RGWMetaSyncEnv *sync_env;
  string raw_key;
  bufferlist bl;

  RGWAsyncMetaStoreEntry *req;

public:
  RGWMetaStoreEntryCR(RGWMetaSyncEnv *_sync_env,
                       const string& _raw_key,
                       bufferlist& _bl) : RGWSimpleCoroutine(_sync_env->cct), sync_env(_sync_env),
                                          raw_key(_raw_key), bl(_bl), req(NULL) {
  }

  ~RGWMetaStoreEntryCR() override {
    if (req) {
      req->finish();
    }
  }

  int send_request() override {
    req = new RGWAsyncMetaStoreEntry(this, stack->create_completion_notifier(),
			           sync_env->store, raw_key, bl);
    sync_env->async_rados->queue(req);
    return 0;
  }

  int request_complete() override {
    return req->get_ret_status();
  }
};

class RGWAsyncMetaRemoveEntry : public RGWAsyncRadosRequest {
  rgw::sal::RGWRadosStore *store;
  string raw_key;
protected:
  int _send_request() override {
    int ret = store->ctl()->meta.mgr->remove(raw_key, null_yield);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: can't remove key: " << raw_key << " ret=" << ret << dendl;
      return ret;
    }
    return 0;
  }
public:
  RGWAsyncMetaRemoveEntry(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, rgw::sal::RGWRadosStore *_store,
                       const string& _raw_key) : RGWAsyncRadosRequest(caller, cn), store(_store),
                                          raw_key(_raw_key) {}
};


class RGWMetaRemoveEntryCR : public RGWSimpleCoroutine {
  RGWMetaSyncEnv *sync_env;
  string raw_key;

  RGWAsyncMetaRemoveEntry *req;

public:
  RGWMetaRemoveEntryCR(RGWMetaSyncEnv *_sync_env,
                       const string& _raw_key) : RGWSimpleCoroutine(_sync_env->cct), sync_env(_sync_env),
                                          raw_key(_raw_key), req(NULL) {
  }

  ~RGWMetaRemoveEntryCR() override {
    if (req) {
      req->finish();
    }
  }

  int send_request() override {
    req = new RGWAsyncMetaRemoveEntry(this, stack->create_completion_notifier(),
			           sync_env->store, raw_key);
    sync_env->async_rados->queue(req);
    return 0;
  }

  int request_complete() override {
    int r = req->get_ret_status();
    if (r == -ENOENT) {
      r = 0;
    }
    return r;
  }
};

#define META_SYNC_UPDATE_MARKER_WINDOW 10


int RGWLastCallerWinsCR::operate() {
  RGWCoroutine *call_cr;
  reenter(this) {
    while (cr) {
      call_cr = cr;
      cr = nullptr;
      yield call(call_cr);
      /* cr might have been modified at this point */
    }
    return set_cr_done();
  }
  return 0;
}

class RGWMetaSyncShardMarkerTrack : public RGWSyncShardMarkerTrack<string, string> {
  RGWMetaSyncEnv *sync_env;

  string marker_oid;
  rgw_meta_sync_marker sync_marker;

  RGWSyncTraceNodeRef tn;

public:
  RGWMetaSyncShardMarkerTrack(RGWMetaSyncEnv *_sync_env,
                         const string& _marker_oid,
                         const rgw_meta_sync_marker& _marker,
                         RGWSyncTraceNodeRef& _tn) : RGWSyncShardMarkerTrack(META_SYNC_UPDATE_MARKER_WINDOW),
                                                                sync_env(_sync_env),
                                                                marker_oid(_marker_oid),
                                                                sync_marker(_marker),
                                                                tn(_tn){}

  RGWCoroutine *store_marker(const string& new_marker, uint64_t index_pos, const real_time& timestamp) override {
    sync_marker.marker = new_marker;
    if (index_pos > 0) {
      sync_marker.pos = index_pos;
    }

    if (!real_clock::is_zero(timestamp)) {
      sync_marker.timestamp = timestamp;
    }

    ldpp_dout(sync_env->dpp, 20) << __func__ << "(): updating marker marker_oid=" << marker_oid << " marker=" << new_marker << " realm_epoch=" << sync_marker.realm_epoch << dendl;
    tn->log(20, SSTR("new marker=" << new_marker));
    rgw::sal::RGWRadosStore *store = sync_env->store;
    return new RGWSimpleRadosWriteCR<rgw_meta_sync_marker>(sync_env->async_rados,
                                                           store->svc()->sysobj,
                                                           rgw_raw_obj(store->svc()->zone->get_zone_params().log_pool, marker_oid),
                                                           sync_marker);
  }

  RGWOrderCallCR *allocate_order_control_cr() override {
    return new RGWLastCallerWinsCR(sync_env->cct);
  }
};

RGWMetaSyncSingleEntryCR::RGWMetaSyncSingleEntryCR(RGWMetaSyncEnv *_sync_env,
		           const string& _raw_key, const string& _entry_marker,
                           const RGWMDLogStatus& _op_status,
                           RGWMetaSyncShardMarkerTrack *_marker_tracker, const RGWSyncTraceNodeRef& _tn_parent) : RGWCoroutine(_sync_env->cct),
                                                      sync_env(_sync_env),
						      raw_key(_raw_key), entry_marker(_entry_marker),
                                                      op_status(_op_status),
                                                      pos(0), sync_status(0),
                                                      marker_tracker(_marker_tracker), tries(0) {
  error_injection = (sync_env->cct->_conf->rgw_sync_meta_inject_err_probability > 0);
  tn = sync_env->sync_tracer->add_node(_tn_parent, "entry", raw_key);
}

int RGWMetaSyncSingleEntryCR::operate() {
  reenter(this) {
#define NUM_TRANSIENT_ERROR_RETRIES 10

    if (error_injection &&
        rand() % 10000 < cct->_conf->rgw_sync_meta_inject_err_probability * 10000.0) {
      return set_cr_error(-EIO);
    }

    if (op_status != MDLOG_STATUS_COMPLETE) {
      tn->log(20, "skipping pending operation");
      yield call(marker_tracker->finish(entry_marker));
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    tn->set_flag(RGW_SNS_FLAG_ACTIVE);
    for (tries = 0; tries < NUM_TRANSIENT_ERROR_RETRIES; tries++) {
      yield {
        pos = raw_key.find(':');
        section = raw_key.substr(0, pos);
        key = raw_key.substr(pos + 1);
        tn->log(10, SSTR("fetching remote metadata entry" << (tries == 0 ? "" : " (retry)")));
        call(new RGWReadRemoteMetadataCR(sync_env, section, key, &md_bl, tn));
      }

      sync_status = retcode;

      if (sync_status == -ENOENT) {
        /* FIXME: do we need to remove the entry from the local zone? */
        break;
      }

      if ((sync_status == -EAGAIN || sync_status == -ECANCELED) && (tries < NUM_TRANSIENT_ERROR_RETRIES - 1)) {
        ldpp_dout(sync_env->dpp, 20) << *this << ": failed to fetch remote metadata: " << section << ":" << key << ", will retry" << dendl;
        continue;
      }

      if (sync_status < 0) {
        tn->log(10, SSTR("failed to send read remote metadata entry: section=" << section << " key=" << key << " status=" << sync_status));
        log_error() << "failed to send read remote metadata entry: section=" << section << " key=" << key << " status=" << sync_status << std::endl;
        yield call(sync_env->error_logger->log_error_cr(sync_env->conn->get_remote_id(), section, key, -sync_status,
                                                        string("failed to read remote metadata entry: ") + cpp_strerror(-sync_status)));
        return set_cr_error(sync_status);
      }

      break;
    }

    retcode = 0;
    for (tries = 0; tries < NUM_TRANSIENT_ERROR_RETRIES; tries++) {
      if (sync_status != -ENOENT) {
        tn->log(10, SSTR("storing local metadata entry"));
          yield call(new RGWMetaStoreEntryCR(sync_env, raw_key, md_bl));
      } else {
        tn->log(10, SSTR("removing local metadata entry"));
          yield call(new RGWMetaRemoveEntryCR(sync_env, raw_key));
      }
      if ((retcode == -EAGAIN || retcode == -ECANCELED) && (tries < NUM_TRANSIENT_ERROR_RETRIES - 1)) {
        ldpp_dout(sync_env->dpp, 20) << *this << ": failed to store metadata: " << section << ":" << key << ", got retcode=" << retcode << dendl;
        continue;
      }
      break;
    }

    sync_status = retcode;

    if (sync_status == 0 && marker_tracker) {
      /* update marker */
      yield call(marker_tracker->finish(entry_marker));
      sync_status = retcode;
    }
    if (sync_status < 0) {
      tn->log(10, SSTR("failed, status=" << sync_status));
      return set_cr_error(sync_status);
    }
    tn->log(10, "success");
    return set_cr_done();
  }
  return 0;
}

class RGWCloneMetaLogCoroutine : public RGWCoroutine {
  RGWMetaSyncEnv *sync_env;
  RGWMetadataLog *mdlog;

  const std::string& period;
  int shard_id;
  string marker;
  bool truncated = false;
  string *new_marker;

  int max_entries = CLONE_MAX_ENTRIES;

  RGWRESTReadResource *http_op = nullptr;
  boost::intrusive_ptr<RGWMetadataLogInfoCompletion> completion;

  RGWMetadataLogInfo shard_info;
  rgw_mdlog_shard_data data;

public:
  RGWCloneMetaLogCoroutine(RGWMetaSyncEnv *_sync_env, RGWMetadataLog* mdlog,
                           const std::string& period, int _id,
                           const string& _marker, string *_new_marker)
    : RGWCoroutine(_sync_env->cct), sync_env(_sync_env), mdlog(mdlog),
      period(period), shard_id(_id), marker(_marker), new_marker(_new_marker) {
    if (new_marker) {
      *new_marker = marker;
    }
  }
  ~RGWCloneMetaLogCoroutine() override {
    if (http_op) {
      http_op->put();
    }
    if (completion) {
      completion->cancel();
    }
  }

  int operate() override;

  int state_init();
  int state_read_shard_status();
  int state_read_shard_status_complete();
  int state_send_rest_request();
  int state_receive_rest_response();
  int state_store_mdlog_entries();
  int state_store_mdlog_entries_complete();
};

class RGWMetaSyncShardCR : public RGWCoroutine {
  RGWMetaSyncEnv *sync_env;

  const rgw_pool& pool;
  const std::string& period; //< currently syncing period id
  const epoch_t realm_epoch; //< realm_epoch of period
  RGWMetadataLog* mdlog; //< log of syncing period
  uint32_t shard_id;
  rgw_meta_sync_marker& sync_marker;
  boost::optional<rgw_meta_sync_marker> temp_marker; //< for pending updates
  string marker;
  string max_marker;
  const std::string& period_marker; //< max marker stored in next period

  RGWRadosGetOmapKeysCR::ResultPtr omapkeys;
  std::set<std::string> entries;
  std::set<std::string>::iterator iter;

  string oid;

  RGWMetaSyncShardMarkerTrack *marker_tracker = nullptr;

  list<cls_log_entry> log_entries;
  list<cls_log_entry>::iterator log_iter;
  bool truncated = false;

  string mdlog_marker;
  string raw_key;
  rgw_mdlog_entry mdlog_entry;

  ceph::mutex inc_lock = ceph::make_mutex("RGWMetaSyncShardCR::inc_lock");
  ceph::condition_variable inc_cond;

  boost::asio::coroutine incremental_cr;
  boost::asio::coroutine full_cr;

  boost::intrusive_ptr<RGWContinuousLeaseCR> lease_cr;
  boost::intrusive_ptr<RGWCoroutinesStack> lease_stack;

  bool lost_lock = false;

  bool *reset_backoff;

  // hold a reference to the cr stack while it's in the map
  using StackRef = boost::intrusive_ptr<RGWCoroutinesStack>;
  map<StackRef, string> stack_to_pos;
  map<string, string> pos_to_prev;

  bool can_adjust_marker = false;
  bool done_with_period = false;

  int total_entries = 0;

  RGWSyncTraceNodeRef tn;
public:
  RGWMetaSyncShardCR(RGWMetaSyncEnv *_sync_env, const rgw_pool& _pool,
                     const std::string& period, epoch_t realm_epoch,
                     RGWMetadataLog* mdlog, uint32_t _shard_id,
                     rgw_meta_sync_marker& _marker,
                     const std::string& period_marker, bool *_reset_backoff,
                     RGWSyncTraceNodeRef& _tn)
    : RGWCoroutine(_sync_env->cct), sync_env(_sync_env), pool(_pool),
      period(period), realm_epoch(realm_epoch), mdlog(mdlog),
      shard_id(_shard_id), sync_marker(_marker),
      period_marker(period_marker),
      reset_backoff(_reset_backoff), tn(_tn) {
    *reset_backoff = false;
  }

  ~RGWMetaSyncShardCR() override {
    delete marker_tracker;
    if (lease_cr) {
      lease_cr->abort();
    }
  }

  void set_marker_tracker(RGWMetaSyncShardMarkerTrack *mt) {
    delete marker_tracker;
    marker_tracker = mt;
  }

  int operate() override {
    int r;
    while (true) {
      switch (sync_marker.state) {
      case rgw_meta_sync_marker::FullSync:
        r  = full_sync();
        if (r < 0) {
          ldpp_dout(sync_env->dpp, 10) << "sync: full_sync: shard_id=" << shard_id << " r=" << r << dendl;
          return set_cr_error(r);
        }
        return 0;
      case rgw_meta_sync_marker::IncrementalSync:
        r  = incremental_sync();
        if (r < 0) {
          ldpp_dout(sync_env->dpp, 10) << "sync: incremental_sync: shard_id=" << shard_id << " r=" << r << dendl;
          return set_cr_error(r);
        }
        return 0;
      }
    }
    /* unreachable */
    return 0;
  }

  void collect_children()
  {
    int child_ret;
    RGWCoroutinesStack *child;
    while (collect_next(&child_ret, &child)) {
      auto iter = stack_to_pos.find(child);
      if (iter == stack_to_pos.end()) {
        /* some other stack that we don't care about */
        continue;
      }

      string& pos = iter->second;

      if (child_ret < 0) {
        ldpp_dout(sync_env->dpp, 0) << *this << ": child operation stack=" << child << " entry=" << pos << " returned " << child_ret << dendl;
      }

      map<string, string>::iterator prev_iter = pos_to_prev.find(pos);
      ceph_assert(prev_iter != pos_to_prev.end());

      /*
       * we should get -EAGAIN for transient errors, for which we want to retry, so we don't
       * update the marker and abort. We'll get called again for these. Permanent errors will be
       * handled by marking the entry at the error log shard, so that we retry on it separately
       */
      if (child_ret == -EAGAIN) {
        can_adjust_marker = false;
      }

      if (pos_to_prev.size() == 1) {
        if (can_adjust_marker) {
          sync_marker.marker = pos;
        }
        pos_to_prev.erase(prev_iter);
      } else {
        ceph_assert(pos_to_prev.size() > 1);
        pos_to_prev.erase(prev_iter);
        prev_iter = pos_to_prev.begin();
        if (can_adjust_marker) {
          sync_marker.marker = prev_iter->second;
        }
      }

      ldpp_dout(sync_env->dpp, 4) << *this << ": adjusting marker pos=" << sync_marker.marker << dendl;
      stack_to_pos.erase(iter);
    }
  }

  int full_sync() {
#define OMAP_GET_MAX_ENTRIES 100
    int max_entries = OMAP_GET_MAX_ENTRIES;
    reenter(&full_cr) {
      set_status("full_sync");
      tn->log(10, "start full sync");
      oid = full_sync_index_shard_oid(shard_id);
      can_adjust_marker = true;
      /* grab lock */
      yield {
	uint32_t lock_duration = cct->_conf->rgw_sync_lease_period;
        string lock_name = "sync_lock";
	rgw::sal::RGWRadosStore *store = sync_env->store;
        lease_cr.reset(new RGWContinuousLeaseCR(sync_env->async_rados, store,
                                                rgw_raw_obj(pool, sync_env->shard_obj_name(shard_id)),
                                                lock_name, lock_duration, this));
        lease_stack.reset(spawn(lease_cr.get(), false));
        lost_lock = false;
      }
      while (!lease_cr->is_locked()) {
        if (lease_cr->is_done()) {
          drain_all();
          tn->log(5, "failed to take lease");
          return lease_cr->get_ret_status();
        }
        set_sleeping(true);
        yield;
      }
      tn->log(10, "took lease");

      /* lock succeeded, a retry now should avoid previous backoff status */
      *reset_backoff = true;

      /* prepare marker tracker */
      set_marker_tracker(new RGWMetaSyncShardMarkerTrack(sync_env,
                                                         sync_env->shard_obj_name(shard_id),
                                                         sync_marker, tn));

      marker = sync_marker.marker;

      total_entries = sync_marker.pos;

      /* sync! */
      do {
        if (!lease_cr->is_locked()) {
          tn->log(10, "lost lease");
          lost_lock = true;
          break;
        }
        omapkeys = std::make_shared<RGWRadosGetOmapKeysCR::Result>();
        yield call(new RGWRadosGetOmapKeysCR(sync_env->store, rgw_raw_obj(pool, oid),
                                             marker, max_entries, omapkeys));
        if (retcode < 0) {
          ldpp_dout(sync_env->dpp, 0) << "ERROR: " << __func__ << "(): RGWRadosGetOmapKeysCR() returned ret=" << retcode << dendl;
          tn->log(0, SSTR("ERROR: failed to list omap keys, status=" << retcode));
          yield lease_cr->go_down();
          drain_all();
          return retcode;
        }
        entries = std::move(omapkeys->entries);
        tn->log(20, SSTR("retrieved " << entries.size() << " entries to sync"));
        if (entries.size() > 0) {
          tn->set_flag(RGW_SNS_FLAG_ACTIVE); /* actually have entries to sync */
        }
        iter = entries.begin();
        for (; iter != entries.end(); ++iter) {
          marker = *iter;
          tn->log(20, SSTR("full sync: " << marker));
          total_entries++;
          if (!marker_tracker->start(marker, total_entries, real_time())) {
            tn->log(0, SSTR("ERROR: cannot start syncing " << marker << ". Duplicate entry?"));
          } else {
            // fetch remote and write locally
            yield {
              RGWCoroutinesStack *stack = spawn(new RGWMetaSyncSingleEntryCR(sync_env, marker, marker, MDLOG_STATUS_COMPLETE, marker_tracker, tn), false);
              // stack_to_pos holds a reference to the stack
              stack_to_pos[stack] = marker;
              pos_to_prev[marker] = marker;
            }
          }
        }
        collect_children();
      } while (omapkeys->more && can_adjust_marker);

      tn->unset_flag(RGW_SNS_FLAG_ACTIVE); /* actually have entries to sync */

      while (num_spawned() > 1) {
        yield wait_for_child();
        collect_children();
      }

      if (!lost_lock) {
        /* update marker to reflect we're done with full sync */
        if (can_adjust_marker) {
          // apply updates to a temporary marker, or operate() will send us
          // to incremental_sync() after we yield
          temp_marker = sync_marker;
	  temp_marker->state = rgw_meta_sync_marker::IncrementalSync;
	  temp_marker->marker = std::move(temp_marker->next_step_marker);
	  temp_marker->next_step_marker.clear();
	  temp_marker->realm_epoch = realm_epoch;
	  ldpp_dout(sync_env->dpp, 4) << *this << ": saving marker pos=" << temp_marker->marker << " realm_epoch=" << realm_epoch << dendl;

	  using WriteMarkerCR = RGWSimpleRadosWriteCR<rgw_meta_sync_marker>;
	  yield call(new WriteMarkerCR(sync_env->async_rados, sync_env->store->svc()->sysobj,
				       rgw_raw_obj(pool, sync_env->shard_obj_name(shard_id)),
				       *temp_marker));
        }

        if (retcode < 0) {
          ldpp_dout(sync_env->dpp, 0) << "ERROR: failed to set sync marker: retcode=" << retcode << dendl;
          yield lease_cr->go_down();
          drain_all();
          return retcode;
        }
      }

      /* 
       * if we reached here, it means that lost_lock is true, otherwise the state
       * change in the previous block will prevent us from reaching here
       */

      yield lease_cr->go_down();

      lease_cr.reset();

      drain_all();

      if (!can_adjust_marker) {
        return -EAGAIN;
      }

      if (lost_lock) {
        return -EBUSY;
      }

      tn->log(10, "full sync complete");

      // apply the sync marker update
      ceph_assert(temp_marker);
      sync_marker = std::move(*temp_marker);
      temp_marker = boost::none;
      // must not yield after this point!
    }
    return 0;
  }
    

  int incremental_sync() {
    reenter(&incremental_cr) {
      set_status("incremental_sync");
      tn->log(10, "start incremental sync");
      can_adjust_marker = true;
      /* grab lock */
      if (!lease_cr) { /* could have had  a lease_cr lock from previous state */
        yield {
          uint32_t lock_duration = cct->_conf->rgw_sync_lease_period;
          string lock_name = "sync_lock";
	  rgw::sal::RGWRadosStore *store = sync_env->store;
          lease_cr.reset( new RGWContinuousLeaseCR(sync_env->async_rados, store,
                                                   rgw_raw_obj(pool, sync_env->shard_obj_name(shard_id)),
                                                   lock_name, lock_duration, this));
          lease_stack.reset(spawn(lease_cr.get(), false));
          lost_lock = false;
        }
        while (!lease_cr->is_locked()) {
          if (lease_cr->is_done()) {
            drain_all();
            tn->log(10, "failed to take lease");
            return lease_cr->get_ret_status();
          }
          set_sleeping(true);
          yield;
        }
      }
      tn->log(10, "took lease");
      // if the period has advanced, we can't use the existing marker
      if (sync_marker.realm_epoch < realm_epoch) {
        ldpp_dout(sync_env->dpp, 4) << "clearing marker=" << sync_marker.marker
            << " from old realm_epoch=" << sync_marker.realm_epoch
            << " (now " << realm_epoch << ')' << dendl;
        sync_marker.realm_epoch = realm_epoch;
        sync_marker.marker.clear();
      }
      mdlog_marker = sync_marker.marker;
      set_marker_tracker(new RGWMetaSyncShardMarkerTrack(sync_env,
                                                         sync_env->shard_obj_name(shard_id),
                                                         sync_marker, tn));

      /*
       * mdlog_marker: the remote sync marker positiion
       * sync_marker: the local sync marker position
       * max_marker: the max mdlog position that we fetched
       * marker: the current position we try to sync
       * period_marker: the last marker before the next period begins (optional)
       */
      marker = max_marker = sync_marker.marker;
      /* inc sync */
      do {
        if (!lease_cr->is_locked()) {
          lost_lock = true;
          tn->log(10, "lost lease");
          break;
        }
#define INCREMENTAL_MAX_ENTRIES 100
        ldpp_dout(sync_env->dpp, 20) << __func__ << ":" << __LINE__ << ": shard_id=" << shard_id << " mdlog_marker=" << mdlog_marker << " sync_marker.marker=" << sync_marker.marker << " period_marker=" << period_marker << dendl;
        if (!period_marker.empty() && period_marker <= mdlog_marker) {
          tn->log(10, SSTR("finished syncing current period: mdlog_marker=" << mdlog_marker << " sync_marker=" << sync_marker.marker << " period_marker=" << period_marker));
          done_with_period = true;
          break;
        }
	if (mdlog_marker <= max_marker) {
	  /* we're at the tip, try to bring more entries */
          ldpp_dout(sync_env->dpp, 20) << __func__ << ":" << __LINE__ << ": shard_id=" << shard_id << " syncing mdlog for shard_id=" << shard_id << dendl;
          yield call(new RGWCloneMetaLogCoroutine(sync_env, mdlog,
                                                  period, shard_id,
                                                  mdlog_marker, &mdlog_marker));
	}
        if (retcode < 0) {
          tn->log(10, SSTR(*this << ": failed to fetch more log entries, retcode=" << retcode));
          yield lease_cr->go_down();
          drain_all();
          *reset_backoff = false; // back off and try again later
          return retcode;
        }
        *reset_backoff = true; /* if we got to this point, all systems function */
	if (mdlog_marker > max_marker) {
          tn->set_flag(RGW_SNS_FLAG_ACTIVE); /* actually have entries to sync */
          tn->log(20, SSTR("mdlog_marker=" << mdlog_marker << " sync_marker=" << sync_marker.marker));
          marker = max_marker;
          yield call(new RGWReadMDLogEntriesCR(sync_env, mdlog, shard_id,
                                               &max_marker, INCREMENTAL_MAX_ENTRIES,
                                               &log_entries, &truncated));
          if (retcode < 0) {
            tn->log(10, SSTR("failed to list mdlog entries, retcode=" << retcode));
            yield lease_cr->go_down();
            drain_all();
            *reset_backoff = false; // back off and try again later
            return retcode;
          }
          for (log_iter = log_entries.begin(); log_iter != log_entries.end() && !done_with_period; ++log_iter) {
            if (!period_marker.empty() && period_marker <= log_iter->id) {
              done_with_period = true;
              if (period_marker < log_iter->id) {
                tn->log(10, SSTR("found key=" << log_iter->id
                    << " past period_marker=" << period_marker));
                break;
              }
              ldpp_dout(sync_env->dpp, 10) << "found key at period_marker=" << period_marker << dendl;
              // sync this entry, then return control to RGWMetaSyncCR
            }
            if (!mdlog_entry.convert_from(*log_iter)) {
              tn->log(0, SSTR("ERROR: failed to convert mdlog entry, shard_id=" << shard_id << " log_entry: " << log_iter->id << ":" << log_iter->section << ":" << log_iter->name << ":" << log_iter->timestamp << " ... skipping entry"));
              continue;
            }
            tn->log(20, SSTR("log_entry: " << log_iter->id << ":" << log_iter->section << ":" << log_iter->name << ":" << log_iter->timestamp));
            if (!marker_tracker->start(log_iter->id, 0, log_iter->timestamp.to_real_time())) {
              ldpp_dout(sync_env->dpp, 0) << "ERROR: cannot start syncing " << log_iter->id << ". Duplicate entry?" << dendl;
            } else {
              raw_key = log_iter->section + ":" + log_iter->name;
              yield {
                RGWCoroutinesStack *stack = spawn(new RGWMetaSyncSingleEntryCR(sync_env, raw_key, log_iter->id, mdlog_entry.log_data.status, marker_tracker, tn), false);
                ceph_assert(stack);
                // stack_to_pos holds a reference to the stack
                stack_to_pos[stack] = log_iter->id;
                pos_to_prev[log_iter->id] = marker;
              }
            }
            marker = log_iter->id;
          }
        }
        collect_children();
	ldpp_dout(sync_env->dpp, 20) << __func__ << ":" << __LINE__ << ": shard_id=" << shard_id << " mdlog_marker=" << mdlog_marker << " max_marker=" << max_marker << " sync_marker.marker=" << sync_marker.marker << " period_marker=" << period_marker << dendl;
        if (done_with_period) {
          // return control to RGWMetaSyncCR and advance to the next period
          tn->log(10, SSTR(*this << ": done with period"));
          break;
        }
	if (mdlog_marker == max_marker && can_adjust_marker) {
          tn->unset_flag(RGW_SNS_FLAG_ACTIVE);
#define INCREMENTAL_INTERVAL 20
	  yield wait(utime_t(INCREMENTAL_INTERVAL, 0));
	}
      } while (can_adjust_marker);

      tn->unset_flag(RGW_SNS_FLAG_ACTIVE);

      while (num_spawned() > 1) {
        yield wait_for_child();
        collect_children();
      }

      yield lease_cr->go_down();

      drain_all();

      if (lost_lock) {
        return -EBUSY;
      }

      if (!can_adjust_marker) {
        return -EAGAIN;
      }

      return set_cr_done();
    }
    /* TODO */
    return 0;
  }
};

class RGWMetaSyncShardControlCR : public RGWBackoffControlCR
{
  RGWMetaSyncEnv *sync_env;

  const rgw_pool& pool;
  const std::string& period;
  epoch_t realm_epoch;
  RGWMetadataLog* mdlog;
  uint32_t shard_id;
  rgw_meta_sync_marker sync_marker;
  const std::string period_marker;

  RGWSyncTraceNodeRef tn;

  static constexpr bool exit_on_error = false; // retry on all errors
public:
  RGWMetaSyncShardControlCR(RGWMetaSyncEnv *_sync_env, const rgw_pool& _pool,
                            const std::string& period, epoch_t realm_epoch,
                            RGWMetadataLog* mdlog, uint32_t _shard_id,
                            const rgw_meta_sync_marker& _marker,
                            std::string&& period_marker,
                            RGWSyncTraceNodeRef& _tn_parent)
    : RGWBackoffControlCR(_sync_env->cct, exit_on_error), sync_env(_sync_env),
      pool(_pool), period(period), realm_epoch(realm_epoch), mdlog(mdlog),
      shard_id(_shard_id), sync_marker(_marker),
      period_marker(std::move(period_marker)) {
    tn = sync_env->sync_tracer->add_node(_tn_parent, "shard",
                                         std::to_string(shard_id));
  }

  RGWCoroutine *alloc_cr() override {
    return new RGWMetaSyncShardCR(sync_env, pool, period, realm_epoch, mdlog,
                                  shard_id, sync_marker, period_marker, backoff_ptr(), tn);
  }

  RGWCoroutine *alloc_finisher_cr() override {
    rgw::sal::RGWRadosStore *store = sync_env->store;
    return new RGWSimpleRadosReadCR<rgw_meta_sync_marker>(sync_env->async_rados, store->svc()->sysobj,
                                                          rgw_raw_obj(pool, sync_env->shard_obj_name(shard_id)),
                                                          &sync_marker);
  }
};

class RGWMetaSyncCR : public RGWCoroutine {
  RGWMetaSyncEnv *sync_env;
  const rgw_pool& pool;
  RGWPeriodHistory::Cursor cursor; //< sync position in period history
  RGWPeriodHistory::Cursor next; //< next period in history
  rgw_meta_sync_status sync_status;
  RGWSyncTraceNodeRef tn;

  std::mutex mutex; //< protect access to shard_crs

  // TODO: it should be enough to hold a reference on the stack only, as calling
  // RGWCoroutinesStack::wakeup() doesn't refer to the RGWCoroutine if it has
  // already completed
  using ControlCRRef = boost::intrusive_ptr<RGWMetaSyncShardControlCR>;
  using StackRef = boost::intrusive_ptr<RGWCoroutinesStack>;
  using RefPair = std::pair<ControlCRRef, StackRef>;
  map<int, RefPair> shard_crs;
  int ret{0};

public:
  RGWMetaSyncCR(RGWMetaSyncEnv *_sync_env, const RGWPeriodHistory::Cursor &cursor,
                const rgw_meta_sync_status& _sync_status, RGWSyncTraceNodeRef& _tn)
    : RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
      pool(sync_env->store->svc()->zone->get_zone_params().log_pool),
      cursor(cursor), sync_status(_sync_status), tn(_tn) {}

  ~RGWMetaSyncCR() {
  }

  int operate() override {
    reenter(this) {
      // loop through one period at a time
      tn->log(1, "start");
      for (;;) {
        if (cursor == sync_env->store->svc()->mdlog->get_period_history()->get_current()) {
          next = RGWPeriodHistory::Cursor{};
          if (cursor) {
            ldpp_dout(sync_env->dpp, 10) << "RGWMetaSyncCR on current period="
                << cursor.get_period().get_id() << dendl;
          } else {
            ldpp_dout(sync_env->dpp, 10) << "RGWMetaSyncCR with no period" << dendl;
          }
        } else {
          next = cursor;
          next.next();
          ldpp_dout(sync_env->dpp, 10) << "RGWMetaSyncCR on period="
              << cursor.get_period().get_id() << ", next="
              << next.get_period().get_id() << dendl;
        }

        yield {
          // get the mdlog for the current period (may be empty)
          auto& period_id = sync_status.sync_info.period;
          auto realm_epoch = sync_status.sync_info.realm_epoch;
          auto mdlog = sync_env->store->svc()->mdlog->get_log(period_id);

          tn->log(1, SSTR("realm epoch=" << realm_epoch << " period id=" << period_id));

          // prevent wakeup() from accessing shard_crs while we're spawning them
          std::lock_guard<std::mutex> lock(mutex);

          // sync this period on each shard
          for (const auto& m : sync_status.sync_markers) {
            uint32_t shard_id = m.first;
            auto& marker = m.second;

            std::string period_marker;
            if (next) {
              // read the maximum marker from the next period's sync status
              period_marker = next.get_period().get_sync_status()[shard_id];
              if (period_marker.empty()) {
                // no metadata changes have occurred on this shard, skip it
                ldpp_dout(sync_env->dpp, 10) << "RGWMetaSyncCR: skipping shard " << shard_id
                    << " with empty period marker" << dendl;
                continue;
              }
            }

            using ShardCR = RGWMetaSyncShardControlCR;
            auto cr = new ShardCR(sync_env, pool, period_id, realm_epoch,
                                  mdlog, shard_id, marker,
                                  std::move(period_marker), tn);
            auto stack = spawn(cr, false);
            shard_crs[shard_id] = RefPair{cr, stack};
          }
        }
        // wait for each shard to complete
        while (ret == 0 && num_spawned() > 0) {
          yield wait_for_child();
          collect(&ret, nullptr);
        }
        drain_all();
        {
          // drop shard cr refs under lock
          std::lock_guard<std::mutex> lock(mutex);
          shard_crs.clear();
        }
        if (ret < 0) {
          return set_cr_error(ret);
        }
        // advance to the next period
        ceph_assert(next);
        cursor = next;

        // write the updated sync info
        sync_status.sync_info.period = cursor.get_period().get_id();
        sync_status.sync_info.realm_epoch = cursor.get_epoch();
        yield call(new RGWSimpleRadosWriteCR<rgw_meta_sync_info>(sync_env->async_rados,
                                                                 sync_env->store->svc()->sysobj,
                                                                 rgw_raw_obj(pool, sync_env->status_oid()),
                                                                 sync_status.sync_info));
      }
    }
    return 0;
  }

  void wakeup(int shard_id) {
    std::lock_guard<std::mutex> lock(mutex);
    auto iter = shard_crs.find(shard_id);
    if (iter == shard_crs.end()) {
      return;
    }
    iter->second.first->wakeup();
  }
};

void RGWRemoteMetaLog::init_sync_env(RGWMetaSyncEnv *env) {
  env->dpp = dpp;
  env->cct = store->ctx();
  env->store = store;
  env->conn = conn;
  env->async_rados = async_rados;
  env->http_manager = &http_manager;
  env->error_logger = error_logger;
  env->sync_tracer = store->getRados()->get_sync_tracer();
}

int RGWRemoteMetaLog::read_sync_status(rgw_meta_sync_status *sync_status)
{
  if (store->svc()->zone->is_meta_master()) {
    return 0;
  }
  // cannot run concurrently with run_sync(), so run in a separate manager
  RGWCoroutinesManager crs(store->ctx(), store->getRados()->get_cr_registry());
  RGWHTTPManager http_manager(store->ctx(), crs.get_completion_mgr());
  int ret = http_manager.start();
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "failed in http_manager.start() ret=" << ret << dendl;
    return ret;
  }
  RGWMetaSyncEnv sync_env_local = sync_env;
  sync_env_local.http_manager = &http_manager;
  tn->log(20, "read sync status");
  ret = crs.run(new RGWReadSyncStatusCoroutine(&sync_env_local, sync_status));
  http_manager.stop();
  return ret;
}

int RGWRemoteMetaLog::init_sync_status()
{
  if (store->svc()->zone->is_meta_master()) {
    return 0;
  }

  rgw_mdlog_info mdlog_info;
  int r = read_log_info(&mdlog_info);
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: fail to fetch master log info (r=" << r << ")" << dendl;
    return r;
  }

  rgw_meta_sync_info sync_info;
  sync_info.num_shards = mdlog_info.num_shards;
  auto cursor = store->svc()->mdlog->get_period_history()->get_current();
  if (cursor) {
    sync_info.period = cursor.get_period().get_id();
    sync_info.realm_epoch = cursor.get_epoch();
  }

  return run(new RGWInitSyncStatusCoroutine(&sync_env, sync_info));
}

int RGWRemoteMetaLog::store_sync_info(const rgw_meta_sync_info& sync_info)
{
  tn->log(20, "store sync info");
  return run(new RGWSimpleRadosWriteCR<rgw_meta_sync_info>(async_rados, store->svc()->sysobj,
                                                           rgw_raw_obj(store->svc()->zone->get_zone_params().log_pool, sync_env.status_oid()),
                                                           sync_info));
}

// return a cursor to the period at our sync position
static RGWPeriodHistory::Cursor get_period_at(rgw::sal::RGWRadosStore* store,
                                              const rgw_meta_sync_info& info)
{
  if (info.period.empty()) {
    // return an empty cursor with error=0
    return RGWPeriodHistory::Cursor{};
  }

  // look for an existing period in our history
  auto cursor = store->svc()->mdlog->get_period_history()->lookup(info.realm_epoch);
  if (cursor) {
    // verify that the period ids match
    auto& existing = cursor.get_period().get_id();
    if (existing != info.period) {
      lderr(store->ctx()) << "ERROR: sync status period=" << info.period
          << " does not match period=" << existing
          << " in history at realm epoch=" << info.realm_epoch << dendl;
      return RGWPeriodHistory::Cursor{-EEXIST};
    }
    return cursor;
  }

  // read the period from rados or pull it from the master
  RGWPeriod period;
  int r = store->svc()->mdlog->pull_period(info.period, period);
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: failed to read period id "
        << info.period << ": " << cpp_strerror(r) << dendl;
    return RGWPeriodHistory::Cursor{r};
  }
  // attach the period to our history
  cursor = store->svc()->mdlog->get_period_history()->attach(std::move(period));
  if (!cursor) {
    r = cursor.get_error();
    lderr(store->ctx()) << "ERROR: failed to read period history back to "
        << info.period << ": " << cpp_strerror(r) << dendl;
  }
  return cursor;
}

int RGWRemoteMetaLog::run_sync()
{
  if (store->svc()->zone->is_meta_master()) {
    return 0;
  }

  int r = 0;

  // get shard count and oldest log period from master
  rgw_mdlog_info mdlog_info;
  for (;;) {
    if (going_down) {
      ldpp_dout(dpp, 1) << __func__ << "(): going down" << dendl;
      return 0;
    }
    r = read_log_info(&mdlog_info);
    if (r == -EIO || r == -ENOENT) {
      // keep retrying if master isn't alive or hasn't initialized the log
      ldpp_dout(dpp, 10) << __func__ << "(): waiting for master.." << dendl;
      backoff.backoff_sleep();
      continue;
    }
    backoff.reset();
    if (r < 0) {
      lderr(store->ctx()) << "ERROR: fail to fetch master log info (r=" << r << ")" << dendl;
      return r;
    }
    break;
  }

  rgw_meta_sync_status sync_status;
  do {
    if (going_down) {
      ldpp_dout(dpp, 1) << __func__ << "(): going down" << dendl;
      return 0;
    }
    r = run(new RGWReadSyncStatusCoroutine(&sync_env, &sync_status));
    if (r < 0 && r != -ENOENT) {
      ldpp_dout(dpp, 0) << "ERROR: failed to fetch sync status r=" << r << dendl;
      return r;
    }

    if (!mdlog_info.period.empty()) {
      // restart sync if the remote has a period, but:
      // a) our status does not, or
      // b) our sync period comes before the remote's oldest log period
      if (sync_status.sync_info.period.empty() ||
          sync_status.sync_info.realm_epoch < mdlog_info.realm_epoch) {
        sync_status.sync_info.state = rgw_meta_sync_info::StateInit;
        string reason;
        if (sync_status.sync_info.period.empty()) {
          reason = "period is empty";
        } else {
          reason = SSTR("sync_info realm epoch is behind: " << sync_status.sync_info.realm_epoch << " < " << mdlog_info.realm_epoch);
        }
        tn->log(1, "initialize sync (reason: " + reason + ")");
        ldpp_dout(dpp, 1) << "epoch=" << sync_status.sync_info.realm_epoch
           << " in sync status comes before remote's oldest mdlog epoch="
           << mdlog_info.realm_epoch << ", restarting sync" << dendl;
      }
    }

    if (sync_status.sync_info.state == rgw_meta_sync_info::StateInit) {
      ldpp_dout(dpp, 20) << __func__ << "(): init" << dendl;
      sync_status.sync_info.num_shards = mdlog_info.num_shards;
      auto cursor = store->svc()->mdlog->get_period_history()->get_current();
      if (cursor) {
        // run full sync, then start incremental from the current period/epoch
        sync_status.sync_info.period = cursor.get_period().get_id();
        sync_status.sync_info.realm_epoch = cursor.get_epoch();
      }
      r = run(new RGWInitSyncStatusCoroutine(&sync_env, sync_status.sync_info));
      if (r == -EBUSY) {
        backoff.backoff_sleep();
        continue;
      }
      backoff.reset();
      if (r < 0) {
        ldpp_dout(dpp, 0) << "ERROR: failed to init sync status r=" << r << dendl;
        return r;
      }
    }
  } while (sync_status.sync_info.state == rgw_meta_sync_info::StateInit);

  auto num_shards = sync_status.sync_info.num_shards;
  if (num_shards != mdlog_info.num_shards) {
    lderr(store->ctx()) << "ERROR: can't sync, mismatch between num shards, master num_shards=" << mdlog_info.num_shards << " local num_shards=" << num_shards << dendl;
    return -EINVAL;
  }

  RGWPeriodHistory::Cursor cursor;
  do {
    r = run(new RGWReadSyncStatusCoroutine(&sync_env, &sync_status));
    if (r < 0 && r != -ENOENT) {
      tn->log(0, SSTR("ERROR: failed to fetch sync status r=" << r));
      return r;
    }

    switch ((rgw_meta_sync_info::SyncState)sync_status.sync_info.state) {
      case rgw_meta_sync_info::StateBuildingFullSyncMaps:
        tn->log(20, "building full sync maps");
        r = run(new RGWFetchAllMetaCR(&sync_env, num_shards, sync_status.sync_markers, tn));
        if (r == -EBUSY || r == -EAGAIN) {
          backoff.backoff_sleep();
          continue;
        }
        backoff.reset();
        if (r < 0) {
          tn->log(0, SSTR("ERROR: failed to fetch all metadata keys (r=" << r << ")"));
          return r;
        }

        sync_status.sync_info.state = rgw_meta_sync_info::StateSync;
        r = store_sync_info(sync_status.sync_info);
        if (r < 0) {
          tn->log(0, SSTR("ERROR: failed to update sync status (r=" << r << ")"));
          return r;
        }
        /* fall through */
      case rgw_meta_sync_info::StateSync:
        tn->log(20, "sync");
        // find our position in the period history (if any)
        cursor = get_period_at(store, sync_status.sync_info);
        r = cursor.get_error();
        if (r < 0) {
          return r;
        }
        meta_sync_cr = new RGWMetaSyncCR(&sync_env, cursor, sync_status, tn);
        r = run(meta_sync_cr);
        if (r < 0) {
          tn->log(0, "ERROR: failed to fetch all metadata keys");
          return r;
        }
        break;
      default:
        tn->log(0, "ERROR: bad sync state!");
        return -EIO;
    }
  } while (!going_down);

  return 0;
}

void RGWRemoteMetaLog::wakeup(int shard_id)
{
  if (!meta_sync_cr) {
    return;
  }
  meta_sync_cr->wakeup(shard_id);
}

int RGWCloneMetaLogCoroutine::operate()
{
  reenter(this) {
    do {
      yield {
        ldpp_dout(sync_env->dpp, 20) << __func__ << ": shard_id=" << shard_id << ": init request" << dendl;
        return state_init();
      }
      yield {
        ldpp_dout(sync_env->dpp, 20) << __func__ << ": shard_id=" << shard_id << ": reading shard status" << dendl;
        return state_read_shard_status();
      }
      yield {
        ldpp_dout(sync_env->dpp, 20) << __func__ << ": shard_id=" << shard_id << ": reading shard status complete" << dendl;
        return state_read_shard_status_complete();
      }
      yield {
        ldpp_dout(sync_env->dpp, 20) << __func__ << ": shard_id=" << shard_id << ": sending rest request" << dendl;
        return state_send_rest_request();
      }
      yield {
        ldpp_dout(sync_env->dpp, 20) << __func__ << ": shard_id=" << shard_id << ": receiving rest response" << dendl;
        return state_receive_rest_response();
      }
      yield {
        ldpp_dout(sync_env->dpp, 20) << __func__ << ": shard_id=" << shard_id << ": storing mdlog entries" << dendl;
        return state_store_mdlog_entries();
      }
    } while (truncated);
    yield {
      ldpp_dout(sync_env->dpp, 20) << __func__ << ": shard_id=" << shard_id << ": storing mdlog entries complete" << dendl;
      return state_store_mdlog_entries_complete();
    }
  }

  return 0;
}

int RGWCloneMetaLogCoroutine::state_init()
{
  data = rgw_mdlog_shard_data();

  return 0;
}

int RGWCloneMetaLogCoroutine::state_read_shard_status()
{
  const bool add_ref = false; // default constructs with refs=1

  completion.reset(new RGWMetadataLogInfoCompletion(
    [this](int ret, const cls_log_header& header) {
      if (ret < 0) {
        if (ret != -ENOENT) {
          ldpp_dout(sync_env->dpp, 1) << "ERROR: failed to read mdlog info with "
                                      << cpp_strerror(ret) << dendl;
        }
      } else {
        shard_info.marker = header.max_marker;
        shard_info.last_update = header.max_time.to_real_time();
      }
      // wake up parent stack
      io_complete();
    }), add_ref);

  int ret = mdlog->get_info_async(shard_id, completion.get());
  if (ret < 0) {
    ldpp_dout(sync_env->dpp, 0) << "ERROR: mdlog->get_info_async() returned ret=" << ret << dendl;
    return set_cr_error(ret);
  }

  return io_block(0);
}

int RGWCloneMetaLogCoroutine::state_read_shard_status_complete()
{
  completion.reset();

  ldpp_dout(sync_env->dpp, 20) << "shard_id=" << shard_id << " marker=" << shard_info.marker << " last_update=" << shard_info.last_update << dendl;

  marker = shard_info.marker;

  return 0;
}

int RGWCloneMetaLogCoroutine::state_send_rest_request()
{
  RGWRESTConn *conn = sync_env->conn;

  char buf[32];
  snprintf(buf, sizeof(buf), "%d", shard_id);

  char max_entries_buf[32];
  snprintf(max_entries_buf, sizeof(max_entries_buf), "%d", max_entries);

  const char *marker_key = (marker.empty() ? "" : "marker");

  rgw_http_param_pair pairs[] = { { "type", "metadata" },
                                  { "id", buf },
                                  { "period", period.c_str() },
                                  { "max-entries", max_entries_buf },
                                  { marker_key, marker.c_str() },
                                  { NULL, NULL } };

  http_op = new RGWRESTReadResource(conn, "/admin/log", pairs, NULL, sync_env->http_manager);

  init_new_io(http_op);

  int ret = http_op->aio_read();
  if (ret < 0) {
    ldpp_dout(sync_env->dpp, 0) << "ERROR: failed to fetch mdlog data" << dendl;
    log_error() << "failed to send http operation: " << http_op->to_str() << " ret=" << ret << std::endl;
    http_op->put();
    http_op = NULL;
    return set_cr_error(ret);
  }

  return io_block(0);
}

int RGWCloneMetaLogCoroutine::state_receive_rest_response()
{
  int ret = http_op->wait(&data, null_yield);
  if (ret < 0) {
    error_stream << "http operation failed: " << http_op->to_str() << " status=" << http_op->get_http_status() << std::endl;
    ldpp_dout(sync_env->dpp, 5) << "failed to wait for op, ret=" << ret << dendl;
    http_op->put();
    http_op = NULL;
    return set_cr_error(ret);
  }
  http_op->put();
  http_op = NULL;

  ldpp_dout(sync_env->dpp, 20) << "remote mdlog, shard_id=" << shard_id << " num of shard entries: " << data.entries.size() << dendl;

  truncated = ((int)data.entries.size() == max_entries);

  if (data.entries.empty()) {
    if (new_marker) {
      *new_marker = marker;
    }
    return set_cr_done();
  }

  if (new_marker) {
    *new_marker = data.entries.back().id;
  }

  return 0;
}


int RGWCloneMetaLogCoroutine::state_store_mdlog_entries()
{
  list<cls_log_entry> dest_entries;

  vector<rgw_mdlog_entry>::iterator iter;
  for (iter = data.entries.begin(); iter != data.entries.end(); ++iter) {
    rgw_mdlog_entry& entry = *iter;
    ldpp_dout(sync_env->dpp, 20) << "entry: name=" << entry.name << dendl;

    cls_log_entry dest_entry;
    dest_entry.id = entry.id;
    dest_entry.section = entry.section;
    dest_entry.name = entry.name;
    dest_entry.timestamp = utime_t(entry.timestamp);
  
    encode(entry.log_data, dest_entry.data);

    dest_entries.push_back(dest_entry);

    marker = entry.id;
  }

  RGWAioCompletionNotifier *cn = stack->create_completion_notifier();

  int ret = mdlog->store_entries_in_shard(dest_entries, shard_id, cn->completion());
  if (ret < 0) {
    cn->put();
    ldpp_dout(sync_env->dpp, 10) << "failed to store md log entries shard_id=" << shard_id << " ret=" << ret << dendl;
    return set_cr_error(ret);
  }
  return io_block(0);
}

int RGWCloneMetaLogCoroutine::state_store_mdlog_entries_complete()
{
  return set_cr_done();
}
