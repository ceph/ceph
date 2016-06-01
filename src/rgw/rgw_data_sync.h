#ifndef CEPH_RGW_DATA_SYNC_H
#define CEPH_RGW_DATA_SYNC_H

#include "rgw_coroutine.h"
#include "rgw_http_client.h"
#include "rgw_bucket.h"

#include "common/RWLock.h"


struct rgw_datalog_info {
  uint32_t num_shards;

  rgw_datalog_info() : num_shards(0) {}

  void decode_json(JSONObj *obj);
};

struct rgw_data_sync_info {
  enum SyncState {
    StateInit = 0,
    StateBuildingFullSyncMaps = 1,
    StateSync = 2,
  };

  uint16_t state;
  uint32_t num_shards;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(state, bl);
    ::encode(num_shards, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
     DECODE_START(1, bl);
     ::decode(state, bl);
     ::decode(num_shards, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const {
    string s;
    switch ((SyncState)state) {
      case StateInit:
	s = "init";
	break;
      case StateBuildingFullSyncMaps:
	s = "building-full-sync-maps";
	break;
      case StateSync:
	s = "sync";
	break;
      default:
	s = "unknown";
	break;
    }
    encode_json("status", s, f);
    encode_json("num_shards", num_shards, f);
  }

  rgw_data_sync_info() : state((int)StateInit), num_shards(0) {}
};
WRITE_CLASS_ENCODER(rgw_data_sync_info)

struct rgw_data_sync_marker {
  enum SyncState {
    FullSync = 0,
    IncrementalSync = 1,
  };
  uint16_t state;
  string marker;
  string next_step_marker;
  uint64_t total_entries;
  uint64_t pos;
  real_time timestamp;

  rgw_data_sync_marker() : state(FullSync), total_entries(0), pos(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(state, bl);
    ::encode(marker, bl);
    ::encode(next_step_marker, bl);
    ::encode(total_entries, bl);
    ::encode(pos, bl);
    ::encode(timestamp, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
     DECODE_START(1, bl);
    ::decode(state, bl);
    ::decode(marker, bl);
    ::decode(next_step_marker, bl);
    ::decode(total_entries, bl);
    ::decode(pos, bl);
    ::decode(timestamp, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const {
    encode_json("state", (int)state, f);
    encode_json("marker", marker, f);
    encode_json("next_step_marker", next_step_marker, f);
    encode_json("total_entries", total_entries, f);
    encode_json("pos", pos, f);
    encode_json("timestamp", utime_t(timestamp), f);
  }
};
WRITE_CLASS_ENCODER(rgw_data_sync_marker)

struct rgw_data_sync_status {
  rgw_data_sync_info sync_info;
  map<uint32_t, rgw_data_sync_marker> sync_markers;

  rgw_data_sync_status() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(sync_info, bl);
    /* sync markers are encoded separately */
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
     DECODE_START(1, bl);
    ::decode(sync_info, bl);
    /* sync markers are decoded separately */
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const {
    encode_json("info", sync_info, f);
    encode_json("markers", sync_markers, f);
  }
};
WRITE_CLASS_ENCODER(rgw_data_sync_status)

struct rgw_datalog_entry {
  string key;
  ceph::real_time timestamp;

  void decode_json(JSONObj *obj);
};

struct rgw_datalog_shard_data {
  string marker;
  bool truncated;
  vector<rgw_datalog_entry> entries;

  void decode_json(JSONObj *obj);
};

class RGWAsyncRadosProcessor;
class RGWDataSyncStatusManager;
class RGWDataSyncControlCR;

struct RGWDataSyncEnv {
  CephContext *cct;
  RGWRados *store;
  RGWRESTConn *conn;
  RGWAsyncRadosProcessor *async_rados;
  RGWHTTPManager *http_manager;
  RGWSyncErrorLogger *error_logger;
  string source_zone;

  RGWDataSyncEnv() : cct(NULL), store(NULL), conn(NULL), async_rados(NULL), http_manager(NULL), error_logger(NULL) {}

  void init(CephContext *_cct, RGWRados *_store, RGWRESTConn *_conn,
            RGWAsyncRadosProcessor *_async_rados, RGWHTTPManager *_http_manager,
            RGWSyncErrorLogger *_error_logger, const string& _source_zone) {
    cct = _cct;
    store = _store;
    conn = _conn;
    async_rados = _async_rados;
    http_manager = _http_manager;
    error_logger = _error_logger;
    source_zone = _source_zone;
  }

  string shard_obj_name(int shard_id);
  string status_oid();
};

class RGWRemoteDataLog : public RGWCoroutinesManager {
  RGWRados *store;
  RGWAsyncRadosProcessor *async_rados;
  RGWHTTPManager http_manager;

  RGWDataSyncEnv sync_env;

  RWLock lock;
  RGWDataSyncControlCR *data_sync_cr;

  bool initialized;

public:
  RGWRemoteDataLog(RGWRados *_store, RGWAsyncRadosProcessor *async_rados)
    : RGWCoroutinesManager(_store->ctx(), _store->get_cr_registry()),
      store(_store), async_rados(async_rados),
      http_manager(store->ctx(), completion_mgr),
      lock("RGWRemoteDataLog::lock"), data_sync_cr(NULL),
      initialized(false) {}
  int init(const string& _source_zone, RGWRESTConn *_conn, RGWSyncErrorLogger *_error_logger);
  void finish();

  int read_log_info(rgw_datalog_info *log_info);
  int read_source_log_shards_info(map<int, RGWDataChangesLogInfo> *shards_info);
  int read_source_log_shards_next(map<int, string> shard_markers, map<int, rgw_datalog_shard_data> *result);
  int get_shard_info(int shard_id);
  int read_sync_status(rgw_data_sync_status *sync_status);
  int init_sync_status(int num_shards);
  int run_sync(int num_shards, rgw_data_sync_status& sync_status);

  void wakeup(int shard_id, set<string>& keys);
};

class RGWDataSyncStatusManager {
  RGWRados *store;
  librados::IoCtx ioctx;

  string source_zone;
  RGWRESTConn *conn;
  RGWSyncErrorLogger *error_logger;

  RGWRemoteDataLog source_log;

  string source_status_oid;
  string source_shard_status_oid_prefix;
  rgw_obj source_status_obj;

  rgw_data_sync_status sync_status;
  map<int, rgw_obj> shard_objs;

  int num_shards;

public:
  RGWDataSyncStatusManager(RGWRados *_store, RGWAsyncRadosProcessor *async_rados,
                           const string& _source_zone)
    : store(_store), source_zone(_source_zone), conn(NULL), error_logger(NULL),
      source_log(store, async_rados), num_shards(0) {}
  ~RGWDataSyncStatusManager() {
    finalize();
  }
  int init();
  void finalize();

  rgw_data_sync_status& get_sync_status() { return sync_status; }

  static string shard_obj_name(const string& source_zone, int shard_id);
  static string sync_status_oid(const string& source_zone);

  int read_sync_status() { return source_log.read_sync_status(&sync_status); }
  int init_sync_status() { return source_log.init_sync_status(num_shards); }

  int read_log_info(rgw_datalog_info *log_info) {
    return source_log.read_log_info(log_info);
  }
  int read_source_log_shards_info(map<int, RGWDataChangesLogInfo> *shards_info) {
    return source_log.read_source_log_shards_info(shards_info);
  }
  int read_source_log_shards_next(map<int, string> shard_markers, map<int, rgw_datalog_shard_data> *result) {
    return source_log.read_source_log_shards_next(shard_markers, result);
  }

  int run() { return source_log.run_sync(num_shards, sync_status); }

  void wakeup(int shard_id, set<string>& keys) { return source_log.wakeup(shard_id, keys); }
  void stop() {
    source_log.finish();
  }
};

class RGWBucketSyncStatusManager;
class RGWBucketSyncCR;

struct rgw_bucket_shard_full_sync_marker {
  rgw_obj_key position;
  uint64_t count;

  rgw_bucket_shard_full_sync_marker() : count(0) {}

  void encode_attr(map<string, bufferlist>& attrs);

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(position, bl);
    ::encode(count, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
     DECODE_START(1, bl);
    ::decode(position, bl);
    ::decode(count, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const {
    encode_json("position", position, f);
    encode_json("count", count, f);
  }
};
WRITE_CLASS_ENCODER(rgw_bucket_shard_full_sync_marker)

struct rgw_bucket_shard_inc_sync_marker {
  string position;

  rgw_bucket_shard_inc_sync_marker() {}

  void encode_attr(map<string, bufferlist>& attrs);

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(position, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
     DECODE_START(1, bl);
    ::decode(position, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const {
    encode_json("position", position, f);
  }

  bool operator<(const rgw_bucket_shard_inc_sync_marker& m) const {
    return (position < m.position);
  }
};
WRITE_CLASS_ENCODER(rgw_bucket_shard_inc_sync_marker)

struct rgw_bucket_shard_sync_info {
  enum SyncState {
    StateInit = 0,
    StateFullSync = 1,
    StateIncrementalSync = 2,
  };

  uint16_t state;
  rgw_bucket_shard_full_sync_marker full_marker;
  rgw_bucket_shard_inc_sync_marker inc_marker;

  void decode_from_attrs(CephContext *cct, map<string, bufferlist>& attrs);
  void encode_all_attrs(map<string, bufferlist>& attrs);
  void encode_state_attr(map<string, bufferlist>& attrs);

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(state, bl);
    ::encode(full_marker, bl);
    ::encode(inc_marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
     DECODE_START(1, bl);
     ::decode(state, bl);
     ::decode(full_marker, bl);
     ::decode(inc_marker, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const {
    string s;
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
      default:
	s = "unknown";
	break;
    }
    encode_json("status", s, f);
    encode_json("full_marker", full_marker, f);
    encode_json("inc_marker", inc_marker, f);
  }

  rgw_bucket_shard_sync_info() : state((int)StateInit) {}

};
WRITE_CLASS_ENCODER(rgw_bucket_shard_sync_info)


class RGWRemoteBucketLog : public RGWCoroutinesManager {
  RGWRados *store;
  RGWRESTConn *conn;
  string source_zone;
  string bucket_name;
  string bucket_id;
  int shard_id;

  RGWBucketSyncStatusManager *status_manager;
  RGWAsyncRadosProcessor *async_rados;
  RGWHTTPManager *http_manager;

  RGWDataSyncEnv sync_env;

  RGWBucketSyncCR *sync_cr;

public:
  RGWRemoteBucketLog(RGWRados *_store, RGWBucketSyncStatusManager *_sm,
                     RGWAsyncRadosProcessor *_async_rados, RGWHTTPManager *_http_manager) : RGWCoroutinesManager(_store->ctx(), _store->get_cr_registry()), store(_store),
                                       conn(NULL), shard_id(0),
                                       status_manager(_sm), async_rados(_async_rados), http_manager(_http_manager),
                                       sync_cr(NULL) {}

  int init(const string& _source_zone, RGWRESTConn *_conn, const string& _bucket_name, const string& _bucket_id, int _shard_id, RGWSyncErrorLogger *_error_logger);
  void finish();

  RGWCoroutine *read_sync_status_cr(rgw_bucket_shard_sync_info *sync_status);
  RGWCoroutine *init_sync_status_cr();
  RGWCoroutine *run_sync_cr();

  void wakeup();
};

class RGWBucketSyncStatusManager {
  RGWRados *store;
  librados::IoCtx ioctx;

  RGWCoroutinesManager cr_mgr;

  RGWAsyncRadosProcessor *async_rados;
  RGWHTTPManager http_manager;

  string source_zone;
  RGWRESTConn *conn;
  RGWSyncErrorLogger *error_logger;

  string bucket_name;
  string bucket_id;

  map<int, RGWRemoteBucketLog *> source_logs;

  string source_status_oid;
  string source_shard_status_oid_prefix;
  rgw_obj source_status_obj;

  map<int, rgw_bucket_shard_sync_info> sync_status;
  rgw_obj status_obj;

  int num_shards;

public:
  RGWBucketSyncStatusManager(RGWRados *_store, const string& _source_zone,
                             const string& _bucket_name, const string& _bucket_id) : store(_store),
                                                                                     cr_mgr(_store->ctx(), _store->get_cr_registry()),
                                                                                     async_rados(NULL),
                                                                                     http_manager(store->ctx(), cr_mgr.get_completion_mgr()),
                                                                                     source_zone(_source_zone),
                                                                                     conn(NULL), error_logger(NULL),
                                                                                     bucket_name(_bucket_name), bucket_id(_bucket_id),
                                                                                     num_shards(0) {}
  ~RGWBucketSyncStatusManager();

  int init();

  map<int, rgw_bucket_shard_sync_info>& get_sync_status() { return sync_status; }
  int init_sync_status();

  static string status_oid(const string& source_zone, const string& bucket_name, const string& bucket_id, int shard_id);

  int read_sync_status();
  int run();
};


#endif
