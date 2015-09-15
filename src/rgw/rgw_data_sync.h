#ifndef CEPH_RGW_DATA_SYNC_H
#define CEPH_RGW_DATA_SYNC_H

#include "rgw_coroutine.h"
#include "rgw_http_client.h"

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

  rgw_data_sync_marker() : state(FullSync) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(state, bl);
    ::encode(marker, bl);
    ::encode(next_step_marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
     DECODE_START(1, bl);
    ::decode(state, bl);
    ::decode(marker, bl);
    ::decode(next_step_marker, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const {
    encode_json("state", (int)state, f);
    encode_json("marker", marker, f);
    encode_json("next_step_marker", next_step_marker, f);
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
    ::encode(sync_markers, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
     DECODE_START(1, bl);
    ::decode(sync_info, bl);
    ::decode(sync_markers, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const {
    encode_json("info", sync_info, f);
    encode_json("markers", sync_markers, f);
  }
};
WRITE_CLASS_ENCODER(rgw_data_sync_status)

class RGWAsyncRadosProcessor;
class RGWDataSyncStatusManager;
class RGWDataSyncCR;

class RGWRemoteDataLog : public RGWCoroutinesManager {
  RGWRados *store;
  RGWRESTConn *conn;
  RGWAsyncRadosProcessor *async_rados;

  RGWHTTPManager http_manager;
  RGWDataSyncStatusManager *status_manager;

  RGWDataSyncCR *data_sync_cr;

public:
  RGWRemoteDataLog(RGWRados *_store, RGWDataSyncStatusManager *_sm) : RGWCoroutinesManager(_store->ctx()), store(_store),
                                       conn(NULL),
                                       http_manager(store->ctx(), &completion_mgr),
                                       status_manager(_sm), data_sync_cr(NULL) {}

  int init(RGWRESTConn *_conn);
  void finish();

  int read_log_info(rgw_datalog_info *log_info);
  int list_shard(int shard_id);
  int list_shards(int num_shards);
  int get_shard_info(int shard_id);
  int read_sync_status(rgw_data_sync_status *sync_status);
  int init_sync_status(int num_shards);
  int set_sync_info(const rgw_data_sync_info& sync_info);
  int run_sync(int num_shards, rgw_data_sync_status& sync_status);

  void wakeup(int shard_id);
};

class RGWDataSyncStatusManager {
  RGWRados *store;
  librados::IoCtx ioctx;

  string source_zone;
  RGWRESTConn *conn;

  RGWRemoteDataLog source_log;

  string source_status_oid;
  string source_shard_status_oid_prefix;
  rgw_obj source_status_obj;

  rgw_data_sync_status sync_status;
  map<int, rgw_obj> shard_objs;

  int num_shards;

public:
  RGWDataSyncStatusManager(RGWRados *_store, const string& _source_zone) : store(_store), source_zone(_source_zone), conn(NULL),
                                                                           source_log(store, this), num_shards(0) {}
  int init();

  rgw_data_sync_status& get_sync_status() { return sync_status; }

  static string shard_obj_name(const string& source_zone, int shard_id);

  int read_sync_status() { return source_log.read_sync_status(&sync_status); }
  int init_sync_status() { return source_log.init_sync_status(num_shards); }

  int run() { return source_log.run_sync(num_shards, sync_status); }

  void wakeup(int shard_id) { return source_log.wakeup(shard_id); }
  void stop() {
    source_log.finish();
  }
};

#endif
