#ifndef CEPH_RGW_SYNC_H
#define CEPH_RGW_SYNC_H

#include "rgw_coroutine.h"

#include "common/RWLock.h"


struct rgw_mdlog_info {
  uint32_t num_shards;

  rgw_mdlog_info() : num_shards(0) {}

  void decode_json(JSONObj *obj);
};

struct rgw_meta_sync_info {
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

  rgw_meta_sync_info() : state((int)StateInit), num_shards(0) {}
};
WRITE_CLASS_ENCODER(rgw_meta_sync_info)

struct rgw_meta_sync_marker {
  enum SyncState {
    FullSync = 0,
    IncrementalSync = 1,
  };
  uint16_t state;
  string marker;
  string next_step_marker;

  rgw_meta_sync_marker() : state(FullSync) {}

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
WRITE_CLASS_ENCODER(rgw_meta_sync_marker)

struct rgw_meta_sync_status {
  rgw_meta_sync_info sync_info;
  map<uint32_t, rgw_meta_sync_marker> sync_markers;

  rgw_meta_sync_status() {}

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
WRITE_CLASS_ENCODER(rgw_meta_sync_status)

class RGWAsyncRadosProcessor;
class RGWMetaSyncStatusManager;
class RGWMetaSyncCR;

class RGWRemoteMetaLog : public RGWCoroutinesManager {
  RGWRados *store;
  RGWRESTConn *conn;
  RGWAsyncRadosProcessor *async_rados;

  RGWHTTPManager http_manager;
  RGWMetaSyncStatusManager *status_manager;

  RGWMetaSyncCR *meta_sync_cr;

public:
  RGWRemoteMetaLog(RGWRados *_store, RGWMetaSyncStatusManager *_sm) : RGWCoroutinesManager(_store->ctx()), store(_store),
                                       conn(NULL),
                                       http_manager(store->ctx(), &completion_mgr),
                                       status_manager(_sm), meta_sync_cr(NULL) {}

  int init();
  void finish();

  int read_log_info(rgw_mdlog_info *log_info);
  int list_shard(int shard_id);
  int list_shards(int num_shards);
  int get_shard_info(int shard_id);
  int clone_shards(int num_shards, vector<string>& clone_markers);
  int fetch(int num_shards, vector<string>& clone_markers);
  int read_sync_status(rgw_meta_sync_status *sync_status);
  int init_sync_status(int num_shards);
  int set_sync_info(const rgw_meta_sync_info& sync_info);
  int run_sync(int num_shards, rgw_meta_sync_status& sync_status);

  void wakeup(int shard_id);
};

class RGWMetaSyncStatusManager {
  RGWRados *store;
  librados::IoCtx ioctx;

  RGWRemoteMetaLog master_log;

  string global_status_oid;
  string shard_status_oid_prefix;
  rgw_obj global_status_obj;

  rgw_meta_sync_status sync_status;
  map<int, rgw_obj> shard_objs;

  int num_shards;

  struct utime_shard {
    utime_t ts;
    int shard_id;

    utime_shard() : shard_id(-1) {}

    bool operator<(const utime_shard& rhs) const {
      if (ts == rhs.ts) {
	return shard_id < rhs.shard_id;
      }
      return ts < rhs.ts;
    }
  };

  RWLock ts_to_shard_lock;
  map<utime_shard, int> ts_to_shard;
  vector<string> clone_markers;

public:
  RGWMetaSyncStatusManager(RGWRados *_store) : store(_store), master_log(store, this), num_shards(0),
                                               ts_to_shard_lock("ts_to_shard_lock") {}
  int init();

  rgw_meta_sync_status& get_sync_status() { return sync_status; }

  static string shard_obj_name(int shard_id);

  int read_sync_status() { return master_log.read_sync_status(&sync_status); }
  int init_sync_status() { return master_log.init_sync_status(num_shards); }
  int fetch() { return master_log.fetch(num_shards, clone_markers); }
  int clone_shards() { return master_log.clone_shards(num_shards, clone_markers); }

  int run() { return master_log.run_sync(num_shards, sync_status); }

  void wakeup(int shard_id) { return master_log.wakeup(shard_id); }
  void stop() {
    master_log.stop();
  }
};

#endif
