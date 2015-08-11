#ifndef CEPH_RGW_SYNC_H
#define CEPH_RGW_SYNC_H

#include "rgw_coroutine.h"

#include "common/RWLock.h"


struct rgw_mdlog_info {
  uint32_t num_shards;

  rgw_mdlog_info() : num_shards(0) {}

  void decode_json(JSONObj *obj);
};

struct RGWMetaSyncGlobalStatus {
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

  RGWMetaSyncGlobalStatus() : state((int)StateInit), num_shards(0) {}
};
WRITE_CLASS_ENCODER(RGWMetaSyncGlobalStatus)

struct rgw_sync_marker {
  uint16_t state;
  string marker;

  rgw_sync_marker() : state(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(state, bl);
    ::encode(marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
     DECODE_START(1, bl);
    ::decode(state, bl);
    ::decode(marker, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const {
    encode_json("state", (int)state, f);
    encode_json("marker", marker, f);
  }
};
WRITE_CLASS_ENCODER(rgw_sync_marker)


class RGWMetaSyncStatusManager {
  RGWRados *store;
  librados::IoCtx ioctx;

  string global_status_oid;
  string shard_status_oid_prefix;
  rgw_obj global_status_obj;

  RGWMetaSyncGlobalStatus global_status;
  map<int, rgw_sync_marker> shard_markers;
  map<int, rgw_obj> shard_objs;

  string shard_obj_name(int shard_id);

  int num_shards;

public:
  RGWMetaSyncStatusManager(RGWRados *_store) : store(_store), num_shards(0) {}

  int init(int _num_shards);
  int read_global_status();
  int read_shard_status(int shard_id);
  rgw_sync_marker& get_shard_status(int shard_id) {
    return shard_markers[shard_id];
  }
  int set_state(RGWMetaSyncGlobalStatus::SyncState state);

  RGWMetaSyncGlobalStatus& get_global_status() { return global_status; } 
};

class RGWAsyncRadosProcessor;

class RGWRemoteMetaLog : public RGWCoroutinesManager {
  RGWRados *store;
  RGWRESTConn *conn;
  RGWAsyncRadosProcessor *async_rados;

  rgw_mdlog_info log_info;

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

  RGWHTTPManager http_manager;
  RGWMetaSyncStatusManager status_manager;

public:
  RGWRemoteMetaLog(RGWRados *_store) : RGWCoroutinesManager(_store->ctx()), store(_store),
                                       conn(NULL), ts_to_shard_lock("ts_to_shard_lock"),
                                       http_manager(store->ctx(), &completion_mgr),
                                       status_manager(store) {}

  int init();
  void finish();

  int list_shard(int shard_id);
  int list_shards();
  int get_shard_info(int shard_id);
  int clone_shards();
  int fetch();
  int get_sync_status(RGWMetaSyncGlobalStatus *sync_status);
  int init_sync_status();
  int get_shard_sync_marker(int shard_id, rgw_sync_marker *shard_status);
};

class RGWMetadataSync {
  RGWRados *store;

  RGWRemoteMetaLog master_log;
public:
  RGWMetadataSync(RGWRados *_store) : store(_store), master_log(store) {}
  ~RGWMetadataSync() {
    master_log.finish();
  }

  int init();

  int get_sync_status(RGWMetaSyncGlobalStatus *sync_status) { return master_log.get_sync_status(sync_status); }
  int init_sync_status() { return master_log.init_sync_status(); }
  int get_shard_sync_marker(int shard_id, rgw_sync_marker *shard_status) {
    return master_log.get_shard_sync_marker(shard_id, shard_status);
  }
  int fetch() { return master_log.fetch(); }
  int clone_shards() { return master_log.clone_shards(); }
};

#endif
