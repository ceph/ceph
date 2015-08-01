#ifndef CEPH_RGW_SYNC_H
#define CEPH_RGW_SYNC_H

#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_metadata.h"
#include "rgw_http_client.h"

#include "common/RWLock.h"
#include "common/RefCountedObj.h"


#define dout_subsys ceph_subsys_rgw


struct rgw_mdlog_info {
  uint32_t num_shards;

  rgw_mdlog_info() : num_shards(0) {}

  void decode_json(JSONObj *obj);
};

#define RGW_ASYNC_OPS_MGR_WINDOW 16

class RGWAsyncOpsStack;
class RGWAsyncOpsManager;
class AioCompletionNotifier;

class RGWAsyncOp : public RefCountedObject {
  friend class RGWAsyncOpsStack;
protected:
  RGWAsyncOpsStack *ops_stack;

  bool blocked;
  int retcode;

  stringstream error_stream;

  void set_blocked(bool flag) { blocked = flag; }
  int yield(int ret) {
    set_blocked(true);
    return ret;
  }

public:
  RGWAsyncOp(RGWAsyncOpsStack *_ops_stack) : ops_stack(_ops_stack), blocked(false), retcode(0) {}
  virtual ~RGWAsyncOp() {}

  virtual int operate() = 0;

  virtual bool is_done() = 0;
  virtual bool is_error() = 0;

  stringstream& log_error() { return error_stream; }
  string error_str() {
    return error_stream.str();
  }

  bool is_blocked() { return blocked; }

  void set_retcode(int r) {
    retcode = r;
  }
};

class RGWAsyncOpsStack {
  CephContext *cct;

  RGWAsyncOpsManager *ops_mgr;

  list<RGWAsyncOp *> ops;
  list<RGWAsyncOp *>::iterator pos;

  bool done_flag;
  bool error_flag;
  bool blocked_flag;

public:
  RGWAsyncOpsStack(CephContext *_cct, RGWAsyncOpsManager *_ops_mgr, RGWAsyncOp *start = NULL);

  int operate();

  bool is_done() {
    return done_flag;
  }
  bool is_error() {
    return error_flag;
  }
  bool is_blocked() {
    return blocked_flag;
  }

  void set_blocked(bool flag);

  string error_str();

  int call(RGWAsyncOp *next_op, int ret = 0);
  int unwind(int retcode);

  AioCompletionNotifier *create_completion_notifier();
  RGWCompletionManager *get_completion_mgr();
};

class RGWAsyncOpsManager {
  CephContext *cct;

protected:
  RGWCompletionManager completion_mgr;

  int ops_window;

  void put_completion_notifier(AioCompletionNotifier *cn);
public:
  RGWAsyncOpsManager(CephContext *_cct) : cct(_cct), ops_window(RGW_ASYNC_OPS_MGR_WINDOW) {}
  virtual ~RGWAsyncOpsManager() {}

  int run(list<RGWAsyncOpsStack *>& ops);
  virtual void report_error(RGWAsyncOpsStack *op);

  AioCompletionNotifier *create_completion_notifier(RGWAsyncOpsStack *stack);
  RGWCompletionManager *get_completion_mgr() { return &completion_mgr; }
};

struct RGWMetaSyncGlobalStatus {
  enum SyncState {
    StateInit = 0,
    StateBuildingFullSyncMaps = 1,
    StateSync = 2,
  };

  uint16_t state;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(state, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
     DECODE_START(1, bl);
     ::decode(state, bl);
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
  }

  RGWMetaSyncGlobalStatus() : state((int)StateInit) {}
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
};
WRITE_CLASS_ENCODER(rgw_sync_marker)


class RGWMetaSyncStatusManager {
  RGWRados *store;
  librados::IoCtx ioctx;

  string global_status_oid;
  rgw_obj global_status_obj;

  RGWMetaSyncGlobalStatus global_status;
  map<int, rgw_sync_marker> shard_markers;

public:
  RGWMetaSyncStatusManager(RGWRados *_store) : store(_store) {}

  int init();
  int read_global_status();
  int set_state(RGWMetaSyncGlobalStatus::SyncState state);

  RGWMetaSyncGlobalStatus& get_global_status() { return global_status; } 
};

class RGWRemoteMetaLog : public RGWAsyncOpsManager {
  RGWRados *store;
  RGWRESTConn *conn;

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
  RGWRemoteMetaLog(RGWRados *_store) : RGWAsyncOpsManager(_store->ctx()), store(_store),
                                       conn(NULL), ts_to_shard_lock("ts_to_shard_lock"),
                                       http_manager(store->ctx(), &completion_mgr),
                                       status_manager(store) {}

  int init();

  int list_shard(int shard_id);
  int list_shards();
  int get_shard_info(int shard_id);
  int clone_shards();
  int fetch();
  int get_sync_status(RGWMetaSyncGlobalStatus *sync_status);
};

class RGWMetadataSync {
  RGWRados *store;

  RGWRemoteMetaLog master_log;
public:
  RGWMetadataSync(RGWRados *_store) : store(_store), master_log(store) {}

  int init();

  int get_sync_status(RGWMetaSyncGlobalStatus *sync_status) { return master_log.get_sync_status(sync_status); }
  int fetch() { return master_log.fetch(); }
  int clone_shards() { return master_log.clone_shards(); }
};

#endif
