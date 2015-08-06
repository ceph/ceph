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

class RGWCoroutinesStack;
class RGWCoroutinesManager;
class AioCompletionNotifier;

struct RGWCoroutinesEnv {
  RGWCoroutinesManager *manager;
  list<RGWCoroutinesStack *> *stacks;
  RGWCoroutinesStack *stack;

  RGWCoroutinesEnv() : manager(NULL), stacks(NULL), stack(NULL) {}
};

class RGWCoroutine : public RefCountedObject {
  friend class RGWCoroutinesStack;
protected:
  RGWCoroutinesEnv *env;
  bool blocked;
  int retcode;

  stringstream error_stream;

  void set_blocked(bool flag) { blocked = flag; }
  int yield(int ret) {
    set_blocked(true);
    return ret;
  }

  int do_operate(RGWCoroutinesEnv *_env) {
    env = _env;
    return operate();
  }

  void call(RGWCoroutine *op);
  void spawn(RGWCoroutine *op);

public:
  RGWCoroutine() : env(NULL), blocked(false), retcode(0) {}
  virtual ~RGWCoroutine() {}

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

class RGWCoroutinesStack {
  CephContext *cct;

  RGWCoroutinesManager *ops_mgr;

  list<RGWCoroutine *> ops;
  list<RGWCoroutine *>::iterator pos;

  set<RGWCoroutinesStack *> blocked_by_stack;
  set<RGWCoroutinesStack *> blocking_stacks;


  bool done_flag;
  bool error_flag;
  bool blocked_flag;

public:
  RGWCoroutinesStack(CephContext *_cct, RGWCoroutinesManager *_ops_mgr, RGWCoroutine *start = NULL);

  int operate(RGWCoroutinesEnv *env);

  bool is_done() {
    return done_flag;
  }
  bool is_error() {
    return error_flag;
  }
  bool is_blocked_by_stack() {
    return !blocked_by_stack.empty();
  }
  bool is_blocked() {
    return blocked_flag || is_blocked_by_stack();
  }

  void set_blocked(bool flag);

  string error_str();

  int call(RGWCoroutine *next_op, int ret = 0);
  int unwind(int retcode);

  AioCompletionNotifier *create_completion_notifier();
  RGWCompletionManager *get_completion_mgr();

  void set_blocked_by(RGWCoroutinesStack *s) {
    blocked_by_stack.insert(s);
    s->blocking_stacks.insert(this);
  }

  bool unblock_stack(RGWCoroutinesStack **s);
};

class RGWCoroutinesManager {
  CephContext *cct;

  void handle_unblocked_stack(list<RGWCoroutinesStack *>& stacks, RGWCoroutinesStack *stack, int *waiting_count);
protected:
  RGWCompletionManager completion_mgr;

  int ops_window;

  void put_completion_notifier(AioCompletionNotifier *cn);
public:
  RGWCoroutinesManager(CephContext *_cct) : cct(_cct), ops_window(RGW_ASYNC_OPS_MGR_WINDOW) {}
  virtual ~RGWCoroutinesManager() {}

  int run(list<RGWCoroutinesStack *>& ops);
  int run(RGWCoroutine *op);

  virtual void report_error(RGWCoroutinesStack *op);

  AioCompletionNotifier *create_completion_notifier(RGWCoroutinesStack *stack);
  RGWCompletionManager *get_completion_mgr() { return &completion_mgr; }

  RGWCoroutinesStack *allocate_stack() {
    return new RGWCoroutinesStack(cct, this);
  }
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
  int get_shard_sync_marker(int shard_id, rgw_sync_marker *shard_status) {
    return master_log.get_shard_sync_marker(shard_id, shard_status);
  }
  int fetch() { return master_log.fetch(); }
  int clone_shards() { return master_log.clone_shards(); }
};

#endif
