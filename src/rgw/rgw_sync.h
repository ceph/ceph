#ifndef CEPH_RGW_SYNC_H
#define CEPH_RGW_SYNC_H

#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_metadata.h"
#include "rgw_http_client.h"

#include "common/RWLock.h"


#define dout_subsys ceph_subsys_rgw


struct rgw_mdlog_info {
  uint32_t num_shards;

  rgw_mdlog_info() : num_shards(0) {}

  void decode_json(JSONObj *obj);
};

#define RGW_ASYNC_OPS_MGR_WINDOW 16

class RGWAsyncOp {
protected:
  RGWCompletionManager *completion_mgr;

  bool blocked;

  stringstream error_stream;

  void set_blocked(int flag) { blocked = flag; }

public:
  RGWAsyncOp(RGWCompletionManager *_completion_mgr) : completion_mgr(_completion_mgr), blocked(false) {}
  virtual ~RGWAsyncOp() {}

  virtual int operate() = 0;

  virtual bool is_done() = 0;
  virtual bool is_error() = 0;

  stringstream& log_error() { return error_stream; }
  string error_str() {
    return error_stream.str();
  }

  bool is_blocked() { return blocked; }
};

class RGWAsyncOpsManager {
  CephContext *cct;

protected:
  RGWCompletionManager completion_mgr;

  int ops_window;

public:
  RGWAsyncOpsManager(CephContext *_cct) : cct(_cct), ops_window(RGW_ASYNC_OPS_MGR_WINDOW) {}
  virtual ~RGWAsyncOpsManager() {}

  int run(list<RGWAsyncOp *>& ops);
  virtual void report_error(RGWAsyncOp *op);
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

public:
  RGWRemoteMetaLog(RGWRados *_store) : RGWAsyncOpsManager(_store->ctx()), store(_store),
                                       conn(NULL), ts_to_shard_lock("ts_to_shard_lock"),
                                       http_manager(store->ctx(), &completion_mgr) {}

  int init();

  int list_shard(int shard_id);
  int list_shards();
  int get_shard_info(int shard_id);
  int clone_shard(int shard_id, const string& marker, string *new_marker, bool *truncated);
  int clone_shards();
};

class RGWMetadataSync {
  RGWRados *store;

  RGWRemoteMetaLog master_log;
public:
  RGWMetadataSync(RGWRados *_store) : store(_store), master_log(store) {}

  int init();

  int clone_shards() { return master_log.clone_shards(); }
};

#endif
