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

#define RGW_META_LOG_OPS_WINDOW 16

class RGWRemoteMetaLog {
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

  RGWCompletionManager completion_mgr;
  RGWHTTPManager http_manager;

  int ops_window;

public:
  RGWRemoteMetaLog(RGWRados *_store) : store(_store), conn(NULL), ts_to_shard_lock("ts_to_shard_lock"),
                                       http_manager(store->ctx(), &completion_mgr),
                                       ops_window(RGW_META_LOG_OPS_WINDOW) {}

  int init();

  int list_shard(int shard_id);
  int list_shards();
  int get_shard_info(int shard_id);
  int clone_shard(int shard_id, const string& marker, string *new_marker, bool *truncated);
  int clone_shards();

  void report_error(RGWCloneMetaLogOp *op);
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
