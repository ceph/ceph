#ifndef CEPH_RGW_SYNC_H
#define CEPH_RGW_SYNC_H

#include "rgw_coroutine.h"
#include "rgw_http_client.h"
#include "rgw_meta_sync_status.h"

#include "common/RWLock.h"

struct rgw_mdlog_info {
  uint32_t num_shards;

  rgw_mdlog_info() : num_shards(0) {}

  void decode_json(JSONObj *obj);
};


class RGWAsyncRadosProcessor;
class RGWMetaSyncStatusManager;
class RGWMetaSyncCR;
class RGWRESTConn;

class RGWReportContainer : public RefCountedObject {
  CephContext *cct;
  RGWReportContainer *parent;

  string id;
  string operation;
  utime_t timestamp;
  string status;

  struct StatusHistoryItem {
    utime_t timestamp;
    string status;

    StatusHistoryItem() {}
    StatusHistoryItem(const utime_t& _ts, const string& _status) : timestamp(_ts), status(_status) {}

    void dump(Formatter *f) const;
  };
  deque<StatusHistoryItem> status_history;

  int max_previous;

  map<string, RGWReportContainer *> actions;

  RWLock lock;

  void _finish_action(const string& id) {
    auto i = actions.find(id);
    if (i != actions.end()) {
      i->second->put();
    }
  }

public:
  RGWReportContainer(CephContext *_cct) : cct(_cct), parent(NULL), lock("RGWStatsuContainer::lock") {}
  RGWReportContainer(CephContext *_cct, RGWReportContainer *_parent, const string& _id, const string& op) : cct(_cct), parent(_parent), id(_id), operation("op"), lock("RGWReportContainer::lock") {}

  void set_status(const string& s);

  RGWReportContainer *new_action(const string& id, const string& op) {
    RWLock::WLocker l(lock);
    _finish_action(id);
    RGWReportContainer *new_status = new RGWReportContainer(cct, this, id, op);
    new_status->get();
    actions[id] = new_status;
    return new_status;
  }

  RGWReportContainer *new_action(RGWReportContainer *new_status) {
    RWLock::WLocker l(lock);
    _finish_action(id);
    new_status->get();
    actions[id] = new_status;
    return new_status;
  }

  void finish_action(const string& action_id) {
    RWLock::WLocker l(lock);
    _finish_action(action_id);
  }

  void finish() {
    if (parent) {
      parent->finish_action(id);
    }
    RWLock::WLocker l(lock);
    put();
  }

  virtual void dump(Formatter *f) const;
};

class RGWMetaSyncReport {
  string source;

  RGWReportContainer report;

  RWLock lock;

public:
  RGWMetaSyncReport(CephContext *cct) : report(cct), lock("RGWMetaSycnReport::lock") {}

  void set_source(const string& s) {
    RWLock::WLocker l(lock);
    source = s;
  }

  RGWReportContainer& get_container() { return report; }

  void dump(Formatter *f) const;
};

#define DEFAULT_BACKOFF_MAX 30

class RGWSyncBackoff {
  int cur_wait;
  int max_secs;

  void update_wait_time();
public:
  RGWSyncBackoff(int _max_secs = DEFAULT_BACKOFF_MAX) : cur_wait(0), max_secs(_max_secs) {}

  void backoff_sleep();
  void reset() {
    cur_wait = 0;
  }

  void backoff(RGWCoroutine *op);
};

class RGWBackoffControlCR : public RGWCoroutine
{
  RGWCoroutine *cr;
  Mutex lock;

  RGWSyncBackoff backoff;
  bool reset_backoff;

protected:
  bool *backoff_ptr() {
    return &reset_backoff;
  }

  Mutex& cr_lock() {
    return lock;
  }

  RGWCoroutine *get_cr() {
    return cr;
  }

public:
  RGWBackoffControlCR(CephContext *_cct) : RGWCoroutine(_cct), cr(NULL), lock("RGWBackoffControlCR::lock"), reset_backoff(false) {
  }

  virtual ~RGWBackoffControlCR() {
    if (cr) {
      cr->put();
    }
  }

  virtual RGWCoroutine *alloc_cr() = 0;
  virtual RGWCoroutine *alloc_finisher_cr() { return NULL; }

  int operate();
};

struct RGWMetaSyncEnv {
  CephContext *cct;
  RGWRados *store;
  RGWRESTConn *conn;
  RGWAsyncRadosProcessor *async_rados;
  RGWHTTPManager *http_manager;

  RGWMetaSyncEnv() : cct(NULL), store(NULL), conn(NULL), async_rados(NULL), http_manager(NULL) {}

  void init(CephContext *_cct, RGWRados *_store, RGWRESTConn *_conn,
            RGWAsyncRadosProcessor *_async_rados, RGWHTTPManager *_http_manager);

  string shard_obj_name(int shard_id);
  string status_oid();
};


class RGWRemoteMetaLog : public RGWCoroutinesManager {
  RGWRados *store;
  RGWRESTConn *conn;
  RGWAsyncRadosProcessor *async_rados;

  RGWHTTPManager http_manager;
  RGWMetaSyncStatusManager *status_manager;

  RGWMetaSyncCR *meta_sync_cr;

  RGWSyncBackoff backoff;

  RGWMetaSyncEnv sync_env;

  RGWMetaSyncReport sync_report;

  void init_sync_env(RGWMetaSyncEnv *env);

public:
  RGWRemoteMetaLog(RGWRados *_store, RGWMetaSyncStatusManager *_sm) : RGWCoroutinesManager(_store->ctx()), store(_store),
                                       conn(NULL), async_rados(nullptr),
                                       http_manager(store->ctx(), &completion_mgr),
                                       status_manager(_sm), meta_sync_cr(NULL), sync_report(_store->ctx()) {}

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

  RGWMetaSyncEnv& get_sync_env() {
    return sync_env;
  }
};

class RGWMetaSyncStatusManager {
  RGWRados *store;
  librados::IoCtx ioctx;

  RGWRemoteMetaLog master_log;

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
  void finish();

  rgw_meta_sync_status& get_sync_status() { return sync_status; }

  int read_sync_status() { return master_log.read_sync_status(&sync_status); }
  int init_sync_status() { return master_log.init_sync_status(num_shards); }
  int fetch() { return master_log.fetch(num_shards, clone_markers); }
  int clone_shards() { return master_log.clone_shards(num_shards, clone_markers); }

  int run() { return master_log.run_sync(num_shards, sync_status); }

  void wakeup(int shard_id) { return master_log.wakeup(shard_id); }
  void stop() {
    master_log.finish();
  }
};

template <class T>
class RGWSyncShardMarkerTrack {
  struct marker_entry {
    uint64_t pos;
    utime_t timestamp;

    marker_entry() : pos(0) {}
    marker_entry(uint64_t _p, const utime_t& _ts) : pos(_p), timestamp(_ts) {}
  };
  typename std::map<T, marker_entry> pending;

  T high_marker;
  marker_entry high_entry;

  int window_size;
  int updates_since_flush;


protected:
  virtual RGWCoroutine *store_marker(const T& new_marker, uint64_t index_pos, const utime_t& timestamp) = 0;
  virtual void handle_finish(const T& marker) { }

public:
  RGWSyncShardMarkerTrack(int _window_size) : window_size(_window_size), updates_since_flush(0) {}
  virtual ~RGWSyncShardMarkerTrack() {}

  bool start(const T& pos, int index_pos, const utime_t& timestamp) {
    if (pending.find(pos) != pending.end()) {
      return false;
    }
    pending[pos] = marker_entry(index_pos, timestamp);
    return true;
  }

  RGWCoroutine *finish(const T& pos) {
    if (pending.empty()) {
      /* can happen, due to a bug that ended up with multiple objects with the same name and version
       * -- which can happen when versioning is enabled an the version is 'null'.
       */
      return NULL;
    }

    typename std::map<T, marker_entry>::iterator iter = pending.begin();
    const T& first_pos = iter->first;

    typename std::map<T, marker_entry>::iterator pos_iter = pending.find(pos);
    if (pos_iter == pending.end()) {
      /* see pending.empty() comment */
      return NULL;
    }

    if (!(pos <= high_marker)) {
      high_marker = pos;
      high_entry = pos_iter->second;
    }

    pending.erase(pos);

    handle_finish(pos);

    updates_since_flush++;

    if (pos == first_pos && (updates_since_flush >= window_size || pending.empty())) {
      return update_marker(high_marker, high_entry);
    }
    return NULL;
  }

  RGWCoroutine *update_marker(const T& new_marker, marker_entry& entry) {
    updates_since_flush = 0;
    return store_marker(new_marker, entry.pos, entry.timestamp);
  }
};

class RGWMetaSyncShardMarkerTrack;

class RGWMetaSyncSingleEntryCR : public RGWCoroutine {
  RGWMetaSyncEnv *sync_env;

  string raw_key;
  string entry_marker;

  ssize_t pos;
  string section;
  string key;

  int sync_status;

  bufferlist md_bl;

  RGWMetaSyncShardMarkerTrack *marker_tracker;

  int tries;

public:
  RGWMetaSyncSingleEntryCR(RGWMetaSyncEnv *_sync_env,
		           const string& _raw_key, const string& _entry_marker, RGWMetaSyncShardMarkerTrack *_marker_tracker) : RGWCoroutine(_sync_env->cct),
                                                      sync_env(_sync_env),
						      raw_key(_raw_key), entry_marker(_entry_marker),
                                                      pos(0), sync_status(0),
                                                      marker_tracker(_marker_tracker), tries(0) {
  }

  int operate();
};



#endif
