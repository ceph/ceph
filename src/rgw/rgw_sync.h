// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_SYNC_H
#define CEPH_RGW_SYNC_H

#include <atomic>

#include "include/stringify.h"
#include "common/RWLock.h"

#include "rgw_coroutine.h"
#include "rgw_http_client.h"
#include "rgw_metadata.h"
#include "rgw_meta_sync_status.h"
#include "rgw_rados.h"
#include "rgw_sync_trace.h"


#define ERROR_LOGGER_SHARDS 32
#define RGW_SYNC_ERROR_LOG_SHARD_PREFIX "sync.error-log"

struct rgw_mdlog_info {
  uint32_t num_shards;
  std::string period; //< period id of the master's oldest metadata log
  epoch_t realm_epoch; //< realm epoch of oldest metadata log

  rgw_mdlog_info() : num_shards(0), realm_epoch(0) {}

  void decode_json(JSONObj *obj);
};


struct rgw_mdlog_entry {
  string id;
  string section;
  string name;
  ceph::real_time timestamp;
  RGWMetadataLogData log_data;

  void decode_json(JSONObj *obj);

  bool convert_from(cls_log_entry& le) {
    id = le.id;
    section = le.section;
    name = le.name;
    timestamp = le.timestamp.to_real_time();
    try {
      auto iter = le.data.cbegin();
      decode(log_data, iter);
    } catch (buffer::error& err) {
      return false;
    }
    return true;
  }
};

struct rgw_mdlog_shard_data {
  string marker;
  bool truncated;
  vector<rgw_mdlog_entry> entries;

  void decode_json(JSONObj *obj);
};

class RGWAsyncRadosProcessor;
class RGWMetaSyncStatusManager;
class RGWMetaSyncCR;
class RGWRESTConn;
class RGWSyncTraceManager;

class RGWSyncErrorLogger {
  RGWRados *store;

  vector<string> oids;
  int num_shards;

  std::atomic<int64_t> counter = { 0 };
public:
  RGWSyncErrorLogger(RGWRados *_store, const string &oid_prefix, int _num_shards);
  RGWCoroutine *log_error_cr(const string& source_zone, const string& section, const string& name, uint32_t error_code, const string& message);

  static string get_shard_oid(const string& oid_prefix, int shard_id);
};

struct rgw_sync_error_info {
  string source_zone;
  uint32_t error_code;
  string message;

  rgw_sync_error_info() : error_code(0) {}
  rgw_sync_error_info(const string& _source_zone, uint32_t _error_code, const string& _message) : source_zone(_source_zone), error_code(_error_code), message(_message) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(source_zone, bl);
    encode(error_code, bl);
    encode(message, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(source_zone, bl);
    decode(error_code, bl);
    decode(message, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_sync_error_info)

#define DEFAULT_BACKOFF_MAX 30

class RGWSyncBackoff {
  int cur_wait;
  int max_secs;

  void update_wait_time();
public:
  explicit RGWSyncBackoff(int _max_secs = DEFAULT_BACKOFF_MAX) : cur_wait(0), max_secs(_max_secs) {}

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

  bool exit_on_error;

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
  RGWBackoffControlCR(CephContext *_cct, bool _exit_on_error) : RGWCoroutine(_cct), cr(NULL), lock("RGWBackoffControlCR::lock:" + stringify(this)),
                                                                reset_backoff(false), exit_on_error(_exit_on_error) {
  }

  ~RGWBackoffControlCR() override {
    if (cr) {
      cr->put();
    }
  }

  virtual RGWCoroutine *alloc_cr() = 0;
  virtual RGWCoroutine *alloc_finisher_cr() { return NULL; }

  int operate() override;
};

struct RGWMetaSyncEnv {
  const DoutPrefixProvider *dpp;
  CephContext *cct{nullptr};
  RGWRados *store{nullptr};
  RGWRESTConn *conn{nullptr};
  RGWAsyncRadosProcessor *async_rados{nullptr};
  RGWHTTPManager *http_manager{nullptr};
  RGWSyncErrorLogger *error_logger{nullptr};
  RGWSyncTraceManager *sync_tracer{nullptr};

  RGWMetaSyncEnv() {}

  void init(const DoutPrefixProvider *_dpp, CephContext *_cct, RGWRados *_store, RGWRESTConn *_conn,
            RGWAsyncRadosProcessor *_async_rados, RGWHTTPManager *_http_manager,
            RGWSyncErrorLogger *_error_logger, RGWSyncTraceManager *_sync_tracer);

  string shard_obj_name(int shard_id);
  string status_oid();
};

class RGWRemoteMetaLog : public RGWCoroutinesManager {
  const DoutPrefixProvider *dpp;
  RGWRados *store;
  RGWRESTConn *conn;
  RGWAsyncRadosProcessor *async_rados;

  RGWHTTPManager http_manager;
  RGWMetaSyncStatusManager *status_manager;
  RGWSyncErrorLogger *error_logger{nullptr};
  RGWSyncTraceManager *sync_tracer{nullptr};

  RGWMetaSyncCR *meta_sync_cr{nullptr};

  RGWSyncBackoff backoff;

  RGWMetaSyncEnv sync_env;

  void init_sync_env(RGWMetaSyncEnv *env);
  int store_sync_info(const rgw_meta_sync_info& sync_info);

  std::atomic<bool> going_down = { false };

  RGWSyncTraceNodeRef tn;

public:
  RGWRemoteMetaLog(const DoutPrefixProvider *dpp, RGWRados *_store,
                   RGWAsyncRadosProcessor *async_rados,
                   RGWMetaSyncStatusManager *_sm)
    : RGWCoroutinesManager(_store->ctx(), _store->get_cr_registry()),
      dpp(dpp), store(_store), conn(NULL), async_rados(async_rados),
      http_manager(store->ctx(), completion_mgr),
      status_manager(_sm) {}

  ~RGWRemoteMetaLog() override;

  int init();
  void finish();

  int read_log_info(rgw_mdlog_info *log_info);
  int read_master_log_shards_info(const string& master_period, map<int, RGWMetadataLogInfo> *shards_info);
  int read_master_log_shards_next(const string& period, map<int, string> shard_markers, map<int, rgw_mdlog_shard_data> *result);
  int read_sync_status(rgw_meta_sync_status *sync_status);
  int init_sync_status();
  int run_sync();

  void wakeup(int shard_id);

  RGWMetaSyncEnv& get_sync_env() {
    return sync_env;
  }
};

class RGWMetaSyncStatusManager : public DoutPrefixProvider {
  RGWRados *store;
  librados::IoCtx ioctx;

  RGWRemoteMetaLog master_log;

  map<int, rgw_raw_obj> shard_objs;

  struct utime_shard {
    real_time ts;
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
  RGWMetaSyncStatusManager(RGWRados *_store, RGWAsyncRadosProcessor *async_rados)
    : store(_store), master_log(this, store, async_rados, this),
      ts_to_shard_lock("ts_to_shard_lock") {}
  int init();

  int read_sync_status(rgw_meta_sync_status *sync_status) {
    return master_log.read_sync_status(sync_status);
  }
  int init_sync_status() { return master_log.init_sync_status(); }
  int read_log_info(rgw_mdlog_info *log_info) {
    return master_log.read_log_info(log_info);
  }
  int read_master_log_shards_info(const string& master_period, map<int, RGWMetadataLogInfo> *shards_info) {
    return master_log.read_master_log_shards_info(master_period, shards_info);
  }
  int read_master_log_shards_next(const string& period, map<int, string> shard_markers, map<int, rgw_mdlog_shard_data> *result) {
    return master_log.read_master_log_shards_next(period, shard_markers, result);
  }

  int run() { return master_log.run_sync(); }


  // implements DoutPrefixProvider
  CephContext *get_cct() const override { return store->ctx(); }
  unsigned get_subsys() const override;
  std::ostream& gen_prefix(std::ostream& out) const override;

  void wakeup(int shard_id) { return master_log.wakeup(shard_id); }
  void stop() {
    master_log.finish();
  }
};

class RGWOrderCallCR : public RGWCoroutine
{
public:
  RGWOrderCallCR(CephContext *cct) : RGWCoroutine(cct) {}

  virtual void call_cr(RGWCoroutine *_cr) = 0;
};

class RGWLastCallerWinsCR : public RGWOrderCallCR
{
  RGWCoroutine *cr{nullptr};

public:
  explicit RGWLastCallerWinsCR(CephContext *cct) : RGWOrderCallCR(cct) {}
  ~RGWLastCallerWinsCR() {
    if (cr) {
      cr->put();
    }
  }

  int operate() override;

  void call_cr(RGWCoroutine *_cr) override {
    if (cr) {
      cr->put();
    }
    cr = _cr;
  }
};

template <class T, class K>
class RGWSyncShardMarkerTrack {
  struct marker_entry {
    uint64_t pos;
    real_time timestamp;

    marker_entry() : pos(0) {}
    marker_entry(uint64_t _p, const real_time& _ts) : pos(_p), timestamp(_ts) {}
  };
  typename std::map<T, marker_entry> pending;

  map<T, marker_entry> finish_markers;

  int window_size;
  int updates_since_flush;

  RGWOrderCallCR *order_cr{nullptr};

protected:
  typename std::set<K> need_retry_set;

  virtual RGWCoroutine *store_marker(const T& new_marker, uint64_t index_pos, const real_time& timestamp) = 0;
  virtual RGWOrderCallCR *allocate_order_control_cr() = 0;
  virtual void handle_finish(const T& marker) { }

public:
  RGWSyncShardMarkerTrack(int _window_size) : window_size(_window_size), updates_since_flush(0) {}
  virtual ~RGWSyncShardMarkerTrack() {
    if (order_cr) {
      order_cr->put();
    }
  }

  bool start(const T& pos, int index_pos, const real_time& timestamp) {
    if (pending.find(pos) != pending.end()) {
      return false;
    }
    pending[pos] = marker_entry(index_pos, timestamp);
    return true;
  }

  void try_update_high_marker(const T& pos, int index_pos, const real_time& timestamp) {
    finish_markers[pos] = marker_entry(index_pos, timestamp);
  }

  RGWCoroutine *finish(const T& pos) {
    if (pending.empty()) {
      /* can happen, due to a bug that ended up with multiple objects with the same name and version
       * -- which can happen when versioning is enabled an the version is 'null'.
       */
      return NULL;
    }

    typename std::map<T, marker_entry>::iterator iter = pending.begin();

    bool is_first = (pos == iter->first);

    typename std::map<T, marker_entry>::iterator pos_iter = pending.find(pos);
    if (pos_iter == pending.end()) {
      /* see pending.empty() comment */
      return NULL;
    }

    finish_markers[pos] = pos_iter->second;

    pending.erase(pos);

    handle_finish(pos);

    updates_since_flush++;

    if (is_first && (updates_since_flush >= window_size || pending.empty())) {
      return flush();
    }
    return NULL;
  }

  RGWCoroutine *flush() {
    if (finish_markers.empty()) {
      return NULL;
    }

    typename std::map<T, marker_entry>::iterator i;

    if (pending.empty()) {
      i = finish_markers.end();
    } else {
      i = finish_markers.lower_bound(pending.begin()->first);
    }
    if (i == finish_markers.begin()) {
      return NULL;
    }
    updates_since_flush = 0;

    auto last = i;
    --i;
    const T& high_marker = i->first;
    marker_entry& high_entry = i->second;
    RGWCoroutine *cr = order(store_marker(high_marker, high_entry.pos, high_entry.timestamp));
    finish_markers.erase(finish_markers.begin(), last);
    return cr;
  }

  /*
   * a key needs retry if it was processing when another marker that points
   * to the same bucket shards arrives. Instead of processing it, we mark
   * it as need_retry so that when we finish processing the original, we
   * retry the processing on the same bucket shard, in case there are more
   * entries to process. This closes a race that can happen.
   */
  bool need_retry(const K& key) {
    return (need_retry_set.find(key) != need_retry_set.end());
  }

  void set_need_retry(const K& key) {
    need_retry_set.insert(key);
  }

  void reset_need_retry(const K& key) {
    need_retry_set.erase(key);
  }

  RGWCoroutine *order(RGWCoroutine *cr) {
    /* either returns a new RGWLastWriteWinsCR, or update existing one, in which case it returns
     * nothing and the existing one will call the cr
     */
    if (order_cr && order_cr->is_done()) {
      order_cr->put();
      order_cr = nullptr;
    }
    if (!order_cr) {
      order_cr = allocate_order_control_cr();
      order_cr->get();
      order_cr->call_cr(cr);
      return order_cr;
    }
    order_cr->call_cr(cr);
    return nullptr; /* don't call it a second time */
  }
};

class RGWMetaSyncShardMarkerTrack;

class RGWMetaSyncSingleEntryCR : public RGWCoroutine {
  RGWMetaSyncEnv *sync_env;

  string raw_key;
  string entry_marker;
  RGWMDLogStatus op_status;

  ssize_t pos;
  string section;
  string key;

  int sync_status;

  bufferlist md_bl;

  RGWMetaSyncShardMarkerTrack *marker_tracker;

  int tries;

  bool error_injection;

  RGWSyncTraceNodeRef tn;

public:
  RGWMetaSyncSingleEntryCR(RGWMetaSyncEnv *_sync_env,
                           const string& _raw_key, const string& _entry_marker,
                           const RGWMDLogStatus& _op_status,
                           RGWMetaSyncShardMarkerTrack *_marker_tracker, const RGWSyncTraceNodeRef& _tn_parent);

  int operate() override;
};

class RGWShardCollectCR : public RGWCoroutine {
  int cur_shard;
  int current_running;
  int max_concurrent;
  int status;

public:
  RGWShardCollectCR(CephContext *_cct, int _max_concurrent) : RGWCoroutine(_cct),
                                                             current_running(0),
                                                             max_concurrent(_max_concurrent),
                                                             status(0) {}

  virtual bool spawn_next() = 0;
  int operate() override;
};

// MetaLogTrimCR factory function
RGWCoroutine* create_meta_log_trim_cr(const DoutPrefixProvider *dpp, RGWRados *store, RGWHTTPManager *http,
                                      int num_shards, utime_t interval);

// factory function for mdlog trim via radosgw-admin
RGWCoroutine* create_admin_meta_log_trim_cr(const DoutPrefixProvider *dpp, RGWRados *store,
                                            RGWHTTPManager *http,
                                            int num_shards);

#endif
