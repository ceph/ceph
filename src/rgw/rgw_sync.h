#ifndef CEPH_RGW_SYNC_H
#define CEPH_RGW_SYNC_H

#include "rgw_coroutine.h"
#include "rgw_http_client.h"
#include "rgw_meta_sync_status.h"

#include "common/RWLock.h"

#define ERROR_LOGGER_SHARDS 32
#define RGW_SYNC_ERROR_LOG_SHARD_PREFIX "sync.error-log"

struct rgw_mdlog_info {
  uint32_t num_shards;
  std::string period; //< period id of the master's oldest metadata log
  epoch_t realm_epoch; //< realm epoch of oldest metadata log

  rgw_mdlog_info() : num_shards(0), realm_epoch(0) {}

  void decode_json(JSONObj *obj);
};


class RGWAsyncRadosProcessor;
class RGWMetaSyncStatusManager;
class RGWMetaSyncCR;
class RGWRESTConn;

class RGWSyncErrorLogger {
  RGWRados *store;

  vector<string> oids;
  int num_shards;

  atomic_t counter;
public:
  RGWSyncErrorLogger(RGWRados *_store, const string oid_prefix, int _num_shards);
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
    ::encode(source_zone, bl);
    ::encode(error_code, bl);
    ::encode(message, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(source_zone, bl);
    ::decode(error_code, bl);
    ::decode(message, bl);
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
  RGWSyncErrorLogger *error_logger;

  RGWMetaSyncEnv() : cct(NULL), store(NULL), conn(NULL), async_rados(NULL), http_manager(NULL), error_logger(NULL) {}

  void init(CephContext *_cct, RGWRados *_store, RGWRESTConn *_conn,
            RGWAsyncRadosProcessor *_async_rados, RGWHTTPManager *_http_manager,
            RGWSyncErrorLogger *_error_logger);

  string shard_obj_name(int shard_id);
  string status_oid();
};

class RGWRemoteMetaLog : public RGWCoroutinesManager {
  RGWRados *store;
  RGWRESTConn *conn;
  RGWAsyncRadosProcessor *async_rados;

  RGWHTTPManager http_manager;
  RGWMetaSyncStatusManager *status_manager;
  RGWSyncErrorLogger *error_logger;

  RGWMetaSyncCR *meta_sync_cr;

  RGWSyncBackoff backoff;

  RGWMetaSyncEnv sync_env;
  rgw_meta_sync_status sync_status;

  void init_sync_env(RGWMetaSyncEnv *env);
  int store_sync_info();

  atomic_t going_down;

public:
  RGWRemoteMetaLog(RGWRados *_store, RGWAsyncRadosProcessor *async_rados,
                   RGWMetaSyncStatusManager *_sm)
    : RGWCoroutinesManager(_store->ctx(), _store->get_cr_registry()),
      store(_store), conn(NULL), async_rados(async_rados),
      http_manager(store->ctx(), completion_mgr),
      status_manager(_sm), error_logger(NULL), meta_sync_cr(NULL) {}

  ~RGWRemoteMetaLog();

  int init();
  void finish();

  int read_log_info(rgw_mdlog_info *log_info);
  int read_sync_status();
  int init_sync_status();
  int run_sync();

  void wakeup(int shard_id);

  RGWMetaSyncEnv& get_sync_env() {
    return sync_env;
  }
  const rgw_meta_sync_status& get_sync_status() const { return sync_status; }
};

class RGWMetaSyncStatusManager {
  RGWRados *store;
  librados::IoCtx ioctx;

  RGWRemoteMetaLog master_log;

  map<int, rgw_obj> shard_objs;

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
  RGWMetaSyncStatusManager(RGWRados *_store, RGWAsyncRadosProcessor *async_rados)
    : store(_store), master_log(store, async_rados, this),
      ts_to_shard_lock("ts_to_shard_lock") {}
  int init();
  void finish();

  const rgw_meta_sync_status& get_sync_status() const {
    return master_log.get_sync_status();
  }

  int read_sync_status() { return master_log.read_sync_status(); }
  int init_sync_status() { return master_log.init_sync_status(); }

  int run() { return master_log.run_sync(); }

  void wakeup(int shard_id) { return master_log.wakeup(shard_id); }
  void stop() {
    master_log.finish();
  }
};

template <class T, class K>
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
  typename std::set<K> need_retry_set;

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

  void try_update_high_marker(const T& pos, int index_pos, const utime_t& timestamp) {
    if (!(pos <= high_marker)) {
      high_marker = pos;
      high_entry = marker_entry(index_pos, timestamp);
    }
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

    if (!(pos <= high_marker)) {
      high_marker = pos;
      high_entry = pos_iter->second;
    }

    pending.erase(pos);

    handle_finish(pos);

    updates_since_flush++;

    if (is_first && (updates_since_flush >= window_size || pending.empty())) {
      return update_marker(high_marker, high_entry);
    }
    return NULL;
  }

  RGWCoroutine *update_marker(const T& new_marker, marker_entry& entry) {
    updates_since_flush = 0;
    return store_marker(new_marker, entry.pos, entry.timestamp);
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

public:
  RGWMetaSyncSingleEntryCR(RGWMetaSyncEnv *_sync_env,
		           const string& _raw_key, const string& _entry_marker,
                           const RGWMDLogStatus& _op_status,
                           RGWMetaSyncShardMarkerTrack *_marker_tracker) : RGWCoroutine(_sync_env->cct),
                                                      sync_env(_sync_env),
						      raw_key(_raw_key), entry_marker(_entry_marker),
                                                      op_status(_op_status),
                                                      pos(0), sync_status(0),
                                                      marker_tracker(_marker_tracker), tries(0) {
  }

  int operate();
};



#endif
