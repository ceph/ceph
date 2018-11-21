// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_METADATA_H
#define CEPH_RGW_METADATA_H

#include <string>
#include <utility>
#include <boost/optional.hpp>

#include "include/types.h"
#include "rgw_common.h"
#include "rgw_period_history.h"
#include "cls/version/cls_version_types.h"
#include "cls/log/cls_log_types.h"
#include "common/RWLock.h"
#include "common/RefCountedObj.h"
#include "common/ceph_time.h"


class RGWRados;
class RGWCoroutine;
class JSONObj;
struct RGWObjVersionTracker;

struct obj_version;


enum RGWMDLogStatus {
  MDLOG_STATUS_UNKNOWN,
  MDLOG_STATUS_WRITE,
  MDLOG_STATUS_SETATTRS,
  MDLOG_STATUS_REMOVE,
  MDLOG_STATUS_COMPLETE,
  MDLOG_STATUS_ABORT,
};

class RGWMetadataObject {
protected:
  obj_version objv;
  ceph::real_time mtime;
  
public:
  RGWMetadataObject() {}
  virtual ~RGWMetadataObject() {}
  obj_version& get_version();
  real_time get_mtime() { return mtime; }

  virtual void dump(Formatter *f) const = 0;
};

class RGWMetadataManager;

class RGWMetadataHandler {
  friend class RGWMetadataManager;

public:
  enum sync_type_t {
    APPLY_ALWAYS,
    APPLY_UPDATES,
    APPLY_NEWER
  };
  static bool string_to_sync_type(const string& sync_string,
                                  sync_type_t& type) {
    if (sync_string.compare("update-by-version") == 0)
      type = APPLY_UPDATES;
    else if (sync_string.compare("update-by-timestamp") == 0)
      type = APPLY_NEWER;
    else if (sync_string.compare("always") == 0)
      type = APPLY_ALWAYS;
    else
      return false;
    return true;
  }

  virtual ~RGWMetadataHandler() {}
  virtual string get_type() = 0;

  virtual int get(RGWRados *store, string& entry, RGWMetadataObject **obj) = 0;
  virtual int put(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker,
                  real_time mtime, JSONObj *obj, sync_type_t type) = 0;
  virtual int remove(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker) = 0;

  virtual int list_keys_init(RGWRados *store, const string& marker, void **phandle) = 0;
  virtual int list_keys_next(void *handle, int max, list<string>& keys, bool *truncated) = 0;
  virtual void list_keys_complete(void *handle) = 0;

  virtual string get_marker(void *handle) = 0;

  /* key to use for hashing entries for log shard placement */
  virtual void get_hash_key(const string& section, const string& key, string& hash_key) {
    hash_key = section + ":" + key;
  }

protected:
  virtual void get_pool_and_oid(RGWRados *store, const string& key, rgw_pool& pool, string& oid) = 0;
  /**
   * Compare an incoming versus on-disk tag/version+mtime combo against
   * the sync mode to see if the new one should replace the on-disk one.
   *
   * @return true if the update should proceed, false otherwise.
   */
  static bool check_versions(const obj_version& ondisk, const real_time& ondisk_time,
                             const obj_version& incoming, const real_time& incoming_time,
                             sync_type_t sync_mode) {
    switch (sync_mode) {
    case APPLY_UPDATES:
      if ((ondisk.tag != incoming.tag) ||
	  (ondisk.ver >= incoming.ver))
	return false;
      break;
    case APPLY_NEWER:
      if (ondisk_time >= incoming_time)
	return false;
      break;
    case APPLY_ALWAYS: //deliberate fall-thru -- we always apply!
    default: break;
    }
    return true;
  }

  /*
   * The tenant_name is always returned on purpose. May be empty, of course.
   */
  static void parse_bucket(const string& bucket,
                           string *tenant_name,
                           string *bucket_name,
                           string *bucket_instance = nullptr /* optional */)
  {
    int pos = bucket.find('/');
    if (pos >= 0) {
      *tenant_name = bucket.substr(0, pos);
    } else {
      tenant_name->clear();
    }
    string bn = bucket.substr(pos + 1);
    pos = bn.find (':');
    if (pos < 0) {
      *bucket_name = std::move(bn);
      return;
    }
    *bucket_name = bn.substr(0, pos);
    if (bucket_instance) {
      *bucket_instance = bn.substr(pos + 1);
    }
  }
};

#define META_LOG_OBJ_PREFIX "meta.log."

struct RGWMetadataLogInfo {
  string marker;
  real_time last_update;

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};

class RGWCompletionManager;

class RGWMetadataLogInfoCompletion : public RefCountedObject {
 public:
  using info_callback_t = std::function<void(int, const cls_log_header&)>;
 private:
  cls_log_header header;
  librados::IoCtx io_ctx;
  librados::AioCompletion *completion;
  std::mutex mutex; //< protects callback between cancel/complete
  boost::optional<info_callback_t> callback; //< cleared on cancel
 public:
  explicit RGWMetadataLogInfoCompletion(info_callback_t callback);
  ~RGWMetadataLogInfoCompletion() override;

  librados::IoCtx& get_io_ctx() { return io_ctx; }
  cls_log_header& get_header() { return header; }
  librados::AioCompletion* get_completion() { return completion; }

  void finish(librados::completion_t cb) {
    std::lock_guard<std::mutex> lock(mutex);
    if (callback) {
      (*callback)(completion->get_return_value(), header);
    }
  }
  void cancel() {
    std::lock_guard<std::mutex> lock(mutex);
    callback = boost::none;
  }
};

class RGWMetadataLog {
  CephContext *cct;
  RGWRados *store;
  const string prefix;

  static std::string make_prefix(const std::string& period) {
    if (period.empty())
      return META_LOG_OBJ_PREFIX;
    return META_LOG_OBJ_PREFIX + period + ".";
  }

  RWLock lock;
  set<int> modified_shards;

  void mark_modified(int shard_id);
public:
  RGWMetadataLog(CephContext *_cct, RGWRados *_store, const std::string& period)
    : cct(_cct), store(_store),
      prefix(make_prefix(period)),
      lock("RGWMetaLog::lock") {}

  void get_shard_oid(int id, string& oid) const {
    char buf[16];
    snprintf(buf, sizeof(buf), "%d", id);
    oid = prefix + buf;
  }

  int add_entry(RGWMetadataHandler *handler, const string& section, const string& key, bufferlist& bl);
  int store_entries_in_shard(list<cls_log_entry>& entries, int shard_id, librados::AioCompletion *completion);

  struct LogListCtx {
    int cur_shard;
    string marker;
    real_time from_time;
    real_time end_time;

    string cur_oid;

    bool done;

    LogListCtx() : cur_shard(0), done(false) {}
  };

  void init_list_entries(int shard_id, const real_time& from_time, const real_time& end_time, string& marker, void **handle);
  void complete_list_entries(void *handle);
  int list_entries(void *handle,
                   int max_entries,
                   list<cls_log_entry>& entries,
		   string *out_marker,
		   bool *truncated);

  int trim(int shard_id, const real_time& from_time, const real_time& end_time, const string& start_marker, const string& end_marker);
  int get_info(int shard_id, RGWMetadataLogInfo *info);
  int get_info_async(int shard_id, RGWMetadataLogInfoCompletion *completion);
  int lock_exclusive(int shard_id, timespan duration, string&zone_id, string& owner_id);
  int unlock(int shard_id, string& zone_id, string& owner_id);

  int update_shards(list<int>& shards);

  void read_clear_modified(set<int> &modified);
};

struct LogStatusDump {
  RGWMDLogStatus status;

  explicit LogStatusDump(RGWMDLogStatus _status) : status(_status) {}
  void dump(Formatter *f) const;
};

struct RGWMetadataLogData {
  obj_version read_version;
  obj_version write_version;
  RGWMDLogStatus status;
  
  RGWMetadataLogData() : status(MDLOG_STATUS_UNKNOWN) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& bl);
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWMetadataLogData)

struct RGWMetadataLogHistory {
  epoch_t oldest_realm_epoch;
  std::string oldest_period_id;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(oldest_realm_epoch, bl);
    encode(oldest_period_id, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& p) {
    DECODE_START(1, p);
    decode(oldest_realm_epoch, p);
    decode(oldest_period_id, p);
    DECODE_FINISH(p);
  }

  static const std::string oid;
};
WRITE_CLASS_ENCODER(RGWMetadataLogHistory)

class RGWMetadataManager {
  map<string, RGWMetadataHandler *> handlers;
  CephContext *cct;
  RGWRados *store;

  // maintain a separate metadata log for each period
  std::map<std::string, RGWMetadataLog> md_logs;
  // use the current period's log for mutating operations
  RGWMetadataLog* current_log = nullptr;

  int find_handler(const string& metadata_key, RGWMetadataHandler **handler, string& entry);
  int pre_modify(RGWMetadataHandler *handler, string& section, const string& key,
                 RGWMetadataLogData& log_data, RGWObjVersionTracker *objv_tracker,
                 RGWMDLogStatus op_type);
  int post_modify(RGWMetadataHandler *handler, const string& section, const string& key, RGWMetadataLogData& log_data,
                 RGWObjVersionTracker *objv_tracker, int ret);

  string heap_oid(RGWMetadataHandler *handler, const string& key, const obj_version& objv);
  int store_in_heap(RGWMetadataHandler *handler, const string& key, bufferlist& bl,
                    RGWObjVersionTracker *objv_tracker, real_time mtime,
                    map<string, bufferlist> *pattrs);
  int remove_from_heap(RGWMetadataHandler *handler, const string& key, RGWObjVersionTracker *objv_tracker);
  int prepare_mutate(RGWRados *store, rgw_pool& pool, const string& oid,
                     const real_time& mtime,
                     RGWObjVersionTracker *objv_tracker,
                     RGWMetadataHandler::sync_type_t sync_mode);

public:
  RGWMetadataManager(CephContext *_cct, RGWRados *_store);
  ~RGWMetadataManager();

  RGWRados* get_store() { return store; }

  int init(const std::string& current_period);

  /// initialize the oldest log period if it doesn't exist, and attach it to
  /// our current history
  RGWPeriodHistory::Cursor init_oldest_log_period();

  /// read the oldest log period, and return a cursor to it in our existing
  /// period history
  RGWPeriodHistory::Cursor read_oldest_log_period() const;

  /// read the oldest log period asynchronously and write its result to the
  /// given cursor pointer
  RGWCoroutine* read_oldest_log_period_cr(RGWPeriodHistory::Cursor *period,
                                          RGWObjVersionTracker *objv) const;

  /// try to advance the oldest log period when the given period is trimmed,
  /// using a rados lock to provide atomicity
  RGWCoroutine* trim_log_period_cr(RGWPeriodHistory::Cursor period,
                                   RGWObjVersionTracker *objv) const;

  /// find or create the metadata log for the given period
  RGWMetadataLog* get_log(const std::string& period);

  int register_handler(RGWMetadataHandler *handler);

  template <typename F>
  int mutate(RGWMetadataHandler *handler, const string& key,
             const ceph::real_time& mtime, RGWObjVersionTracker *objv_tracker,
             RGWMDLogStatus op_type,
             RGWMetadataHandler::sync_type_t sync_mode,
             F&& f);

  RGWMetadataHandler *get_handler(const string& type);

  int put_entry(RGWMetadataHandler *handler, const string& key, bufferlist& bl, bool exclusive,
                RGWObjVersionTracker *objv_tracker, real_time mtime, map<string, bufferlist> *pattrs = NULL);
  int remove_entry(RGWMetadataHandler *handler,
		   const string& key,
		   RGWObjVersionTracker *objv_tracker);
  int get(string& metadata_key, Formatter *f);
  int put(string& metadata_key, bufferlist& bl,
          RGWMetadataHandler::sync_type_t sync_mode,
          obj_version *existing_version = NULL);
  int remove(string& metadata_key);

  int list_keys_init(const string& section, void **phandle);
  int list_keys_init(const string& section, const string& marker, void **phandle);
  int list_keys_next(void *handle, int max, list<string>& keys, bool *truncated);
  void list_keys_complete(void *handle);

  string get_marker(void *handle);

  void dump_log_entry(cls_log_entry& entry, Formatter *f);

  void get_sections(list<string>& sections);
  int lock_exclusive(string& metadata_key, timespan duration, string& owner_id);
  int unlock(string& metadata_key, string& owner_id);

  int get_log_shard_id(const string& section, const string& key, int *shard_id);

  void parse_metadata_key(const string& metadata_key, string& type, string& entry);
};

template <typename F>
int RGWMetadataManager::mutate(RGWMetadataHandler *handler, const string& key,
                               const ceph::real_time& mtime, RGWObjVersionTracker *objv_tracker,
                               RGWMDLogStatus op_type,
                               RGWMetadataHandler::sync_type_t sync_mode,
                               F&& f)
{
  string oid;
  rgw_pool pool;

  handler->get_pool_and_oid(store, key, pool, oid);

  int ret = prepare_mutate(store, pool, oid, mtime, objv_tracker, sync_mode);
  if (ret < 0 ||
      ret == STATUS_NO_APPLY) {
    return ret;
  }

  string section;
  RGWMetadataLogData log_data;
  ret = pre_modify(handler, section, key, log_data, objv_tracker, MDLOG_STATUS_WRITE);
  if (ret < 0) {
    return ret;
  }

  ret = std::forward<F>(f)();

  /* cascading ret into post_modify() */

  ret = post_modify(handler, section, key, log_data, objv_tracker, ret);
  if (ret < 0)
    return ret;

  return 0;
}

#endif
