#ifndef CEPH_RGW_METADATA_H
#define CEPH_RGW_METADATA_H

#include <string>

#include "include/types.h"
#include "rgw_common.h"
#include "cls/version/cls_version_types.h"
#include "cls/log/cls_log_types.h"


class RGWRados;
class JSONObj;
class RGWObjVersionTracker;

struct obj_version;


enum RGWMDLogStatus {
  MDLOG_STATUS_UNKNOWN,
  MDLOG_STATUS_WRITE,
  MDLOG_STATUS_SETATTRS,
  MDLOG_STATUS_REMOVE,
  MDLOG_STATUS_COMPLETE,
};

class RGWMetadataObject {
protected:
  obj_version objv;
  
public:
  virtual ~RGWMetadataObject() {}
  obj_version& get_version();

  virtual void dump(Formatter *f) const = 0;
};

class RGWMetadataManager;

class RGWMetadataHandler {
  friend class RGWMetadataManager;

protected:
  virtual void get_pool_and_oid(RGWRados *store, string& key, rgw_bucket& bucket, string& oid) = 0;
public:
  virtual ~RGWMetadataHandler() {}
  virtual string get_type() = 0;

  virtual int get(RGWRados *store, string& entry, RGWMetadataObject **obj) = 0;
  virtual int put(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker, JSONObj *obj) = 0;
  virtual int remove(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker) = 0;

  virtual int list_keys_init(RGWRados *store, void **phandle) = 0;
  virtual int list_keys_next(void *handle, int max, list<string>& keys, bool *truncated) = 0;
  virtual void list_keys_complete(void *handle) = 0;
};

#define META_LOG_OBJ_PREFIX "meta.log."

class RGWMetadataLog {
  CephContext *cct;
  RGWRados *store;
  string prefix;

  void get_shard_oid(int id, string& oid) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%d", id);
    oid = prefix + buf;
  }

public:
  RGWMetadataLog(CephContext *_cct, RGWRados *_store) : cct(_cct), store(_store) {
    prefix = META_LOG_OBJ_PREFIX;
  }

  int add_entry(RGWRados *store, string& section, string& key, bufferlist& bl);

  struct LogListCtx {
    int cur_shard;
    string marker;
    utime_t from_time;
    utime_t end_time;

    string cur_oid;

    bool done;

    LogListCtx() : done(false) {}
  };

  void init_list_entries(int shard_id, utime_t& from_time, utime_t& end_time, void **handle);
  void complete_list_entries(void *handle);
  int list_entries(void *handle,
                   int max_entries,
                   list<cls_log_entry>& entries,
                   bool *truncated);

  int trim(int shard_id, utime_t& from_time, utime_t& end_time);
  int lock_exclusive(int shard_id, utime_t& duration, string& owner_id);
  int unlock(int shard_id, string& owner_id);
};

class RGWMetadataLogData;

class RGWMetadataManager {
  map<string, RGWMetadataHandler *> handlers;
  CephContext *cct;
  RGWRados *store;
  RGWMetadataLog *md_log;

  void parse_metadata_key(const string& metadata_key, string& type, string& entry);

  int find_handler(const string& metadata_key, RGWMetadataHandler **handler, string& entry);
  int pre_modify(RGWMetadataHandler *handler, string& section, string& key,
                 RGWMetadataLogData& log_data, RGWObjVersionTracker *objv_tracker,
                 RGWMDLogStatus op_type);
  int post_modify(string& section, string& key, RGWMetadataLogData& log_data,
                 RGWObjVersionTracker *objv_tracker);

public:
  RGWMetadataManager(CephContext *_cct, RGWRados *_store);
  ~RGWMetadataManager();

  int register_handler(RGWMetadataHandler *handler);

  RGWMetadataHandler *get_handler(const char *type);

  int put_entry(RGWMetadataHandler *handler, string& key, bufferlist& bl, bool exclusive,
                RGWObjVersionTracker *objv_tracker, map<string, bufferlist> *pattrs = NULL);
  int remove_entry(RGWMetadataHandler *handler, string& key, RGWObjVersionTracker *objv_tracker);
  int set_attr(RGWMetadataHandler *handler, string& key, rgw_obj& obj, string& attr, bufferlist& bl,
               RGWObjVersionTracker *objv_tracker);
  int set_attrs(RGWMetadataHandler *handler, string& key,
                rgw_obj& obj, map<string, bufferlist>& attrs,
                map<string, bufferlist>* rmattrs,
                RGWObjVersionTracker *objv_tracker);
  int get(string& metadata_key, Formatter *f);
  int put(string& metadata_key, bufferlist& bl);
  int remove(string& metadata_key);

  int list_keys_init(string& section, void **phandle);
  int list_keys_next(void *handle, int max, list<string>& keys, bool *truncated);
  void list_keys_complete(void *handle);

  void dump_log_entry(cls_log_entry& entry, Formatter *f);

  void get_sections(list<string>& sections);
  int lock_exclusive(string& metadata_key, utime_t duration, string& owner_id);
  int unlock(string& metadata_key, string& owner_id);

  RGWMetadataLog *get_log() { return md_log; }
};

#endif
