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
  virtual int put_entry(RGWRados *store, string& key, bufferlist& bl, bool exclusive,
                        RGWObjVersionTracker *objv_tracker, map<string, bufferlist> *pattrs = NULL) = 0;
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
    RGWRados *store;
    int cur_shard;
    string marker;
    utime_t from_time;
    utime_t end_time;

    string cur_oid;

    bool done;

    LogListCtx(RGWRados *_store) : store(_store), cur_shard(0), done(false) {}
  };

  void init_list_entries(RGWRados *store, utime_t& from_time, utime_t& end_time, void **handle);
  void complete_list_entries(void *handle);
  int list_entries(void *handle,
                   int max_entries,
                   list<cls_log_entry>& entries,
                   bool *truncated);

  int trim(RGWRados *store, utime_t& from_time, utime_t& end_time);
};

class RGWMetadataManager {
  map<string, RGWMetadataHandler *> handlers;
  CephContext *cct;
  RGWRados *store;
  RGWMetadataLog *md_log;

  void parse_metadata_key(const string& metadata_key, string& type, string& entry);

  int find_handler(const string& metadata_key, RGWMetadataHandler **handler, string& entry);

public:
  RGWMetadataManager(CephContext *_cct, RGWRados *_store);
  ~RGWMetadataManager();

  int register_handler(RGWMetadataHandler *handler);

  RGWMetadataHandler *get_handler(const char *type);

  int put_entry(RGWMetadataHandler *handler, string& key, bufferlist& bl, bool exclusive,
                RGWObjVersionTracker *objv_tracker, map<string, bufferlist> *pattrs = NULL);
  int get(string& metadata_key, Formatter *f);
  int put(string& metadata_key, bufferlist& bl);
  int remove(string& metadata_key);

  int list_keys_init(string& section, void **phandle);
  int list_keys_next(void *handle, int max, list<string>& keys, bool *truncated);
  void list_keys_complete(void *handle);

  void dump_log_entry(cls_log_entry& entry, Formatter *f);

  void get_sections(list<string>& sections);

  RGWMetadataLog *get_log() { return md_log; }
};

#endif
