#ifndef CEPH_RGW_METADATA_H
#define CEPH_RGW_METADATA_H

#include <string>

#include "include/types.h"
#include "rgw_common.h"
#include "cls/version/cls_version_types.h"


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
  virtual int put_obj(RGWRados *store, string& key, bufferlist& bl, bool exclusive,
                      RGWObjVersionTracker *objv_tracker, map<string, bufferlist> *pattrs = NULL) = 0;
public:
  virtual ~RGWMetadataHandler() {}
  virtual string get_type() = 0;

  virtual int get(RGWRados *store, string& entry, RGWMetadataObject **obj) = 0;
  virtual int put(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker, JSONObj *obj) = 0;


  virtual int list_keys_init(RGWRados *store, void **phandle) = 0;
  virtual int list_keys_next(void *handle, int max, list<string>& keys, bool *truncated) = 0;
  virtual void list_keys_complete(void *handle) = 0;
};

class RGWMetadataManager {
  map<string, RGWMetadataHandler *> handlers;
  RGWRados *store;

  void parse_metadata_key(const string& metadata_key, string& type, string& entry);

  int find_handler(const string& metadata_key, RGWMetadataHandler **handler, string& entry);

public:
  RGWMetadataManager(RGWRados *_store) : store(_store) {}
  ~RGWMetadataManager();

  int register_handler(RGWMetadataHandler *handler);

  RGWMetadataHandler *get_handler(const char *type);

  int put_obj(RGWMetadataHandler *handler, string& key, bufferlist& bl, bool exclusive,
              RGWObjVersionTracker *objv_tracker, map<string, bufferlist> *pattrs = NULL);
  int get(string& metadata_key, Formatter *f);
  int put(string& metadata_key, bufferlist& bl);

  int list_keys_init(string& section, void **phandle);
  int list_keys_next(void *handle, int max, list<string>& keys, bool *truncated);
  void list_keys_complete(void *handle);

  void get_sections(list<string>& sections);
};

#endif
