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
#include "rgw_mdlog_types.h"
#include "cls/version/cls_version_types.h"
#include "cls/log/cls_log_types.h"
#include "common/RWLock.h"
#include "common/RefCountedObj.h"
#include "common/ceph_time.h"

#include "services/svc_meta_be.h"


class RGWRados;
class RGWCoroutine;
class JSONObj;
struct RGWObjVersionTracker;

struct obj_version;


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
  friend class RGWSI_MetaBackend;
  friend class RGWMetadataManager;

  RGWSI_MetaBackend_Handle be_handle{0};

public:
  virtual ~RGWMetadataHandler() {}
  virtual string get_type() = 0;

  virtual RGWSI_MetaBackend::ModuleRef get_backend_module(RGWSI_MetaBackend::Type be_type) = 0;

  virtual int get(RGWRados *store, string& entry, RGWMetadataObject **obj) = 0;
  virtual int put(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker,
                  real_time mtime, JSONObj *obj, RGWMDLogSyncType type) = 0;
  virtual int remove(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker) = 0;

  virtual int list_keys_init(RGWRados *store, const string& marker, void **phandle) = 0;
  virtual int list_keys_next(void *handle, int max, list<string>& keys, bool *truncated) = 0;
  virtual void list_keys_complete(void *handle) = 0;

  virtual string get_marker(void *handle) = 0;

  int init(RGWMetadataManager *manager);


protected:
  /**
   * Compare an incoming versus on-disk tag/version+mtime combo against
   * the sync mode to see if the new one should replace the on-disk one.
   *
   * @return true if the update should proceed, false otherwise.
   */
  static bool check_versions(const obj_version& ondisk, const real_time& ondisk_time,
                             const obj_version& incoming, const real_time& incoming_time,
                             RGWMDLogSyncType sync_mode) {
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
};

class RGWMetadataManager {
  map<string, RGWMetadataHandler *> handlers;
  CephContext *cct;

  int find_handler(const string& metadata_key, RGWMetadataHandler **handler, string& entry);

protected:
  int register_handler(RGWMetadataHandler *handler, RGWSI_MetaBackend_Handle *phandle);

public:
  RGWMetadataManager(CephContext *_cct);
  ~RGWMetadataManager();

  RGWMetadataHandler *get_handler(const string& type);

  int get(string& metadata_key, Formatter *f);
  int put(string& metadata_key, bufferlist& bl,
          RGWMDLogSyncType sync_mode,
          obj_version *existing_version = NULL);
  int remove(string& metadata_key);

  int list_keys_init(const string& section, void **phandle);
  int list_keys_init(const string& section, const string& marker, void **phandle);
  int list_keys_next(void *handle, int max, list<string>& keys, bool *truncated);
  void list_keys_complete(void *handle);

  string get_marker(void *handle);

  void dump_log_entry(cls_log_entry& entry, Formatter *f);

  void get_sections(list<string>& sections);

  int get_log_shard_id(const string& section, const string& key, int *shard_id);

  void parse_metadata_key(const string& metadata_key, string& type, string& entry);
};


#endif
