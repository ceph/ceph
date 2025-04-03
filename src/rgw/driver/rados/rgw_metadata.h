// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>
#include <utility>
#include <boost/optional.hpp>

#include "include/types.h"
#include "rgw_common.h"
#include "rgw_period_history.h"
#include "rgw_mdlog_types.h"
#include "cls/version/cls_version_types.h"
#include "cls/log/cls_log_types.h"
#include "common/RefCountedObj.h"
#include "common/ceph_time.h"
#include "rgw_sal_fwd.h"


class RGWCoroutine;
class JSONObj;
struct RGWObjVersionTracker;

struct obj_version;


class RGWMetadataObject {
protected:
  obj_version objv;
  ceph::real_time mtime;
  std::map<std::string, bufferlist> *pattrs{nullptr};
  
public:
  RGWMetadataObject() {}
  RGWMetadataObject(const obj_version& v,
		    real_time m) : objv(v), mtime(m) {}
  virtual ~RGWMetadataObject() {}
  obj_version& get_version();
  real_time& get_mtime() { return mtime; }
  void set_pattrs(std::map<std::string, bufferlist> *_pattrs) {
    pattrs = _pattrs;
  }
  std::map<std::string, bufferlist> *get_pattrs() {
    return pattrs;
  }

  virtual void dump(Formatter *f) const {}
};

class RGWMetadataManager;

class RGWMetadataHandler {
  friend class RGWMetadataManager;

public:
  RGWMetadataHandler() {}
  virtual ~RGWMetadataHandler();
  virtual std::string get_type() = 0;

  virtual RGWMetadataObject *get_meta_obj(JSONObj *jo, const obj_version& objv, const ceph::real_time& mtime) = 0;

  virtual int get(std::string& entry, RGWMetadataObject **obj, optional_yield, const DoutPrefixProvider *dpp) = 0;
  virtual int put(std::string& entry,
                  RGWMetadataObject *obj,
                  RGWObjVersionTracker& objv_tracker,
                  optional_yield, 
                  const DoutPrefixProvider *dpp,
                  RGWMDLogSyncType type,
                  bool from_remote_zone) = 0;
  virtual int remove(std::string& entry, RGWObjVersionTracker& objv_tracker, optional_yield, const DoutPrefixProvider *dpp) = 0;

  virtual int mutate(const std::string& entry,
		     const ceph::real_time& mtime,
		     RGWObjVersionTracker *objv_tracker,
                     optional_yield y,
                     const DoutPrefixProvider *dpp,
		     RGWMDLogStatus op_type,
		     std::function<int()> f) = 0;

  virtual int list_keys_init(const DoutPrefixProvider *dpp, const std::string& marker, void **phandle) = 0;
  virtual int list_keys_next(const DoutPrefixProvider *dpp, void *handle, int max, std::list<std::string>& keys, bool *truncated) = 0;
  virtual void list_keys_complete(void *handle) = 0;

  virtual std::string get_marker(void *handle) = 0;

  virtual int get_shard_id(const std::string& entry, int *shard_id) {
    *shard_id = 0;
    return 0;
  }
  virtual int attach(RGWMetadataManager *manager);
};

class RGWMetadataTopHandler;

class RGWMetadataManager {
  friend class RGWMetadataHandler;

  CephContext *cct;
  std::map<std::string, RGWMetadataHandler *> handlers;
  std::unique_ptr<RGWMetadataTopHandler> md_top_handler;

  int find_handler(const std::string& metadata_key, RGWMetadataHandler **handler, std::string& entry);
  int register_handler(RGWMetadataHandler *handler);

public:
  RGWMetadataManager();
  ~RGWMetadataManager();

  RGWMetadataHandler *get_handler(const std::string& type);

  int get(std::string& metadata_key, Formatter *f, optional_yield y, const DoutPrefixProvider *dpp);
  int put(std::string& metadata_key, bufferlist& bl, optional_yield y,
          const DoutPrefixProvider *dpp,
          RGWMDLogSyncType sync_mode,
          bool from_remote_zone,
          obj_version *existing_version = NULL);
  int remove(std::string& metadata_key, optional_yield y, const DoutPrefixProvider *dpp);

  int mutate(const std::string& metadata_key,
	     const ceph::real_time& mtime,
	     RGWObjVersionTracker *objv_tracker,
             optional_yield y,
             const DoutPrefixProvider *dpp,
	     RGWMDLogStatus op_type,
	     std::function<int()> f);

  int list_keys_init(const DoutPrefixProvider *dpp, const std::string& section, void **phandle);
  int list_keys_init(const DoutPrefixProvider *dpp, const std::string& section, const std::string& marker, void **phandle);
  int list_keys_next(const DoutPrefixProvider *dpp, void *handle, int max, std::list<std::string>& keys, bool *truncated);
  void list_keys_complete(void *handle);

  std::string get_marker(void *handle);

  void dump_log_entry(cls::log::entry& entry, Formatter *f);

  void get_sections(std::list<std::string>& sections);

  void parse_metadata_key(const std::string& metadata_key, std::string& type, std::string& entry);

  int get_shard_id(const std::string& section, const std::string& key, int *shard_id);
};

void rgw_shard_name(const std::string& prefix, unsigned max_shards, const std::string& key, std::string& name, int *shard_id);
void rgw_shard_name(const std::string& prefix, unsigned max_shards, const std::string& section, const std::string& key, std::string& name);
void rgw_shard_name(const std::string& prefix, unsigned shard_id, std::string& name);

