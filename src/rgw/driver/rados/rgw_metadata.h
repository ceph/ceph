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
#include "services/svc_meta_be.h"
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

protected:
  CephContext *cct;

public:
  RGWMetadataHandler() {}
  virtual ~RGWMetadataHandler();
  virtual std::string get_type() = 0;

  void base_init(CephContext *_cct) {
    cct = _cct;
  }

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

class RGWMetadataHandler_GenericMetaBE : public RGWMetadataHandler {
  friend class RGWSI_MetaBackend;
  friend class RGWMetadataManager;
  friend class Put;

public:
  class Put;

protected:
  RGWSI_MetaBackend_Handler *be_handler;

  virtual int do_get(RGWSI_MetaBackend_Handler::Op *op, std::string& entry, RGWMetadataObject **obj, optional_yield y, const DoutPrefixProvider *dpp) = 0;
  virtual int do_put(RGWSI_MetaBackend_Handler::Op *op, std::string& entry, RGWMetadataObject *obj,
                     RGWObjVersionTracker& objv_tracker, optional_yield y,
                     const DoutPrefixProvider *dpp, RGWMDLogSyncType type, 
                     bool from_remote_zone) = 0;
  virtual int do_put_operate(Put *put_op, const DoutPrefixProvider *dpp);
  virtual int do_remove(RGWSI_MetaBackend_Handler::Op *op, std::string& entry, RGWObjVersionTracker& objv_tracker, optional_yield y, const DoutPrefixProvider *dpp) = 0;

public:
  RGWMetadataHandler_GenericMetaBE() {}

  void base_init(CephContext *_cct,
            RGWSI_MetaBackend_Handler *_be_handler) {
    RGWMetadataHandler::base_init(_cct);
    be_handler = _be_handler;
  }

  RGWSI_MetaBackend_Handler *get_be_handler() {
    return be_handler;
  }

  class Put {
  protected:
    RGWMetadataHandler_GenericMetaBE *handler;
    RGWSI_MetaBackend_Handler::Op *op;
    std::string& entry;
    RGWMetadataObject *obj;
    RGWObjVersionTracker& objv_tracker;
    RGWMDLogSyncType apply_type;
    optional_yield y;
    bool from_remote_zone{false};

    int get(RGWMetadataObject **obj, const DoutPrefixProvider *dpp) {
      return handler->do_get(op, entry, obj, y, dpp);
    }
  public:
    Put(RGWMetadataHandler_GenericMetaBE *_handler, RGWSI_MetaBackend_Handler::Op *_op,
        std::string& _entry, RGWMetadataObject *_obj,
        RGWObjVersionTracker& _objv_tracker, optional_yield _y,
        RGWMDLogSyncType _type, bool from_remote_zone);

    virtual ~Put() {}

    virtual int put_pre(const DoutPrefixProvider *dpp) {
      return 0;
    }
    virtual int put(const DoutPrefixProvider *dpp) {
      return 0;
    }
    virtual int put_post(const DoutPrefixProvider *dpp) {
      return 0;
    }
    virtual int finalize() {
      return 0;
    }
  };

  int get(std::string& entry, RGWMetadataObject **obj, optional_yield, const DoutPrefixProvider *dpp) override;
  int put(std::string& entry, RGWMetadataObject *obj, RGWObjVersionTracker& objv_tracker, optional_yield, const DoutPrefixProvider *dpp, RGWMDLogSyncType type, bool from_remote_zone) override;
  int remove(std::string& entry, RGWObjVersionTracker& objv_tracker, optional_yield, const DoutPrefixProvider *dpp) override;

  int mutate(const std::string& entry,
	     const ceph::real_time& mtime,
	     RGWObjVersionTracker *objv_tracker,
             optional_yield y,
             const DoutPrefixProvider *dpp,
	     RGWMDLogStatus op_type,
	     std::function<int()> f) override;

  int get_shard_id(const std::string& entry, int *shard_id) override;

  int list_keys_init(const DoutPrefixProvider *dpp, const std::string& marker, void **phandle) override;
  int list_keys_next(const DoutPrefixProvider *dpp, void *handle, int max, std::list<std::string>& keys, bool *truncated) override;
  void list_keys_complete(void *handle) override;

  std::string get_marker(void *handle) override;

  /**
   * Compare an incoming versus on-disk tag/version+mtime combo against
   * the sync mode to see if the new one should replace the on-disk one.
   *
   * @return true if the update should proceed, false otherwise.
   */
  static bool check_versions(bool exists,
                             const obj_version& ondisk, const real_time& ondisk_time,
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
    case APPLY_EXCLUSIVE:
      if (exists)
        return false;
      break;
    case APPLY_ALWAYS: //deliberate fall-thru -- we always apply!
    default: break;
    }
    return true;
  }
};

class RGWMetadataTopHandler;

class RGWMetadataManager {
  friend class RGWMetadataHandler;

  CephContext *cct;
  RGWSI_Meta *meta_svc;
  std::map<std::string, RGWMetadataHandler *> handlers;
  std::unique_ptr<RGWMetadataTopHandler> md_top_handler;

  int find_handler(const std::string& metadata_key, RGWMetadataHandler **handler, std::string& entry);
  int register_handler(RGWMetadataHandler *handler);

public:
  RGWMetadataManager(RGWSI_Meta *_meta_svc);
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

  void dump_log_entry(cls_log_entry& entry, Formatter *f);

  void get_sections(std::list<std::string>& sections);

  void parse_metadata_key(const std::string& metadata_key, std::string& type, std::string& entry);

  int get_shard_id(const std::string& section, const std::string& key, int *shard_id);
};

class RGWMetadataHandlerPut_SObj : public RGWMetadataHandler_GenericMetaBE::Put
{
protected:
  std::unique_ptr<RGWMetadataObject> oo;
  RGWMetadataObject *old_obj{nullptr};
  bool exists{false};

public:
  RGWMetadataHandlerPut_SObj(RGWMetadataHandler_GenericMetaBE *handler, RGWSI_MetaBackend_Handler::Op *op,
                             std::string& entry, RGWMetadataObject *obj, RGWObjVersionTracker& objv_tracker,
			     optional_yield y,
                             RGWMDLogSyncType type, bool from_remote_zone);
  ~RGWMetadataHandlerPut_SObj();

  int put_pre(const DoutPrefixProvider *dpp) override;
  int put(const DoutPrefixProvider *dpp) override;
  virtual int put_check(const DoutPrefixProvider *dpp) {
    return 0;
  }
  virtual int put_checked(const DoutPrefixProvider *dpp);
  virtual void encode_obj(bufferlist *bl) {}
};

void rgw_shard_name(const std::string& prefix, unsigned max_shards, const std::string& key, std::string& name, int *shard_id);
void rgw_shard_name(const std::string& prefix, unsigned max_shards, const std::string& section, const std::string& key, std::string& name);
void rgw_shard_name(const std::string& prefix, unsigned shard_id, std::string& name);

