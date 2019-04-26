// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include <string>
#include <map>
#include <boost/algorithm/string.hpp>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "rgw_rados.h"
#include "rgw_zone.h"

#include "include/types.h"

#include "rgw_common.h"
#include "rgw_tools.h"

#include "services/svc_zone.h"
#include "services/svc_cls.h"
#include "services/svc_meta.h"
#include "services/svc_meta_be.h"
#include "services/svc_meta_be_otp.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;


static RGWMetadataHandler *otp_meta_handler = NULL;


class RGWOTPMetadataHandler;

class RGWOTPMetadataObject : public RGWMetadataObject {
  friend class RGWOTPMetadataHandler;

  otp_devices_list_t devices;
public:
  RGWOTPMetadataObject() {}
  RGWOTPMetadataObject(otp_devices_list_t&& _devices, const obj_version& v, const real_time m) {
    devices = std::move(_devices);
    objv = v;
    mtime = m;
  }

  void dump(Formatter *f) const override {
    encode_json("devices", devices, f);
  }

  otp_devices_list_t& get_devs() {
    return devices;
  }
};

class RGW_MB_Handler_Module_OTP : public RGWSI_MBSObj_Handler_Module {
  RGWSI_Zone *zone_svc;
public:
  RGW_MB_Handler_Module_OTP(RGWSI_Zone *_zone_svc) : zone_svc(_zone_svc) {}

  void get_pool_and_oid(const string& key, rgw_pool& pool, string& oid) override {
    oid = key;
    pool = zone_svc->get_zone_params().otp_pool;
  }
};


class RGWOTPMetadataHandler : public RGWMetadataHandler {
  struct Svc {
    RGWSI_Zone *zone;
    RGWSI_MetaBackend *meta_be;
  } svc;

  int init_module() override {
    be_module.reset(new RGW_MB_Handler_Module_OTP(svc.zone));
    return 0;
  }

  RGWMetadataObject *get_meta_obj(JSONObj *jo, const obj_version& objv, const ceph::real_time& mtime) override {
    otp_devices_list_t devices;
    try {
      JSONDecoder::decode_json("devices", devices, jo);
    } catch (JSONDecoder::err& e) {
      return nullptr;
    }

    return new RGWOTPMetadataObject(std::move(devices), objv, mtime);
  }

  int do_get(RGWSI_MetaBackend::Context *ctx, string& entry, RGWMetadataObject **obj) override {
    RGWObjVersionTracker objv_tracker;

    RGWOTPMetadataObject *mdo = new RGWOTPMetadataObject;

    
    RGWSI_MBOTP_GetParams params;
    params.pdevices = &(mdo->get_devs());

    int ret = svc.meta_be->get_entry(ctx, params, &objv_tracker);
    if (ret < 0) {
      return ret;
    }

    mdo->objv = objv_tracker.read_version;

    *obj = mdo;

    return 0;
  }

  int do_put(RGWSI_MetaBackend::Context *ctx, string& entry,
             RGWMetadataObject *_obj, RGWObjVersionTracker& objv_tracker,
             RGWMDLogSyncType type) override {
    RGWOTPMetadataObject *obj = static_cast<RGWOTPMetadataObject *>(_obj);

    RGWSI_MBOTP_PutParams params;
    params.mtime = obj->mtime;
    params.devices = obj->devices;

    int ret = svc.meta_be->put_entry(ctx, params, &objv_tracker);
    if (ret < 0) {
      return ret;
    }

    return STATUS_APPLIED;
  }

  int do_remove(RGWSI_MetaBackend::Context *ctx, string& entry, RGWObjVersionTracker& objv_tracker) override {
    RGWSI_MBOTP_RemoveParams params;
    return svc.meta_be->remove_entry(ctx, params, &objv_tracker);
  }

public:
  RGWOTPMetadataHandler(RGWSI_Zone *zone_svc,
                        RGWSI_MetaBackend *meta_be_svc) {
    svc.zone = zone_svc;
    svc.meta_be = meta_be_svc;
  }

  string get_type() override { return "otp"; }

  struct list_keys_info {
    RGWRados *store;
    RGWListRawObjsCtx ctx;
  };

  int list_keys_init(const string& marker, void **phandle) override
  {
    auto info = std::make_unique<list_keys_info>();

    info->store = store;

    int ret = store->list_raw_objects_init(svc.zone->get_zone_params().otp_pool, marker,
                                           &info->ctx);
    if (ret < 0) {
      return ret;
    }

    *phandle = (void *)info.release();

    return 0;
  }

  int list_keys_next(void *handle, int max, list<string>& keys, bool *truncated) override {
    list_keys_info *info = static_cast<list_keys_info *>(handle);

    string no_filter;

    keys.clear();

    RGWRados *store = info->store;

    int ret = store->list_raw_objects_next(no_filter, max, info->ctx,
                                           keys, truncated);
    if (ret < 0 && ret != -ENOENT)
      return ret;
    if (ret == -ENOENT) {
      if (truncated)
        *truncated = false;
      return 0;
    }

    return 0;
  }

  void list_keys_complete(void *handle) override {
    list_keys_info *info = static_cast<list_keys_info *>(handle);
    delete info;
  }

  string get_marker(void *handle) override {
    list_keys_info *info = static_cast<list_keys_info *>(handle);
    return info->store->list_raw_objs_get_cursor(info->ctx);
  }
};

RGWMetadataHandler *rgw_otp_get_handler()
{
  return otp_meta_handler;
}

RGWMetadataHandler *RGWOTPMetaHandlerAllocator::alloc(RGWSI_Zone *zone_svc,
                                                      RGWSI_MetaBackend *meta_be_svc)
{
  return new RGWOTPMetadataHandler(zone_svc, meta_be_svc);
}
