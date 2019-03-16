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
#include "services/svc_meta_be.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;


static RGWMetadataHandler *otp_meta_handler = NULL;


class RGWOTPMetadataHandler;

class RGWOTPMetadataObject : public RGWMetadataObject {
  friend class RGWOTPMetadataHandler;

  list<rados::cls::otp::otp_info_t> devices;
public:
  RGWOTPMetadataObject() {}
  RGWOTPMetadataObject(list<rados::cls::otp::otp_info_t>& _devices, obj_version& v, real_time m) {
    devices.swap(_devices);
    objv = v;
    mtime = m;
  }

  void dump(Formatter *f) const override {
    encode_json("devices", devices, f);
  }

  list<rados::cls::otp::otp_info_t>& get_devs() {
    return devices;
  }
};

class RGW_MB_Handler_Module_OTP : public RGWSI_MBSObj_Handler_Module {
  RGWSI_Zone *zone_svc;
public:
  RGW_MB_Handler_Module_OTP(RGWSI_Zone *_zone_svc) : zone_svc {}

  void get_pool_and_oid(RGWRados *store, const string& key, rgw_pool& pool, string& oid) override {
    oid = key;
    pool = store->svc.zone->get_zone_params().otp_pool;
  }
};

class RGWOTPMetadataHandler : public RGWMetadataHandler {
  int init_module() override {
    be_module.reset(new RGW_MB_Handler_Module_OTP(store->svc.zone));
    return 0;
  }

  RGWSI_MetaBackend::Type required_be_type() override {
    return MDBE_OTP;
  }

  int do_get(RGWSI_MetaBackend::Context *ctx, string& entry, RGWMetadataObject **obj) override {
    RGWObjVersionTracker objv_tracker;
    real_time mtime;

    RGWOTPMetadataObject *mdo = new RGWOTPMetadataObject;

    ctx->obj = mdo;

    int ret = meta_be->get_entry(ctx, nullptr,
                                 objv_tracker, &mdo->mtime,
				 nullptr,
                                 nullptr, nullopt);
    if (ret < 0) {
      return ret;
    }

    mdo->read_version = objv_tracker.read_version;

    *obj = mdo;

    return 0;
  }

  int do_put(RGWSI_MetaBackend::Context *ctx, string& entry,
             RGWMetadataObject *_obj, RGWObjVersionTracker& objv_tracker,
             RGWMDLogSyncType type) override {
    RGWOTPMetadataObject *obj = static_cast<RGWOTPMetadataObject *>(_obj);

    ctx->obj = obj;

    bufferlist bl;
    int ret = meta_be->put(ctx, bl,
                           false, objv_tracker,
			   obj->mtime, pattrs,
                           APPLY_ALWAYS);
    if (ret < 0) {
      return ret;
    }

    return STATUS_APPLIED;
  }

  int do_remove(RGWSI_MetaBackend::Context *ctx, string& entry, RGWObjVersionTracker& objv_tracker) override {
    return svc.meta_be->remove_entry(this, entry, &objv_tracker);
  }

public:
  string get_type() override { return "otp"; }

  struct list_keys_info {
    RGWRados *store;
    RGWListRawObjsCtx ctx;
  };

  int list_keys_init(const string& marker, void **phandle) override
  {
    auto info = std::make_unique<list_keys_info>();

    info->store = store;

    int ret = store->list_raw_objects_init(store->svc.zone->get_zone_params().otp_pool, marker,
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

void rgw_otp_init(RGWRados *store)
{
  otp_meta_handler = new RGWOTPMetadataHandler;
  store->meta_mgr->register_handler(otp_meta_handler);
}
