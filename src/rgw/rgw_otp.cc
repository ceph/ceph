// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <errno.h>

#include <string>
#include <map>
#include <boost/algorithm/string.hpp>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "rgw_otp.h"
#include "rgw_zone.h"
#include "rgw_metadata.h"

#include "include/types.h"

#include "rgw_common.h"
#include "rgw_tools.h"

#include "services/svc_zone.h"
#include "services/svc_meta.h"
#include "services/svc_meta_be.h"
#include "services/svc_meta_be_otp.h"
#include "services/svc_otp.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;


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


class RGWOTPMetadataHandler : public RGWOTPMetadataHandlerBase {
  friend class RGWOTPCtl;

  struct Svc {
    RGWSI_Zone *zone;
    RGWSI_MetaBackend *meta_be;
    RGWSI_OTP *otp;
  } svc;

  int init(RGWSI_Zone *zone,
           RGWSI_MetaBackend *_meta_be,
           RGWSI_OTP *_otp) {
    base_init(zone->ctx(), _otp->get_be_handler().get());
    svc.zone = zone;
    svc.meta_be = _meta_be;
    svc.otp = _otp;
    return 0;
  }

  int call(std::function<int(RGWSI_OTP_BE_Ctx& ctx)> f) {
    return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
      RGWSI_OTP_BE_Ctx ctx(op->ctx());
      return f(ctx);
    });
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

  int do_get(RGWSI_MetaBackend_Handler::Op *op, string& entry, RGWMetadataObject **obj, optional_yield y, const DoutPrefixProvider *dpp) override {
    RGWObjVersionTracker objv_tracker;

    std::unique_ptr<RGWOTPMetadataObject> mdo(new RGWOTPMetadataObject);

    
    RGWSI_OTP_BE_Ctx be_ctx(op->ctx());

    int ret = svc.otp->read_all(be_ctx,
                                entry,
                                &mdo->get_devs(),
                                &mdo->get_mtime(),
                                &objv_tracker,
                                y,
                                dpp);
    if (ret < 0) {
      return ret;
    }

    mdo->objv = objv_tracker.read_version;

    *obj = mdo.release();

    return 0;
  }

  int do_put(RGWSI_MetaBackend_Handler::Op *op, string& entry,
             RGWMetadataObject *_obj, RGWObjVersionTracker& objv_tracker,
             optional_yield y,
             const DoutPrefixProvider *dpp,
             RGWMDLogSyncType type, bool from_remote_zone) override {
    RGWOTPMetadataObject *obj = static_cast<RGWOTPMetadataObject *>(_obj);

    RGWSI_OTP_BE_Ctx be_ctx(op->ctx());

    int ret = svc.otp->store_all(dpp, be_ctx,
                                 entry,
                                 obj->devices,
                                 obj->mtime,
                                 &objv_tracker,
                                 y);
    if (ret < 0) {
      return ret;
    }

    return STATUS_APPLIED;
  }

  int do_remove(RGWSI_MetaBackend_Handler::Op *op, string& entry, RGWObjVersionTracker& objv_tracker,
                optional_yield y, const DoutPrefixProvider *dpp) override {
    RGWSI_MBOTP_RemoveParams params;

    RGWSI_OTP_BE_Ctx be_ctx(op->ctx());

    return svc.otp->remove_all(dpp, be_ctx,
                               entry,
                               &objv_tracker,
                               y);
  }

public:
  RGWOTPMetadataHandler() {}

  string get_type() override { return "otp"; }
};


RGWOTPCtl::RGWOTPCtl(RGWSI_Zone *zone_svc,
		     RGWSI_OTP *otp_svc)
{
  svc.zone = zone_svc;
  svc.otp = otp_svc;
}


void RGWOTPCtl::init(RGWOTPMetadataHandler *_meta_handler)
{
  meta_handler = _meta_handler;
  be_handler = meta_handler->get_be_handler();
}

int RGWOTPCtl::read_all(const rgw_user& uid,
                        RGWOTPInfo *info,
                        optional_yield y,
                        const DoutPrefixProvider *dpp,
                        const GetParams& params)
{
  info->uid = uid;
  return meta_handler->call([&](RGWSI_OTP_BE_Ctx& ctx) {
    return svc.otp->read_all(ctx, uid, &info->devices, params.mtime, params.objv_tracker, y, dpp);
  });
}

int RGWOTPCtl::store_all(const DoutPrefixProvider *dpp, 
                         const RGWOTPInfo& info,
                         optional_yield y,
                         const PutParams& params)
{
  return meta_handler->call([&](RGWSI_OTP_BE_Ctx& ctx) {
    return svc.otp->store_all(dpp, ctx, info.uid, info.devices, params.mtime, params.objv_tracker, y);
  });
}

int RGWOTPCtl::remove_all(const DoutPrefixProvider *dpp,
                          const rgw_user& uid,
                          optional_yield y,
                          const RemoveParams& params)
{
  return meta_handler->call([&](RGWSI_OTP_BE_Ctx& ctx) {
    return svc.otp->remove_all(dpp, ctx, uid, params.objv_tracker, y);
  });
}


RGWMetadataHandler *RGWOTPMetaHandlerAllocator::alloc()
{
  return new RGWOTPMetadataHandler();
}
