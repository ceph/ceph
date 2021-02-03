// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "svc_otp.h"
#include "svc_zone.h"
#include "svc_meta.h"
#include "svc_meta_be_sobj.h"

#include "rgw/rgw_zone.h"

#define dout_subsys ceph_subsys_rgw

class RGW_MB_Handler_Module_OTP : public RGWSI_MBSObj_Handler_Module {
  RGWSI_Zone *zone_svc;
  string prefix;
public:
  RGW_MB_Handler_Module_OTP(RGWSI_Zone *_zone_svc) : RGWSI_MBSObj_Handler_Module("otp"),
                                                     zone_svc(_zone_svc) {}

  void get_pool_and_oid(const string& key, rgw_pool *pool, string *oid) override {
    if (pool) {
      *pool = zone_svc->get_zone_params().otp_pool;
    }

    if (oid) {
      *oid = key;
    }
  }

  const string& get_oid_prefix() override {
    return prefix;
  }

  bool is_valid_oid(const string& oid) override {
    return true;
  }

  string key_to_oid(const string& key) override {
    return key;
  }

  string oid_to_key(const string& oid) override {
    return oid;
  }
};

RGWSI_OTP::RGWSI_OTP(CephContext *cct): RGWServiceInstance(cct) {
}

RGWSI_OTP::~RGWSI_OTP() {
}

void RGWSI_OTP::init(RGWSI_Zone *_zone_svc,
                        RGWSI_Meta *_meta_svc,
                        RGWSI_MetaBackend *_meta_be_svc)
{
  svc.otp = this;
  svc.zone = _zone_svc;
  svc.meta = _meta_svc;
  svc.meta_be = _meta_be_svc;
}

int RGWSI_OTP::do_start(optional_yield, const DoutPrefixProvider *dpp)
{
  /* create first backend handler for bucket entrypoints */

  RGWSI_MetaBackend_Handler *_otp_be_handler;

  int r = svc.meta->create_be_handler(RGWSI_MetaBackend::Type::MDBE_OTP, &_otp_be_handler);
  if (r < 0) {
    ldout(ctx(), 0) << "ERROR: failed to create be handler: r=" << r << dendl;
    return r;
  }

  be_handler = _otp_be_handler;

  RGWSI_MetaBackend_Handler_OTP *otp_be_handler = static_cast<RGWSI_MetaBackend_Handler_OTP *>(_otp_be_handler);

  auto otp_be_module = new RGW_MB_Handler_Module_OTP(svc.zone);
  be_module.reset(otp_be_module);
  otp_be_handler->set_module(otp_be_module);

  return 0;
}

int RGWSI_OTP::read_all(RGWSI_OTP_BE_Ctx& ctx,
                        const string& key,
                        otp_devices_list_t *devices,
                        real_time *pmtime,
                        RGWObjVersionTracker *objv_tracker,
                        optional_yield y, const DoutPrefixProvider *dpp)
{
  RGWSI_MBOTP_GetParams params;
  params.pdevices = devices;
  params.pmtime = pmtime;

  int ret = svc.meta_be->get_entry(ctx.get(), key, params, objv_tracker, y, dpp);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RGWSI_OTP::read_all(RGWSI_OTP_BE_Ctx& ctx,
                        const rgw_user& uid,
                        otp_devices_list_t *devices,
                        real_time *pmtime,
                        RGWObjVersionTracker *objv_tracker,
                        optional_yield y,
                        const DoutPrefixProvider *dpp)
{
  return read_all(ctx,
                  uid.to_str(),
                  devices,
                  pmtime,
                  objv_tracker,
                  y,
                  dpp);
}

int RGWSI_OTP::store_all(RGWSI_OTP_BE_Ctx& ctx,
                         const string& key,
                         const otp_devices_list_t& devices,
                         real_time mtime,
                         RGWObjVersionTracker *objv_tracker,
                         optional_yield y)
{
  RGWSI_MBOTP_PutParams params;
  params.mtime = mtime;
  params.devices = devices;

  int ret = svc.meta_be->put_entry(ctx.get(), key, params, objv_tracker, y);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RGWSI_OTP::store_all(RGWSI_OTP_BE_Ctx& ctx,
                         const rgw_user& uid,
                         const otp_devices_list_t& devices,
                         real_time mtime,
                         RGWObjVersionTracker *objv_tracker,
                         optional_yield y)
{
  return store_all(ctx,
                   uid.to_str(),
                   devices,
                   mtime,
                   objv_tracker,
                   y);
}

int RGWSI_OTP::remove_all(RGWSI_OTP_BE_Ctx& ctx,
                          const string& key,
                          RGWObjVersionTracker *objv_tracker,
                          optional_yield y)
{
  RGWSI_MBOTP_RemoveParams params;

  int ret = svc.meta_be->remove_entry(ctx.get(), key, params, objv_tracker, y);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RGWSI_OTP::remove_all(RGWSI_OTP_BE_Ctx& ctx,
                          const rgw_user& uid,
                          RGWObjVersionTracker *objv_tracker,
                          optional_yield y)
{
  return remove_all(ctx,
                    uid.to_str(),
                    objv_tracker,
                    y);
}
