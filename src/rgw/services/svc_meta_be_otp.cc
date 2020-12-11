// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "svc_meta_be_otp.h"

#include "rgw/rgw_tools.h"
#include "rgw/rgw_metadata.h"
#include "rgw/rgw_mdlog.h"

#define dout_subsys ceph_subsys_rgw


RGWSI_MetaBackend_OTP::RGWSI_MetaBackend_OTP(CephContext *cct) : RGWSI_MetaBackend_SObj(cct) {
}

RGWSI_MetaBackend_OTP::~RGWSI_MetaBackend_OTP() {
}

string RGWSI_MetaBackend_OTP::get_meta_key(const rgw_user& user)
{
  return string("otp:user:") + user.to_str();
}

RGWSI_MetaBackend_Handler *RGWSI_MetaBackend_OTP::alloc_be_handler()
{
  return new RGWSI_MetaBackend_Handler_OTP(this);
}

RGWSI_MetaBackend::Context *RGWSI_MetaBackend_OTP::alloc_ctx()
{
  return new Context_OTP(sysobj_svc);
}

int RGWSI_MetaBackend_OTP::call_with_get_params(ceph::real_time *pmtime, std::function<int(RGWSI_MetaBackend::GetParams&)> cb)
{
  otp_devices_list_t devices;
  RGWSI_MBOTP_GetParams params;
  params.pdevices = &devices;
  params.pmtime = pmtime;
  return cb(params);
}

int RGWSI_MetaBackend_OTP::get_entry(RGWSI_MetaBackend::Context *_ctx,
                                     const string& key,
                                     RGWSI_MetaBackend::GetParams& _params,
                                     RGWObjVersionTracker *objv_tracker,
                                     optional_yield y,
                                     const DoutPrefixProvider *dpp)
{
  RGWSI_MBOTP_GetParams& params = static_cast<RGWSI_MBOTP_GetParams&>(_params);

  int r = cls_svc->mfa.list_mfa(key, params.pdevices, objv_tracker, params.pmtime, y);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWSI_MetaBackend_OTP::put_entry(RGWSI_MetaBackend::Context *_ctx,
                                     const string& key,
                                     RGWSI_MetaBackend::PutParams& _params,
                                     RGWObjVersionTracker *objv_tracker,
                                     optional_yield y)
{
  RGWSI_MBOTP_PutParams& params = static_cast<RGWSI_MBOTP_PutParams&>(_params);

  return cls_svc->mfa.set_mfa(key, params.devices, true, objv_tracker, params.mtime, y);
}

