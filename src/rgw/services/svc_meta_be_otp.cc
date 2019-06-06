
#include "svc_meta_be_otp.h"

#include "rgw/rgw_tools.h"
#include "rgw/rgw_metadata.h"
#include "rgw/rgw_mdlog.h"

#define dout_subsys ceph_subsys_rgw


RGWSI_MetaBackend_OTP::RGWSI_MetaBackend_OTP(CephContext *cct) : RGWSI_MetaBackend_SObj(cct) {
}

RGWSI_MetaBackend_OTP::~RGWSI_MetaBackend_OTP() {
}

RGWSI_MetaBackend::Context *RGWSI_MetaBackend_SObj::alloc_ctx()
{
  return new Context_OTP(sysobj_svc);
}

int RGWSI_MetaBackend_OTP::call_with_get_params(ceph::real_time *pmtime, std::function<int(RGWSI_MetaBackend::GetParams&)> cb)
{
  otp_device_list_t devices;
  RGWSI_MBOTP_GetParams params;
  params.pdevices = &devices;
  params.pmtime = pmtime;
  return cb(params);
}

int RGWSI_MetaBackend_SObj::get_entry(RGWSI_MetaBackend::Context *_ctx,
                                      RGWSI_MetaBackend::GetParams& _params,
                                      RGWObjVersionTracker *objv_tracker)
{
  RGWSI_MetaBackend_SObj::Context_OTP *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_OTP *>(_ctx);
  RGWSI_MBOTP_GetParams& params = static_cast<RGWSI_MBOTP_GetParams&>(_params);

  int r = svc.cls->mfa.list_mfa(ctx->key, params.pdevices, objv_tracker, params.pmtime, null_yield);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWSI_MetaBackend_SObj::put_entry(RGWSI_MetaBackend::Context *_ctx,
                                      RGWSI_MetaBackend::PutParams& _params,
                                      RGWObjVersionTracker *objv_tracker)
{
  RGWSI_MetaBackend_OTP::Context_OTP *ctx = static_cast<RGWSI_MetaBackend_OTP::Context_OTP *>(_ctx);
  RGWSI_MBOTP_PutParams& params = static_cast<RGWSI_MBOTP_PutParams&>(_params);

  return svc.cls->mfa.set_mfa(ctx->key, params.devices, true, &objv_tracker, mtime, null_yield);
}

