
#include "svc_meta_be_sobj.h"

#include "rgw/rgw_tools.h"
#include "rgw/rgw_metadata.h"
#include "rgw/rgw_mdlog.h"

#define dout_subsys ceph_subsys_rgw


RGWSI_MetaBackend_OTP::RGWSI_MetaBackend_OTP(CephContext *cct) : RGWSI_MetaBackend(cct) {
}

RGWSI_MetaBackend_OTP::~RGWSI_MetaBackend_OTP() {
}

void RGWSI_MetaBackend_OTP::init_ctx(RGWSI_MetaBackend_Handle handle, RGWSI_MetaBackend::Context *_ctx)
{
  RGWSI_MetaBackend_OTP::Context_OTP *ctx = static_cast<RGWSI_MetaBackend_OTP::Context_OTP *>(_ctx);
  rgwsi_meta_be_otp_handler_info *h = static_cast<rgwsi_meta_be_otp_handler_info *>(handle);

  ctx->handle = handle;
  ctx->module = h->module;
  ctx->section = h->section;
  ctx->obj_ctx.emplace(sysobj_svc->init_obj_ctx());
  static_cast<RGWSI_MBOTP_Handler_Module *>(ctx->module)->get_pool_and_oid(key, ctx->pool, ctx->oid);
}

RGWSI_MetaBackend::GetParams *RGWSI_MetaBackend_OTP::alloc_default_get_params(ceph::real_time *pmtime)
{
  auto params = new RGWSI_MBOTP_GetParams;
  params->pmtime = pmtime;
  params->_devices = otp_devices_list_t();
  params->pdevices = &(*params->_devices);
  return params;
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

