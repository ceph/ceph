
#include "svc_meta_be_sobj.h"

#include "rgw/rgw_tools.h"
#include "rgw/rgw_metadata.h"
#include "rgw/rgw_mdlog.h"

#define dout_subsys ceph_subsys_rgw


RGWSI_MetaBackend_SObj::RGWSI_MetaBackend_SObj(CephContext *cct) : RGWSI_MetaBackend(cct) {
}

RGWSI_MetaBackend_SObj::~RGWSI_MetaBackend_SObj() {
}

int RGWSI_MetaBackend_SObj::init_handler(RGWMetadataHandler *handler, RGWSI_MetaBackend_Handle *phandle)
{
  const auto& section = handler->get_type();

  auto& info = handlers[section];
  info.section = section;
  info._module = handler->get_be_module();
  info.module = static_cast<RGWSI_MBSObj_Handler_Module *>(info._module.get());
  
  *phandle = (RGWSI_MetaBackend_Handle)(&info);

  return 0;
}

void RGWSI_MetaBackend_SObj::init_ctx(RGWSI_MetaBackend_Handle handle, const string& key, RGWMetadataObject *obj, RGWSI_MetaBackend::Context *_ctx)
{
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);
  rgwsi_meta_be_sobj_handler_info *h = static_cast<rgwsi_meta_be_sobj_handler_info *>(handle);

  ctx->handle = handle;
  ctx->module = h->module;
  ctx->section = h->section;
  ctx->key = key;
  ctx->obj = obj;
  ctx->obj_ctx.emplace(sysobj_svc->init_obj_ctx());
  static_cast<RGWSI_MBSObj_Handler_Module *>(ctx->module)->get_pool_and_oid(key, ctx->pool, ctx->oid);
}

RGWSI_MetaBackend::GetParams *RGWSI_MetaBackend_SObj::alloc_default_get_params(ceph::real_time *pmtime)
{
  auto params = new RGWSI_MBSObj_GetParams;
  params->pmtime = pmtime;
  params->_bl = bufferlist();
  params->pbl = &(*params->_bl);
  return params;
}

int RGWSI_MetaBackend_SObj::get_entry(RGWSI_MetaBackend::Context *_ctx,
                                      GetParams& _params,
                                      RGWObjVersionTracker *objv_tracker)
{
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);
  RGWSI_MBSObj_GetParams& params = static_cast<RGWSI_MBSObj_GetParams&>(_params);

  return rgw_get_system_obj(*ctx->obj_ctx, ctx->pool, ctx->oid, *params.pbl,
                            objv_tracker, params.pmtime,
                            params.y,
                            params.pattrs, params.cache_info,
                            params.refresh_version);
}

int RGWSI_MetaBackend_SObj::put_entry(RGWSI_MetaBackend::Context *_ctx,
                                      PutParams& _params,
                                      RGWObjVersionTracker *objv_tracker)
{
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);
  RGWSI_MBSObj_PutParams& params = static_cast<RGWSI_MBSObj_PutParams&>(_params);

  return rgw_put_system_obj(*ctx->obj_ctx, ctx->pool, ctx->oid, params.bl, params.exclusive,
                            objv_tracker, params.mtime, params.pattrs);
}

int RGWSI_MetaBackend_SObj::remove_entry(RGWSI_MetaBackend::Context *_ctx,
                                         RemoveParams& params,
                                         RGWObjVersionTracker *objv_tracker)
{
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);

  rgw_raw_obj k(ctx->pool, ctx->oid);

  auto sysobj = ctx->obj_ctx->get_obj(k);
  return sysobj.wop()
               .set_objv_tracker(objv_tracker)
               .remove(null_yield);
}

