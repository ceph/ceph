
#include "svc_meta_be_sobj.h"

#include "rgw/rgw_tools.h"
#include "rgw/rgw_metadata.h"
#include "rgw/rgw_mdlog.h"

struct rgwsi_meta_be_sobj_handler_info {
  RGWSI_MetaBackend::ModuleRef _module;
  RGWSI_MBSObj_Handler_Module *module;
  string section;
};

RGWSI_MetaBackend_SObj::RGWSI_MetaBackend_SObj(CephContext *cct) : RGWSI_MetaBackend(cct) {
}

RGWSI_MetaBackend_SObj::~RGWSI_MetaBackend_SObj() {
}

int RGWSI_MetaBackend_SObj::init_handler(RGWMetadataHandler *handler, RGWSI_MetaBackend_Handle *phandle)
{
  const auto& section = handler->get_type();

  auto& info = handlers[handler->get_type()];
  info.section = section;

  info._module = handler->get_backend_module(get_type());
  info.module = static_cast<RGWSI_MBSObj_Handler_Module *>(info._module.get());
  
  *phandle = (RGWSI_MetaBackend_Handle)(&info);

  return 0;
}

void RGWSI_MetaBackend_SObj::init_ctx(RGWSI_MetaBackend_Handle handle, const string& key, RGWSI_MetaBackend::Context *_ctx)
{
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);
  rgwsi_meta_be_sobj_handler_info *h = static_cast<rgwsi_meta_be_sobj_handler_info *>(ctx->handle);

  ctx->handle = handle;
  ctx->module = h->module;
  ctx->section = h->section;
  ctx->key = key;
  ctx->obj_ctx.emplace(sysobj_svc->init_obj_ctx());
  static_cast<RGWSI_MBSObj_Handler_Module *>(ctx->module)->get_pool_and_oid(key, ctx->pool, ctx->oid);
}

int RGWSI_MetaBackend_SObj::get_entry(RGWSI_MetaBackend::Context *_ctx,
                                      bufferlist *pbl,
                                      RGWObjVersionTracker *objv_tracker,
                                      real_time *pmtime,
                                      map<string, bufferlist> *pattrs,
                                      rgw_cache_entry_info *cache_info,
                                      boost::optional<obj_version> refresh_version)
{
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);

  return rgw_get_system_obj(*ctx->obj_ctx, ctx->pool, ctx->oid, *pbl,
                            objv_tracker, pmtime,
                            pattrs, cache_info,
                            refresh_version);
}

int RGWSI_MetaBackend_SObj::put_entry(RGWSI_MetaBackend::Context *_ctx,
                                      bufferlist& bl, bool exclusive,
                                      RGWObjVersionTracker *objv_tracker,
                                      real_time mtime, map<string, bufferlist> *pattrs)
{
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);

  return rgw_put_system_obj(*ctx->obj_ctx, ctx->pool, ctx->oid, bl, exclusive,
                            objv_tracker, mtime, pattrs);
}

int RGWSI_MetaBackend_SObj::remove_entry(RGWSI_MetaBackend::Context *_ctx,
                                         RGWObjVersionTracker *objv_tracker)
{
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);

  rgw_raw_obj k(ctx->pool, ctx->oid);

  auto sysobj = ctx->obj_ctx->get_obj(k);
  return sysobj.wop()
               .set_objv_tracker(objv_tracker)
               .remove();
}

