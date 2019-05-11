
#include "svc_meta_be_sobj.h"

#include "rgw/rgw_tools.h"
#include "rgw/rgw_metadata.h"
#include "rgw/rgw_mdlog.h"

#define dout_subsys ceph_subsys_rgw


RGWSI_MetaBackend_SObj::RGWSI_MetaBackend_SObj(CephContext *cct) : RGWSI_MetaBackend(cct) {
}

RGWSI_MetaBackend_SObj::~RGWSI_MetaBackend_SObj() {
}

RGWSI_MetaBackend_Handler *RGWSI_MetaBackend_SObj::alloc_be_handler()
{
  return new RGWSI_MetaBackend_Handler_SObj(this);
}

RGWSI_MetaBackend::Context *RGWSI_MetaBackend_SObj::alloc_ctx()
{
  return new ctx(sysobj_svc);
}

int RGWSI_MetaBackend_SObj::call(std::function<int(RGWSI_MetaBackend::Context *)> f)
{
  RGWSI_MetaBackend_SObj::Context_SObj ctx(sysobj_svc);
  return f(&ctx);
}

void RGWSI_MetaBackend_SObj::Context_SObj::init(RGWSI_MetaBackend_Handler *h)
{
  RGWSI_MetaBackend_Handler_SObj *handler = static_cast<RGWSI_MetaBackend_Handler_SObj *>(h);
  module = handler->module;
  obj_ctx.emplace(sysobj_svc->init_obj_ctx());
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
                                      const string& key,
                                      GetParams& _params,
                                      RGWObjVersionTracker *objv_tracker)
{
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);
  RGWSI_MBSObj_GetParams& params = static_cast<RGWSI_MBSObj_GetParams&>(_params);

  rgw_pool pool;
  string oid;
  ctx->module->get_pool_and_oid(key, &pool, &oid);

  return rgw_get_system_obj(*ctx->obj_ctx, pool, oid, *params.pbl,
                            objv_tracker, params.pmtime,
                            params.y,
                            params.pattrs, params.cache_info,
                            params.refresh_version);
}

int RGWSI_MetaBackend_SObj::put_entry(RGWSI_MetaBackend::Context *_ctx,
                                      const string& key,
                                      PutParams& _params,
                                      RGWObjVersionTracker *objv_tracker)
{
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);
  RGWSI_MBSObj_PutParams& params = static_cast<RGWSI_MBSObj_PutParams&>(_params);

  rgw_pool pool;
  string oid;
  ctx->module->get_pool_and_oid(key, &pool, &oid);

  return rgw_put_system_obj(*ctx->obj_ctx, pool, oid, params.bl, params.exclusive,
                            objv_tracker, params.mtime, params.y, params.pattrs);
}

int RGWSI_MetaBackend_SObj::remove_entry(RGWSI_MetaBackend::Context *_ctx,
                                         const string& key,
                                         RemoveParams& params,
                                         RGWObjVersionTracker *objv_tracker)
{
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);

  rgw_pool pool;
  string oid;
  ctx->module->get_pool_and_oid(key, &pool, &oid);
  rgw_raw_obj k(pool, oid);

  auto sysobj = ctx->obj_ctx->get_obj(k);
  return sysobj.wop()
               .set_objv_tracker(objv_tracker)
               .remove(params.y);
}

int RGWSI_MetaBackend_SObj::list_init(RGWSI_MetaBackend::Context *_ctx,
                                      const string& marker)
{
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);

  rgw_pool pool;

  string no_key;
  ctx->module->get_pool_and_oid(no_key, &pool, nullptr);

  ctx->list.pool = sysobj_svc->get_pool(pool);
  ctx->list.op.emplace(ctx->list.pool->op());

  string prefix = ctx->module->get_prefix();
  ctx->list.op->init(marker, prefix);

  return 0;
}

int RGWSI_MetaBackend_SObj::list_next(RGWSI_MetaBackend::Context *_ctx,
                                      int max, list<string> *keys,
                                      bool *truncated)
{
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);

  vector<string> oids;

  keys->clear();

  int ret = ctx->list.op->get_next(max, &oids, truncated);
  if (ret < 0 && ret != -ENOENT)
    return ret;
  if (ret == -ENOENT) {
    if (truncated)
      *truncated = false;
    return 0;
  }

  auto module = ctx->module;

  for (auto& o : oids) {
    if (!module->is_valid_oid(o)) {
      continue;
    }
    keys->emplace_back(module->oid_to_key(o));
  }

  return 0;
}

int RGWSI_MetaBackend_SObj::list_get_marker(RGWSI_MetaBackend::Context *_ctx,
                                            string *marker)
{
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);

  return ctx->list.op->get_marker(marker);
}

