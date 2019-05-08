
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

int RGWSI_MetaBackend_SObj::list_init(const string& marker, void **phandle) override {
    auto info = std::make_unique<list_keys_info>();

    int ret = store->list_raw_objects_init(store->svc.zone->get_zone_params().domain_root, marker,
                                           &info->ctx);
    if (ret < 0) {
      return ret;
    }
    *phandle = (void *)info.release();

    return 0;
  }

int RGWSI_MetaBackend_SObj::list_next(void *handle, int max, list<string>& keys, bool *truncated) override {
  list_keys_info *info = static_cast<list_keys_info *>(handle);

  string no_filter;

  keys.clear();

  list<string> unfiltered_keys;

  int ret = store->list_raw_objects_next(no_filter, max, info->ctx,
                                         unfiltered_keys, truncated);
  if (ret < 0 && ret != -ENOENT)
    return ret;
  if (ret == -ENOENT) {
    if (truncated)
      *truncated = false;
    return 0;
  }

  constexpr int prefix_size = sizeof(RGW_BUCKET_INSTANCE_MD_PREFIX) - 1;
  // now filter in the relevant entries
  list<string>::iterator iter;
  for (iter = unfiltered_keys.begin(); iter != unfiltered_keys.end(); ++iter) {
    string& k = *iter;

    if (k.compare(0, prefix_size, RGW_BUCKET_INSTANCE_MD_PREFIX) == 0) {
      auto oid = k.substr(prefix_size);
      rgw_bucket_instance_oid_to_key(oid);
      keys.emplace_back(std::move(oid));
    }
  }

  return 0;
}

void RGWSI_MetaBackend_SObj::list_complete(void *handle) override {
  list_keys_info *info = static_cast<list_keys_info *>(handle);
  delete info;
}

string get_marker(void *handle) override {
  list_keys_info *info = static_cast<list_keys_info *>(handle);
  return info->store->list_raw_objs_get_cursor(info->ctx);
}

