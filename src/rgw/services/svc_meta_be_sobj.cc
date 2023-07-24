// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "svc_meta_be_sobj.h"
#include "svc_meta_be_params.h"
#include "svc_mdlog.h"

#include "rgw_tools.h"
#include "rgw_metadata.h"
#include "rgw_mdlog.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

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
  return new Context_SObj;
}

int RGWSI_MetaBackend_SObj::pre_modify(const DoutPrefixProvider *dpp, RGWSI_MetaBackend::Context *_ctx,
                                       const string& key,
                                       RGWMetadataLogData& log_data,
                                       RGWObjVersionTracker *objv_tracker,
                                       RGWMDLogStatus op_type,
                                       optional_yield y)
{
  auto ctx = static_cast<Context_SObj *>(_ctx);
  int ret = RGWSI_MetaBackend::pre_modify(dpp, ctx, key, log_data,
                                          objv_tracker, op_type,
                                          y);
  if (ret < 0) {
    return ret;
  }

  /* if write version has not been set, and there's a read version, set it so that we can
   * log it
   */
  if (objv_tracker) {
    log_data.read_version = objv_tracker->read_version;
    log_data.write_version = objv_tracker->write_version;
  }

  log_data.status = op_type;

  bufferlist logbl;
  encode(log_data, logbl);

  ret = mdlog_svc->add_entry(dpp, ctx->module->get_hash_key(key), ctx->module->get_section(), key, logbl, y);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWSI_MetaBackend_SObj::post_modify(const DoutPrefixProvider *dpp, 
                                        RGWSI_MetaBackend::Context *_ctx,
                                        const string& key,
                                        RGWMetadataLogData& log_data,
                                        RGWObjVersionTracker *objv_tracker, int ret,
                                        optional_yield y)
{
  auto ctx = static_cast<Context_SObj *>(_ctx);
  if (ret >= 0)
    log_data.status = MDLOG_STATUS_COMPLETE;
  else 
    log_data.status = MDLOG_STATUS_ABORT;

  bufferlist logbl;
  encode(log_data, logbl);

  int r = mdlog_svc->add_entry(dpp, ctx->module->get_hash_key(key), ctx->module->get_section(), key, logbl, y);
  if (ret < 0)
    return ret;

  if (r < 0)
    return r;

  return RGWSI_MetaBackend::post_modify(dpp, ctx, key, log_data, objv_tracker, ret, y);
}

int RGWSI_MetaBackend_SObj::get_shard_id(RGWSI_MetaBackend::Context *_ctx,
					 const std::string& key,
					 int *shard_id)
{
  auto ctx = static_cast<Context_SObj *>(_ctx);
  *shard_id = mdlog_svc->get_shard_id(ctx->module->get_hash_key(key), shard_id);
  return 0;
}

int RGWSI_MetaBackend_SObj::call(std::optional<RGWSI_MetaBackend_CtxParams> opt,
                                 std::function<int(RGWSI_MetaBackend::Context *)> f)
{
  RGWSI_MetaBackend_SObj::Context_SObj ctx;
  return f(&ctx);
}

void RGWSI_MetaBackend_SObj::Context_SObj::init(RGWSI_MetaBackend_Handler *h)
{
  RGWSI_MetaBackend_Handler_SObj *handler = static_cast<RGWSI_MetaBackend_Handler_SObj *>(h);
  module = handler->module;
}

int RGWSI_MetaBackend_SObj::call_with_get_params(ceph::real_time *pmtime, std::function<int(RGWSI_MetaBackend::GetParams&)> cb)
{
  bufferlist bl;
  RGWSI_MBSObj_GetParams params;
  params.pmtime = pmtime;
  params.pbl = &bl;
  return cb(params);
}

int RGWSI_MetaBackend_SObj::get_entry(RGWSI_MetaBackend::Context *_ctx,
                                      const string& key,
                                      GetParams& _params,
                                      RGWObjVersionTracker *objv_tracker,
                                      optional_yield y,
                                      const DoutPrefixProvider *dpp,
                                      bool get_raw_attrs)
{
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);
  RGWSI_MBSObj_GetParams& params = static_cast<RGWSI_MBSObj_GetParams&>(_params);

  rgw_pool pool;
  string oid;
  ctx->module->get_pool_and_oid(key, &pool, &oid);

  int ret = 0;
  ret = rgw_get_system_obj(sysobj_svc, pool, oid, *params.pbl,
                            objv_tracker, params.pmtime,
                            y, dpp,
                            params.pattrs, params.cache_info,
                            params.refresh_version, get_raw_attrs);

  return ret;
}

int RGWSI_MetaBackend_SObj::put_entry(const DoutPrefixProvider *dpp, 
                                      RGWSI_MetaBackend::Context *_ctx,
                                      const string& key,
                                      PutParams& _params,
                                      RGWObjVersionTracker *objv_tracker,
                                      optional_yield y)
{
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);
  RGWSI_MBSObj_PutParams& params = static_cast<RGWSI_MBSObj_PutParams&>(_params);

  rgw_pool pool;
  string oid;
  ctx->module->get_pool_and_oid(key, &pool, &oid);

  return rgw_put_system_obj(dpp, sysobj_svc, pool, oid, params.bl, params.exclusive,
                            objv_tracker, params.mtime, y, params.pattrs);
}

int RGWSI_MetaBackend_SObj::remove_entry(const DoutPrefixProvider *dpp, 
                                         RGWSI_MetaBackend::Context *_ctx,
                                         const string& key,
                                         RemoveParams& params,
                                         RGWObjVersionTracker *objv_tracker,
                                         optional_yield y)
{
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);

  rgw_pool pool;
  string oid;
  ctx->module->get_pool_and_oid(key, &pool, &oid);
  rgw_raw_obj k(pool, oid);

  auto sysobj = sysobj_svc->get_obj(k);
  return sysobj.wop()
               .set_objv_tracker(objv_tracker)
               .remove(dpp, y);
}

int RGWSI_MetaBackend_SObj::list_init(const DoutPrefixProvider *dpp,
                                      RGWSI_MetaBackend::Context *_ctx,
                                      const string& marker)
{
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);

  rgw_pool pool;

  string no_key;
  ctx->module->get_pool_and_oid(no_key, &pool, nullptr);

  ctx->list.pool = sysobj_svc->get_pool(pool);
  ctx->list.op.emplace(ctx->list.pool->op());

  string prefix = ctx->module->get_oid_prefix();
  ctx->list.op->init(dpp, marker, prefix);

  return 0;
}

int RGWSI_MetaBackend_SObj::list_next(const DoutPrefixProvider *dpp,
                                      RGWSI_MetaBackend::Context *_ctx,
                                      int max, list<string> *keys,
                                      bool *truncated)
{
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);

  vector<string> oids;

  keys->clear();

  int ret = ctx->list.op->get_next(dpp, max, &oids, truncated);
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

