// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp


#include "svc_meta_be.h"

#include "rgw/rgw_mdlog.h"

#define dout_subsys ceph_subsys_rgw


RGWSI_MetaBackend::Context::~Context() {} // needed, even though destructor is pure virtual
RGWSI_MetaBackend::Module::~Module() {} // ditto
RGWSI_MetaBackend::PutParams::~PutParams() {} // ...
RGWSI_MetaBackend::GetParams::~GetParams() {} // ...
RGWSI_MetaBackend::RemoveParams::~RemoveParams() {} // ...

int RGWSI_MetaBackend::pre_modify(const DoutPrefixProvider *dpp, 
                                  RGWSI_MetaBackend::Context *ctx,
                                  const string& key,
                                  RGWMetadataLogData& log_data,
                                  RGWObjVersionTracker *objv_tracker,
                                  RGWMDLogStatus op_type,
                                  optional_yield y)
{
  /* if write version has not been set, and there's a read version, set it so that we can
   * log it
   */
  if (objv_tracker &&
      objv_tracker->read_version.ver && !objv_tracker->write_version.ver) {
    objv_tracker->write_version = objv_tracker->read_version;
    objv_tracker->write_version.ver++;
  }

  return 0;
}

int RGWSI_MetaBackend::post_modify(const DoutPrefixProvider *dpp, 
                                   RGWSI_MetaBackend::Context *ctx,
                                   const string& key,
                                   RGWMetadataLogData& log_data,
                                   RGWObjVersionTracker *objv_tracker, int ret,
                                   optional_yield y)
{
  return ret;
}

int RGWSI_MetaBackend::prepare_mutate(RGWSI_MetaBackend::Context *ctx,
                                      const string& key,
                                      const real_time& mtime,
                                      RGWObjVersionTracker *objv_tracker,
                                      optional_yield y,
                                      const DoutPrefixProvider *dpp)
{
  real_time orig_mtime;

  int ret = call_with_get_params(&orig_mtime, [&](GetParams& params) {
    return get_entry(ctx, key, params, objv_tracker, y, dpp);
  });
  if (ret < 0 && ret != -ENOENT) {
    return ret;
  }

  if (objv_tracker->write_version.tag.empty()) {
    if (objv_tracker->read_version.tag.empty()) {
      objv_tracker->generate_new_write_ver(cct);
    } else {
      objv_tracker->write_version = objv_tracker->read_version;
      objv_tracker->write_version.ver++;
    }
  }
  return 0;
}

int RGWSI_MetaBackend::do_mutate(RGWSI_MetaBackend::Context *ctx,
				 const string& key,
				 const ceph::real_time& mtime,
				 RGWObjVersionTracker *objv_tracker,
				 RGWMDLogStatus op_type,
                                 optional_yield y,
				 std::function<int()> f,
				 bool generic_prepare,
                                 const DoutPrefixProvider *dpp)
{
  int ret;

  if (generic_prepare) {
    ret = prepare_mutate(ctx, key, mtime, objv_tracker, y, dpp);
    if (ret < 0 ||
	ret == STATUS_NO_APPLY) {
      return ret;
    }
  }

  RGWMetadataLogData log_data;
  ret = pre_modify(dpp, ctx, key, log_data, objv_tracker, op_type, y);
  if (ret < 0) {
    return ret;
  }

  ret = f();

  /* cascading ret into post_modify() */

  ret = post_modify(dpp, ctx, key, log_data, objv_tracker, ret, y);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWSI_MetaBackend::get(Context *ctx,
			   const string& key,
			   GetParams& params,
			   RGWObjVersionTracker *objv_tracker,
                           optional_yield y,
                           const DoutPrefixProvider *dpp)
{
  return get_entry(ctx, key, params, objv_tracker, y, dpp);
}

int RGWSI_MetaBackend::put(Context *ctx,
			   const string& key,
			   PutParams& params,
			   RGWObjVersionTracker *objv_tracker,
                           optional_yield y,
                           const DoutPrefixProvider *dpp)
{
  std::function<int()> f = [&]() {
    return put_entry(dpp, ctx, key, params, objv_tracker, y);
  };

  return do_mutate(ctx, key, params.mtime, objv_tracker,
                MDLOG_STATUS_WRITE,
                y,
                f,
                false,
                dpp);
}

int RGWSI_MetaBackend::remove(Context *ctx,
                              const string& key,
                              RemoveParams& params,
                              RGWObjVersionTracker *objv_tracker,
                              optional_yield y,
                              const DoutPrefixProvider *dpp)
{
  std::function<int()> f = [&]() {
    return remove_entry(dpp, ctx, key, params, objv_tracker, y);
  };

  return do_mutate(ctx, key, params.mtime, objv_tracker,
                MDLOG_STATUS_REMOVE,
                y,
                f,
                false,
                dpp);
}

int RGWSI_MetaBackend::mutate(Context *ctx,
			      const std::string& key,
			      MutateParams& params,
			      RGWObjVersionTracker *objv_tracker,
                              optional_yield y,
			      std::function<int()> f,
                              const DoutPrefixProvider *dpp)
{
  return do_mutate(ctx, key, params.mtime, objv_tracker,
		   params.op_type, y,
		   f,
		   false,
                   dpp);
}

int RGWSI_MetaBackend_Handler::call(std::optional<RGWSI_MetaBackend_CtxParams> bectx_params,
                                    std::function<int(Op *)> f)
{
  return be->call(bectx_params, [&](RGWSI_MetaBackend::Context *ctx) {
    ctx->init(this);
    Op op(be, ctx);
    return f(&op);
  });
}

RGWSI_MetaBackend_Handler::Op_ManagedCtx::Op_ManagedCtx(RGWSI_MetaBackend_Handler *handler) : Op(handler->be, handler->be->alloc_ctx())
{
  auto c = ctx();
  c->init(handler);
  pctx.reset(c);
}

