// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp


#include "svc_meta_be.h"

#include "rgw/rgw_error_code.h"
#include "rgw/rgw_mdlog.h"

#define dout_subsys ceph_subsys_rgw


RGWSI_MetaBackend::Context::~Context() {} // needed, even though destructor is pure virtual
RGWSI_MetaBackend::Module::~Module() {} // ditto
RGWSI_MetaBackend::PutParams::~PutParams() {} // ...
RGWSI_MetaBackend::GetParams::~GetParams() {} // ...
RGWSI_MetaBackend::RemoveParams::~RemoveParams() {} // ...

boost::system::error_code
RGWSI_MetaBackend::pre_modify(RGWSI_MetaBackend::Context *ctx,
			      const string& key,
			      RGWMetadataLogData& log_data,
			      RGWObjVersionTracker *objv_tracker,
			      RGWMDLogStatus op_type,
			      optional_yield y)
{
  /* if write version has not been set, and there's a read version,
   * set it so that we can log it
   */
  if (objv_tracker &&
      objv_tracker->read_version.ver && !objv_tracker->write_version.ver) {
    objv_tracker->write_version = objv_tracker->read_version;
    objv_tracker->write_version.ver++;
  }

  return {};
}

boost::system::error_code RGWSI_MetaBackend::post_modify(
  RGWSI_MetaBackend::Context *ctx, const string& key,
  RGWMetadataLogData& log_data, RGWObjVersionTracker *objv_tracker,
  boost::system::error_code ret,
  optional_yield y)
{
  return ret;
}

boost::system::error_code RGWSI_MetaBackend::prepare_mutate(RGWSI_MetaBackend::Context *ctx,
                                      const string& key,
                                      const real_time& mtime,
                                      RGWObjVersionTracker *objv_tracker,
                                      optional_yield y)
{
  real_time orig_mtime;

  auto ec = call_with_get_params(&orig_mtime, [&](GetParams& params) {
    return get_entry(ctx, key, params, objv_tracker, y);
  });
  if (ec && ec != boost::system::errc::no_such_file_or_directory) {
    return ec;
  }

  if (objv_tracker->write_version.tag.empty()) {
    if (objv_tracker->read_version.tag.empty()) {
      objv_tracker->generate_new_write_ver(cct);
    } else {
      objv_tracker->write_version = objv_tracker->read_version;
      objv_tracker->write_version.ver++;
    }
  }
  return {};
}

boost::system::error_code
RGWSI_MetaBackend::do_mutate(RGWSI_MetaBackend::Context *ctx,
			     const string& key,
			     const ceph::real_time& mtime,
			     RGWObjVersionTracker *objv_tracker,
			     RGWMDLogStatus op_type,
			     optional_yield y,
			     std::function<boost::system::error_code()> f,
			     bool generic_prepare)
{
  boost::system::error_code ret;

  if (generic_prepare) {
    ret = prepare_mutate(ctx, key, mtime, objv_tracker, y);
    if (ret || ret == rgw_errc::no_apply) {
      return ret;
    }
  }

  RGWMetadataLogData log_data;
  ret = pre_modify(ctx, key, log_data, objv_tracker, op_type, y);
  if (ret) {
    return ret;
  }

  ret = f();

  /* cascading ret into post_modify() */

  return post_modify(ctx, key, log_data, objv_tracker, ret, y);
}

boost::system::error_code RGWSI_MetaBackend::get(Context *ctx,
			   const string& key,
			   GetParams& params,
			   RGWObjVersionTracker *objv_tracker,
                           optional_yield y)
{
  return get_entry(ctx, key, params, objv_tracker, y);
}

boost::system::error_code RGWSI_MetaBackend::put(Context *ctx,
			   const string& key,
			   PutParams& params,
			   RGWObjVersionTracker *objv_tracker,
                           optional_yield y)
{
  std::function<boost::system::error_code()> f = [&]() {
    return put_entry(ctx, key, params, objv_tracker, y);
  };

  return do_mutate(ctx, key, params.mtime, objv_tracker,
                MDLOG_STATUS_WRITE,
                y,
                f,
                false);
}

boost::system::error_code RGWSI_MetaBackend::remove(Context *ctx,
                              const string& key,
                              RemoveParams& params,
                              RGWObjVersionTracker *objv_tracker,
                              optional_yield y)
{
  std::function<boost::system::error_code()> f = [&]() {
    return remove_entry(ctx, key, params, objv_tracker, y);
  };

  return do_mutate(ctx, key, params.mtime, objv_tracker,
                MDLOG_STATUS_REMOVE,
                y,
                f,
                false);
}

boost::system::error_code RGWSI_MetaBackend::mutate(Context *ctx,
			      const std::string& key,
			      MutateParams& params,
			      RGWObjVersionTracker *objv_tracker,
                              optional_yield y,
			      std::function<boost::system::error_code()> f)
{
  return do_mutate(ctx, key, params.mtime, objv_tracker,
		   params.op_type, y,
		   f,
		   false);
}

boost::system::error_code RGWSI_MetaBackend_Handler::call(
  std::optional<RGWSI_MetaBackend_CtxParams> bectx_params,
  std::function<boost::system::error_code(Op *)> f)
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
