

#include "svc_meta_be.h"
#include "svc_mdlog.h"

#include "rgw/rgw_mdlog.h"

#define dout_subsys ceph_subsys_rgw


RGWSI_MetaBackend::Context::~Context() {} // needed, even though destructor is pure virtual
RGWSI_MetaBackend::Module::~Module() {} // needed, even though destructor is pure virtual

int RGWSI_MetaBackend::pre_modify(RGWSI_MetaBackend::Context *ctx,
                                  RGWMetadataLogData& log_data,
                                  RGWObjVersionTracker *objv_tracker,
                                  RGWMDLogStatus op_type)
{
  /* if write version has not been set, and there's a read version, set it so that we can
   * log it
   */
  if (objv_tracker) {
    if (objv_tracker->read_version.ver && !objv_tracker->write_version.ver) {
      objv_tracker->write_version = objv_tracker->read_version;
      objv_tracker->write_version.ver++;
    }
    log_data.read_version = objv_tracker->read_version;
    log_data.write_version = objv_tracker->write_version;
  }

  log_data.status = op_type;

  bufferlist logbl;
  encode(log_data, logbl);

  int ret = mdlog_svc->add_entry(ctx->module, ctx->section, ctx->key, logbl);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWSI_MetaBackend::post_modify(RGWSI_MetaBackend::Context *ctx,
                                   RGWMetadataLogData& log_data,
                                   RGWObjVersionTracker *objv_tracker, int ret)
{
  if (ret >= 0)
    log_data.status = MDLOG_STATUS_COMPLETE;
  else 
    log_data.status = MDLOG_STATUS_ABORT;

  bufferlist logbl;
  encode(log_data, logbl);

  int r = mdlog_svc->add_entry(ctx->module, ctx->section, ctx->key, logbl);
  if (ret < 0)
    return ret;

  if (r < 0)
    return r;

  return 0;
}

int RGWSI_MetaBackend::prepare_mutate(RGWSI_MetaBackend::Context *ctx,
                                      const real_time& mtime,
                                      RGWObjVersionTracker *objv_tracker,
                                      RGWMDLogSyncType sync_mode)
{
  bufferlist bl;
  real_time orig_mtime;
  int ret = get_entry(ctx, &bl, objv_tracker, &orig_mtime,
                      nullptr, nullptr, boost::none);
  if (ret < 0 && ret != -ENOENT) {
    return ret;
  }
  if (ret != -ENOENT &&
      !RGWMetadataHandler::check_versions(objv_tracker->read_version, orig_mtime,
                                          objv_tracker->write_version, mtime, sync_mode)) {
    return STATUS_NO_APPLY;
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

int RGWSI_MetaBackend::mutate(RGWSI_MetaBackend::Context *ctx,
                              const ceph::real_time& mtime,
                              RGWObjVersionTracker *objv_tracker,
                              RGWMDLogStatus op_type,
                              RGWMDLogSyncType sync_mode,
                              std::function<int()> f,
                              bool generic_prepare)
{
  int ret;

  if (generic_prepare) {
    ret = prepare_mutate(ctx, mtime, objv_tracker, sync_mode);
    if (ret < 0 ||
        ret == STATUS_NO_APPLY) {
      return ret;
    }
  }

  RGWMetadataLogData log_data;
  ret = pre_modify(ctx, log_data, objv_tracker, op_type);
  if (ret < 0) {
    return ret;
  }

  ret = f();

  /* cascading ret into post_modify() */

  ret = post_modify(ctx, log_data, objv_tracker, ret);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWSI_MetaBackend::get(Context *ctx,
                           bufferlist *pbl,
                           RGWObjVersionTracker *objv_tracker,
                           real_time *pmtime,
                           map<string, bufferlist> *pattrs,
                           rgw_cache_entry_info *cache_info,
                           boost::optional<obj_version> refresh_version)
{
  return get_entry(ctx, pbl,
                   objv_tracker, pmtime,
                   pattrs,
                   cache_info,
                   refresh_version);
}

int RGWSI_MetaBackend::put(Context *ctx,
                           bufferlist& bl,
                           bool exclusive,
                           RGWObjVersionTracker *objv_tracker,
                           const ceph::real_time& mtime,
                           map<string, bufferlist> *pattrs,
                           RGWMDLogSyncType sync_mode)
{
  std::function<int()> f = [&]() {
    return put_entry(ctx, bl,
                     exclusive, objv_tracker,
                     mtime, pattrs);
  };

  return mutate(ctx, mtime, objv_tracker,
                MDLOG_STATUS_WRITE, sync_mode,
                f,
                false);
}

int RGWSI_MetaBackend::remove(Context *ctx,
                              RGWObjVersionTracker *objv_tracker,
                              const ceph::real_time& mtime,
                              RGWMDLogSyncType sync_mode)
{
  std::function<int()> f = [&]() {
    return remove_entry(ctx, objv_tracker);
  };

  return mutate(ctx, mtime, objv_tracker,
                MDLOG_STATUS_REMOVE, sync_mode,
                f,
                false);
}
