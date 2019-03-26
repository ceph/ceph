

#include "svc_user.h"
#include "svc_zone.h"
#include "svc_sys_obj.h"
#include "svc_sys_obj_cache.h"
#include "svc_meta.h"
#include "svc_sync_modules.h"

#include "rgw/rgw_user.h"
#include "rgw/rgw_tools.h"
#include "rgw/rgw_zone.h"

#define dout_subsys ceph_subsys_rgw

RGWSI_User::RGWSI_User(CephContext *cct): RGWServiceInstance(cct) {
}

RGWSI_User::~RGWSI_User() {
}

RGWSI_User::User RGWSI_User::user(RGWSysObjectCtx& _ctx,
                                  const rgw_user& _user) {
  return User(this, _ctx, _user);
}

void RGWSI_User::init(RGWSI_Zone *_zone_svc, RGWSI_SysObj *_sysobj_svc,
                        RGWSI_SysObj_Cache *_cache_svc, RGWSI_Meta *_meta_svc,
                        RGWSI_SyncModules *_sync_modules_svc)
{
  svc.user = this;
  svc.zone = _zone_svc;
  svc.sysobj = _sysobj_svc;
  svc.cache = _cache_svc;
  svc.meta = _meta_svc;
  svc.sync_modules = _sync_modules_svc;
}

int RGWSI_User::do_start()
{
  uinfo_cache.reset(new RGWChainedCacheImpl<user_info_cache_entry>);
  uinfo_cache->init(svc.cache);

  auto mm = svc.meta->get_mgr();
  user_meta_handler = RGWUserMetaHandlerAllocator::alloc();

  int r = user_meta_handler->init(mm);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWSI_User::User::read_user_info(const rgw_user& user,
                                     RGWUserInfo *info,
                                     real_time *pmtime,
                                     map<string, bufferlist> *pattrs,
                                     boost::optional<obj_version> refresh_version)
{
#warning FIXME
}

int RGWSI_User::User::write_user_info(RGWUserInfo& info,
                                      bool exclusive,
                                      real_time mtime,
                                      map<string, bufferlist> *pattrs)
{
#warning FIXME
}

int RGWSI_User::User::GetOp::exec()
{
  int r = source.read_user_info(source.user, &source.user_info,
                                pmtime, pattrs, refresh_version);
  if (r < 0) {
    return r;
  }

  if (pinfo) {
    *pinfo = source.user_info;
  }

  return 0;
}

int RGWSI_User::User::SetOp::exec()
{
  int r = source.write_user_info(source.user_info,
                                 exclusive,
                                 mtime, pattrs);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWSI_User::read_user_info(const rgw_user& user,
                               RGWUserInfo *info,
                               real_time *pmtime,
                               RGWObjVersionTracker *objv_tracker,
                               map<string, bufferlist> *pattrs,
                               rgw_cache_entry_info *cache_info)
{
  bufferlist bl;
  RGWUID user_id;

  string key = User::get_meta_key(user);
  RGWSI_MBSObj_GetParams params = {
    .pmtime = pmtime,
    .pattrs = pattrs,
    .pbl = &bl;
  };
  int ret = svc.meta_be->get_entry(ctx, params, objv_tracker);
  if (ret < 0) {
    return ret;
  }

  auto iter = bl.cbegin();
  try {
    decode(user_id, iter);
    if (user_id.user_id.compare(user) != 0) {
      lderr(store->ctx())  << "ERROR: " << __func__ << "(): user id mismatch: " << user_id.user_id << " != " << user << dendl;
      return -EIO;
    }
    if (!iter.end()) {
      decode(info, iter);
    }
  } catch (buffer::error& err) {
    ldout(store->ctx(), 0) << "ERROR: failed to decode user info, caught buffer::error" << dendl;
    return -EIO;
  }

  return 0;
}

int RGWSI_User::store_user_info(RGWUserInfo& user_info, bool exclusive,
                                map<string, bufferlist>& attrs,
                                RGWObjVersionTracker *objv_tracker,
                                real_time mtime)
{
  string entry = User::get_meta_oid(user);
  auto apply_type = (exclusive ? APPLY_EXCLUSIVE : APPLY_ALWAYS);
  RGWUserCompleteInfo bci{user_info, attrs};
  RGWUserMetadataObject mdo(bci, objv_tracker->write_version, mtime);
  return user_meta_handler->mutate(entry, &mdo, *objv_tracker, apply_type);
}

int RGWSI_User::remove_user_info(const rgw_user& user,
                                 RGWObjVersionTracker *objv_tracker)
{
  string entry = User::get_meta_oid(user);
  return user_handler->remove(entry, *objv_tracker);
}


