

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
                        RGWSI_MetaBackend *_meta_be_svc,
                        RGWSI_SyncModules *_sync_modules_svc)
{
  svc.user = this;
  svc.zone = _zone_svc;
  svc.sysobj = _sysobj_svc;
  svc.cache = _cache_svc;
  svc.meta = _meta_svc;
  svc.meta_be = _meta_be_svc;
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

#warning clean me?
#if 0
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
#endif

int RGWSI_User::User::GetOp::exec()
{
  int r = source.svc.user->read_user_info(source.user, &source.user_info,
                                          pmtime, pattrs,
                                          objv_tracker, refresh_version);
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
  int r = source.svc.user->store_user_info(source.user_info,
                                           exclusive,
                                           objv_tracker,
                                           mtime, pattrs);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWSI_User::read_user_info_meta_entry(RGWSI_MetaBackend::Context *ctx,
                                          string& entry,
                                          RGWUserInfo& info,
                                          RGWObjVersionTracker * const objv_tracker,
                                          real_time * const pmtime,
                                          rgw_cache_entry_info * const cache_info,
                                          map<string, bufferlist> * const pattrs) {
  bufferlist bl;
  RGWUID user_id;

  RGWSI_MBSObj_GetParams params = {
    pmtime,
    std::nullopt, /* _bl */
    &bl,
    pattrs,
    nullptr, /* cache_info */
  };
  int ret = svc.meta_be->get_entry(ctx, params, objv_tracker);
  if (ret < 0) {
    return ret;
  }

  auto iter = bl.cbegin();
  try {
    decode(user_id, iter);
    if (User::get_meta_key(user_id.user_id).compare(entry) != 0) {
      lderr(svc.meta_be->ctx())  << "ERROR: rgw_get_user_info_by_uid(): user id mismatch: " << User::get_meta_key(user_id.user_id) << " != " << entry << dendl;
      return -EIO;
    }
    if (!iter.end()) {
      decode(info, iter);
    }
  } catch (buffer::error& err) {
    ldout(svc.meta_be->ctx(), 0) << "ERROR: failed to decode user info, caught buffer::error" << dendl;
    return -EIO;
  }

  return 0;
}

int RGWSI_User::read_user_info(const rgw_user& user,
                               RGWUserInfo *info,
                               real_time *pmtime,
                               map<string, bufferlist> *pattrs,
                               RGWObjVersionTracker *objv_tracker,
                               rgw_cache_entry_info *cache_info)
{
#warning cache?
  string key = User::get_meta_key(user);
  RGWUserMetadataObject *meta;
  int ret = user_meta_handler->get(key, (RGWMetadataObject **)&meta);
  if (ret < 0) {
    return ret;
  }

  auto& uci = meta->get_uci();

  if (info) {
    *info = std::move(uci.info);
  }

  if (pmtime) {
    *pmtime = meta->get_mtime();
  }

  if (objv_tracker) {
    objv_tracker->read_version = meta->get_version();
  }

  delete meta;

  return 0;
}

class RGWMetaPut_User
{
  RGWSI_MetaBackend::Context *ctx;
  RGWUID ui;
  string& entry;
  RGWUserInfo& info;
  RGWUserInfo *old_info;
  RGWObjVersionTracker *objv_tracker;
  real_time& mtime;
  bool exclusive;
  map<string, bufferlist> *pattrs;
  
  RGWMetaPut_User(RGWSI_MetaBackend::Context *ctx,
                  string& entry,
                  RGWUserInfo& info,
                  RGWUserInfo *old_info,
                  RGWObjVersionTracker *objv_tracker,
                  real_time& mtime,
                  bool exclusive,
                  map<string, bufferlist> *pattrs) :
      ctx(ctx), entry(entry),
      info(info), old_info(old_info),
      objv_tracker(objv_tracker), mtime(mtime),
      exclusive(exclusive), pattrs(pattrs) {
    ui.user_id = info.user_id;
  }

  int prepare() {
    RGWObjVersionTracker ot;

    if (objv_tracker) {
      ot = *objv_tracker;
    }

    if (ot.write_version.tag.empty()) {
      if (ot.read_version.tag.empty()) {
        ot.generate_new_write_ver(store->ctx());
      } else {
        ot.write_version = ot.read_version;
        ot.write_version.ver++;
      }
    }

    map<string, RGWAccessKey>::iterator iter;
    for (iter = info.swift_keys.begin(); iter != info.swift_keys.end(); ++iter) {
      if (old_info && old_info->swift_keys.count(iter->first) != 0)
        continue;
      RGWAccessKey& k = iter->second;
      /* check if swift mapping exists */
      RGWUserInfo inf;
#warning this needs to change to use svc.sysobj
      int r = do_get_user_info_by_swift(meta_be, ctx, k.id, inf);
      if (r >= 0 && inf.user_id.compare(info.user_id) != 0) {
        ldout(store->ctx(), 0) << "WARNING: can't store user info, swift id (" << k.id
          << ") already mapped to another user (" << info.user_id << ")" << dendl;
        return -EEXIST;
      }
    }

    if (!info.access_keys.empty()) {
      /* check if access keys already exist */
      RGWUserInfo inf;
      map<string, RGWAccessKey>::iterator iter = info.access_keys.begin();
      for (; iter != info.access_keys.end(); ++iter) {
        RGWAccessKey& k = iter->second;
        if (old_info && old_info->access_keys.count(iter->first) != 0)
          continue;
        int r = do_get_user_info_by_access_key(meta_be, ctx, k.id, inf);
        if (r >= 0 && inf.user_id.compare(info.user_id) != 0) {
          ldout(store->ctx(), 0) << "WARNING: can't store user info, access key already mapped to another user" << dendl;
          return -EEXIST;
        }
      }
    }

    return 0;
  }

  int put() {
    bufferlist data_bl;
    encode(ui, data_bl);
    encode(info, data_bl);

    int ret = svc.meta_be->put_entry(ctx, data_bl, exclusive, &ot, mtime, pattrs);
    if (ret < 0)
      return ret;

    return 0;
  }

  int complete() {
    int ret;

    if (!info.user_email.empty()) {
      if (!old_info ||
          old_info->user_email.compare(info.user_email) != 0) { /* only if new index changed */
        bufferlist link_bl;
        encode(ui, link_bl);

        ret = rgw_put_system_obj(store, store->svc.zone->get_zone_params().user_email_pool, info.user_email,
                                 link_bl, exclusive, NULL, real_time());
        if (ret < 0)
          return ret;
      }
    }

    if (!info.access_keys.empty()) {
      map<string, RGWAccessKey>::iterator iter = info.access_keys.begin();
      for (; iter != info.access_keys.end(); ++iter) {
        RGWAccessKey& k = iter->second;
        if (old_info && old_info->access_keys.count(iter->first) != 0)
          continue;

        ret = rgw_put_system_obj(store, store->svc.zone->get_zone_params().user_keys_pool, k.id,
                                 link_bl, exclusive, NULL, real_time());
        if (ret < 0)
          return ret;
      }
    }

    map<string, RGWAccessKey>::iterator siter;
    for (siter = info.swift_keys.begin(); siter != info.swift_keys.end(); ++siter) {
      RGWAccessKey& k = siter->second;
      if (old_info && old_info->swift_keys.count(siter->first) != 0)
        continue;

      ret = rgw_put_system_obj(store, store->svc.zone->get_zone_params().user_swift_pool, k.id,
                               link_bl, exclusive, NULL, real_time());
      if (ret < 0)
        return ret;
    }

    return 0;
  }
};

int RGWSI_User::store_user_info(RGWUserInfo& user_info, bool exclusive,
                                map<string, bufferlist>& attrs,
                                RGWObjVersionTracker *objv_tracker,
                                real_time mtime)
{
  string entry = User::get_meta_key(user);
  auto apply_type = (exclusive ? APPLY_EXCLUSIVE : APPLY_ALWAYS);
  RGWUserCompleteInfo bci{user_info, attrs};
  RGWUserMetadataObject mdo(bci, objv_tracker->write_version, mtime);
  return user_meta_handler->put(entry, &mdo, *objv_tracker, apply_type);
}

int RGWSI_User::remove_user_info(const rgw_user& user,
                                 RGWObjVersionTracker *objv_tracker)
{
  string entry = User::get_meta_key(user);
  return user_handler->remove(entry, *objv_tracker);
}


