

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
  return 0;
}

int RGWSI_User::read_user_info(RGWSI_MetaBackend::Context *ctx,
                               RGWUserInfo *info,
                               RGWObjVersionTracker * const objv_tracker,
                               real_time * const pmtime,
                               rgw_cache_entry_info * const cache_info,
                               map<string, bufferlist> * const pattrs,
                               optional_yield y)
{
#warning cache_info?
  bufferlist bl;
  RGWUID user_id;

  RGWSI_MBSObj_GetParams params(&bl, pattrs, pmtime);

  int ret = svc.meta_be->get_entry(ctx, params, objv_tracker, y);
  if (ret < 0) {
    return ret;
  }

  auto iter = bl.cbegin();
  try {
    decode(user_id, iter);
    auto meta_key = get_meta_key(user_id.user_id);
    if (meta_key != ctx->key) {
      lderr(svc.meta_be->ctx())  << "ERROR: rgw_get_user_info_by_uid(): user id mismatch: " << meta_key << " != " << ctx->key << dendl;
      return -EIO;
    }
    if (!iter.end()) {
      decode(*info, iter);
    }
  } catch (buffer::error& err) {
    ldout(svc.meta_be->ctx(), 0) << "ERROR: failed to decode user info, caught buffer::error" << dendl;
    return -EIO;
  }

  return 0;
}

class PutOperation
{
  RGWSI_User::Svc& svc;
  RGWSI_MetaBackend_SObj::Context_SObj *ctx;
  RGWUID ui;
  RGWUserInfo& info;
  RGWUserInfo *old_info;
  RGWObjVersionTracker *objv_tracker;
  real_time& mtime;
  bool exclusive;
  map<string, bufferlist> *pattrs;
  RGWObjVersionTracker ot;
  string err_msg;
  optional_yield y;

  void set_err_msg(string msg) {
    if (!err_msg.empty()) {
      err_msg = std::move(msg);
    }
  }

public:  
  PutOperation(RGWSI_User::Svc& svc,
               RGWSI_MetaBackend::Context *_ctx,
               RGWUserInfo& info,
               RGWUserInfo *old_info,
               RGWObjVersionTracker *objv_tracker,
               real_time& mtime,
               bool exclusive,
               map<string, bufferlist> *pattrs,
               optional_yield _y) :
      svc(svc), info(info), old_info(old_info),
      objv_tracker(objv_tracker), mtime(mtime),
      exclusive(exclusive), pattrs(pattrs), y(_y) {
    ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);
    ui.user_id = info.user_id;
  }

  int prepare() {
    if (objv_tracker) {
      ot = *objv_tracker;
    }

    if (ot.write_version.tag.empty()) {
      if (ot.read_version.tag.empty()) {
        ot.generate_new_write_ver(svc.meta_be->ctx());
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
      int r = svc.user->get_user_info_by_swift(ctx, k.id, inf, nullptr, nullptr);
      if (r >= 0 && inf.user_id.compare(info.user_id) != 0) {
        ldout(svc.meta_be->ctx(), 0) << "WARNING: can't store user info, swift id (" << k.id
          << ") already mapped to another user (" << info.user_id << ")" << dendl;
        return -EEXIST;
      }
    }

    /* check if access keys already exist */
    for (auto iter = info.access_keys.begin(); iter != info.access_keys.end(); ++iter) {
      if (old_info && old_info->access_keys.count(iter->first) != 0)
        continue;
      RGWAccessKey& k = iter->second;
      RGWUserInfo inf;
      int r = svc.user->get_user_info_by_access_key(ctx, k.id, &inf, nullptr, nullptr, y);
      if (r >= 0 && inf.user_id.compare(info.user_id) != 0) {
        ldout(svc.meta_be->ctx(), 0) << "WARNING: can't store user info, access key already mapped to another user" << dendl;
        return -EEXIST;
      }
    }

    return 0;
  }

  int put() {
    bufferlist data_bl;
    encode(ui, data_bl);
    encode(info, data_bl);

    RGWSI_MBSObj_PutParams params(data_bl, pattrs, mtime, exclusive);

    int ret = svc.meta_be->put_entry(ctx, params, &ot);
    if (ret < 0)
      return ret;

    return 0;
  }

  int complete() {
    int ret;

    bufferlist link_bl;
    encode(ui, link_bl);

    auto& obj_ctx = *ctx->obj_ctx;

    if (!info.user_email.empty()) {
      if (!old_info ||
          old_info->user_email.compare(info.user_email) != 0) { /* only if new index changed */
        ret = rgw_put_system_obj(obj_ctx, svc.zone->get_zone_params().user_email_pool, info.user_email,
                                 link_bl, exclusive, NULL, real_time(), y);
        if (ret < 0)
          return ret;
      }
    }

    for (auto iter = info.access_keys.begin(); iter != info.access_keys.end(); ++iter) {
      RGWAccessKey& k = iter->second;
      if (old_info && old_info->access_keys.count(iter->first) != 0)
        continue;

      ret = rgw_put_system_obj(obj_ctx, svc.zone->get_zone_params().user_keys_pool, k.id,
                               link_bl, exclusive, NULL, real_time(), y);
      if (ret < 0)
        return ret;
    }

    map<string, RGWAccessKey>::iterator siter;
    for (siter = info.swift_keys.begin(); siter != info.swift_keys.end(); ++siter) {
      RGWAccessKey& k = siter->second;
      if (old_info && old_info->swift_keys.count(siter->first) != 0)
        continue;

      ret = rgw_put_system_obj(obj_ctx, svc.zone->get_zone_params().user_swift_pool, k.id,
                               link_bl, exclusive, NULL, real_time(), y);
      if (ret < 0)
        return ret;
    }

    if (old_info) {
      ret = remove_old_indexes(*old_info, info, y);
      if (ret < 0) {
        return ret;
      }
    }

    return 0;
  }

  int remove_old_indexes(RGWUserInfo& old_info, RGWUserInfo& new_info, optional_yield y) {
    int ret;

    if (!old_info.user_id.empty() &&
        old_info.user_id != new_info.user_id) {
      if (old_info.user_id.tenant != new_info.user_id.tenant) {
        ldout(svc.user->ctx(), 0) << "ERROR: tenant mismatch: " << old_info.user_id.tenant << " != " << new_info.user_id.tenant << dendl;
        return -EINVAL;
      }
      ret = svc.user->remove_uid_index(ctx, old_info, nullptr, y);
      if (ret < 0 && ret != -ENOENT) {
        set_err_msg("ERROR: could not remove index for uid " + old_info.user_id.to_str());
        return ret;
      }
    }

    if (!old_info.user_email.empty() &&
        old_info.user_email.compare(new_info.user_email) != 0) {
      ret = svc.user->remove_email_index(ctx, old_info.user_email, y);
      if (ret < 0 && ret != -ENOENT) {
        set_err_msg("ERROR: could not remove index for email " + old_info.user_email);
        return ret;
      }
    }

    map<string, RGWAccessKey>::iterator old_iter;
    for (old_iter = old_info.swift_keys.begin(); old_iter != old_info.swift_keys.end(); ++old_iter) {
      RGWAccessKey& swift_key = old_iter->second;
      map<string, RGWAccessKey>::iterator new_iter = new_info.swift_keys.find(swift_key.id);
      if (new_iter == new_info.swift_keys.end()) {
        ret = svc.user->remove_swift_name_index(ctx, swift_key.id, y);
        if (ret < 0 && ret != -ENOENT) {
          set_err_msg("ERROR: could not remove index for swift_name " + swift_key.id);
          return ret;
        }
      }
    }

    return 0;
  }

  const string& get_err_msg() {
    return err_msg;
  }
};

int RGWSI_User::store_user_info(RGWSI_MetaBackend::Context *ctx,
                                RGWUserInfo& info,
                                RGWUserInfo *old_info,
                                RGWObjVersionTracker *objv_tracker,
                                real_time& mtime,
                                bool exclusive,
                                map<string, bufferlist> *attrs,
                                optional_yield y)
{
  PutOperation op(svc, ctx,
                  info, old_info,
                  objv_tracker,
                  mtime, exclusive,
                  attrs,
                  y);

  int r = op.prepare();
  if (r < 0) {
    return r;
  }

  r = op.put();
  if (r < 0) {
    return r;
  }

  r = op.complete();
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWSI_User::remove_key_index(RGWSI_MetaBackend::Context *_ctx,
                                 const RGWAccessKey& access_key,
                                 optional_yield y)
{
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);
  rgw_raw_obj obj(svc.zone->get_zone_params().user_keys_pool, access_key.id);
  auto sysobj = ctx->obj_ctx->get_obj(obj);
  return sysobj.wop().remove(y);
}

int RGWSI_User::remove_email_index(RGWSI_MetaBackend::Context *_ctx,
                                   const string& email,
                                   optional_yield y)
{
  if (email.empty()) {
    return 0;
  }
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);
  rgw_raw_obj obj(svc.zone->get_zone_params().user_email_pool, email);
  auto sysobj = ctx->obj_ctx->get_obj(obj);
  return sysobj.wop().remove(y);
}

int RGWSI_User::remove_swift_name_index(RGWSI_MetaBackend::Context *_ctx, const string& swift_name,
                                        optional_yield y)
{
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);
  rgw_raw_obj obj(svc.zone->get_zone_params().user_swift_pool, swift_name);
  auto sysobj = ctx->obj_ctx->get_obj(obj);
  return sysobj.wop().remove(y);
}

/**
 * delete a user's presence from the RGW system.
 * First remove their bucket ACLs, then delete them
 * from the user and user email pools. This leaves the pools
 * themselves alone, as well as any ACLs embedded in object xattrs.
 */
int RGWSI_User::remove_user_info(RGWSI_MetaBackend::Context *_ctx,
                                 const RGWUserInfo& info,
                                 RGWObjVersionTracker *objv_tracker,
                                 optional_yield y)
{
  int ret;

  auto cct = svc.meta_be->ctx();

  auto kiter = info.access_keys.begin();
  for (; kiter != info.access_keys.end(); ++kiter) {
    ldout(cct, 10) << "removing key index: " << kiter->first << dendl;
    ret = remove_key_index(_ctx, kiter->second, y);
    if (ret < 0 && ret != -ENOENT) {
      ldout(cct, 0) << "ERROR: could not remove " << kiter->first << " (access key object), should be fixed (err=" << ret << ")" << dendl;
      return ret;
    }
  }

  auto siter = info.swift_keys.begin();
  for (; siter != info.swift_keys.end(); ++siter) {
    auto& k = siter->second;
    ldout(cct, 10) << "removing swift subuser index: " << k.id << dendl;
    /* check if swift mapping exists */
    ret = remove_swift_name_index(_ctx, k.id, y);
    if (ret < 0 && ret != -ENOENT) {
      ldout(cct, 0) << "ERROR: could not remove " << k.id << " (swift name object), should be fixed (err=" << ret << ")" << dendl;
      return ret;
    }
  }

  ldout(cct, 10) << "removing email index: " << info.user_email << dendl;
  ret = remove_email_index(_ctx, info.user_email, y);
  if (ret < 0 && ret != -ENOENT) {
    ldout(cct, 0) << "ERROR: could not remove email index object for "
        << info.user_email << ", should be fixed (err=" << ret << ")" << dendl;
    return ret;
  }

  string buckets_obj_id = get_buckets_oid(info.user_id);
  rgw_raw_obj uid_bucks(svc.zone->get_zone_params().user_uid_pool, buckets_obj_id);
  ldout(cct, 10) << "removing user buckets index" << dendl;
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);
  auto sysobj = ctx->obj_ctx->get_obj(uid_bucks);
  ret = sysobj.wop().remove(y);
  if (ret < 0 && ret != -ENOENT) {
    ldout(cct, 0) << "ERROR: could not remove " << info.user_id << ":" << uid_bucks << ", should be fixed (err=" << ret << ")" << dendl;
    return ret;
  }

  ret = remove_uid_index(ctx, info, objv_tracker, y);
  if (ret < 0 && ret != -ENOENT) {
    return ret;
  }

  return 0;
}

int RGWSI_User::remove_uid_index(RGWSI_MetaBackend::Context *ctx, const RGWUserInfo& user_info, RGWObjVersionTracker *objv_tracker,
                                 optional_yield y)
{
  ldout(cct, 10) << "removing user index: " << user_info.user_id << dendl;

  RGWSI_MBSObj_RemoveParams params;
#warning need mtime?
#if 0
  params.mtime = user_info.mtime;
#endif
  int ret = svc.meta_be->remove_entry(ctx, params, objv_tracker, y);
  if (ret < 0 && ret != -ENOENT && ret  != -ECANCELED) {
    string key;
    user_info.user_id.to_str(key);
    rgw_raw_obj uid_obj(svc.zone->get_zone_params().user_uid_pool, key);
    ldout(cct, 0) << "ERROR: could not remove " << user_info.user_id << ":" << uid_obj << ", should be fixed (err=" << ret << ")" << dendl;
    return ret;
  }

  return 0;
}

int RGWSI_User::get_user_info_from_index(RGWSI_MetaBackend::Context *_ctx,
                                         const string& key,
                                         const rgw_pool& pool,
                                         RGWUserInfo *info,
                                         RGWObjVersionTracker * const objv_tracker,
                                         real_time * const pmtime,
                                         optional_yield y)
{
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);

#warning uinfo_cache needs to add index to lookup
  if (auto e = uinfo_cache->find(key)) {
    *info = e->info;
    if (objv_tracker)
      *objv_tracker = e->objv_tracker;
    if (pmtime)
      *pmtime = e->mtime;
    return 0;
  }

  user_info_cache_entry e;
  bufferlist bl;
  RGWUID uid;

  int ret = rgw_get_system_obj(*ctx->obj_ctx, pool, key, bl, nullptr, &e.mtime, y);
  if (ret < 0)
    return ret;

  rgw_cache_entry_info cache_info;

  auto iter = bl.cbegin();
  try {
    decode(uid, iter);

    int ret = read_user_info(ctx->clone(get_meta_key(uid.user_id)),
                             &e.info, &e.objv_tracker, nullptr, &cache_info, nullptr, y);
    if (ret < 0) {
      return ret;
    }
  } catch (buffer::error& err) {
    ldout(svc.meta_be->ctx(), 0) << "ERROR: failed to decode user info, caught buffer::error" << dendl;
    return -EIO;
  }

  uinfo_cache->put(svc.cache, key, &e, { &cache_info });

  *info = e.info;
  if (objv_tracker)
    *objv_tracker = e.objv_tracker;
  if (pmtime)
    *pmtime = e.mtime;

  return 0;
}

/**
 * Given an email, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
int RGWSI_User::get_user_info_by_email(RGWSI_MetaBackend::Context *ctx,
                                       const string& email, RGWUserInfo *info,
                                       RGWObjVersionTracker *objv_tracker,
                                       real_time *pmtime,
                                       optional_yield y)
{
  return get_user_info_from_index(ctx, email, svc.zone->get_zone_params().user_email_pool,
                                  info, objv_tracker, pmtime, y);
}

/**
 * Given an swift username, finds the user_info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
int RGWSI_User::get_user_info_by_swift(RGWSI_MetaBackend::Context *ctx,
                                       const string& swift_name,
                                       RGWUserInfo *info,        /* out */
                                       RGWObjVersionTracker * const objv_tracker,
                                       real_time * const pmtime,
                                       optional_yield y)
{
  return get_user_info_from_index(ctx,
                                  swift_name,
                                  svc.zone->get_zone_params().user_swift_pool,
                                  info, objv_tracker, pmtime, y);
}

/**
 * Given an access key, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
int RGWSI_User::get_user_info_by_access_key(RGWSI_MetaBackend::Context *ctx,
                                            const std::string& access_key,
                                            RGWUserInfo *info,
                                            RGWObjVersionTracker* objv_tracker,
                                            real_time *pmtime,
                                            optional_yield y)
{
  return get_user_info_from_index(ctx,
                                  access_key,
                                  svc.zone->get_zone_params().user_keys_pool,
                                  info, objv_tracker, pmtime, y);
}

