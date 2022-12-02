// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <boost/algorithm/string.hpp>

#include "svc_user.h"
#include "svc_user_rados.h"
#include "svc_zone.h"
#include "svc_sys_obj.h"
#include "svc_sys_obj_cache.h"
#include "svc_meta.h"
#include "svc_meta_be_sobj.h"
#include "svc_sync_modules.h"

#include "rgw_user.h"
#include "rgw_bucket.h"
#include "rgw_tools.h"
#include "rgw_zone.h"
#include "rgw_rados.h"

#include "cls/user/cls_user_client.h"

#define dout_subsys ceph_subsys_rgw

#define RGW_BUCKETS_OBJ_SUFFIX ".buckets"

using namespace std;

class RGWSI_User_Module : public RGWSI_MBSObj_Handler_Module {
  RGWSI_User_RADOS::Svc& svc;

  const string prefix;
public:
  RGWSI_User_Module(RGWSI_User_RADOS::Svc& _svc) : RGWSI_MBSObj_Handler_Module("user"),
                                                   svc(_svc) {}

  void get_pool_and_oid(const string& key, rgw_pool *pool, string *oid) override {
    if (pool) {
      *pool = svc.zone->get_zone_params().user_uid_pool;
    }
    if (oid) {
      *oid = key;
    }
  }

  const string& get_oid_prefix() override {
    return prefix;
  }

  bool is_valid_oid(const string& oid) override {
    // filter out the user.buckets objects
    return !boost::algorithm::ends_with(oid, RGW_BUCKETS_OBJ_SUFFIX);
  }

  string key_to_oid(const string& key) override {
    return key;
  }

  string oid_to_key(const string& oid) override {
    return oid;
  }
};

RGWSI_User_RADOS::RGWSI_User_RADOS(CephContext *cct): RGWSI_User(cct) {
}

RGWSI_User_RADOS::~RGWSI_User_RADOS() {
}

void RGWSI_User_RADOS::init(librados::Rados* rados_,
                            RGWSI_Zone *_zone_svc, RGWSI_SysObj *_sysobj_svc,
                            RGWSI_SysObj_Cache *_cache_svc, RGWSI_Meta *_meta_svc,
                            RGWSI_MetaBackend *_meta_be_svc,
                            RGWSI_SyncModules *_sync_modules_svc)
{
  svc.user = this;
  rados = rados_;
  svc.zone = _zone_svc;
  svc.sysobj = _sysobj_svc;
  svc.cache = _cache_svc;
  svc.meta = _meta_svc;
  svc.meta_be = _meta_be_svc;
  svc.sync_modules = _sync_modules_svc;
}

int RGWSI_User_RADOS::do_start(optional_yield, const DoutPrefixProvider *dpp)
{
  uinfo_cache.reset(new RGWChainedCacheImpl<user_info_cache_entry>);
  uinfo_cache->init(svc.cache);

  int r = svc.meta->create_be_handler(RGWSI_MetaBackend::Type::MDBE_SOBJ, &be_handler);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to create be handler: r=" << r << dendl;
    return r;
  }

  RGWSI_MetaBackend_Handler_SObj *bh = static_cast<RGWSI_MetaBackend_Handler_SObj *>(be_handler);

  auto module = new RGWSI_User_Module(svc);
  be_module.reset(module);
  bh->set_module(module);
  return 0;
}

rgw_raw_obj RGWSI_User_RADOS::get_buckets_obj(const rgw_user& user) const
{
  string oid = user.to_str() + RGW_BUCKETS_OBJ_SUFFIX;
  return rgw_raw_obj(svc.zone->get_zone_params().user_uid_pool, oid);
}

int RGWSI_User_RADOS::read_user_info(RGWSI_MetaBackend::Context *ctx,
                               const rgw_user& user,
                               RGWUserInfo *info,
                               RGWObjVersionTracker * const objv_tracker,
                               real_time * const pmtime,
                               rgw_cache_entry_info * const cache_info,
                               map<string, bufferlist> * const pattrs,
                               optional_yield y,
                               const DoutPrefixProvider *dpp)
{
  if(user.id == RGW_USER_ANON_ID) {
    ldpp_dout(dpp, 20) << "RGWSI_User_RADOS::read_user_info(): anonymous user" << dendl;
    return -ENOENT;
  }
  bufferlist bl;
  RGWUID user_id;

  RGWSI_MBSObj_GetParams params(&bl, pattrs, pmtime);
  params.set_cache_info(cache_info);

  int ret = svc.meta_be->get_entry(ctx, get_meta_key(user), params, objv_tracker, y, dpp);
  if (ret < 0) {
    return ret;
  }

  auto iter = bl.cbegin();
  try {
    decode(user_id, iter);
    if (user_id.user_id != user) {
      ldpp_dout(dpp, -1)  << "ERROR: rgw_get_user_info_by_uid(): user id mismatch: " << user_id.user_id << " != " << user << dendl;
      return -EIO;
    }
    if (!iter.end()) {
      decode(*info, iter);
    }
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode user info, caught buffer::error" << dendl;
    return -EIO;
  }

  return 0;
}

class PutOperation
{
  RGWSI_User_RADOS::Svc& svc;
  RGWSI_MetaBackend_SObj::Context_SObj *ctx;
  RGWUID ui;
  const RGWUserInfo& info;
  RGWUserInfo *old_info;
  RGWObjVersionTracker *objv_tracker;
  const real_time& mtime;
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
  PutOperation(RGWSI_User_RADOS::Svc& svc,
               RGWSI_MetaBackend::Context *_ctx,
               const RGWUserInfo& info,
               RGWUserInfo *old_info,
               RGWObjVersionTracker *objv_tracker,
               const real_time& mtime,
               bool exclusive,
               map<string, bufferlist> *pattrs,
               optional_yield y) :
      svc(svc), info(info), old_info(old_info),
      objv_tracker(objv_tracker), mtime(mtime),
      exclusive(exclusive), pattrs(pattrs), y(y) {
    ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);
    ui.user_id = info.user_id;
  }

  int prepare(const DoutPrefixProvider *dpp) {
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

    for (auto iter = info.swift_keys.begin(); iter != info.swift_keys.end(); ++iter) {
      if (old_info && old_info->swift_keys.count(iter->first) != 0)
        continue;
      auto& k = iter->second;
      /* check if swift mapping exists */
      RGWUserInfo inf;
      int r = svc.user->get_user_info_by_swift(ctx, k.id, &inf, nullptr, nullptr, y, dpp);
      if (r >= 0 && inf.user_id != info.user_id &&
          (!old_info || inf.user_id != old_info->user_id)) {
        ldpp_dout(dpp, 0) << "WARNING: can't store user info, swift id (" << k.id
          << ") already mapped to another user (" << info.user_id << ")" << dendl;
        return -EEXIST;
      }
    }

    /* check if access keys already exist */
    for (auto iter = info.access_keys.begin(); iter != info.access_keys.end(); ++iter) {
      if (old_info && old_info->access_keys.count(iter->first) != 0)
        continue;
      auto& k = iter->second;
      RGWUserInfo inf;
      int r = svc.user->get_user_info_by_access_key(ctx, k.id, &inf, nullptr, nullptr, y, dpp);
      if (r >= 0 && inf.user_id != info.user_id &&
          (!old_info || inf.user_id != old_info->user_id)) {
        ldpp_dout(dpp, 0) << "WARNING: can't store user info, access key already mapped to another user" << dendl;
        return -EEXIST;
      }
    }

    return 0;
  }

  int put(const DoutPrefixProvider *dpp) {
    bufferlist data_bl;
    encode(ui, data_bl);
    encode(info, data_bl);

    RGWSI_MBSObj_PutParams params(data_bl, pattrs, mtime, exclusive);

    int ret = svc.meta_be->put(ctx, RGWSI_User::get_meta_key(info.user_id), params, &ot, y, dpp);
    if (ret < 0)
      return ret;

    return 0;
  }

  int complete(const DoutPrefixProvider *dpp) {
    int ret;

    bufferlist link_bl;
    encode(ui, link_bl);

    if (!info.user_email.empty()) {
      if (!old_info ||
          old_info->user_email.compare(info.user_email) != 0) { /* only if new index changed */
        ret = rgw_put_system_obj(dpp, svc.sysobj, svc.zone->get_zone_params().user_email_pool, info.user_email,
                                 link_bl, exclusive, NULL, real_time(), y);
        if (ret < 0)
          return ret;
      }
    }

    const bool renamed = old_info && old_info->user_id != info.user_id;
    for (auto iter = info.access_keys.begin(); iter != info.access_keys.end(); ++iter) {
      auto& k = iter->second;
      if (old_info && old_info->access_keys.count(iter->first) != 0 && !renamed)
        continue;

      ret = rgw_put_system_obj(dpp, svc.sysobj, svc.zone->get_zone_params().user_keys_pool, k.id,
                               link_bl, exclusive, NULL, real_time(), y);
      if (ret < 0)
        return ret;
    }

    for (auto siter = info.swift_keys.begin(); siter != info.swift_keys.end(); ++siter) {
      auto& k = siter->second;
      if (old_info && old_info->swift_keys.count(siter->first) != 0 && !renamed)
        continue;

      ret = rgw_put_system_obj(dpp, svc.sysobj, svc.zone->get_zone_params().user_swift_pool, k.id,
                               link_bl, exclusive, NULL, real_time(), y);
      if (ret < 0)
        return ret;
    }

    if (old_info) {
      ret = remove_old_indexes(*old_info, info, y, dpp);
      if (ret < 0) {
        return ret;
      }
    }

    return 0;
  }

  int remove_old_indexes(const RGWUserInfo& old_info, const RGWUserInfo& new_info, optional_yield y, const DoutPrefixProvider *dpp) {
    int ret;

    if (!old_info.user_id.empty() &&
        old_info.user_id != new_info.user_id) {
      if (old_info.user_id.tenant != new_info.user_id.tenant) {
        ldpp_dout(dpp, 0) << "ERROR: tenant mismatch: " << old_info.user_id.tenant << " != " << new_info.user_id.tenant << dendl;
        return -EINVAL;
      }
      ret = svc.user->remove_uid_index(ctx, old_info, nullptr, y, dpp);
      if (ret < 0 && ret != -ENOENT) {
        set_err_msg("ERROR: could not remove index for uid " + old_info.user_id.to_str());
        return ret;
      }
    }

    if (!old_info.user_email.empty() &&
        old_info.user_email != new_info.user_email) {
      ret = svc.user->remove_email_index(dpp, old_info.user_email, y);
      if (ret < 0 && ret != -ENOENT) {
        set_err_msg("ERROR: could not remove index for email " + old_info.user_email);
        return ret;
      }
    }

    for ([[maybe_unused]] const auto& [name, access_key] : old_info.access_keys) {
      if (!new_info.access_keys.count(access_key.id)) {
        ret = svc.user->remove_key_index(dpp, access_key, y);
        if (ret < 0 && ret != -ENOENT) {
          set_err_msg("ERROR: could not remove index for key " + access_key.id);
          return ret;
        }
      }
    }

    for (auto old_iter = old_info.swift_keys.begin(); old_iter != old_info.swift_keys.end(); ++old_iter) {
      const auto& swift_key = old_iter->second;
      auto new_iter = new_info.swift_keys.find(swift_key.id);
      if (new_iter == new_info.swift_keys.end()) {
        ret = svc.user->remove_swift_name_index(dpp, swift_key.id, y);
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

int RGWSI_User_RADOS::store_user_info(RGWSI_MetaBackend::Context *ctx,
                                const RGWUserInfo& info,
                                RGWUserInfo *old_info,
                                RGWObjVersionTracker *objv_tracker,
                                const real_time& mtime,
                                bool exclusive,
                                map<string, bufferlist> *attrs,
                                optional_yield y,
                                const DoutPrefixProvider *dpp)
{
  PutOperation op(svc, ctx,
                  info, old_info,
                  objv_tracker,
                  mtime, exclusive,
                  attrs,
                  y);

  int r = op.prepare(dpp);
  if (r < 0) {
    return r;
  }

  r = op.put(dpp);
  if (r < 0) {
    return r;
  }

  r = op.complete(dpp);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWSI_User_RADOS::remove_key_index(const DoutPrefixProvider *dpp, 
                                       const RGWAccessKey& access_key,
                                       optional_yield y)
{
  rgw_raw_obj obj(svc.zone->get_zone_params().user_keys_pool, access_key.id);
  auto sysobj = svc.sysobj->get_obj(obj);
  return sysobj.wop().remove(dpp, y);
}

int RGWSI_User_RADOS::remove_email_index(const DoutPrefixProvider *dpp, 
                                         const string& email,
                                         optional_yield y)
{
  if (email.empty()) {
    return 0;
  }
  rgw_raw_obj obj(svc.zone->get_zone_params().user_email_pool, email);
  auto sysobj = svc.sysobj->get_obj(obj);
  return sysobj.wop().remove(dpp, y);
}

int RGWSI_User_RADOS::remove_swift_name_index(const DoutPrefixProvider *dpp,
                                              const string& swift_name,
                                              optional_yield y)
{
  rgw_raw_obj obj(svc.zone->get_zone_params().user_swift_pool, swift_name);
  auto sysobj = svc.sysobj->get_obj(obj);
  return sysobj.wop().remove(dpp, y);
}

/**
 * delete a user's presence from the RGW system.
 * First remove their bucket ACLs, then delete them
 * from the user and user email pools. This leaves the pools
 * themselves alone, as well as any ACLs embedded in object xattrs.
 */
int RGWSI_User_RADOS::remove_user_info(RGWSI_MetaBackend::Context *ctx,
                                 const RGWUserInfo& info,
                                 RGWObjVersionTracker *objv_tracker,
                                 optional_yield y,
                                 const DoutPrefixProvider *dpp)
{
  int ret;

  auto kiter = info.access_keys.begin();
  for (; kiter != info.access_keys.end(); ++kiter) {
    ldpp_dout(dpp, 10) << "removing key index: " << kiter->first << dendl;
    ret = remove_key_index(dpp, kiter->second, y);
    if (ret < 0 && ret != -ENOENT) {
      ldpp_dout(dpp, 0) << "ERROR: could not remove " << kiter->first << " (access key object), should be fixed (err=" << ret << ")" << dendl;
      return ret;
    }
  }

  auto siter = info.swift_keys.begin();
  for (; siter != info.swift_keys.end(); ++siter) {
    auto& k = siter->second;
    ldpp_dout(dpp, 10) << "removing swift subuser index: " << k.id << dendl;
    /* check if swift mapping exists */
    ret = remove_swift_name_index(dpp, k.id, y);
    if (ret < 0 && ret != -ENOENT) {
      ldpp_dout(dpp, 0) << "ERROR: could not remove " << k.id << " (swift name object), should be fixed (err=" << ret << ")" << dendl;
      return ret;
    }
  }

  ldpp_dout(dpp, 10) << "removing email index: " << info.user_email << dendl;
  ret = remove_email_index(dpp, info.user_email, y);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 0) << "ERROR: could not remove email index object for "
        << info.user_email << ", should be fixed (err=" << ret << ")" << dendl;
    return ret;
  }

  rgw_raw_obj uid_bucks = get_buckets_obj(info.user_id);
  ldpp_dout(dpp, 10) << "removing user buckets index" << dendl;
  auto sysobj = svc.sysobj->get_obj(uid_bucks);
  ret = sysobj.wop().remove(dpp, y);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 0) << "ERROR: could not remove " << info.user_id << ":" << uid_bucks << ", should be fixed (err=" << ret << ")" << dendl;
    return ret;
  }

  ret = remove_uid_index(ctx, info, objv_tracker, y, dpp);
  if (ret < 0 && ret != -ENOENT) {
    return ret;
  }

  return 0;
}

int RGWSI_User_RADOS::remove_uid_index(RGWSI_MetaBackend::Context *ctx, const RGWUserInfo& user_info, RGWObjVersionTracker *objv_tracker,
                                       optional_yield y, const DoutPrefixProvider *dpp)
{
  ldpp_dout(dpp, 10) << "removing user index: " << user_info.user_id << dendl;

  RGWSI_MBSObj_RemoveParams params;
  int ret = svc.meta_be->remove(ctx, get_meta_key(user_info.user_id), params, objv_tracker, y, dpp);
  if (ret < 0 && ret != -ENOENT && ret  != -ECANCELED) {
    string key;
    user_info.user_id.to_str(key);
    rgw_raw_obj uid_obj(svc.zone->get_zone_params().user_uid_pool, key);
    ldpp_dout(dpp, 0) << "ERROR: could not remove " << user_info.user_id << ":" << uid_obj << ", should be fixed (err=" << ret << ")" << dendl;
    return ret;
  }

  return 0;
}

int RGWSI_User_RADOS::get_user_info_from_index(RGWSI_MetaBackend::Context* ctx,
                                               const string& key,
                                               const rgw_pool& pool,
                                               RGWUserInfo *info,
                                               RGWObjVersionTracker* objv_tracker,
                                               real_time* pmtime, optional_yield y,
                                               const DoutPrefixProvider* dpp)
{
  string cache_key = pool.to_str() + "/" + key;

  if (auto e = uinfo_cache->find(cache_key)) {
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

  int ret = rgw_get_system_obj(svc.sysobj, pool, key, bl, nullptr, &e.mtime, y, dpp);
  if (ret < 0)
    return ret;

  rgw_cache_entry_info cache_info;

  auto iter = bl.cbegin();
  try {
    decode(uid, iter);

    int ret = read_user_info(ctx, uid.user_id,
                             &e.info, &e.objv_tracker, nullptr, &cache_info, nullptr,
                             y, dpp);
    if (ret < 0) {
      return ret;
    }
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode user info, caught buffer::error" << dendl;
    return -EIO;
  }

  uinfo_cache->put(dpp, svc.cache, cache_key, &e, { &cache_info });

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
int RGWSI_User_RADOS::get_user_info_by_email(RGWSI_MetaBackend::Context *ctx,
                                       const string& email, RGWUserInfo *info,
                                       RGWObjVersionTracker *objv_tracker,
                                       real_time *pmtime, optional_yield y,
                                       const DoutPrefixProvider *dpp)
{
  return get_user_info_from_index(ctx, email, svc.zone->get_zone_params().user_email_pool,
                                  info, objv_tracker, pmtime, y, dpp);
}

/**
 * Given an swift username, finds the user_info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
int RGWSI_User_RADOS::get_user_info_by_swift(RGWSI_MetaBackend::Context *ctx,
                                       const string& swift_name,
                                       RGWUserInfo *info,        /* out */
                                       RGWObjVersionTracker * const objv_tracker,
                                       real_time * const pmtime, optional_yield y,
                                       const DoutPrefixProvider *dpp)
{
  return get_user_info_from_index(ctx,
                                  swift_name,
                                  svc.zone->get_zone_params().user_swift_pool,
                                  info, objv_tracker, pmtime, y, dpp);
}

/**
 * Given an access key, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
int RGWSI_User_RADOS::get_user_info_by_access_key(RGWSI_MetaBackend::Context *ctx,
                                            const std::string& access_key,
                                            RGWUserInfo *info,
                                            RGWObjVersionTracker* objv_tracker,
                                            real_time *pmtime, optional_yield y,
                                            const DoutPrefixProvider *dpp)
{
  return get_user_info_from_index(ctx,
                                  access_key,
                                  svc.zone->get_zone_params().user_keys_pool,
                                  info, objv_tracker, pmtime, y, dpp);
}

int RGWSI_User_RADOS::cls_user_update_buckets(const DoutPrefixProvider *dpp, rgw_raw_obj& obj, list<cls_user_bucket_entry>& entries, bool add, optional_yield y)
{
  rgw_rados_ref rados_obj;
  int r = rgw_get_rados_ref(dpp, rados, obj, &rados_obj);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  cls_user_set_buckets(op, entries, add);
  r = rados_obj.operate(dpp, &op, y);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWSI_User_RADOS::cls_user_add_bucket(const DoutPrefixProvider *dpp, rgw_raw_obj& obj, const cls_user_bucket_entry& entry, optional_yield y)
{
  list<cls_user_bucket_entry> l;
  l.push_back(entry);

  return cls_user_update_buckets(dpp, obj, l, true, y);
}

int RGWSI_User_RADOS::cls_user_remove_bucket(const DoutPrefixProvider *dpp, rgw_raw_obj& obj, const cls_user_bucket& bucket, optional_yield y)
{
  rgw_rados_ref rados_obj;
  int r = rgw_get_rados_ref(dpp, rados, obj, &rados_obj);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  ::cls_user_remove_bucket(op, bucket);
  r = rados_obj.operate(dpp, &op, y);
  if (r < 0)
    return r;

  return 0;
}

int RGWSI_User_RADOS::add_bucket(const DoutPrefixProvider *dpp, 
                                 const rgw_user& user,
                                 const rgw_bucket& bucket,
                                 ceph::real_time creation_time,
				 optional_yield y)
{
  int ret;

  cls_user_bucket_entry new_bucket;

  bucket.convert(&new_bucket.bucket);
  new_bucket.size = 0;
  if (real_clock::is_zero(creation_time))
    new_bucket.creation_time = real_clock::now();
  else
    new_bucket.creation_time = creation_time;

  rgw_raw_obj obj = get_buckets_obj(user);
  ret = cls_user_add_bucket(dpp, obj, new_bucket, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: error adding bucket to user: ret=" << ret << dendl;
    return ret;
  }

  return 0;
}


int RGWSI_User_RADOS::remove_bucket(const DoutPrefixProvider *dpp, 
                                    const rgw_user& user,
                                    const rgw_bucket& _bucket,
				    optional_yield y)
{
  cls_user_bucket bucket;
  bucket.name = _bucket.name;
  rgw_raw_obj obj = get_buckets_obj(user);
  int ret = cls_user_remove_bucket(dpp, obj, bucket, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: error removing bucket from user: ret=" << ret << dendl;
  }

  return 0;
}

int RGWSI_User_RADOS::cls_user_flush_bucket_stats(const DoutPrefixProvider *dpp, 
                                                  rgw_raw_obj& user_obj,
                                                  const RGWBucketEnt& ent, optional_yield y)
{
  cls_user_bucket_entry entry;
  ent.convert(&entry);

  list<cls_user_bucket_entry> entries;
  entries.push_back(entry);

  int r = cls_user_update_buckets(dpp, user_obj, entries, false, y);
  if (r < 0) {
    ldpp_dout(dpp, 20) << "cls_user_update_buckets() returned " << r << dendl;
    return r;
  }

  return 0;
}

int RGWSI_User_RADOS::cls_user_list_buckets(const DoutPrefixProvider *dpp, 
                                            rgw_raw_obj& obj,
                                            const string& in_marker,
                                            const string& end_marker,
                                            const int max_entries,
                                            list<cls_user_bucket_entry>& entries,
                                            string * const out_marker,
                                            bool * const truncated,
					    optional_yield y)
{
  rgw_rados_ref rados_obj;
  int r = rgw_get_rados_ref(dpp, rados, obj, &rados_obj);
  if (r < 0) {
    return r;
  }

  librados::ObjectReadOperation op;
  int rc;

  cls_user_bucket_list(op, in_marker, end_marker, max_entries, entries, out_marker, truncated, &rc);
  bufferlist ibl;
  r = rados_obj.operate(dpp, &op, &ibl, y);
  if (r < 0)
    return r;
  if (rc < 0)
    return rc;

  return 0;
}

int RGWSI_User_RADOS::list_buckets(const DoutPrefixProvider *dpp, 
				   const rgw_user& user,
				   const string& marker,
				   const string& end_marker,
				   uint64_t max,
				   RGWUserBuckets *buckets,
				   bool *is_truncated, optional_yield y)
{
  int ret;

  buckets->clear();
   if (user.id == RGW_USER_ANON_ID) {
    ldpp_dout(dpp, 20) << "RGWSI_User_RADOS::list_buckets(): anonymous user" << dendl;
    *is_truncated = false;
    return 0;
  }
  rgw_raw_obj obj = get_buckets_obj(user);

  bool truncated = false;
  string m = marker;

  uint64_t total = 0;

  do {
    std::list<cls_user_bucket_entry> entries;
    ret = cls_user_list_buckets(dpp, obj, m, end_marker, max - total, entries, &m, &truncated, y);
    if (ret == -ENOENT) {
      ret = 0;
    }

    if (ret < 0) {
      return ret;
    }

    for (auto& entry : entries) {
      buckets->add(RGWBucketEnt(user, std::move(entry)));
      total++;
    }

  } while (truncated && total < max);

  if (is_truncated) {
    *is_truncated = truncated;
  }

  return 0;
}

int RGWSI_User_RADOS::flush_bucket_stats(const DoutPrefixProvider *dpp, 
                                         const rgw_user& user,
                                         const RGWBucketEnt& ent,
					 optional_yield y)
{
  rgw_raw_obj obj = get_buckets_obj(user);

  return cls_user_flush_bucket_stats(dpp, obj, ent, y);
}

int RGWSI_User_RADOS::reset_bucket_stats(const DoutPrefixProvider *dpp, 
                                         const rgw_user& user,
					 optional_yield y)
{
  return cls_user_reset_stats(dpp, user, y);
}

int RGWSI_User_RADOS::cls_user_reset_stats(const DoutPrefixProvider *dpp, const rgw_user& user, optional_yield y)
{
  rgw_raw_obj obj = get_buckets_obj(user);
  rgw_rados_ref rados_obj;
  int r = rgw_get_rados_ref(dpp, rados, obj, &rados_obj);
  if (r < 0) {
    return r;
  }

  int rval;

  cls_user_reset_stats2_op call;
  cls_user_reset_stats2_ret ret;

  do {
    buffer::list in, out;
    librados::ObjectWriteOperation op;

    call.time = real_clock::now();
    ret.update_call(call);

    encode(call, in);
    op.exec("user", "reset_user_stats2", in, &out, &rval);
    r = rados_obj.operate(dpp, &op, y, librados::OPERATION_RETURNVEC);
    if (r < 0) {
      return r;
    }
    try {
      auto bliter = out.cbegin();
      decode(ret, bliter);
    } catch (ceph::buffer::error& err) {
      return -EINVAL;
    }
  } while (ret.truncated);

  return rval;
}

int RGWSI_User_RADOS::complete_flush_stats(const DoutPrefixProvider *dpp, 
                                           const rgw_user& user, optional_yield y)
{
  rgw_raw_obj obj = get_buckets_obj(user);
  rgw_rados_ref rados_obj;
  int r = rgw_get_rados_ref(dpp, rados, obj, &rados_obj);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  ::cls_user_complete_stats_sync(op);
  return rados_obj.operate(dpp, &op, y);
}

int RGWSI_User_RADOS::cls_user_get_header(const DoutPrefixProvider *dpp, 
                                          const rgw_user& user, cls_user_header *header,
					  optional_yield y)
{
  rgw_raw_obj obj = get_buckets_obj(user);
  rgw_rados_ref rados_obj;
  int r = rgw_get_rados_ref(dpp, rados, obj, &rados_obj);
  if (r < 0) {
    return r;
  }
  int rc;
  bufferlist ibl;
  librados::ObjectReadOperation op;
  ::cls_user_get_header(op, header, &rc);
  return rados_obj.operate(dpp, &op, &ibl, y);
}

int RGWSI_User_RADOS::cls_user_get_header_async(const DoutPrefixProvider *dpp, const string& user_str, RGWGetUserHeader_CB *cb)
{
  rgw_raw_obj obj = get_buckets_obj(rgw_user(user_str));
  rgw_rados_ref ref;
  int r = rgw_get_rados_ref(dpp, rados, obj, &ref);
  if (r < 0) {
    return r;
  }

  r = ::cls_user_get_header_async(ref.ioctx, ref.obj.oid, cb);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWSI_User_RADOS::read_stats(const DoutPrefixProvider *dpp, 
                                 RGWSI_MetaBackend::Context *ctx,
                                 const rgw_user& user, RGWStorageStats *stats,
                                 ceph::real_time *last_stats_sync,
                                 ceph::real_time *last_stats_update,
				 optional_yield y)
{
  string user_str = user.to_str();

  RGWUserInfo info;
  real_time mtime;
  int ret = read_user_info(ctx, user, &info, nullptr, &mtime, nullptr, nullptr, y, dpp);
  if (ret < 0)
  {
    return ret;
  }

  cls_user_header header;
  int r = cls_user_get_header(dpp, rgw_user(user_str), &header, y);
  if (r < 0 && r != -ENOENT)
    return r;

  const cls_user_stats& hs = header.stats;

  stats->size = hs.total_bytes;
  stats->size_rounded = hs.total_bytes_rounded;
  stats->num_objects = hs.total_entries;

  if (last_stats_sync) {
    *last_stats_sync = header.last_stats_sync;
  }

  if (last_stats_update) {
   *last_stats_update = header.last_stats_update;
  }

  return 0;
}

class RGWGetUserStatsContext : public RGWGetUserHeader_CB {
  boost::intrusive_ptr<rgw::sal::ReadStatsCB> cb;

public:
  explicit RGWGetUserStatsContext(boost::intrusive_ptr<rgw::sal::ReadStatsCB> cb)
    : cb(std::move(cb)) {}

  void handle_response(int r, cls_user_header& header) override {
    const cls_user_stats& hs = header.stats;
    RGWStorageStats stats;

    stats.size = hs.total_bytes;
    stats.size_rounded = hs.total_bytes_rounded;
    stats.num_objects = hs.total_entries;

    cb->handle_response(r, stats);
    cb.reset();
  }
};

int RGWSI_User_RADOS::read_stats_async(const DoutPrefixProvider *dpp,
                                       const rgw_user& user,
                                       boost::intrusive_ptr<rgw::sal::ReadStatsCB> _cb)
{
  string user_str = user.to_str();

  RGWGetUserStatsContext *cb = new RGWGetUserStatsContext(std::move(_cb));
  int r = cls_user_get_header_async(dpp, user_str, cb);
  if (r < 0) {
    delete cb;
    return r;
  }

  return 0;
}

