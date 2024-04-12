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
#include "rgw_account.h"
#include "rgw_bucket.h"
#include "rgw_tools.h"
#include "rgw_zone.h"
#include "rgw_rados.h"

#include "driver/rados/account.h"
#include "driver/rados/buckets.h"
#include "driver/rados/group.h"
#include "driver/rados/users.h"

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
    if (rgw_user{user_id.id} != user) {
      ldpp_dout(dpp, -1)  << "ERROR: rgw_get_user_info_by_uid(): user id mismatch: " << user_id.id << " != " << user << dendl;
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

// simple struct and function to help decide whether we need to add/remove
// links to the account users index
struct users_entry {
  std::string_view account_id, path, name;
  constexpr operator bool() { return !account_id.empty(); }
  constexpr auto operator<=>(const users_entry&) const = default;
};

static users_entry account_users_link(const RGWUserInfo* info) {
  if (info && !info->account_id.empty()) {
    return {info->account_id, info->path, info->display_name};
  }
  return {};
}

static bool s3_key_active(const RGWUserInfo* info, const std::string& id) {
  if (!info) {
    return false;
  }
  auto i = info->access_keys.find(id);
  return i != info->access_keys.end() && i->second.active;
}

static bool swift_key_active(const RGWUserInfo* info, const std::string& id) {
  if (!info) {
    return false;
  }
  auto i = info->swift_keys.find(id);
  return i != info->swift_keys.end() && i->second.active;
}

class PutOperation
{
  RGWSI_User_RADOS::Svc& svc;
  librados::Rados& rados;
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
               librados::Rados& rados,
               RGWSI_MetaBackend::Context *_ctx,
               const RGWUserInfo& info,
               RGWUserInfo *old_info,
               RGWObjVersionTracker *objv_tracker,
               const real_time& mtime,
               bool exclusive,
               map<string, bufferlist> *pattrs,
               optional_yield y) :
      svc(svc), rados(rados), info(info), old_info(old_info),
      objv_tracker(objv_tracker), mtime(mtime),
      exclusive(exclusive), pattrs(pattrs), y(y) {
    ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);
    ui.id = info.user_id.to_str();
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

    for (const auto& [id, key] : info.swift_keys) {
      if (!key.active || swift_key_active(old_info, id))
        continue;
      /* check if swift mapping exists */
      RGWUserInfo inf;
      int r = svc.user->get_user_info_by_swift(ctx, id, &inf, nullptr, nullptr, nullptr, y, dpp);
      if (r >= 0 && inf.user_id != info.user_id &&
          (!old_info || inf.user_id != old_info->user_id)) {
        ldpp_dout(dpp, 0) << "WARNING: can't store user info, swift id (" << id
          << ") already mapped to another user (" << info.user_id << ")" << dendl;
        return -EEXIST;
      }
    }

    /* check if access keys already exist */
    for (const auto& [id, key] : info.access_keys) {
      if (!key.active) // new key not active
        continue;
      if (s3_key_active(old_info, id)) // old key already active
        continue;
      RGWUserInfo inf;
      int r = svc.user->get_user_info_by_access_key(ctx, id, &inf, nullptr, nullptr, nullptr, y, dpp);
      if (r >= 0 && inf.user_id != info.user_id &&
          (!old_info || inf.user_id != old_info->user_id)) {
        ldpp_dout(dpp, 0) << "WARNING: can't store user info, access key already mapped to another user" << dendl;
        return -EEXIST;
      }
    }

    if (account_users_link(&info) &&
        account_users_link(&info) != account_users_link(old_info)) {
      if (info.display_name.empty()) {
        ldpp_dout(dpp, 0) << "WARNING: can't store user info, display name "
            "can't be empty in an account" << dendl;
        return -EINVAL;
      }

      const RGWZoneParams& zone = svc.zone->get_zone_params();
      const auto& users = rgwrados::account::get_users_obj(zone, info.account_id);
      std::string existing_uid;
      int r = rgwrados::users::get(dpp, y, rados, users,
                                   info.display_name, existing_uid);
      if (r >= 0 && existing_uid != info.user_id.id) {
        ldpp_dout(dpp, 0) << "WARNING: can't store user info, display name "
            "already exists in account" << dendl;
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
      // only if new index changed
      if (!old_info || !boost::iequals(info.user_email, old_info->user_email)) {
        // store as lower case for case-insensitive matching
        std::string oid = info.user_email;
        boost::to_lower(oid);
        ret = rgw_put_system_obj(dpp, svc.sysobj, svc.zone->get_zone_params().user_email_pool, oid,
                                 link_bl, exclusive, NULL, real_time(), y);
        if (ret < 0)
          return ret;
      }
    }

    const bool renamed = old_info && old_info->user_id != info.user_id;
    for (const auto& [id, key] : info.access_keys) {
      if (!key.active)
        continue;
      if (s3_key_active(old_info, id) && !renamed)
        continue;

      ret = rgw_put_system_obj(dpp, svc.sysobj, svc.zone->get_zone_params().user_keys_pool, id,
                               link_bl, exclusive, NULL, real_time(), y);
      if (ret < 0)
        return ret;
    }

    for (const auto& [id, key] : info.swift_keys) {
      if (!key.active)
        continue;
      if (swift_key_active(old_info, id) && !renamed)
        continue;

      ret = rgw_put_system_obj(dpp, svc.sysobj, svc.zone->get_zone_params().user_swift_pool, id,
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

    if (account_users_link(&info) &&
        account_users_link(&info) != account_users_link(old_info)) {
      // link the user to its account
      const RGWZoneParams& zone = svc.zone->get_zone_params();
      const auto& users = rgwrados::account::get_users_obj(zone, info.account_id);
      ret = rgwrados::users::add(dpp, y, rados, users, info, false,
                                 std::numeric_limits<uint32_t>::max());
      if (ret < 0) {
        ldpp_dout(dpp, 20) << "WARNING: failed to link user "
            << info.user_id << " to account " << info.account_id
            << ": " << cpp_strerror(ret) << dendl;
        return ret;
      }
      ldpp_dout(dpp, 20) << "linked user " << info.user_id
          << " to account " << info.account_id << dendl;
    }

    for (const auto& group_id : info.group_ids) {
      if (old_info && old_info->group_ids.count(group_id)) {
        continue;
      }
      // link the user to its group
      const RGWZoneParams& zone = svc.zone->get_zone_params();
      const auto& users = rgwrados::group::get_users_obj(zone, group_id);
      ret = rgwrados::users::add(dpp, y, rados, users, info, false,
                                 std::numeric_limits<uint32_t>::max());
      if (ret < 0) {
        ldpp_dout(dpp, 20) << "WARNING: failed to link user "
            << info.user_id << " to group " << group_id
            << ": " << cpp_strerror(ret) << dendl;
        return ret;
      }
      ldpp_dout(dpp, 20) << "linked user " << info.user_id
          << " to group " << group_id << dendl;
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
        !boost::iequals(old_info.user_email, new_info.user_email)) {
      ret = svc.user->remove_email_index(dpp, old_info.user_email, y);
      if (ret < 0 && ret != -ENOENT) {
        set_err_msg("ERROR: could not remove index for email " + old_info.user_email);
        return ret;
      }
    }

    for (const auto& [id, key] : old_info.access_keys) {
      if (key.active && !s3_key_active(&new_info, id)) {
        ret = svc.user->remove_key_index(dpp, key, y);
        if (ret < 0 && ret != -ENOENT) {
          set_err_msg("ERROR: could not remove index for key " + id);
          return ret;
        }
      }
    }

    for (const auto& [id, key] : old_info.swift_keys) {
      if (key.active && !swift_key_active(&new_info, id)) {
        ret = svc.user->remove_swift_name_index(dpp, id, y);
        if (ret < 0 && ret != -ENOENT) {
          set_err_msg("ERROR: could not remove index for swift_name " + id);
          return ret;
        }
      }
    }

    if (account_users_link(&old_info) &&
        account_users_link(&old_info) != account_users_link(&info)) {
      // unlink the old name from its account
      const RGWZoneParams& zone = svc.zone->get_zone_params();
      const auto& users = rgwrados::account::get_users_obj(zone, old_info.account_id);
      ret = rgwrados::users::remove(dpp, y, rados, users, old_info.display_name);
      if (ret < 0 && ret != -ENOENT) {
        set_err_msg("ERROR: could not unlink from account " + old_info.account_id);
        return ret;
      }
    }

    for (const auto& group_id : old_info.group_ids) {
      if (info.group_ids.count(group_id)) {
        continue;
      }
      // remove from the old group
      const RGWZoneParams& zone = svc.zone->get_zone_params();
      const auto& users = rgwrados::group::get_users_obj(zone, group_id);
      ret = rgwrados::users::remove(dpp, y, rados, users, old_info.display_name);
      if (ret < 0 && ret != -ENOENT) {
        set_err_msg("ERROR: could not unlink from group " + group_id);
        return ret;
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
  PutOperation op(svc, *rados, ctx,
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
  std::string oid = email;
  boost::to_lower(oid);
  rgw_raw_obj obj(svc.zone->get_zone_params().user_email_pool, oid);
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

  for (const auto& [id, key] : info.access_keys) {
    if (!key.active) {
      continue;
    }
    ldpp_dout(dpp, 10) << "removing key index: " << id << dendl;
    ret = remove_key_index(dpp, key, y);
    if (ret < 0 && ret != -ENOENT) {
      ldpp_dout(dpp, 0) << "ERROR: could not remove " << id << " (access key object), should be fixed (err=" << ret << ")" << dendl;
      return ret;
    }
  }

  for (const auto& [id, key] : info.swift_keys) {
    if (!key.active) {
      continue;
    }
    ldpp_dout(dpp, 10) << "removing swift subuser index: " << id << dendl;
    /* check if swift mapping exists */
    ret = remove_swift_name_index(dpp, id, y);
    if (ret < 0 && ret != -ENOENT) {
      ldpp_dout(dpp, 0) << "ERROR: could not remove " << id << " (swift name object), should be fixed (err=" << ret << ")" << dendl;
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

  if (info.account_id.empty()) {
    rgw_raw_obj uid_bucks = get_buckets_obj(info.user_id);
    ldpp_dout(dpp, 10) << "removing user buckets index" << dendl;
    auto sysobj = svc.sysobj->get_obj(uid_bucks);
    ret = sysobj.wop().remove(dpp, y);
    if (ret < 0 && ret != -ENOENT) {
      ldpp_dout(dpp, 0) << "ERROR: could not remove " << info.user_id << ":" << uid_bucks << ", should be fixed (err=" << ret << ")" << dendl;
      return ret;
    }
  } else if (info.type != TYPE_ROOT) {
    // unlink the name from its account
    const RGWZoneParams& zone = svc.zone->get_zone_params();
    const auto& users = rgwrados::account::get_users_obj(zone, info.account_id);
    ret = rgwrados::users::remove(dpp, y, *rados, users, info.display_name);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not unlink from account "
          << info.account_id << ": " << cpp_strerror(ret) << dendl;
      return ret;
    }
  }

  for (const auto& group_id : info.group_ids) {
    const RGWZoneParams& zone = svc.zone->get_zone_params();
    const auto& users = rgwrados::group::get_users_obj(zone, group_id);
    ret = rgwrados::users::remove(dpp, y, *rados, users, info.display_name);
    if (ret < 0 && ret != -ENOENT) {
      ldpp_dout(dpp, 0) << "ERROR: could not unlink from group "
          << group_id << ": " << cpp_strerror(ret) << dendl;
      return ret;
    }
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

static int read_index(const DoutPrefixProvider* dpp, optional_yield y,
                      RGWSI_SysObj* svc_sysobj, const rgw_pool& pool,
                      const std::string& key, ceph::real_time* mtime,
                      RGWUID& uid)
{
  bufferlist bl;
  int r = rgw_get_system_obj(svc_sysobj, pool, key, bl,
                             nullptr, mtime, y, dpp);
  if (r < 0) {
    return r;
  }
  try {
    auto iter = bl.cbegin();
    decode(uid, iter);
  } catch (const buffer::error&) {
    return -EIO;
  }
  return 0;
}

int RGWSI_User_RADOS::get_user_info_from_index(RGWSI_MetaBackend::Context* ctx,
                                               const string& key,
                                               const rgw_pool& pool,
                                               RGWUserInfo *info,
                                               RGWObjVersionTracker* objv_tracker,
                                               std::map<std::string, bufferlist>* pattrs,
                                               real_time* pmtime, optional_yield y,
                                               const DoutPrefixProvider* dpp)
{
  string cache_key = pool.to_str() + "/" + key;

  if (auto e = uinfo_cache->find(cache_key)) {
    *info = e->info;
    if (objv_tracker)
      *objv_tracker = e->objv_tracker;
    if (pattrs)
      *pattrs = e->attrs;
    if (pmtime)
      *pmtime = e->mtime;
    return 0;
  }

  user_info_cache_entry e;
  RGWUID uid;

  int ret = read_index(dpp, y, svc.sysobj, pool, key, &e.mtime, uid);
  if (ret < 0) {
    return ret;
  }

  if (rgw::account::validate_id(uid.id)) {
    // this index is used for an account, not a user
    return -ENOENT;
  }

  rgw_cache_entry_info cache_info;
  ret = read_user_info(ctx, rgw_user{uid.id}, &e.info, &e.objv_tracker,
                       nullptr, &cache_info, &e.attrs, y, dpp);
  if (ret < 0) {
    return ret;
  }

  uinfo_cache->put(dpp, svc.cache, cache_key, &e, { &cache_info });

  *info = e.info;
  if (objv_tracker)
    *objv_tracker = e.objv_tracker;
  if (pmtime)
    *pmtime = e.mtime;
  ldpp_dout(dpp, 20) << "get_user_info_from_index found " << e.attrs.size() << " xattrs" << dendl;
  if (pattrs)
    *pattrs = std::move(e.attrs);

  return 0;
}

/**
 * Given an email, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
int RGWSI_User_RADOS::get_user_info_by_email(RGWSI_MetaBackend::Context *ctx,
                                       const string& email, RGWUserInfo *info,
                                       RGWObjVersionTracker *objv_tracker,
                                       std::map<std::string, bufferlist>* pattrs,
                                       real_time *pmtime, optional_yield y,
                                       const DoutPrefixProvider *dpp)
{
  std::string oid = email;
  boost::to_lower(oid);
  return get_user_info_from_index(ctx, oid, svc.zone->get_zone_params().user_email_pool,
                                  info, objv_tracker, pattrs, pmtime, y, dpp);
}

/**
 * Given an swift username, finds the user_info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
int RGWSI_User_RADOS::get_user_info_by_swift(RGWSI_MetaBackend::Context *ctx,
                                       const string& swift_name,
                                       RGWUserInfo *info,        /* out */
                                       RGWObjVersionTracker * const objv_tracker,
                                       std::map<std::string, bufferlist>* pattrs,
                                       real_time * const pmtime, optional_yield y,
                                       const DoutPrefixProvider *dpp)
{
  return get_user_info_from_index(ctx,
                                  swift_name,
                                  svc.zone->get_zone_params().user_swift_pool,
                                  info, objv_tracker, pattrs, pmtime, y, dpp);
}

/**
 * Given an access key, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
int RGWSI_User_RADOS::get_user_info_by_access_key(RGWSI_MetaBackend::Context *ctx,
                                            const std::string& access_key,
                                            RGWUserInfo *info,
                                            RGWObjVersionTracker* objv_tracker,
                                            std::map<std::string, bufferlist>* pattrs,
                                            real_time *pmtime, optional_yield y,
                                            const DoutPrefixProvider *dpp)
{
  return get_user_info_from_index(ctx,
                                  access_key,
                                  svc.zone->get_zone_params().user_keys_pool,
                                  info, objv_tracker, pattrs, pmtime, y, dpp);
}

int RGWSI_User_RADOS::read_email_index(const DoutPrefixProvider* dpp,
                                       optional_yield y,
                                       std::string_view email,
                                       RGWUID& uid)
{
  const rgw_pool& pool = svc.zone->get_zone_params().user_email_pool;
  std::string oid{email};
  boost::to_lower(oid);
  return read_index(dpp, y, svc.sysobj, pool, oid, nullptr, uid);
}
