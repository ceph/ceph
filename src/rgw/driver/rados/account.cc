// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "account.h"

#include <boost/algorithm/string.hpp>
#include "include/rados/librados.hpp"
#include "cls/user/cls_user_types.h"
#include "common/errno.h"
#include "rgw_account.h"
#include "rgw_common.h"
#include "rgw_metadata.h"
#include "rgw_metadata_lister.h"
#include "rgw_obj_types.h"
#include "rgw_string.h"
#include "rgw_tools.h"
#include "rgw_user.h"
#include "rgw_zone.h"
#include "services/svc_sys_obj.h"

namespace rgwrados::account {

static constexpr std::string_view buckets_oid_prefix = "buckets.";
static constexpr std::string_view users_oid_prefix = "users.";
static constexpr std::string_view groups_oid_prefix = "groups.";
static constexpr std::string_view roles_oid_prefix = "roles.";
static constexpr std::string_view topics_oid_prefix = "topics.";
static const std::string account_oid_prefix = "account.";
static constexpr std::string_view name_oid_prefix = "name.";

// metadata keys/objects
static std::string get_buckets_key(std::string_view account_id) {
  return string_cat_reserve(buckets_oid_prefix, account_id);
}
rgw_raw_obj get_buckets_obj(const RGWZoneParams& zone,
                            std::string_view account_id) {
  return {zone.account_pool, get_buckets_key(account_id)};
}

static std::string get_users_key(std::string_view account_id) {
  return string_cat_reserve(users_oid_prefix, account_id);
}
rgw_raw_obj get_users_obj(const RGWZoneParams& zone,
                          std::string_view account_id) {
  return {zone.account_pool, get_users_key(account_id)};
}

static std::string get_groups_key(std::string_view account_id) {
  return string_cat_reserve(groups_oid_prefix, account_id);
}
rgw_raw_obj get_groups_obj(const RGWZoneParams& zone,
                           std::string_view account_id) {
  return {zone.account_pool, get_groups_key(account_id)};
}

static std::string get_roles_key(std::string_view account_id) {
  return string_cat_reserve(roles_oid_prefix, account_id);
}
rgw_raw_obj get_roles_obj(const RGWZoneParams& zone,
                          std::string_view account_id) {
  return {zone.account_pool, get_roles_key(account_id)};
}

static std::string get_topics_key(std::string_view account_id) {
  return string_cat_reserve(topics_oid_prefix, account_id);
}
rgw_raw_obj get_topics_obj(const RGWZoneParams& zone,
                          std::string_view account_id) {
  return {zone.account_pool, get_topics_key(account_id)};
}

static std::string get_account_key(std::string_view account_id) {
  return string_cat_reserve(account_oid_prefix, account_id);
}
static rgw_raw_obj get_account_obj(const RGWZoneParams& zone,
                                   std::string_view account_id) {
  return {zone.account_pool, get_account_key(account_id)};
}

static std::string get_name_key(std::string_view tenant,
                                std::string_view name) {
  return string_cat_reserve(name_oid_prefix, tenant, "$", name);
}
static rgw_raw_obj get_name_obj(const RGWZoneParams& zone,
                                std::string_view tenant,
                                std::string_view name) {
  return {zone.account_pool, get_name_key(tenant, name)};
}

// store in lower case for case-insensitive matching
static std::string get_email_key(std::string_view email) {
  auto lower = std::string{email};
  boost::to_lower(lower);
  return lower;
}
// note that account email oids conflict with user email oids. this ensures
// that all emails are globally unique. we rely on rgw::account::validate_id()
// to distinguish between user and account ids
static rgw_raw_obj get_email_obj(const RGWZoneParams& zone,
                                 std::string_view email) {
  return {zone.user_email_pool, get_email_key(email)};
}


struct RedirectObj {
  rgw_raw_obj obj;
  RGWUID data;
  RGWObjVersionTracker objv;
};

static int read_redirect(const DoutPrefixProvider* dpp,
                         optional_yield y,
                         RGWSI_SysObj& sysobj,
                         RedirectObj& redirect)
{
  bufferlist bl;
  int r = rgw_get_system_obj(&sysobj, redirect.obj.pool, redirect.obj.oid,
                             bl, &redirect.objv, nullptr, y, dpp);
  if (r < 0) {
    ldpp_dout(dpp, 20) << "failed to read " << redirect.obj.oid
        << " with: " << cpp_strerror(r) << dendl;
    return r;
  }

  try {
    auto p = bl.cbegin();
    decode(redirect.data, p);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode account redirect: "
        << e.what() << dendl;
    return -EIO;
  }
  return 0;
}

static int write_redirect(const DoutPrefixProvider* dpp,
                          optional_yield y,
                          RGWSI_SysObj& sysobj,
                          RedirectObj& redirect)
{
  bufferlist bl;
  encode(redirect.data, bl);

  constexpr bool exclusive = true;
  return rgw_put_system_obj(dpp, &sysobj, redirect.obj.pool,
                            redirect.obj.oid, bl, exclusive,
                            &redirect.objv, ceph::real_time{}, y);
}


int read(const DoutPrefixProvider* dpp,
         optional_yield y,
         RGWSI_SysObj& sysobj,
         const RGWZoneParams& zone,
         std::string_view account_id,
         RGWAccountInfo& info,
         std::map<std::string, ceph::buffer::list>& attrs,
         ceph::real_time& mtime,
         RGWObjVersionTracker& objv)
{
  const rgw_raw_obj obj = get_account_obj(zone, account_id);

  bufferlist bl;
  int r = rgw_get_system_obj(&sysobj, obj.pool, obj.oid, bl,
                             &objv, &mtime, y, dpp, &attrs);
  if (r < 0) {
    ldpp_dout(dpp, 20) << "account lookup with id=" << account_id
        << " failed: " << cpp_strerror(r) << dendl;
    return r;
  }

  try {
    auto p = bl.cbegin();
    decode(info, p);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode account info: "
        << e.what() << dendl;
    return -EIO;
  }
  if (info.id != account_id) {
    ldpp_dout(dpp, 0) << "ERROR: read account id mismatch "
        << info.id << " != " << account_id << dendl;
    return -EIO;
  }
  return 0;
}

int read_by_name(const DoutPrefixProvider* dpp,
                 optional_yield y,
                 RGWSI_SysObj& sysobj,
                 const RGWZoneParams& zone,
                 std::string_view tenant,
                 std::string_view name,
                 RGWAccountInfo& info,
                 std::map<std::string, ceph::buffer::list>& attrs,
                 RGWObjVersionTracker& objv)
{
  auto redirect = RedirectObj{.obj = get_name_obj(zone, tenant, name)};
  int r = read_redirect(dpp, y, sysobj, redirect);
  if (r < 0) {
    return r;
  }
  ceph::real_time mtime; // ignored
  return read(dpp, y, sysobj, zone, redirect.data.id, info, attrs, mtime, objv);
}

int read_by_email(const DoutPrefixProvider* dpp,
                  optional_yield y,
                  RGWSI_SysObj& sysobj,
                  const RGWZoneParams& zone,
                  std::string_view email,
                  RGWAccountInfo& info,
                  std::map<std::string, ceph::buffer::list>& attrs,
                  RGWObjVersionTracker& objv)
{
  auto redirect = RedirectObj{.obj = get_email_obj(zone, email)};
  int r = read_redirect(dpp, y, sysobj, redirect);
  if (r < 0) {
    return r;
  }
  if (!rgw::account::validate_id(redirect.data.id)) {
    // this index is used for a user, not an account
    return -ENOENT;
  }
  ceph::real_time mtime; // ignored
  return read(dpp, y, sysobj, zone, redirect.data.id, info, attrs, mtime, objv);
}


int write(const DoutPrefixProvider* dpp,
          optional_yield y,
          RGWSI_SysObj& sysobj,
          const RGWZoneParams& zone,
          const RGWAccountInfo& info,
          const RGWAccountInfo* old_info,
          const std::map<std::string, ceph::buffer::list>& attrs,
          ceph::real_time mtime,
          bool exclusive,
          RGWObjVersionTracker& objv)
{
  const rgw_raw_obj obj = get_account_obj(zone, info.id);

  const bool same_name = old_info
      && old_info->tenant == info.tenant
      && old_info->name == info.name;
  const bool same_email = old_info
      && boost::iequals(old_info->email, info.email);

  std::optional<RedirectObj> remove_name;
  std::optional<RedirectObj> remove_email;
  if (old_info) {
    if (old_info->id != info.id) {
      ldpp_dout(dpp, 1) << "ERROR: can't modify account id" << dendl;
      return -EINVAL;
    }
    if (!same_name && !old_info->name.empty()) {
      // read old account name object
      RedirectObj redirect;
      redirect.obj = get_name_obj(zone, old_info->tenant, old_info->name);
      int r = read_redirect(dpp, y, sysobj, redirect);
      if (r == -ENOENT) {
        // leave remove_name empty
      } else if (r < 0) {
        return r;
      } else if (redirect.data.id == info.id) {
        remove_name = std::move(redirect);
      }
    }
    if (!same_email && !old_info->email.empty()) {
      // read old account email object
      RedirectObj redirect;
      redirect.obj = get_email_obj(zone, old_info->email);
      int r = read_redirect(dpp, y, sysobj, redirect);
      if (r == -ENOENT) {
        // leave remove_email empty
      } else if (r < 0) {
        return r;
      } else if (redirect.data.id == info.id) {
        remove_email = std::move(redirect);
      }
    }
  } // old_info

  if (!same_name && !info.name.empty()) {
    // read new account name object
    RedirectObj redirect;
    redirect.obj = get_name_obj(zone, info.tenant, info.name);
    int r = read_redirect(dpp, y, sysobj, redirect);
    if (r == -ENOENT) {
      // write the new name object below
    } else if (r == 0) {
      ldpp_dout(dpp, 1) << "ERROR: account name obj " << redirect.obj
          << " already taken for account id " << redirect.data.id << dendl;
      return -EEXIST;
    } else if (r < 0) {
      return r;
    }
  }

  if (!same_email && !info.email.empty()) {
    // read new account email object
    RedirectObj redirect;
    redirect.obj = get_email_obj(zone, info.email);
    int r = read_redirect(dpp, y, sysobj, redirect);
    if (r == -ENOENT) {
      // write the new email object below
    } else if (r == 0) {
      ldpp_dout(dpp, 1) << "ERROR: account email obj " << redirect.obj
          << " already taken for " << redirect.data.id << dendl;
      return -EEXIST;
    } else if (r < 0) {
      return r;
    }
  }

  // encode/write the account info
  {
    bufferlist bl;
    encode(info, bl);

    const rgw_raw_obj obj = get_account_obj(zone, info.id);
    int r = rgw_put_system_obj(dpp, &sysobj, obj.pool, obj.oid, bl,
                               exclusive, &objv, mtime, y, &attrs);
    if (r < 0) {
      ldpp_dout(dpp, 1) << "ERROR: failed to write account obj " << obj
          << " with: " << cpp_strerror(r) << dendl;
      return r;
    }
  }

  if (remove_name) {
    // remove the old name object, ignoring errors
    auto& redirect = *remove_name;
    int r = rgw_delete_system_obj(dpp, &sysobj, redirect.obj.pool,
                                  redirect.obj.oid, &redirect.objv, y);
    if (r < 0) {
      ldpp_dout(dpp, 20) << "WARNING: failed to remove old name obj "
          << redirect.obj.oid << ": " << cpp_strerror(r) << dendl;
    } // not fatal
  }
  if (!same_name && !info.name.empty()) {
    // write the new name object
    RedirectObj redirect;
    redirect.obj = get_name_obj(zone, info.tenant, info.name);
    redirect.data.id = info.id;
    redirect.objv.generate_new_write_ver(dpp->get_cct());

    int r = write_redirect(dpp, y, sysobj, redirect);
    if (r < 0) {
      ldpp_dout(dpp, 20) << "WARNING: failed to write name obj "
          << redirect.obj << " with: " << cpp_strerror(r) << dendl;
    } // not fatal
  }

  if (remove_email) {
    // remove the old email object, ignoring errors
    auto& redirect = *remove_email;
    int r = rgw_delete_system_obj(dpp, &sysobj, redirect.obj.pool,
                                  redirect.obj.oid, &redirect.objv, y);
    if (r < 0) {
      ldpp_dout(dpp, 20) << "WARNING: failed to remove old email obj "
          << redirect.obj.oid << ": " << cpp_strerror(r) << dendl;
    } // not fatal
  }
  if (!same_email && !info.email.empty()) {
    // write the new email object
    RedirectObj redirect;
    redirect.obj = get_email_obj(zone, info.email);
    redirect.data.id = info.id;
    redirect.objv.generate_new_write_ver(dpp->get_cct());

    int r = write_redirect(dpp, y, sysobj, redirect);
    if (r < 0) {
      ldpp_dout(dpp, 20) << "WARNING: failed to write email obj "
          << redirect.obj << " with: " << cpp_strerror(r) << dendl;
    } // not fatal
  }

  return 0;
}

int remove(const DoutPrefixProvider* dpp,
           optional_yield y,
           RGWSI_SysObj& sysobj,
           const RGWZoneParams& zone,
           const RGWAccountInfo& info,
           RGWObjVersionTracker& objv)
{
  const rgw_raw_obj obj = get_account_obj(zone, info.id);
  int r = rgw_delete_system_obj(dpp, &sysobj, obj.pool, obj.oid, &objv, y);
  if (r < 0) {
      ldpp_dout(dpp, 1) << "ERROR: failed to remove account obj "
          << obj << " with: " << cpp_strerror(r) << dendl;
    return r;
  }

  if (!info.name.empty()) {
    // remove the name object
    const rgw_raw_obj obj = get_name_obj(zone, info.tenant, info.name);
    r = rgw_delete_system_obj(dpp, &sysobj, obj.pool, obj.oid, nullptr, y);
    if (r < 0) {
      ldpp_dout(dpp, 20) << "WARNING: failed to remove name obj "
        << obj << " with: " << cpp_strerror(r) << dendl;
    } // not fatal
  }
  if (!info.email.empty()) {
    // remove the email object
    const rgw_raw_obj obj = get_email_obj(zone, info.email);
    r = rgw_delete_system_obj(dpp, &sysobj, obj.pool, obj.oid, nullptr, y);
    if (r < 0) {
      ldpp_dout(dpp, 20) << "WARNING: failed to remove email obj "
        << obj << " with: " << cpp_strerror(r) << dendl;
    } // not fatal
  }
  {
    // remove the users object
    const rgw_raw_obj obj = get_users_obj(zone, info.id);
    r = rgw_delete_system_obj(dpp, &sysobj, obj.pool, obj.oid, nullptr, y);
    if (r < 0) {
      ldpp_dout(dpp, 20) << "WARNING: failed to remove users obj "
        << obj << " with: " << cpp_strerror(r) << dendl;
    } // not fatal
  }

  return 0;
}

// read the resource count from cls_user_account_header
int resource_count(const DoutPrefixProvider* dpp,
                   optional_yield y,
                   librados::Rados& rados,
                   const rgw_raw_obj& obj,
                   uint32_t& count)
{
  rgw_rados_ref ref;
  int r = rgw_get_rados_ref(dpp, &rados, obj, &ref);
  if (r < 0) {
    return r;
  }

  librados::ObjectReadOperation op;
  bufferlist bl;
  int ret = 0;
  op.omap_get_header(&bl, &ret);

  r = ref.operate(dpp, &op, nullptr, y);
  if (r == -ENOENT) { // doesn't exist yet
    count = 0;
    return 0;
  }
  if (r < 0) {
    return r;
  }

  if (!bl.length()) { // exists but no header yet
    count = 0;
    return 0;
  }

  cls_user_account_header header;
  try {
    auto p = bl.cbegin();
    decode(header, p);
  } catch (const buffer::error&) {
    return -EIO;
  }

  count = header.count;
  return 0;
}


// metadata abstraction

struct CompleteInfo {
  RGWAccountInfo info;
  std::map<std::string, bufferlist> attrs;
  bool has_attrs = false;

  void dump(Formatter* f) const {
    info.dump(f);
    encode_json("attrs", attrs, f);
  }

  void decode_json(JSONObj* obj) {
    decode_json_obj(info, obj);
    has_attrs = JSONDecoder::decode_json("attrs", attrs, obj);
  }
};

class MetadataObject : public RGWMetadataObject {
  CompleteInfo aci;
 public:
  MetadataObject(const CompleteInfo& aci, const obj_version& v,
                 ceph::real_time m)
    : RGWMetadataObject(v, m), aci(aci) {}

  void dump(Formatter *f) const override {
    aci.dump(f);
  }

  CompleteInfo& get() { return aci; }
};


class MetadataLister : public RGWMetadataLister {
 public:
  using RGWMetadataLister::RGWMetadataLister;

  void filter_transform(std::vector<std::string>& oids,
                        std::list<std::string>& keys) override
  {
    // remove the oid prefix from keys
    constexpr auto trim = [] (const std::string& oid) {
      return oid.substr(account_oid_prefix.size());
    };
    std::transform(oids.begin(), oids.end(),
                   std::back_inserter(keys),
                   trim);
  }
};

class MetadataHandler : public RGWMetadataHandler {
  RGWSI_SysObj& sysobj;
  const RGWZoneParams& zone;
 public:
  MetadataHandler(RGWSI_SysObj& sysobj, const RGWZoneParams& zone)
    : sysobj(sysobj), zone(zone) {}

  std::string get_type() override { return "account"; }

  RGWMetadataObject* get_meta_obj(JSONObj* obj,
                                  const obj_version& objv,
                                  const ceph::real_time& mtime) override
  {
    CompleteInfo aci;
    try {
      decode_json_obj(aci, obj);
    } catch (const JSONDecoder::err&) {
      return nullptr;
    }
    return new MetadataObject(aci, objv, mtime);
  }

  int get(std::string& entry, RGWMetadataObject** obj,
          optional_yield y, const DoutPrefixProvider* dpp) override
  {
    const std::string& account_id = entry;
    CompleteInfo aci;
    RGWObjVersionTracker objv;
    ceph::real_time mtime;

    int r = read(dpp, y, sysobj, zone, account_id,
                 aci.info, aci.attrs, mtime, objv);
    if (r < 0) {
      return r;
    }

    *obj = new MetadataObject(aci, objv.read_version, mtime);
    return 0;
  }

  int put(std::string& entry, RGWMetadataObject* obj,
          RGWObjVersionTracker& objv, optional_yield y,
          const DoutPrefixProvider* dpp,
          RGWMDLogSyncType type, bool from_remote_zone) override
  {
    const std::string& account_id = entry;
    auto account_obj = static_cast<MetadataObject*>(obj);
    const auto& new_info = account_obj->get().info;

    // account id must match metadata key
    if (new_info.id != account_id) {
      return -EINVAL;
    }

    // read existing metadata
    RGWAccountInfo old_info;
    std::map<std::string, ceph::buffer::list> old_attrs;
    ceph::real_time old_mtime;
    int r = read(dpp, y, sysobj, zone, account_id,
                 old_info, old_attrs, old_mtime, objv);
    if (r < 0 && r != -ENOENT) {
      return r;
    }
    const RGWAccountInfo* pold_info = (r == -ENOENT ? nullptr : &old_info);

    // write/overwrite metadata
    constexpr bool exclusive = false;
    return write(dpp, y, sysobj, zone, new_info, pold_info,
                 account_obj->get().attrs, obj->get_mtime(),
                 exclusive, objv);
  }

  int remove(std::string& entry, RGWObjVersionTracker& objv,
             optional_yield y, const DoutPrefixProvider* dpp) override
  {
    const std::string& account_id = entry;

    // read existing metadata
    RGWAccountInfo info;
    std::map<std::string, ceph::buffer::list> attrs;
    ceph::real_time mtime;
    int r = read(dpp, y, sysobj, zone, account_id,
                 info, attrs, mtime, objv);
    if (r < 0) {
      return r;
    }

    return account::remove(dpp, y, sysobj, zone, info, objv);
  }

  int mutate(const std::string& entry,
             const ceph::real_time& mtime,
             RGWObjVersionTracker* objv,
             optional_yield y,
             const DoutPrefixProvider* dpp,
             RGWMDLogStatus op_type,
             std::function<int()> f) override
  {
    return -ENOTSUP; // unused
  }

  int list_keys_init(const DoutPrefixProvider* dpp,
                     const std::string& marker, void** phandle) override
  {
    auto lister = std::make_unique<MetadataLister>(
        sysobj.get_pool(zone.account_pool));
    int r = lister->init(dpp, marker, account_oid_prefix);
    if (r < 0) {
      return r;
    }
    *phandle = lister.release();
    return 0;
  }

  int list_keys_next(const DoutPrefixProvider* dpp, void* handle, int max,
                     std::list<std::string>& keys, bool* truncated) override
  {
    auto lister = static_cast<MetadataLister*>(handle);
    return lister->get_next(dpp, max, keys, truncated);
  }

  void list_keys_complete(void* handle) override
  {
    delete static_cast<MetadataLister*>(handle);
  }

  std::string get_marker(void* handle) override
  {
    auto lister = static_cast<MetadataLister*>(handle);
    return lister->get_marker();
  }
};

auto create_metadata_handler(RGWSI_SysObj& sysobj, const RGWZoneParams& zone)
    -> std::unique_ptr<RGWMetadataHandler>
{
  return std::make_unique<MetadataHandler>(sysobj, zone);
}

} // namespace rgwrados::account
