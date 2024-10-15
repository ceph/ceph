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

#include "role.h"

#include <optional>
#include <variant>

#include "common/errno.h"
#include "rgw_common.h"
#include "rgw_metadata.h"
#include "rgw_metadata_lister.h"
#include "rgw_role.h"
#include "rgw_string.h"
#include "rgw_tools.h"
#include "rgw_zone.h"
#include "svc_mdlog.h"

#include "account.h"
#include "roles.h"

namespace rgwrados::role {

// RGWRoleInfo is stored in rados objects named "roles.{id}",
// where ids are assumed to be globally unique
static const std::string oid_prefix = "roles.";
// read_by_name() is enabled by rados objects
// "{tenant}role_names.{name}" for tenant roles, or
// "{account}role_names.{name}" for account roles
constexpr std::string_view name_oid_prefix = "role_names.";
// list() by path/prefix is enabled by rados objects
// "{tenant}role_paths.{path}roles.{id}" for tenant roles.
// see rgwrados::roles::list() for account roles
constexpr std::string_view path_oid_prefix = "role_paths.";


static rgw_raw_obj get_id_obj(const RGWZoneParams& zone,
                              std::string_view id)
{
  return {zone.roles_pool, string_cat_reserve(oid_prefix, id)};
}

static int read_info(const DoutPrefixProvider* dpp, optional_yield y,
                     RGWSI_SysObj& sysobj, const rgw_raw_obj& obj,
                     RGWRoleInfo& info, ceph::real_time* pmtime,
                     RGWObjVersionTracker* pobjv,
                     rgw_cache_entry_info* pcache_info)
{
  bufferlist bl;
  std::map<std::string, bufferlist> attrs;
  // "tagging" doesn't start with RGW_ATTR_PREFIX, don't filter it out
  constexpr bool raw_attrs = true;
  int r = rgw_get_system_obj(&sysobj, obj.pool, obj.oid, bl, pobjv,
                             pmtime, y, dpp, &attrs, pcache_info,
                             boost::none, raw_attrs);
  if (r < 0) {
    return r;
  }

  try {
    auto p = bl.cbegin();
    decode(info, p);
  } catch (const buffer::error&) {
    return -EIO;
  }

  if (auto i = attrs.find("tagging"); i != attrs.end()) {
    try {
      using ceph::decode;
      auto p = i->second.cbegin();
      decode(info.tags, p);
    } catch (const buffer::error&) {
      ldpp_dout(dpp, 0) << "ERROR: failed to decode attrs " << info.id << dendl;
      return -EIO;
    }
  }

  return 0;
}

int read_by_id(const DoutPrefixProvider* dpp, optional_yield y,
               RGWSI_SysObj& sysobj, const RGWZoneParams& zone,
               std::string_view role_id, RGWRoleInfo& info,
               ceph::real_time* pmtime, RGWObjVersionTracker* pobjv,
               rgw_cache_entry_info* pcache_info)
{
  const rgw_raw_obj& obj = get_id_obj(zone, role_id);
  return read_info(dpp, y, sysobj, obj, info, pmtime, pobjv, pcache_info);
}

static int write_info(const DoutPrefixProvider* dpp, optional_yield y,
                      RGWSI_SysObj& sysobj, const RGWZoneParams& zone,
                      const RGWRoleInfo& info, RGWObjVersionTracker& objv,
                      ceph::real_time mtime, bool exclusive)
{
  std::map<std::string, bufferlist> attrs;
  if (!info.tags.empty()) {
    using ceph::encode;
    bufferlist tagbl;
    encode(info.tags, tagbl);
    attrs.emplace("tagging", std::move(tagbl));
  }

  bufferlist bl;
  encode(info, bl);

  const rgw_raw_obj& obj = get_id_obj(zone, info.id);
  int r = rgw_put_system_obj(dpp, &sysobj, obj.pool, obj.oid,
                             bl, exclusive, &objv, mtime, y, &attrs);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to write role obj " << obj
        << " with: " << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

struct IndexObj {
  rgw_raw_obj obj;
  RGWObjVersionTracker objv;
};

static rgw_raw_obj get_name_obj(const RGWZoneParams& zone,
                                std::string_view tenant,
                                const rgw_account_id& account,
                                std::string_view name)
{
  if (account.empty()) {
    // use tenant as prefix
    std::string oid = string_cat_reserve(tenant, name_oid_prefix, name);
    return {zone.roles_pool, std::move(oid)};
  } else {
    // names are case-insensitive, so store them in lower case
    std::string lower_name{name};
    boost::algorithm::to_lower(lower_name);
    // use account id as prefix
    std::string oid = string_cat_reserve(account, name_oid_prefix, lower_name);
    return {zone.roles_pool, std::move(oid)};
  }
}
static rgw_raw_obj get_name_obj(const RGWZoneParams& zone,
                                const RGWRoleInfo& info)
{
  return get_name_obj(zone, info.tenant, info.account_id, info.name);
}

static int write_name(const DoutPrefixProvider* dpp, optional_yield y,
                      RGWSI_SysObj& sysobj, const std::string& role_id,
                      IndexObj& index)
{
  RGWNameToId nameToId;
  nameToId.obj_id = role_id;

  bufferlist bl;
  encode(nameToId, bl);

  return rgw_put_system_obj(dpp, &sysobj, index.obj.pool, index.obj.oid, bl,
                            true, &index.objv, ceph::real_time(), y);
}

static int read_name(const DoutPrefixProvider* dpp, optional_yield y,
                     RGWSI_SysObj& sysobj, IndexObj& name,
                     RGWNameToId& name_to_id)
{
  bufferlist bl;
  int r = rgw_get_system_obj(&sysobj, name.obj.pool, name.obj.oid,
                             bl, &name.objv, nullptr, y, dpp);
  if (r < 0) {
    ldpp_dout(dpp, 4) << "failed to read role name object " << name.obj
        << " with: " << cpp_strerror(r) << dendl;
    return r;
  }

  try {
    auto p = bl.cbegin();
    decode(name_to_id, p);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 4) << "failed to decode role name object: "
        << e.what() << dendl;
    return -EIO;
  }
  return 0;
}

static int remove_index(const DoutPrefixProvider* dpp, optional_yield y,
                        RGWSI_SysObj& sysobj, IndexObj& index)
{
  int r = rgw_delete_system_obj(dpp, &sysobj, index.obj.pool,
                                index.obj.oid, &index.objv, y);
  if (r < 0) {
    ldpp_dout(dpp, 20) << "WARNING: failed to remove "
        << index.obj << " with " << cpp_strerror(r) << dendl;
  }
  return r;
}

using NameIndex = std::optional<IndexObj>;

static int remove_index(const DoutPrefixProvider* dpp, optional_yield y,
                        RGWSI_SysObj& sysobj, NameIndex& index)
{
  if (index) {
    return remove_index(dpp, y, sysobj, *index);
  }
  return 0;
}


static rgw_raw_obj get_tenant_path_obj(const RGWZoneParams& zone,
                                       const RGWRoleInfo& info)
{
  std::string oid = string_cat_reserve(info.tenant, path_oid_prefix,
                                       info.path, oid_prefix, info.id);
  return {zone.roles_pool, std::move(oid)};
}

static int write_tenant_path(const DoutPrefixProvider* dpp, optional_yield y,
                             RGWSI_SysObj& sysobj, IndexObj& path)
{
  bufferlist bl;
  return rgw_put_system_obj(dpp, &sysobj, path.obj.pool, path.obj.oid, bl,
                            true, &path.objv, ceph::real_time(), y);
}

static int read_tenant_path(const DoutPrefixProvider* dpp, optional_yield y,
                            RGWSI_SysObj& sysobj, IndexObj& path)
{
  bufferlist bl;
  return rgw_get_system_obj(&sysobj, path.obj.pool, path.obj.oid,
                            bl, &path.objv, nullptr, y, dpp);
}

struct AccountIndex {
  rgw_raw_obj obj;
  std::string_view name;
};

static int remove_index(const DoutPrefixProvider* dpp,
                        optional_yield y, librados::Rados& rados,
                        const AccountIndex& index)
{
  return roles::remove(dpp, y, rados, index.obj, index.name);
}

using PathIndex = std::variant<std::monostate, IndexObj, AccountIndex>;

static int write_path(const DoutPrefixProvider* dpp, optional_yield y,
                      librados::Rados& rados, RGWSI_SysObj& sysobj,
                      const RGWZoneParams& zone, const RGWRoleInfo& info,
                      PathIndex& index)
{
  if (!info.account_id.empty()) {
    // add the new role to its account
    AccountIndex path;
    path.obj = account::get_roles_obj(zone, info.account_id);
    path.name = info.name;

    constexpr bool exclusive = true;
    constexpr uint32_t no_limit = std::numeric_limits<uint32_t>::max();
    int r = roles::add(dpp, y, rados, path.obj, info, exclusive, no_limit);
    if (r < 0) {
      ldpp_dout(dpp, 1) << "failed to add role to account "
          << path.obj << " with: " << cpp_strerror(r) << dendl;
      return r;
    }
    index = std::move(path);
  } else {
    // write the new path object
    IndexObj path;
    path.obj = get_tenant_path_obj(zone, info);
    path.objv.generate_new_write_ver(dpp->get_cct());

    int r = write_tenant_path(dpp, y, sysobj, path);
    if (r < 0) {
      ldpp_dout(dpp, 1) << "failed to write role path obj "
          << path.obj << " with: " << cpp_strerror(r) << dendl;
      return r;
    }
    index = std::move(path);
  }
  return 0;
}

static int remove_index(const DoutPrefixProvider* dpp,
                        optional_yield y, librados::Rados& rados,
                        RGWSI_SysObj& sysobj, PathIndex& index)
{
  return std::visit(fu2::overload(
          [&] (std::monostate&) { return 0; },
          [&] (IndexObj& path) {
            return remove_index(dpp, y, sysobj, path);
          },
          [&] (AccountIndex& path) {
            return remove_index(dpp, y, rados, path);
          }), index);
}

int read_by_name(const DoutPrefixProvider* dpp, optional_yield y,
                 RGWSI_SysObj& sysobj, const RGWZoneParams& zone,
                 std::string_view tenant, const rgw_account_id& account,
                 std::string_view name, RGWRoleInfo& info,
                 ceph::real_time* pmtime, RGWObjVersionTracker* pobjv,
                 rgw_cache_entry_info* pcache_info)
{
  IndexObj n;
  n.obj = get_name_obj(zone, tenant, account, name);

  RGWNameToId name_to_id;
  int r = read_name(dpp, y, sysobj, n, name_to_id);
  if (r < 0) {
    return r;
  }

  return read_by_id(dpp, y, sysobj, zone, name_to_id.obj_id,
                    info, pmtime, pobjv, pcache_info);
}

int write(const DoutPrefixProvider* dpp, optional_yield y,
          librados::Rados& rados, RGWSI_SysObj& sysobj, RGWSI_MDLog* mdlog,
          const RGWZoneParams& zone, const RGWRoleInfo& info,
          RGWObjVersionTracker& objv, ceph::real_time mtime,
          bool exclusive)
{
  int r = 0;

  // read existing info in case we need to remove its name/path
  RGWRoleInfo old;
  RGWRoleInfo* old_info = nullptr;
  if (!exclusive) {
    r = read_by_id(dpp, y, sysobj, zone, info.id, old);
    if (r == -ENOENT) {
    } else if (r < 0) {
      return r;
    } else {
      old_info = &old;
    }
  }

  const bool same_name = old_info &&
      old_info->tenant == info.tenant &&
      old_info->account_id == info.account_id &&
      old_info->name == info.name;
  const bool same_path = old_info &&
      old_info->tenant == info.tenant &&
      old_info->account_id == info.account_id &&
      old_info->path == info.path;

  NameIndex remove_name;
  PathIndex remove_path;
  if (old_info) {
    if (old_info->id != info.id) {
      ldpp_dout(dpp, 1) << "ERROR: can't modify role id" << dendl;
      return -EINVAL;
    }
    if (!same_name && !old_info->name.empty()) {
      IndexObj name;
      name.obj = get_name_obj(zone, *old_info);
      RGWNameToId name_to_id;
      r = read_name(dpp, y, sysobj, name, name_to_id);
      if (r == -ENOENT) {
        // leave remove_name empty
      } else if (r < 0) {
        return r;
      } else if (name_to_id.obj_id == info.id) {
        remove_name = std::move(name);
      }
    }
    if (!same_path) {
      if (!old_info->account_id.empty()) {
        AccountIndex path;
        path.obj = account::get_roles_obj(zone, old_info->account_id);
        path.name = old_info->name;
        remove_path = std::move(path);
      } else {
        // look up tenant path
        IndexObj path;
        path.obj = get_tenant_path_obj(zone, *old_info);
        r = read_tenant_path(dpp, y, sysobj, path);
        if (r == -ENOENT) {
          // leave remove_path empty
        } else if (r < 0) {
          return r;
        } else {
          remove_path = std::move(path);
        }
      }
    }
  } // old_info

  // write the new name object, fail on conflict
  NameIndex new_name;
  if (!same_name && !info.name.empty()) {
    IndexObj name;
    name.obj = get_name_obj(zone, info);
    name.objv.generate_new_write_ver(dpp->get_cct());

    r = write_name(dpp, y, sysobj, info.id, name);
    if (r < 0) {
      ldpp_dout(dpp, 1) << "failed to write name obj "
          << name.obj << " with: " << cpp_strerror(r) << dendl;
      return r;
    }
    new_name = std::move(name);
  }

  // check for path conflict
  PathIndex new_path;
  if (!same_path) {
    r = write_path(dpp, y, rados, sysobj, zone, info, new_path);
    if (r < 0) {
      // roll back new name object
      std::ignore = remove_index(dpp, y, sysobj, new_name);
      return r;
    }
  }

  // write info by id
  r = write_info(dpp, y, sysobj, zone, info, objv, mtime, exclusive);
  if (r < 0) {
    // roll back the new name/path indices
    std::ignore = remove_index(dpp, y, sysobj, new_name);
    std::ignore = remove_index(dpp, y, rados, sysobj, new_path);
    return r;
  }

  // remove the old name/path indices
  std::ignore = remove_index(dpp, y, sysobj, remove_name);
  std::ignore = remove_index(dpp, y, rados, sysobj, remove_path);

  // record in the mdlog on success
  if (mdlog) {
    return mdlog->complete_entry(dpp, y, "roles", info.id, &objv);
  }
  return 0;
}

static int remove_by_id(const DoutPrefixProvider* dpp, optional_yield y,
                        librados::Rados& rados, RGWSI_SysObj& sysobj,
                        RGWSI_MDLog* mdlog, const RGWZoneParams& zone,
                        std::string_view role_id)
{
  const rgw_raw_obj& obj = get_id_obj(zone, role_id);

  RGWRoleInfo info;
  int r = read_info(dpp, y, sysobj, obj, info,
                    nullptr, &info.objv_tracker, nullptr);
  if (r < 0) {
    return r;
  }

  // delete role info
  r = rgw_delete_system_obj(dpp, &sysobj, obj.pool, obj.oid,
                            &info.objv_tracker, y);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to remove role "
        << info.id << " with: " << cpp_strerror(r) << dendl;
    return r;
  }

  // delete the name object
  if (!info.name.empty()) {
    IndexObj name;
    name.obj = get_name_obj(zone, info);
    std::ignore = remove_index(dpp, y, sysobj, name);
  }

  // delete the path object
  if (!info.account_id.empty()) {
    AccountIndex path;
    path.obj = account::get_roles_obj(zone, info.account_id);
    path.name = info.name;
    std::ignore = remove_index(dpp, y, rados, path);
  } else {
    IndexObj path;
    path.obj = get_tenant_path_obj(zone, info);
    std::ignore = remove_index(dpp, y, sysobj, path);
  }

  // record in the mdlog on success
  if (mdlog) {
    return mdlog->complete_entry(dpp, y, "roles", info.id, &info.objv_tracker);
  }
  return 0;
}

int remove(const DoutPrefixProvider* dpp, optional_yield y,
           librados::Rados& rados, RGWSI_SysObj& sysobj, RGWSI_MDLog* mdlog,
           const RGWZoneParams& zone, std::string_view tenant,
           const rgw_account_id& account, std::string_view name)
{
  IndexObj n;
  n.obj = get_name_obj(zone, tenant, account, name);
  RGWNameToId name_to_id;

  int r = read_name(dpp, y, sysobj, n, name_to_id);
  if (r < 0) {
    return r;
  }

  return remove_by_id(dpp, y, rados, sysobj, mdlog, zone, name_to_id.obj_id);
}


int list_tenant(const DoutPrefixProvider* dpp, optional_yield y,
                RGWSI_SysObj& sysobj, const RGWZoneParams& zone,
                std::string_view tenant, const std::string& marker,
                int max_items, std::string_view path_prefix,
                std::vector<RGWRoleInfo>& roles, std::string& next_marker)
{
  // List all roles if path prefix is empty
  std::string prefix;
  if (!path_prefix.empty()) {
    prefix = string_cat_reserve(tenant, path_oid_prefix, path_prefix);
  } else {
    prefix = string_cat_reserve(tenant, path_oid_prefix, "/");
  }

  auto pool = sysobj.get_pool(zone.roles_pool);
  auto listing = pool.op();
  int r = listing.init(dpp, marker, prefix);
  if (r < 0) {
    return r;
  }

  std::vector<std::string> oids;
  bool truncated = false;
  r = listing.get_next(dpp, max_items, &oids, &truncated);
  if (r < 0) {
    return r;
  }

  for (auto& oid : oids) {
    // remove the entire prefix
    oid.erase(0, prefix.size());
    // verify that the entire oid_prefix is still present (path_prefix may have
    // matched part of it)
    size_t pos = oid.rfind(oid_prefix);
    if (pos == std::string::npos) {
      continue;
    }
    // after trimming the oid_prefix, we should be left with just the role id
    oid.erase(0, pos + oid_prefix.size());

    RGWRoleInfo info;
    r = read_by_id(dpp, y, sysobj, zone, oid, info, nullptr, nullptr, nullptr);
    if (r == -ENOENT) {
      continue; // ok, listing race with deletion
    }
    if (r < 0) {
      return r;
    }
    roles.push_back(std::move(info));
  }

  if (truncated) {
    listing.get_marker(&next_marker);
  }
  return 0;
}


class MetadataObject : public RGWMetadataObject {
  RGWRoleInfo info;
public:
  MetadataObject(const RGWRoleInfo& info, const obj_version& v, real_time m)
    : RGWMetadataObject(v, m), info(info) {}

  void dump(Formatter *f) const override {
    info.dump(f);
  }

  RGWRoleInfo& get_role_info() {
    return info;
  }
};

class MetadataLister : public RGWMetadataLister {
 public:
  using RGWMetadataLister::RGWMetadataLister;

  virtual void filter_transform(std::vector<std::string>& oids,
                                std::list<std::string>& keys) {
    // remove the oid prefix from keys
    constexpr auto trim = [] (const std::string& oid) {
      return oid.substr(oid_prefix.size());
    };
    std::transform(oids.begin(), oids.end(),
                   std::back_inserter(keys),
                   trim);
  }
};

class MetadataHandler : public RGWMetadataHandler {
  librados::Rados& rados;
  RGWSI_SysObj& sysobj;
  RGWSI_MDLog& mdlog;
  const RGWZoneParams& zone;
 public:
  MetadataHandler(librados::Rados& rados, RGWSI_SysObj& sysobj,
                  RGWSI_MDLog& mdlog, const RGWZoneParams& zone)
    : rados(rados), sysobj(sysobj), mdlog(mdlog), zone(zone) {}

  std::string get_type() final { return "roles";  }

  RGWMetadataObject* get_meta_obj(JSONObj *jo,
                                  const obj_version& objv,
                                  const ceph::real_time& mtime) override
  {
    RGWRoleInfo info;

    try {
      info.decode_json(jo);
    } catch (JSONDecoder:: err& e) {
      return nullptr;
    }

    return new MetadataObject(info, objv, mtime);
  }

  int get(std::string& entry, RGWMetadataObject** obj,
          optional_yield y, const DoutPrefixProvider* dpp) override
  {
    RGWRoleInfo info;
    int ret = read_by_id(dpp, y, sysobj, zone, entry, info,
                         &info.mtime, &info.objv_tracker);
    if (ret < 0) {
      return ret;
    }

    *obj = new MetadataObject(info, info.objv_tracker.read_version, info.mtime);
    return 0;
  }

  int put(std::string& entry, RGWMetadataObject* obj,
          RGWObjVersionTracker& objv_tracker,
          optional_yield y, const DoutPrefixProvider* dpp,
          RGWMDLogSyncType type, bool from_remote_zone) override
  {
    auto robj = static_cast<MetadataObject*>(obj);
    auto& info = robj->get_role_info();
    info.mtime = robj->get_mtime();

    constexpr bool exclusive = false;
    int ret = write(dpp, y, rados, sysobj, &mdlog, zone, info,
                    info.objv_tracker, info.mtime, exclusive);
    return ret < 0 ? ret : STATUS_APPLIED;
  }

  int remove(std::string& entry, RGWObjVersionTracker& objv_tracker,
             optional_yield y, const DoutPrefixProvider *dpp) override
  {
    return remove_by_id(dpp, y, rados, sysobj, &mdlog, zone, entry);
  }

  int mutate(const std::string& entry, const ceph::real_time& mtime,
             RGWObjVersionTracker* objv_tracker, optional_yield y,
             const DoutPrefixProvider* dpp, RGWMDLogStatus op_type,
             std::function<int()> f) override
  {
    return -ENOTSUP; // unused
  }

  int list_keys_init(const DoutPrefixProvider* dpp,
                     const std::string& marker,
                     void** phandle) override
  {
    const auto& pool = zone.roles_pool;
    auto lister = std::make_unique<MetadataLister>(sysobj.get_pool(pool));
    int ret = lister->init(dpp, marker, oid_prefix);
    if (ret < 0) {
      return ret;
    }
    *phandle = lister.release(); // release ownership
    return 0;
  }

  int list_keys_next(const DoutPrefixProvider* dpp,
                     void* handle, int max,
                     std::list<std::string>& keys,
                     bool* truncated) override
  {
    auto lister = static_cast<RGWMetadataLister*>(handle);
    return lister->get_next(dpp, max, keys, truncated);
  }

  void list_keys_complete(void *handle) override
  {
    delete static_cast<RGWMetadataLister*>(handle);
  }

  std::string get_marker(void *handle) override
  {
    auto lister = static_cast<RGWMetadataLister*>(handle);
    return lister->get_marker();
  }
};


auto create_metadata_handler(librados::Rados& rados,
                             RGWSI_SysObj& sysobj,
                             RGWSI_MDLog& mdlog,
                             const RGWZoneParams& zone)
    -> std::unique_ptr<RGWMetadataHandler>
{
  return std::make_unique<MetadataHandler>(rados, sysobj, mdlog, zone);
}

} // rgwrados::role
