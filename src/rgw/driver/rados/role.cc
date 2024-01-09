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
#include "common/errno.h"
#include "rgw_common.h"
#include "rgw_metadata.h"
#include "rgw_metadata_lister.h"
#include "rgw_role.h"
#include "rgw_string.h"
#include "rgw_tools.h"
#include "rgw_zone.h"
#include "svc_mdlog.h"

namespace rgwrados::role {

static const std::string name_oid_prefix = "role_names.";
static const std::string oid_prefix = "roles.";
static const std::string path_oid_prefix = "role_paths.";

int read_by_id(const DoutPrefixProvider* dpp, optional_yield y,
               RGWSI_SysObj& sysobj, const RGWZoneParams& zone,
               const std::string& role_id, RGWRoleInfo& info,
               ceph::real_time* pmtime, RGWObjVersionTracker* pobjv,
               rgw_cache_entry_info* pcache_info)
{
  bufferlist bl;
  std::map<std::string, bufferlist> attrs;
  int r = rgw_get_system_obj(&sysobj, zone.roles_pool, role_id, bl,
                             pobjv, pmtime, y, dpp, &attrs, pcache_info);
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

struct IndexObj {
  rgw_raw_obj obj;
  RGWObjVersionTracker objv;
};

static rgw_raw_obj get_name_obj(const RGWZoneParams& zone,
                                const std::string& tenant,
                                const std::string& name)
{
  std::string oid = string_cat_reserve(tenant, name_oid_prefix, name);
  return {zone.roles_pool, std::move(oid)};
}

static rgw_raw_obj get_path_obj(const RGWZoneParams& zone,
                                const std::string& tenant,
                                const std::string& path,
                                const std::string& id)
{
  std::string oid = string_cat_reserve(tenant, path_oid_prefix, path,
                                       oid_prefix, id);
  return {zone.roles_pool, std::move(oid)};
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
    return r;
  }

  try {
    auto p = bl.cbegin();
    decode(name_to_id, p);
  } catch (const buffer::error&) {
    return -EIO;
  }
  return 0;
}

static int write_path(const DoutPrefixProvider* dpp, optional_yield y,
                      RGWSI_SysObj& sysobj, IndexObj& path)
{
  bufferlist bl;
  return rgw_put_system_obj(dpp, &sysobj, path.obj.pool, path.obj.oid, bl,
                            true, &path.objv, ceph::real_time(), y);
}

static int read_path(const DoutPrefixProvider* dpp, optional_yield y,
                     RGWSI_SysObj& sysobj, IndexObj& path)
{
  bufferlist bl;
  return rgw_get_system_obj(&sysobj, path.obj.pool, path.obj.oid,
                            bl, &path.objv, nullptr, y, dpp);
}

int read_by_name(const DoutPrefixProvider* dpp, optional_yield y,
                 RGWSI_SysObj& sysobj, const RGWZoneParams& zone,
                 const std::string& tenant, const std::string& name,
                 RGWRoleInfo& info,
                 ceph::real_time* pmtime,
                 RGWObjVersionTracker* pobjv,
                 rgw_cache_entry_info* pcache_info)
{
  IndexObj n;
  n.obj = get_name_obj(zone, tenant, name);
  RGWNameToId name_to_id;

  int r = read_name(dpp, y, sysobj, n, name_to_id);
  if (r < 0) {
    return r;
  }

  return read_by_id(dpp, y, sysobj, zone, name_to_id.obj_id,
                    info, pmtime, pobjv, pcache_info);
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

  int r = rgw_put_system_obj(dpp, &sysobj, zone.roles_pool, info.id,
                             bl, exclusive, &objv, mtime, y, &attrs);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to write role obj " << info.id
        << " with: " << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

int write(const DoutPrefixProvider* dpp, optional_yield y,
          RGWSI_SysObj& sysobj, RGWSI_MDLog* mdlog,
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
      old_info->name == info.name;
  const bool same_path = old_info &&
      old_info->tenant == info.tenant &&
      old_info->path == info.path;

  std::optional<IndexObj> remove_name;
  std::optional<IndexObj> remove_path;
  if (old_info) {
    if (old_info->id != info.id) {
      ldpp_dout(dpp, 1) << "ERROR: can't modify role id" << dendl;
      return -EINVAL;
    }
    if (!same_name && !old_info->name.empty()) {
      IndexObj name;
      name.obj = get_name_obj(zone, old_info->tenant, old_info->name);
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
      IndexObj path;
      path.obj = get_path_obj(zone, old_info->tenant, old_info->path, old_info->id);
      r = read_path(dpp, y, sysobj, path);
      if (r == -ENOENT) {
        // leave remove_path empty
      } else if (r < 0) {
        return r;
      } else {
        remove_path = std::move(path);
      }
    }
  } // old_info

  // check for name conflict
  if (!same_name && !info.name.empty()) {
    // read new role name object
    IndexObj name;
    name.obj = get_name_obj(zone, info.tenant, info.name);
    RGWNameToId name_to_id;
    r = read_name(dpp, y, sysobj, name, name_to_id);
    if (r == -ENOENT) {
      // write the new name object below
    } else if (r == 0) {
      ldpp_dout(dpp, 1) << "ERROR: role name obj " << name.obj
          << " already taken for role id " << name_to_id.obj_id << dendl;
      return -EEXIST;
    } else if (r < 0) {
      return r;
    }
  }

  // check for path conflict
  if (!same_path) {
    // read new role path object
    IndexObj path;
    path.obj = get_path_obj(zone, info.tenant, info.path, info.id);
    r = read_path(dpp, y, sysobj, path);
    if (r == -ENOENT) {
      // write the new path object below
    } else if (r == 0) {
      ldpp_dout(dpp, 1) << "ERROR: role path obj " << path.obj
          << " already taken" << dendl;
      return -EEXIST;
    } else if (r < 0) {
      return r;
    }
  }

  // write info by id
  r = write_info(dpp, y, sysobj, zone, info, objv, mtime, exclusive);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to write role obj " << info.id
        << " with: " << cpp_strerror(r) << dendl;
    return r;
  }

  // update name index
  if (remove_name) {
    // remove the old name object, ignoring errors
    auto& name = *remove_name;
    r = rgw_delete_system_obj(dpp, &sysobj, name.obj.pool,
                              name.obj.oid, &name.objv, y);
    if (r < 0) {
      ldpp_dout(dpp, 20) << "WARNING: failed to remove old name obj "
          << name.obj << ": " << cpp_strerror(r) << dendl;
    } // not fatal
  }
  if (!same_name && !info.name.empty()) {
    // write the new name object
    IndexObj name;
    name.obj = get_name_obj(zone, info.tenant, info.name);
    name.objv.generate_new_write_ver(dpp->get_cct());

    r = write_name(dpp, y, sysobj, info.id, name);
    if (r < 0) {
      ldpp_dout(dpp, 20) << "WARNING: failed to write name obj "
          << name.obj << " with: " << cpp_strerror(r) << dendl;
    } // not fatal
  }

  // update path index
  if (remove_path) {
    // remove the old path object, ignoring errors
    auto& path = *remove_path;
    r = rgw_delete_system_obj(dpp, &sysobj, path.obj.pool,
                              path.obj.oid, &path.objv, y);
    if (r < 0) {
      ldpp_dout(dpp, 20) << "WARNING: failed to remove old path obj "
          << path.obj << ": " << cpp_strerror(r) << dendl;
    } // not fatal
  }
  if (!same_path) {
    // write the new path object
    IndexObj path;
    path.obj = get_path_obj(zone, info.tenant, info.path, info.id);
    path.objv.generate_new_write_ver(dpp->get_cct());

    r = write_path(dpp, y, sysobj, path);
    if (r < 0) {
      ldpp_dout(dpp, 20) << "WARNING: failed to write path obj "
          << path.obj << " with: " << cpp_strerror(r) << dendl;
    } // not fatal
  }

  // record in the mdlog on success
  if (mdlog) {
    return mdlog->complete_entry(dpp, y, "roles", info.id, &objv);
  }
  return 0;
}

static int remove_by_id(const DoutPrefixProvider* dpp, optional_yield y,
                        RGWSI_SysObj& sysobj, RGWSI_MDLog* mdlog,
                        const RGWZoneParams& zone, const std::string& role_id)
{
  RGWRoleInfo info;
  int r = read_by_id(dpp, y, sysobj, zone, role_id, info,
                     nullptr, &info.objv_tracker);
  if (r < 0) {
    return r;
  }

  if (! info.perm_policy_map.empty()) {
    return -ERR_DELETE_CONFLICT;
  }

  // delete role info
  r = rgw_delete_system_obj(dpp, &sysobj, zone.roles_pool,
                            info.id, &info.objv_tracker, y);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to remove role "
        << info.id << " with: " << cpp_strerror(r) << dendl;
    return r;
  }

  // delete the name object
  if (!info.name.empty()) {
    IndexObj name;
    name.obj = get_name_obj(zone, info.tenant, info.name);
    r = rgw_delete_system_obj(dpp, &sysobj, name.obj.pool,
                              name.obj.oid, nullptr, y);
    if (r < 0) {
      ldpp_dout(dpp, 20) << "WARNING: failed to remove role name obj "
          << name.obj.oid << " with: " << cpp_strerror(r) << dendl;
    } // not fatal
  }

  // delete the path object
  IndexObj path;
  path.obj = get_path_obj(zone, info.tenant, info.path, info.id);
  r = rgw_delete_system_obj(dpp, &sysobj, path.obj.pool,
                            path.obj.oid, nullptr, y);
  if (r < 0) {
    ldpp_dout(dpp, 20) << "WARNING: failed to remove role path obj "
        << path.obj.oid << " with: " << cpp_strerror(r) << dendl;
  } // not fatal

  // record in the mdlog on success
  if (mdlog) {
    return mdlog->complete_entry(dpp, y, "roles", info.id, &info.objv_tracker);
  }
  return 0;
}

int remove(const DoutPrefixProvider* dpp, optional_yield y,
           RGWSI_SysObj& sysobj, RGWSI_MDLog* mdlog,
           const RGWZoneParams& zone, const std::string& tenant,
           const std::string& name)
{
  IndexObj n;
  n.obj = get_name_obj(zone, tenant, name);
  RGWNameToId name_to_id;

  int r = read_name(dpp, y, sysobj, n, name_to_id);
  if (r < 0) {
    return r;
  }

  return remove_by_id(dpp, y, sysobj, mdlog, zone, name_to_id.obj_id);
}


int list(const DoutPrefixProvider* dpp, optional_yield y,
         RGWSI_SysObj& sysobj, const RGWZoneParams& zone,
         const std::string& tenant, const std::string& marker,
         int max_items, const std::string& path_prefix,
         std::vector<RGWRoleInfo>& roles, std::string& next_marker)
{
  // List all roles if path prefix is empty
  std::string prefix;
  if (! path_prefix.empty()) {
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
  RGWSI_SysObj& sysobj;
  RGWSI_MDLog& mdlog;
  const RGWZoneParams& zone;
 public:
  MetadataHandler(RGWSI_SysObj& sysobj, RGWSI_MDLog& mdlog,
                  const RGWZoneParams& zone)
    : sysobj(sysobj), mdlog(mdlog), zone(zone) {}

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
    int ret = write(dpp, y, sysobj, &mdlog, zone, info,
                    info.objv_tracker, info.mtime, exclusive);
    return ret < 0 ? ret : STATUS_APPLIED;
  }

  int remove(std::string& entry, RGWObjVersionTracker& objv_tracker,
             optional_yield y, const DoutPrefixProvider *dpp) override
  {
    return remove_by_id(dpp, y, sysobj, &mdlog, zone, entry);
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


auto create_metadata_handler(RGWSI_SysObj& sysobj,
                             RGWSI_MDLog& mdlog,
                             const RGWZoneParams& zone)
    -> std::unique_ptr<RGWMetadataHandler>
{
  return std::make_unique<MetadataHandler>(sysobj, mdlog, zone);
}

} // rgwrados::role
