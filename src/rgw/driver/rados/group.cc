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

#include "group.h"

#include <boost/algorithm/string.hpp>
#include "common/errno.h"
#include "account.h"
#include "groups.h"
#include "rgw_common.h"
#include "rgw_metadata.h"
#include "rgw_metadata_lister.h"
#include "rgw_obj_types.h"
#include "rgw_string.h"
#include "rgw_tools.h"
#include "rgw_user.h"
#include "rgw_zone.h"
#include "services/svc_sys_obj.h"

namespace rgwrados::group {

static constexpr std::string_view info_oid_prefix = "info.";
static constexpr std::string_view name_oid_prefix = "name.";
static constexpr std::string_view users_oid_prefix = "users.";

// metadata keys/objects
std::string get_users_key(std::string_view group_id) {
  return string_cat_reserve(users_oid_prefix, group_id);
}
rgw_raw_obj get_users_obj(const RGWZoneParams& zone,
                          std::string_view group_id) {
  return {zone.group_pool, get_users_key(group_id)};
}

static std::string get_group_key(std::string_view group_id) {
  return string_cat_reserve(info_oid_prefix, group_id);
}
static rgw_raw_obj get_group_obj(const RGWZoneParams& zone,
                                 std::string_view group_id) {
  return {zone.group_pool, get_group_key(group_id)};
}

static std::string get_name_key(std::string_view account,
                                std::string_view name) {
  // names are case-insensitive, so store them in lower case
  std::string lower_name{name};
  boost::algorithm::to_lower(lower_name);
  return string_cat_reserve(name_oid_prefix, account, "$", lower_name);
}
static rgw_raw_obj get_name_obj(const RGWZoneParams& zone,
                                std::string_view account,
                                std::string_view name) {
  return {zone.group_pool, get_name_key(account, name)};
}


struct NameObj {
  rgw_raw_obj obj;
  RGWUID data;
  RGWObjVersionTracker objv;
};

static int read_name(const DoutPrefixProvider* dpp,
                     optional_yield y,
                     RGWSI_SysObj& sysobj,
                     NameObj& name)
{
  bufferlist bl;
  int r = rgw_get_system_obj(&sysobj, name.obj.pool, name.obj.oid,
                             bl, &name.objv, nullptr, y, dpp);
  if (r < 0) {
    ldpp_dout(dpp, 20) << "failed to read " << name.obj.oid
        << " with: " << cpp_strerror(r) << dendl;
    return r;
  }

  try {
    auto p = bl.cbegin();
    decode(name.data, p);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode group name: "
        << e.what() << dendl;
    return -EIO;
  }
  return 0;
}

static int write_name(const DoutPrefixProvider* dpp,
                      optional_yield y,
                      RGWSI_SysObj& sysobj,
                      NameObj& name)
{
  bufferlist bl;
  encode(name.data, bl);

  constexpr bool exclusive = true;
  return rgw_put_system_obj(dpp, &sysobj, name.obj.pool,
                            name.obj.oid, bl, exclusive,
                            &name.objv, ceph::real_time{}, y);
}


int read(const DoutPrefixProvider* dpp,
         optional_yield y,
         RGWSI_SysObj& sysobj,
         const RGWZoneParams& zone,
         std::string_view id,
         RGWGroupInfo& info,
         std::map<std::string, ceph::buffer::list>& attrs,
         ceph::real_time& mtime,
         RGWObjVersionTracker& objv)
{
  const rgw_raw_obj obj = get_group_obj(zone, id);

  bufferlist bl;
  int r = rgw_get_system_obj(&sysobj, obj.pool, obj.oid, bl,
                             &objv, &mtime, y, dpp, &attrs);
  if (r < 0) {
    ldpp_dout(dpp, 20) << "group lookup with id=" << id
        << " failed: " << cpp_strerror(r) << dendl;
    return r;
  }

  try {
    auto p = bl.cbegin();
    decode(info, p);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode group info: "
        << e.what() << dendl;
    return -EIO;
  }
  if (info.id != id) {
    ldpp_dout(dpp, 0) << "ERROR: read group id mismatch "
        << info.id << " != " << id << dendl;
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
                 RGWGroupInfo& info,
                 std::map<std::string, ceph::buffer::list>& attrs,
                 RGWObjVersionTracker& objv)
{
  auto nameobj = NameObj{.obj = get_name_obj(zone, tenant, name)};
  int r = read_name(dpp, y, sysobj, nameobj);
  if (r < 0) {
    return r;
  }
  ceph::real_time mtime; // ignored
  return read(dpp, y, sysobj, zone, nameobj.data.id, info, attrs, mtime, objv);
}


int write(const DoutPrefixProvider* dpp,
          optional_yield y,
          RGWSI_SysObj& sysobj,
          librados::Rados& rados,
          const RGWZoneParams& zone,
          const RGWGroupInfo& info,
          const RGWGroupInfo* old_info,
          const std::map<std::string, ceph::buffer::list>& attrs,
          ceph::real_time mtime,
          bool exclusive,
          RGWObjVersionTracker& objv)
{
  const rgw_raw_obj obj = get_group_obj(zone, info.id);

  const bool same_name = old_info
      && old_info->account_id == info.account_id
      && old_info->name == info.name;

  std::optional<NameObj> remove_name;
  if (old_info) {
    if (old_info->id != info.id) {
      ldpp_dout(dpp, 1) << "ERROR: can't modify group id" << dendl;
      return -EINVAL;
    }
    if (!same_name && !old_info->name.empty()) {
      // read old group name object
      NameObj nameobj;
      nameobj.obj = get_name_obj(zone, old_info->account_id, old_info->name);
      int r = read_name(dpp, y, sysobj, nameobj);
      if (r == -ENOENT) {
        // leave remove_name empty
      } else if (r < 0) {
        return r;
      } else if (nameobj.data.id == info.id) {
        remove_name = std::move(nameobj);
      }
    }
  } // old_info

  if (!same_name && !info.name.empty()) {
    // read new account name object
    NameObj nameobj;
    nameobj.obj = get_name_obj(zone, info.account_id, info.name);
    int r = read_name(dpp, y, sysobj, nameobj);
    if (r == -ENOENT) {
      // write the new name object below
    } else if (r == 0) {
      ldpp_dout(dpp, 1) << "ERROR: group name obj " << nameobj.obj
          << " already taken for group id " << nameobj.data.id << dendl;
      return -EEXIST;
    } else if (r < 0) {
      return r;
    }
  }

  // encode/write the group info
  {
    bufferlist bl;
    encode(info, bl);

    const rgw_raw_obj obj = get_group_obj(zone, info.id);
    int r = rgw_put_system_obj(dpp, &sysobj, obj.pool, obj.oid, bl,
                               exclusive, &objv, mtime, y, &attrs);
    if (r < 0) {
      ldpp_dout(dpp, 1) << "ERROR: failed to write group obj " << obj
          << " with: " << cpp_strerror(r) << dendl;
      return r;
    }
  }

  if (remove_name) {
    // remove the old name object, ignoring errors
    auto& nameobj = *remove_name;
    int r = rgw_delete_system_obj(dpp, &sysobj, nameobj.obj.pool,
                                  nameobj.obj.oid, &nameobj.objv, y);
    if (r < 0) {
      ldpp_dout(dpp, 20) << "WARNING: failed to remove old name obj "
          << nameobj.obj.oid << ": " << cpp_strerror(r) << dendl;
    } // not fatal
    // unlink the old name from its account
    const auto& users = account::get_groups_obj(zone, old_info->account_id);
    r = groups::remove(dpp, y, rados, users, old_info->name);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not unlink from account "
          << old_info->account_id << ": " << cpp_strerror(r) << dendl;
    } // not fatal
  }
  if (!same_name && !info.name.empty()) {
    // write the new name object
    NameObj nameobj;
    nameobj.obj = get_name_obj(zone, info.account_id, info.name);
    nameobj.data.id = info.id;
    nameobj.objv.generate_new_write_ver(dpp->get_cct());

    int r = write_name(dpp, y, sysobj, nameobj);
    if (r < 0) {
      ldpp_dout(dpp, 20) << "WARNING: failed to write name obj "
          << nameobj.obj << " with: " << cpp_strerror(r) << dendl;
    } // not fatal
    // link the new name to its account
    const auto& users = account::get_groups_obj(zone, info.account_id);
    r = groups::add(dpp, y, rados, users, info, false,
                    std::numeric_limits<uint32_t>::max());
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not link to account "
          << info.account_id << ": " << cpp_strerror(r) << dendl;
    } // not fatal
  }

  return 0;
}

int remove(const DoutPrefixProvider* dpp,
           optional_yield y,
           RGWSI_SysObj& sysobj,
           librados::Rados& rados,
           const RGWZoneParams& zone,
           const RGWGroupInfo& info,
           RGWObjVersionTracker& objv)
{
  const rgw_raw_obj obj = get_group_obj(zone, info.id);
  int r = rgw_delete_system_obj(dpp, &sysobj, obj.pool, obj.oid, &objv, y);
  if (r < 0) {
      ldpp_dout(dpp, 1) << "ERROR: failed to remove account obj "
          << obj << " with: " << cpp_strerror(r) << dendl;
    return r;
  }

  {
    // remove the name object
    const rgw_raw_obj obj = get_name_obj(zone, info.account_id, info.name);
    r = rgw_delete_system_obj(dpp, &sysobj, obj.pool, obj.oid, nullptr, y);
    if (r < 0) {
      ldpp_dout(dpp, 20) << "WARNING: failed to remove name obj "
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
  {
    // unlink the name from its account
    const auto& users = account::get_groups_obj(zone, info.account_id);
    r = groups::remove(dpp, y, rados, users, info.name);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not unlink from account "
          << info.account_id << ": " << cpp_strerror(r) << dendl;
    } // not fatal
  }

  return 0;
}


// metadata abstraction

struct CompleteInfo {
  RGWGroupInfo info;
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
  MetadataObject(CompleteInfo& aci, const obj_version& v, ceph::real_time m)
    : RGWMetadataObject(v, m), aci(std::move(aci)) {}

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
      return oid.substr(info_oid_prefix.size());
    };
    std::transform(oids.begin(), oids.end(),
                   std::back_inserter(keys),
                   trim);
  }
};

class MetadataHandler : public RGWMetadataHandler {
  RGWSI_SysObj& sysobj;
  librados::Rados& rados;
  const RGWZoneParams& zone;
 public:
  MetadataHandler(RGWSI_SysObj& sysobj, librados::Rados& rados,
                  const RGWZoneParams& zone)
    : sysobj(sysobj), rados(rados), zone(zone) {}

  std::string get_type() override { return "group"; }

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
    const std::string& group_id = entry;
    CompleteInfo aci;
    RGWObjVersionTracker objv;
    ceph::real_time mtime;

    int r = read(dpp, y, sysobj, zone, group_id,
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
    const std::string& group_id = entry;
    auto group_obj = static_cast<MetadataObject*>(obj);
    const auto& new_info = group_obj->get().info;

    // account id must match metadata key
    if (new_info.id != group_id) {
      return -EINVAL;
    }

    // read existing metadata
    RGWGroupInfo old_info;
    std::map<std::string, ceph::buffer::list> old_attrs;
    ceph::real_time old_mtime;
    int r = read(dpp, y, sysobj, zone, group_id,
                 old_info, old_attrs, old_mtime, objv);
    if (r < 0 && r != -ENOENT) {
      return r;
    }
    const RGWGroupInfo* pold_info = (r == -ENOENT ? nullptr : &old_info);

    // write/overwrite metadata
    constexpr bool exclusive = false;
    return write(dpp, y, sysobj, rados, zone, new_info, pold_info,
                 group_obj->get().attrs, obj->get_mtime(),
                 exclusive, objv);
  }

  int remove(std::string& entry, RGWObjVersionTracker& objv,
             optional_yield y, const DoutPrefixProvider* dpp) override
  {
    const std::string& group_id = entry;

    // read existing metadata
    RGWGroupInfo info;
    std::map<std::string, ceph::buffer::list> attrs;
    ceph::real_time mtime;
    int r = read(dpp, y, sysobj, zone, group_id,
                 info, attrs, mtime, objv);
    if (r < 0) {
      return r;
    }

    return group::remove(dpp, y, sysobj, rados, zone, info, objv);
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
        sysobj.get_pool(zone.group_pool));
    int r = lister->init(dpp, marker, std::string{info_oid_prefix});
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

auto create_metadata_handler(RGWSI_SysObj& sysobj, librados::Rados& rados,
                             const RGWZoneParams& zone)
    -> std::unique_ptr<RGWMetadataHandler>
{
  return std::make_unique<MetadataHandler>(sysobj, rados, zone);
}

} // namespace rgwrados::group
