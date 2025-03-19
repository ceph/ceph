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

#include "policy.h"

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
#include "rgw_zone_types.h"
#include "svc_mdlog.h"

#include "account.h"
#include "rgw_customer_managed_policy.h"
#include "cls/user/cls_user_client.h"

namespace rgwrados::policy
{
struct IndexObj {
  rgw_raw_obj obj;
  RGWObjVersionTracker objv;
};

struct AccountIndex
{
  rgw_raw_obj obj;
  std::string_view name;
};
using NameIndex = std::optional<IndexObj>;
using PathIndex = std::variant<std::monostate, IndexObj, AccountIndex>;

// RGWRoleInfo is stored in rados objects named "roles.{id}",
// where ids are assumed to be globally unique
static const std::string oid_prefix = "managed_policy.";
// read_by_name() is enabled by rados objects
// "{tenant}role_names.{name}" for tenant roles, or
// "{account}role_names.{name}" for account roles
constexpr std::string_view name_oid_prefix = "managed_policy_names.";
// list() by path/prefix is enabled by rados objects
// "{tenant}role_paths.{path}roles.{id}" for tenant roles.
// see rgwrados::roles::list() for account roles
constexpr std::string_view path_oid_prefix = "managed_policy_paths.";
static const std::string account_oid_prefix = "account.";

static rgw_raw_obj get_name_obj(const RGWZoneParams &zone,
                                std::string_view tenant,
                                const rgw_account_id &account,
                                std::string_view name)
{
  if (account.empty())
  {
    // use tenant as prefix
    std::string oid = string_cat_reserve(tenant, name_oid_prefix, name);
    return {zone.account_pool, std::move(oid)};
  }
  else
  {
    // names are case-insensitive, so store them in lower case
    std::string lower_name{name};
    boost::algorithm::to_lower(lower_name);
    // use account id as prefix
    std::string oid = string_cat_reserve(RGW_ATTR_PREFIX, name_oid_prefix, lower_name); // RGW_ATTR_PREFIX ?? account
    return {zone.account_pool, std::move(oid)};
  }
}

static rgw_raw_obj get_name_obj(const RGWZoneParams &zone,
                                const ManagedPolicyInfo &info)
{
  return get_name_obj(zone, info.tenant, info.account_id, info.policy_name);
}

static int write_name(const DoutPrefixProvider *dpp, optional_yield y,
                      RGWSI_SysObj &sysobj, const std::string &role_id,
                      IndexObj &index)
{
  RGWNameToId nameToId;
  nameToId.obj_id = role_id;

  bufferlist bl;
  encode(nameToId, bl);

  return rgw_put_system_obj(dpp, &sysobj, index.obj.pool, index.obj.oid, bl,
                            true, &index.objv, ceph::real_time(), y);
}
static rgw_raw_obj get_tenant_path_obj(const RGWZoneParams &zone,
                                       const ManagedPolicyInfo &info)
{
  std::string oid = string_cat_reserve(info.tenant, path_oid_prefix,
                                       info.path, oid_prefix, info.id);
  return {zone.account_pool, std::move(oid)};
}
static rgw_raw_obj get_id_obj(const RGWZoneParams &zone,
                              std::string_view id)
{
  return {zone.account_pool, string_cat_reserve(oid_prefix, id)};
}

static std::string get_account_key(std::string_view account_id) {
  return string_cat_reserve(account_oid_prefix, account_id);
}

static rgw_raw_obj get_account_obj(const RGWZoneParams& zone,
                                   std::string_view account_id) {
  return {zone.account_pool, get_account_key(account_id)};
}

static int write_tenant_path(const DoutPrefixProvider* dpp, optional_yield y,
                             RGWSI_SysObj& sysobj, IndexObj& path)
{
  bufferlist bl;
  return rgw_put_system_obj(dpp, &sysobj, path.obj.pool, path.obj.oid, bl,
                            true, &path.objv, ceph::real_time(), y);
}

int add(const DoutPrefixProvider* dpp,
        optional_yield y,
        librados::Rados& rados,
        const rgw_raw_obj& obj,
        const ManagedPolicyInfo& info,
        bool exclusive, uint32_t limit){
  resource_metadata meta;
  meta.policy_id = info.id;

  cls_user_account_resource resource;
  resource.name = info.policy_name;
  resource.path = info.path;
  encode(meta, resource.metadata);

  rgw_rados_ref ref;
  int r = rgw_get_rados_ref(dpp, &rados, obj, &ref);
  if(r < 0){
    return r;
  }

  librados::ObjectWriteOperation operation;
  ::cls_user_account_resource_add(operation, resource, exclusive, limit);
  return ref.operate(dpp, std::move(operation), y);
}

static int write_path(const DoutPrefixProvider *dpp, optional_yield y,
                      librados::Rados &rados, RGWSI_SysObj &sysobj,
                      const RGWZoneParams &zone, const ManagedPolicyInfo &info,
                      PathIndex &index)
{
  if (!info.account_id.empty())
  {
    // add the new role to its account
    AccountIndex path;
    path.obj = account::get_policy_obj(zone, info.account_id);
    path.name = info.policy_name;

    constexpr bool exclusive = true;
    constexpr uint32_t no_limit = std::numeric_limits<uint32_t>::max();
    //TODO: add
  
    int r = add(dpp, y, rados, path.obj, info, exclusive, no_limit);
    if (r < 0)
    {
      ldpp_dout(dpp, 1) << "failed to add role to account "
                        << path.obj << " with: " << cpp_strerror(r) << dendl;
      return r;
    }
    index = std::move(path);
  }
  else
  {
    // write the new path object
    IndexObj path;
    path.obj = get_tenant_path_obj(zone, info);
    path.objv.generate_new_write_ver(dpp->get_cct());

    int r = write_tenant_path(dpp, y, sysobj, path);
    if (r < 0)
    {
      ldpp_dout(dpp, 1) << "failed to write role path obj "
                        << path.obj << " with: " << cpp_strerror(r) << dendl;
      return r;
    }
    index = std::move(path);
  }
  return 0;
}

static int write_info(const DoutPrefixProvider *dpp, optional_yield y,
                      RGWSI_SysObj &sysobj, const RGWZoneParams &zone,
                      const ManagedPolicyInfo &info, 
                      const RGWAccountInfo &acc_info, std::map<std::string, bufferlist> &acc_attrs,
                      RGWObjVersionTracker &objv,
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
  std::string key = "rgw.managedpolicy.raja";
  acc_attrs[key] = bl;
  bufferlist acc_bl;
  encode(acc_info, acc_bl);

  const rgw_raw_obj& obj = get_account_obj(zone, info.id);
  int r = rgw_put_system_obj(dpp, &sysobj, obj.pool, obj.oid,
                             acc_bl, exclusive, &objv, mtime, y, &acc_attrs);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to write role obj " << obj
        << " with: " << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}


int write(const DoutPrefixProvider *dpp, optional_yield y,
          librados::Rados &rados, RGWSI_SysObj &sysobj, RGWSI_MDLog *mdlog,
          const RGWZoneParams &zone, const ManagedPolicyInfo &info,
          RGWObjVersionTracker &objv, ceph::real_time mtime,
          bool exclusive)
{}

/// Write or overwrite policy info and update its name/path objects.
int write(const DoutPrefixProvider *dpp, optional_yield y,
          librados::Rados &rados, RGWSI_SysObj &sysobj, RGWSI_MDLog *mdlog,
          const RGWZoneParams &zone, const ManagedPolicyInfo &info,
          const RGWAccountInfo &acc_info, std::map<std::string, bufferlist> &acc_attrs,
          RGWObjVersionTracker &objv, ceph::real_time mtime,
          bool exclusive)
{
  int r = 0;

  ManagedPolicyInfo old;
  ManagedPolicyInfo* old_info = nullptr;

  if(!exclusive) {
    //TODO: exclusive is true
  }
  //TODO: old info
    NameIndex new_name;
    if(!info.policy_name.empty()){
      IndexObj name;
      name.obj = get_name_obj(zone, info); // name obj RGW52720492365036953policy_names.myreadonlys3policy
      name.objv.generate_new_write_ver(dpp->get_cct());
      r = write_name(dpp, y, sysobj, info.id, name);
      if(r < 0){
        ldpp_dout(dpp, 1) << "failed to write name obj "
            << name.obj << " with: " << cpp_strerror(r) << dendl;
        return r;
      }
    }

    // check for path conflict
    PathIndex new_path;
    r = write_path(dpp, y, rados, sysobj, zone, info, new_path);
    if (r < 0) {
      // roll back new name object
      return r;
    }

    // write info by id
    r = write_info(dpp, y, sysobj, zone, info, acc_info, acc_attrs, objv, mtime, exclusive);
    if (r < 0)
    {
      // roll back the new name/path indices
      return r;
    }

    // record in the mdlog on success
    if (mdlog)
    {
      return mdlog->complete_entry(dpp, y, "policy", info.id, &objv);
    }
    return 0;
}
}
