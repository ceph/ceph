// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "policy.h"
#include "common/errno.h"
#include "rgw_string.h"
#include "rgw_zone.h"
#include "svc_mdlog.h"
#include "rgw_iam_managed_policy.h"
#include "account.h"
#include "cls/user/cls_user_client.h"

namespace rgwrados::policy
{
static const int max_policy_versions = 5;
static const std::string oid_prefix = "customer-managed-policy.";
static constexpr std::string_view policy_oid_prefix = "policies.";

void resource_metadata::dump(ceph::Formatter* f) const
{
  encode_json("policy_name", policy_name, f);
}

void resource_metadata::generate_test_instances(std::list<resource_metadata*>& o)
{
  o.push_back(new resource_metadata);
  auto m = new resource_metadata;
  m->policy_name = "policy_name";
  o.push_back(m);
}

static std::string get_name_key( const std::string_view& account_id, const std::string_view& policy_name)
{
  std::string lower_name(policy_name);
  boost::algorithm::to_lower(lower_name);
  return string_cat_reserve(oid_prefix, account_id, ".", lower_name);
}
rgw_raw_obj get_name_obj(const RGWZoneParams& zone, const rgw::IAM::ManagedPolicyInfo& info)
{
    return {zone.policy_pool, get_name_key(info.account_id, info.name)};
}

static std::string get_policy_key(std::string_view account_id) {
  return string_cat_reserve(policy_oid_prefix, account_id);
}
rgw_raw_obj get_policy_obj(const RGWZoneParams& zone,
                          std::string_view account_id) {
  return {zone.account_pool, get_policy_key(account_id)};
}

static int add(const DoutPrefixProvider* dpp,
        optional_yield y,
        librados::Rados& rados,
        const rgw_raw_obj& obj,
        const rgw::IAM::ManagedPolicyInfo& info,
        bool exclusive, uint32_t limit)
{
  resource_metadata meta;
  meta.policy_name = info.name;

  cls_user_account_resource resource;
  resource.name = info.name;
  resource.path = info.path;
  encode(meta, resource.metadata);

  rgw_rados_ref ref;
  int r = rgw_get_rados_ref(dpp, &rados, obj, &ref);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  ::cls_user_account_resource_add(op, resource, exclusive, limit);
  return ref.operate(dpp, std::move(op), y);
}

static int remove(const DoutPrefixProvider* dpp,
           optional_yield y,
           librados::Rados& rados,
           PolicyObj& policy)
{
  rgw_rados_ref ref;
  int r = rgw_get_rados_ref(dpp, &rados, policy.obj, &ref);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  ::cls_user_account_resource_rm(op, policy.name);
  return ref.operate(dpp, std::move(op), y);
}

static int remove_index(const DoutPrefixProvider* dpp, optional_yield y,
                        RGWSI_SysObj& sysobj, IndexObj& index)
{
  int r = rgw_delete_system_obj(dpp, &sysobj, index.obj.pool,
                                index.obj.oid, &index.objv, y);
  if (r < 0) {
    ldpp_dout(dpp, 20) << "WARNING: failed to remove " << index.obj << " with " << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

static int write_path(const DoutPrefixProvider* dpp, optional_yield y,
                      librados::Rados& rados, RGWSI_SysObj& sysobj,
                      const RGWZoneParams& zone, const rgw::IAM::ManagedPolicyInfo& info,
                      PolicyIndex& index)
{
  // add the new policy to its account
  PolicyObj policy;
  policy.obj = get_policy_obj(zone, info.account_id);
  policy.name = info.name;

  constexpr bool exclusive = true;
  constexpr uint32_t no_limit = std::numeric_limits<uint32_t>::max();
  int r = add(dpp, y, rados, policy.obj, info, exclusive, no_limit);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "failed to add policy to account "<< policy.obj << " with: " << cpp_strerror(r) << dendl;
    return r;
  }
  index = std::move(policy);
  return 0;
}

int write_policy(const DoutPrefixProvider *dpp, optional_yield y, librados::Rados& rados, RGWSI_SysObj &sysobj, 
          const RGWZoneParams &zone, const rgw::IAM::ManagedPolicyInfo &info, 
          bool exclusive)
{
  int r = -EINVAL;
  IndexObj iObj;
  if(exclusive && !info.name.empty()) {
    iObj.obj = get_name_obj(zone, info);
    iObj.objv.generate_new_write_ver(dpp->get_cct());

    bufferlist data;
    encode(info, data);

    r = rgw_put_system_obj(dpp, &sysobj, iObj.obj.pool, iObj.obj.oid, data, exclusive, &iObj.objv, info.creation_date, y, nullptr);
    if(r < 0) {
      ldpp_dout(dpp, 20) << "failed to write policy obj " << iObj.obj << " with: " << cpp_strerror(r) << dendl;
      return r;
    }

    PolicyIndex policy_index;
    r = write_path(dpp, y, rados, sysobj, zone, info, policy_index);
    if (r < 0) {
      // roll back new policy object
      ldpp_dout(dpp, 20) << "failed to write path obj " << iObj.obj << " with: " << cpp_strerror(r) << dendl;
      std::ignore = remove_index(dpp, y, sysobj, iObj);
      return r;
    }
  } else {
    bufferlist data;
    encode(info, data);
    iObj.obj = get_name_obj(zone, info);

    r = rgw_put_system_obj(dpp, &sysobj, iObj.obj.pool, iObj.obj.oid, data, exclusive, &iObj.objv, info.update_date, y, nullptr);
    if(r < 0) {
      ldpp_dout(dpp, 20) << "failed to modify policy obj " << iObj.obj.oid << " with: " << cpp_strerror(r) << dendl;
      return r;
    }
  }

  return r;
}

int get_policy(const DoutPrefixProvider *dpp,
              optional_yield y,
              RGWSI_SysObj &sysobj,
              const RGWZoneParams &zone,
              std::string_view account,
              std::string_view name,
              rgw::IAM::ManagedPolicyInfo &info)
{
  bufferlist bl; 
  auto oid = get_name_key(account, name);
  int ret = rgw_get_system_obj(&sysobj, zone.policy_pool, oid, bl, nullptr, nullptr, y, dpp);
  if (ret < 0) {
    return ret;
  }

  try {
    using ceph::decode;
    auto iter = bl.cbegin();
    decode(info, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode info from pool: " << zone.policy_pool <<
                  ": " << name << dendl;
    return -EIO;
  }

  return 0;
}

int delete_policy(const DoutPrefixProvider *dpp,
              optional_yield y,
              librados::Rados& rados,
              RGWSI_SysObj &sysobj,
              const RGWZoneParams &zone,
              std::string_view account,
              std::string_view name)
{
  rgw::IAM::ManagedPolicyInfo info;
  auto oid = get_name_key(account, name);
  int ret = get_policy(dpp, y, sysobj, zone, account, name, info);
  if(ret < 0){
    return ret;
  }
  ret = rgw_delete_system_obj(dpp, &sysobj, zone.policy_pool, oid, nullptr, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: deleting iam policy from pool: " << zone.policy_pool.name << ": "
                  << name << ": " << cpp_strerror(ret) << dendl;
  }

  // delete the path object
  if (!info.account_id.empty()) {
    PolicyObj policy;
    policy.obj = get_policy_obj(zone, info.account_id);
    policy.name = info.name;
    std::ignore = remove(dpp, y, rados, policy);

    uint32_t count = 0;
    rgwrados::account::resource_count(dpp, y, rados, policy.obj, count);
    if (!count) {
      IndexObj iObj;
      iObj.obj = get_policy_obj(zone, info.account_id);
      std::ignore = remove_index(dpp, y, sysobj, iObj);
    }
  }

  return ret;
}

static int list(const DoutPrefixProvider* dpp,
         optional_yield y,
         librados::Rados& rados,
         const rgw_raw_obj& obj,
         std::string_view marker,
         std::string_view path_prefix,
         uint32_t max_items,
         std::vector<std::string>& names,
         std::string& next_marker)
{
  rgw_rados_ref ref;
  int r = rgw_get_rados_ref(dpp, &rados, obj, &ref);
  if (r < 0) {
    return r;
  }

  librados::ObjectReadOperation op;
  std::vector<cls_user_account_resource> entries;
  bool truncated = false;
  int ret = 0;
  ::cls_user_account_resource_list(op, marker, path_prefix, max_items,
                                   entries, &truncated, &next_marker, &ret);

  r = ref.operate(dpp, std::move(op), nullptr, y);
  if (r == -ENOENT) {
    next_marker.clear();
    return 0;
  }
  if (r < 0) {
    return r;
  }
  if (ret < 0) {
    return ret;
  }

  for (auto& resource : entries) {
    resource_metadata meta;
    try {
      auto p = resource.metadata.cbegin();
      decode(meta, p);
    } catch (const buffer::error&) {
      return -EIO;
    }
    names.push_back(std::move(meta.policy_name));
  }

  if (!truncated) {
    next_marker.clear();
  }
  return 0;
}

static int get_aws_managed_policies(rgw::IAM::PolicyList& listing)
{
  auto aws_policies = rgw::IAM::list_aws_managed_policy();
  listing.policies.insert(listing.policies.end(),
                          aws_policies.begin(), aws_policies.end());
  return 0;
}

static int get_customer_managed_policies(const DoutPrefixProvider *dpp,
              optional_yield y,
              librados::Rados& rados,
              RGWSI_SysObj &sysobj,
              const RGWZoneParams &zone,
              std::string_view account_id,
              bool only_attached,
              rgw::IAM::PolicyUsageFilter policy_usage_filter,
              std::string_view path_prefix,
              std::string_view marker,
              uint32_t max_items,
              rgw::IAM::PolicyList& listing)
{
  std::vector<std::string> names;
  const rgw_raw_obj& obj = rgwrados::policy::get_policy_obj(zone, account_id);
  int ret = list(dpp, y, rados, obj, marker, path_prefix, max_items, names, listing.next_marker);
  if(ret < 0) {
    return ret;
  }

  for(const auto &name : names) {
    rgw::IAM::ManagedPolicyInfo info;
    auto oid = get_name_key(account_id, name);
    int ret = get_policy(dpp, y, sysobj, zone, account_id, name, info);
    if(ret < 0){
      return ret;
    }

    if(only_attached && info.attachment_count == 0) {
      continue;
    }
    if(policy_usage_filter == rgw::IAM::PolicyUsageFilter::PermissionsBoundary 
        && info.permissions_boundary_usage_count == 0) {
      continue;
    }
    listing.policies.push_back(std::move(info));
  }

  return 0;
}


int list_policies(const DoutPrefixProvider *dpp,
              optional_yield y,
              librados::Rados& rados,
              RGWSI_SysObj &sysobj,
              const RGWZoneParams &zone,
              std::string_view account_id,
              rgw::IAM::Scope scope,
              bool only_attached,
              std::string_view path_prefix,
              rgw::IAM::PolicyUsageFilter policy_usage_filter,
              std::string_view marker,
              uint32_t max_items,
              rgw::IAM::PolicyList& listing)
{
  int ret = -ENOENT;
  switch(scope) {
    case rgw::IAM::Scope::AWS:
      ret = get_aws_managed_policies(listing);
      break;
    case rgw::IAM::Scope::Local:
      ret = get_customer_managed_policies(dpp, y, rados, sysobj, zone, account_id,
              only_attached, policy_usage_filter, path_prefix, marker, max_items, listing);
      break;
    case rgw::IAM::Scope::All:
    default:
      ret = get_aws_managed_policies(listing);
      if(ret < 0) return ret;
      ret = get_customer_managed_policies(dpp, y, rados, sysobj, zone, account_id,
              only_attached, policy_usage_filter, path_prefix, marker, max_items, listing);
  }

  return ret;
}

int create_policy_version(const DoutPrefixProvider *dpp,
    optional_yield y,
    librados::Rados& rados,
    RGWSI_SysObj &sysobj,
    const RGWZoneParams &zone,
    std::string_view account,
    std::string_view policy_name,
    std::string_view policy_document,
    bool set_as_default,
    std::string &version_id,
    ceph::real_time &create_date,
    bool exclusive)
{
  rgw::IAM::ManagedPolicyInfo info;
  rgw::IAM::PolicyVersion policy_version;
  auto oid = get_name_key(account, policy_name);
  int ret = get_policy(dpp, y, sysobj, zone, account, policy_name, info);
  if(ret < 0){
    return ret;
  }

  if(info.versions.size() >= max_policy_versions) {
    ldpp_dout(dpp, 20) << "Error: max_policy_versions reached" << dendl;
    return -ENOMEM;
  }

  int version = 1;
  if (!info.versions.empty()) {
      version = info.versions.rbegin()->first + 1;
  }

  version_id = "v" + std::to_string(version);
  policy_version.document = policy_document;
  policy_version.version_id = version_id;
  policy_version.create_date = real_clock::now();
  policy_version.is_default_version = set_as_default;
  create_date = policy_version.create_date;

  info.update_date = create_date;
  info.versions[version] = policy_version;
  if(set_as_default) {
    info.default_version = policy_version.version_id;
    info.is_attachable = false;
  }

  ret = write_policy(dpp, y, rados, sysobj, zone, info, exclusive);
  if(ret < 0) {
    ldpp_dout(dpp, 20) << "failed to create_policy_version " << info.name << " with: " << cpp_strerror(ret) << dendl;
    return ret;
  }

  return ret;
}
}
