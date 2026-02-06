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
  if(!info.name.empty()) {
    iObj.obj = get_name_obj(zone, info);
    iObj.objv.generate_new_write_ver(dpp->get_cct());

    bufferlist data;
    encode(info, data);

    r = rgw_put_system_obj(dpp, &sysobj, iObj.obj.pool, iObj.obj.oid, data, exclusive, &iObj.objv, info.creation_date, y, nullptr);
    if(r < 0) {
      ldpp_dout(dpp, 20) << "failed to write policy obj " << iObj.obj << " with: " << cpp_strerror(r) << dendl;
      return r;
    }
  }

  PolicyIndex policy_index;
  r = write_path(dpp, y, rados, sysobj, zone, info, policy_index);
  if (r < 0) {
    // roll back new policy object
    ldpp_dout(dpp, 20) << "failed to write path obj " << iObj.obj << " with: " << cpp_strerror(r) << dendl;
    std::ignore = remove_index(dpp, y, sysobj, iObj);
    return r;
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
}
