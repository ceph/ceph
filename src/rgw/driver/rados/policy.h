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

#pragma once

#include "rgw_iam_managed_policy.h"
#include "common/ceph_time.h"
#include "include/rados/librados_fwd.hpp"
#include "rgw_zone.h"

namespace ceph { class Formatter; }
class DoutPrefixProvider;
class optional_yield;
class RGWObjVersionTracker;
class RGWSI_SysObj;
class RGWZoneParams;
class ManagedPolicyInfo;
class RGWSI_MDLog;

namespace rgwrados::policy
{

struct PolicyObj {
  rgw_raw_obj obj;
  std::string_view name;
};
struct IndexObj {
  rgw_raw_obj obj;
  RGWObjVersionTracker objv;
};

using PolicyIndex = std::variant<std::monostate, IndexObj, PolicyObj>;

struct resource_metadata {
  std::string policy_name;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(policy_name, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(policy_name, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<resource_metadata*>& o);
};
WRITE_CLASS_ENCODER(resource_metadata);


/* 
 * Return the rados object that tracks the given account's policies.
 * This can be used with the cls_user interface in namespace rgwrados::policy.
 */
rgw_raw_obj get_policy_obj(const RGWZoneParams& zone,
                          std::string_view account_id);

/* 
 * Return the rados object that stores the given account's policy.
 */
rgw_raw_obj get_name_obj(const RGWZoneParams& zone, const rgw::IAM::ManagedPolicyInfo& info);

int write_policy(const DoutPrefixProvider *dpp, optional_yield y, librados::Rados& rados, RGWSI_SysObj &sysobj, 
          const RGWZoneParams &zone, const rgw::IAM::ManagedPolicyInfo &info, 
          bool exclusive);
int get_policy(const DoutPrefixProvider *dpp,
              optional_yield y,
              RGWSI_SysObj &sysobj,
              const RGWZoneParams &zone,
              std::string_view account,
              std::string_view name,
              rgw::IAM::ManagedPolicyInfo &info);
int delete_policy(const DoutPrefixProvider *dpp,
              optional_yield y,
              librados::Rados& rados,
              RGWSI_SysObj &sysobj,
              const RGWZoneParams &zone,
              std::string_view account,
              std::string_view name);

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
              rgw::IAM::PolicyList& listing);

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
              bool exclusive);
}
