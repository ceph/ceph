// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

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

#include <optional>
#include <string>
#include <boost/container/flat_set.hpp>
#include "common/ceph_context.h"
#include "include/buffer_fwd.h"
#include <map>
#include "rgw_arn.h"
#include "common/ceph_json.h"
#include "common/Formatter.h"
#include "rgw_user_types.h"
#include "include/encoding.h"
#include "rgw_common.h"

namespace rgw::IAM {

struct Policy;

/// Return a managed policy by ARN.
auto get_managed_policy(CephContext* cct, std::string_view arn)
    -> std::optional<Policy>;

/// A serializable container for managed policy ARNs.
struct ManagedPolicies {
  boost::container::flat_set<std::string> arns;
};
void encode(const ManagedPolicies&, bufferlist&, uint64_t f=0);
void decode(ManagedPolicies&, bufferlist::const_iterator&);

struct ManagedPolicyAttachment {
  std::string arn;
  std::string  status;

  void encode(bufferlist& bl) const
  {
    ENCODE_START(2, 1, bl);
    encode(arn, bl);
    encode(status, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl)
  {
    DECODE_START(2, bl);
     decode(arn, bl);
     decode(status, bl);
     DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<ManagedPolicyAttachment*>& o);
};
WRITE_CLASS_ENCODER(ManagedPolicyAttachment)

struct PolicyVersion {
  std::string document;
  std::string  version_id;
  bool is_default_version{false};
  ceph::real_time create_date{ceph::real_clock::zero()};

  void encode(bufferlist& bl) const
  {
    ENCODE_START(2, 1, bl);
    encode(document, bl);
    encode(version_id, bl);
    encode(is_default_version, bl);
    encode(create_date, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl)
  {
    DECODE_START(2, bl);
     decode(document, bl);
     decode(version_id, bl);
     decode(is_default_version, bl);
     decode(create_date, bl);
     DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(PolicyVersion)

struct ManagedPolicyInfo {
  std::string id;
  std::string name;
  std::string path{"/"};
  std::string arn;
  std::string policy_document;
  std::string description;
  rgw_account_id account_id;
  ceph::real_time update_date{ceph::real_clock::zero()};
  ceph::real_time creation_date{ceph::real_clock::zero()};
  std::string default_version{"v1"};
  bool is_attachable{true};
  uint32_t attachment_count{0};
  uint32_t permissions_boundary_usage_count{0};
  std::multimap<std::string, std::string> tags;
  std::map<std::string, ManagedPolicyAttachment> attachments;
  std::map<int, PolicyVersion> versions;

  void encode(bufferlist &bl) const
  {
    ENCODE_START(2, 1, bl);
    encode(id, bl);
    encode(name, bl);
    encode(path, bl);
    encode(update_date, bl);
    encode(creation_date, bl);
    encode(policy_document, bl);
    encode(description, bl);
    encode(default_version, bl);
    encode(account_id, bl);
    encode(tags, bl);
    encode(arn, bl);
    encode(attachment_count, bl);
    encode(permissions_boundary_usage_count, bl);
    encode(is_attachable, bl);
    encode(attachments, bl);
    encode(versions, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &bl)
  {
    DECODE_START(2, bl);
    decode(id, bl);
    decode(name, bl);
    decode(path, bl);
    decode(update_date, bl);
    decode(creation_date, bl);
    decode(policy_document, bl);
    decode(description, bl);
    decode(default_version, bl);
    decode(account_id, bl);
    decode(tags, bl);
    decode(arn, bl);
    decode(attachment_count, bl);
    decode(permissions_boundary_usage_count, bl);
    decode(is_attachable, bl);
    decode(attachments, bl);
    decode(versions, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj* obj); 
  static void generate_test_instances(std::list<ManagedPolicyInfo*>& ls);
};
WRITE_CLASS_ENCODER(ManagedPolicyInfo)
// A list of policies
struct PolicyList {
  // The list of results, sorted by name
  std::vector<ManagedPolicyInfo> policies;
  // The next marker to resume listing, or empty
  std::string next_marker;
};

struct VersionList {
  std::vector<PolicyVersion> versions;
  std::string next_marker;
};

struct PolicyTagList {
  std::multimap<std::string, std::string> tags;
  std::string next_marker;
};

enum class Scope { All, AWS, Local };
enum class PolicyUsageFilter { PermissionsPolicy, PermissionsBoundary };
std::vector<ManagedPolicyInfo> list_aws_managed_policy();

} // namespace rgw::IAM
