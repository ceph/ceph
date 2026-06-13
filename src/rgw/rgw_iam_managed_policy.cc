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

#include "rgw_iam_managed_policy.h"
#include "rgw_iam_policy.h"
#define dout_subsys ceph_subsys_rgw

namespace rgw::IAM {

// Type: AWS managed policy
// Creation time: February 06, 2015, 18:40 UTC
// Edited time: June 21, 2019, 19:40 UTC
// ARN: arn:aws:iam::aws:policy/IAMFullAccess
// Policy version: v2 (default)
static constexpr std::string_view IAMFullAccess = R"(
{
  "Version" : "2012-10-17",
  "Statement" : [
    {
      "Effect" : "Allow",
      "Action" : [
        "iam:*",
        "organizations:DescribeAccount",
        "organizations:DescribeOrganization",
        "organizations:DescribeOrganizationalUnit",
        "organizations:DescribePolicy",
        "organizations:ListChildren",
        "organizations:ListParents",
        "organizations:ListPoliciesForTarget",
        "organizations:ListRoots",
        "organizations:ListPolicies",
        "organizations:ListTargetsForPolicy"
      ],
      "Resource" : "*"
    }
  ]
})";

// Type: AWS managed policy
// Creation time: February 06, 2015, 18:40 UTC
// Edited time: January 25, 2018, 19:11 UTC
// ARN: arn:aws:iam::aws:policy/IAMReadOnlyAccess
// Policy version: v4 (default)
static constexpr std::string_view IAMReadOnlyAccess = R"(
{
  "Version" : "2012-10-17",
  "Statement" : [
    {
      "Effect" : "Allow",
      "Action" : [
        "iam:GenerateCredentialReport",
        "iam:GenerateServiceLastAccessedDetails",
        "iam:Get*",
        "iam:List*",
        "iam:SimulateCustomPolicy",
        "iam:SimulatePrincipalPolicy"
      ],
      "Resource" : "*"
    }
  ]
})";

// Type: AWS managed policy
// Creation time: February 06, 2015, 18:41 UTC
// Edited time: February 06, 2015, 18:41 UTC
// ARN: arn:aws:iam::aws:policy/AmazonSNSFullAccess
// Policy version: v1 (default)
static constexpr std::string_view AmazonSNSFullAccess = R"(
{
  "Version" : "2012-10-17",
  "Statement" : [
    {
      "Action" : [
        "sns:*"
      ],
      "Effect" : "Allow",
      "Resource" : "*"
    }
  ]
})";

// Type: AWS managed policy
// Creation time: February 06, 2015, 18:41 UTC
// Edited time: February 06, 2015, 18:41 UTC
// ARN: arn:aws:iam::aws:policy/AmazonSNSReadOnlyAccess
// Policy version: v1 (default)
static constexpr std::string_view AmazonSNSReadOnlyAccess = R"(
{
  "Version" : "2012-10-17",
  "Statement" : [
    {
      "Effect" : "Allow",
      "Action" : [
        "sns:GetTopicAttributes",
        "sns:List*"
      ],
      "Resource" : "*"
    }
  ]
})";

// Type: AWS managed policy
// Creation time: February 06, 2015, 18:40 UTC
// Edited time: September 27, 2021, 20:16 UTC
// ARN: arn:aws:iam::aws:policy/AmazonS3FullAccess
// Policy version: v2 (default)
static constexpr std::string_view AmazonS3FullAccess = R"(
{
  "Version" : "2012-10-17",
  "Statement" : [
    {
      "Effect" : "Allow",
      "Action" : [
        "s3:*",
        "s3-object-lambda:*"
      ],
      "Resource" : "*"
    }
  ]
})";

// Type: AWS managed policy
// Creation time: February 06, 2015, 18:40 UTC
// Edited time: August 10, 2023, 21:31 UTC
// ARN: arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess
// Policy version: v3 (default)
static constexpr std::string_view AmazonS3ReadOnlyAccess = R"(
{
  "Version" : "2012-10-17",
  "Statement" : [
    {
      "Effect" : "Allow",
      "Action" : [
        "s3:Get*",
        "s3:List*",
        "s3:Describe*",
        "s3-object-lambda:Get*",
        "s3-object-lambda:List*"
      ],
      "Resource" : "*"
    }
  ]
})";

auto get_managed_policy(CephContext* cct, std::string_view arn)
    -> std::optional<Policy>
{
  const std::string* tenant = nullptr;
  constexpr bool reject = false; // reject_invalid_principals
  if (arn == "arn:aws:iam::aws:policy/IAMFullAccess") {
    return Policy{cct, tenant, std::string{IAMFullAccess}, reject};
  } else if (arn == "arn:aws:iam::aws:policy/IAMReadOnlyAccess") {
    return Policy{cct, tenant, std::string{IAMReadOnlyAccess}, reject};
  } else if (arn == "arn:aws:iam::aws:policy/AmazonSNSFullAccess") {
    return Policy{cct, tenant, std::string{AmazonSNSFullAccess}, reject};
  } else if (arn == "arn:aws:iam::aws:policy/AmazonSNSReadOnlyAccess") {
    return Policy{cct, tenant, std::string{AmazonSNSReadOnlyAccess}, reject};
  } else if (arn == "arn:aws:iam::aws:policy/AmazonS3FullAccess") {
    return Policy{cct, tenant, std::string{AmazonS3FullAccess}, reject};
  } else if (arn == "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess") {
    return Policy{cct, tenant, std::string{AmazonS3ReadOnlyAccess}, reject};
  }
  return {};
}

void encode(const ManagedPolicies& m, bufferlist& bl, uint64_t f)
{
  ENCODE_START(1, 1, bl);
  encode(m.arns, bl);
  ENCODE_FINISH(bl);
}

void decode(ManagedPolicies& m, bufferlist::const_iterator& bl)
{
  DECODE_START(1, bl);
  decode(m.arns, bl);
  DECODE_FINISH(bl);
}

void ManagedPolicyAttachment::generate_test_instances(std::list<ManagedPolicyAttachment*>& o)
{

  ManagedPolicyAttachment* u = new ManagedPolicyAttachment;
  u->arn = "arn:aws:iam::123456789012:policy/TestPolicy1";
  u->status = "PENDING";
  o.push_back(u);

  ManagedPolicyAttachment* v = new ManagedPolicyAttachment;
  v->arn = "arn:aws:iam::123456789012:policy/TestPolicy2";
  v->status = "ATTACHED";
  o.push_back(v);
}

void ManagedPolicyAttachment::dump(Formatter *f) const
{
  encode_json("arn", arn, f);
  encode_json("status", status, f);
}

void ManagedPolicyAttachment::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("arn", arn, obj);
  JSONDecoder::decode_json("status", status, obj);
}

void ManagedPolicyInfo::dump(Formatter * const f) const
{
  encode_json("id", id, f);
  encode_json("policy_name", name, f);
  encode_json("path", path, f);
  encode_json("creation_time", creation_date, f);
  encode_json("update_time", update_date, f);
  encode_json("policy_document", policy_document, f);
  encode_json("description", description, f);
  encode_json("default_version", default_version, f);
  encode_json("account_id", account_id, f);
  encode_json("tags", tags, f);
  encode_json_map("attachments", "key", "val", attachments, f);
  encode_json("arn", arn, f);
  encode_json("attachment_count", attachment_count, f);
  encode_json("permissions_boundary_usage_count", attachment_count, f);
  encode_json("is_attachable", is_attachable, f);
}

static void decode_attachments(std::map<std::string, ManagedPolicyAttachment>& attachments, JSONObj* o)
{
  ManagedPolicyAttachment mpa;
  mpa.decode_json(o);
    if (!mpa.arn.empty()) {
    attachments[mpa.arn] = mpa;
  }
}

void ManagedPolicyInfo::decode_json(JSONObj * obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("policy_name", name, obj);
  JSONDecoder::decode_json("path", path, obj);
  JSONDecoder::decode_json("creation_time", creation_date, obj);
  JSONDecoder::decode_json("update_time", update_date, obj);
  JSONDecoder::decode_json("policy_document", policy_document, obj);
  JSONDecoder::decode_json("description", description, obj);
  JSONDecoder::decode_json("default_version", default_version, obj);
  JSONDecoder::decode_json("account_id", account_id, obj);
  JSONDecoder::decode_json("tags", tags, obj);
  JSONDecoder::decode_json("attachments", attachments, decode_attachments, obj);
  JSONDecoder::decode_json("arn", arn, obj);
  JSONDecoder::decode_json("attachment_count", attachment_count, obj);
  JSONDecoder::decode_json("permissions_boundary_usage_count", attachment_count, obj);
  JSONDecoder::decode_json("is_attachable", is_attachable, obj);
}

void ManagedPolicyInfo::generate_test_instances(std::list<ManagedPolicyInfo*>& o)
{
  o.push_back(new ManagedPolicyInfo);
  auto p = new ManagedPolicyInfo;
  p->id = "id";
  p->name = "policy_name";
  p->path = "/path/";
  p->account_id = "account";
  o.push_back(p);
}

std::vector<ManagedPolicyInfo> list_aws_managed_policy()
{
  std::vector<ManagedPolicyInfo> policies;

  auto add = [&](std::string_view name,
                std::string_view id,
                std::string_view policy_doc,
                std::string_view arn,
                std::string_view desc,
                std::string_view version) {
    ManagedPolicyInfo info;
    info.name = name;
    info.policy_document = policy_doc;
    info.arn = arn;
    info.description = desc;
    info.default_version = version;
    info.id = id;
    policies.push_back(info);
  };

  add("IAMFullAccess", "IAMFullAccess", IAMFullAccess, "arn:aws:iam::aws:policy/IAMFullAccess", "Full access to IAM", "v2");
  add("IAMReadOnlyAccess", "IAMReadOnlyAccess", IAMReadOnlyAccess, "arn:aws:iam::aws:policy/IAMReadOnlyAccess", "Read-only access to IAM", "v4");
  add("AmazonSNSFullAccess", "AmazonSNSFullAccess", AmazonSNSFullAccess, "arn:aws:iam::aws:policy/AmazonSNSFullAccess", "Full access to SNS", "v1");
  add("AmazonSNSReadOnlyAccess", "AmazonSNSReadOnlyAccess", AmazonSNSReadOnlyAccess, "arn:aws:iam::aws:policy/AmazonSNSReadOnlyAccess", "Read-only access to SNS", "v1");
  add("AmazonS3FullAccess", "AmazonS3FullAccess", AmazonS3FullAccess, "arn:aws:iam::aws:policy/AmazonS3FullAccess", "Full access to S3", "v2");
  add("AmazonS3ReadOnlyAccess", "AmazonS3ReadOnlyAccess", AmazonS3ReadOnlyAccess, "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess", "Read-only access to S3", "v3");

  return std::move(policies);
}

} // namespace rgw::IAM
