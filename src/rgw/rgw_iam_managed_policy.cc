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

#include "rgw_iam_managed_policy.h"
#include "rgw_iam_policy.h"

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

} // namespace rgw::IAM
