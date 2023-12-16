// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <map>
#include <string>
#include <iosfwd>
#include <include/types.h>

#include "common/async/yield_context.h"
#include "rgw_xml.h"
#include "rgw_acl.h"
#include "rgw_sal_fwd.h"

class RGWEnv;

namespace rgw::s3 {

ACLGroupTypeEnum acl_uri_to_group(std::string_view uri);
bool acl_group_to_uri(ACLGroupTypeEnum group, std::string& uri);

/// Construct a policy from an AccessControlPolicy xml document. Email grantees
/// are looked up and converted to a corresponding CanonicalUser grant. All user
/// ids are verified to exist.
int parse_policy(const DoutPrefixProvider* dpp, optional_yield y,
                 rgw::sal::Driver* driver, std::string_view document,
                 RGWAccessControlPolicy& policy, std::string& err_msg);

/// Write an AccessControlPolicy xml document for the given policy.
void write_policy_xml(const RGWAccessControlPolicy& policy,
                      std::ostream& out);

/// Construct a policy from a s3 canned acl string.
int create_canned_acl(const ACLOwner& owner,
                      const ACLOwner& bucket_owner,
                      const std::string& canned_acl,
                      RGWAccessControlPolicy& policy);

/// Construct a policy from x-amz-grant-* request headers.
int create_policy_from_headers(const DoutPrefixProvider* dpp,
                               optional_yield y,
                               rgw::sal::Driver* driver,
                               const ACLOwner& owner,
                               const RGWEnv& env,
                               RGWAccessControlPolicy& policy);

} // namespace rgw::s3
