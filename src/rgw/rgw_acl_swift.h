// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>
#include "rgw_sal_fwd.h"
#include "rgw_user_types.h"

struct ACLOwner;
class DoutPrefixProvider;
class RGWAccessControlPolicy;

namespace rgw::swift {

/// Create a policy based on swift container acl headers
/// X-Container-Read/X-Container-Write.
int create_container_policy(const DoutPrefixProvider *dpp,
                            rgw::sal::Driver* driver,
                            const ACLOwner& owner,
                            const char* read_list,
                            const char* write_list,
                            uint32_t& rw_mask,
                            RGWAccessControlPolicy& policy);

/// Copy grants matching the permission mask (SWIFT_PERM_READ/WRITE) from
/// one policy to another.
void merge_policy(uint32_t rw_mask, const RGWAccessControlPolicy& src,
                  RGWAccessControlPolicy& dest);

/// Format the policy in terms of X-Container-Read/X-Container-Write strings.
void format_container_acls(const RGWAccessControlPolicy& policy,
                           std::string& read, std::string& write);

/// Create a policy based on swift account acl header X-Account-Access-Control.
int create_account_policy(const DoutPrefixProvider* dpp,
                          rgw::sal::Driver* driver,
                          const ACLOwner& owner,
                          const std::string& acl_str,
                          RGWAccessControlPolicy& policy);

/// Format the policy in terms of the X-Account-Access-Control string. Returns
/// std::nullopt if there are no admin/read-write/read-only entries.
auto format_account_acl(const RGWAccessControlPolicy& policy)
  -> std::optional<std::string>;

} // namespace rgw::swift
