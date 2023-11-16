// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <map>
#include <vector>
#include <string>
#include <include/types.h>

#include <boost/optional.hpp>

#include "rgw_acl.h"

class RGWUserCtl;

namespace rgw::swift {

/// Create a policy based on swift container acl headers
/// X-Container-Read/X-Container-Write.
int create_container_policy(const DoutPrefixProvider *dpp,
                            rgw::sal::Driver* driver,
                            const rgw_user& id,
                            const std::string& name,
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

} // namespace rgw::swift

class RGWAccessControlPolicy_SWIFT : public RGWAccessControlPolicy
{
};

class RGWAccessControlPolicy_SWIFTAcct : public RGWAccessControlPolicy
{
public:
  bool create(const DoutPrefixProvider *dpp,
	      rgw::sal::Driver* driver,
              const rgw_user& id,
              const std::string& name,
              const std::string& acl_str);
  boost::optional<std::string> to_str() const;
};
