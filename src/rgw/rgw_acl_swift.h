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

} // namespace rgw::swift

class RGWAccessControlPolicy_SWIFT : public RGWAccessControlPolicy
{
public:
  void filter_merge(uint32_t mask, RGWAccessControlPolicy_SWIFT *policy);
  void to_str(std::string& read, std::string& write);
};

class RGWAccessControlPolicy_SWIFTAcct : public RGWAccessControlPolicy
{
  void add_grants(const DoutPrefixProvider* dpp, rgw::sal::Driver* driver,
                  const std::vector<std::string>& uids, uint32_t perm);
public:
  bool create(const DoutPrefixProvider *dpp,
	      rgw::sal::Driver* driver,
              const rgw_user& id,
              const std::string& name,
              const std::string& acl_str);
  boost::optional<std::string> to_str() const;
};
