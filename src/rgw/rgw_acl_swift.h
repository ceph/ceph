// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_ACL_SWIFT_H
#define CEPH_RGW_ACL_SWIFT_H

#include <map>
#include <vector>
#include <string>
#include <include/types.h>

#include <boost/optional.hpp>

#include "rgw_acl.h"

class RGWAccessControlPolicy_SWIFT : public RGWAccessControlPolicy
{
  int add_grants(RGWRados *store,
                 const std::vector<std::string>& uids,
                 uint32_t perm);

public:
  explicit RGWAccessControlPolicy_SWIFT(CephContext* const cct)
    : RGWAccessControlPolicy(cct) {
  }
  ~RGWAccessControlPolicy_SWIFT() override = default;

  int create(RGWRados *store,
             const rgw_user& id,
             const std::string& name,
             const std::string& read_list,
             const std::string& write_list,
             uint32_t& rw_mask);
  void filter_merge(uint32_t mask, RGWAccessControlPolicy_SWIFT *policy);
  void to_str(std::string& read, std::string& write);
};

class RGWAccessControlPolicy_SWIFTAcct : public RGWAccessControlPolicy
{
public:
  RGWAccessControlPolicy_SWIFTAcct(CephContext * const cct)
    : RGWAccessControlPolicy(cct) {
  }
  ~RGWAccessControlPolicy_SWIFTAcct() override {}

  void add_grants(RGWRados *store,
                  const std::vector<std::string>& uids,
                  uint32_t perm);
  bool create(RGWRados *store,
              const rgw_user& id,
              const std::string& name,
              const std::string& acl_str);
  boost::optional<std::string> to_str() const;
};
#endif
