// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_ACL_SWIFT_H
#define CEPH_RGW_ACL_SWIFT_H

#include <list>
#include <map>
#include <string>
#include <include/types.h>

#include "rgw_acl.h"

class RGWAccessControlPolicy_SWIFT : public RGWAccessControlPolicy
{
public:
  explicit RGWAccessControlPolicy_SWIFT(CephContext *_cct) : RGWAccessControlPolicy(_cct) {}
  ~RGWAccessControlPolicy_SWIFT() {}

  void add_grants(RGWRados *store,
                  std::list<string>& uids, int perm);
  bool create(RGWRados *store, rgw_user& id,
              std::string& name,
              std::string& read_list,
              std::string& write_list);
  void to_str(std::string& read, std::string& write);
};

class RGWAccessControlPolicy_SWIFTAcct : public RGWAccessControlPolicy
{
public:
  RGWAccessControlPolicy_SWIFTAcct(CephContext * const _cct)
    : RGWAccessControlPolicy(_cct)
  {}
  ~RGWAccessControlPolicy_SWIFTAcct() {}

  void add_grants(RGWRados *store, const std::list<string>& uids, int perm);
  bool create(RGWRados *store,
              const rgw_user& id,
              const std::string& name,
              const std::string& acl_str);
  void to_str(std::string& acl) const;
};
#endif
