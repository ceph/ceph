// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_ACL_SWIFT_H
#define CEPH_RGW_ACL_SWIFT_H

#include <list>
#include <map>
#include <string>
#include <include/types.h>

#include "rgw_acl.h"

using namespace std;

class RGWAccessControlPolicy_SWIFT : public RGWAccessControlPolicy
{
public:
  explicit RGWAccessControlPolicy_SWIFT(CephContext *_cct) : RGWAccessControlPolicy(_cct) {}
  ~RGWAccessControlPolicy_SWIFT() {}

  void add_grants(RGWRados *store, list<string>& uids, int perm);
  bool create(RGWRados *store, rgw_user& id, string& name, const char* read_list, const char* write_list, uint32_t& rw_mask);
  void filter_merge(uint32_t mask, RGWAccessControlPolicy_SWIFT *policy);
  void to_str(string& read, string& write);
};

#endif
