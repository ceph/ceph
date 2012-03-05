#ifndef CEPH_RGW_ACL_SWIFT_H
#define CEPH_RGW_ACL_SWIFT3_H

#include <map>
#include <string>
#include <iostream>
#include <vector>
#include <include/types.h>

#include "rgw_acl.h"

using namespace std;

class RGWAccessControlPolicy_SWIFT : public RGWAccessControlPolicy
{
public:
  RGWAccessControlPolicy_SWIFT() {}
  ~RGWAccessControlPolicy_SWIFT() {}

  void add_grants(vector<string>& uids, int perm);
  bool create(string& id, string& name, string& read_list, string& write_list);
  void to_str(string& read, string& write);
};

#endif
