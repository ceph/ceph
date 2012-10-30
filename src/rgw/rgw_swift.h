
#ifndef CEPH_RGW_SWIFT_H
#define CEPH_RGW_SWIFT_H

#include "rgw_common.h"

class RGWRados;

struct rgw_swift_auth_info {
  int status;
  string auth_groups;
  string user;
  long long ttl;

  rgw_swift_auth_info() : status(0), ttl(0) {}
};

bool rgw_verify_swift_token(RGWRados *store, req_state *s);


#endif

