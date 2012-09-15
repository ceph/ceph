
#ifndef CEPH_RGW_SWIFT_H
#define CEPH_RGW_SWIFT_H

#include "rgw_common.h"

class RGWRados;

struct rgw_swift_auth_info {
  int status;
  char *auth_groups;
  char *user;
  long long ttl;
};

bool rgw_verify_swift_token(RGWRados *store, req_state *s);


#endif

