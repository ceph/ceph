
#ifndef CEPH_RGW_SWIFT_H
#define CEPH_RGW_SWIFT_H

#include "rgw_common.h"


struct rgw_swift_auth_info {
  int status;
  char *auth_groups;
  char *user;
  long long ttl;
};

bool rgw_verify_os_token(req_state *s);


#endif

