
#ifndef CEPH_RGW_OS_H
#define CEPH_RGW_OS_H


struct rgw_os_auth_info {
  int retcode;
  char *auth_groups;
  long long ttl;
};

bool rgw_verify_os_token(req_state *s);


#endif

