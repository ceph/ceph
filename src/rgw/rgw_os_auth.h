#ifndef CEPH_RGW_OS_AUTH_H
#define CEPH_RGW_OS_AUTH_H

#include "rgw_op.h"

class RGW_OS_Auth_Get : public RGWOp {
public:
  RGW_OS_Auth_Get() {}
  ~RGW_OS_Auth_Get() {}

  void execute();
};

class RGWHandler_OS_Auth : public RGWHandler {
public:
  RGWHandler_OS_Auth() {}
  ~RGWHandler_OS_Auth() {}
  RGWOp *get_op();

  bool authorize(struct req_state *s);
  int read_permissions() { return 0; }
};


#endif
