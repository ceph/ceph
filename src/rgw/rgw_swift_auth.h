#ifndef CEPH_RGW_SWIFT_AUTH_H
#define CEPH_RGW_SWIFT_AUTH_H

#include "rgw_op.h"

#define RGW_SWIFT_TOKEN_EXPIRATION (15 * 60)

extern int rgw_swift_verify_signed_token(CephContext *cct, RGWRados *store, const char *token, RGWUserInfo& info);

class RGW_SWIFT_Auth_Get : public RGWOp {
public:
  RGW_SWIFT_Auth_Get() {}
  ~RGW_SWIFT_Auth_Get() {}

  int verify_permission() { return 0; }
  void execute();
  virtual const char *name() { return "swift_auth_get"; }
};

class RGWHandler_SWIFT_Auth : public RGWHandler {
public:
  RGWHandler_SWIFT_Auth() {}
  ~RGWHandler_SWIFT_Auth() {}
  RGWOp *get_op();
  void put_op(RGWOp *op);

  int init(RGWRados *store, struct req_state *state, FCGX_Request *fcgx);
  int authorize();
  int read_permissions(RGWOp *op) { return 0; }

  virtual RGWAccessControlPolicy *alloc_policy() { return NULL; }
  virtual void free_policy(RGWAccessControlPolicy *policy) {}
};

#endif
