#ifndef CEPH_RGW_SWIFT_AUTH_H
#define CEPH_RGW_SWIFT_AUTH_H

#include "rgw_op.h"

#define RGW_SWIFT_TOKEN_EXPIRATION (15 * 60)

extern int rgw_swift_verify_signed_token(CephContext *cct, const char *token, RGWUserInfo& info);

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

  bool filter_request(struct req_state *s);

  int validate_bucket_name(const string& bucket) { return 0; }
  int validate_object_name(const string& object) { return 0; }
  int init(struct req_state *state, FCGX_Request *fcgx);
  int authorize();
  int read_permissions(RGWOp *op) { return 0; }

  virtual RGWAccessControlPolicy *alloc_policy() { return NULL; }
  virtual void free_policy(RGWAccessControlPolicy *policy) {}
};

#endif
