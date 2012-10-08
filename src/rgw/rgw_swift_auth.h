#ifndef CEPH_RGW_SWIFT_AUTH_H
#define CEPH_RGW_SWIFT_AUTH_H

#include "rgw_op.h"
#include "rgw_rest.h"

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
  RGWOp *op_get();

  int init(RGWRados *store, struct req_state *state, RGWClientIO *cio);
  int authorize();
  int read_permissions(RGWOp *op) { return 0; }

  virtual RGWAccessControlPolicy *alloc_policy() { return NULL; }
  virtual void free_policy(RGWAccessControlPolicy *policy) {}
};

class RGWRESTMgr_SWIFT_Auth : public RGWRESTMgr {
public:
  RGWRESTMgr_SWIFT_Auth() {}
  virtual ~RGWRESTMgr_SWIFT_Auth() {}

  virtual RGWRESTMgr *get_resource_mgr(struct req_state *s, const string& uri) {
    return this;
  }
  virtual RGWHandler *get_handler(struct req_state *s) {
    return new RGWHandler_SWIFT_Auth;
  }
};


#endif
