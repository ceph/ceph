// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_REST_USER_H
#define CEPH_RGW_REST_USER_H

#include "rgw_rest.h"
#include "rgw_rest_s3.h"


class RGWHandler_User : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get();
  RGWOp *op_put();
  RGWOp *op_post();
  RGWOp *op_delete();
public:
  RGWHandler_User() {}
  virtual ~RGWHandler_User() {}

  int read_permissions(RGWOp*) {
    return 0;
  }
};

class RGWRESTMgr_User : public RGWRESTMgr {
public:
  RGWRESTMgr_User() {}
  virtual ~RGWRESTMgr_User() {}

  RGWHandler_REST *get_handler(struct req_state *s) {
    return new RGWHandler_User;
  }
};

#endif
