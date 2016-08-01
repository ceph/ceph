// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_REST_BUCKET_H
#define CEPH_RGW_REST_BUCKET_H

#include "rgw_rest.h"
#include "rgw_rest_s3.h"


class RGWHandler_Bucket : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get();
  RGWOp *op_put();
  RGWOp *op_post();
  RGWOp *op_delete();
public:
  RGWHandler_Bucket() {}
  virtual ~RGWHandler_Bucket() {}

  int read_permissions(RGWOp*) {
    return 0;
  }
};

class RGWRESTMgr_Bucket : public RGWRESTMgr {
public:
  RGWRESTMgr_Bucket() {}
  virtual ~RGWRESTMgr_Bucket() {}

  RGWHandler_REST* get_handler(struct req_state *s) {
    return new RGWHandler_Bucket;
  }
};

#endif
