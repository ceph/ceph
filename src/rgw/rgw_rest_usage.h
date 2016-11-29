// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_REST_USAGE_H
#define CEPH_RGW_REST_USAGE_H

#include "rgw_rest.h"
#include "rgw_rest_s3.h"


class RGWHandler_Usage : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get();
  RGWOp *op_delete();
public:
  RGWHandler_Usage() {}
  virtual ~RGWHandler_Usage() {}

  int read_permissions(RGWOp*) {
    return 0;
  }
};

class RGWRESTMgr_Usage : public RGWRESTMgr {
public:
  RGWRESTMgr_Usage() = default;
  virtual ~RGWRESTMgr_Usage() = default;

  RGWHandler_REST* get_handler(struct req_state*,
                               const std::string&) override {
    return new RGWHandler_Usage;
  }
};

#endif
