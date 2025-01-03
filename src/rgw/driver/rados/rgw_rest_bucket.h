// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_rest.h"
#include "rgw_rest_s3.h"


class RGWHandler_Bucket : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get() override;
  RGWOp *op_put() override;
  RGWOp *op_post() override;
  RGWOp *op_delete() override;
public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_Bucket() override = default;

  int read_permissions(RGWOp*, optional_yield y) override {
    return 0;
  }
};

class RGWRESTMgr_Bucket : public RGWRESTMgr {
public:
  RGWRESTMgr_Bucket() = default;
  ~RGWRESTMgr_Bucket() override = default;

  RGWHandler_REST* get_handler(rgw::sal::Driver* driver,
			       req_state*,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string&) override {
    return new RGWHandler_Bucket(auth_registry);
  }
};
