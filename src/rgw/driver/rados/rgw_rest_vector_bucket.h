// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include "rgw_rest.h"
#include "rgw_rest_s3.h"

class RGWHandler_VectorBucket : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get() override;
  RGWOp *op_delete() override;

public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_VectorBucket() override = default;

  int read_permissions(RGWOp*, optional_yield) override {
    return 0;
  }
};

class RGWRESTMgr_VectorBucket : public RGWRESTMgr {
public:
  RGWRESTMgr_VectorBucket() = default;
  ~RGWRESTMgr_VectorBucket() override = default;

  RGWHandler_REST* get_handler(rgw::sal::Driver*,
                               req_state*,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string&) override {
    return new RGWHandler_VectorBucket(auth_registry);
  }
};
