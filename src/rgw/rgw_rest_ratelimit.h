// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "rgw_sal_rados.h"

class RGWHandler_Ratelimit : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get() override;
  RGWOp *op_post() override;
public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_Ratelimit() override = default;

  int read_permissions(RGWOp*, optional_yield) override {
    return 0;
  }
};

class RGWRESTMgr_Ratelimit : public RGWRESTMgr {
public:
  RGWRESTMgr_Ratelimit() = default;
  ~RGWRESTMgr_Ratelimit() override = default;

  RGWHandler_REST *get_handler(rgw::sal::Store* store,
			       struct req_state*,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string&) override {
    return new RGWHandler_Ratelimit(auth_registry);
  }
};