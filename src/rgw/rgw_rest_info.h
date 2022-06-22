// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_rest.h"
#include "rgw_rest_s3.h"


class RGWHandler_Info : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get() override;
public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_Info() override = default;

  int read_permissions(RGWOp*, optional_yield) override {
    return 0;
  }
};

class RGWRESTMgr_Info : public RGWRESTMgr {
public:
  RGWRESTMgr_Info() = default;
  ~RGWRESTMgr_Info() override = default;

  RGWHandler_REST* get_handler(rgw::sal::Store* store,
			       req_state*,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string&) override {
    return new RGWHandler_Info(auth_registry);
  }
};
