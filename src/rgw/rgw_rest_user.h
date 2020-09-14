// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_rest.h"
#include "rgw_rest_s3.h"


class RGWHandler_User : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get() override;
  RGWOp *op_put() override;
  RGWOp *op_post() override;
  RGWOp *op_delete() override;
public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_User() override = default;

  int read_permissions(RGWOp*) override {
    return 0;
  }
};

class RGWRESTMgr_User : public RGWRESTMgr {
public:
  RGWRESTMgr_User() = default;
  ~RGWRESTMgr_User() override = default;

  RGWHandler_REST *get_handler(rgw::sal::RGWRadosStore *store,
			       struct req_state*,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string&) override {
    return new RGWHandler_User(auth_registry);
  }
};
