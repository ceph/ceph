// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "rgw_rest_s3.h"

class RGWHandler_REST_s3Vector : public RGWHandler_REST_S3 {
protected:
  int init_permissions(RGWOp* op, optional_yield y) override {return 0;}
  int read_permissions(RGWOp* op, optional_yield y) override {return 0;}
  bool supports_quota() override {return false;}
public:
  explicit RGWHandler_REST_s3Vector(const rgw::auth::StrategyRegistry& auth_registry)
    : RGWHandler_REST_S3(auth_registry) {}
  virtual ~RGWHandler_REST_s3Vector() = default;
  RGWOp *op_post() override;
  static RGWOp* create_post_op(const std::string& op_name);
};

