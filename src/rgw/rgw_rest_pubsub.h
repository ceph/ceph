// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include "rgw_rest_s3.h"

// s3 compliant notification handler factory
class RGWHandler_REST_PSNotifs_S3 : public RGWHandler_REST_S3 {
protected:
  int init_permissions(RGWOp* op) override {return 0;}
  int read_permissions(RGWOp* op) override {return 0;}
  bool supports_quota() override {return false;}
  RGWOp* op_get() override;
  RGWOp* op_put() override;
  RGWOp* op_delete() override;
public:
  RGWHandler_REST_PSNotifs_S3(const rgw::auth::StrategyRegistry& auth_registry) : RGWHandler_REST_S3(auth_registry) {}
  virtual ~RGWHandler_REST_PSNotifs_S3() = default;
};

// AWS compliant topics handler factory
class RGWHandler_REST_PSTopic_AWS : public RGWHandler_REST_S3 {
protected:
  int init_permissions(RGWOp* op) override {return 0;}
  int read_permissions(RGWOp* op) override {return 0;}
  bool supports_quota() override {return false;}
  RGWOp *op_post() override;
public:
  explicit RGWHandler_REST_PSTopic_AWS(const rgw::auth::StrategyRegistry& auth_registry) : RGWHandler_REST_S3(auth_registry) {}
  virtual ~RGWHandler_REST_PSTopic_AWS() = default;
};

