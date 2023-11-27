// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include "rgw_rest_s3.h"

// s3 compliant notification handler factory
class RGWHandler_REST_PSNotifs_S3 : public RGWHandler_REST_S3 {
protected:
  int init_permissions(RGWOp* op, optional_yield y) override {return 0;}
  int read_permissions(RGWOp* op, optional_yield y) override {return 0;}
  bool supports_quota() override {return false;}
  RGWOp* op_get() override;
  RGWOp* op_put() override;
  RGWOp* op_delete() override;
public:
  using RGWHandler_REST_S3::RGWHandler_REST_S3;
  virtual ~RGWHandler_REST_PSNotifs_S3() = default;
  // following are used to generate the operations when invoked by another REST handler
  static RGWOp* create_get_op();
  static RGWOp* create_put_op();
  static RGWOp* create_delete_op();
};

// AWS compliant topics handler factory
class RGWHandler_REST_PSTopic_AWS : public RGWHandler_REST {
  const rgw::auth::StrategyRegistry& auth_registry;
protected:
  RGWOp* op_post() override;
public:
  RGWHandler_REST_PSTopic_AWS(const rgw::auth::StrategyRegistry& _auth_registry) : 
      auth_registry(_auth_registry) {}
  virtual ~RGWHandler_REST_PSTopic_AWS() = default;
  int postauth_init(optional_yield) override { return 0; }
  int authorize(const DoutPrefixProvider* dpp, optional_yield y) override;
  static bool action_exists(const req_state* s);
  static bool action_exists(const req_info& info);
};

