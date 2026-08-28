// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "rgw_rest.h"
#include "rgw_rest_s3.h"

class RGWHandler_REST_s3Vector : public RGWHandler_REST {
  const rgw::auth::StrategyRegistry& auth_registry;
  bufferlist bl_post_body;
  RGWOp *op_post() override;

public:
  RGWHandler_REST_s3Vector(const rgw::auth::StrategyRegistry& auth_registry,
                           const bufferlist& bl_post_body)
    : auth_registry(auth_registry), bl_post_body(bl_post_body) {}
  ~RGWHandler_REST_s3Vector() override = default;

  int init(rgw::sal::Driver* driver, req_state *s, rgw::io::BasicClient *cio) override;
  int authorize(const DoutPrefixProvider* dpp, optional_yield y) override;
  int postauth_init(optional_yield y) override { return 0; }
  int read_permissions(RGWOp* op, optional_yield y) override { return 0; }
};

// Ceph admin REST API for vector-bucket internals, mounted at
// /admin/vectorbucket. Operator/test-infra facing (not the S3 data path).
// Dispatches by query parameter; currently serves ?rebuild=true (background
// rebuild status + event log). Future session ops (?session=true) attach here.
class RGWHandler_VectorBucket : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get() override;
public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_VectorBucket() override = default;

  // admin handler: skip object-level permission loading (matches RGWHandler_Info)
  int read_permissions(RGWOp*, optional_yield) override { return 0; }
};

class RGWRESTMgr_VectorBucket : public RGWRESTMgr {
public:
  RGWRESTMgr_VectorBucket() = default;
  ~RGWRESTMgr_VectorBucket() override = default;

  RGWHandler_REST* get_handler(rgw::sal::Driver* driver,
                               req_state*,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string&) override {
    return new RGWHandler_VectorBucket(auth_registry);
  }
};
