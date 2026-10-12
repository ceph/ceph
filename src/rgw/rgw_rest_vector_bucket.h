// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include "rgw_rest.h"
#include "rgw_rest_s3.h"

// GET /admin/vectorbucket/rebuild — background rebuild status + event log.
class RGWHandler_VectorBucketRebuild : public RGWHandler_Auth_S3 {
protected:
  RGWOp* op_get() override;

public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_VectorBucketRebuild() override = default;

  int read_permissions(RGWOp*, optional_yield) override { return 0; }
};

// /admin/vectorbucket has no operations of its own: vector buckets are managed
// by the S3 Vectors API. it only holds sub-resources ("rebuild")
class RGWRESTMgr_VectorBucket : public RGWRESTMgr {
public:
  RGWRESTMgr_VectorBucket();
  ~RGWRESTMgr_VectorBucket() override = default;
};

class RGWRESTMgr_VectorBucketRebuild : public RGWRESTMgr {
public:
  RGWRESTMgr_VectorBucketRebuild() = default;
  ~RGWRESTMgr_VectorBucketRebuild() override = default;

  RGWHandler_REST* get_handler(rgw::sal::Driver*, req_state*,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string&) override {
    return new RGWHandler_VectorBucketRebuild(auth_registry);
  }
};
