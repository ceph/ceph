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
  explicit RGWHandler_REST_s3Vector(const rgw::auth::StrategyRegistry& auth_registry)
    : auth_registry(auth_registry) {}
  ~RGWHandler_REST_s3Vector() override = default;

  int init(rgw::sal::Driver* driver, req_state *s, rgw::io::BasicClient *cio) override;
  int authorize(const DoutPrefixProvider* dpp, optional_yield y) override;
  int postauth_init(optional_yield y) override;
  int read_permissions(RGWOp* op, optional_yield y) override { return 0; }
};

// GET/DELETE /admin/vectorbucket/session — admin API for the LanceDB session cache.
class RGWHandler_VectorBucketSession : public RGWHandler_Auth_S3 {
protected:
  RGWOp* op_get() override;
  RGWOp* op_delete() override;
public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_VectorBucketSession() override = default;
  int read_permissions(RGWOp*, optional_yield) override { return 0; }
};

class RGWRESTMgr_VectorBucketSession : public RGWRESTMgr {
public:
  RGWHandler_REST* get_handler(rgw::sal::Driver*,
                               req_state*,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string&) override {
    return new RGWHandler_VectorBucketSession(auth_registry);
  }
};

// GET /admin/vectorbucket/rebuild — background rebuild status + event log.
class RGWHandler_VectorBucketRebuild : public RGWHandler_Auth_S3 {
protected:
  RGWOp* op_get() override;
public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_VectorBucketRebuild() override = default;
  int read_permissions(RGWOp*, optional_yield) override { return 0; }
};

class RGWRESTMgr_VectorBucketRebuild : public RGWRESTMgr {
public:
  RGWHandler_REST* get_handler(rgw::sal::Driver*,
                               req_state*,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string&) override {
    return new RGWHandler_VectorBucketRebuild(auth_registry);
  }
};

// Ceph admin REST API for vector-bucket internals, mounted at /admin/vectorbucket.
// Sub-resources:
//   /admin/vectorbucket/rebuild — background rebuild status + event log
//   /admin/vectorbucket/session — LanceDB session cache (GET list, DELETE evict)
class RGWHandler_VectorBucket : public RGWHandler_Auth_S3 {
public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_VectorBucket() override = default;
  int read_permissions(RGWOp*, optional_yield) override { return 0; }
};

class RGWRESTMgr_VectorBucket : public RGWRESTMgr {
public:
  RGWRESTMgr_VectorBucket() {
    register_resource("session", new RGWRESTMgr_VectorBucketSession);
    register_resource("rebuild", new RGWRESTMgr_VectorBucketRebuild);
  }
  ~RGWRESTMgr_VectorBucket() override = default;

  RGWHandler_REST* get_handler(rgw::sal::Driver*,
                               req_state*,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string&) override {
    return new RGWHandler_VectorBucket(auth_registry);
  }
};
