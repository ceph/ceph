// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "rgw_rest.h"
#include "rgw_rest_s3.h"

/// Admin Ops API of the STS external authorizer config
///   GET    /admin/ext-authz   read (the credential is redacted)
///   PUT    /admin/ext-authz   set, body is the json config
///   DELETE /admin/ext-authz   remove
/// Requires the 'ext-authz' capability (read for GET, write otherwise).
class RGWHandler_ExtAuthz : public RGWHandler_Auth_S3 {
 protected:
  RGWOp* op_get() override;
  RGWOp* op_put() override;
  RGWOp* op_delete() override;
 public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_ExtAuthz() override = default;

  int read_permissions(RGWOp*, optional_yield) override {
    return 0;
  }
};

class RGWRESTMgr_ExtAuthz : public RGWRESTMgr {
 public:
  RGWRESTMgr_ExtAuthz() = default;
  ~RGWRESTMgr_ExtAuthz() override = default;

  RGWHandler_REST* get_handler(rgw::sal::Driver* driver, req_state*,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string&) override {
    return new RGWHandler_ExtAuthz(auth_registry);
  }
};
