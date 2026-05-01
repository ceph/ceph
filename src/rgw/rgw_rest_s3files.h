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

// REST manager for the AWS S3 Files API control plane.
//
// Wire protocol is REST + JSON (Smithy `restJson1`), authenticated
// with sigv4. The Smithy model is vendored in-tree at
// src/rgw/spec/aws/s3files/<version>/. The manager is registered
// in rgw::AppMain::cond_init_apis() at four top-level URL prefixes
// matching the AWS shape:
//
//   /file-systems
//   /access-points
//   /mount-targets
//   /resource-tags
//
// This skeleton wires the manager and dialect (RGW_REST_S3FILES)
// through to the request pipeline. Per-op handlers land in
// follow-on commits and use rgw::file_state::Store as the data
// layer.

#pragma once

#include "rgw_auth.h"
#include "rgw_rest.h"
#include "rgw_sal_fwd.h"

class DoutPrefixProvider;

class RGWHandler_REST_S3Files : public RGWHandler_REST {
  const rgw::auth::StrategyRegistry& auth_registry;

 public:
  explicit RGWHandler_REST_S3Files(
      const rgw::auth::StrategyRegistry& auth_registry)
    : RGWHandler_REST(),
      auth_registry(auth_registry) {}
  ~RGWHandler_REST_S3Files() override = default;

  int init(rgw::sal::Driver* driver,
           req_state* s,
           rgw::io::BasicClient* cio) override;
  int authorize(const DoutPrefixProvider* dpp, optional_yield y) override;
  int postauth_init(optional_yield) override { return 0; }

  // Per-op handlers are wired in subsequent commits. The base
  // class returns nullptr by default, which yields a 405 / 404
  // response for any method on any path — matching "API enabled
  // but no operations registered yet".
};

class RGWRESTMgr_S3Files : public RGWRESTMgr {
 public:
  RGWRESTMgr_S3Files() = default;
  ~RGWRESTMgr_S3Files() override = default;

  RGWRESTMgr* get_resource_mgr(
      req_state* const s,
      const std::string& uri,
      std::string* const out_uri) override {
    return this;
  }

  RGWHandler_REST* get_handler(
      rgw::sal::Driver* driver,
      req_state* s,
      const rgw::auth::StrategyRegistry& auth_registry,
      const std::string& frontend_prefix) override;
};
