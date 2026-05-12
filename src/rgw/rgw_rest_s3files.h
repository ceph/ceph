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
// Per-op handlers are wired in opto the framework via op_put /
// op_get / op_post / op_delete on RGWHandler_REST_S3Files. Each
// dispatches by HTTP method + URI path to a concrete RGWOp
// subclass that interacts with the rgw::file_state::Store data
// layer.

#pragma once

#include "rgw_auth.h"
#include "rgw_rest.h"
#include "rgw_sal_fwd.h"

#include <memory>

#include "file_state/store.h"

class DoutPrefixProvider;

namespace rgw::s3files {

// Process-wide default Store used by the REST handlers.
//
// v1 returns a singleton MemoryStore; an FDB-backed store will
// replace this once PR ceph/ceph#65535 lands a shared FDB client
// and generic rgw_fdb_* configuration. Tests may swap the store
// before init via set_default_store().
rgw::file_state::Store& default_store();

// Set the default Store. Intended for tests; not thread-safe
// against concurrent default_store() calls.
void set_default_store(rgw::file_state::Store* store);

// Lifetime bundle for the in-process s3files reconciler. Owns
// an InProcessChangeFeed, a DbusGaneshaSink, and the Reconciler
// itself; wires the active Store's on-change hook to the feed,
// reads config from `cct`, and starts the worker thread on
// construction. Destruction stops the worker and tears
// everything down in the safe order. AppMain owns one of these
// when the s3files API is enabled AND
// rgw_s3files_reconciler_enabled is true.
class ReconcilerHarness {
 public:
  ReconcilerHarness(rgw::file_state::Store& store,
                     rgw::sal::Driver* driver,
                     CephContext* cct);
  ~ReconcilerHarness();
  ReconcilerHarness(const ReconcilerHarness&) = delete;
  ReconcilerHarness& operator=(const ReconcilerHarness&) = delete;
 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace rgw::s3files

class RGWHandler_REST_S3Files : public RGWHandler_REST {
  const rgw::auth::StrategyRegistry& auth_registry;
  // Body pre-read in RGWRESTMgr_S3Files::get_handler so the
  // `PayloadHash` argument is set before sigv4 verification (the
  // restJson1 service signs with service=s3files, and the
  // canonical-request body hash isn't on a wire header). Ops that
  // need the body grab it from here.
  ceph::bufferlist body_;

 public:
  RGWHandler_REST_S3Files(
      const rgw::auth::StrategyRegistry& auth_registry,
      ceph::bufferlist body)
    : RGWHandler_REST(),
      auth_registry(auth_registry),
      body_(std::move(body)) {}
  ~RGWHandler_REST_S3Files() override = default;

  int init(rgw::sal::Driver* driver,
           req_state* s,
           rgw::io::BasicClient* cio) override;
  int authorize(const DoutPrefixProvider* dpp, optional_yield y) override;
  int postauth_init(optional_yield) override { return 0; }

  // Per-method dispatch by URI path. Methods inspect
  // s->info.request_uri and return the matching RGWOp, or
  // nullptr to fall through to the framework's default 405/404
  // handling. Each method's body documents which paths it
  // recognizes.
  RGWOp* op_get() override;
  RGWOp* op_put() override;
  RGWOp* op_post() override;
  RGWOp* op_delete() override;
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
