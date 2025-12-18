// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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

class RGWHandler_Restore : public RGWHandler_Auth_S3 {
 protected:
  RGWOp *op_get() override;
 public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_Restore() override = default;

  int read_permissions(RGWOp*, optional_yield y) override {
    return 0;
  }
};

class RGWRESTMgr_Restore : public RGWRESTMgr {
 public:
  RGWRESTMgr_Restore() = default;
  ~RGWRESTMgr_Restore() override = default;

  RGWHandler_REST *get_handler(rgw::sal::Driver* driver, struct req_state*,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string&) override {
    return new RGWHandler_Restore(auth_registry);
  }
};