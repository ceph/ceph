
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 SUSE LLC
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

class RGWHandler_S3AccountPublicAccessBlock : public RGWHandler_REST_S3 {
protected:
  RGWOp *op_put() override;
public:
  using RGWHandler_REST_S3::RGWHandler_REST_S3;
  ~RGWHandler_S3AccountPublicAccessBlock() override = default;
};

class RGWRESTMgr_S3AccountPublicAccessBlock : public RGWRESTMgr {
public:
  RGWRESTMgr_S3AccountPublicAccessBlock() = default;
  ~RGWRESTMgr_S3AccountPublicAccessBlock() override = default;

  RGWHandler_REST* get_handler(struct req_state* const s,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string&) override {
    if (RGWHandler_REST_S3::init_from_header(s, RGW_FORMAT_XML, true) < 0) {
      return nullptr;
    }

    return new RGWHandler_S3AccountPublicAccessBlock(auth_registry);
  }
};

class RGWHandler_S3Control: public RGWHandler_REST_S3 {
public:
  using RGWHandler_REST_S3::RGWHandler_REST_S3;
  ~RGWHandler_S3Control() override = default;
};

class RGWRESTMgr_S3Control : public RGWRESTMgr {
public:
  ~RGWRESTMgr_S3Control() override = default;
  RGWRESTMgr_S3Control() {
    register_resource("configuration/publicAccessBlock",
                      new RGWRESTMgr_S3AccountPublicAccessBlock());
  }

  RGWHandler_REST* get_handler(struct req_state*,
			       const rgw::auth::StrategyRegistry& auth_registry,
			       const std::string&) override {
    return new RGWHandler_S3Control(auth_registry);
  }
};

class RGWPutAccountPublicAccessBlock : public RGWRESTOp {
protected:
  bufferlist data;
  PublicAccessBlockConfiguration access_conf;
public:
  int check_caps(RGWUserCaps& caps) override;
  int verify_permission() override;
  const char* name() const override { return "put_account_public_access_block";}
  int get_params();
  RGWOpType get_type() override;
  void execute() override;
  void send_response() override;
};
