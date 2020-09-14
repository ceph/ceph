// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_auth.h"
#include "rgw_auth_filters.h"

class RGWHandler_REST_IAM : public RGWHandler_REST {
  const rgw::auth::StrategyRegistry& auth_registry;
  const string& post_body;
  RGWOp *op_post() override;
  void rgw_iam_parse_input();
public:

  static int init_from_header(struct req_state *s, int default_formatter, bool configurable_format);

  RGWHandler_REST_IAM(const rgw::auth::StrategyRegistry& auth_registry, const string& post_body="")
    : RGWHandler_REST(),
      auth_registry(auth_registry),
      post_body(post_body) {}
  ~RGWHandler_REST_IAM() override = default;

  int init(rgw::sal::RGWRadosStore *store,
           struct req_state *s,
           rgw::io::BasicClient *cio) override;
  int authorize(const DoutPrefixProvider* dpp) override;
  int postauth_init() override { return 0; }
};

class RGWRESTMgr_IAM : public RGWRESTMgr {
public:
  RGWRESTMgr_IAM() = default;
  ~RGWRESTMgr_IAM() override = default;

  RGWRESTMgr *get_resource_mgr(struct req_state* const s,
                               const std::string& uri,
                               std::string* const out_uri) override {
    return this;
  }

  RGWHandler_REST* get_handler(rgw::sal::RGWRadosStore *store,
			       struct req_state*,
                               const rgw::auth::StrategyRegistry&,
                               const std::string&) override;
};
