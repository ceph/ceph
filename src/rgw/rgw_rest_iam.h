// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_auth.h"
#include "rgw_auth_filters.h"
#include "rgw_rest.h"
#include "rgw_xml.h"

inline constexpr const char* RGW_REST_IAM_XMLNS =
    "https://iam.amazonaws.com/doc/2010-05-08/";

class DoutPrefixProvider;
namespace rgw { class SiteConfig; }
struct RGWUserInfo;

int forward_iam_request_to_master(const DoutPrefixProvider* dpp,
                                  const rgw::SiteConfig& site,
                                  const RGWUserInfo& user,
                                  bufferlist& indata,
                                  RGWXMLDecoder::XMLParser& parser,
                                  req_info& req, optional_yield y);
class RGWHandler_REST_IAM : public RGWHandler_REST {
  const rgw::auth::StrategyRegistry& auth_registry;
  bufferlist bl_post_body;
  RGWOp *op_post() override;

public:

  static bool action_exists(const req_state* s);

  RGWHandler_REST_IAM(const rgw::auth::StrategyRegistry& auth_registry,
		      bufferlist& bl_post_body)
    : RGWHandler_REST(),
      auth_registry(auth_registry),
      bl_post_body(bl_post_body) {}
  ~RGWHandler_REST_IAM() override = default;

  int init(rgw::sal::Driver* driver,
           req_state *s,
           rgw::io::BasicClient *cio) override;
  int authorize(const DoutPrefixProvider* dpp, optional_yield y) override;
  int postauth_init(optional_yield y) override { return 0; }
};

class RGWRESTMgr_IAM : public RGWRESTMgr {
public:
  RGWRESTMgr_IAM() = default;
  ~RGWRESTMgr_IAM() override = default;

  RGWRESTMgr *get_resource_mgr(req_state* const s,
                               const std::string& uri,
                               std::string* const out_uri) override {
    return this;
  }

  RGWHandler_REST* get_handler(rgw::sal::Driver* driver,
			       req_state*,
                               const rgw::auth::StrategyRegistry&,
                               const std::string&) override;
};
