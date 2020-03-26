// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_auth.h"
#include "rgw_auth_filters.h"
#include "rgw_sts.h"
#include "rgw_web_idp.h"
#include "jwt-cpp/jwt.h"

namespace rgw::auth::sts {

class WebTokenEngine : public rgw::auth::Engine {
  CephContext* const cct;

  using result_t = rgw::auth::Engine::result_t;
  using token_t = rgw::web_idp::WebTokenClaims;

  const rgw::auth::TokenExtractor* const extractor;
  const rgw::auth::WebIdentityApplier::Factory* const apl_factory;

  bool is_applicable(const std::string& token) const noexcept;

  boost::optional<WebTokenEngine::token_t>
  get_from_jwt(const DoutPrefixProvider* dpp, const std::string& token) const;

  void validate_signature (const DoutPrefixProvider* dpp, const jwt::decoded_jwt& decoded, const string& algorithm, const string& iss) const;

  result_t authenticate(const DoutPrefixProvider* dpp,
                        const std::string& token,
                        const req_state* s) const;

public:
  WebTokenEngine(CephContext* const cct,
                    const rgw::auth::TokenExtractor* const extractor,
                    const rgw::auth::WebIdentityApplier::Factory* const apl_factory)
    : cct(cct),
      extractor(extractor),
      apl_factory(apl_factory) {
  }

  const char* get_name() const noexcept override {
    return "rgw::auth::sts::WebTokenEngine";
  }

  result_t authenticate(const DoutPrefixProvider* dpp, const req_state* const s) const override {
    return authenticate(dpp, extractor->get_token(s), s);
  }
}; /* class WebTokenEngine */

class DefaultStrategy : public rgw::auth::Strategy,
                        public rgw::auth::TokenExtractor,
                        public rgw::auth::WebIdentityApplier::Factory {
  RGWCtl* const ctl;
  ImplicitTenants& implicit_tenant_context;

  /* The engine. */
  const WebTokenEngine web_token_engine;

  using aplptr_t = rgw::auth::IdentityApplier::aplptr_t;

  /* The method implements TokenExtractor for Web Token in req_state. */
  std::string get_token(const req_state* const s) const override {
    return s->info.args.get("WebIdentityToken");
  }

  aplptr_t create_apl_web_identity( CephContext* cct,
                                    const req_state* s,
                                    const rgw::web_idp::WebTokenClaims& token) const override {
    auto apl = rgw::auth::add_sysreq(cct, ctl, s,
      rgw::auth::WebIdentityApplier(cct, ctl, token));
    return aplptr_t(new decltype(apl)(std::move(apl)));
  }

public:
  DefaultStrategy(CephContext* const cct,
                  ImplicitTenants& implicit_tenant_context,
                  RGWCtl* const ctl)
    : ctl(ctl),
      implicit_tenant_context(implicit_tenant_context),
      web_token_engine(cct,
                        static_cast<rgw::auth::TokenExtractor*>(this),
                        static_cast<rgw::auth::WebIdentityApplier::Factory*>(this)) {
    /* When the constructor's body is being executed, all member engines
     * should be initialized. Thus, we can safely add them. */
    using Control = rgw::auth::Strategy::Control;
    add_engine(Control::SUFFICIENT, web_token_engine);
  }

  const char* get_name() const noexcept override {
    return "rgw::auth::sts::DefaultStrategy";
  }
};

} // namespace rgw::auth::sts

class RGWREST_STS : public RGWRESTOp {
protected:
  STS::STSService sts;
public:
  RGWREST_STS() = default;
  int verify_permission() override;
  void send_response() override;
};

class RGWSTSAssumeRoleWithWebIdentity : public RGWREST_STS {
protected:
  string duration;
  string providerId;
  string policy;
  string roleArn;
  string roleSessionName;
  string sub;
  string aud;
  string iss;
public:
  RGWSTSAssumeRoleWithWebIdentity() = default;
  void execute() override;
  int get_params();
  const char* name() const override { return "assume_role_web_identity"; }
  RGWOpType get_type() override { return RGW_STS_ASSUME_ROLE_WEB_IDENTITY; }
};

class RGWSTSAssumeRole : public RGWREST_STS {
protected:
  string duration;
  string externalId;
  string policy;
  string roleArn;
  string roleSessionName;
  string serialNumber;
  string tokenCode;
public:
  RGWSTSAssumeRole() = default;
  void execute() override;
  int get_params();
  const char* name() const override { return "assume_role"; }
  RGWOpType get_type() override { return RGW_STS_ASSUME_ROLE; }
};

class RGWSTSGetSessionToken : public RGWREST_STS {
protected:
  string duration;
  string serialNumber;
  string tokenCode;
public:
  RGWSTSGetSessionToken() = default;
  void execute() override;
  int verify_permission() override;
  int get_params();
  const char* name() const override { return "get_session_token"; }
  RGWOpType get_type() override { return RGW_STS_GET_SESSION_TOKEN; }
};

class RGW_Auth_STS {
public:
  static int authorize(const DoutPrefixProvider *dpp,
                       rgw::sal::RGWRadosStore *store,
                       const rgw::auth::StrategyRegistry& auth_registry,
                       struct req_state *s);
};

class RGWHandler_REST_STS : public RGWHandler_REST {
  const rgw::auth::StrategyRegistry& auth_registry;
  const string& post_body;
  RGWOp *op_post() override;
  void rgw_sts_parse_input();
public:

  static int init_from_header(struct req_state *s, int default_formatter, bool configurable_format);

  RGWHandler_REST_STS(const rgw::auth::StrategyRegistry& auth_registry, const string& post_body="")
    : RGWHandler_REST(),
      auth_registry(auth_registry),
      post_body(post_body) {}
  ~RGWHandler_REST_STS() override = default;

  int init(rgw::sal::RGWRadosStore *store,
           struct req_state *s,
           rgw::io::BasicClient *cio) override;
  int authorize(const DoutPrefixProvider* dpp) override;
  int postauth_init() override { return 0; }
};

class RGWRESTMgr_STS : public RGWRESTMgr {
public:
  RGWRESTMgr_STS() = default;
  ~RGWRESTMgr_STS() override = default;
  
  RGWRESTMgr *get_resource_mgr(struct req_state* const s,
                               const std::string& uri,
                               std::string* const out_uri) override {
    return this;
  }

  RGWHandler_REST* get_handler(struct req_state*,
                               const rgw::auth::StrategyRegistry&,
                               const std::string&) override;
};
