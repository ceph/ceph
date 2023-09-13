// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_auth.h"
#include "rgw_auth_filters.h"
#include "rgw_rest.h"
#include "rgw_sts.h"
#include "rgw_web_idp.h"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#include "jwt-cpp/jwt.h"
#pragma clang diagnostic pop
#pragma GCC diagnostic pop
#include "rgw_oidc_provider.h"


namespace rgw::auth::sts {

class WebTokenEngine : public rgw::auth::Engine {
  static constexpr std::string_view princTagsNamespace = "https://aws.amazon.com/tags";
  CephContext* const cct;
  rgw::sal::Driver* driver;

  using result_t = rgw::auth::Engine::result_t;
  using Pair = std::pair<std::string, std::string>;
  using token_t = std::unordered_multimap<string, string>;
  using principal_tags_t = std::set<Pair>;

  const rgw::auth::TokenExtractor* const extractor;
  const rgw::auth::WebIdentityApplier::Factory* const apl_factory;

  bool is_applicable(const std::string& token) const noexcept;

  bool is_client_id_valid(std::vector<std::string>& client_ids, const std::string& client_id) const;

  bool is_cert_valid(const std::vector<std::string>& thumbprints, const std::string& cert) const;

  std::unique_ptr<rgw::sal::RGWOIDCProvider> get_provider(const DoutPrefixProvider *dpp, const std::string& role_arn, const std::string& iss, optional_yield y) const;

  std::string get_role_tenant(const std::string& role_arn) const;

  std::string get_role_name(const string& role_arn) const;

  std::string get_cert_url(const std::string& iss, const DoutPrefixProvider *dpp,optional_yield y) const;

  std::tuple<boost::optional<WebTokenEngine::token_t>, boost::optional<WebTokenEngine::principal_tags_t>>
  get_from_jwt(const DoutPrefixProvider* dpp, const std::string& token, const req_state* const s, optional_yield y) const;

  void validate_signature (const DoutPrefixProvider* dpp, const jwt::decoded_jwt& decoded, const std::string& algorithm, const std::string& iss, const std::vector<std::string>& thumbprints, optional_yield y) const;

  result_t authenticate(const DoutPrefixProvider* dpp,
                        const std::string& token,
                        const req_state* s, optional_yield y) const;

  template <typename T>
  void recurse_and_insert(const string& key, const jwt::claim& c, T& t) const;
  WebTokenEngine::token_t get_token_claims(const jwt::decoded_jwt& decoded) const;

public:
  WebTokenEngine(CephContext* const cct,
                    rgw::sal::Driver* driver,
                    const rgw::auth::TokenExtractor* const extractor,
                    const rgw::auth::WebIdentityApplier::Factory* const apl_factory)
    : cct(cct),
      driver(driver),
      extractor(extractor),
      apl_factory(apl_factory) {
  }

  const char* get_name() const noexcept override {
    return "rgw::auth::sts::WebTokenEngine";
  }

  result_t authenticate(const DoutPrefixProvider* dpp, const req_state* const s, optional_yield y) const override {
    return authenticate(dpp, extractor->get_token(s), s, y);
  }
}; /* class WebTokenEngine */

class DefaultStrategy : public rgw::auth::Strategy,
                        public rgw::auth::TokenExtractor,
                        public rgw::auth::WebIdentityApplier::Factory {
  rgw::sal::Driver* driver;
  const ImplicitTenants& implicit_tenant_context;

  /* The engine. */
  const WebTokenEngine web_token_engine;

  using aplptr_t = rgw::auth::IdentityApplier::aplptr_t;

  /* The method implements TokenExtractor for Web Token in req_state. */
  std::string get_token(const req_state* const s) const override {
    return s->info.args.get("WebIdentityToken");
  }

  aplptr_t create_apl_web_identity( CephContext* cct,
                                    const req_state* s,
                                    const std::string& role_session,
                                    const std::string& role_tenant,
                                    const std::unordered_multimap<std::string, std::string>& token,
                                    boost::optional<std::multimap<std::string, std::string>> role_tags,
                                    boost::optional<std::set<std::pair<std::string, std::string>>> principal_tags) const override {
    auto apl = rgw::auth::add_sysreq(cct, driver, s,
      rgw::auth::WebIdentityApplier(cct, driver, role_session, role_tenant, token, role_tags, principal_tags));
    return aplptr_t(new decltype(apl)(std::move(apl)));
  }

public:
  DefaultStrategy(CephContext* const cct,
                  const ImplicitTenants& implicit_tenant_context,
                  rgw::sal::Driver* driver)
    : driver(driver),
      implicit_tenant_context(implicit_tenant_context),
      web_token_engine(cct, driver,
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
  int verify_permission(optional_yield y) override;
  void send_response() override;
};

class RGWSTSAssumeRoleWithWebIdentity : public RGWREST_STS {
protected:
  std::string duration;
  std::string providerId;
  std::string policy;
  std::string roleArn;
  std::string roleSessionName;
  std::string sub;
  std::string aud;
  std::string iss;
public:
  RGWSTSAssumeRoleWithWebIdentity() = default;
  void execute(optional_yield y) override;
  int get_params();
  const char* name() const override { return "assume_role_web_identity"; }
  RGWOpType get_type() override { return RGW_STS_ASSUME_ROLE_WEB_IDENTITY; }
};

class RGWSTSAssumeRole : public RGWREST_STS {
protected:
  std::string duration;
  std::string externalId;
  std::string policy;
  std::string roleArn;
  std::string roleSessionName;
  std::string serialNumber;
  std::string tokenCode;
public:
  RGWSTSAssumeRole() = default;
  void execute(optional_yield y) override;
  int get_params();
  const char* name() const override { return "assume_role"; }
  RGWOpType get_type() override { return RGW_STS_ASSUME_ROLE; }
};

class RGWSTSGetSessionToken : public RGWREST_STS {
protected:
  std::string duration;
  std::string serialNumber;
  std::string tokenCode;
public:
  RGWSTSGetSessionToken() = default;
  void execute(optional_yield y) override;
  int verify_permission(optional_yield y) override;
  int get_params();
  const char* name() const override { return "get_session_token"; }
  RGWOpType get_type() override { return RGW_STS_GET_SESSION_TOKEN; }
};

class RGW_Auth_STS {
public:
  static int authorize(const DoutPrefixProvider *dpp,
                       rgw::sal::Driver* driver,
                       const rgw::auth::StrategyRegistry& auth_registry,
                       req_state *s, optional_yield y);
};

class RGWHandler_REST_STS : public RGWHandler_REST {
  const rgw::auth::StrategyRegistry& auth_registry;
  RGWOp *op_post() override;
public:

  static bool action_exists(const req_state* s);

  RGWHandler_REST_STS(const rgw::auth::StrategyRegistry& auth_registry)
    : RGWHandler_REST(),
      auth_registry(auth_registry) {}
  ~RGWHandler_REST_STS() override = default;

  int init(rgw::sal::Driver* driver,
           req_state *s,
           rgw::io::BasicClient *cio) override;
  int authorize(const DoutPrefixProvider* dpp, optional_yield y) override;
  int postauth_init(optional_yield y) override { return 0; }
};

class RGWRESTMgr_STS : public RGWRESTMgr {
public:
  RGWRESTMgr_STS() = default;
  ~RGWRESTMgr_STS() override = default;

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

