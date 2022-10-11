// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_SWIFT_AUTH_H
#define CEPH_RGW_SWIFT_AUTH_H

#include "rgw_common.h"
#include "rgw_user.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_auth.h"
#include "rgw_auth_keystone.h"
#include "rgw_auth_filters.h"
#include "rgw_sal.h"

#define RGW_SWIFT_TOKEN_EXPIRATION (15 * 60)

namespace rgw {
namespace auth {
namespace swift {

/* TempURL: applier. */
class TempURLApplier : public rgw::auth::LocalApplier {
public:
  TempURLApplier(CephContext* const cct,
                 const RGWUserInfo& user_info)
    : LocalApplier(cct, user_info, LocalApplier::NO_SUBUSER, std::nullopt, LocalApplier::NO_ACCESS_KEY) {
  };

  void modify_request_state(const DoutPrefixProvider* dpp, req_state * s) const override; /* in/out */
  void write_ops_log_entry(rgw_log_entry& entry) const override;

  struct Factory {
    virtual ~Factory() {}
    virtual aplptr_t create_apl_turl(CephContext* cct,
                                     const req_state* s,
                                     const RGWUserInfo& user_info) const = 0;
  };
};

/* TempURL: engine */
class TempURLEngine : public rgw::auth::Engine {
  using result_t = rgw::auth::Engine::result_t;

  CephContext* const cct;
  rgw::sal::Store* store;
  const TempURLApplier::Factory* const apl_factory;

  /* Helper methods. */
  void get_owner_info(const DoutPrefixProvider* dpp, 
                      const req_state* s,
                      RGWUserInfo& owner_info,
		      optional_yield y) const;
  std::string convert_from_iso8601(std::string expires) const;
  bool is_applicable(const req_state* s) const noexcept;
  bool is_expired(const std::string& expires) const;
  bool is_disallowed_header_present(const req_info& info) const;

  class SignatureHelper;
  class PrefixableSignatureHelper;

public:
  TempURLEngine(CephContext* const cct,
                rgw::sal::Store* _store ,
                const TempURLApplier::Factory* const apl_factory)
    : cct(cct),
      store(_store),
      apl_factory(apl_factory) {
  }

  /* Interface implementations. */
  const char* get_name() const noexcept override {
    return "rgw::auth::swift::TempURLEngine";
  }

  result_t authenticate(const DoutPrefixProvider* dpp, const req_state* const s, optional_yield y) const override;
};


/* AUTH_rgwtk */
class SignedTokenEngine : public rgw::auth::Engine {
  using result_t = rgw::auth::Engine::result_t;

  CephContext* const cct;
  rgw::sal::Store* store;
  const rgw::auth::TokenExtractor* const extractor;
  const rgw::auth::LocalApplier::Factory* const apl_factory;

  bool is_applicable(const std::string& token) const noexcept;
  using rgw::auth::Engine::authenticate;
  result_t authenticate(const DoutPrefixProvider* dpp,
                        const std::string& token,
                        const req_state* s) const;

public:
  SignedTokenEngine(CephContext* const cct,
                    rgw::sal::Store* _store,
                    const rgw::auth::TokenExtractor* const extractor,
                    const rgw::auth::LocalApplier::Factory* const apl_factory)
    : cct(cct),
      store(_store),
      extractor(extractor),
      apl_factory(apl_factory) {
  }

  const char* get_name() const noexcept override {
    return "rgw::auth::swift::SignedTokenEngine";
  }

  result_t authenticate(const DoutPrefixProvider* dpp, const req_state* const s,
			optional_yield y) const override {
    return authenticate(dpp, extractor->get_token(s), s);
  }
};


/* External token */
class ExternalTokenEngine : public rgw::auth::Engine {
  using result_t = rgw::auth::Engine::result_t;

  CephContext* const cct;
  rgw::sal::Store* store;
  const rgw::auth::TokenExtractor* const extractor;
  const rgw::auth::LocalApplier::Factory* const apl_factory;

  bool is_applicable(const std::string& token) const noexcept;
  result_t authenticate(const DoutPrefixProvider* dpp,
                        const std::string& token,
                        const req_state* s, optional_yield y) const;

public:
  ExternalTokenEngine(CephContext* const cct,
                      rgw::sal::Store* _store,
                      const rgw::auth::TokenExtractor* const extractor,
                      const rgw::auth::LocalApplier::Factory* const apl_factory)
    : cct(cct),
      store(_store),
      extractor(extractor),
      apl_factory(apl_factory) {
  }

  const char* get_name() const noexcept override {
    return "rgw::auth::swift::ExternalTokenEngine";
  }

  result_t authenticate(const DoutPrefixProvider* dpp, const req_state* const s,
			optional_yield y) const override {
    return authenticate(dpp, extractor->get_token(s), s, y);
  }
};

/* SwiftAnonymous: applier. */
class SwiftAnonymousApplier : public rgw::auth::LocalApplier {
  public:
    SwiftAnonymousApplier(CephContext* const cct,
                          const RGWUserInfo& user_info)
      : LocalApplier(cct, user_info, LocalApplier::NO_SUBUSER, std::nullopt, LocalApplier::NO_ACCESS_KEY) {
    }
    bool is_admin_of(const rgw_user& uid) const {return false;}
    bool is_owner_of(const rgw_user& uid) const {return uid.id.compare(RGW_USER_ANON_ID) == 0;}
};

class SwiftAnonymousEngine : public rgw::auth::AnonymousEngine {
  const rgw::auth::TokenExtractor* const extractor;

  bool is_applicable(const req_state* s) const noexcept override {
    return extractor->get_token(s).empty();
  }

public:
  SwiftAnonymousEngine(CephContext* const cct,
                       const SwiftAnonymousApplier::Factory* const apl_factory,
                       const rgw::auth::TokenExtractor* const extractor)
    : AnonymousEngine(cct, apl_factory),
      extractor(extractor) {
  }

  const char* get_name() const noexcept override {
    return "rgw::auth::swift::SwiftAnonymousEngine";
  }
};


class DefaultStrategy : public rgw::auth::Strategy,
                        public rgw::auth::RemoteApplier::Factory,
                        public rgw::auth::LocalApplier::Factory,
                        public rgw::auth::swift::TempURLApplier::Factory {
  rgw::sal::Store* store;
  ImplicitTenants& implicit_tenant_context;

  /* The engines. */
  const rgw::auth::swift::TempURLEngine tempurl_engine;
  const rgw::auth::swift::SignedTokenEngine signed_engine;
  boost::optional <const rgw::auth::keystone::TokenEngine> keystone_engine;
  const rgw::auth::swift::ExternalTokenEngine external_engine;
  const rgw::auth::swift::SwiftAnonymousEngine anon_engine;

  using keystone_config_t = rgw::keystone::CephCtxConfig;
  using keystone_cache_t = rgw::keystone::TokenCache;
  using aplptr_t = rgw::auth::IdentityApplier::aplptr_t;
  using acl_strategy_t = rgw::auth::RemoteApplier::acl_strategy_t;

  /* The method implements TokenExtractor for X-Auth-Token present in req_state. */
  struct AuthTokenExtractor : rgw::auth::TokenExtractor {
    std::string get_token(const req_state* const s) const override {
      /* Returning a reference here would end in GCC complaining about a reference
       * to temporary. */
      return s->info.env->get("HTTP_X_AUTH_TOKEN", "");
    }
  } auth_token_extractor;

  /* The method implements TokenExtractor for X-Service-Token present in req_state. */
  struct ServiceTokenExtractor : rgw::auth::TokenExtractor {
    std::string get_token(const req_state* const s) const override {
      return s->info.env->get("HTTP_X_SERVICE_TOKEN", "");
    }
  } service_token_extractor;

  aplptr_t create_apl_remote(CephContext* const cct,
                             const req_state* const s,
                             acl_strategy_t&& extra_acl_strategy,
                             const rgw::auth::RemoteApplier::AuthInfo &info) const override {
    auto apl = \
      rgw::auth::add_3rdparty(store, rgw_user(s->account_name),
        rgw::auth::add_sysreq(cct, store, s,
          rgw::auth::RemoteApplier(cct, store, std::move(extra_acl_strategy), info,
                                   implicit_tenant_context,
                                   rgw::auth::ImplicitTenants::IMPLICIT_TENANTS_SWIFT)));
    /* TODO(rzarzynski): replace with static_ptr. */
    return aplptr_t(new decltype(apl)(std::move(apl)));
  }

  aplptr_t create_apl_local(CephContext* const cct,
                            const req_state* const s,
                            const RGWUserInfo& user_info,
                            const std::string& subuser,
                            const std::optional<uint32_t>& perm_mask,
                            const std::string& access_key_id) const override {
    auto apl = \
      rgw::auth::add_3rdparty(store, rgw_user(s->account_name),
        rgw::auth::add_sysreq(cct, store, s,
          rgw::auth::LocalApplier(cct, user_info, subuser, perm_mask, access_key_id)));
    /* TODO(rzarzynski): replace with static_ptr. */
    return aplptr_t(new decltype(apl)(std::move(apl)));
  }

  aplptr_t create_apl_turl(CephContext* const cct,
                           const req_state* const s,
                           const RGWUserInfo& user_info) const override {
    /* TempURL doesn't need any user account override. It's a Swift-specific
     * mechanism that requires  account name internally, so there is no
     * business with delegating the responsibility outside. */
    return aplptr_t(new rgw::auth::swift::TempURLApplier(cct, user_info));
  }

public:
  DefaultStrategy(CephContext* const cct,
                  ImplicitTenants& implicit_tenant_context,
                  rgw::sal::Store* _store)
    : store(_store),
      implicit_tenant_context(implicit_tenant_context),
      tempurl_engine(cct,
                     store,
                     static_cast<rgw::auth::swift::TempURLApplier::Factory*>(this)),
      signed_engine(cct,
                    store,
                    static_cast<rgw::auth::TokenExtractor*>(&auth_token_extractor),
                    static_cast<rgw::auth::LocalApplier::Factory*>(this)),
      external_engine(cct,
                      store,
                      static_cast<rgw::auth::TokenExtractor*>(&auth_token_extractor),
                      static_cast<rgw::auth::LocalApplier::Factory*>(this)),
      anon_engine(cct,
                  static_cast<SwiftAnonymousApplier::Factory*>(this),
                  static_cast<rgw::auth::TokenExtractor*>(&auth_token_extractor)) {
    /* When the constructor's body is being executed, all member engines
     * should be initialized. Thus, we can safely add them. */
    using Control = rgw::auth::Strategy::Control;

    add_engine(Control::SUFFICIENT, tempurl_engine);
    add_engine(Control::SUFFICIENT, signed_engine);

    /* The auth strategy is responsible for deciding whether a parcular
     * engine is disabled or not. */
    if (! cct->_conf->rgw_keystone_url.empty()) {
      keystone_engine.emplace(cct,
                              static_cast<rgw::auth::TokenExtractor*>(&auth_token_extractor),
                              static_cast<rgw::auth::TokenExtractor*>(&service_token_extractor),
                              static_cast<rgw::auth::RemoteApplier::Factory*>(this),
                              keystone_config_t::get_instance(),
                              keystone_cache_t::get_instance<keystone_config_t>());

      add_engine(Control::SUFFICIENT, *keystone_engine);
    }
    if (! cct->_conf->rgw_swift_auth_url.empty()) {
      add_engine(Control::SUFFICIENT, external_engine);
    }

    add_engine(Control::SUFFICIENT, anon_engine);
  }

  const char* get_name() const noexcept override {
    return "rgw::auth::swift::DefaultStrategy";
  }
};

} /* namespace swift */
} /* namespace auth */
} /* namespace rgw */


class RGW_SWIFT_Auth_Get : public RGWOp {
public:
  RGW_SWIFT_Auth_Get() {}
  ~RGW_SWIFT_Auth_Get() override {}

  int verify_permission(optional_yield) override { return 0; }
  void execute(optional_yield y) override;
  const char* name() const override { return "swift_auth_get"; }
  dmc::client_id dmclock_client() override { return dmc::client_id::auth; }
};

class RGWHandler_SWIFT_Auth : public RGWHandler_REST {
public:
  RGWHandler_SWIFT_Auth() {}
  ~RGWHandler_SWIFT_Auth() override {}
  RGWOp *op_get() override;

  int init(rgw::sal::Store* store, req_state *state, rgw::io::BasicClient *cio) override;
  int authorize(const DoutPrefixProvider *dpp, optional_yield y) override;
  int postauth_init(optional_yield) override { return 0; }
  int read_permissions(RGWOp *op, optional_yield) override { return 0; }

  virtual RGWAccessControlPolicy *alloc_policy() { return NULL; }
  virtual void free_policy(RGWAccessControlPolicy *policy) {}
};

class RGWRESTMgr_SWIFT_Auth : public RGWRESTMgr {
public:
  RGWRESTMgr_SWIFT_Auth() = default;
  ~RGWRESTMgr_SWIFT_Auth() override = default;

  RGWRESTMgr *get_resource_mgr(req_state* const s,
                               const std::string& uri,
                               std::string* const out_uri) override {
    return this;
  }

  RGWHandler_REST* get_handler(rgw::sal::Store* store,
			       req_state*,
                               const rgw::auth::StrategyRegistry&,
                               const std::string&) override {
    return new RGWHandler_SWIFT_Auth;
  }
};


#endif
