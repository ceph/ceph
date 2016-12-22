// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_SWIFT_AUTH_H
#define CEPH_RGW_SWIFT_AUTH_H

#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_auth.h"
#include "rgw_auth_keystone.h"
#include "rgw_auth_filters.h"

#define RGW_SWIFT_TOKEN_EXPIRATION (15 * 60)

/* TempURL: applier. */
class RGWTempURLAuthApplier : public RGWLocalAuthApplier {
public:
  RGWTempURLAuthApplier(CephContext * const cct,
                        const RGWUserInfo& user_info)
    : RGWLocalAuthApplier(cct, user_info, RGWLocalAuthApplier::NO_SUBUSER) {
  };

  virtual void modify_request_state(req_state * s) const override; /* in/out */

  struct Factory {
    virtual ~Factory() {}
    virtual aplptr_t create_apl_turl(CephContext * const cct,
                                     const RGWUserInfo& user_info) const = 0;
  };
};

/* TempURL: engine */
class RGWTempURLAuthEngine : public RGWAuthEngine {
protected:
  /* const */ RGWRados * const store;
  const req_state * const s;
  const RGWTempURLAuthApplier::Factory * const apl_factory;

  /* Helper methods. */
  void get_owner_info(RGWUserInfo& owner_info) const;
  bool is_expired(const std::string& expires) const;

  class SignatureHelper;

public:
  RGWTempURLAuthEngine(const req_state * const s,
                       /*const*/ RGWRados * const store,
                       const RGWTempURLAuthApplier::Factory * const apl_factory)
    : RGWAuthEngine(s->cct),
      store(store),
      s(s),
      apl_factory(apl_factory) {
  }

  /* Interface implementations. */
  const char* get_name() const noexcept override {
    return "RGWTempURLAuthEngine";
  }

  bool is_applicable() const noexcept override;
  RGWAuthApplier::aplptr_t authenticate() const override;
};


namespace rgw {
namespace auth {
namespace swift {

/* AUTH_rgwtk */
class SignedTokenEngine : public rgw::auth::Engine {
  using result_t = rgw::auth::Engine::result_t;

  CephContext* const cct;
  RGWRados* const store;
  const rgw::auth::TokenExtractor* const extractor;
  const rgw::auth::LocalApplier::Factory* const apl_factory;

  bool is_applicable(const std::string& token) const noexcept;
  result_t authenticate(const std::string& token) const;

public:
  SignedTokenEngine(CephContext* const cct,
                    /* const */RGWRados* const store,
                    const rgw::auth::TokenExtractor* const extractor,
                    const rgw::auth::LocalApplier::Factory* const apl_factory)
    : cct(cct),
      store(store),
      extractor(extractor),
      apl_factory(apl_factory) {
  }

  const char* get_name() const noexcept override {
    return "RGWSignedTokenAuthEngine";
  }

  result_t authenticate(const req_state* const s) const override {
    return authenticate(extractor->get_token(s));
  }
};


/* External token */
class ExternalTokenEngine : public rgw::auth::Engine {
  using result_t = rgw::auth::Engine::result_t;

  CephContext* const cct;
  RGWRados* const store;
  const rgw::auth::TokenExtractor* const extractor;
  const rgw::auth::LocalApplier::Factory* const apl_factory;

  bool is_applicable(const std::string& token) const noexcept;
  result_t authenticate(const std::string& token) const;

public:
  ExternalTokenEngine(CephContext* const cct,
                      /* const */RGWRados* const store,
                      const rgw::auth::TokenExtractor* const extractor,
                      const rgw::auth::LocalApplier::Factory* const apl_factory)
    : cct(cct),
      store(store),
      extractor(extractor),
      apl_factory(apl_factory) {
  }

  const char* get_name() const noexcept override {
    return "rgw::auth::swift::ExternalTokenEngine";
  }

  result_t authenticate(const req_state* const s) const override {
    return authenticate(extractor->get_token(s));
  }
};

class DefaultStrategy : public rgw::auth::Strategy,
                        public rgw::auth::TokenExtractor,
                        public rgw::auth::RemoteApplier::Factory,
                        public rgw::auth::LocalApplier::Factory {
  /* The engines. */
  const rgw::auth::keystone::TokenEngine keystone_engine;
  const rgw::auth::AnonymousEngine anon_engine;

  using keystone_config_t = rgw::keystone::CephCtxConfig;
  using keystone_cache_t = rgw::keystone::TokenCache;

  DefaultStrategy(CephContext* const cct)
    : keystone_engine(cct,
                      static_cast<rgw::auth::TokenExtractor*>(this),
                      static_cast<rgw::auth::RemoteApplier::Factory*>(this),
                      keystone_config_t::get_instance(),
                      keystone_cache_t::get_instance<keystone_config_t>()),
      anon_engine(cct,
                  static_cast<rgw::auth::LocalApplier::Factory*>(this)) {
    /* When the constructor's body is being executed, all member engines
     * should be initialized. Thus, we can safely add them. */
    using Control = rgw::auth::Strategy::Control;
    add_engine(Control::SUFFICIENT, keystone_engine);
    add_engine(Control::SUFFICIENT, anon_engine);
  }

  using aplptr_t = rgw::auth::Applier::aplptr_t;
  using acl_strategy_t = rgw::auth::RemoteApplier::acl_strategy_t;

  std::string get_token(const req_state* const s) const override {
    return "ala";
  }

  aplptr_t create_apl_remote(CephContext* const cct,
                             acl_strategy_t&& extra_acl_strategy,
                             const rgw::auth::RemoteApplier::AuthInfo info) const override {
    return aplptr_t(
      new rgw::auth::RemoteApplier(cct, nullptr, std::move(extra_acl_strategy), info));
  }

  aplptr_t create_apl_local(CephContext* const cct,
                            const RGWUserInfo& user_info,
                            const std::string& subuser) const override {
    return aplptr_t(new rgw::auth::LocalApplier(cct, user_info, subuser));
  }

public:
  const char* get_name() const noexcept override {
    return "rgw::auth::swift::DefaultStrategy";
  }

  static const DefaultStrategy& get_instance() {
    static const DefaultStrategy instance(g_ceph_context);
    return instance;
  }
};

} /* namespace swift */
} /* namespace auth */
} /* namespace rgw */


class RGW_SWIFT_Auth_Get : public RGWOp {
public:
  RGW_SWIFT_Auth_Get() {}
  ~RGW_SWIFT_Auth_Get() {}

  int verify_permission() { return 0; }
  void execute();
  virtual const string name() { return "swift_auth_get"; }
};

class RGWHandler_SWIFT_Auth : public RGWHandler_REST {
public:
  RGWHandler_SWIFT_Auth() {}
  ~RGWHandler_SWIFT_Auth() {}
  RGWOp *op_get();

  int init(RGWRados *store, struct req_state *state, rgw::io::BasicClient *cio);
  int authorize();
  int postauth_init() { return 0; }
  int read_permissions(RGWOp *op) { return 0; }

  virtual RGWAccessControlPolicy *alloc_policy() { return NULL; }
  virtual void free_policy(RGWAccessControlPolicy *policy) {}
};

class RGWRESTMgr_SWIFT_Auth : public RGWRESTMgr {
public:
  RGWRESTMgr_SWIFT_Auth() = default;
  virtual ~RGWRESTMgr_SWIFT_Auth() = default;

  virtual RGWRESTMgr *get_resource_mgr(struct req_state* const s,
                                       const std::string& uri,
                                       std::string* const out_uri) override {
    return this;
  }

  virtual RGWHandler_REST* get_handler(struct req_state*,
                                       const std::string&) override {
    return new RGWHandler_SWIFT_Auth;
  }
};


#endif
