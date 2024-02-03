// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_common.h"
#include "rgw_user.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_auth.h"
#include "rgw_auth_keystone.h"
#include "rgw_auth_filters.h"
#include "rgw_sal.h"
#include "rgw_b64.h"

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
  friend class TempURLSignature;
  using result_t = rgw::auth::Engine::result_t;

  CephContext* const cct;
  rgw::sal::Driver* driver;
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
                rgw::sal::Driver* _driver ,
                const TempURLApplier::Factory* const apl_factory)
    : cct(cct),
      driver(_driver),
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
  rgw::sal::Driver* driver;
  const rgw::auth::TokenExtractor* const extractor;
  const rgw::auth::LocalApplier::Factory* const apl_factory;

  bool is_applicable(const std::string& token) const noexcept;
  using rgw::auth::Engine::authenticate;
  result_t authenticate(const DoutPrefixProvider* dpp,
                        const std::string& token,
                        const req_state* s) const;

public:
  SignedTokenEngine(CephContext* const cct,
                    rgw::sal::Driver* _driver,
                    const rgw::auth::TokenExtractor* const extractor,
                    const rgw::auth::LocalApplier::Factory* const apl_factory)
    : cct(cct),
      driver(_driver),
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
  rgw::sal::Driver* driver;
  const rgw::auth::TokenExtractor* const extractor;
  const rgw::auth::LocalApplier::Factory* const apl_factory;

  bool is_applicable(const std::string& token) const noexcept;
  result_t authenticate(const DoutPrefixProvider* dpp,
                        const std::string& token,
                        const req_state* s, optional_yield y) const;

public:
  ExternalTokenEngine(CephContext* const cct,
                      rgw::sal::Driver* _driver,
                      const rgw::auth::TokenExtractor* const extractor,
                      const rgw::auth::LocalApplier::Factory* const apl_factory)
    : cct(cct),
      driver(_driver),
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
  rgw::sal::Driver* driver;
  const ImplicitTenants& implicit_tenant_context;

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
      rgw::auth::add_3rdparty(driver, rgw_user(s->account_name),
        rgw::auth::add_sysreq(cct, driver, s,
          rgw::auth::RemoteApplier(cct, driver, std::move(extra_acl_strategy), info,
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
      rgw::auth::add_3rdparty(driver, rgw_user(s->account_name),
        rgw::auth::add_sysreq(cct, driver, s,
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
                  const ImplicitTenants& implicit_tenant_context,
                  rgw::sal::Driver* _driver)
    : driver(_driver),
      implicit_tenant_context(implicit_tenant_context),
      tempurl_engine(cct,
                     driver,
                     static_cast<rgw::auth::swift::TempURLApplier::Factory*>(this)),
      signed_engine(cct,
                    driver,
                    static_cast<rgw::auth::TokenExtractor*>(&auth_token_extractor),
                    static_cast<rgw::auth::LocalApplier::Factory*>(this)),
      external_engine(cct,
                      driver,
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

// shared logic for swift tempurl and formpost signatures
template <class HASHFLAVOR>
inline constexpr uint32_t signature_hash_size = -1;
template <>
inline constexpr uint32_t signature_hash_size<ceph::crypto::HMACSHA1> = CEPH_CRYPTO_HMACSHA1_DIGESTSIZE;
template<>
inline constexpr uint32_t signature_hash_size<ceph::crypto::HMACSHA256> = CEPH_CRYPTO_HMACSHA256_DIGESTSIZE;
template<>
inline constexpr uint32_t signature_hash_size<ceph::crypto::HMACSHA512> = CEPH_CRYPTO_HMACSHA512_DIGESTSIZE;

const char sha1_name[] = "sha1";
const char sha256_name[] = "sha256";
const char sha512_name[] = "sha512";

template <class HASHFLAVOR>
const char * signature_hash_name;
template<>
inline constexpr const char * signature_hash_name<ceph::crypto::HMACSHA1> = sha1_name;;
template<>
inline constexpr const char * signature_hash_name<ceph::crypto::HMACSHA256> = sha256_name;
template<>
inline constexpr const char * signature_hash_name<ceph::crypto::HMACSHA512> = sha512_name;

template <class HASHFLAVOR>
inline const uint32_t signature_hash_name_size = -1;
template<>
inline constexpr uint32_t signature_hash_name_size<ceph::crypto::HMACSHA1> = sizeof sha1_name;;
template<>
inline constexpr uint32_t signature_hash_name_size<ceph::crypto::HMACSHA256> = sizeof sha256_name;
template<>
inline constexpr uint32_t signature_hash_name_size<ceph::crypto::HMACSHA512> = sizeof sha512_name;

template <class HASHFLAVOR>
class SignatureHelperT {
protected:
  static constexpr uint32_t hash_size = signature_hash_size<HASHFLAVOR>;
  static constexpr uint32_t output_size = hash_size * 2 + 1;
  const char * signature_name = signature_hash_name<HASHFLAVOR>;
  uint32_t signature_name_size = signature_hash_name_size<HASHFLAVOR>;
  char dest_str[output_size];
  uint32_t dest_size = 0;
  unsigned char dest[hash_size];

public:
  ~SignatureHelperT() { };

  void Update(const unsigned char *input, size_t length);

  const char* get_signature() const {
    return dest_str;
  }

  bool is_equal_to(const std::string& rhs) const {
    /* never allow out-of-range exception */
    if (!dest_size || rhs.size() < dest_size) {
      return false;
    }
    return rhs.compare(0 /* pos */,  dest_size + 1, dest_str) == 0;
  }
};

enum class SignatureFlavor {
  BARE_HEX,
  NAMED_BASE64
};

template <typename HASHFLAVOR, SignatureFlavor SIGNATUREFLAVOR>
class FormatSignature {
};

// hexadecimal
template <typename HASHFLAVOR>
class FormatSignature<HASHFLAVOR, SignatureFlavor::BARE_HEX> : public SignatureHelperT<HASHFLAVOR> {
  using UCHARPTR = const unsigned char*;
  using base_t = SignatureHelperT<HASHFLAVOR>;
public:
  const char *result() {
    buf_to_hex((UCHARPTR) base_t::dest,
      signature_hash_size<HASHFLAVOR>,
      base_t::dest_str);
    base_t::dest_size = strlen(base_t::dest_str);
    return base_t::dest_str;
  };
};

// prefix:base64
template <typename HASHFLAVOR>
class FormatSignature<HASHFLAVOR, SignatureFlavor::NAMED_BASE64> : public SignatureHelperT<HASHFLAVOR> {
  using UCHARPTR = const unsigned char*;
  using base_t = SignatureHelperT<HASHFLAVOR>;
public:
  char * const result() {
    const char *prefix = base_t::signature_name;
    const int prefix_size = base_t::signature_name_size;
    std::string_view dest_view((char*)base_t::dest, sizeof base_t::dest);
    auto b { rgw::to_base64(dest_view) };
    for (auto &v: b ) {	// translate to "url safe" (rfc 4648 section 5)
      switch(v) {
      case '+': v = '-'; break;
      case '/': v = '_'; break;
      }
    }
    base_t::dest_size = prefix_size + b.length();
    if (base_t::dest_size < base_t::output_size) {
      ::memcpy(base_t::dest_str, prefix, prefix_size - 1);
      base_t::dest_str[prefix_size-1] = ':';
      ::strcpy(base_t::dest_str + prefix_size, b.c_str());
    } else {
      base_t::dest_size = 0;
    }
    return base_t::dest_str;
  };
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

  int init(rgw::sal::Driver* driver, req_state *state, rgw::io::BasicClient *cio) override;
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

  RGWHandler_REST* get_handler(rgw::sal::Driver* driver,
			       req_state*,
                               const rgw::auth::StrategyRegistry&,
                               const std::string&) override {
    return new RGWHandler_SWIFT_Auth;
  }
};
