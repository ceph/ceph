// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <string_view>
#include <utility>
#include <boost/optional.hpp>

#include "common/async/call_once.h"
#include "common/web_cache.h"
#include "include/expected.hpp"
#include "rgw_auth.h"
#include "rgw_rest_s3.h"
#include "rgw_common.h"
#include "rgw_keystone.h"

namespace rgw {
namespace auth {
namespace keystone {

/* Dedicated namespace for Keystone-related auth engines. We need it because
 * Keystone offers three different authentication mechanisms (token, EC2 and
 * regular user/pass). RadosGW actually does support the first two. */

class TokenEngine : public rgw::auth::Engine {
  CephContext* const cct;

  using acl_strategy_t = rgw::auth::RemoteApplier::acl_strategy_t;
  using auth_info_t = rgw::auth::RemoteApplier::AuthInfo;
  using result_t = rgw::auth::Engine::result_t;
  using token_envelope_t = rgw::keystone::TokenEnvelope;

  const rgw::auth::TokenExtractor* const auth_token_extractor;
  const rgw::auth::TokenExtractor* const service_token_extractor;
  const rgw::auth::RemoteApplier::Factory* const apl_factory;
  rgw::keystone::Config& config;
  rgw::keystone::TokenCache& token_cache;

  /* Helper methods. */
  bool is_applicable(const std::string& token) const noexcept;

  boost::optional<token_envelope_t>
  get_from_keystone(const DoutPrefixProvider* dpp,
                    const std::string& token,
                    bool allow_expired,
                    optional_yield y) const;

  acl_strategy_t get_acl_strategy(const token_envelope_t& token) const;
  auth_info_t get_creds_info(const token_envelope_t& token) const noexcept;
  result_t authenticate(const DoutPrefixProvider* dpp,
                        const std::string& token,
                        const std::string& service_token,
                        const req_state* s,
                        optional_yield y) const;

public:
  TokenEngine(CephContext* const cct,
              const rgw::auth::TokenExtractor* const auth_token_extractor,
              const rgw::auth::TokenExtractor* const service_token_extractor,
              const rgw::auth::RemoteApplier::Factory* const apl_factory,
              rgw::keystone::Config& config,
              rgw::keystone::TokenCache& token_cache)
    : cct(cct),
      auth_token_extractor(auth_token_extractor),
      service_token_extractor(service_token_extractor),
      apl_factory(apl_factory),
      config(config),
      token_cache(token_cache) {
  }

  const char* get_name() const noexcept override {
    return "rgw::auth::keystone::TokenEngine";
  }

  result_t authenticate(const DoutPrefixProvider* dpp, const req_state* const s,
			optional_yield y) const override {
    return authenticate(dpp, auth_token_extractor->get_token(s),
                        service_token_extractor->get_token(s), s, y);
  }
}; /* class TokenEngine */

class SecretCache {
  using token_envelope_t = rgw::keystone::TokenEnvelope;

public:
  struct secret_entry {
    token_envelope_t token;
    boost::optional<std::string> secret;
    utime_t expires;
  };
  using result_t = tl::expected<secret_entry, int>;
  using value_t = ceph::async::once_result<result_t>;
  using value_ptr = std::shared_ptr<value_t>;

private:
  const boost::intrusive_ptr<CephContext> cct;

  webcache::WebCache<std::string, value_t> cache;

  const utime_t s3_token_expiry_length;

  SecretCache()
    : cct(g_ceph_context),
      cache(cct.get(), "keystone-secret-cache",
            cct->_conf->rgw_keystone_token_cache_size),
      s3_token_expiry_length(cct->_conf->rgw_keystone_token_cache_ttl, 0) {
  }

  ~SecretCache() {}

public:
  SecretCache(const SecretCache&) = delete;
  void operator=(const SecretCache&) = delete;

  static SecretCache& get_instance() {
    /* In C++11 this is thread safe. */
    static SecretCache instance;
    return instance;
  }

  value_ptr lookup_or_insert(const std::string& access_key_id) {
    return cache.lookup_or(access_key_id, std::make_shared<value_t>());
  }
  void remove(const std::string& access_key_id, const value_ptr& value) {
    cache.remove_if(access_key_id, value);
  }
  void add(const std::string& access_key_id, value_ptr value) {
    cache.add(access_key_id, std::move(value));
  }
  utime_t expiry() const {
    return ceph_clock_now() + s3_token_expiry_length;
  }
  bool enabled() const {
    return cct->_conf->rgw_keystone_token_cache_size > 0;
  }
}; /* class SecretCache */

class EC2Engine : public rgw::auth::s3::AWSEngine {
  using acl_strategy_t = rgw::auth::RemoteApplier::acl_strategy_t;
  using auth_info_t = rgw::auth::RemoteApplier::AuthInfo;
  using result_t = rgw::auth::Engine::result_t;
  using token_envelope_t = rgw::keystone::TokenEnvelope;

  const rgw::auth::RemoteApplier::Factory* const apl_factory;
  rgw::keystone::Config& config;
  rgw::keystone::TokenCache& token_cache;
  rgw::auth::keystone::SecretCache& secret_cache;

  /* Helper methods. */
  acl_strategy_t get_acl_strategy(const token_envelope_t& token) const;
  auth_info_t get_creds_info(const token_envelope_t& token,
                             const std::vector<std::string>& admin_roles,
                             const std::string& access_key_id
                            ) const noexcept;
  std::pair<boost::optional<token_envelope_t>, int>
  get_from_keystone(const DoutPrefixProvider* dpp,
                    const std::string_view& access_key_id,
                    const std::string& string_to_sign,
                    const std::string_view& signature,
                    optional_yield y) const;

  struct access_token_result {
    boost::optional<token_envelope_t> token;
    boost::optional<std::string> secret_key;
    int failure_reason = 0;
  };
  access_token_result
  get_access_token(const DoutPrefixProvider* dpp,
                   const std::string_view& access_key_id,
                   const std::string& string_to_sign,
                   const std::string_view& signature,
		   const signature_factory_t& signature_factory,
                   bool ignore_signature,
                   optional_yield y) const;
  result_t authenticate(const DoutPrefixProvider* dpp,
                        const std::string_view& access_key_id,
                        const std::string_view& signature,
                        const std::string_view& session_token,
                        const string_to_sign_t& string_to_sign,
                        const signature_factory_t& signature_factory,
                        const completer_factory_t& completer_factory,
                        const req_state* s,
			optional_yield y) const override;
  auto get_secret_from_keystone(const DoutPrefixProvider* dpp,
                                const std::string& user_id,
                                const std::string_view& access_key_id,
                                optional_yield y) const
      -> std::pair<boost::optional<std::string>, int>;
public:
  EC2Engine(CephContext* const cct,
            const rgw::auth::s3::AWSEngine::VersionAbstractor* const ver_abstractor,
            const rgw::auth::RemoteApplier::Factory* const apl_factory,
            rgw::keystone::Config& config,
            /* The token cache is used ONLY for the retrieving admin token.
             * Due to the architecture of AWS Auth S3 credentials cannot be
             * cached at all. */
            rgw::keystone::TokenCache& token_cache,
	    rgw::auth::keystone::SecretCache& secret_cache)
    : AWSEngine(cct, *ver_abstractor),
      apl_factory(apl_factory),
      config(config),
      token_cache(token_cache),
      secret_cache(secret_cache) {
  }

  using AWSEngine::authenticate;

  const char* get_name() const noexcept override {
    return "rgw::auth::keystone::EC2Engine";
  }

}; /* class EC2Engine */

}; /* namespace keystone */
}; /* namespace auth */
}; /* namespace rgw */
