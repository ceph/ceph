// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <chrono>
#include <string_view>
#include <tuple>
#include <utility>
#include <boost/optional.hpp>

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

  struct secret_entry {
    token_envelope_t token;
    std::string secret;
    utime_t expires;
    std::list<std::string>::iterator lru_iter;
  };

  const boost::intrusive_ptr<CephContext> cct;

  std::map<std::string, secret_entry> secrets;
  std::list<std::string> secrets_lru;

  std::mutex lock;

  const size_t max;

  const utime_t s3_token_expiry_length;

  SecretCache()
    : cct(g_ceph_context),
      lock(),
      max(cct->_conf->rgw_keystone_token_cache_size),
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

  bool find(const std::string& token_id, token_envelope_t& token, std::string& secret);
  boost::optional<boost::tuple<token_envelope_t, std::string>> find(const std::string& token_id) {
    token_envelope_t token_envlp;
    std::string secret;
    if (find(token_id, token_envlp, secret)) {
      return boost::make_tuple(token_envlp, secret);
    }
    return boost::none;
  }
  void add(const std::string& token_id, const token_envelope_t& token, const std::string& secret);
}; /* class SecretCache */

// Signing material is process-local, owned, and never mixed with SecretCache.
class SigningKeyCache {
public:
  using mono_time = std::chrono::steady_clock::time_point;
  struct Key {
    std::string access, date, region, service, endpoint;
    bool operator<(const Key& rhs) const {
      return std::tie(access, date, region, service, endpoint) <
             std::tie(rhs.access, rhs.date, rhs.region, rhs.service, rhs.endpoint);
    }
  };
  struct Value {
    rgw::keystone::TokenEnvelope identity;
    sha256_digest_t signing_key;
    std::string credential_id;
    time_t wall_expires = 0;
    mono_time mono_expires;
  };

  explicit SigningKeyCache(size_t capacity) : capacity(capacity) {}
  boost::optional<Value> find(const Key&, time_t now, mono_time mono_now);
  bool insert(const Key&, Value, time_t now, mono_time mono_now);
  size_t size() const;

private:
  struct Entry {
    Value value;
    std::list<Key>::iterator lru;
  };
  const size_t capacity;
  mutable std::mutex mutex;
  std::map<Key, Entry> entries;
  std::list<Key> lru;
};

// Production uses real clocks; tests can model independent wall/elapsed time.
struct S3ProofClock {
  std::function<time_t()> wall;
  std::function<SigningKeyCache::mono_time()> mono;
};

struct S3ProofResult {
  rgw::keystone::TokenEnvelope identity;
  std::string credential_id;
  boost::optional<sha256_digest_t> signing_key;
  time_t cache_until = 0; // zero on validation-only replies
};

// Strict protocol boundaries, also exercised independently by unit tests.
int decode_s3_proof_response(const std::string& body, bool key_response,
                             const SigningKeyCache::Key& scope,
                             S3ProofResult& result);
int validate_s3_v4_proof(std::string_view proof, bool query,
                         std::string_view expires, time_t now,
                         SigningKeyCache::Key& scope);

class EC2Engine : public rgw::auth::s3::AWSEngine {
  using acl_strategy_t = rgw::auth::RemoteApplier::acl_strategy_t;
  using auth_info_t = rgw::auth::RemoteApplier::AuthInfo;
  using result_t = rgw::auth::Engine::result_t;
  using token_envelope_t = rgw::keystone::TokenEnvelope;

  const rgw::auth::RemoteApplier::Factory* const apl_factory;
  rgw::keystone::Config& config;
  rgw::keystone::TokenCache& token_cache;
  rgw::auth::keystone::SecretCache& secret_cache;
  const bool signing_key_mode;
  const bool allow_legacy_signatures;
  const unsigned local_cache_ttl;
  const std::string proof_endpoint;
  const S3ProofClock proof_clock;
  time_t wall_now() const {
    return proof_clock.wall ? proof_clock.wall() : ceph_clock_now().sec();
  }
  SigningKeyCache::mono_time mono_now() const {
    return proof_clock.mono ? proof_clock.mono() : std::chrono::steady_clock::now();
  }
  mutable SigningKeyCache signing_keys;
  mutable std::atomic<bool> capability_checked{false};
  mutable std::atomic<uint64_t> legacy_validations{0};

  struct proof_http_result {
    std::string body;
    long status = 0;
    int error = 0;
  };
  proof_http_result proof_request(const DoutPrefixProvider*,
                                  const std::string& method,
                                  const std::string& endpoint,
                                  const std::string& body,
                                  optional_yield) const;
  int check_capability(const DoutPrefixProvider*, optional_yield) const;
  result_t authenticate(const DoutPrefixProvider*,
                         const VersionAbstractor::auth_data_t&,
                         const req_state*, optional_yield) const override;

  /* Helper methods. */
  acl_strategy_t get_acl_strategy(const token_envelope_t& token) const;
  auth_info_t get_creds_info(const token_envelope_t& token,
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
	    rgw::auth::keystone::SecretCache& secret_cache,
            S3ProofClock proof_clock = {})
    : AWSEngine(cct, *ver_abstractor),
      apl_factory(apl_factory),
      config(config),
      token_cache(token_cache),
      secret_cache(secret_cache),
      signing_key_mode(cct->_conf->rgw_keystone_s3_auth_mode == "signing_key"),
      allow_legacy_signatures(cct->_conf->rgw_keystone_s3_allow_legacy_signatures),
      local_cache_ttl(cct->_conf->rgw_keystone_signing_key_cache_ttl),
      proof_endpoint(config.get_endpoint_url()),
      proof_clock(std::move(proof_clock)),
      signing_keys(cct->_conf->rgw_keystone_signing_key_cache_size) {
  }

  using AWSEngine::authenticate;

  const char* get_name() const noexcept override {
    return "rgw::auth::keystone::EC2Engine";
  }

}; /* class EC2Engine */

}; /* namespace keystone */
}; /* namespace auth */
}; /* namespace rgw */
