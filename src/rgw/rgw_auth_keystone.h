// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#ifndef CEPH_RGW_AUTH_KEYSTONE_H
#define CEPH_RGW_AUTH_KEYSTONE_H

#include <utility>
#include <boost/optional.hpp>
#include <boost/utility/string_view.hpp>

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

  const rgw::auth::TokenExtractor* const extractor;
  const rgw::auth::RemoteApplier::Factory* const apl_factory;
  rgw::keystone::Config& config;
  rgw::keystone::TokenCache& token_cache;

  /* Helper methods. */
  bool is_applicable(const std::string& token) const noexcept;
  token_envelope_t decode_pki_token(const DoutPrefixProvider* dpp, const std::string& token) const;

  boost::optional<token_envelope_t>
  get_from_keystone(const DoutPrefixProvider* dpp, const std::string& token) const;

  acl_strategy_t get_acl_strategy(const token_envelope_t& token) const;
  auth_info_t get_creds_info(const token_envelope_t& token,
                             const std::vector<std::string>& admin_roles
                            ) const noexcept;
  result_t authenticate(const DoutPrefixProvider* dpp,
                        const std::string& token,
                        const req_state* s) const;

public:
  TokenEngine(CephContext* const cct,
              const rgw::auth::TokenExtractor* const extractor,
              const rgw::auth::RemoteApplier::Factory* const apl_factory,
              rgw::keystone::Config& config,
              rgw::keystone::TokenCache& token_cache)
    : cct(cct),
      extractor(extractor),
      apl_factory(apl_factory),
      config(config),
      token_cache(token_cache) {
  }

  const char* get_name() const noexcept override {
    return "rgw::auth::keystone::TokenEngine";
  }

  result_t authenticate(const DoutPrefixProvider* dpp, const req_state* const s) const override {
    return authenticate(dpp, extractor->get_token(s), s);
  }
}; /* class TokenEngine */

class SecretCacheSize: public md_config_obs_t {
private:
  uint64_t saved;
  bool changed;
  ConfigProxy& ic;
  void recompute_value(const ConfigProxy& );
public:
  SecretCacheSize(ConfigProxy& _c)
    : ic(_c) {
    ic.add_observer(this);
    recompute_value(ic);
  }
  ~SecretCacheSize() { ic.remove_observer(this); }
  uint64_t get_value() {
    return saved;
  }
  bool was_changed() { return changed; }
  void reset_changed() { changed = false; }
private:
  const char** get_tracked_conf_keys() const override;
  void handle_conf_change(const ConfigProxy& conf,
    const std::set <std::string> &changed) override;
};

class SecretCache {
  using token_envelope_t = rgw::keystone::TokenEnvelope;

  struct secret_entry {
    token_envelope_t token;
    std::string secret;
    utime_t expires;
    list<std::string>::iterator lru_iter;
  };

  const boost::intrusive_ptr<CephContext> cct;

  std::map<std::string, secret_entry> secrets;
  std::list<std::string> secrets_lru;

  std::mutex lock;

  SecretCacheSize max;

  const utime_t s3_token_expiry_length;

  SecretCache()
    : cct(g_ceph_context),
      lock(),
      max(g_conf()),
      s3_token_expiry_length(300, 0) {
  }

  ~SecretCache() {}

  void _trim();

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
  void trim();
  inline bool disabled() { return max.get_value() == 0; }
  inline bool empty() { if (max.was_changed()) trim(); return secrets_lru.empty(); }
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
                             const std::vector<std::string>& admin_roles
                            ) const noexcept;
  std::pair<boost::optional<token_envelope_t>, int>
  get_from_keystone(const DoutPrefixProvider* dpp,
                    const boost::string_view& access_key_id,
                    const std::string& string_to_sign,
                    const boost::string_view& signature) const;
#ifdef CEPH_KEYSTONE_SECRET_CACHE
  std::pair<boost::optional<token_envelope_t>, int>
  get_access_token(const DoutPrefixProvider* dpp,
                   const boost::string_view& access_key_id,
                   const std::string& string_to_sign,
                   const boost::string_view& signature,
		   const signature_factory_t& signature_factory) const;
  std::pair<boost::optional<std::string>, int> get_secret_from_keystone(const DoutPrefixProvider* dpp,
                                                                        const std::string& user_id,
                                                                        const boost::string_view& access_key_id) const;
#endif
  result_t authenticate(const DoutPrefixProvider* dpp,
                        const boost::string_view& access_key_id,
                        const boost::string_view& signature,
                        const boost::string_view& session_token,
                        const string_to_sign_t& string_to_sign,
                        const signature_factory_t& signature_factory,
                        const completer_factory_t& completer_factory,
                        const req_state* s) const override;
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

#endif /* CEPH_RGW_AUTH_KEYSTONE_H */
