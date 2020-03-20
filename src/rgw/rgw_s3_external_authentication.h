// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_EXTERNAL_AUTHENTICATION_H
#define CEPH_RGW_EXTERNAL_AUTHENTICATION_H

#include "rgw_auth.h"
#include "rgw_rest_s3.h"
#include "rgw_common.h"

namespace rgw::auth::s3 {
namespace external_authentication {

class Config {
protected:
  Config() = default;

public:
  std::string get_auth_endpoint_url() const noexcept {
    return g_ceph_context->_conf->rgw_s3_external_authentication_auth_endpoint;
  }
  std::string get_secret_endpoint_url() const noexcept {
    return g_ceph_context->_conf->rgw_s3_external_authentication_secret_endpoint;
  }
  std::string get_token() const noexcept {
    return g_ceph_context->_conf->rgw_s3_external_authentication_token;
  }
  std::bool verify_ssl() const noexcept {
    return g_ceph_context->_conf->rgw_s3_external_authentication_verify_ssl;
  }
};

class EAUserInfo {
  string user_id;
  string name;
  string tenant;
  RGWSubUser subuser;
  string access_key_id;
  bool admin = false;

public:
  EAUserInfo() = default;

  const std::string& get_user_id() const {return user_id;};
  const std::string& get_user_name() const {return name;};
  const std::string& get_tenant() const {return tenant;};
  const RGWSubUser& get_subuser() const {return subuser;};
  const std::string& get_access_key_id() const {return access_key_id;};
  const bool& is_admin() const {return admin;};
  int parse(CephContext* cct,
            const std::string& access_key_id_str,
            ceph::buffer::list& bl /* in */);
};

class SecretCache {
  using user_info_t = rgw::auth::s3::external_authentication::EAUserInfo;

  struct secret_entry {
    user_info_t user_info;
    std::string secret;
    utime_t expires;
    list<std::string>::iterator lru_iter;
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
      max(cct->_conf->rgw_s3_external_authentication_key_cache_size),
      s3_token_expiry_length(300, 0) {
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

  bool find(const std::string& access_key_id, user_info_t& user_info, std::string& secret);
  boost::optional<boost::tuple<user_info_t, std::string>> find(const std::string& access_key_id) {
    user_info_t user_info;
    std::string secret;
    if (find(access_key_id, user_info, secret)) {
      return boost::make_tuple(user_info, secret);
    }
    return boost::none;
  }
  void add(const std::string& access_key_id, const user_info_t& user_info, const std::string& secret);
}; /* class SecretCache */

class EAEngine : public rgw::auth::s3::AWSEngine
{
    using acl_strategy_t = rgw::auth::RemoteApplier::acl_strategy_t;
    using auth_info_t = rgw::auth::RemoteApplier::AuthInfo;
    using result_t = rgw::auth::Engine::result_t;
    using user_info_t = rgw::auth::s3::external_authentication::EAUserInfo;

    const rgw::auth::RemoteApplier::Factory* const apl_factory;
    rgw::auth::s3::external_authentication::Config config;
    rgw::auth::s3::external_authentication::SecretCache& secret_cache;


    acl_strategy_t get_acl_strategy() const;
    auth_info_t get_creds_info(const user_info_t& user_info) const noexcept;
    result_t authenticate(const DoutPrefixProvider* dpp,
                          const boost::string_view& access_key_id,
                          const boost::string_view& signature,
                          const boost::string_view& session_token,
                          const string_to_sign_t& string_to_sign,
                          const signature_factory_t& signature_factory,
                          const completer_factory_t& completer_factory,
                          const req_state* s) const override;
    std::pair<boost::optional<user_info_t>, int>
    get_user_info(const DoutPrefixProvider* dpp,
                  const boost::string_view& access_key_id,
                  const std::string& string_to_sign,
                  const boost::string_view& signature,
		              const signature_factory_t& signature_factory) const;
    std::pair<boost::optional<user_info_t>, int>
    get_from_server(const DoutPrefixProvider* dpp,
                      const boost::string_view& access_key_id,
                      const std::string& string_to_sign,
                      const boost::string_view& signature) const;
    std::pair<boost::optional<std::string>, int>
    get_secret_from_server(const DoutPrefixProvider* dpp,
                            const std::string& user_id,
                            const boost::string_view& access_key_id) const;
public:
  EAEngine(CephContext* const cct,
            const rgw::auth::s3::AWSEngine::VersionAbstractor* const ver_abstractor,
            const rgw::auth::RemoteApplier::Factory* const apl_factory,
            rgw::auth::s3::external_authentication::SecretCache& secret_cache)
    : AWSEngine(cct, *ver_abstractor),
      apl_factory(apl_factory),
      secret_cache(secret_cache) {
  }

  using AWSEngine::authenticate;

  const char* get_name() const noexcept override {
    return "rgw::auth::s3::external_authentication::EAEngine";
  }
}; /* class EAEngine */

}; /* namespace external_authentication */
}; /* namespace rgw::auth::s3 */


#endif /* CEPH_RGW_EXTERNAL_AUTHENTICATION_H */
