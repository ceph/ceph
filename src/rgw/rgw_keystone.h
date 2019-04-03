// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_KEYSTONE_H
#define CEPH_RGW_KEYSTONE_H

#include <type_traits>

#include <boost/optional.hpp>
#include <boost/utility/string_ref.hpp>

#include "rgw_common.h"
#include "rgw_http_client.h"
#include "common/Cond.h"
#include "global/global_init.h"

#include <atomic>

int rgw_open_cms_envelope(CephContext *cct,
                          const std::string& src,
                          std::string& dst);            /* out */
int rgw_decode_b64_cms(CephContext *cct,
                       const string& signed_b64,
                       bufferlist& bl);
bool rgw_is_pki_token(const string& token);
void rgw_get_token_id(const string& token, string& token_id);
static inline std::string rgw_get_token_id(const string& token)
{
  std::string token_id;
  rgw_get_token_id(token, token_id);

  return token_id;
}

namespace rgw {
namespace keystone {

enum class ApiVersion {
  VER_2,
  VER_3
};


class Config {
protected:
  Config() = default;
  virtual ~Config() = default;

public:
  virtual std::string get_endpoint_url() const noexcept = 0;
  virtual ApiVersion get_api_version() const noexcept = 0;

  virtual std::string get_admin_token() const noexcept = 0;
  virtual boost::string_ref get_admin_user() const noexcept = 0;
  virtual std::string get_admin_password() const noexcept = 0;
  virtual boost::string_ref get_admin_tenant() const noexcept = 0;
  virtual boost::string_ref get_admin_project() const noexcept = 0;
  virtual boost::string_ref get_admin_domain() const noexcept = 0;
};

class CephCtxConfig : public Config {
protected:
  CephCtxConfig() = default;
  virtual ~CephCtxConfig() = default;

  const static std::string empty;

public:
  static CephCtxConfig& get_instance() {
    static CephCtxConfig instance;
    return instance;
  }

  std::string get_endpoint_url() const noexcept override;
  ApiVersion get_api_version() const noexcept override;

  std::string get_admin_token() const noexcept override;

  boost::string_ref get_admin_user() const noexcept override {
    return g_ceph_context->_conf->rgw_keystone_admin_user;
  }

  std::string get_admin_password() const noexcept override;

  boost::string_ref get_admin_tenant() const noexcept override {
    return g_ceph_context->_conf->rgw_keystone_admin_tenant;
  }

  boost::string_ref get_admin_project() const noexcept override {
    return g_ceph_context->_conf->rgw_keystone_admin_project;
  }

  boost::string_ref get_admin_domain() const noexcept override {
    return g_ceph_context->_conf->rgw_keystone_admin_domain;
  }
};


class TokenEnvelope;
class TokenCache;

class Service {
public:
  class RGWKeystoneHTTPTransceiver : public RGWHTTPTransceiver {
  public:
    RGWKeystoneHTTPTransceiver(CephContext * const cct,
                               const string& method,
                               const string& url,
                               bufferlist * const token_body_bl)
      : RGWHTTPTransceiver(cct, method, url, token_body_bl,
                           cct->_conf->rgw_keystone_verify_ssl,
                           { "X-Subject-Token" }) {
    }

    const header_value_t& get_subject_token() const {
      try {
        return get_header_value("X-Subject-Token");
      } catch (std::out_of_range&) {
        static header_value_t empty_val;
        return empty_val;
      }
    }
  };

  typedef RGWKeystoneHTTPTransceiver RGWValidateKeystoneToken;
  typedef RGWKeystoneHTTPTransceiver RGWGetKeystoneAdminToken;
  typedef RGWKeystoneHTTPTransceiver RGWGetRevokedTokens;

  static int get_admin_token(CephContext* const cct,
                             TokenCache& token_cache,
                             const Config& config,
                             std::string& token);
  static int issue_admin_token_request(CephContext* const cct,
                                       const Config& config,
                                       TokenEnvelope& token);
  static int get_keystone_barbican_token(CephContext * const cct,
                                         std::string& token);
};


class TokenEnvelope {
public:
  class Domain {
  public:
    string id;
    string name;
    void decode_json(JSONObj *obj);
  };
  class Project {
  public:
    Domain domain;
    string id;
    string name;
    void decode_json(JSONObj *obj);
  };

  class Token {
  public:
    Token() : expires(0) { }
    string id;
    time_t expires;
    Project tenant_v2;
    void decode_json(JSONObj *obj);
  };

  class Role {
  public:
    string id;
    string name;
    void decode_json(JSONObj *obj);
  };

  class User {
  public:
    string id;
    string name;
    Domain domain;
    list<Role> roles_v2;
    void decode_json(JSONObj *obj);
  };

  Token token;
  Project project;
  User user;
  list<Role> roles;

  void decode_v3(JSONObj* obj);
  void decode_v2(JSONObj* obj);

public:
  /* We really need the default ctor because of the internals of TokenCache. */
  TokenEnvelope() = default;

  time_t get_expires() const { return token.expires; }
  const std::string& get_domain_id() const {return project.domain.id;};
  const std::string& get_domain_name() const {return project.domain.name;};
  const std::string& get_project_id() const {return project.id;};
  const std::string& get_project_name() const {return project.name;};
  const std::string& get_user_id() const {return user.id;};
  const std::string& get_user_name() const {return user.name;};
  bool has_role(const string& r) const;
  bool expired() const {
    const uint64_t now = ceph_clock_now().sec();
    return now >= static_cast<uint64_t>(get_expires());
  }
  int parse(CephContext* cct,
            const std::string& token_str,
            ceph::buffer::list& bl /* in */,
            ApiVersion version);
};


class TokenCache {
  struct token_entry {
    TokenEnvelope token;
    list<string>::iterator lru_iter;
  };

  std::atomic<bool> down_flag = { false };

  class RevokeThread : public Thread {
    friend class TokenCache;
    typedef RGWPostHTTPData RGWGetRevokedTokens;

    CephContext* const cct;
    TokenCache* const cache;
    const rgw::keystone::Config& config;

    Mutex lock;
    Cond cond;

    RevokeThread(CephContext* const cct,
                 TokenCache* const cache,
                 const rgw::keystone::Config& config)
      : cct(cct),
        cache(cache),
        config(config),
        lock("rgw::keystone::TokenCache::RevokeThread") {
    }

    void *entry() override;
    void stop();
    int check_revoked();
  } revocator;

  const boost::intrusive_ptr<CephContext> cct;

  std::string admin_token_id;
  std::string barbican_token_id;
  std::map<std::string, token_entry> tokens;
  std::list<std::string> tokens_lru;

  Mutex lock;

  const size_t max;

  explicit TokenCache(const rgw::keystone::Config& config)
    : revocator(g_ceph_context, this, config),
      cct(g_ceph_context),
      lock("rgw::keystone::TokenCache"),
      max(cct->_conf->rgw_keystone_token_cache_size) {
    /* revocation logic needs to be smarter, but meanwhile,
     *  make it optional.
     * see http://tracker.ceph.com/issues/9493
     *     http://tracker.ceph.com/issues/19499
     */
    if (cct->_conf->rgw_keystone_revocation_interval > 0
        && cct->_conf->rgw_keystone_token_cache_size ) {
      /* The thread name has been kept for backward compliance. */
      revocator.create("rgw_swift_k_rev");
    }
  }

  ~TokenCache() {
    down_flag = true;

    // Only stop and join if revocator thread is started.
    if (revocator.is_started()) {
      revocator.stop();
      revocator.join();
    }
  }

public:
  TokenCache(const TokenCache&) = delete;
  void operator=(const TokenCache&) = delete;

  template<class ConfigT>
  static TokenCache& get_instance() {
    static_assert(std::is_base_of<rgw::keystone::Config, ConfigT>::value,
                  "ConfigT must be a subclass of rgw::keystone::Config");

    /* In C++11 this is thread safe. */
    static TokenCache instance(ConfigT::get_instance());
    return instance;
  }

  bool find(const std::string& token_id, TokenEnvelope& token);
  boost::optional<TokenEnvelope> find(const std::string& token_id) {
    TokenEnvelope token_envlp;
    if (find(token_id, token_envlp)) {
      return token_envlp;
    }
    return boost::none;
  }
  bool find_admin(TokenEnvelope& token);
  bool find_barbican(TokenEnvelope& token);
  void add(const std::string& token_id, const TokenEnvelope& token);
  void add_admin(const TokenEnvelope& token);
  void add_barbican(const TokenEnvelope& token);
  void invalidate(const std::string& token_id);
  bool going_down() const;
private:
  void add_locked(const std::string& token_id, const TokenEnvelope& token);
  bool find_locked(const std::string& token_id, TokenEnvelope& token);

};


class AdminTokenRequest {
public:
  virtual ~AdminTokenRequest() = default;
  virtual void dump(Formatter* f) const = 0;
};

class AdminTokenRequestVer2 : public AdminTokenRequest {
  const Config& conf;

public:
  explicit AdminTokenRequestVer2(const Config& conf)
    : conf(conf) {
  }
  void dump(Formatter *f) const override;
};

class AdminTokenRequestVer3 : public AdminTokenRequest {
  const Config& conf;

public:
  explicit AdminTokenRequestVer3(const Config& conf)
    : conf(conf) {
  }
  void dump(Formatter *f) const override;
};

class BarbicanTokenRequestVer2 : public AdminTokenRequest {
  CephContext *cct;

public:
  explicit BarbicanTokenRequestVer2(CephContext * const _cct)
    : cct(_cct) {
  }
  void dump(Formatter *f) const override;
};

class BarbicanTokenRequestVer3 : public AdminTokenRequest {
  CephContext *cct;

public:
  explicit BarbicanTokenRequestVer3(CephContext * const _cct)
    : cct(_cct) {
  }
  void dump(Formatter *f) const override;
};


}; /* namespace keystone */
}; /* namespace rgw */

#endif
