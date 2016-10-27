// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_KEYSTONE_H
#define CEPH_RGW_KEYSTONE_H

#include "rgw_common.h"
#include "rgw_http_client.h"
#include "common/Cond.h"

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
bool rgw_decode_pki_token(CephContext *cct,
                          const string& token,
                          bufferlist& bl);

namespace rgw {
namespace keystone {

enum class ApiVersion {
  VER_2,
  VER_3
};

class Service {
public:
  class RGWKeystoneHTTPTransceiver : public RGWHTTPTransceiver {
  public:
    RGWKeystoneHTTPTransceiver(CephContext * const cct,
                               bufferlist * const token_body_bl)
      : RGWHTTPTransceiver(cct, token_body_bl,
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

  static ApiVersion get_api_version();

  static int get_keystone_url(CephContext * const cct,
                              std::string& url);
  static int get_keystone_admin_token(CephContext * const cct,
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

public:
  // FIXME: default ctor needs to be eradicated here
  TokenEnvelope() = default;
  time_t get_expires() const { return token.expires; }
  const std::string& get_domain_id() const {return project.domain.id;};
  const std::string& get_domain_name() const {return project.domain.name;};
  const std::string& get_project_id() const {return project.id;};
  const std::string& get_project_name() const {return project.name;};
  const std::string& get_user_id() const {return user.id;};
  const std::string& get_user_name() const {return user.name;};
  bool has_role(const string& r) const;
  bool expired() {
    uint64_t now = ceph_clock_now().sec();
    return (now >= (uint64_t)get_expires());
  }
  int parse(CephContext *cct,
            const string& token_str,
            bufferlist& bl /* in */);
  void decode_json(JSONObj *access_obj);
};


class TokenCache {
  struct token_entry {
    TokenEnvelope token;
    list<string>::iterator lru_iter;
  };

  const rgw::keystone::Config& config;
  atomic_t down_flag;

  class RevokeThread : public Thread {
    friend class TokenCache;
    typedef RGWPostHTTPData RGWGetRevokedTokens;

    CephContext * const cct;
    TokenCache* const cache;
    Mutex lock;
    Cond cond;

    RevokeThread(CephContext* const cct,
                 TokenCache* const cache)
      : cct(cct),
        cache(cache),
        lock("rgw::keystone::TokenCache::RevokeThread") {
    }
    void *entry();
    void stop();
    int check_revoked();
  } revocator;

  CephContext * const cct;

  std::string admin_token_id;
  std::map<std::string, token_entry> tokens;
  std::list<std::string> tokens_lru;

  Mutex lock;

  const size_t max;

  TokenCache()
    : revocator(g_ceph_context, this),
      cct(g_ceph_context),
      lock("rgw::keystone::TokenCache"),
      max(cct->_conf->rgw_keystone_token_cache_size) {
    /* The thread name has been kept for backward compliance. */
    revocator.create("rgw_swift_k_rev");
  }

  ~TokenCache() {
    down_flag.set(1);

    revocator.stop();
    revocator.join();
  }

public:
  TokenCache(const TokenCache&) = delete;
  void operator=(const TokenCache&) = delete;

  static TokenCache& get_instance();

    /* In C++11 this is thread safe. */
    static TokenCache instance(ConfigT::get_instance());
    return instance;
  }

  bool find(const std::string& token_id, TokenEnvelope& token);
  bool find_admin(TokenEnvelope& token);
  void add(const std::string& token_id, const TokenEnvelope& token);
  void add_admin(const TokenEnvelope& token);
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
  CephContext* cct;

public:
  AdminTokenRequestVer2(CephContext* const cct)
    : cct(cct) {
  }
  void dump(Formatter* f) const;
};

class AdminTokenRequestVer3 : public AdminTokenRequest {
  CephContext* cct;

public:
  AdminTokenRequestVer3(CephContext* const cct)
    : cct(cct) {
  }
  void dump(Formatter* f) const;
};

}; /* namespace keystone */
}; /* namespace rgw */

#endif
