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
bool rgw_decode_pki_token(CephContext *cct,
                          const string& token,
                          bufferlist& bl);

enum class KeystoneApiVersion {
  VER_2,
  VER_3
};

class KeystoneService {
public:
  static KeystoneApiVersion get_api_version();
};

class KeystoneToken {
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
  KeystoneToken() = default;
  time_t get_expires() const { return token.expires; }
  string get_domain_id() const {return project.domain.id;};
  string get_domain_name() const {return project.domain.name;};
  string get_project_id() const {return project.id;};
  string get_project_name() const {return project.name;};
  string get_user_id() const {return user.id;};
  string get_user_name() const {return user.name;};
  bool has_role(const string& r) const;
  bool expired() {
    uint64_t now = ceph_clock_now(NULL).sec();
    return (now >= (uint64_t)get_expires());
  }
  int parse(CephContext *cct,
            const string& token_str,
            bufferlist& bl /* in */);
  void decode_json(JSONObj *access_obj);
};


class RGWKeystoneTokenCache {
  struct token_entry {
    KeystoneToken token;
    list<string>::iterator lru_iter;
  };

  atomic_t down_flag;

  class RevokeThread : public Thread {
    friend class RGWKeystoneTokenCache;
    typedef RGWPostHTTPData RGWGetRevokedTokens;

    CephContext * const cct;
    RGWKeystoneTokenCache * const cache;
    Mutex lock;
    Cond cond;

    RevokeThread(CephContext * const cct, RGWKeystoneTokenCache * cache)
      : cct(cct),
        cache(cache),
        lock("RGWKeystoneTokenCache::RevokeThread") {
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

  RGWKeystoneTokenCache()
    : revocator(g_ceph_context, this),
      cct(g_ceph_context),
      lock("RGWKeystoneTokenCache", true /* recursive */),
      max(cct->_conf->rgw_keystone_token_cache_size) {
    /* The thread name has been kept for backward compliance. */
    revocator.create("rgw_swift_k_rev");
  }
  ~RGWKeystoneTokenCache() {
    down_flag.set(1);

    revocator.stop();
    revocator.join();
  }

public:
  RGWKeystoneTokenCache(const RGWKeystoneTokenCache&) = delete;
  void operator=(const RGWKeystoneTokenCache&) = delete;

  static RGWKeystoneTokenCache& get_instance();

  bool find(const string& token_id, KeystoneToken& token);
  bool find_admin(KeystoneToken& token);
  void add(const string& token_id, const KeystoneToken& token);
  void add_admin(const KeystoneToken& token);
  void invalidate(const string& token_id);
  bool going_down() const;
};


class KeystoneAdminTokenRequest {
public:
  virtual ~KeystoneAdminTokenRequest() = default;
  virtual void dump(Formatter *f) const = 0;
};

class KeystoneAdminTokenRequestVer2 : public KeystoneAdminTokenRequest {
  CephContext *cct;

public:
  KeystoneAdminTokenRequestVer2(CephContext * const _cct)
    : cct(_cct) {
  }
  void dump(Formatter *f) const;
};

class KeystoneAdminTokenRequestVer3 : public KeystoneAdminTokenRequest {
  CephContext *cct;

public:
  KeystoneAdminTokenRequestVer3(CephContext * const _cct)
    : cct(_cct) {
  }
  void dump(Formatter *f) const;
};

#endif
