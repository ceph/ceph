// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_KEYSTONE_H
#define CEPH_RGW_KEYSTONE_H

#include "rgw_common.h"

int rgw_open_cms_envelope(CephContext *cct, string& src, string& dst);
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

  CephContext *cct;

  string admin_token_id;
  map<string, token_entry> tokens;
  list<string> tokens_lru;

  Mutex lock;

  size_t max;

public:
  RGWKeystoneTokenCache(CephContext *_cct, int _max)
    : cct(_cct),
      lock("RGWKeystoneTokenCache", true /* recursive */),
      max(_max) {
  }

  bool find(const string& token_id, KeystoneToken& token);
  bool find_admin(KeystoneToken& token);
  void add(const string& token_id, const KeystoneToken& token);
  void add_admin(const KeystoneToken& token);
  void invalidate(const string& token_id);
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
