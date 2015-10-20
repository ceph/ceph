// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_KEYSTONE_H
#define CEPH_RGW_KEYSTONE_H

#include "rgw_common.h"
#include "common/Cond.h"
#include "rgw_http_client.h"

class KeystoneToken {
protected:
  string version;

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
  KeystoneToken() : version("") {};
  KeystoneToken(string _version) : version(_version) {};
  time_t get_expires() { return token.expires; }
  string get_domain_id() {return project.domain.id;};
  string get_domain_name()  {return project.domain.name;};
  string get_project_id() {return project.id;};
  string get_project_name() {return project.name;};
  string get_user_id() {return user.id;};
  string get_user_name() {return user.name;};
  bool has_role(const string& r);
  bool expired() {
    uint64_t now = ceph_clock_now(NULL).sec();
    return (now >= (uint64_t)get_expires());
  }
  int parse(CephContext *cct, bufferlist& bl);
  void decode_json(JSONObj *access_obj);
};

struct token_entry {
  KeystoneToken token;
  list<string>::iterator lru_iter;
};

class RGWPostHTTPData : public RGWHTTPClient {
  bufferlist *bl;
  string post_data;
  size_t post_data_index;
  string subject_token;
public:
  RGWPostHTTPData(CephContext *_cct, bufferlist *_bl) : RGWHTTPClient(_cct), bl(_bl), post_data_index(0) { }

  void set_post_data(const string &_post_data) {
    this->post_data = _post_data;
  }

  int send_data(void *ptr, size_t len) {
    int length_to_copy = 0;
    if (post_data_index < post_data.length()) {
      length_to_copy = min(post_data.length() - post_data_index, len);
      memcpy(ptr, post_data.data() + post_data_index, length_to_copy);
      post_data_index += length_to_copy;
    }
    return length_to_copy;
  }

  int receive_data(void *ptr, size_t len) {
    bl->append((char *) ptr, len);
    return 0;
  }

  int receive_header(void *ptr, size_t len);

  string get_subject_token() {
    return subject_token;
  }
};

typedef RGWPostHTTPData RGWValidateKeystoneToken;
typedef RGWPostHTTPData RGWGetKeystoneAdminToken;
typedef RGWPostHTTPData RGWGetRevokedTokens;

class Keystone {
  CephContext *cct;
  atomic_t stopping_flag;

protected:
  class KeystoneRevokeThread : public Thread {
    CephContext *cct;
    Keystone *keystone;
    Mutex lock;
    Cond cond;

  public:
    KeystoneRevokeThread(CephContext *_cct, Keystone *_keystone) : cct(_cct), keystone(_keystone), lock("KeystoneRevokeThread") {}
    void *entry();
    void stop();
  };
  class RGWKeystoneTokenCache {
    CephContext *cct;

    map<string, token_entry> tokens;
    list<string> tokens_lru;

    Mutex lock;

    size_t max;

  public:
    RGWKeystoneTokenCache(CephContext *_cct, int _max) : cct(_cct), lock("RGWKeystoneTokenCache"), max(_max) {}

    bool find(const string& token_id, KeystoneToken& token);
    void add(const string& token_id, KeystoneToken& token);
    void invalidate(const string& token_id);
  };

  KeystoneRevokeThread *keystone_revoke_thread;
  RGWKeystoneTokenCache *cache;
  int check_revoked();
  int get_keystone_url(std::string& url);
  int get_keystone_admin_token(std::string& token);
  int parse_response(const string &token, bufferlist bl, KeystoneToken &t);
  void init();
  void finalize();

public:
  Keystone(CephContext *_cct) : cct(_cct) {
    cache = NULL;
    init();
  }
  ~Keystone() {
    finalize();
  }
  bool enabled() {
    return !cct->_conf->rgw_keystone_url.empty();
  }
  int validate_token(const string& token, KeystoneToken& t);

  bool stopping();
  void stop();
};

#endif
