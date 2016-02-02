// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_KEYSTONE_H
#define CEPH_RGW_KEYSTONE_H

#include "rgw_common.h"

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


#endif
