// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_KEYSTONE_H
#define CEPH_RGW_KEYSTONE_H

#include "rgw_common.h"

class KeystoneToken {
public:
  class Metadata {
  public:
    Metadata() : is_admin(false) { }
    bool is_admin;
    void decode_json(JSONObj *obj);
  };

  class Service {
  public:
    class Endpoint {
    public:
      string id;
      string admin_url;
      string public_url;
      string internal_url;
      string region;
      void decode_json(JSONObj *obj);
    };
    string type;
    string name;
    list<Endpoint> endpoints;
    void decode_json(JSONObj *obj);
  };

  class Token {
  public:
    Token() : expires(0) { }
    class Tenant {
    public:
      Tenant() : enabled(false) { }
      string id;
      string name;
      string description;
      bool enabled;
      void decode_json(JSONObj *obj);
    };
    string id;
    time_t expires;
    Tenant tenant;
    void decode_json(JSONObj *obj);
  };

  class User {
  public:
    class Role {
    public:
      string id;
      string name;
      void decode_json(JSONObj *obj);
    };
    string id;
    string name;
    string user_name;
    list<Role> roles;
    void decode_json(JSONObj *obj);
    bool has_role(const string& r);
  };

  Metadata metadata;
  list<Service> service_catalog;
  Token token;
  User user;

public:
  int parse(CephContext *cct, bufferlist& bl);

  bool expired() {
    uint64_t now = ceph_clock_now(NULL).sec();
    return (now >= (uint64_t)token.expires);
  }

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
