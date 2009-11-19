// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef __AUTHTYPES_H
#define __AUTHTYPES_H

#include "Crypto.h"
#include "msg/msg_types.h"

#include "config.h"

#include <errno.h>

class Cond;

struct EntityName {
  uint32_t entity_type;
  string name;

  void encode(bufferlist& bl) const {
    ::encode(entity_type, bl);
    ::encode(name, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(entity_type, bl);
    ::decode(name, bl);
  }

  void to_str(string& str) const {
    str.append(ceph_entity_type_name(entity_type));
    str.append(".");
    str.append(name);
  }
  string to_str() const {
    string s;
    to_str(s);
    return s;
  }

  bool from_str(string& s) {
    int pos = s.find('.');

    if (pos < 0)
      return false;
   
    string pre = s.substr(0, pos);
    const char *pres = pre.c_str();

    set_type(pres);

    name = s.substr(pos + 1);

    return true;
  }

  void set_type(const char *type) {
    if (strcmp(type, "auth") == 0) {
      entity_type = CEPH_ENTITY_TYPE_AUTH;
    } else if (strcmp(type, "mon") == 0) {
      entity_type = CEPH_ENTITY_TYPE_MON;
    } else if (strcmp(type, "osd") == 0) {
      entity_type = CEPH_ENTITY_TYPE_OSD;
    } else if (strcmp(type, "mds") == 0) {
      entity_type = CEPH_ENTITY_TYPE_MDS;
    } else {
      entity_type = CEPH_ENTITY_TYPE_CLIENT;
    }
  }
  void from_type_id(const char *type, const char *id) {
    set_type(type);
    name = id;
  }

  void get_type_str(string& s) {
    s = ceph_entity_type_name(entity_type);
  }

  bool is_admin() {
    return (name.compare("admin") == 0);
  }
};
WRITE_CLASS_ENCODER(EntityName);

inline bool operator<(const EntityName& a, const EntityName& b) {
  return (a.entity_type < b.entity_type) || (a.entity_type == b.entity_type && a.name < b.name);
}

static inline ostream& operator<<(ostream& out, const EntityName& n) {
  return out << n.to_str();
}



struct EntityAuth {
  CryptoKey key;
  map<string, bufferlist> caps;

  void encode(bufferlist& bl) const {
    ::encode(key, bl);
    ::encode(caps, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(key, bl);
    ::decode(caps, bl);
  }
};
WRITE_CLASS_ENCODER(EntityAuth)

static inline ostream& operator<<(ostream& out, const EntityAuth& a) {
  return out << "auth(key=" << a.key << " with " << a.caps.size() << " caps)";
}

struct AuthCapsInfo {
  bool allow_all;
  bufferlist caps;

 void encode(bufferlist& bl) const {
    uint32_t a = (uint32_t)allow_all;
    ::encode(a, bl);
    ::encode(caps, bl);
  }
  void decode(bufferlist::iterator& bl) {
    uint32_t a;
    ::decode(a, bl);
    allow_all = (bool)a;
    ::decode(caps, bl);
  }
};
WRITE_CLASS_ENCODER(AuthCapsInfo)

/*
 * The ticket (if properly validated) authorizes the principal use
 * services as described by 'caps' during the specified validity
 * period.
 */
struct AuthTicket {
  EntityName name;
  uint64_t global_id; /* global instance id */
  utime_t created, renew_after, expires;
  AuthCapsInfo caps;
  __u32 flags;

  AuthTicket() : flags(0) {}

  void init_timestamps(utime_t now, double ttl) {
    created = now;
    expires = now;
    expires += ttl;
    renew_after = now;
    renew_after += ttl / 2.0;
  }

  void encode(bufferlist& bl) const {
    __u8 v = 1;
    ::encode(v, bl);
    ::encode(name, bl);
    ::encode(global_id, bl);
    ::encode(created, bl);
    ::encode(expires, bl);
    ::encode(caps, bl);
    ::encode(flags, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 v;
    ::decode(v, bl);
    ::decode(name, bl);
    ::decode(global_id, bl);
    ::decode(created, bl);
    ::decode(expires, bl);
    ::decode(caps, bl);
    ::decode(flags, bl);
  }
};
WRITE_CLASS_ENCODER(AuthTicket)


/*
 * abstract authorizer class
 */
struct AuthAuthorizer {
  __u32 protocol;
  bufferlist bl;

  AuthAuthorizer(__u32 p) : protocol(p) {}
  virtual ~AuthAuthorizer() {}
  virtual bool verify_reply(bufferlist::iterator& reply) = 0;
};


/*
 * Key management
 */ 
#define KEY_ROTATE_NUM 3

struct ExpiringCryptoKey {
  CryptoKey key;
  utime_t expiration;

  void encode(bufferlist& bl) const {
    ::encode(key, bl);
    ::encode(expiration, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(key, bl);
    ::decode(expiration, bl);
  }
};
WRITE_CLASS_ENCODER(ExpiringCryptoKey);

struct RotatingSecrets {
  map<uint64_t, ExpiringCryptoKey> secrets;
  version_t max_ver;

  void encode(bufferlist& bl) const {
    ::encode(secrets, bl);
    ::encode(max_ver, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(secrets, bl);
    ::decode(max_ver, bl);
  }

  void add(ExpiringCryptoKey& key);
};
WRITE_CLASS_ENCODER(RotatingSecrets);




class KeyStore {
public:
  virtual ~KeyStore() {}
  virtual bool get_secret(EntityName& name, CryptoKey& secret) = 0;
  virtual bool get_service_secret(uint32_t service_id, uint64_t secret_id, CryptoKey& secret) = 0;
};

static inline bool auth_principal_needs_rotating_keys(EntityName& name)
{
  return ((name.entity_type == CEPH_ENTITY_TYPE_OSD) ||
          (name.entity_type == CEPH_ENTITY_TYPE_MDS));
}


#endif
