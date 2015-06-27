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

#ifndef CEPH_AUTHTYPES_H
#define CEPH_AUTHTYPES_H

#include "Crypto.h"
#include "msg/msg_types.h"

#include "common/config.h"
#include "common/entity_name.h"

class Cond;

struct EntityAuth {
  uint64_t auid;
  CryptoKey key;
  map<string, bufferlist> caps;

  EntityAuth() : auid(CEPH_AUTH_UID_DEFAULT) {}

  void encode(bufferlist& bl) const {
    __u8 struct_v = 2;
    ::encode(struct_v, bl);
    ::encode(auid, bl);
    ::encode(key, bl);
    ::encode(caps, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    if (struct_v >= 2)
      ::decode(auid, bl);
    else auid = CEPH_AUTH_UID_DEFAULT;
    ::decode(key, bl);
    ::decode(caps, bl);
  }
};
WRITE_CLASS_ENCODER(EntityAuth)

static inline ostream& operator<<(ostream& out, const EntityAuth& a) {
  return out << "auth(auid = " << a.auid << " key=" << a.key << " with " << a.caps.size() << " caps)";
}

struct AuthCapsInfo {
  bool allow_all;
  bufferlist caps;

  AuthCapsInfo() : allow_all(false) {}

  void encode(bufferlist& bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    __u8 a = (__u8)allow_all;
    ::encode(a, bl);
    ::encode(caps, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    __u8 a;
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
  uint64_t auid;
  utime_t created, renew_after, expires;
  AuthCapsInfo caps;
  __u32 flags;

  AuthTicket() : global_id(0), auid(CEPH_AUTH_UID_DEFAULT), flags(0){}

  void init_timestamps(utime_t now, double ttl) {
    created = now;
    expires = now;
    expires += ttl;
    renew_after = now;
    renew_after += ttl / 2.0;
  }

  void encode(bufferlist& bl) const {
    __u8 struct_v = 2;
    ::encode(struct_v, bl);
    ::encode(name, bl);
    ::encode(global_id, bl);
    ::encode(auid, bl);
    ::encode(created, bl);
    ::encode(expires, bl);
    ::encode(caps, bl);
    ::encode(flags, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(name, bl);
    ::decode(global_id, bl);
    if (struct_v >= 2)
      ::decode(auid, bl);
    else auid = CEPH_AUTH_UID_DEFAULT;
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
  CryptoKey session_key;

  explicit AuthAuthorizer(__u32 p) : protocol(p) {}
  virtual ~AuthAuthorizer() {}
  virtual bool verify_reply(bufferlist::iterator& reply) = 0;
};


/*
 * Key management
 */ 
#define KEY_ROTATE_NUM 3   /* prev, current, next */

struct ExpiringCryptoKey {
  CryptoKey key;
  utime_t expiration;

  void encode(bufferlist& bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(key, bl);
    ::encode(expiration, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(key, bl);
    ::decode(expiration, bl);
  }
};
WRITE_CLASS_ENCODER(ExpiringCryptoKey)

static inline ostream& operator<<(ostream& out, const ExpiringCryptoKey& c)
{
  return out << c.key << " expires " << c.expiration;
}

struct RotatingSecrets {
  map<uint64_t, ExpiringCryptoKey> secrets;
  version_t max_ver;
  
  RotatingSecrets() : max_ver(0) {}
  
  void encode(bufferlist& bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(secrets, bl);
    ::encode(max_ver, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(secrets, bl);
    ::decode(max_ver, bl);
  }
  
  uint64_t add(ExpiringCryptoKey& key) {
    secrets[++max_ver] = key;
    while (secrets.size() > KEY_ROTATE_NUM)
      secrets.erase(secrets.begin());
    return max_ver;
  }
  
  bool need_new_secrets() const {
    return secrets.size() < KEY_ROTATE_NUM;
  }
  bool need_new_secrets(utime_t now) const {
    return secrets.size() < KEY_ROTATE_NUM || current().expiration <= now;
  }

  ExpiringCryptoKey& previous() {
    return secrets.begin()->second;
  }
  ExpiringCryptoKey& current() {
    map<uint64_t, ExpiringCryptoKey>::iterator p = secrets.begin();
    ++p;
    return p->second;
  }
  const ExpiringCryptoKey& current() const {
    map<uint64_t, ExpiringCryptoKey>::const_iterator p = secrets.begin();
    ++p;
    return p->second;
  }
  ExpiringCryptoKey& next() {
    return secrets.rbegin()->second;
  }
  bool empty() {
    return secrets.empty();
  }

  void dump();
};
WRITE_CLASS_ENCODER(RotatingSecrets)



class KeyStore {
public:
  virtual ~KeyStore() {}
  virtual bool get_secret(const EntityName& name, CryptoKey& secret) const = 0;
  virtual bool get_service_secret(uint32_t service_id, uint64_t secret_id,
				  CryptoKey& secret) const = 0;
};

static inline bool auth_principal_needs_rotating_keys(EntityName& name)
{
  uint32_t ty(name.get_type());
  return ((ty == CEPH_ENTITY_TYPE_OSD) || (ty == CEPH_ENTITY_TYPE_MDS));
}

#endif
