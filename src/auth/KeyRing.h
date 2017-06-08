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

#ifndef CEPH_KEYRING_H
#define CEPH_KEYRING_H

#include "common/config.h"

#include "auth/Crypto.h"
#include "auth/Auth.h"


class KeyRing : public KeyStore {
  map<EntityName, EntityAuth> keys;

  int set_modifier(const char *type, const char *val, EntityName& name, map<string, bufferlist>& caps);
public:
  void decode_plaintext(bufferlist::iterator& bl);
  /* Create a KeyRing from a Ceph context.
   * We will use the configuration stored inside the context. */
  int from_ceph_context(CephContext *cct);

  /* Create an empty KeyRing */
  static KeyRing *create_empty();

  map<EntityName, EntityAuth>& get_keys() { return keys; }  // yuck

  int load(CephContext *cct, const std::string &filename);
  void print(ostream& out);

  // accessors
  bool get_auth(const EntityName& name, EntityAuth &a) const {
    map<EntityName, EntityAuth>::const_iterator k = keys.find(name);
    if (k == keys.end())
      return false;
    a = k->second;
    return true;
  }
  bool get_secret(const EntityName& name, CryptoKey& secret) const {
    map<EntityName, EntityAuth>::const_iterator k = keys.find(name);
    if (k == keys.end())
      return false;
    secret = k->second.key;
    return true;
  }
  bool get_service_secret(uint32_t service_id, uint64_t secret_id,
			  CryptoKey& secret) const {
    return false;
  }
  bool get_caps(const EntityName& name,
		const std::string& type, AuthCapsInfo& caps) const {
    map<EntityName, EntityAuth>::const_iterator k = keys.find(name);
    if (k == keys.end())
      return false;
    map<string,bufferlist>::const_iterator i = k->second.caps.find(type);
    if (i != k->second.caps.end()) {
      caps.caps = i->second;
    }
    return true;
  }

  // modifiers
  void add(const EntityName& name, EntityAuth &a) {
    keys[name] = a;
  }
  void add(const EntityName& name, CryptoKey &k) {
    EntityAuth a;
    a.key = k;
    keys[name] = a;
  }
  void remove(const EntityName& name) {
    keys.erase(name);
  }
  void set_caps(EntityName& name, map<string, bufferlist>& caps) {
    keys[name].caps = caps;
  }
  void set_uid(EntityName& ename, uint64_t auid) {
    keys[ename].auid = auid;
  }
  void set_key(EntityName& ename, CryptoKey& key) {
    keys[ename].key = key;
  }
  void import(CephContext *cct, KeyRing& other);

  // encoders
  void decode(bufferlist::iterator& bl);

  void encode_plaintext(bufferlist& bl);
  void encode_formatted(string label, Formatter *f, bufferlist& bl);
};

// don't use WRITE_CLASS_ENCODER macro because we don't have an encode
// macro.  don't juse encode_plaintext in that case because it is not
// wrappable; it assumes it gets the entire bufferlist.
static inline void decode(KeyRing& kr, bufferlist::iterator& p) {
  kr.decode(p);
}

#endif
