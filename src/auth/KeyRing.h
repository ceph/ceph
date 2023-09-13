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

#include "auth/Auth.h"
#include "include/common_fwd.h"

class KeyRing : public KeyStore {
  std::map<EntityName, EntityAuth> keys;

  int set_modifier(const char *type, const char *val, EntityName& name, std::map<std::string, ceph::buffer::list>& caps);
public:
  void decode_plaintext(ceph::buffer::list::const_iterator& bl);
  /* Create a KeyRing from a Ceph context.
   * We will use the configuration stored inside the context. */
  int from_ceph_context(CephContext *cct);

  std::map<EntityName, EntityAuth>& get_keys() { return keys; }  // yuck

  int load(CephContext *cct, const std::string &filename);
  void print(std::ostream& out);

  // accessors
  bool exists(const EntityName& name) const {
    auto p = keys.find(name);
    return p != keys.end();
  }
  bool get_auth(const EntityName& name, EntityAuth &a) const {
    std::map<EntityName, EntityAuth>::const_iterator k = keys.find(name);
    if (k == keys.end())
      return false;
    a = k->second;
    return true;
  }
  bool get_secret(const EntityName& name, CryptoKey& secret) const override {
    std::map<EntityName, EntityAuth>::const_iterator k = keys.find(name);
    if (k == keys.end())
      return false;
    secret = k->second.key;
    return true;
  }
  bool get_service_secret(uint32_t service_id, uint64_t secret_id,
			  CryptoKey& secret) const override {
    return false;
  }
  bool get_caps(const EntityName& name,
		const std::string& type, AuthCapsInfo& caps) const {
    std::map<EntityName, EntityAuth>::const_iterator k = keys.find(name);
    if (k == keys.end())
      return false;
    std::map<std::string,ceph::buffer::list>::const_iterator i = k->second.caps.find(type);
    if (i != k->second.caps.end()) {
      caps.caps = i->second;
    }
    return true;
  }
  size_t size() const {
    return keys.size();
  }

  // modifiers
  void add(const EntityName& name, const EntityAuth &a) {
    keys[name] = a;
  }
  void add(const EntityName& name, const CryptoKey &k) {
    EntityAuth a;
    a.key = k;
    keys[name] = a;
  }
  void add(const EntityName& name, const CryptoKey &k, const CryptoKey &pk) {
    EntityAuth a;
    a.key = k;
    a.pending_key = pk;
    keys[name] = a;
  }
  void remove(const EntityName& name) {
    keys.erase(name);
  }
  void set_caps(const EntityName& name, std::map<std::string, ceph::buffer::list>& caps) {
    keys[name].caps = caps;
  }
  void set_key(EntityName& ename, CryptoKey& key) {
    keys[ename].key = key;
  }
  void import(CephContext *cct, KeyRing& other);

  // decode as plaintext
  void decode(ceph::buffer::list::const_iterator& bl);

  void encode_plaintext(ceph::buffer::list& bl);
  void encode_formatted(std::string label, ceph::Formatter *f, ceph::buffer::list& bl);
};

// don't use WRITE_CLASS_ENCODER macro because we don't have an encode
// macro.  don't juse encode_plaintext in that case because it is not
// wrappable; it assumes it gets the entire ceph::buffer::list.
static inline void decode(KeyRing& kr, ceph::buffer::list::const_iterator& p) {
  kr.decode(p);
}

#endif
