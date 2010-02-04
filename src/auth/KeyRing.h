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

#ifndef __KEYRING_H
#define __KEYRING_H

#include "config.h"

#include "auth/Crypto.h"
#include "auth/Auth.h"

/*
  KeyRing is being used at the service side, for holding the temporary rotating
  key of that service
*/

class KeyRing : public KeyStore {
  map<string, EntityAuth> keys;
  RotatingSecrets rotating_secrets;
  Mutex lock;
public:
  KeyRing() : lock("KeyRing") {}

  map<string, EntityAuth>& get_keys() { return keys; }  // yuck

  bool load(const char *filename);
  void print(ostream& out);

  bool get_auth(EntityName& name, EntityAuth &a) {
    string n = name.to_str();
    if (keys.count(n)) {
      a = keys[n];
      return true;
    }
    return false;
  }
  bool get_secret(EntityName& name, CryptoKey& secret) {
    string n = name.to_str();
    if (keys.count(n)) {
      secret = keys[n].key;
      return true;
    }
    return false;
  }
  void get_master(CryptoKey& dest) {
    get_secret(*g_conf.entity_name, dest);
  }

  //
  void add(EntityName& name, EntityAuth &a) {
    string s = name.to_str();
    keys[s] = a;
  }
  void set_caps(EntityName& name, map<string, bufferlist>& caps) {
    string s = name.to_str();
    keys[s].caps = caps;
  }
  void import(KeyRing& other);

  // weirdness
  void dump_rotating();
  void set_rotating(RotatingSecrets& secrets);
  bool need_rotating_secrets();
  bool get_service_secret(uint32_t service_id, uint64_t secret_id, CryptoKey& secret);

  void encode(bufferlist& bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(keys, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(keys, bl);
  }
};
WRITE_CLASS_ENCODER(KeyRing)

extern KeyRing g_keyring;



#endif
