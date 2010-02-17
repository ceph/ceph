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

class KeyRing {
  map<EntityName, EntityAuth> keys;

public:
  map<EntityName, EntityAuth>& get_keys() { return keys; }  // yuck

  bool load(const char *filename);
  void print(ostream& out);

  // accessors
  bool get_auth(EntityName& name, EntityAuth &a) {
    if (keys.count(name)) {
      a = keys[name];
      return true;
    }
    return false;
  }
  bool get_secret(EntityName& name, CryptoKey& secret) {
    if (keys.count(name)) {
      secret = keys[name].key;
      return true;
    }
    return false;
  }
  void get_master(CryptoKey& dest) {
    get_secret(*g_conf.entity_name, dest);
  }

  // modifiers
  void add(const EntityName& name, EntityAuth &a) {
    keys[name] = a;
  }
  void set_caps(EntityName& name, map<string, bufferlist>& caps) {
    keys[name].caps = caps;
  }
  void import(KeyRing& other);

  // encoders
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
