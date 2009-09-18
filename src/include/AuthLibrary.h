// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef __AUTHLIBRARY_H
#define __AUTHLIBRARY_H

#include "include/types.h"
#include "include/encoding.h"
#include "auth/Auth.h"
#include "auth/KeysServer.h"

struct AuthLibEntry {
  EntityName name;
  CryptoKey secret;

  void encode(bufferlist& bl) const {
    ::encode(name, bl);
    ::encode(secret, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(name, bl);
    ::decode(secret, bl);
  }
};
WRITE_CLASS_ENCODER(AuthLibEntry)

typedef enum {
  AUTH_INC_NOP,
  AUTH_INC_ADD,
  AUTH_INC_DEL,
  AUTH_INC_ACTIVATE,
} AuthLibIncOp;

struct AuthLibIncremental {
   AuthLibIncOp op;
   bufferlist info;

  void encode(bufferlist& bl) const {
    __u32 _op = (__u32)op;
    ::encode(_op, bl);
    ::encode(info, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u32 _op;
    ::decode(_op, bl);
    op = (AuthLibIncOp)_op;
    assert( op >= AUTH_INC_NOP && op <= AUTH_INC_ACTIVATE);
    ::decode(info, bl);
  }

  void decode_entry(AuthLibEntry& e) {
     bufferlist::iterator iter = info.begin();
     ::decode(e, iter);
  }
};
WRITE_CLASS_ENCODER(AuthLibIncremental)

struct AuthLibrary {
  version_t version;
  KeysServerData keys;

  AuthLibrary() : version(0) {}

  void add(const EntityName& name, CryptoKey& secret) {
    keys.add_secret(name, secret);
  }

  void add(AuthLibEntry& entry) {
    add(entry.name, entry.secret);
  }

  void remove(const EntityName& name) {
    keys.remove_secret(name);
  }

  bool contains(EntityName& name) {
    return keys.contains(name);
  }
  void encode(bufferlist& bl) const {
    ::encode(version, bl);
    ::encode(keys, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(version, bl);
    ::decode(keys, bl);
  }
};
WRITE_CLASS_ENCODER(AuthLibrary)

inline ostream& operator<<(ostream& out, const AuthLibEntry& e)
{
  return out << e.name.to_str();
}

#endif
