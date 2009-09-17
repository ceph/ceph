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
  map<EntityName, AuthLibEntry> library_map;

  AuthLibrary() : version(0) {}

  void add(const EntityName& name, CryptoKey& secret) {
    AuthLibEntry entry;
    entry.name = name;
    entry.secret = secret;
    add(entry);
  }

  void add(AuthLibEntry& entry) {
    library_map[entry.name] = entry;
  }

  void remove(const EntityName& name) {
    map<EntityName, AuthLibEntry>::iterator mapiter = library_map.find(name);
    if (mapiter == library_map.end())
      return;
    library_map.erase(mapiter);
  }

  bool contains(EntityName& name) {
    return (library_map.find(name) != library_map.end());
  }
  void encode(bufferlist& bl) const {
    ::encode(version, bl);
    ::encode(library_map, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(version, bl);
    ::decode(library_map, bl);
  }
};
WRITE_CLASS_ENCODER(AuthLibrary)

inline ostream& operator<<(ostream& out, const AuthLibEntry& e)
{
  return out << e.name.to_str();
}

#endif
