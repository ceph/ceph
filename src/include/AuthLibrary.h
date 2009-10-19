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
  bool rotating;

  EntityName name;
  EntityAuth auth;

  bufferlist rotating_bl;

  void encode(bufferlist& bl) const {
    __s32 r = (__s32)rotating;
    ::encode(r, bl); 
    if (!rotating) {
      ::encode(name, bl);
      ::encode(auth, bl);
    } else {
      ::encode(rotating_bl, bl);
    }
  }
  void decode(bufferlist::iterator& bl) {
    __s32 r;
    ::decode(r, bl);
    rotating = (bool)r;
    if (!rotating) {
      ::decode(name, bl);
      ::decode(auth, bl);
    } else {
      ::decode(rotating_bl, bl);
    }
  }

  AuthLibEntry() : rotating(false) {}
};
WRITE_CLASS_ENCODER(AuthLibEntry)

typedef enum {
  AUTH_INC_NOP,
  AUTH_INC_ADD,
  AUTH_INC_DEL,
  AUTH_INC_SET_ROTATING,
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
    assert( op >= AUTH_INC_NOP && op <= AUTH_INC_SET_ROTATING);
    ::decode(info, bl);
  }

  void decode_entry(AuthLibEntry& e) {
     bufferlist::iterator iter = info.begin();
     ::decode(e, iter);
  }
};
WRITE_CLASS_ENCODER(AuthLibIncremental)

inline ostream& operator<<(ostream& out, const AuthLibEntry& e)
{
  return out << e.name.to_str();
}

#endif
