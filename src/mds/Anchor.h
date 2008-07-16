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

#ifndef __ANCHOR_H
#define __ANCHOR_H

#include <string>
using std::string;

#include "include/types.h"
#include "mdstypes.h"
#include "include/buffer.h"

enum {
  ANCHOR_OP_CREATE,
  ANCHOR_OP_DESTROY,
  ANCHOR_OP_UPDATE,
};

inline const char* get_anchor_opname(int o) {
  switch (o) {
  case ANCHOR_OP_CREATE: return "create";
  case ANCHOR_OP_DESTROY: return "destroy";
  case ANCHOR_OP_UPDATE: return "update";
  default: assert(0); return 0;
  }
}


// identifies a anchor table mutation



// anchor type

class Anchor {
public:
  inodeno_t ino;      // anchored ino
  inodeno_t dirino;
  __u32     dn_hash;
  int       nref;     // reference count

  Anchor() : dn_hash(0), nref(0) {}
  Anchor(inodeno_t i, inodeno_t di, __u32 hash, int nr=0) :
    ino(i), dirino(di), dn_hash(hash), nref(nr) { }
  Anchor(inodeno_t i, inodeno_t di, const nstring &dname, int nr=0) :
    ino(i), dirino(di),
    dn_hash(ceph_full_name_hash(dname.data(), dname.length())),
    nref(nr) { }
  
  void encode(bufferlist &bl) const {
    ::encode(ino, bl);
    ::encode(dirino, bl);
    ::encode(dn_hash, bl);
    ::encode(nref, bl);
  }
  void decode(bufferlist::iterator &bl) {
    ::decode(ino, bl);
    ::decode(dirino, bl);
    ::decode(dn_hash, bl);
    ::decode(nref, bl);
  }
};
WRITE_CLASS_ENCODER(Anchor)

inline ostream& operator<<(ostream& out, const Anchor &a)
{
  return out << "a(" << a.ino << " " << a.dirino << "/" << a.dn_hash << " " << a.nref << ")";
}

#endif
