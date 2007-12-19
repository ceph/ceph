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


// anchor ops
#define ANCHOR_OP_LOOKUP          1
#define ANCHOR_OP_LOOKUP_REPLY    -2

#define ANCHOR_OP_CREATE_PREPARE  11
#define ANCHOR_OP_CREATE_AGREE    -12

#define ANCHOR_OP_DESTROY_PREPARE 21
#define ANCHOR_OP_DESTROY_AGREE   -22

#define ANCHOR_OP_UPDATE_PREPARE  31
#define ANCHOR_OP_UPDATE_AGREE    -32

#define ANCHOR_OP_COMMIT   41
#define ANCHOR_OP_ACK      -42
#define ANCHOR_OP_ROLLBACK 43



inline const char* get_anchor_opname(int o) {
  switch (o) {
  case ANCHOR_OP_LOOKUP: return "lookup";
  case ANCHOR_OP_LOOKUP_REPLY: return "lookup_reply";

  case ANCHOR_OP_CREATE_PREPARE: return "create_prepare";
  case ANCHOR_OP_CREATE_AGREE: return "create_agree";
  case ANCHOR_OP_DESTROY_PREPARE: return "destroy_prepare";
  case ANCHOR_OP_DESTROY_AGREE: return "destroy_agree";
  case ANCHOR_OP_UPDATE_PREPARE: return "update_prepare";
  case ANCHOR_OP_UPDATE_AGREE: return "update_agree";

  case ANCHOR_OP_COMMIT: return "commit";
  case ANCHOR_OP_ACK: return "ack";
  case ANCHOR_OP_ROLLBACK: return "rollback";
  default: assert(0); return 0;
  }
}


// identifies a anchor table mutation



// anchor type

class Anchor {
public:
  inodeno_t ino;      // anchored ino
  dirfrag_t dirfrag;  // containing dirfrag
  //string    ref_dn;   // referring dentry
  int       nref;     // reference count

  Anchor() {}
  Anchor(inodeno_t i, dirfrag_t df, 
	 //string& rd, 
	 int nr=0) :
    ino(i), dirfrag(df),
    //ref_dn(rd), 
    nref(nr) { }
  
  void _encode(bufferlist &bl) {
    bl.append((char*)&ino, sizeof(ino));
    bl.append((char*)&dirfrag, sizeof(dirfrag));
    bl.append((char*)&nref, sizeof(nref));
    //::_encode(ref_dn, bl);
  }
  void _decode(bufferlist& bl, int& off) {
    bl.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);
    bl.copy(off, sizeof(dirfrag), (char*)&dirfrag);
    off += sizeof(dirfrag);
    bl.copy(off, sizeof(nref), (char*)&nref);
    off += sizeof(nref);
    //::_decode(ref_dn, bl, off);
  }
};

inline ostream& operator<<(ostream& out, Anchor& a)
{
  return out << "a(" << a.ino << " " << a.dirfrag << " " << a.nref << ")";
}

#endif
