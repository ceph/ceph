// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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
#define ANCHOR_OP_LOOKUP_REPLY    2
#define ANCHOR_OP_CREATE_PREPARE  3
#define ANCHOR_OP_CREATE_ACK      4
#define ANCHOR_OP_CREATE_COMMIT   5
#define ANCHOR_OP_DESTROY_PREPARE 6
#define ANCHOR_OP_DESTROY_ACK     7
#define ANCHOR_OP_DESTROY_COMMIT  8
#define ANCHOR_OP_UPDATE_PREPARE  9
#define ANCHOR_OP_UPDATE_ACK      10
#define ANCHOR_OP_UPDATE_COMMIT   11

inline const char* get_anchor_opname(int o) {
  switch (o) {
  case ANCHOR_OP_LOOKUP: return "lookup";
  case ANCHOR_OP_LOOKUP_REPLY: return "lookup_reply";
  case ANCHOR_OP_CREATE_PREPARE: return "create_prepare";
  case ANCHOR_OP_CREATE_ACK: return "create_ack";
  case ANCHOR_OP_CREATE_COMMIT: return "create_commit";
  case ANCHOR_OP_DESTROY_PREPARE: return "destroy_prepare";
  case ANCHOR_OP_DESTROY_ACK: return "destroy_ack";
  case ANCHOR_OP_DESTROY_COMMIT: return "destroy_commit";
  case ANCHOR_OP_UPDATE_PREPARE: return "update_prepare";
  case ANCHOR_OP_UPDATE_ACK: return "update_ack";
  case ANCHOR_OP_UPDATE_COMMIT: return "update_commit";
  default: assert(0); 
  }
}


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
