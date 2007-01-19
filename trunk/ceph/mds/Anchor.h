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
#include "include/buffer.h"

class Anchor {
public:
  inodeno_t ino;      // my ino
  inodeno_t dirino;   // containing dir
  string    ref_dn;   // referring dentry
  int       nref;     // reference count

  Anchor() {}
  Anchor(inodeno_t ino, inodeno_t dirino, string& ref_dn, int nref=0) {
    this->ino = ino;
    this->dirino = dirino;
    this->ref_dn = ref_dn;
    this->nref = nref;
  }  
  
  void _encode(bufferlist &bl) {
    bl.append((char*)&ino, sizeof(ino));
    bl.append((char*)&dirino, sizeof(dirino));
    bl.append((char*)&nref, sizeof(nref));
    ::_encode(ref_dn, bl);
  }
  void _decode(bufferlist& bl, int& off) {
    bl.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);
    bl.copy(off, sizeof(dirino), (char*)&dirino);
    off += sizeof(dirino);
    bl.copy(off, sizeof(nref), (char*)&nref);
    off += sizeof(nref);
    ::_decode(ref_dn, bl, off);
  }
} ;

#endif
