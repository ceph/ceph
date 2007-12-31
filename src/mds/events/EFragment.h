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

#ifndef __MDS_EFRAGMENT_H
#define __MDS_EFRAGMENT_H

#include "../LogEvent.h"
#include "EMetaBlob.h"

class EFragment : public LogEvent {
public:
  EMetaBlob metablob;
  inodeno_t ino;
  frag_t basefrag;
  int bits;         // positive for split (from basefrag), negative for merge (to basefrag)

  EFragment() : LogEvent(EVENT_FRAGMENT) { }
  EFragment(MDLog *mdlog, inodeno_t i, frag_t bf, int b) : 
    LogEvent(EVENT_FRAGMENT), metablob(mdlog), 
    ino(i), basefrag(bf), bits(b) { }
  void print(ostream& out) {
    out << "EFragment " << ino << " " << basefrag << " by " << bits << " " << metablob;
  }

  void encode_payload(bufferlist& bl) {
    ::_encode(ino, bl);
    ::_encode(basefrag, bl);
    ::_encode(bits, bl);
    metablob._encode(bl);
  } 
  void decode_payload(bufferlist& bl, int& off) {
    ::_decode(ino, bl, off);
    ::_decode(basefrag, bl, off);
    ::_decode(bits, bl, off);
    metablob._decode(bl, off);
  }

  void replay(MDS *mds);
};

#endif
