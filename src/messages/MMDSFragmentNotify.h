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

#ifndef CEPH_MMDSFRAGMENTNOTIFY_H
#define CEPH_MMDSFRAGMENTNOTIFY_H

#include "msg/Message.h"

class MMDSFragmentNotify : public Message {
  inodeno_t ino;
  frag_t basefrag;
  int8_t bits;

 public:
  inodeno_t get_ino() { return ino; }
  frag_t get_basefrag() { return basefrag; }
  int get_bits() { return bits; }

  bufferlist basebl;

  MMDSFragmentNotify() : Message(MSG_MDS_FRAGMENTNOTIFY) {}
  MMDSFragmentNotify(dirfrag_t df, int b) :
	Message(MSG_MDS_FRAGMENTNOTIFY),
    ino(df.ino), basefrag(df.frag), bits(b) { }
private:
  ~MMDSFragmentNotify() {}

public:  
  const char *get_type_name() const { return "fragment_notify"; }
  void print(ostream& o) const {
    o << "fragment_notify(" << ino << "." << basefrag
      << " " << (int)bits << ")";
  }

  void encode_payload(uint64_t features) {
    ::encode(ino, payload);
    ::encode(basefrag, payload);
    ::encode(bits, payload);
    ::encode(basebl, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(ino, p);
    ::decode(basefrag, p);
    ::decode(bits, p);
    ::decode(basebl, p);
  }
  
};
REGISTER_MESSAGE(MMDSFragmentNotify, MSG_MDS_FRAGMENTNOTIFY);
#endif
