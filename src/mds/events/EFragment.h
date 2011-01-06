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

#ifndef CEPH_MDS_EFRAGMENT_H
#define CEPH_MDS_EFRAGMENT_H

#include "../LogEvent.h"
#include "EMetaBlob.h"

class EFragment : public LogEvent {
public:
  EMetaBlob metablob;
  __u8 op;
  inodeno_t ino;
  frag_t basefrag;
  __s32 bits;         // positive for split (from basefrag), negative for merge (to basefrag)

  EFragment() : LogEvent(EVENT_FRAGMENT) { }
  EFragment(MDLog *mdlog, int o, inodeno_t i, frag_t bf, int b) : 
    LogEvent(EVENT_FRAGMENT), metablob(mdlog), 
    op(o), ino(i), basefrag(bf), bits(b) { }
  void print(ostream& out) {
    out << "EFragment " << op_name(op) << " " << ino << " " << basefrag << " by " << bits << " " << metablob;
  }

  enum {
    OP_PREPARE = 1,
    OP_COMMIT = 2,
    OP_ROLLBACK = 3,
    OP_ONESHOT = 4,  // (legacy) PREPARE+COMMIT
  };
  const char *op_name(int o) {
    switch (o) {
    case OP_PREPARE: return "prepare";
    case OP_COMMIT: return "commit";
    case OP_ROLLBACK: return "rollback";
    default: return "???";
    }
  }

  void encode(bufferlist &bl) const {
    __u8 struct_v = 3;
    ::encode(struct_v, bl);
    ::encode(stamp, bl);
    ::encode(op, bl);
    ::encode(ino, bl);
    ::encode(basefrag, bl);
    ::encode(bits, bl);
    ::encode(metablob, bl);
  } 
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    if (struct_v >= 2)
      ::decode(stamp, bl);
    if (struct_v >= 3)
      ::decode(op, bl);
    else
      op = OP_ONESHOT;
    ::decode(ino, bl);
    ::decode(basefrag, bl);
    ::decode(bits, bl);
    ::decode(metablob, bl);
  }

  void replay(MDS *mds);
};

#endif
