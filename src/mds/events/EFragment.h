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
  list<frag_t> orig_frags;

  EFragment() : LogEvent(EVENT_FRAGMENT) { }
  EFragment(MDLog *mdlog, int o, inodeno_t i, frag_t bf, int b) : 
    LogEvent(EVENT_FRAGMENT), metablob(mdlog), 
    op(o), ino(i), basefrag(bf), bits(b) { }

  void print(ostream& out) const {
    out << "EFragment " << op_name(op) << " " << ino << " " << basefrag << " by " << bits << " " << metablob;
  }

  enum {
    OP_PREPARE = 1,
    OP_COMMIT = 2,
    OP_ROLLBACK = 3,
    OP_FINISH = 4, // finish deleting orphan dirfrags
    OP_ONESHOT = 5,  // (legacy) PREPARE+COMMIT
  };
  static const char *op_name(int o) {
    switch (o) {
    case OP_PREPARE: return "prepare";
    case OP_COMMIT: return "commit";
    case OP_ROLLBACK: return "rollback";
    case OP_FINISH: return "finish";
    default: return "???";
    }
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<EFragment*>& ls);
  void replay(MDS *mds);
};

#endif
