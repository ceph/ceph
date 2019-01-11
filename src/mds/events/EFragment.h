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

struct dirfrag_rollback {
  fnode_t fnode;
  dirfrag_rollback() { }
  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& bl);
};
WRITE_CLASS_ENCODER(dirfrag_rollback)

class EFragment : public LogEvent {
public:
  EMetaBlob metablob;
  __u8 op{0};
  inodeno_t ino;
  frag_t basefrag;
  __s32 bits{0};         // positive for split (from basefrag), negative for merge (to basefrag)
  frag_vec_t orig_frags;
  bufferlist rollback;

  EFragment() : LogEvent(EVENT_FRAGMENT) { }
  EFragment(MDLog *mdlog, int o, dirfrag_t df, int b) :
    LogEvent(EVENT_FRAGMENT),
    op(o), ino(df.ino), basefrag(df.frag), bits(b) { }

  void print(ostream& out) const override {
    out << "EFragment " << op_name(op) << " " << ino << " " << basefrag << " by " << bits << " " << metablob;
  }

  enum {
    OP_PREPARE = 1,
    OP_COMMIT = 2,
    OP_ROLLBACK = 3,
    OP_FINISH = 4 // finish deleting orphan dirfrags
  };
  static std::string_view op_name(int o) {
    switch (o) {
    case OP_PREPARE: return "prepare";
    case OP_COMMIT: return "commit";
    case OP_ROLLBACK: return "rollback";
    case OP_FINISH: return "finish";
    default: return "???";
    }
  }

  void add_orig_frag(frag_t df, dirfrag_rollback *drb=NULL) {
    using ceph::encode;
    orig_frags.push_back(df);
    if (drb)
      encode(*drb, rollback);
  }

  EMetaBlob *get_metablob() override { return &metablob; }

  void encode(bufferlist &bl, uint64_t features) const override;
  void decode(bufferlist::const_iterator &bl) override;
  void dump(Formatter *f) const override;
  static void generate_test_instances(list<EFragment*>& ls);
  void replay(MDSRank *mds) override;
};
WRITE_CLASS_ENCODER_FEATURES(EFragment)

#endif
