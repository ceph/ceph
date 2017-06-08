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


#ifndef CEPH_MEXPORTDIRPREP_H
#define CEPH_MEXPORTDIRPREP_H

#include "msg/Message.h"
#include "include/types.h"

class MExportDirPrep : public Message {
  dirfrag_t dirfrag;
 public:
  bufferlist basedir;
  list<dirfrag_t> bounds;
  list<bufferlist> traces;
private:
  set<mds_rank_t> bystanders;
  bool b_did_assim;

public:
  dirfrag_t get_dirfrag() { return dirfrag; }
  list<dirfrag_t>& get_bounds() { return bounds; }
  set<mds_rank_t> &get_bystanders() { return bystanders; }

  bool did_assim() { return b_did_assim; }
  void mark_assim() { b_did_assim = true; }

  MExportDirPrep() {
    b_did_assim = false;
  }
  MExportDirPrep(dirfrag_t df, uint64_t tid) :
    Message(MSG_MDS_EXPORTDIRPREP),
    dirfrag(df), b_did_assim(false) {
    set_tid(tid);
  }
private:
  ~MExportDirPrep() {}

public:
  const char *get_type_name() const { return "ExP"; }
  void print(ostream& o) const {
    o << "export_prep(" << dirfrag << ")";
  }

  void add_bound(dirfrag_t df) {
    bounds.push_back( df );
  }
  void add_trace(bufferlist& bl) {
    traces.push_back(bl);
  }
  void add_bystander(mds_rank_t who) {
    bystanders.insert(who);
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(dirfrag, p);
    ::decode(basedir, p);
    ::decode(bounds, p);
    ::decode(traces, p);
    ::decode(bystanders, p);
  }

  void encode_payload(uint64_t features) {
    ::encode(dirfrag, payload);
    ::encode(basedir, payload);
    ::encode(bounds, payload);
    ::encode(traces, payload);
    ::encode(bystanders, payload);
  }
};

#endif
