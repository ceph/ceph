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


#ifndef __MEXPORTDIRPREP_H
#define __MEXPORTDIRPREP_H

#include "msg/Message.h"
#include "include/types.h"

class MExportDirPrep : public Message {
  dirfrag_t dirfrag;
 public:
  bufferlist basedir;
  list<dirfrag_t> bounds;
  list<bufferlist> traces;
private:
  set<__s32> bystanders;
  bool b_did_assim;

public:
  dirfrag_t get_dirfrag() { return dirfrag; }
  list<dirfrag_t>& get_bounds() { return bounds; }
  set<__s32> &get_bystanders() { return bystanders; }

  bool did_assim() { return b_did_assim; }
  void mark_assim() { b_did_assim = true; }

  MExportDirPrep() {
    b_did_assim = false;
  }
  MExportDirPrep(dirfrag_t df) : 
    Message(MSG_MDS_EXPORTDIRPREP),
    dirfrag(df),
    b_did_assim(false) { }


  const char *get_type_name() { return "ExP"; }
  void print(ostream& o) {
    o << "export_prep(" << dirfrag << ")";
  }

  void add_bound(dirfrag_t df) {
    bounds.push_back( df );
  }
  void add_trace(bufferlist& bl) {
    traces.push_back(bl);
  }
  void add_bystander(int who) {
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

  void encode_payload() {
    ::encode(dirfrag, payload);
    ::encode(basedir, payload);
    ::encode(bounds, payload);
    ::encode(traces, payload);
    ::encode(bystanders, payload);
  }
};

#endif
