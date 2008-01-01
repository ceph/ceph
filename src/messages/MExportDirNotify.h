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

#ifndef __MEXPORTDIRNOTIFY_H
#define __MEXPORTDIRNOTIFY_H

#include "msg/Message.h"
#include <string>
using namespace std;

class MExportDirNotify : public Message {
  dirfrag_t base;
  bool ack;
  pair<int,int> old_auth, new_auth;
  list<dirfrag_t> bounds;  // bounds; these dirs are _not_ included (tho the dirfragdes are)

 public:
  dirfrag_t get_dirfrag() { return base; }
  pair<int,int> get_old_auth() { return old_auth; }
  pair<int,int> get_new_auth() { return new_auth; }
  bool wants_ack() { return ack; }
  list<dirfrag_t>& get_bounds() { return bounds; }

  MExportDirNotify() {}
  MExportDirNotify(dirfrag_t i, bool a, pair<int,int> oa, pair<int,int> na) :
    Message(MSG_MDS_EXPORTDIRNOTIFY),
    base(i), ack(a), old_auth(oa), new_auth(na) { }
  
  const char *get_type_name() { return "ExNot"; }
  void print(ostream& o) {
    o << "export_notify(" << base;
    o << " " << old_auth << " -> " << new_auth;
    if (ack) 
      o << " ack)";
    else
      o << " no ack)";
  }
  
  void copy_bounds(list<dirfrag_t>& ex) {
    this->bounds = ex;
  }
  void copy_bounds(set<dirfrag_t>& ex) {
    for (set<dirfrag_t>::iterator i = ex.begin();
	 i != ex.end(); ++i)
      bounds.push_back(*i);
  }
  void copy_bounds(set<CDir*>& ex) {
    for (set<CDir*>::iterator i = ex.begin();
	 i != ex.end(); ++i)
      bounds.push_back((*i)->dirfrag());
  }

  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(base), (char*)&base);
    off += sizeof(base);
    payload.copy(off, sizeof(ack), (char*)&ack);
    off += sizeof(ack);
    payload.copy(off, sizeof(old_auth), (char*)&old_auth);
    off += sizeof(old_auth);
    payload.copy(off, sizeof(new_auth), (char*)&new_auth);
    off += sizeof(new_auth);
    ::_decode(bounds, payload, off);
  }
  virtual void encode_payload() {
    payload.append((char*)&base, sizeof(base));
    payload.append((char*)&ack, sizeof(ack));
    payload.append((char*)&old_auth, sizeof(old_auth));
    payload.append((char*)&new_auth, sizeof(new_auth));
    ::_encode(bounds, payload);
  }
};

#endif
