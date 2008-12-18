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
  pair<__s32,__s32> old_auth, new_auth;
  list<dirfrag_t> bounds;  // bounds; these dirs are _not_ included (tho the dirfragdes are)

 public:
  dirfrag_t get_dirfrag() { return base; }
  pair<__s32,__s32> get_old_auth() { return old_auth; }
  pair<__s32,__s32> get_new_auth() { return new_auth; }
  bool wants_ack() { return ack; }
  list<dirfrag_t>& get_bounds() { return bounds; }

  MExportDirNotify() {}
  MExportDirNotify(dirfrag_t i, bool a, pair<__s32,__s32> oa, pair<__s32,__s32> na) :
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

  void encode_payload() {
    ::encode(base, payload);
    ::encode(ack, payload);
    ::encode(old_auth, payload);
    ::encode(new_auth, payload);
    ::encode(bounds, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(base, p);
    ::decode(ack, p);
    ::decode(old_auth, p);
    ::decode(new_auth, p);
    ::decode(bounds, p);
  }
};

#endif
