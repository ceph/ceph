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

#ifndef __MEXPORTDIRNOTIFY_H
#define __MEXPORTDIRNOTIFY_H

#include "msg/Message.h"
#include <string>
using namespace std;

class MExportDirNotify : public Message {
  inodeno_t ino;
  bool ack;
  pair<int,int> old_auth, new_auth;
  list<inodeno_t> exports;  // bounds; these dirs are _not_ included (tho the inodes are)

  //list<inodeno_t> subdirs;

 public:
  inodeno_t get_ino() { return ino; }
  pair<int,int> get_old_auth() { return old_auth; }
  pair<int,int> get_new_auth() { return new_auth; }
  bool wants_ack() { return ack; }
  list<inodeno_t>& get_exports() { return exports; }
  //list<inodeno_t>::iterator subdirs_begin() { return subdirs.begin(); }
  //list<inodeno_t>::iterator subdirs_end() { return subdirs.end(); }
  //int num_subdirs() { return subdirs.size(); }

  MExportDirNotify() {}
  MExportDirNotify(inodeno_t i, bool a, pair<int,int> oa, pair<int,int> na) :
    Message(MSG_MDS_EXPORTDIRNOTIFY),
    ino(i), ack(a), old_auth(oa), new_auth(na) { }
  
  virtual char *get_type_name() { return "ExNot"; }
  void print(ostream& o) {
    o << "export_notify(" << ino;
    o << " " << old_auth << " -> " << new_auth;
    if (ack) 
      o << " ack)";
    else
      o << " no ack)";
  }
  
  /*
  void copy_subdirs(list<inodeno_t>& s) {
    this->subdirs = s;
  }
  */
  void copy_exports(list<inodeno_t>& ex) {
    this->exports = ex;
  }
  void copy_exports(set<inodeno_t>& ex) {
    for (set<inodeno_t>::iterator i = ex.begin();
	 i != ex.end(); ++i)
      exports.push_back(*i);
  }
  void copy_exports(set<CDir*>& ex) {
    for (set<CDir*>::iterator i = ex.begin();
	 i != ex.end(); ++i)
      exports.push_back((*i)->ino());
  }

  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);
    payload.copy(off, sizeof(ack), (char*)&ack);
    off += sizeof(ack);
    payload.copy(off, sizeof(old_auth), (char*)&old_auth);
    off += sizeof(old_auth);
    payload.copy(off, sizeof(new_auth), (char*)&new_auth);
    off += sizeof(new_auth);
    ::_decode(exports, payload, off);
    //::_decode(subdirs, payload, off);
  }
  virtual void encode_payload() {
    payload.append((char*)&ino, sizeof(ino));
    payload.append((char*)&ack, sizeof(ack));
    payload.append((char*)&old_auth, sizeof(old_auth));
    payload.append((char*)&new_auth, sizeof(new_auth));
    ::_encode(exports, payload);
    //::_encode(subdirs, payload);
  }
};

#endif
