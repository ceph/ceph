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
  int       new_auth;
  int       old_auth;
  inodeno_t ino;
  
  list<inodeno_t> exports;  // bounds; these dirs are _not_ included (tho the inodes are)
  list<inodeno_t> subdirs;

 public:
  inodeno_t get_ino() { return ino; }
  int get_new_auth() { return new_auth; }
  int get_old_auth() { return old_auth; }
  list<inodeno_t>& get_exports() { return exports; }
  list<inodeno_t>::iterator subdirs_begin() { return subdirs.begin(); }
  list<inodeno_t>::iterator subdirs_end() { return subdirs.end(); }
  int num_subdirs() { return subdirs.size(); }

  MExportDirNotify() {}
  MExportDirNotify(inodeno_t ino, int old_auth, int new_auth) :
    Message(MSG_MDS_EXPORTDIRNOTIFY) {
    this->ino = ino;
    this->old_auth = old_auth;
    this->new_auth = new_auth;
  }
  virtual char *get_type_name() { return "ExNot"; }
  
  void copy_subdirs(list<inodeno_t>& s) {
    this->subdirs = s;
  }
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
    payload.copy(off, sizeof(int), (char*)&new_auth);
    off += sizeof(int);
    payload.copy(off, sizeof(int), (char*)&old_auth);
    off += sizeof(int);
    payload.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);
    ::_decode(exports, payload, off);
    ::_decode(subdirs, payload, off);
  }
  virtual void encode_payload() {
    payload.append((char*)&new_auth, sizeof(int));
    payload.append((char*)&old_auth, sizeof(int));
    payload.append((char*)&ino, sizeof(ino));
    ::_encode(exports, payload);
    ::_encode(subdirs, payload);
  }
};

#endif
