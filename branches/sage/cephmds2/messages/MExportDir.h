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


#ifndef __MEXPORTDIR_H
#define __MEXPORTDIR_H

#include "msg/Message.h"


class MExportDir : public Message {
  inodeno_t ino;
  
  int         ndirs;
  bufferlist  state;
  
  list<inodeno_t> exports;

  // hashed pre-discovers
  //map<inodeno_t, set<string> > hashed_prediscover;

 public:  
  MExportDir() {}
  MExportDir(CInode *in) : 
    Message(MSG_MDS_EXPORTDIR) {
    this->ino = in->inode.ino;
    ndirs = 0;
  }
  virtual char *get_type_name() { return "Ex"; }

  inodeno_t get_ino() { return ino; }
  int get_ndirs() { return ndirs; }
  bufferlist& get_state() { return state; }
  list<inodeno_t>& get_exports() { return exports; }
  
  void add_dir(bufferlist& dir) {
    state.claim_append( dir );
    ndirs++;
  }
  void add_export(CDir *dir) { 
    exports.push_back(dir->ino()); 
  }

  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);
    payload.copy(off, sizeof(ndirs), (char*)&ndirs);
    off += sizeof(ndirs);
    ::_decode(exports, payload, off);
    ::_decode(state, payload, off);
  }
  virtual void encode_payload() {
    payload.append((char*)&ino, sizeof(ino));
    payload.append((char*)&ndirs, sizeof(ndirs));
    ::_encode(exports, payload);
    ::_encode(state, payload);
  }

};

#endif
