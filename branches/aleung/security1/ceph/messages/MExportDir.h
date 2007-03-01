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
  
  list<bufferlist> dirstate; // a bl for reach dir
  list<inodeno_t>  exports;

 public:  
  MExportDir() {}
  MExportDir(inodeno_t dirino) : 
    Message(MSG_MDS_EXPORTDIR),
    ino(dirino) {
  }
  virtual char *get_type_name() { return "Ex"; }

  inodeno_t get_ino() { return ino; }
  list<bufferlist>& get_dirstate() { return dirstate; }
  list<inodeno_t>& get_exports() { return exports; }

  void add_dir(bufferlist& dir) {
    dirstate.push_back(dir);
  }
  void set_dirstate(const list<bufferlist>& ls) {
    dirstate = ls;
  }
  void add_export(inodeno_t dirino) { 
    exports.push_back(dirino); 
  }

  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);
    ::_decode(exports, payload, off);
    ::_decode(dirstate, payload, off);
  }
  virtual void encode_payload() {
    payload.append((char*)&ino, sizeof(ino));
    ::_encode(exports, payload);
    ::_encode(dirstate, payload);
  }

};

#endif
