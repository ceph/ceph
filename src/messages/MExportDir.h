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


#ifndef __MEXPORTDIR_H
#define __MEXPORTDIR_H

#include "msg/Message.h"


class MExportDir : public Message {
  dirfrag_t dirfrag;
  
  bufferlist dirstate;
  list<dirfrag_t> bounds;

 public:  
  MExportDir() {}
  MExportDir(dirfrag_t df) : 
    Message(MSG_MDS_EXPORTDIR),
    dirfrag(df) {
  }
  const char *get_type_name() { return "Ex"; }
  void print(ostream& o) {
    o << "export(" << dirfrag << ")";
  }

  dirfrag_t get_dirfrag() { return dirfrag; }
  bufferlist& get_dirstate() { return dirstate; }
  list<dirfrag_t>& get_bounds() { return bounds; }

  void take_dirstate(bufferlist& bl) {
    dirstate.claim(bl);
  }
  void add_export(dirfrag_t df) { 
    bounds.push_back(df); 
  }

  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(dirfrag), (char*)&dirfrag);
    off += sizeof(dirfrag);
    ::_decode(bounds, payload, off);
    ::_decode(dirstate, payload, off);
  }
  virtual void encode_payload() {
    payload.append((char*)&dirfrag, sizeof(dirfrag));
    ::_encode(bounds, payload);
    ::_encode(dirstate, payload);
  }

};

#endif
