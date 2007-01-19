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


#ifndef __MHASHDIR_H
#define __MHASHDIR_H

#include "msg/Message.h"

class MHashDir : public Message {
  inodeno_t ino;
  bufferlist state;
  int nden;
  
 public:  
  MHashDir() {}
  MHashDir(inodeno_t ino) : 
    Message(MSG_MDS_HASHDIR) {
    this->ino = ino;
    nden = 0;
  }
  virtual char *get_type_name() { return "Ha"; }

  inodeno_t get_ino() { return ino; }
  bufferlist& get_state() { return state; }
  bufferlist* get_state_ptr() { return &state; }
  int       get_nden() { return nden; }
  
  void set_nden(int n) { nden = n; }
  void inc_nden() { nden++; }

  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);
    payload.copy(off, sizeof(nden), (char*)&nden);
    off += sizeof(nden);

    size_t len;
    payload.copy(off, sizeof(len), (char*)&len);
    off += sizeof(len);
    state.substr_of(payload, off, len);
  }
  void encode_payload() {
    payload.append((char*)&ino, sizeof(ino));
    payload.append((char*)&nden, sizeof(nden));
    size_t size = state.length();
    payload.append((char*)&size, sizeof(size));
    payload.claim_append(state);
  }

};

#endif
