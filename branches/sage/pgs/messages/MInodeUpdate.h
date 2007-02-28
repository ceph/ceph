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


#ifndef __MINODEUPDATE_H
#define __MINODEUPDATE_H

#include "msg/Message.h"

#include <set>
using namespace std;

class MInodeUpdate : public Message {
  int nonce;
  crope inode_basic_state;

 public:
  inodeno_t get_ino() { 
    inodeno_t ino;
    inode_basic_state.copy(0, sizeof(inodeno_t), (char*)&ino);
    return ino;
  }
  int get_nonce() { return nonce; }
  
  MInodeUpdate() {}
  MInodeUpdate(CInode *in, int nonce) :
    Message(MSG_MDS_INODEUPDATE) {
    inode_basic_state = in->encode_basic_state();
    this->nonce = nonce;
  }
  virtual char *get_type_name() { return "Iup"; }

  virtual void decode_payload(crope& s, int& off) {
    s.copy(off, sizeof(int), (char*)&nonce);
    off += sizeof(int);
    size_t len;
    s.copy(off, sizeof(len), (char*)&len);
    off += sizeof(len);
    inode_basic_state = s.substr(off, len);
    off += len;
  }
  virtual void encode_payload(crope& s) {
    s.append((char*)&nonce, sizeof(int));
    size_t len = inode_basic_state.length();
    s.append((char*)&len, sizeof(len));
    s.append(inode_basic_state);
  }
      
};

#endif
