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


#ifndef __MUNHASHDIRNOTIFY_H
#define __MUNHASHDIRNOTIFY_H

#include "msg/Message.h"

class MUnhashDirNotify : public Message {
  inodeno_t ino;
  //int peer;

 public:
  inodeno_t get_ino() { return ino; }
  //int get_peer() { return peer; }

  MUnhashDirNotify() {}
  MUnhashDirNotify(inodeno_t ino/*, int peer*/) :
    Message(MSG_MDS_UNHASHDIRNOTIFY) {
    this->ino = ino;
    //this->peer = peer;
  }  
  virtual char *get_type_name() { return "UHN"; }
  
  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);
    //payload.copy(off, sizeof(peer), (char*)&peer);
    //off += sizeof(peer);
  }
  virtual void encode_payload() {
    payload.append((char*)&ino, sizeof(ino));
    //payload.append((char*)&peer, sizeof(peer));
  }

};

#endif
