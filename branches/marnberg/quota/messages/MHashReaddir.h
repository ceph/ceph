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


#ifndef __MHASHREADDIR_H
#define __MHASHREADDIR_H

#include "include/types.h"
#include "msg/Message.h"

class MHashReaddir : public Message {
  inodeno_t ino;

 public:
  MHashReaddir() { }
  MHashReaddir(inodeno_t ino) :
    Message(MSG_MDS_HASHREADDIR) {
    this->ino = ino;
  }

  inodeno_t get_ino() { return ino; }

  virtual char *get_type_name() { return "Hls"; }

  virtual void decode_payload() {
    payload.copy(0, sizeof(ino), (char*)&ino);
  }
  virtual void encode_payload() {
    payload.append((char*)&ino, sizeof(ino));
  }

};

#endif
