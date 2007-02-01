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


#ifndef __MHASHDIRDISCOVER_H
#define __MHASHDIRDISCOVER_H

#include "msg/Message.h"
#include "mds/CInode.h"
#include "include/types.h"

class MHashDirDiscover : public Message {
  inodeno_t ino;
  string path;

 public:
  inodeno_t get_ino() { return ino; }
  string& get_path() { return path; }

  MHashDirDiscover() {}
  MHashDirDiscover(CInode *in) : 
    Message(MSG_MDS_HASHDIRDISCOVER) {
    in->make_path(path);
    ino = in->ino();
  }
  virtual char *get_type_name() { return "HDis"; }


  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);
    _decode(path, payload, off);
  }

  void encode_payload() {
    payload.append((char*)&ino, sizeof(ino));
    _encode(path, payload);
  }
};

#endif
