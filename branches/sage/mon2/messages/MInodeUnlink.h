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


#ifndef __MINODEUNLINK_H
#define __MINODEUNLINK_H

typedef struct {
  inodeno_t ino;
  int from;
} MInodeUnlink_st;

class MInodeUnlink : public Message {
  MInodeUnlink_st st;

 public:
  inodeno_t get_ino() { return st.ino; }
  int get_from() { return st.from; }

  MInodeUnlink() {}
  MInodeUnlink(inodeno_t ino, int from) :
    Message(MSG_MDS_INODEUNLINK) {
    st.ino = ino;
    st.from = from;
  }
  virtual char *get_type_name() { return "InUl";}
  
  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(st), (char*)&st);
    off += sizeof(st);
  }
  virtual void encode_payload() {
    payload.append((char*)&st,sizeof(st));
  }
};

#endif
