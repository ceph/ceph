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


#ifndef __MINODELINKACK_H
#define __MINODELINKACK_H

typedef struct {
  inodeno_t ino;
  bool success;
} MInodeLinkAck_st;

class MInodeLinkAck : public Message {
  MInodeLinkAck_st st;

 public:
  inodeno_t get_ino() { return st.ino; }
  bool is_success() { return st.success; }

  MInodeLinkAck() {}
  MInodeLinkAck(inodeno_t ino, bool success) :
    Message(MSG_MDS_INODELINKACK) {
    st.ino = ino;
    st.success = success;
  }
  virtual char *get_type_name() { return "InLA";}
  
  virtual void decode_payload(crope& s, int& off) {
    s.copy(off, sizeof(st), (char*)&st);
    off += sizeof(st);
  }
  virtual void encode_payload(crope& s) {
    s.append((char*)&st,sizeof(st));
  }
};

#endif
