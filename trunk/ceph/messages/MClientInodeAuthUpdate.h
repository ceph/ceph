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


#ifndef __MCLIENTINODEAUTHUPDATE_H
#define __MCLIENTINODEAUTHUPDATE_H

class MClientInodeAuthUpdate : public Message {
  inodeno_t ino;
  int       newauth;

 public:
  inodeno_t get_ino() { return ino; }
  int       get_auth() { return newauth; }

  MClientInodeAuthUpdate() {}
  MClientInodeAuthUpdate(inodeno_t ino, int newauth) :
    Message(MSG_CLIENT_INODEAUTHUPDATE) {
    this->ino = ino;
    this->newauth = newauth;
  }
  virtual char *get_type_name() { return "Ciau";}
  
  virtual void decode_payload(crope& s, int& off) {
    s.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);
    s.copy(off, sizeof(newauth), (char*)&newauth);
    off += sizeof(newauth);
  }
  virtual void encode_payload(crope& s) {
    s.append((char*)&ino,sizeof(ino));
    s.append((char*)&newauth,sizeof(newauth));
  }
};

#endif
