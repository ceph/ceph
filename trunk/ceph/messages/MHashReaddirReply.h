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


#ifndef __MHASHREADDIRREPLY_H
#define __MHASHREADDIRREPLY_H

#include "MClientReply.h"

class MHashReaddirReply : public Message {
  inodeno_t ino;

  list<InodeStat*> dir_in;
  list<string>     dir_dn;
  
  int num;

 public:
  MHashReaddirReply() { }
  MHashReaddirReply(inodeno_t _ino, list<InodeStat*>& inls, list<string>& dnls, int n) :
    Message(MSG_MDS_HASHREADDIRREPLY),
    ino(_ino),
    num(n) {
    dir_in.swap(inls);
    dir_dn.swap(dnls);
  }
  ~MHashReaddirReply() {
    for (list<InodeStat*>::iterator it = dir_in.begin(); it != dir_in.end(); it++) 
      delete *it;
  }

  inodeno_t get_ino() { return ino; }
  list<InodeStat*>& get_in() { return dir_in; }
  list<string>& get_dn() { return dir_dn; }

  virtual char *get_type_name() { return "Hls"; }

  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);
    int n;
    payload.copy(n, sizeof(n), (char*)&n);
    off += sizeof(n);
    for (int i=0; i<n; i++) {
      string dn;
      ::_decode(dn, payload, off);
      dir_dn.push_back(dn);

      InodeStat *ci = new InodeStat;
      ci->_decode(payload, off);
      dir_in.push_back(ci);
    }
  }
  virtual void encode_payload() {
    payload.append((char*)&ino, sizeof(ino));
    int n = dir_in.size();                           // FIXME?
    payload.append((char*)&n, sizeof(n));
    list<string>::iterator pdn = dir_dn.begin();
    for (list<InodeStat*>::iterator pin = dir_in.begin(); 
         pin != dir_in.end(); 
         ++pin, ++pdn) {
      ::_encode(*pdn, payload);
      (*pin)->_encode(payload);
    }
  }

};

#endif
