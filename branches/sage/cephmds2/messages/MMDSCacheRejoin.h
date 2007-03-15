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

#ifndef __MMDSCACHEREJOIN_H
#define __MMDSCACHEREJOIN_H

#include "msg/Message.h"

#include "include/types.h"

// sent from replica to auth

class MMDSCacheRejoin : public Message {
 public:
  map<inodeno_t,int> inodes; // ino -> caps_wanted
  set<dirfrag_t> dirfrags;
  map<dirfrag_t, set<string> > dentries;   // dir -> (dentries...)

  MMDSCacheRejoin() : Message(MSG_MDS_CACHEREJOIN) {}

  char *get_type_name() { return "cache_rejoin"; }

  void add_dirfrag(dirfrag_t dirfrag) {
    dirfrags.insert(dirfrag);
  }
  void add_dentry(dirfrag_t dirfrag, const string& dn) {
    dentries[dirfrag].insert(dn);
  }
  void add_inode(inodeno_t ino, int cw) {
    inodes[ino] = cw;
  }
  
  void encode_payload() {
    ::_encode(inodes, payload);
    ::_encode(dirfrags, payload);
    for (set<dirfrag_t>::iterator p = dirfrags.begin(); p != dirfrags.end(); ++p)
      ::_encode(dentries[*p], payload);
  }
  void decode_payload() {
    int off = 0;
    ::_decode(inodes, payload, off);
    ::_decode(dirfrags, payload, off);
    for (set<dirfrag_t>::iterator p = dirfrags.begin(); p != dirfrags.end(); ++p)
      ::_decode(dentries[*p], payload, off);
  }
};

#endif
