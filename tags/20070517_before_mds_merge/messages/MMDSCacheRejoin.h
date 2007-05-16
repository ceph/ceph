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
  set<inodeno_t> dirs;
  map<inodeno_t, set<string> > dentries;   // dir -> (dentries...)

  MMDSCacheRejoin() : Message(MSG_MDS_CACHEREJOIN) {}

  char *get_type_name() { return "cache_rejoin"; }

  void print(ostream& out) {
    out << "cache_rejoin" << endl;
  }

  void add_dir(inodeno_t dirino) {
    dirs.insert(dirino);
  }
  void add_dentry(inodeno_t dirino, const string& dn) {
    dentries[dirino].insert(dn);
  }
  void add_inode(inodeno_t ino, int cw) {
    inodes[ino] = cw;
  }
  
  void encode_payload() {
    ::_encode(inodes, payload);
    ::_encode(dirs, payload);
    for (set<inodeno_t>::iterator p = dirs.begin(); p != dirs.end(); ++p)
      ::_encode(dentries[*p], payload);
  }
  void decode_payload() {
    int off = 0;
    ::_decode(inodes, payload, off);
    ::_decode(dirs, payload, off);
    for (set<inodeno_t>::iterator p = dirs.begin(); p != dirs.end(); ++p)
      ::_decode(dentries[*p], payload, off);
  }
};

#endif
