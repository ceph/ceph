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

#ifndef __MMDSCACHEREJOINACK_H
#define __MMDSCACHEREJOINACK_H

#include "msg/Message.h"

#include "include/types.h"

// sent from auth back to replica

class MMDSCacheRejoinAck : public Message {
 public:
  struct inodeinfo { 
    inodeno_t ino;
    int hardlock;
    int filelock;
    int nonce;
    inodeinfo() {}
    inodeinfo(inodeno_t i, int h, int f, int n) : ino(i), hardlock(h), filelock(f), nonce(n) {}
  };
  struct dninfo {
    int lock;
    int nonce;
    dninfo() {}
    dninfo(int l, int n) : lock(l), nonce(n) {}
  };
  struct dirinfo {
    inodeno_t dirino;
    int nonce;
    dirinfo() {}
    dirinfo(inodeno_t i, int n) : dirino(i), nonce(n) {}
  };
  list<inodeinfo> inodes; 
  map<inodeno_t, map<string,dninfo> > dentries;
  list<dirinfo> dirs;

  MMDSCacheRejoinAck() : Message(MSG_MDS_CACHEREJOINACK) {}

  char *get_type_name() { return "cache_rejoin_ack"; }

  void print(ostream& out) {
    out << "cache_rejoin" << endl;
  }

  void add_dir(inodeno_t dirino, int nonce) {
    dirs.push_back(dirinfo(dirino,nonce));
  }
  void add_dentry(inodeno_t dirino, const string& dn, int ls, int nonce) {
    dentries[dirino][dn] = dninfo(ls, nonce);
  }
  void add_inode(inodeno_t ino, int hl, int fl, int nonce) {
    inodes.push_back(inodeinfo(ino, hl, fl, nonce));
  }
  
  void encode_payload() {
    ::_encode(inodes, payload);
    ::_encode(dirs, payload);
    for (list<dirinfo>::iterator p = dirs.begin(); p != dirs.end(); ++p)
      ::_encode(dentries[p->dirino], payload);
  }
  void decode_payload() {
    int off = 0;
    ::_decode(inodes, payload, off);
    ::_decode(dirs, payload, off);
    for (list<dirinfo>::iterator p = dirs.begin(); p != dirs.end(); ++p)
      ::_decode(dentries[p->dirino], payload, off);
 }
};

#endif
