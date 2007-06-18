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

#ifndef __MMDSCACHEREJOINACK_H
#define __MMDSCACHEREJOINACK_H

#include "msg/Message.h"

#include "include/types.h"

// sent from auth back to replica

class MMDSCacheRejoinAck : public Message {
 public:
  struct inodeinfo { 
    inodeno_t ino;
    int authlock;
    int linklock;
    int dirfragtreelock;
    int filelock;
    int nonce;
    inodeinfo() {}
    inodeinfo(inodeno_t i, int a, int l, int dft, int f, int n) : 
      ino(i), 
      authlock(a), linklock(l), dirfragtreelock(dft), filelock(f), 
      nonce(n) {}
  };
  struct dninfo {
    int lock;
    int nonce;
    dninfo() {}
    dninfo(int l, int n) : lock(l), nonce(n) {}
  };
  struct dirinfo {
    dirfrag_t dirfrag;
    int nonce;
    dirinfo() {}
    dirinfo(dirfrag_t df, int n) : dirfrag(df), nonce(n) {}
  };
  list<inodeinfo> inodes; 
  map<dirfrag_t, map<string,dninfo> > dentries;
  list<dirinfo> dirfrags;

  MMDSCacheRejoinAck() : Message(MSG_MDS_CACHEREJOINACK) {}

  char *get_type_name() { return "cache_rejoin_ack"; }

  void add_dirfrag(dirfrag_t dirfrag, int nonce) {
    dirfrags.push_back(dirinfo(dirfrag,nonce));
  }
  void add_dentry(dirfrag_t dirfrag, const string& dn, int ls, int nonce) {
    dentries[dirfrag][dn] = dninfo(ls, nonce);
  }
  void add_inode(inodeno_t ino, int authl, int linkl, int dftl, int fl, int nonce) {
    inodes.push_back(inodeinfo(ino, authl, linkl, dftl, fl, nonce));
  }
  
  void encode_payload() {
    ::_encode(inodes, payload);
    ::_encode(dirfrags, payload);
    for (list<dirinfo>::iterator p = dirfrags.begin(); p != dirfrags.end(); ++p)
      ::_encode(dentries[p->dirfrag], payload);
  }
  void decode_payload() {
    int off = 0;
    ::_decode(inodes, payload, off);
    ::_decode(dirfrags, payload, off);
    for (list<dirinfo>::iterator p = dirfrags.begin(); p != dirfrags.end(); ++p)
      ::_decode(dentries[p->dirfrag], payload, off);
  }
};

#endif
