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

#ifndef __MMDSCACHEREJOIN_H
#define __MMDSCACHEREJOIN_H

#include "msg/Message.h"

#include "include/types.h"

// sent from replica to auth

class MMDSCacheRejoin : public Message {
 public:
  static const int OP_REJOIN  = 1;  // replica -> auth, i exist.  and maybe my lock state.
  static const int OP_ACK     = 3;  // auth -> replica, here is your lock state.
  static const int OP_MISSING = 4;  // auth -> replica, i am missing these items
  static const int OP_FULL    = 5;  // replica -> auth, here is the full object.
  static const char *get_opname(int op) {
    switch (op) {
    case OP_REJOIN: return "rejoin";
    case OP_ACK: return "ack";
    case OP_MISSING: return "missing";
    case OP_FULL: return "full";
    default: assert(0);
    }
  }

  // -- types --
  struct inode_strong { 
    int32_t caps_wanted;
    int32_t nonce;
    int32_t authlock;
    int32_t linklock;
    int32_t dirfragtreelock;
    int32_t filelock;
    inode_strong() {}
    inode_strong(int n, int cw=0, int a=0, int l=0, int dft=0, int f=0) : 
      caps_wanted(cw),
      nonce(n),
      authlock(a), linklock(l), dirfragtreelock(dft), filelock(f) { }
  };
  struct inode_full {
    inode_t inode;
    string symlink;
    fragtree_t dirfragtree;
    inode_full() {}
    inode_full(const inode_t& i, const string& s, const fragtree_t& f) :
      inode(i), symlink(s), dirfragtree(f) {}
    inode_full(bufferlist& bl, int& off) {
      ::_decode(inode, bl, off);
      ::_decode(symlink, bl, off);
      ::_decode(dirfragtree, bl, off);
    }
    void _encode(bufferlist& bl) {
      ::_encode(inode, bl);
      ::_encode(symlink, bl);
      ::_encode(dirfragtree, bl);
    }
  };

  struct dirfrag_strong {
    int32_t nonce;
    dirfrag_strong() {}
    dirfrag_strong(int n) : nonce(n) {}
  };
  struct dn_strong {
    int32_t nonce;
    int32_t lock;
    dn_strong() {}
    dn_strong(int n, int l) : nonce(n), lock(l) {}
  };

  // -- data --
  int32_t op;

  set<inodeno_t> weak_inodes;
  map<inodeno_t, inode_strong> strong_inodes;
  list<inode_full> full_inodes;
  map<inodeno_t, map<int, metareqid_t> > xlocked_inodes;

  set<dirfrag_t> weak_dirfrags;
  map<dirfrag_t, dirfrag_strong> strong_dirfrags;

  map<dirfrag_t, set<string> > weak_dentries;
  map<dirfrag_t, map<string, dn_strong> > strong_dentries;
  map<dirfrag_t, map<string, metareqid_t> > xlocked_dentries;

  MMDSCacheRejoin() : Message(MSG_MDS_CACHEREJOIN) {}
  MMDSCacheRejoin(int o) : 
    Message(MSG_MDS_CACHEREJOIN),
    op(o) {}

  char *get_type_name() { return "cache_rejoin"; }
  void print(ostream& out) {
    out << "cache_rejoin " << get_opname(op);
  }

  // -- builders --
  // inodes
  void add_weak_inode(inodeno_t ino) {
    weak_inodes.insert(ino);
  }
  void add_strong_inode(inodeno_t i, int n, int cw, int a, int l, int dft, int f) {
    strong_inodes[i] = inode_strong(n, cw, a, l, dft, f);
  }
  void add_full_inode(inode_t &i, const string& s, const fragtree_t &f) {
    full_inodes.push_back(inode_full(i, s, f));
  }
  void add_inode_xlock(inodeno_t ino, int lt, const metareqid_t& ri) {
    xlocked_inodes[ino][lt] = ri;
  }
  
  // dirfrags
  void add_weak_dirfrag(dirfrag_t df) {
    weak_dirfrags.insert(df);
  }
  void add_strong_dirfrag(dirfrag_t df, int n) {
    strong_dirfrags[df] = dirfrag_strong(n);
  }
   
  // dentries
  void add_weak_dentry(dirfrag_t df, const string& dname) {
    weak_dentries[df].insert(dname);
  }
  void add_strong_dentry(dirfrag_t df, const string& dname, int n, int ls) {
    strong_dentries[df][dname] = dn_strong(n, ls);
  }
  void add_dentry_xlock(dirfrag_t df, const string& dname, const metareqid_t& ri) {
    xlocked_dentries[df][dname] = ri;
  }

  // -- encoding --
  void encode_payload() {
    ::_encode(op, payload);
    ::_encode(weak_inodes, payload);
    ::_encode(strong_inodes, payload);

    uint32_t nfull = full_inodes.size();
    ::_encode(nfull, payload);
    for (list<inode_full>::iterator p = full_inodes.begin(); p != full_inodes.end(); ++p)
      p->_encode(payload);

    ::_encode(xlocked_inodes, payload);
    ::_encode(weak_dirfrags, payload);
    ::_encode(strong_dirfrags, payload);
    ::_encode(weak_dentries, payload);
    ::_encode(strong_dentries, payload);
    ::_encode(xlocked_dentries, payload);
  }
  void decode_payload() {
    int off = 0;
    ::_decode(op, payload, off);
    ::_decode(weak_inodes, payload, off);
    ::_decode(strong_inodes, payload, off);

    uint32_t nfull;
    ::_decode(nfull, payload, off);
    for (unsigned i=0; i<nfull; i++) 
      full_inodes.push_back(inode_full(payload, off));

    ::_decode(xlocked_inodes, payload, off);
    ::_decode(weak_dirfrags, payload, off);
    ::_decode(strong_dirfrags, payload, off);
    ::_decode(weak_dentries, payload, off);
    ::_decode(strong_dentries, payload, off);
    ::_decode(xlocked_dentries, payload, off);
  }

};

#endif
