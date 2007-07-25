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
#include "include/encodable.h"

// sent from replica to auth

class MMDSCacheRejoin : public Message {
 public:
  static const int OP_WEAK    = 1;  // replica -> auth, i exist, + maybe open files.
  static const int OP_STRONG  = 2;  // replica -> auth, i exist, + open files and lock state.
  static const int OP_ACK     = 3;  // auth -> replica, here is your lock state.
  static const int OP_PURGE   = 4;  // auth -> replica, remove these items, they are old/obsolete.
  static const int OP_MISSING = 5;  // auth -> replica, i am missing these items
  static const int OP_FULL    = 6;  // replica -> auth, here is the full object.
  static const char *get_opname(int op) {
    switch (op) {
    case OP_WEAK: return "weak";
    case OP_STRONG: return "strong";
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
    __int32_t dirlock;
    inode_strong() {}
    inode_strong(int n, int cw=0, int a=0, int l=0, int dft=0, int f=0, int dl=0) : 
      caps_wanted(cw),
      nonce(n),
      authlock(a), linklock(l), dirfragtreelock(dft), filelock(f), dirlock(dl) { }
  };
  struct inode_full {
    inode_t inode;
    string symlink;
    fragtree_t dirfragtree;
    inode_full() {}
    inode_full(const inode_t& i, const string& s, const fragtree_t& f) :
      inode(i), symlink(s), dirfragtree(f) {}

    void _decode(bufferlist& bl, int& off) {
      ::_decode(inode, bl, off);
      ::_decode(symlink, bl, off);
      ::_decode(dirfragtree, bl, off);
    }
    void _encode(bufferlist& bl) const {
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
    unsigned char d_type;
    inodeno_t ino;
    inodeno_t remote_ino;
    int32_t nonce;
    int32_t lock;
    dn_strong() : d_type(0), ino(0), remote_ino(0), nonce(0), lock(0) {}
    dn_strong(unsigned char dt, inodeno_t pi, inodeno_t ri, int n, int l) : 
      d_type(dt), ino(pi), remote_ino(ri), nonce(n), lock(l) {}
    bool is_primary() { return ino > 0; }
    bool is_remote() { return remote_ino > 0; }
    bool is_null() { return ino == 0 && remote_ino == 0; }
  };

  struct dn_weak {
    unsigned char d_type;
    inodeno_t ino;
    inodeno_t remote_ino;
    dn_weak() : d_type(0), ino(0), remote_ino(0) {}
    dn_weak(unsigned char dt, inodeno_t pi, inodeno_t ri) : d_type(dt), ino(pi), remote_ino(ri) {}
    bool is_primary() { return ino > 0; }
    bool is_remote() { return remote_ino > 0; }
    bool is_null() { return ino == 0 && remote_ino == 0; }
  };

  // -- data --
  int32_t op;

  // weak
  map<dirfrag_t, map<string, dn_weak> > weak;
  set<inodeno_t> weak_inodes;

  // strong
  map<dirfrag_t, dirfrag_strong> strong_dirfrags;
  map<dirfrag_t, map<string, dn_strong> > strong_dentries;
  map<inodeno_t, inode_strong> strong_inodes;

  // open
  bufferlist cap_export_bl;
  map<inodeno_t,map<int, inode_caps_reconnect_t> > cap_exports;
  map<inodeno_t,string> cap_export_paths;

  // full
  list<inode_full> full_inodes;

  // authpins, xlocks
  map<inodeno_t, metareqid_t> authpinned_inodes;
  map<inodeno_t, map<int, metareqid_t> > xlocked_inodes;
  map<dirfrag_t, map<string, metareqid_t> > authpinned_dentries;
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
  void add_weak_inode(inodeno_t i) {
    weak_inodes.insert(i);
  }
  void add_strong_inode(inodeno_t i, int n, int cw, int a, int l, int dft, int f, int dl) {
    strong_inodes[i] = inode_strong(n, cw, a, l, dft, f, dl);
  }
  void add_full_inode(inode_t &i, const string& s, const fragtree_t &f) {
    full_inodes.push_back(inode_full(i, s, f));
  }
  void add_inode_authpin(inodeno_t ino, const metareqid_t& ri) {
    authpinned_inodes[ino] = ri;
  }
  void add_inode_xlock(inodeno_t ino, int lt, const metareqid_t& ri) {
    xlocked_inodes[ino][lt] = ri;
  }

  void copy_cap_exports(bufferlist &bl) {
    cap_export_bl = bl;
  }
  
  // dirfrags
  void add_weak_dirfrag(dirfrag_t df) {
    weak[df];
  }
  void add_weak_dirfrag(dirfrag_t df, map<string,dn_weak>& dnmap) {
    weak[df] = dnmap;
  }
  void add_strong_dirfrag(dirfrag_t df, int n) {
    strong_dirfrags[df] = dirfrag_strong(n);
  }
   
  // dentries
  void add_weak_dentry(dirfrag_t df, const string& dname, dn_weak& dnw) {
    weak[df][dname] = dnw;
  }
  void add_weak_null_dentry(dirfrag_t df, const string& dname) {
    weak[df][dname] = dn_weak(0, 0, 0);
  }
  void add_weak_primary_dentry(dirfrag_t df, const string& dname, unsigned char dt, inodeno_t ino) {
    weak[df][dname] = dn_weak(dt, ino, 0);
  }
  void add_weak_remote_dentry(dirfrag_t df, const string& dname, unsigned char dt, inodeno_t ino) {
    weak[df][dname] = dn_weak(dt, 0, ino);
  }
  void add_strong_dentry(dirfrag_t df, const string& dname, unsigned char dt, inodeno_t pi, inodeno_t ri, int n, int ls) {
    strong_dentries[df][dname] = dn_strong(dt, pi, ri, n, ls);
  }
  void add_dentry_authpin(dirfrag_t df, const string& dname, const metareqid_t& ri) {
    authpinned_dentries[df][dname] = ri;
  }
  void add_dentry_xlock(dirfrag_t df, const string& dname, const metareqid_t& ri) {
    xlocked_dentries[df][dname] = ri;
  }

  // -- encoding --
  void encode_payload() {
    ::_encode(op, payload);
    ::_encode(strong_inodes, payload);
    ::_encode_complex(full_inodes, payload);
    ::_encode(authpinned_inodes, payload);
    ::_encode(xlocked_inodes, payload);
    ::_encode(cap_export_bl, payload);
    ::_encode(strong_dirfrags, payload);
    ::_encode(weak, payload);
    ::_encode(weak_inodes, payload);
    ::_encode(strong_dentries, payload);
    ::_encode(authpinned_dentries, payload);
    ::_encode(xlocked_dentries, payload);
  }
  void decode_payload() {
    int off = 0;
    ::_decode(op, payload, off);
    ::_decode(strong_inodes, payload, off);
    ::_decode_complex(full_inodes, payload, off);
    ::_decode(authpinned_inodes, payload, off);
    ::_decode(xlocked_inodes, payload, off);
    ::_decode(cap_export_bl, payload, off);
    if (cap_export_bl.length()) {
      int off = 0;
      ::_decode(cap_exports, cap_export_bl, off);
      ::_decode(cap_export_paths, cap_export_bl, off);
    }
    ::_decode(strong_dirfrags, payload, off);
    ::_decode(weak, payload, off);
    ::_decode(weak_inodes, payload, off);
    ::_decode(strong_dentries, payload, off);
    ::_decode(authpinned_dentries, payload, off);
    ::_decode(xlocked_dentries, payload, off);
  }

};

#endif
