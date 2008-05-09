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
  static const int OP_WEAK    = 1;  // replica -> auth, i exist, + maybe open files.
  static const int OP_STRONG  = 2;  // replica -> auth, i exist, + open files and lock state.
  static const int OP_ACK     = 3;  // auth -> replica, here is your lock state.
  //static const int OP_PURGE   = 4;  // auth -> replica, remove these items, they are old/obsolete.
  static const int OP_MISSING = 5;  // auth -> replica, i am missing these items
  static const int OP_FULL    = 6;  // replica -> auth, here is the full object.
  static const char *get_opname(int op) {
    switch (op) {
    case OP_WEAK: return "weak";
    case OP_STRONG: return "strong";
    case OP_ACK: return "ack";
    case OP_MISSING: return "missing";
    case OP_FULL: return "full";
    default: assert(0); return 0;
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
    int32_t dirlock;
    inode_strong() {}
    inode_strong(int n, int cw=0, int a=0, int l=0, int dft=0, int f=0, int dl=0) : 
      caps_wanted(cw),
      nonce(n),
      authlock(a), linklock(l), dirfragtreelock(dft), filelock(f), dirlock(dl) { }
    void encode(bufferlist &bl) const {
      ::encode(caps_wanted, bl);
      ::encode(nonce, bl);
      ::encode(authlock, bl);
      ::encode(linklock, bl);
      ::encode(dirfragtreelock, bl);
      ::encode(filelock, bl);
      ::encode(dirlock, bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(caps_wanted, bl);
      ::decode(nonce, bl);
      ::decode(authlock, bl);
      ::decode(linklock, bl);
      ::decode(dirfragtreelock, bl);
      ::decode(filelock, bl);
      ::decode(dirlock, bl);
    }
  };
  WRITE_CLASS_ENCODERS(inode_strong)

  struct inode_full {
    inode_t inode;
    string symlink;
    fragtree_t dirfragtree;
    inode_full() {}
    inode_full(const inode_t& i, const string& s, const fragtree_t& f) :
      inode(i), symlink(s), dirfragtree(f) {}

    void decode(bufferlist::iterator& p) {
      ::decode(inode, p);
      ::decode(symlink, p);
      ::decode(dirfragtree, p);
    }
    void encode(bufferlist& bl) const {
      ::encode(inode, bl);
      ::encode(symlink, bl);
      ::encode(dirfragtree, bl);
    }
  };
  WRITE_CLASS_ENCODERS(inode_full)

  struct dirfrag_strong {
    int32_t nonce;
    int8_t  dir_rep;
    dirfrag_strong() {}
    dirfrag_strong(int n, int dr) : nonce(n), dir_rep(dr) {}
    void encode(bufferlist &bl) const {
      ::encode(nonce, bl);
      ::encode(dir_rep, bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(nonce, bl);
      ::decode(dir_rep, bl);
    }
  };
  WRITE_CLASS_ENCODERS(dirfrag_strong)

  struct dn_strong {
    inodeno_t ino;
    inodeno_t remote_ino;
    unsigned char remote_d_type;
    int32_t nonce;
    int32_t lock;
    dn_strong() : 
      ino(0), remote_ino(0), remote_d_type(0), nonce(0), lock(0) {}
    dn_strong(inodeno_t pi, inodeno_t ri, unsigned char rdt, int n, int l) : 
      ino(pi), remote_ino(ri), remote_d_type(rdt), nonce(n), lock(l) {}
    bool is_primary() { return ino > 0; }
    bool is_remote() { return remote_ino > 0; }
    bool is_null() { return ino == 0 && remote_ino == 0; }
    void encode(bufferlist &bl) const {
      ::encode(ino, bl);
      ::encode(remote_ino, bl);
      ::encode(remote_d_type, bl);
      ::encode(nonce, bl);
      ::encode(lock, bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(ino, bl);
      ::decode(remote_ino, bl);
      ::decode(remote_d_type, bl);
      ::decode(nonce, bl);
      ::decode(lock, bl);
    }
  };
  WRITE_CLASS_ENCODERS(dn_strong)

  struct dn_weak {
    inodeno_t ino;
    dn_weak() : ino(0) {}
    dn_weak(inodeno_t pi) : ino(pi) {}
    void encode(bufferlist &bl) const {
      ::encode(ino, bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(ino, bl);
    }
  };
  WRITE_CLASS_ENCODERS(dn_weak)

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
  map<inodeno_t,map<__s32, inode_caps_reconnect_t> > cap_exports;
  map<inodeno_t,string> cap_export_paths;

  // full
  list<inode_full> full_inodes;

  // authpins, xlocks
  map<inodeno_t, metareqid_t> authpinned_inodes;
  map<inodeno_t, map<__s32, metareqid_t> > xlocked_inodes;
  map<dirfrag_t, map<string, metareqid_t> > authpinned_dentries;
  map<dirfrag_t, map<string, metareqid_t> > xlocked_dentries;

  MMDSCacheRejoin() : Message(MSG_MDS_CACHEREJOIN) {}
  MMDSCacheRejoin(int o) : 
    Message(MSG_MDS_CACHEREJOIN),
    op(o) {}

  const char *get_type_name() { return "cache_rejoin"; }
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
  void add_strong_dirfrag(dirfrag_t df, int n, int dr) {
    strong_dirfrags[df] = dirfrag_strong(n, dr);
  }
   
  // dentries
  void add_weak_dentry(dirfrag_t df, const string& dname, dn_weak& dnw) {
    weak[df][dname] = dnw;
  }
  void add_weak_primary_dentry(dirfrag_t df, const string& dname, inodeno_t ino) {
    weak[df][dname] = dn_weak(ino);
  }
  void add_strong_dentry(dirfrag_t df, const string& dname, inodeno_t pi, inodeno_t ri, unsigned char rdt, int n, int ls) {
    strong_dentries[df][dname] = dn_strong(pi, ri, rdt, n, ls);
  }
  void add_dentry_authpin(dirfrag_t df, const string& dname, const metareqid_t& ri) {
    authpinned_dentries[df][dname] = ri;
  }
  void add_dentry_xlock(dirfrag_t df, const string& dname, const metareqid_t& ri) {
    xlocked_dentries[df][dname] = ri;
  }

  // -- encoding --
  void encode_payload() {
    ::encode(op, payload);
    ::encode(strong_inodes, payload);
    ::encode(full_inodes, payload);
    ::encode(authpinned_inodes, payload);
    ::encode(xlocked_inodes, payload);
    ::encode(cap_export_bl, payload);
    ::encode(strong_dirfrags, payload);
    ::encode(weak, payload);
    ::encode(weak_inodes, payload);
    ::encode(strong_dentries, payload);
    ::encode(authpinned_dentries, payload);
    ::encode(xlocked_dentries, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(op, p);
    ::decode(strong_inodes, p);
    ::decode(full_inodes, p);
    ::decode(authpinned_inodes, p);
    ::decode(xlocked_inodes, p);
    ::decode(cap_export_bl, p);
    if (cap_export_bl.length()) {
      bufferlist::iterator q = cap_export_bl.begin();
      ::decode(cap_exports, q);
      ::decode(cap_export_paths, q);
    }
    ::decode(strong_dirfrags, p);
    ::decode(weak, p);
    ::decode(weak_inodes, p);
    ::decode(strong_dentries, p);
    ::decode(authpinned_dentries, p);
    ::decode(xlocked_dentries, p);
  }

};

WRITE_CLASS_ENCODERS(MMDSCacheRejoin::inode_strong)
WRITE_CLASS_ENCODERS(MMDSCacheRejoin::inode_full)
WRITE_CLASS_ENCODERS(MMDSCacheRejoin::dirfrag_strong)
WRITE_CLASS_ENCODERS(MMDSCacheRejoin::dn_strong)
WRITE_CLASS_ENCODERS(MMDSCacheRejoin::dn_weak)

#endif
