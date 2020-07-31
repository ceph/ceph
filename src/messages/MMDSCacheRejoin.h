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

#ifndef CEPH_MMDSCACHEREJOIN_H
#define CEPH_MMDSCACHEREJOIN_H

#include <string_view>
#include "include/types.h"
#include "mds/CInode.h"
#include "mds/CDir.h"
#include "mds/mdstypes.h"
#include "messages/MMDSOp.h"

// sent from replica to auth

class MMDSCacheRejoin : public MMDSOp {
public:
  static constexpr int OP_WEAK    = 1;  // replica -> auth, i exist, + maybe open files.
  static constexpr int OP_STRONG  = 2;  // replica -> auth, i exist, + open files and lock state.
  static constexpr int OP_ACK     = 3;  // auth -> replica, here is your lock state.
  static const char *get_opname(int op) {
    switch (op) {
    case OP_WEAK: return "weak";
    case OP_STRONG: return "strong";
    case OP_ACK: return "ack";
    default: ceph_abort(); return 0;
    }
  }

  // -- types --
  struct inode_strong { 
    uint32_t nonce = 0;
    int32_t caps_wanted = 0;
    int32_t filelock = 0, nestlock = 0, dftlock = 0;
    inode_strong() {}
    inode_strong(int n, int cw, int dl, int nl, int dftl) :
      nonce(n), caps_wanted(cw),
      filelock(dl), nestlock(nl), dftlock(dftl) { }
    void encode(ceph::buffer::list &bl) const {
      using ceph::encode;
      encode(nonce, bl);
      encode(caps_wanted, bl);
      encode(filelock, bl);
      encode(nestlock, bl);
      encode(dftlock, bl);
    }
    void decode(ceph::buffer::list::const_iterator &bl) {
      using ceph::decode;
      decode(nonce, bl);
      decode(caps_wanted, bl);
      decode(filelock, bl);
      decode(nestlock, bl);
      decode(dftlock, bl);
    }
  };
  WRITE_CLASS_ENCODER(inode_strong)

  struct dirfrag_strong {
    uint32_t nonce = 0;
    int8_t  dir_rep = 0;
    dirfrag_strong() {}
    dirfrag_strong(int n, int dr) : nonce(n), dir_rep(dr) {}
    void encode(ceph::buffer::list &bl) const {
      using ceph::encode;
      encode(nonce, bl);
      encode(dir_rep, bl);
    }
    void decode(ceph::buffer::list::const_iterator &bl) {
      using ceph::decode;
      decode(nonce, bl);
      decode(dir_rep, bl);
    }
  };
  WRITE_CLASS_ENCODER(dirfrag_strong)

  struct dn_strong {
    snapid_t first;
    inodeno_t ino;
    inodeno_t remote_ino;
    unsigned char remote_d_type;
    uint32_t nonce;
    int32_t lock;
    dn_strong() : 
      ino(0), remote_ino(0), remote_d_type(0), nonce(0), lock(0) {}
    dn_strong(snapid_t f, inodeno_t pi, inodeno_t ri, unsigned char rdt, int n, int l) : 
      first(f), ino(pi), remote_ino(ri), remote_d_type(rdt), nonce(n), lock(l) {}
    bool is_primary() const { return ino > 0; }
    bool is_remote() const { return remote_ino > 0; }
    bool is_null() const { return ino == 0 && remote_ino == 0; }
    void encode(ceph::buffer::list &bl) const {
      using ceph::encode;
      encode(first, bl);
      encode(ino, bl);
      encode(remote_ino, bl);
      encode(remote_d_type, bl);
      encode(nonce, bl);
      encode(lock, bl);
    }
    void decode(ceph::buffer::list::const_iterator &bl) {
      using ceph::decode;
      decode(first, bl);
      decode(ino, bl);
      decode(remote_ino, bl);
      decode(remote_d_type, bl);
      decode(nonce, bl);
      decode(lock, bl);
    }
  };
  WRITE_CLASS_ENCODER(dn_strong)

  struct dn_weak {
    snapid_t first;
    inodeno_t ino;
    dn_weak() : ino(0) {}
    dn_weak(snapid_t f, inodeno_t pi) : first(f), ino(pi) {}
    void encode(ceph::buffer::list &bl) const {
      using ceph::encode;
      encode(first, bl);
      encode(ino, bl);
    }
    void decode(ceph::buffer::list::const_iterator &bl) {
      using ceph::decode;
      decode(first, bl);
      decode(ino, bl);
    }
  };
  WRITE_CLASS_ENCODER(dn_weak)

  struct lock_bls {
    ceph::buffer::list file, nest, dft;
    void encode(ceph::buffer::list& bl) const {
      using ceph::encode;
      encode(file, bl);
      encode(nest, bl);
      encode(dft, bl);
    }
    void decode(ceph::buffer::list::const_iterator& bl) {
      using ceph::decode;
      decode(file, bl);
      decode(nest, bl);
      decode(dft, bl);
    }
  };
  WRITE_CLASS_ENCODER(lock_bls)

  // authpins, xlocks
  struct peer_reqid {
    metareqid_t reqid;
    __u32 attempt;
    peer_reqid() : attempt(0) {}
    peer_reqid(const metareqid_t& r, __u32 a)
      : reqid(r), attempt(a) {}
    void encode(ceph::buffer::list& bl) const {
      using ceph::encode;
      encode(reqid, bl);
      encode(attempt, bl);
    }
    void decode(ceph::buffer::list::const_iterator& bl) {
      using ceph::decode;
      decode(reqid, bl);
      decode(attempt, bl);
    }
  };

  std::string_view get_type_name() const override { return "cache_rejoin"; }
  void print(std::ostream& out) const override {
    out << "cache_rejoin " << get_opname(op);
  }

  // -- builders --
  // inodes
  void add_weak_inode(vinodeno_t i) {
    weak_inodes.insert(i);
  }
  void add_strong_inode(vinodeno_t i, int n, int cw, int dl, int nl, int dftl) {
    strong_inodes[i] = inode_strong(n, cw, dl, nl, dftl);
  }
  void add_inode_locks(CInode *in, __u32 nonce, ceph::buffer::list& bl) {
    using ceph::encode;
    encode(in->ino(), inode_locks);
    encode(in->last, inode_locks);
    encode(nonce, inode_locks);
    encode(bl, inode_locks);
  }
  void add_inode_base(CInode *in, uint64_t features) {
    using ceph::encode;
    encode(in->ino(), inode_base);
    encode(in->last, inode_base);
    ceph::buffer::list bl;
    in->_encode_base(bl, features);
    encode(bl, inode_base);
  }
  void add_inode_authpin(vinodeno_t ino, const metareqid_t& ri, __u32 attempt) {
    authpinned_inodes[ino].push_back(peer_reqid(ri, attempt));
  }
  void add_inode_frozen_authpin(vinodeno_t ino, const metareqid_t& ri, __u32 attempt) {
    frozen_authpin_inodes[ino] = peer_reqid(ri, attempt);
  }
  void add_inode_xlock(vinodeno_t ino, int lt, const metareqid_t& ri, __u32 attempt) {
    xlocked_inodes[ino][lt] = peer_reqid(ri, attempt);
  }
  void add_inode_wrlock(vinodeno_t ino, int lt, const metareqid_t& ri, __u32 attempt) {
    wrlocked_inodes[ino][lt].push_back(peer_reqid(ri, attempt));
  }

  void add_scatterlock_state(CInode *in) {
    if (inode_scatterlocks.count(in->ino()))
      return;  // already added this one
    in->encode_lock_state(CEPH_LOCK_IFILE, inode_scatterlocks[in->ino()].file);
    in->encode_lock_state(CEPH_LOCK_INEST, inode_scatterlocks[in->ino()].nest);
    in->encode_lock_state(CEPH_LOCK_IDFT, inode_scatterlocks[in->ino()].dft);
  }

  // dirfrags
  void add_strong_dirfrag(dirfrag_t df, int n, int dr) {
    strong_dirfrags[df] = dirfrag_strong(n, dr);
  }
  void add_dirfrag_base(CDir *dir) {
    ceph::buffer::list& bl = dirfrag_bases[dir->dirfrag()];
    dir->_encode_base(bl);
  }

  // dentries
  void add_weak_dirfrag(dirfrag_t df) {
    weak_dirfrags.insert(df);
  }
  void add_weak_dentry(inodeno_t dirino, std::string_view dname, snapid_t last, dn_weak& dnw) {
    weak[dirino][string_snap_t(dname, last)] = dnw;
  }
  void add_weak_primary_dentry(inodeno_t dirino, std::string_view dname, snapid_t first, snapid_t last, inodeno_t ino) {
    weak[dirino][string_snap_t(dname, last)] = dn_weak(first, ino);
  }
  void add_strong_dentry(dirfrag_t df, std::string_view dname, snapid_t first, snapid_t last, inodeno_t pi, inodeno_t ri, unsigned char rdt, int n, int ls) {
    strong_dentries[df][string_snap_t(dname, last)] = dn_strong(first, pi, ri, rdt, n, ls);
  }
  void add_dentry_authpin(dirfrag_t df, std::string_view dname, snapid_t last,
			  const metareqid_t& ri, __u32 attempt) {
    authpinned_dentries[df][string_snap_t(dname, last)].push_back(peer_reqid(ri, attempt));
  }
  void add_dentry_xlock(dirfrag_t df, std::string_view dname, snapid_t last,
			const metareqid_t& ri, __u32 attempt) {
    xlocked_dentries[df][string_snap_t(dname, last)] = peer_reqid(ri, attempt);
  }

  // -- encoding --
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(op, payload);
    encode(strong_inodes, payload);
    encode(inode_base, payload);
    encode(inode_locks, payload);
    encode(inode_scatterlocks, payload);
    encode(authpinned_inodes, payload);
    encode(frozen_authpin_inodes, payload);
    encode(xlocked_inodes, payload);
    encode(wrlocked_inodes, payload);
    encode(cap_exports, payload);
    encode(client_map, payload, features);
    encode(imported_caps, payload);
    encode(strong_dirfrags, payload);
    encode(dirfrag_bases, payload);
    encode(weak, payload);
    encode(weak_dirfrags, payload);
    encode(weak_inodes, payload);
    encode(strong_dentries, payload);
    encode(authpinned_dentries, payload);
    encode(xlocked_dentries, payload);
    encode(client_metadata_map, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    using ceph::decode;
    decode(op, p);
    decode(strong_inodes, p);
    decode(inode_base, p);
    decode(inode_locks, p);
    decode(inode_scatterlocks, p);
    decode(authpinned_inodes, p);
    decode(frozen_authpin_inodes, p);
    decode(xlocked_inodes, p);
    decode(wrlocked_inodes, p);
    decode(cap_exports, p);
    decode(client_map, p);
    decode(imported_caps, p);
    decode(strong_dirfrags, p);
    decode(dirfrag_bases, p);
    decode(weak, p);
    decode(weak_dirfrags, p);
    decode(weak_inodes, p);
    decode(strong_dentries, p);
    decode(authpinned_dentries, p);
    decode(xlocked_dentries, p);
    if (header.version >= 2)
      decode(client_metadata_map, p);
  }

  // -- data --
  int32_t op = 0;

  // weak
  std::map<inodeno_t, std::map<string_snap_t, dn_weak> > weak;
  std::set<dirfrag_t> weak_dirfrags;
  std::set<vinodeno_t> weak_inodes;
  std::map<inodeno_t, lock_bls> inode_scatterlocks;

  // strong
  std::map<dirfrag_t, dirfrag_strong> strong_dirfrags;
  std::map<dirfrag_t, std::map<string_snap_t, dn_strong> > strong_dentries;
  std::map<vinodeno_t, inode_strong> strong_inodes;

  // open
  std::map<inodeno_t,std::map<client_t, cap_reconnect_t> > cap_exports;
  std::map<client_t, entity_inst_t> client_map;
  std::map<client_t,client_metadata_t> client_metadata_map;
  ceph::buffer::list imported_caps;

  // full
  ceph::buffer::list inode_base;
  ceph::buffer::list inode_locks;
  std::map<dirfrag_t, ceph::buffer::list> dirfrag_bases;

  std::map<vinodeno_t, std::list<peer_reqid> > authpinned_inodes;
  std::map<vinodeno_t, peer_reqid> frozen_authpin_inodes;
  std::map<vinodeno_t, std::map<__s32, peer_reqid> > xlocked_inodes;
  std::map<vinodeno_t, std::map<__s32, std::list<peer_reqid> > > wrlocked_inodes;
  std::map<dirfrag_t, std::map<string_snap_t, std::list<peer_reqid> > > authpinned_dentries;
  std::map<dirfrag_t, std::map<string_snap_t, peer_reqid> > xlocked_dentries;

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);

  static constexpr int HEAD_VERSION = 2;
  static constexpr int COMPAT_VERSION = 1;

  MMDSCacheRejoin(int o) : MMDSCacheRejoin() { op = o; }
  MMDSCacheRejoin() : MMDSOp{MSG_MDS_CACHEREJOIN, HEAD_VERSION, COMPAT_VERSION} {}
  ~MMDSCacheRejoin() override {}
};

WRITE_CLASS_ENCODER(MMDSCacheRejoin::inode_strong)
WRITE_CLASS_ENCODER(MMDSCacheRejoin::dirfrag_strong)
WRITE_CLASS_ENCODER(MMDSCacheRejoin::dn_strong)
WRITE_CLASS_ENCODER(MMDSCacheRejoin::dn_weak)
WRITE_CLASS_ENCODER(MMDSCacheRejoin::lock_bls)
WRITE_CLASS_ENCODER(MMDSCacheRejoin::peer_reqid)

inline std::ostream& operator<<(std::ostream& out, const MMDSCacheRejoin::peer_reqid& r) {
  return out << r.reqid << '.' << r.attempt;
}

#endif
