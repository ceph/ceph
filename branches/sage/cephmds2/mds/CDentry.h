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



#ifndef __CDENTRY_H
#define __CDENTRY_H

#include <assert.h>
#include <string>
#include <set>
using namespace std;

#include "include/types.h"
#include "include/buffer.h"
#include "include/lru.h"
#include "mdstypes.h"

#include "SimpleLock.h"

class CInode;
class CDir;
class MDRequest;

class Message;
class CDentryDiscover;
class Anchor;

class CDentry;

// define an ordering
bool operator<(const CDentry& l, const CDentry& r);

// dentry
class CDentry : public MDSCacheObject, public LRUObject {
 public:
  // -- state --

  // -- pins --
  static const int PIN_INODEPIN = 1;   // linked inode is pinned
  const char *pin_name(int p) {
    switch (p) {
    case PIN_INODEPIN: return "inodepin";
    default: return generic_pin_name(p);
    }
  };

  // -- wait --
  static const int WAIT_LOCK_OFFSET = 8;

  void add_waiter(int tag, Context *c);

  static const int EXPORT_NONCE = 1;

  bool is_lt(const MDSCacheObject *r) const {
    return *this < *(CDentry*)r;
  }

 protected:
  string          name;
  CInode         *inode;
  CDir           *dir;

  inodeno_t       remote_ino;      // if remote dentry

  version_t       version;  // dir version when last touched.
  version_t       projected_version;  // what it will be when i unlock/commit.


  friend class Migrator;
  friend class Locker;
  friend class Renamer;
  friend class Server;
  friend class MDCache;
  friend class MDS;
  friend class CInode;
  friend class C_MDC_XlockRequest;


public:
  // lock
  SimpleLock lock;



 public:
  // cons
  CDentry() :
    inode(0),
    dir(0),
    remote_ino(0),
    version(0),
    projected_version(0),
    lock(this, LOCK_OTYPE_DN, WAIT_LOCK_OFFSET) { }
  CDentry(const string& n, inodeno_t ino, CInode *in=0) :
    name(n),
    inode(in),
    dir(0),
    remote_ino(ino),
    version(0),
    projected_version(0),
    lock(this, LOCK_OTYPE_DN, WAIT_LOCK_OFFSET) { }
  CDentry(const string& n, CInode *in) :
    name(n),
    inode(in),
    dir(0),
    remote_ino(0),
    version(0),
    projected_version(0),
    lock(this, LOCK_OTYPE_DN, WAIT_LOCK_OFFSET) { }

  CInode *get_inode() const { return inode; }
  CDir *get_dir() const { return dir; }
  const string& get_name() const { return name; }
  inodeno_t get_ino();
  inodeno_t get_remote_ino() { return remote_ino; }

  void set_remote_ino(inodeno_t ino) { remote_ino = ino; }


  // ref counts: pin ourselves in the LRU when we're pinned.
  void first_get() {
    lru_pin();
  }
  void last_put() {
    lru_unpin();
  }
  

  // dentry type is primary || remote || null
  // inode ptr is required for primary, optional for remote, undefined for null
  bool is_primary() { return remote_ino == 0 && inode != 0; }
  bool is_remote() { return remote_ino > 0; }
  bool is_null() { return (remote_ino == 0 && inode == 0) ? true:false; }

  // remote links
  void link_remote(CInode *in);
  void unlink_remote();
  

  // copy cons
  CDentry(const CDentry& m);
  const CDentry& operator= (const CDentry& right);

  // misc
  void make_path(string& p);
  void make_path(string& p, inodeno_t tobase);
  void make_anchor_trace(vector<class Anchor>& trace, CInode *in);

  // -- version --
  version_t get_version() { return version; }
  void set_version(version_t v) { projected_version = version = v; }
  version_t get_projected_version() { return projected_version; }
  void set_projected_version(version_t v) { projected_version = v; }
  
  pair<int,int> authority();

  version_t pre_dirty(version_t min=0);
  void _mark_dirty();
  void mark_dirty(version_t projected_dirv);
  void mark_clean();

  
  // -- replication
  CDentryDiscover *replicate_to(int rep);


  // -- exporting
  // note: this assumes the dentry already exists.  
  // i.e., the name is already extracted... so we just need the other state.
  void encode_export_state(bufferlist& bl) {
    bl.append((char*)&state, sizeof(state));
    bl.append((char*)&version, sizeof(version));
    bl.append((char*)&projected_version, sizeof(projected_version));
    lock._encode(bl);
    ::_encode(replicas, bl);

    // twiddle
    clear_replicas();
    replica_nonce = EXPORT_NONCE;
    state_clear(CDentry::STATE_AUTH);
    if (is_dirty())
      mark_clean();
  }
  void decode_import_state(bufferlist& bl, int& off, int from, int to) {
    int nstate;
    bl.copy(off, sizeof(nstate), (char*)&nstate);
    off += sizeof(nstate);
    bl.copy(off, sizeof(version), (char*)&version);
    off += sizeof(version);
    bl.copy(off, sizeof(projected_version), (char*)&projected_version);
    off += sizeof(projected_version);
    lock._decode(bl, off);
    ::_decode(replicas, bl, off);

    // twiddle
    state = 0;
    state_set(CDentry::STATE_AUTH);
    if (nstate & STATE_DIRTY)
      _mark_dirty();
    if (!replicas.empty())
      get(PIN_REPLICATED);
    add_replica(from, EXPORT_NONCE);
    if (is_replica(to))
      remove_replica(to);
  }

  // -- locking --
  SimpleLock* get_lock(int type) {
    assert(type == LOCK_OTYPE_DN);
    return &lock;
  }
  void set_object_info(MDSCacheObjectInfo &info);
  void encode_lock_state(int type, bufferlist& bl);
  void decode_lock_state(int type, bufferlist& bl);

  
  ostream& print_db_line_prefix(ostream& out);
  void print(ostream& out);

  friend class CDir;
};

ostream& operator<<(ostream& out, CDentry& dn);



class CDentryDiscover {
  string dname;
  int    replica_nonce;
  int    lockstate;

  inodeno_t ino;
  inodeno_t remote_ino;

public:
  CDentryDiscover() {}
  CDentryDiscover(CDentry *dn, int nonce) :
    dname(dn->get_name()), replica_nonce(nonce),
    lockstate(dn->lock.get_replica_state()),
    ino(dn->get_ino()),
    remote_ino(dn->get_remote_ino()) { }

  string& get_dname() { return dname; }
  int get_nonce() { return replica_nonce; }
  bool is_remote() { return remote_ino ? true:false; }
  inodeno_t get_remote_ino() { return remote_ino; }

  void update_dentry(CDentry *dn) {
    dn->set_replica_nonce( replica_nonce );
    if (remote_ino)
      dn->set_remote_ino(remote_ino);
  }
  void update_new_dentry(CDentry *dn) {
    update_dentry(dn);
    dn->lock.set_state( lockstate );
  }

  void _encode(bufferlist& bl) {
    ::_encode(dname, bl);
    bl.append((char*)&replica_nonce, sizeof(replica_nonce));
    bl.append((char*)&lockstate, sizeof(lockstate));
  }
  
  void _decode(bufferlist& bl, int& off) {
    ::_decode(dname, bl, off);
    bl.copy(off, sizeof(replica_nonce), (char*)&replica_nonce);
    off += sizeof(replica_nonce);
    bl.copy(off, sizeof(lockstate), (char*)&lockstate);
    off += sizeof(lockstate);
  }

};



#endif
