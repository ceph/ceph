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
#include "include/xlist.h"
#include "include/filepath.h"
#include "mdstypes.h"

#include "SimpleLock.h"

class CInode;
class CDir;
class MDRequest;

class Message;
class CDentryDiscover;
class Anchor;

class CDentry;
class LogSegment;


// define an ordering
bool operator<(const CDentry& l, const CDentry& r);

// dentry
class CDentry : public MDSCacheObject, public LRUObject {
 public:
  // -- state --
  static const int STATE_NEW = 1;
  static const int STATE_FRAGMENTING = 2;

  // -- pins --
  static const int PIN_INODEPIN =     1;  // linked inode is pinned
  static const int PIN_FRAGMENTING = -2;  // containing dir is refragmenting
  const char *pin_name(int p) {
    switch (p) {
    case PIN_INODEPIN: return "inodepin";
    case PIN_FRAGMENTING: return "fragmenting";
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
  string name;

  inodeno_t remote_ino;      // if remote dentry
  unsigned char remote_d_type;

  CInode *inode; // linked inode (if any)
  CDir *dir;     // containing dirfrag

  version_t version;  // dir version when last touched.
  version_t projected_version;  // what it will be when i unlock/commit.

  xlist<CDentry*>::item xlist_dirty;

  off_t dir_offset;   

  int auth_pins, nested_auth_pins;

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
    remote_ino(0), remote_d_type(0),
    inode(0), dir(0),
    version(0), projected_version(0),
    xlist_dirty(this),
    dir_offset(0),
    auth_pins(0), nested_auth_pins(0),
    lock(this, LOCK_OTYPE_DN, WAIT_LOCK_OFFSET) { }
  CDentry(const string& n, CInode *in) :
    name(n),
    remote_ino(0), remote_d_type(0),
    inode(in), dir(0),
    version(0), projected_version(0),
    xlist_dirty(this),
    dir_offset(0),
    auth_pins(0), nested_auth_pins(0),
    lock(this, LOCK_OTYPE_DN, WAIT_LOCK_OFFSET) { }
  CDentry(const string& n, inodeno_t ino, unsigned char dt, CInode *in=0) :
    name(n),
    remote_ino(ino), remote_d_type(dt),
    inode(in), dir(0),
    version(0), projected_version(0),
    xlist_dirty(this),
    dir_offset(0),
    auth_pins(0), nested_auth_pins(0),
    lock(this, LOCK_OTYPE_DN, WAIT_LOCK_OFFSET) { }

  CInode *get_inode() const { return inode; }
  CDir *get_dir() const { return dir; }
  const string& get_name() const { return name; }
  inodeno_t get_ino();

  off_t get_dir_offset() { return dir_offset; }
  void set_dir_offset(off_t o) { dir_offset = o; }
  void clear_dir_offset() { dir_offset = 0; }

  inodeno_t get_remote_ino() { return remote_ino; }
  unsigned char get_remote_d_type() { return remote_d_type; }
  void set_remote(inodeno_t ino, unsigned char d_type) { 
    remote_ino = ino;
    remote_d_type = d_type;
  }

  // ref counts: pin ourselves in the LRU when we're pinned.
  void first_get() {
    lru_pin();
  }
  void last_put() {
    lru_unpin();
  }

  // auth pins
  bool can_auth_pin();
  void auth_pin();
  void auth_unpin();
  void adjust_nested_auth_pins(int by);
  bool is_frozen();
  

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
  void make_path_string(string& s);
  void make_path(filepath& fp);
  void make_anchor_trace(vector<class Anchor>& trace, CInode *in);

  // -- version --
  version_t get_version() { return version; }
  void set_version(version_t v) { projected_version = version = v; }
  version_t get_projected_version() { return projected_version; }
  void set_projected_version(version_t v) { projected_version = v; }
  
  pair<int,int> authority();

  version_t pre_dirty(version_t min=0);
  void _mark_dirty(LogSegment *ls);
  void mark_dirty(version_t projected_dirv, LogSegment *ls);
  void mark_clean();

  void mark_new();
  bool is_new() { return state_test(STATE_NEW); }
  
  // -- replication
  CDentryDiscover *replicate_to(int rep);


  // -- exporting
  // note: this assumes the dentry already exists.  
  // i.e., the name is already extracted... so we just need the other state.
  void encode_export(bufferlist& bl) {
    ::_encode_simple(state, bl);
    ::_encode_simple(version, bl);
    ::_encode_simple(projected_version, bl);
    lock._encode(bl);
    ::_encode_simple(replica_map, bl);
    get(PIN_TEMPEXPORTING);
  }
  void finish_export() {
    // twiddle
    clear_replica_map();
    replica_nonce = EXPORT_NONCE;
    state_clear(CDentry::STATE_AUTH);
    if (is_dirty())
      mark_clean();
    put(PIN_TEMPEXPORTING);
  }
  void abort_export() {
    put(PIN_TEMPEXPORTING);
  }
  void decode_import(bufferlist::iterator& blp, LogSegment *ls) {
    int nstate;
    ::_decode_simple(nstate, blp);
    ::_decode_simple(version, blp);
    ::_decode_simple(projected_version, blp);
    lock._decode(blp);
    ::_decode_simple(replica_map, blp);

    // twiddle
    state = 0;
    state_set(CDentry::STATE_AUTH);
    if (nstate & STATE_DIRTY)
      _mark_dirty(ls);
    if (!replica_map.empty())
      get(PIN_REPLICATED);
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
  off_t  dir_offset;
  inodeno_t remote_ino;
  unsigned char remote_d_type;

public:
  CDentryDiscover() {}
  CDentryDiscover(CDentry *dn, int nonce) :
    dname(dn->get_name()), replica_nonce(nonce),
    lockstate(dn->lock.get_replica_state()),
    dir_offset(dn->get_dir_offset()),
    remote_ino(dn->get_remote_ino()), remote_d_type(dn->get_remote_d_type()) { }

  string& get_dname() { return dname; }
  int get_nonce() { return replica_nonce; }
  bool is_remote() { return remote_ino ? true:false; }
  inodeno_t get_remote_ino() { return remote_ino; }
  unsigned char get_remote_d_type() { return remote_d_type; }

  void update_dentry(CDentry *dn) {
    dn->set_dir_offset(dir_offset);
    dn->set_replica_nonce(replica_nonce);
  }
  void init_dentry_lock(CDentry *dn) {
    dn->lock.set_state( lockstate );
  }

  void _encode(bufferlist& bl) {
    ::_encode(dname, bl);
    ::_encode(dir_offset, bl);
    ::_encode(remote_ino, bl);
    ::_encode(remote_d_type, bl);
    ::_encode(replica_nonce, bl);
    ::_encode(lockstate, bl);
  }
  
  void _decode(bufferlist& bl, int& off) {
    ::_decode(dname, bl, off);
    ::_decode(dir_offset, bl, off);
    ::_decode(remote_ino, bl, off);
    ::_decode(remote_d_type, bl, off);
    ::_decode(replica_nonce, bl, off);
    ::_decode(lockstate, bl, off);
  }

};



#endif
