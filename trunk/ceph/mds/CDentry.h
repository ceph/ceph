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

class CInode;
class CDir;

#define DN_LOCK_SYNC      0
#define DN_LOCK_PREXLOCK  1
#define DN_LOCK_XLOCK     2
#define DN_LOCK_UNPINNING 3  // waiting for pins to go away .. FIXME REVIEW THIS CODE ..

#define DN_XLOCK_FOREIGN  ((Message*)0x1)  // not 0, not a valid pointer.

class Message;
class CDentryDiscover;

// dentry
class CDentry : public MDSCacheObject, public LRUObject {
 public:
  // state
  static const int STATE_AUTH =       (1<<0);
  static const int STATE_DIRTY =      (1<<1);

  // pins
  static const int PIN_INODEPIN = 0;   // linked inode is pinned
  static const int PIN_REPLICATED = 1; // replicated by another MDS
  static const int PIN_DIRTY = 2;      //
  static const int PIN_PROXY = 3;      //
  static const char *pin_name(int p) {
    switch (p) {
    case PIN_INODEPIN: return "inodepin";
    case PIN_REPLICATED: return "replicated";
    case PIN_DIRTY: return "dirty";
    case PIN_PROXY: return "proxy";
    default: assert(0);
    }
  };


 protected:
  string          name;
  CInode         *inode;
  CDir           *dir;

  inodeno_t       remote_ino;      // if remote dentry

  version_t       version;  // dir version when last touched.
  version_t       projected_version;  // what it will be when i unlock/commit.

  // locking
  int            lockstate;
  Message        *xlockedby;
  set<int>       gather_set;
  
  // path pins
  int            npins;
  multiset<Message*> pinset;

  friend class Migrator;
  friend class Locker;
  friend class Renamer;
  friend class Server;
  friend class MDCache;
  friend class MDS;
  friend class CInode;
  friend class C_MDC_XlockRequest;

 public:
  // cons
  CDentry() :
    inode(0),
    dir(0),
    remote_ino(0),
    version(0),
    projected_version(0),
    lockstate(DN_LOCK_SYNC),
    xlockedby(0),
    npins(0) { }
  CDentry(const string& n, inodeno_t ino, CInode *in=0) :
    name(n),
    inode(in),
    dir(0),
    remote_ino(ino),
    version(0),
    projected_version(0),
    lockstate(DN_LOCK_SYNC),
    xlockedby(0),
    npins(0) { }
  CDentry(const string& n, CInode *in) :
    name(n),
    inode(in),
    dir(0),
    remote_ino(0),
    version(0),
    projected_version(0),
    lockstate(DN_LOCK_SYNC),
    xlockedby(0),
    npins(0) { }

  CInode *get_inode() { return inode; }
  CDir *get_dir() { return dir; }
  const string& get_name() { return name; }
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

  // comparisons
  bool operator== (const CDentry& right) const;
  bool operator!= (const CDentry& right) const;
  bool operator< (const CDentry& right) const;
  bool operator> (const CDentry& right) const;
  bool operator>= (const CDentry& right) const;
  bool operator<= (const CDentry& right) const;

  // misc
  void make_path(string& p);

  // -- state
  version_t get_version() { return version; }
  void set_version(version_t v) { projected_version = version = v; }
  version_t get_projected_version() { return projected_version; }
  void set_projected_version(version_t v) { projected_version = v; }
  
  int authority();

  bool is_auth() { return state & STATE_AUTH; }
  bool is_dirty() { return state & STATE_DIRTY; }
  bool is_clean() { return !is_dirty(); }

  version_t pre_dirty();
  void _mark_dirty();
  void mark_dirty(version_t projected_dirv);
  void mark_clean();

  
  // -- replication
  CDentryDiscover *replicate_to(int rep);


  // -- locking
  int get_lockstate() { return lockstate; }
  set<int>& get_gather_set() { return gather_set; }

  bool is_sync() { return lockstate == DN_LOCK_SYNC; }
  bool can_read()  { return (lockstate == DN_LOCK_SYNC) || (lockstate == DN_LOCK_UNPINNING);  }
  bool can_read(Message *m) { return is_xlockedbyme(m) || can_read(); }
  bool is_xlocked() { return lockstate == DN_LOCK_XLOCK; }
  Message* get_xlockedby() { return xlockedby; } 
  bool is_xlockedbyother(Message *m) { return (lockstate == DN_LOCK_XLOCK) && m != xlockedby; }
  bool is_xlockedbyme(Message *m) { return (lockstate == DN_LOCK_XLOCK) && m == xlockedby; }
  bool is_prexlockbyother(Message *m) {
    return (lockstate == DN_LOCK_PREXLOCK) && m != xlockedby;
  }

  int get_replica_lockstate() {
    switch (lockstate) {
    case DN_LOCK_XLOCK:
    case DN_LOCK_SYNC: 
      return lockstate;
    case DN_LOCK_PREXLOCK:
      return DN_LOCK_XLOCK;
    case DN_LOCK_UNPINNING:
      return DN_LOCK_SYNC;
    }
    assert(0); 
    return 0;
  }
  void set_lockstate(int s) { lockstate = s; }
  
  // path pins
  void pin(Message *m) { 
    npins++; 
    pinset.insert(m);
    assert(pinset.size() == (unsigned)npins);
  }
  void unpin(Message *m) { 
    npins--; 
    assert(npins >= 0); 
    assert(pinset.count(m) > 0);
    pinset.erase(pinset.find(m));
    assert(pinset.size() == (unsigned)npins);
  }
  bool is_pinnable(Message *m) { 
    return (lockstate == DN_LOCK_SYNC) ||
      (lockstate == DN_LOCK_UNPINNING && pinset.count(m)); 
  }
  bool is_pinned() { return npins>0; }
  int num_pins() { return npins; }

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
    lockstate(dn->get_replica_lockstate()),
    ino(dn->get_ino()),
    remote_ino(dn->get_remote_ino()) { }

  string& get_dname() { return dname; }
  int get_nonce() { return replica_nonce; }

  void update_dentry(CDentry *dn) {
    dn->set_replica_nonce( replica_nonce );
    dn->set_lockstate( lockstate );
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
