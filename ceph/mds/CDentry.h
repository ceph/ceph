// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
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

class CInode;
class CDir;

#define DN_LOCK_SYNC      0
#define DN_LOCK_PREXLOCK  1
#define DN_LOCK_XLOCK     2
#define DN_LOCK_UNPINNING 3  // waiting for pins to go away

#define DN_XLOCK_FOREIGN  ((Message*)0x1)  // not 0, not a valid pointer.

class Message;

// dentry
class CDentry {
 protected:
  string          name;
  CInode         *inode;
  CDir           *dir;

  inodeno_t       remote_ino;      // if remote dentry

  // state
  bool             dirty;
  __uint64_t       parent_dir_version;  // dir version when last touched.

  // locking
  int            lockstate;
  Message        *xlockedby;
  set<int>       gather_set;
  
  int            npins;
  multiset<Message*> pinset;

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
	dirty(0),
	parent_dir_version(0),
	lockstate(DN_LOCK_SYNC),
	xlockedby(0),
	npins(0) { }
  CDentry(const string& n, inodeno_t ino, CInode *in=0) :
	name(n),
	inode(in),
	dir(0),
	remote_ino(ino),
	dirty(0),
	parent_dir_version(0),
	lockstate(DN_LOCK_SYNC),
	xlockedby(0),
	npins(0) { }
  CDentry(const string& n, CInode *in) :
	name(n),
	inode(in),
	dir(0),
	remote_ino(0),
	dirty(0),
	parent_dir_version(0),
	lockstate(DN_LOCK_SYNC),
	xlockedby(0),
	npins(0) { }

  CInode *get_inode() { return inode; }
  CDir *get_dir() { return dir; }
  const string& get_name() { return name; }
  inodeno_t get_remote_ino() { return remote_ino; }

  void set_remote_ino(inodeno_t ino) { remote_ino = ino; }

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
  __uint64_t get_parent_dir_version() { return parent_dir_version; }
  void float_parent_dir_version(__uint64_t ge) {
	if (parent_dir_version < ge)
	  parent_dir_version = ge;
  }
  
  bool is_dirty() { return dirty; }
  bool is_clean() { return !dirty; }

  void mark_dirty();
  void mark_clean();


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
  
  // pins
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


#endif
