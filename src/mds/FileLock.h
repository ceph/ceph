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


#ifndef __FILELOCK_H
#define __FILELOCK_H

#include <set>
using namespace std;

#include "include/buffer.h"

#include "SimpleLock.h"
#include "ScatterLock.h"

// states and such.
//  C = cache reads, R = read, W = write, A = append, B = buffer writes, L = lazyio
//
// lower-case lock (R/W) on a transition state means a 'trailing'
// rdlock or wrlock..  the old locks still there (from the prior
// state), but new locks aren't allowed.
//
// lower-case caps means loner-only.

//                                   -----auth--------   ---replica-------
#define LOCK_SYNC_        1  // AR   R . / C R . . . L   R . / C R . . . L   stat()
#define LOCK_LONER_SYNC  -12 // A    . . / C r . . . L *                     loner -> sync
#define LOCK_MIXED_SYNC  -13 // A    . w / . R . . . L   . w / . R . . . L
#define LOCK_LOCK_SYNC_  -14 // A    . w / C . . . b L

#define LOCK_LOCK_        2  // AR   R W / C . . . B .   . . / C . . . . .   truncate()
#define LOCK_SYNC_LOCK_  -3  // AR   R . / C . . . . .   r . / C . . . . .
#define LOCK_LONER_LOCK  -4  // A    . . / C . . . B .                       loner -> lock
#define LOCK_MIXED_LOCK  -5  // A    . w / . . . . . .   . w / . . . . . .

#define LOCK_MIXED        6  // AR   . W / . R W A . L   . W / . R . . . L
#define LOCK_SYNC_MIXED  -7  // AR   r . / . R . . . L   r . / . R . . . L 
#define LOCK_LONER_MIXED -8  // A    . . / . r w a . L *                     loner -> mixed

#define LOCK_LONER        9  // A    . . / c r w a b L *      (lock)      
#define LOCK_SYNC_LONER  -10 // A    r . / . R . . . L 
#define LOCK_MIXED_LONER -11 // A    . w / . R W A . L 
#define LOCK_LOCK_LONER  -15 // A    . . / c . . . b . *

//                                                 * <- varies if client is loner vs non-loner.
 
inline const char *get_filelock_state_name(int n) {
  switch (n) {
  case LOCK_SYNC: return "sync";
  case LOCK_LONER_SYNC: return "loner->sync";
  case LOCK_MIXED_SYNC: return "mixed->sync";
  case LOCK_LOCK_SYNC: return "lock->sync";
  case LOCK_LOCK: return "lock";
  case LOCK_SYNC_LOCK: return "sync->lock";
  case LOCK_LONER_LOCK: return "loner->lock";
  case LOCK_MIXED_LOCK: return "mixed->lock";
  case LOCK_MIXED: return "mixed";
  case LOCK_SYNC_MIXED: return "sync->mixed";
  case LOCK_LONER_MIXED: return "loner->mixed";
  case LOCK_LONER: return "loner";
  case LOCK_SYNC_LONER: return "sync->loner";
  case LOCK_MIXED_LONER: return "mixed->loner";
  case LOCK_LOCK_LONER: return "lock->loner";
  default: assert(0); return 0;
  }
}


/* no append scenarios:

loner + truncate():
  - loner needs to lose A (?unless it's the loner doing the truncate?)
loner + statlite(size):
  - loner needs to lose A

any + statlite(size)
  - all lose A

any + statlite(mtime)
  - all lose W

-> we need to add lonerfixed and mixedfixed states (and associated transitions)
 in order to efficiently support statlite(size) and truncate().  until then,
 we have to LOCK.

 */


class Mutation;

class FileLock : public ScatterLock {
 public:
  FileLock(MDSCacheObject *o, int t, int wo) : 
    ScatterLock(o, t, wo) {}
  
  int get_replica_state() const {
    switch (state) {
    case LOCK_LOCK:
    case LOCK_MIXED_LOCK:
    case LOCK_LONER_LOCK:
    case LOCK_SYNC_LOCK: 
    case LOCK_LONER:
    case LOCK_SYNC_LONER:
    case LOCK_MIXED_LONER:
    case LOCK_LOCK_LONER:
      return LOCK_LOCK;
    case LOCK_MIXED:
    case LOCK_SYNC_MIXED:
      return LOCK_MIXED;
    case LOCK_SYNC:
    case LOCK_LOCK_SYNC:
      return LOCK_SYNC;

      // after gather auth will bc LOCK_AC_MIXED or whatever
    case LOCK_MIXED_SYNC:
      return LOCK_MIXED;
    case LOCK_LONER_SYNC:
    case LOCK_LONER_MIXED:     // ** LOCK isn't exact right state, but works.
      return LOCK_LOCK;

    default: 
      assert(0);
    }
    return 0;
  }
  void export_twiddle() {
    clear_gather();
    state = get_replica_state();
  }


  // read/write access
  bool can_rdlock() {
    if (!parent->is_auth()) {
      if (state == LOCK_LOCK && !xlock_by)
	return true;
      return (state == LOCK_SYNC);
    } else
      return (state == LOCK_SYNC);
  }
  bool can_rdlock_soon() {
    if (parent->is_auth())
      return 
	(state == LOCK_LONER_LOCK) ||
	(state == LOCK_LOCK && xlock_by);
    else
      return false;
  }

  // xlock
  bool can_xlock_soon() {
    if (parent->is_auth())
      return (state == LOCK_SYNC_LOCK ||
	      state == LOCK_LONER_LOCK ||
	      state == LOCK_MIXED_LOCK);
    else
      return false;
  }

  // wrlock
  bool can_wrlock() {
    if (parent->is_auth())
      return (state == LOCK_LOCK ||
	      state == LOCK_MIXED);
    else
      return (state == LOCK_MIXED);    
  }


  // client caps allowed
  int caps_allowed_ever() {
    if (parent->is_auth())
      return CEPH_CAP_PIN | 
	CEPH_CAP_RDCACHE | CEPH_CAP_RD | 
	CEPH_CAP_WR | CEPH_CAP_WREXTEND | CEPH_CAP_WRBUFFER | CEPH_CAP_EXCL |
	CEPH_CAP_LAZYIO;
    else
      return CEPH_CAP_PIN | 
	CEPH_CAP_RDCACHE | CEPH_CAP_RD | CEPH_CAP_LAZYIO;
  }
  int caps_allowed(bool loner) {
    if (parent->is_auth())
      switch (state) {
      case LOCK_SYNC:
        return CEPH_CAP_PIN | CEPH_CAP_RDCACHE | CEPH_CAP_RD | CEPH_CAP_LAZYIO;
      case LOCK_SYNC_LOCK:
         return CEPH_CAP_PIN | CEPH_CAP_RDCACHE;
      case LOCK_LOCK:
      case LOCK_LONER_LOCK:
        return CEPH_CAP_PIN | CEPH_CAP_RDCACHE | CEPH_CAP_WRBUFFER;

      case LOCK_MIXED_LOCK:
        return CEPH_CAP_PIN;

      case LOCK_MIXED:
        return CEPH_CAP_PIN | CEPH_CAP_RD | CEPH_CAP_WR | CEPH_CAP_WREXTEND | CEPH_CAP_LAZYIO;
      case LOCK_SYNC_MIXED:
        return CEPH_CAP_PIN | CEPH_CAP_RD | CEPH_CAP_LAZYIO;
      case LOCK_LONER_MIXED:
        return CEPH_CAP_PIN | (loner ? (CEPH_CAP_RD | CEPH_CAP_WR | CEPH_CAP_WREXTEND) : 0);

      case LOCK_LONER:  // single client writer, of course.
        return CEPH_CAP_PIN | CEPH_CAP_LAZYIO |
	  ( loner ? (CEPH_CAP_RDCACHE | CEPH_CAP_RD | CEPH_CAP_WR | CEPH_CAP_WREXTEND | CEPH_CAP_WRBUFFER | CEPH_CAP_EXCL) : 0 );
      case LOCK_SYNC_LONER:
        return CEPH_CAP_PIN | CEPH_CAP_RD | CEPH_CAP_LAZYIO;
      case LOCK_MIXED_LONER:
        return CEPH_CAP_PIN | CEPH_CAP_RD | CEPH_CAP_WR | CEPH_CAP_WREXTEND | CEPH_CAP_LAZYIO;
      case LOCK_LOCK_LONER:
        return CEPH_CAP_PIN | (loner ? (CEPH_CAP_RDCACHE | CEPH_CAP_WRBUFFER) : 0);

      case LOCK_LONER_SYNC:
        return CEPH_CAP_PIN | CEPH_CAP_RDCACHE | (loner ? CEPH_CAP_RD:0) | CEPH_CAP_LAZYIO;
      case LOCK_MIXED_SYNC:
        return CEPH_CAP_PIN | CEPH_CAP_RD | CEPH_CAP_LAZYIO;
      }
    else
      switch (state) {
      case LOCK_SYNC:
        return CEPH_CAP_PIN | CEPH_CAP_RDCACHE | CEPH_CAP_RD | CEPH_CAP_LAZYIO;
      case LOCK_LOCK:
      case LOCK_SYNC_LOCK:
        return CEPH_CAP_PIN | CEPH_CAP_RDCACHE;
      case LOCK_SYNC_MIXED:
      case LOCK_MIXED:
        return CEPH_CAP_PIN | CEPH_CAP_RD | CEPH_CAP_LAZYIO;
      }
    assert(0);
    return 0;
  }

  // true if we are in a "loner" mode that distinguishes between a loner and everyone else
  bool is_loner_mode() {
    return (state == LOCK_LONER_SYNC ||
	    state == LOCK_LONER_MIXED ||
	    state == LOCK_LONER ||
	    state == LOCK_LOCK_LONER);
  }


  void print(ostream& out) {
    out << "(";
    out << get_lock_type_name(get_type()) << " ";
    out << get_filelock_state_name(get_state());
    if (!get_gather_set().empty()) out << " g=" << get_gather_set();
    if (get_num_client_lease())
      out << " c=" << get_num_client_lease();
    if (is_wrlocked())
      out << " w=" << get_num_wrlocks();
    if (is_rdlocked()) 
      out << " r=" << get_num_rdlocks();
    if (is_xlocked())
      out << " x=" << get_xlocked_by();
    out << ")";
  }
};


#endif
