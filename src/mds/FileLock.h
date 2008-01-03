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

#include <assert.h>
#include <set>
using namespace std;

#include "include/buffer.h"

#include "SimpleLock.h"

// states and such.
//  C = cache reads, R = read, W = write, A = append, B = buffer writes, L = lazyio

//                               -----auth--------   ---replica-------
#define LOCK_SYNC_    1  // AR   R . / C R . . . L   R . / C R . . . L   stat()
#define LOCK_GSYNCL  -12 // A    . . / C ? . . . L                       loner -> sync (*)
#define LOCK_GSYNCM  -13 // A    . . / . R . . . L

#define LOCK_LOCK_    2  // AR   R W / C . . . . .   . . / C . . . . .   truncate()
#define LOCK_GLOCKR_ -3  // AR   R . / C . . . . .   . . / C . . . . .
#define LOCK_GLOCKL  -4  // A    . . / C . . . . .                       loner -> lock
#define LOCK_GLOCKM  -5  // A    . . / . . . . . .

#define LOCK_MIXED    6  // AR   . . / . R W A . L   . . / . R . . . L
#define LOCK_GMIXEDR -7  // AR   R . / . R . . . L   . . / . R . . . L 
#define LOCK_GMIXEDL -8  // A    . . / . . . . . L                       loner -> mixed

#define LOCK_LONER    9  // A    . . / C R W A B L        (lock)      
#define LOCK_GLONERR -10 // A    . . / . R . . . L
#define LOCK_GLONERM -11 // A    . . / . R W A . L

// (*) FIXME: how to let old loner keep R, somehow, during GSYNCL

//   4 stable
//  +9 transition
//  13 total

inline const char *get_filelock_state_name(int n) {
  switch (n) {
  case LOCK_SYNC: return "sync";
  case LOCK_GSYNCL: return "gsyncl";
  case LOCK_GSYNCM: return "gsyncm";
  case LOCK_LOCK: return "lock";
  case LOCK_GLOCKR: return "glockr";
  case LOCK_GLOCKL: return "glockl";
  case LOCK_GLOCKM: return "glockm";
  case LOCK_MIXED: return "mixed";
  case LOCK_GMIXEDR: return "gmixedr";
  case LOCK_GMIXEDL: return "gmixedl";
  case LOCK_LONER: return "loner";
  case LOCK_GLONERR: return "glonerr";
  case LOCK_GLONERM: return "glonerm";
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

// -- lock... hard or file

class MDRequest;

class FileLock : public SimpleLock {
 public:
  FileLock(MDSCacheObject *o, int t, int wo) : SimpleLock(o, t, wo) { }
  
  int get_replica_state() {
    switch (state) {
    case LOCK_LOCK:
    case LOCK_GLOCKM:
    case LOCK_GLOCKL:
    case LOCK_GLOCKR: 
    case LOCK_LONER:
    case LOCK_GLONERR:
    case LOCK_GLONERM:
      return LOCK_LOCK;
    case LOCK_MIXED:
    case LOCK_GMIXEDR:
      return LOCK_MIXED;
    case LOCK_SYNC:
      return LOCK_SYNC;

      // after gather auth will bc LOCK_AC_MIXED or whatever
    case LOCK_GSYNCM:
      return LOCK_MIXED;
    case LOCK_GSYNCL:
    case LOCK_GMIXEDL:     // ** LOCK isn't exact right state, but works.
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
  bool can_rdlock(MDRequest *mdr) {
    if (!parent->is_auth()) return (state == LOCK_SYNC);
    //if (state == LOCK_LOCK && mdr && xlock_by == mdr) return true;
    if (state == LOCK_LOCK && !xlock_by) return true;
    return 
      (state == LOCK_SYNC) ||
      (state == LOCK_GMIXEDR) || 
      (state == LOCK_GLOCKR);
  }
  bool can_rdlock_soon() {
    if (parent->is_auth())
      return (state == LOCK_GLOCKL);
    else
      return false;
  }
  bool can_xlock_soon() {
    if (parent->is_auth())
      return (state == LOCK_GLOCKR) || (state == LOCK_GLOCKL)
        || (state == LOCK_GLOCKM);
    else
      return false;
  }

  // client caps allowed
  int caps_allowed_ever() {
    if (parent->is_auth())
      return CEPH_CAP_RDCACHE | CEPH_CAP_RD | CEPH_CAP_WR | CEPH_CAP_WREXTEND | CEPH_CAP_WRBUFFER | CEPH_CAP_LAZYIO;
    else
      return CEPH_CAP_RDCACHE | CEPH_CAP_RD | CEPH_CAP_LAZYIO;
  }
  int caps_allowed() {
    if (parent->is_auth())
      switch (state) {
      case LOCK_SYNC:
        return CEPH_CAP_RDCACHE | CEPH_CAP_RD | CEPH_CAP_LAZYIO;
      case LOCK_LOCK:
      case LOCK_GLOCKR:
      case LOCK_GLOCKL:
        return CEPH_CAP_RDCACHE;

      case LOCK_GLOCKM:
        return 0;

      case LOCK_MIXED:
        return CEPH_CAP_RD | CEPH_CAP_WR | CEPH_CAP_WREXTEND | CEPH_CAP_LAZYIO;
      case LOCK_GMIXEDR:
        return CEPH_CAP_RD | CEPH_CAP_LAZYIO;
      case LOCK_GMIXEDL:
        return 0;

      case LOCK_LONER:  // single client writer, of course.
        return CEPH_CAP_RDCACHE | CEPH_CAP_RD | CEPH_CAP_WR | CEPH_CAP_WREXTEND | CEPH_CAP_WRBUFFER | CEPH_CAP_LAZYIO;
      case LOCK_GLONERR:
        return CEPH_CAP_RD | CEPH_CAP_LAZYIO;
      case LOCK_GLONERM:
        return CEPH_CAP_RD | CEPH_CAP_WR | CEPH_CAP_WREXTEND | CEPH_CAP_LAZYIO;

      case LOCK_GSYNCL:
        return CEPH_CAP_RDCACHE | CEPH_CAP_LAZYIO;
      case LOCK_GSYNCM:
        return CEPH_CAP_RD | CEPH_CAP_LAZYIO;
      }
    else
      switch (state) {
      case LOCK_SYNC:
        return CEPH_CAP_RDCACHE | CEPH_CAP_RD | CEPH_CAP_LAZYIO;
      case LOCK_LOCK:
      case LOCK_GLOCKR:
        return CEPH_CAP_RDCACHE;
      case LOCK_GMIXEDR:
      case LOCK_MIXED:
        return CEPH_CAP_RD | CEPH_CAP_LAZYIO;
      }
    assert(0);
    return 0;
  }

  void print(ostream& out) {
    out << "(";
    out << get_lock_type_name(get_type()) << " ";
    out << get_filelock_state_name(get_state());
    if (!get_gather_set().empty()) out << " g=" << get_gather_set();
    if (is_rdlocked()) 
      out << " r=" << get_num_rdlocks();
    if (is_xlocked())
      out << " x=" << get_xlocked_by();
    out << ")";
  }
};


#endif
