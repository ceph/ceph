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


#ifndef __FILELOCK_H
#define __FILELOCK_H

#include <assert.h>
#include <set>
using namespace std;

#include "include/buffer.h"

#include "SimpleLock.h"
#include "Capability.h"

// states and such.
//  C = cache reads, R = read, W = write, A = append, B = buffer writes
//  L = lazyio
//  R = readable replicas, S = sloppy writes

//                                -----auth------------   ---replica-----------
#define LOCK_SYNC_     1  // AR   R . / C R . . . L R .   R . / C R . . . L . .   stat()
#define LOCK_GSYNCL   -12 // A    . . / C ? . . . L . .                           loner -> sync (*)
#define LOCK_GSYNCRM  -13 // A    . . / . R . . . L R .
#define LOCK_GSYNCWM  -13 // A    . . / . R . . . L . .

#define LOCK_LOCK_     2  // AR   R W / C . . . . . . .   . . / C . . . . . . .   truncate()
#define LOCK_GLOCKR_  -3  // AR   R . / C . . . . . . .   . . / C . . . . . . .
#define LOCK_GLOCKL   -4  // A    . . / C . . . . . . .                           loner -> lock
#define LOCK_GLOCKM   -5  // A    . . / . . . . . . . .

#define LOCK_RMIXED    6  // AR   . . / . R W A . L R .   . . / . R . . . L R .
#define LOCK_GRMIXEDR -7  // AR   R . / . R . . . L R .   . . / . R . . . L . . 
#define LOCK_GRMIXEDL -8  // A    . . / . . . . . L R .                           loner -> rmixed
#define LOCK_GRMIXEDM -17 // A    . . / . R W A . L R .   . . / . R . . . L . .   wmixed -> rmixed

#define LOCK_WMIXED    14 // AR   . . / . R W A . L . S   . . / . R . . . L . S
#define LOCK_GWMIXEDR -15 // AR   R . / . R . . . L . S   . . / . R . . . L . . 
#define LOCK_GWMIXEDL -16 // A    . . / . . . . . L . S                           loner -> mixed
#define LOCK_GWMIXEDM -17 // A    . . / . R W A . L . S   . . / . R . . . L . .   rmixed -> wmixed

#define LOCK_LONER     9  // A    . . / C R W A B L R S        (lock)      
#define LOCK_GLONERR  -10 // A    . . / . R . . . L R .
#define LOCK_GLONERRM -11 // A    . . / . R W A . L R .
#define LOCK_GLONERWM -18 // A    . . / . R W A . L . S

// (*) FIXME: how to let old loner keep R, somehow, during GSYNCL

inline const char *get_filelock_state_name(int n) {
  switch (n) {
  case LOCK_SYNC: return "sync";
  case LOCK_GSYNCL: return "gsyncl";
  case LOCK_GSYNCRM: return "gsyncrm";
  case LOCK_GSYNCWM: return "gsyncwm";
  case LOCK_LOCK: return "lock";
  case LOCK_GLOCKR: return "glockr";
  case LOCK_GLOCKL: return "glockl";
  case LOCK_GLOCKM: return "glockm";
  case LOCK_RMIXED: return "rmixed";
  case LOCK_GRMIXEDR: return "grmixedr";
  case LOCK_GRMIXEDL: return "grmixedl";
  case LOCK_WMIXED: return "wmixed";
  case LOCK_GWMIXEDR: return "gwmixedr";
  case LOCK_GWMIXEDL: return "gwmixedl";
  case LOCK_LONER: return "loner";
  case LOCK_GLONERR: return "glonerr";
  case LOCK_GLONERRM: return "glonerrm";
  case LOCK_GLONERWM: return "glonerwm";
  default: assert(0);
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
  
  char get_replica_state() {
    switch (state) {
    case LOCK_LOCK:
    case LOCK_GLOCKM:
    case LOCK_GLOCKL:
    case LOCK_GLOCKR: 
    case LOCK_LONER:
    case LOCK_GLONERR:
    case LOCK_GLONERM:
      return LOCK_LOCK;
    case LOCK_RMIXED:
    case LOCK_GRMIXEDR:
      return LOCK_RMIXED;
    case LOCK_WMIXED:
    case LOCK_GWMIXEDR:
      return LOCK_WMIXED;
    case LOCK_SYNC:
      return LOCK_SYNC;

    case LOCK_GRMIXEDM:
      return LOCK_GRMIXEDM;
    case LOCK_GWMIXEDM:
      return LOCK_GWMIXEDM;

      // after gather auth will bc LOCK_AC_MIXED or whatever
    case LOCK_GSYNCRM:
      return LOCK_RMIXED;
    case LOCK_GSYNCWM:
      return LOCK_WMIXED;
    case LOCK_GSYNCL:
    case LOCK_GMIXEDL:     // ** LOCK isn't exact right state, but works.
      return LOCK_LOCK;

    default: 
      assert(0);
    }
    return 0;
  }


  // read/write access
  bool can_rdlock(MDRequest *mdr) {
    if (!parent->is_auth())
      return (state == LOCK_SYNC);
    if (state == LOCK_LOCK && mdr && xlock_by == mdr)
      return true;
    if (state == LOCK_LOCK && !xlock_by) 
      return true;
    return (state == LOCK_SYNC) || (state == LOCK_GMIXEDR) 
      || (state == LOCK_GLOCKR);
  }
  bool can_rdlock_soon() {
    if (parent->is_auth())
      return (state == LOCK_FGLOCKL);
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
      return CAP_FILE_RDCACHE | CAP_FILE_RD | CAP_FILE_WR | CAP_FILE_WREXTEND | CAP_FILE_WRBUFFER | CAP_FILE_LAZYIO;
    else
      return CAP_FILE_RDCACHE | CAP_FILE_RD | CAP_FILE_LAZYIO;
  }
  int caps_allowed() {
    if (parent->is_auth())
      switch (state) {
      case LOCK_SYNC:
        return CAP_FILE_RDCACHE | CAP_FILE_RD | CAP_FILE_LAZYIO | CAP_FILE_RDREPLICA;
      case LOCK_LOCK:
      case LOCK_GLOCKR:
      case LOCK_GLOCKL:
        return CAP_FILE_RDCACHE;

      case LOCK_GLOCKM:
        return 0;

      case LOCK_MIXED:
        return CAP_FILE_RD | CAP_FILE_WR | CAP_FILE_WREXTEND | CAP_FILE_LAZYIO;
      case LOCK_GMIXEDR:
        return CAP_FILE_RD | CAP_FILE_LAZYIO;
      case LOCK_GMIXEDL:
        return 0;

      case LOCK_LONER:  // single client writer, of course.
        return CAP_FILE_RDCACHE | CAP_FILE_RD | CAP_FILE_WR | CAP_FILE_WREXTEND | CAP_FILE_WRBUFFER | CAP_FILE_LAZYIO | CAP_FILE_RDREPLICA | CAP_FILE_WRSLOPPY;
      case LOCK_GLONERR:
        return CAP_FILE_RD | CAP_FILE_LAZYIO;
      case LOCK_GLONERM:
        return CAP_FILE_RD | CAP_FILE_WR | CAP_FILE_WREXTEND | CAP_FILE_LAZYIO;

      case LOCK_GSYNCL:
        return CAP_FILE_RDCACHE | CAP_FILE_LAZYIO;
      case LOCK_GSYNCM:
        return CAP_FILE_RD | CAP_FILE_LAZYIO;
      }
    else
      switch (state) {
      case LOCK_SYNC:
        return CAP_FILE_RDCACHE | CAP_FILE_RD | CAP_FILE_LAZYIO | CAP_FILE_RDREPLICA;
      case LOCK_LOCK:
      case LOCK_GLOCKR:
        return CAP_FILE_RDCACHE;
      case LOCK_GMIXEDR:
      case LOCK_MIXED:
        return CAP_FILE_RD | CAP_FILE_LAZYIO;
      }
    assert(0);
    return 0;
  }

  void print(ostream& out) {
    out << "(";
    //out << get_lock_type_name(l.get_type()) << " ";
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
