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

class Mutation;

class FileLock : public ScatterLock {
 public:
  FileLock(MDSCacheObject *o, int t, int ws, int cs) : 
    ScatterLock(o, t, ws, cs) {}
  
  const char *get_state_name(int n) {
    switch (n) {
    case LOCK_SYNC: return "sync";
    case LOCK_EXCL_SYNC: return "excl->sync";
    case LOCK_MIX_SYNC: return "mix->sync";
    case LOCK_MIX_SYNC2: return "mix->sync2";
    case LOCK_LOCK_SYNC: return "lock->sync";
    case LOCK_LOCK: return "lock";
    case LOCK_SYNC_LOCK: return "sync->lock";
    case LOCK_EXCL_LOCK: return "excl->lock";
    case LOCK_MIX_LOCK: return "mix->lock";
    case LOCK_MIX: return "mix";
    case LOCK_SYNC_MIX: return "sync->mix";
    case LOCK_EXCL_MIX: return "excl->mix";
    case LOCK_EXCL: return "excl";
    case LOCK_SYNC_EXCL: return "sync->excl";
    case LOCK_MIX_EXCL: return "mix->excl";
    case LOCK_LOCK_EXCL: return "lock->excl";
    default: assert(0); return 0;
    }
  }

  // xlock
  bool can_xlock_soon() {
    if (parent->is_auth())
      return (state == LOCK_SYNC_LOCK ||
	      state == LOCK_LONER_LOCK ||
	      state == LOCK_MIXED_LOCK ||
	      state == LOCK_LOCK);
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


  // caps

  // true if we are in a "loner" mode that distinguishes between a loner and everyone else
  bool is_loner_mode() {
    return (state == LOCK_LONER_SYNC ||
	    state == LOCK_LONER_MIXED ||
	    state == LOCK_LONER ||
	    state == LOCK_SYNC_LONER ||
	    state == LOCK_LOCK_LONER);
  }
  int gcaps_allowed_ever() {
    if (parent->is_auth())
      return
	CEPH_CAP_GRDCACHE | CEPH_CAP_GEXCL |
	CEPH_CAP_GRD | CEPH_CAP_GWR |
	CEPH_CAP_GWREXTEND |
	CEPH_CAP_GWRBUFFER | 
	CEPH_CAP_GLAZYIO;
    else
      return
	CEPH_CAP_GRDCACHE | CEPH_CAP_GRD | CEPH_CAP_GLAZYIO;
  }
  int gcaps_allowed(bool loner) {
    if (loner && !is_loner_mode())
      loner = false;
    if (parent->is_auth())
      switch (state) {
      case LOCK_SYNC:
        return CEPH_CAP_GRDCACHE | CEPH_CAP_GRD | CEPH_CAP_GLAZYIO;
      case LOCK_SYNC_LOCK:
         return CEPH_CAP_GRDCACHE;
      case LOCK_LOCK:
      case LOCK_LONER_LOCK:
        return CEPH_CAP_GRDCACHE | CEPH_CAP_GWRBUFFER;

      case LOCK_LOCK_SYNC:
	return CEPH_CAP_GRDCACHE | CEPH_CAP_GLAZYIO;

      case LOCK_MIXED_LOCK:
        return 0;

      case LOCK_MIXED:
        return CEPH_CAP_GRD | CEPH_CAP_GWR | CEPH_CAP_GWREXTEND | CEPH_CAP_GLAZYIO;
      case LOCK_SYNC_MIXED:
        return CEPH_CAP_GRD | CEPH_CAP_GLAZYIO;
      case LOCK_LONER_MIXED:
        return (loner ? (CEPH_CAP_GRD | CEPH_CAP_GWR | CEPH_CAP_GWREXTEND) : 0);

      case LOCK_LONER:
        return CEPH_CAP_GLAZYIO |
	  ( loner ? (CEPH_CAP_GRDCACHE | CEPH_CAP_GRD | CEPH_CAP_GWR | CEPH_CAP_GWREXTEND | CEPH_CAP_GWRBUFFER | CEPH_CAP_GEXCL) : 0 );
      case LOCK_SYNC_LONER:
        return CEPH_CAP_GRD | CEPH_CAP_GLAZYIO | (loner ? CEPH_CAP_GRDCACHE : 0);
      case LOCK_MIXED_LONER:
        return CEPH_CAP_GRD | CEPH_CAP_GWR | CEPH_CAP_GWREXTEND | CEPH_CAP_GLAZYIO;
      case LOCK_LOCK_LONER:
        return (loner ? (CEPH_CAP_GRDCACHE | CEPH_CAP_GWRBUFFER) : 0);

      case LOCK_LONER_SYNC:
        return CEPH_CAP_GRDCACHE | (loner ? CEPH_CAP_GRD:0) | CEPH_CAP_GLAZYIO;
      case LOCK_MIXED_SYNC:
        return CEPH_CAP_GRD | CEPH_CAP_GLAZYIO;
      }
    else
      switch (state) {
      case LOCK_SYNC:
        return CEPH_CAP_GRDCACHE | CEPH_CAP_GRD | CEPH_CAP_GLAZYIO;
      case LOCK_LOCK:
      case LOCK_SYNC_LOCK:
        return CEPH_CAP_GRDCACHE;
      case LOCK_SYNC_MIXED:
      case LOCK_MIXED:
        return CEPH_CAP_GRD | CEPH_CAP_GLAZYIO;
      case LOCK_MIXED_SYNC:
      case LOCK_MIXED_SYNC2:
	return CEPH_CAP_GRDCACHE | CEPH_CAP_GLAZYIO;
      }
    assert(0);
    return 0;
  }
  int gcaps_careful() {
    if (num_wrlock)
      return CEPH_CAP_GRDCACHE | CEPH_CAP_GEXCL | CEPH_CAP_GWRBUFFER;
    return 0;
  }


};


#endif
