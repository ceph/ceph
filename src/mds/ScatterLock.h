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


#ifndef __SCATTERLOCK_H
#define __SCATTERLOCK_H

#include "SimpleLock.h"


// lock state machine states:
//  Sync  --  Lock  --  sCatter
//  Tempsync _/
//                              auth repl
#define LOCK_SYNC__                 // R .  R .  rdlocks allowed on auth and replicas
#define LOCK_SYNC_LOCK__       -20  // r .  r .  waiting for replicas+rdlocks (auth), or rdlocks to release (replica)
#define LOCK_SYNC_SCATTER      -28  // r .  r .  

#define LOCK_LOCK_SYNC_             // . w       LOCK on replica.
#define LOCK_LOCK__                 // . W  . .
#define LOCK_LOCK_TEMPSYNC     -21  // . w       LOCK on replica.

#define LOCK_SCATTER_LOCK      -22  // . wp . wp waiting for replicas+wrlocks (auth), or wrlocks to release (replica)
#define LOCK_SCATTER            23  // . Wp . WP mtime updates on replicas allowed, no reads.  stable here.
#define LOCK_SCATTER_TEMPSYNC  -24  // . wp . wp GLOCKC|LOCK on replica

#define LOCK_TEMPSYNC_SCATTER  -25  // r .       LOCK on replica.
#define LOCK_TEMPSYNC_LOCK     -26  // r .       LOCK on replica.
#define LOCK_TEMPSYNC           27  // R .       LOCK on replica.



class ScatterLock : public SimpleLock {
  bool updated;
  utime_t last_scatter;

public:
  xlist<ScatterLock*>::item xlistitem_updated;
  utime_t update_stamp;

  ScatterLock(MDSCacheObject *o, int t, int ws, int cs) : 
    SimpleLock(o, t, ws, cs),
    updated(false),
    xlistitem_updated(this) {}
  ~ScatterLock() {
    xlistitem_updated.remove_myself();   // FIXME this should happen sooner, i think...
  }

  const char *get_state_name(int s) {
    switch(s) {
    case LOCK_SYNC: return "sync";
    case LOCK_SYNC_LOCK: return "sync->lock";
    case LOCK_SYNC_SCATTER: return "sync->scatter";
      
    case LOCK_LOCK_SYNC: return "lock->sync";
    case LOCK_LOCK: return "lock";
    case LOCK_LOCK_TEMPSYNC: return "lock->tempsync";
      
    case LOCK_SCATTER_LOCK: return "scatter->lock";
    case LOCK_SCATTER: return "scatter";
    case LOCK_SCATTER_TEMPSYNC: return "scatter->tempsync";
      
    case LOCK_TEMPSYNC_SCATTER: return "tempsync->scatter";
    case LOCK_TEMPSYNC_LOCK: return "tempsync->lock";
    case LOCK_TEMPSYNC: return "tempsync";
      
    default: assert(0); return 0;
    }
  }

  int get_replica_state() const {
    switch (state) {
    case LOCK_SYNC: 
      return LOCK_SYNC;
      
    case LOCK_SYNC_SCATTER:  // hrm.
    case LOCK_SYNC_LOCK:
    case LOCK_LOCK_SYNC:
    case LOCK_LOCK:
    case LOCK_LOCK_TEMPSYNC:
    case LOCK_SCATTER_LOCK:
      return LOCK_LOCK;

    case LOCK_SCATTER:
      return LOCK_SCATTER;

    case LOCK_SCATTER_TEMPSYNC:
    case LOCK_TEMPSYNC_SCATTER:
    case LOCK_TEMPSYNC_LOCK:
    case LOCK_TEMPSYNC:
      return LOCK_LOCK;
    default:
      assert(0);
      return 0;
    }
  }

  void set_updated() { 
    if (!updated) {
      parent->get(MDSCacheObject::PIN_DIRTYSCATTERED);
      updated = true;
    }
  }
  void clear_updated() { 
    if (updated) {
      parent->put(MDSCacheObject::PIN_DIRTYSCATTERED);
      updated = false; 
      parent->clear_dirty_scattered(type);
    }
  }
  bool is_updated() { return updated; }
  
  void set_last_scatter(utime_t t) { last_scatter = t; }
  utime_t get_last_scatter() { return last_scatter; }

  void replicate_relax() {
  }

  void export_twiddle() {
    clear_gather();
    state = get_replica_state();
  }

  // rdlock
  bool can_rdlock() {
    return state == LOCK_SYNC || state == LOCK_TEMPSYNC;
  }
  bool can_rdlock_soon() {
    return state == LOCK_SCATTER_TEMPSYNC;
  }
  
  // xlock
  bool can_xlock_soon() {
    if (parent->is_auth())
      return (state == LOCK_SCATTER_LOCK ||
	      state == LOCK_SYNC_LOCK ||
	      state == LOCK_LOCK);
    else
      return false;
  }

  // wrlock
  bool can_wrlock() {
    return 
      state == LOCK_SCATTER ||
      (parent->is_auth() && state == LOCK_LOCK);
  }

  void print(ostream& out) {
    out << "(";
    _print(out);
    if (updated)
      out << " updated";
    out << ")";
  }

};

#endif
