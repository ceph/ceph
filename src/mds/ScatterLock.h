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
#define LOCK_SYNC__          // R .  R .  rdlocks allowed on auth and replicas
#define LOCK_GLOCKS     -20  // r .  r .  waiting for replicas+rdlocks (auth), or rdlocks to release (replica)
#define LOCK_GSCATTERS  -28  // r .  r .  

#define LOCK_GSYNCL__        // . w       LOCK on replica.
#define LOCK_LOCK__          // . W  . .
#define LOCK_GTEMPSYNCL -21  // . w       LOCK on replica.

#define LOCK_GLOCKC     -22  // . wp . wp waiting for replicas+wrlocks (auth), or wrlocks to release (replica)
#define LOCK_SCATTER     23  // . Wp . WP mtime updates on replicas allowed, no reads.  stable here.
#define LOCK_GTEMPSYNCC -24  // . wp . wp GLOCKC|LOCK on replica

#define LOCK_GSCATTERT  -25  // r .       LOCK on replica.
#define LOCK_GLOCKT     -26  // r .       LOCK on replica.
#define LOCK_TEMPSYNC    27  // R .       LOCK on replica.


inline const char *get_scatterlock_state_name(int s) {
  switch(s) {
  case LOCK_SYNC: return "Sync";
  case LOCK_GLOCKS: return "gLockS";
  case LOCK_GSCATTERS: return "gScatterS";
    
  case LOCK_GSYNCL: return "gSyncL";
  case LOCK_LOCK: return "Lock";
  case LOCK_GTEMPSYNCL: return "gTempsyncL";
    
  case LOCK_GLOCKC: return "gLockC";
  case LOCK_SCATTER: return "sCatter";
  case LOCK_GTEMPSYNCC: return "gTempsyncC";
    
  case LOCK_GSCATTERT: return "gsCatterT";
  case LOCK_GLOCKT: return "gLockT";
  case LOCK_TEMPSYNC: return "Tempsync";
    
  default: assert(0); return 0;
  }
}

class ScatterLock : public SimpleLock {
  int num_wrlock;
  bool updated;
  utime_t last_scatter;

public:
  xlist<ScatterLock*>::item xlistitem_autoscattered;

  ScatterLock(MDSCacheObject *o, int t, int wo) : 
    SimpleLock(o, t, wo),
    num_wrlock(0),
    updated(false),
    xlistitem_autoscattered(this) {}

  int get_replica_state() {
    switch (state) {
    case LOCK_SYNC: 
      return LOCK_SYNC;
      
    case LOCK_GSCATTERS:  // hrm.
    case LOCK_GLOCKS:
    case LOCK_GSYNCL:
    case LOCK_LOCK:
    case LOCK_GTEMPSYNCL:
    case LOCK_GLOCKC:
      return LOCK_LOCK;

    case LOCK_SCATTER:
      return LOCK_SCATTER;

    case LOCK_GTEMPSYNCC:
    case LOCK_GSCATTERT:
    case LOCK_GLOCKT:
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
  bool can_rdlock(MDRequest *mdr) {
    return state == LOCK_SYNC || state == LOCK_TEMPSYNC;
  }
  bool can_rdlock_soon() {
    return state == LOCK_GTEMPSYNCC;
  }
  
  // xlock
  bool can_xlock_soon() {
    if (parent->is_auth())
      return (state == LOCK_GLOCKC ||
	      state == LOCK_GLOCKS);
    else
      return false;
  }

  // wrlock
  bool can_wrlock() {
    return state == LOCK_SCATTER || state == LOCK_LOCK;
  }
  void get_wrlock() {
    assert(can_wrlock());
    if (num_wrlock == 0) parent->get(MDSCacheObject::PIN_LOCK);
    ++num_wrlock;
  }
  void put_wrlock() {
    --num_wrlock;
    if (num_wrlock == 0) parent->put(MDSCacheObject::PIN_LOCK);
  }
  bool is_wrlocked() { return num_wrlock > 0; }
  int get_num_wrlocks() { return num_wrlock; }

  void print(ostream& out) {
    out << "(";
    out << get_lock_type_name(get_type()) << " ";
    out << get_scatterlock_state_name(get_state());
    if (!get_gather_set().empty()) out << " g=" << get_gather_set();
    if (is_rdlocked()) 
      out << " r=" << get_num_rdlocks();
    if (is_xlocked())
      out << " x=" << get_xlocked_by();
    if (is_wrlocked()) 
      out << " wr=" << get_num_wrlocks();
    if (updated)
      out << " updated";
    out << ")";
  }

};

#endif
