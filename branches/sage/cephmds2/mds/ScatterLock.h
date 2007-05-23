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


#ifndef __SCATTERLOCK_H
#define __SCATTERLOCK_H

#include "SimpleLock.h"


// lock state machine states.
#define LOCK_SYNC__        // rdlocks allowed (e.g., for stat)
#define LOCK_GSYNCS   -20  // waiting for replicas to gather
#define LOCK_SCATTER   21  // mtime updates on replicas allowed, no reads.
#define LOCK_GSCATTERS 22  // waiting for rdlocks to release

inline const char *get_scatterlock_state_name(int s) {
  switch(s) {
  case LOCK_SYNC: return "sync";
  case LOCK_GSYNCS: return "gsyncs";
  case LOCK_SCATTER: return "scatter";
  case LOCK_GSCATTERS: return "gscatters";
  default: assert(0);
  }
}

class ScatterLock : public SimpleLock {
  int num_wrlock;
  
public:
  ScatterLock(MDSCacheObject *o, int t, int wo) : SimpleLock(o, t, wo) {}

  char get_replica_state() {
    switch (state) {
    case LOCK_SYNC: 
    case LOCK_GSYNCS:
    case LOCK_GSCATTERS:
      return LOCK_SYNC;
    case LOCK_SCATTER:
      return LOCK_SCATTER;
    default:
      assert(0);
    }
  }

  void replicate_relax() {
    if (state == LOCK_SYNC && !is_rdlocked())
      state = LOCK_SCATTER;
  }

  // rdlock
  bool can_rdlock(MDRequest *mdr) {
    return state == LOCK_SYNC;
  }
  bool can_rdlock_soon() {
    return state == LOCK_SYNC || state == LOCK_GSYNCS;
  }

  // wrlock
  bool can_wrlock() {
    return state == LOCK_SCATTER;
  }
  void get_wrlock() {
    assert(state == LOCK_SCATTER);
    ++num_wrlock;
  }
  void put_wrlock() {
    --num_wrlock;
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
    //if (l.is_xlocked())
    //out << " x=" << l.get_xlocked_by();
    if (is_wrlocked()) 
      out << " wr=" << get_num_wrlocks();
    out << ")";
  }

};

#endif
