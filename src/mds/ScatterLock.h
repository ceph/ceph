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

class ScatterLock : public SimpleLock {
  bool dirty, flushing;
  bool scatter_wanted;
  utime_t last_scatter;

public:
  xlist<ScatterLock*>::item xlistitem_updated;
  utime_t update_stamp;

  ScatterLock(MDSCacheObject *o, int t, int ws) : 
    SimpleLock(o, t, ws),
    dirty(false), flushing(false), scatter_wanted(false),
    xlistitem_updated(this) {}
  ~ScatterLock() {
    xlistitem_updated.remove_myself();   // FIXME this should happen sooner, i think...
  }

  void set_scatter_wanted() { scatter_wanted = true; }
  void clear_scatter_wanted() { scatter_wanted = false; }
  bool get_scatter_wanted() { return scatter_wanted; }

  void mark_dirty() { 
    if (!dirty) {
      if (!flushing) 
	parent->get(MDSCacheObject::PIN_DIRTYSCATTERED);
      dirty = true;
    }
  }
  void start_flush() {
    flushing |= dirty;
    dirty = false;
  }
  void finish_flush() {
    if (flushing) {
      flushing = false;
      if (!dirty) {
	parent->put(MDSCacheObject::PIN_DIRTYSCATTERED);
	parent->clear_dirty_scattered(type);
      }
    }
  }
  void clear_dirty() {
    start_flush();
    finish_flush();
  }
  
  void set_last_scatter(utime_t t) { last_scatter = t; }
  utime_t get_last_scatter() { return last_scatter; }

  void infer_state_from_strong_rejoin(int rstate, bool locktoo) {
    if (rstate == LOCK_MIX || 
	rstate == LOCK_MIX_LOCK || // replica still has wrlocks?
	rstate == LOCK_MIX_SYNC || // "
	rstate == LOCK_MIX_TSYN)  // "
      state = LOCK_MIX;
    else if (locktoo && rstate == LOCK_LOCK)
      state = LOCK_LOCK;
  }

  void print(ostream& out) {
    out << "(";
    _print(out);
    if (dirty)
      out << " dirty";
    if (flushing)
      out << " flushing";
    out << ")";
  }
};

#endif
