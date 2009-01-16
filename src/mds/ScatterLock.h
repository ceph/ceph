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

  void print(ostream& out) {
    out << "(";
    _print(out);
    if (updated)
      out << " updated";
    out << ")";
  }
};

typedef ScatterLock FileLock;

#endif
