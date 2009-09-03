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


#ifndef __LOCALLOCK_H
#define __LOCALLOCK_H

#include "SimpleLock.h"

class LocalLock : public SimpleLock {
public:
  LocalLock(MDSCacheObject *o, int t) : 
    SimpleLock(o, t) {
    set_state(LOCK_LOCK); // always.
  }

  bool can_wrlock() {
    return !is_xlocked();
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
};


#endif
