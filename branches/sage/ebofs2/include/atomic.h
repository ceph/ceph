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

#ifndef __CEPH_ATOMIC_H
#define __CEPH_ATOMIC_H

#ifdef BUFFER_USE_CCPP
# include "cc++/thread.h"

class atomic_t {
  mutable ost::AtomicCounter nref;    // mutable for const-ness of operator<<
public:
  atomic_t(int i=0) : nref(i) {}
  void get() { ++nref; }
  int put() { --nref; }
  int test() const { return nref; }
};

#else
# include "common/Mutex.h"

class atomic_t {
  Mutex lock;
  int nref;
public:
  atomic_t(int i=0) : lock(false), nref(i) {}
  void get() { 
    lock.Lock();
    ++nref;
    lock.Unlock();
  }
  int put() {
    lock.Lock();
    int r = --nref; 
    lock.Unlock();
    return r;
  }
  int test() const {
    return nref;
  }
};

#endif

#endif
