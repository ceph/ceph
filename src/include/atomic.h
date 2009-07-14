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

#ifdef HAVE_CONFIG_H
# include "acconfig.h"
#endif

#ifdef WITH_CCGNU
/*
 * use commonc++ library AtomicCounter.
 */
# include "cc++/thread.h"

class atomic_t {
  mutable ost::AtomicCounter nref;    // mutable for const-ness of operator<<
public:
  atomic_t(int i=0) : nref(i) {}
  void inc() { ++nref; }
  int dec() { return --nref; }
  int test() const { return nref; }
  void add(int i) { nref += i; }
  void sub(int i) { nref -= i; }
};

#else
/*
 * crappy slow implementation that uses a pthreads spinlock.
 */
#include "common/Spinlock.h"

class atomic_t {
  Spinlock lock;
  long nref;
public:
  atomic_t(int i=0) : lock("atomic_t::lock", false /* no lockdep */), nref(i) {}
  atomic_t(const atomic_t& other);
  int inc() { 
    lock.lock();
    int r = ++nref;
    lock.unlock();
    return r;
  }
  int dec() {
    lock.lock();
    int r = --nref; 
    lock.unlock();
    return r;
  }
  void add(int d) {
    lock.lock();
    nref += d;
    lock.unlock();
  }
  void sub(int d) {
    lock.lock();
    nref -= d;
    lock.unlock();
  }
  int test() const {
    return nref;
  }
};

#endif

#endif
