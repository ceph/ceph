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

#ifndef CEPH_ATOMIC_H
#define CEPH_ATOMIC_H

#ifdef __CEPH__
# include "acconfig.h"
#endif


#ifndef NO_ATOMIC_OPS
//libatomic_ops implementation
#include <atomic_ops.h>
namespace ceph {

class atomic_t {
  AO_t val;
public:
  atomic_t(AO_t i=0) : val(i) {}
  void inc() {
    AO_fetch_and_add1(&val);
  }
  AO_t dec() {
    return AO_fetch_and_sub1_write(&val) - 1;
  }
  void add(AO_t add_me) {
    AO_fetch_and_add(&val, add_me);
  }
  void sub(int sub_me) {
    int sub = 0 - sub_me;
    AO_fetch_and_add_write(&val, (AO_t)sub);
  }
  AO_t read() const {
    // cast away const on the pointer.  this is only needed to build
    // on lenny, but not newer debians, so the atomic_ops.h got fixed
    // at some point.  this hack can go away someday...
    return AO_load_full((AO_t *)&val);  
  }
};
}
#else
/*
 * crappy slow implementation that uses a pthreads spinlock.
 */
#include "include/Spinlock.h"
#include "include/assert.h"

namespace ceph {

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
    assert(nref > 0);
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
    assert(nref >= d);
    nref -= d;
    lock.unlock();
  }
  int read() const {
    return nref;
  }
};

}
#endif
#endif
