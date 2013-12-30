// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 * @author Sage Weil <sage@newdream.net>
 */

#ifndef CEPH_ATOMIC_H
#define CEPH_ATOMIC_H

#ifdef __CEPH__
# include "acconfig.h"
#endif

#include <stdlib.h>

#ifndef NO_ATOMIC_OPS

// libatomic_ops implementation
#include <atomic_ops.h>

// reinclude our assert to clobber the system one
#include "include/assert.h"

namespace ceph {
  class atomic_t {
    AO_t val;
  public:
    atomic_t(AO_t i=0) : val(i) {}
    void set(size_t v) {
      AO_store(&val, v);
    }
    AO_t inc() {
      return AO_fetch_and_add1(&val) + 1;
    }
    AO_t dec() {
      return AO_fetch_and_sub1_write(&val) - 1;
    }
    void add(AO_t add_me) {
      AO_fetch_and_add(&val, add_me);
    }
    void sub(int sub_me) {
      int negsub = 0 - sub_me;
      AO_fetch_and_add_write(&val, (AO_t)negsub);
    }
    AO_t read() const {
      // cast away const on the pointer.  this is only needed to build
      // on lenny, but not newer debians, so the atomic_ops.h got fixed
      // at some point.  this hack can go away someday...
      return AO_load_full((AO_t *)&val);
    }
  private:
    // forbid copying
    atomic_t(const atomic_t &other);
    atomic_t &operator=(const atomic_t &rhs);
  };
}
#else
/*
 * crappy slow implementation that uses a pthreads spinlock.
 */
#include "include/Spinlock.h"

namespace ceph {
  class atomic_t {
    mutable ceph_spinlock_t lock;
    signed long val;
  public:
    atomic_t(int i=0)
      : val(i) {
      ceph_spin_init(&lock);
    }
    ~atomic_t() {
      ceph_spin_destroy(&lock);
    }
    void set(size_t v) {
      ceph_spin_lock(&lock);
      val = v;
      ceph_spin_unlock(&lock);
    }
    int inc() {
      ceph_spin_lock(&lock);
      int r = ++val;
      ceph_spin_unlock(&lock);
      return r;
    }
    int dec() {
      ceph_spin_lock(&lock);
      int r = --val;
      ceph_spin_unlock(&lock);
      return r;
    }
    void add(int d) {
      ceph_spin_lock(&lock);
      val += d;
      ceph_spin_unlock(&lock);
    }
    void sub(int d) {
      ceph_spin_lock(&lock);
      val -= d;
      ceph_spin_unlock(&lock);
    }
    int read() const {
      signed long ret;
      ceph_spin_lock(&lock);
      ret = val;
      ceph_spin_unlock(&lock);
      return ret;
    }
  private:
    // forbid copying
    atomic_t(const atomic_t &other);
    atomic_t &operator=(const atomic_t &rhs);
  };
}
#endif
#endif
