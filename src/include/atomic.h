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
#include "include/Spinlock.h"

namespace ceph {
  template <class T>
  class atomic_spinlock_t {
    mutable ceph_spinlock_t lock;
    T val;
  public:
    atomic_spinlock_t(T i=0)
      : val(i) {
      ceph_spin_init(&lock);
    }
    ~atomic_spinlock_t() {
      ceph_spin_destroy(&lock);
    }
    void set(T v) {
      ceph_spin_lock(&lock);
      val = v;
      ceph_spin_unlock(&lock);
    }
    T inc() {
      ceph_spin_lock(&lock);
      T r = ++val;
      ceph_spin_unlock(&lock);
      return r;
    }
    T dec() {
      ceph_spin_lock(&lock);
      T r = --val;
      ceph_spin_unlock(&lock);
      return r;
    }
    void add(T d) {
      ceph_spin_lock(&lock);
      val += d;
      ceph_spin_unlock(&lock);
    }
    void sub(T d) {
      ceph_spin_lock(&lock);
      val -= d;
      ceph_spin_unlock(&lock);
    }
    T read() const {
      T ret;
      ceph_spin_lock(&lock);
      ret = val;
      ceph_spin_unlock(&lock);
      return ret;
    }
    bool compare_and_swap(T o, T n) {
      bool success = false;
      ceph_spin_lock(&lock);
      if (val == o) {
        success = true;
        val = n;
      }
      ceph_spin_unlock(&lock);
      return success;
    }

  private:
    // forbid copying
    atomic_spinlock_t(const atomic_spinlock_t<T> &other);
    atomic_spinlock_t &operator=(const atomic_spinlock_t<T> &rhs);
  };
}

#ifndef NO_ATOMIC_OPS

// libatomic_ops implementation
#define AO_REQUIRE_CAS
#include <atomic_ops.h>

// reinclude our assert to clobber the system one
#include "include/assert.h"

namespace ceph {
  class atomic_t {
    AO_t val;
  public:
    atomic_t(AO_t i=0) : val(i) {}
    void set(AO_t v) {
      AO_store(&val, v);
    }
    AO_t inc() {
      return AO_fetch_and_add1(&val) + 1;
    }
    AO_t dec() {
      return AO_fetch_and_sub1_write(&val) - 1;
    }
    AO_t add(AO_t add_me) {
      return AO_fetch_and_add(&val, add_me) + add_me;
    }
    AO_t sub(AO_t sub_me) {
      AO_t negsub = 0 - sub_me;
      return AO_fetch_and_add_write(&val, negsub) + negsub;
    }
    AO_t read() const {
      // cast away const on the pointer.  this is only needed to build
      // on lenny, but not newer debians, so the atomic_ops.h got fixed
      // at some point.  this hack can go away someday...
      return AO_load_full((AO_t *)&val);
    }
    bool compare_and_swap(AO_t o, AO_t n) {
      return AO_compare_and_swap(&val, o, n);
    }

  private:
    // forbid copying
    atomic_t(const atomic_t &other);
    atomic_t &operator=(const atomic_t &rhs);
  };

#if SIZEOF_AO_T == 8
  typedef atomic_t atomic64_t;
#else
  typedef atomic_spinlock_t<unsigned long long> atomic64_t;
#endif

}

#else
/*
 * crappy slow implementation that uses a pthreads spinlock.
 */
#include "include/Spinlock.h"

namespace ceph {
  typedef atomic_spinlock_t<unsigned> atomic_t;
  typedef atomic_spinlock_t<unsigned long long> atomic64_t;
}

#endif
#endif
