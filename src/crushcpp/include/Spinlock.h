// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 * @author Sage Weil <sage@inktank.com>
 */

#ifndef CEPH_SPINLOCK_H
#define CEPH_SPINLOCK_H

#include "acconfig.h"

#include <pthread.h>

typedef struct {
#ifdef HAVE_PTHREAD_SPINLOCK
  pthread_spinlock_t lock;
#else
  pthread_mutex_t lock;
#endif
} ceph_spinlock_t;

#ifdef HAVE_PTHREAD_SPINLOCK

static inline int ceph_spin_init(ceph_spinlock_t *l)
{
  return pthread_spin_init(&l->lock, PTHREAD_PROCESS_PRIVATE);
}

static inline int ceph_spin_destroy(ceph_spinlock_t *l)
{
  return pthread_spin_destroy(&l->lock);
}

static inline int ceph_spin_lock(ceph_spinlock_t *l)
{
  return pthread_spin_lock(&l->lock);
}

static inline int ceph_spin_unlock(ceph_spinlock_t *l)
{
  return pthread_spin_unlock(&l->lock);
}

#else /* !HAVE_PTHREAD_SPINLOCK */

static inline int ceph_spin_init(ceph_spinlock_t *l)
{
  return pthread_mutex_init(&l->lock, NULL);
}

static inline int ceph_spin_destroy(ceph_spinlock_t *l)
{
  return pthread_mutex_destroy(&l->lock);
}

static inline int ceph_spin_lock(ceph_spinlock_t *l)
{
  return pthread_mutex_lock(&l->lock);
}

static inline int ceph_spin_unlock(ceph_spinlock_t *l)
{
  return pthread_mutex_unlock(&l->lock);
}

#endif

class Spinlock {
  mutable ceph_spinlock_t _lock;

public:
  Spinlock() {
    ceph_spin_init(&_lock);
  }
  ~Spinlock() {
    ceph_spin_destroy(&_lock);
  }

  // don't allow copying.
  void operator=(Spinlock& s);
  Spinlock(const Spinlock& s);

  /// acquire spinlock
  void lock() const {
    ceph_spin_lock(&_lock);
  }
  /// release spinlock
  void unlock() const {
    ceph_spin_unlock(&_lock);
  }

  class Locker {
    const Spinlock& spinlock;
  public:
    Locker(const Spinlock& s) : spinlock(s) {
      spinlock.lock();
    }
    ~Locker() {
      spinlock.unlock();
    }
  };
};

#endif
