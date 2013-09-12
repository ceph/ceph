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

#include <pthread.h>

class Spinlock {
  mutable pthread_spinlock_t _lock;

public:
  Spinlock() {
    pthread_spin_init(&_lock, PTHREAD_PROCESS_PRIVATE);
  }
  ~Spinlock() {
    pthread_spin_destroy(&_lock);
  }

  // don't allow copying.
  void operator=(Spinlock& s);
  Spinlock(const Spinlock& s);

  /// acquire spinlock
  void lock() const {
    pthread_spin_lock(&_lock);
  }
  /// release spinlock
  void unlock() const {
    pthread_spin_unlock(&_lock);
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
