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

#include <atomic>

namespace ceph {

inline void simple_spin_lock(std::atomic_flag& lock)
{
 while(lock.test_and_set(std::memory_order_acquire))
  ;
}

inline void simple_spin_unlock(std::atomic_flag& lock)
{
 lock.clear(std::memory_order_release);
}

inline void simple_spin_lock(std::atomic_flag *lock)
{
 simple_spin_lock(*lock);
}

inline void simple_spin_unlock(std::atomic_flag *lock)
{
 simple_spin_unlock(*lock);
}

} // namespace ceph

namespace ceph {

class Spinlock {
  mutable std::atomic_flag spin_lock = ATOMIC_FLAG_INIT;

public:
  /// acquire spinlock
  void lock() const {
    simple_spin_lock(spin_lock);
  }
  /// release spinlock
  void unlock() const {
    simple_spin_unlock(spin_lock);
  }

  // Scoped control of a Spinlock:
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

inline void ceph_spin_lock(ceph::Spinlock& l)
{
 l.lock();
}

inline void ceph_spin_unlock(ceph::Spinlock& l)
{
 l.unlock();
}

// Pointer parameters:
inline void ceph_spin_lock(ceph::Spinlock *l)
{
 return ceph_spin_lock(*l);
}

inline void ceph_spin_unlock(ceph::Spinlock *l)
{
 return ceph_spin_unlock(*l);
}

} // namespace ceph

#endif
