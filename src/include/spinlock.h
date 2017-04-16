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

#ifndef CEPH_SPINLOCK_HPP
#define CEPH_SPINLOCK_HPP

#include <atomic>

namespace ceph {

inline void spin_lock(std::atomic_flag& lock)
{
 while(lock.test_and_set(std::memory_order_acquire))
  ;
}

inline void spin_unlock(std::atomic_flag& lock)
{
 lock.clear(std::memory_order_release);
}

inline void spin_lock(std::atomic_flag *lock)
{
 spin_lock(*lock);
}

inline void spin_unlock(std::atomic_flag *lock)
{
 spin_unlock(*lock);
}

} // namespace ceph

namespace ceph {

class Spinlock {
  mutable std::atomic_flag spinlock { false };

public:
  /// acquire spinlock
  void lock() const {
    ceph::spin_lock(spinlock);
  }
  /// release spinlock
  void unlock() const {
    ceph::spin_unlock(spinlock);
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

} // namespace ceph

namespace ceph {

inline void spin_lock(ceph::Spinlock& lock)
{
 lock.lock();
}

inline void spin_unlock(ceph::Spinlock& lock)
{
 lock.unlock();
}

inline void spin_lock(ceph::Spinlock *lock)
{
 spin_lock(*lock);
}

inline void spin_unlock(ceph::Spinlock *lock)
{
 spin_unlock(*lock);
}

} // namespace ceph


// FIXME: These names should go away as a final patch:
namespace ceph {

inline void simple_spin_lock(std::atomic_flag& lock)
{
 return spin_lock(lock);
}

inline void simple_spin_unlock(std::atomic_flag& lock)
{
 return spin_unlock(lock);
}

inline void simple_spin_lock(std::atomic_flag *lock)
{
 return spin_lock(lock);
}

inline void simple_spin_unlock(std::atomic_flag *lock)
{
 return spin_unlock(lock);
}

} // namespace ceph

// FIXME: These names should go away as a final patch:
namespace ceph {

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
