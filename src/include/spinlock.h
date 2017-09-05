// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 * @author Jesse Williamson <jwilliamson@suse.de>
 *
*/

#ifndef CEPH_SPINLOCK_HPP
#define CEPH_SPINLOCK_HPP

#include <atomic>

namespace ceph {
inline namespace version_1_0 {

class spinlock;

inline void spin_lock(std::atomic_flag& lock);
inline void spin_unlock(std::atomic_flag& lock);
inline void spin_lock(ceph::spinlock& lock);
inline void spin_unlock(ceph::spinlock& lock);

/* A pre-packaged spinlock type modelling BasicLockable: */
class spinlock final
{
  std::atomic_flag af = ATOMIC_FLAG_INIT;

  public:
  void lock() {
    ceph::spin_lock(af);
  }
 
  void unlock() noexcept {
    ceph::spin_unlock(af);
  }
};

// Free functions:
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

inline void spin_lock(ceph::spinlock& lock)
{
 lock.lock();
}

inline void spin_unlock(ceph::spinlock& lock)
{
 lock.unlock();
}

inline void spin_lock(ceph::spinlock *lock)
{
 spin_lock(*lock);
}

inline void spin_unlock(ceph::spinlock *lock)
{
 spin_unlock(*lock);
}

} // inline namespace (version)
} // namespace ceph

#endif
