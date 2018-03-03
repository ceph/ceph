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
#include <thread>

#if defined(__i386__) || defined(__x86_64__)
#  include <immintrin.h>
#endif

#include "common/likely.h"

namespace ceph {
inline namespace version_1_0 {

static inline void emit_pause() {
#if defined(__i386__) || defined(__x86_64__)
  // The spinning part is indistinguishable for CPU without
  // additional hint. On x86 there is PAUSE instruction for
  // that. We definitely want to use it because of:
  //   * not disturbing second thread on the same core (SMT),
  //   * saving power.
  // Although the instruction is available since P4, binary
  // transcription into `rep; nop` allows its decoding even
  // on i386, so no need for `cpuid` or other costly things.
  // For details please refer to:
  //  * "Long Duration Spin-wait Loops on Hyper-Threading
  //     Technology Enabled Intel Processors",
  //  * "Benefitting Power and Performance Sleep Loops".
  _mm_pause();
#endif
}

class spinlock;

inline void spin_lock(std::atomic_bool& locked);
inline void spin_unlock(std::atomic_bool& locked);
inline void spin_lock(ceph::spinlock& locked);
inline void spin_unlock(ceph::spinlock& locked);

/* A pre-packaged spinlock type modelling BasicLockable: */
class spinlock final
{
  // Not using atomic_flag anymore because it doesn't
  // provide the load nor store operation.
  std::atomic_bool locked = false;

  // In contrast to atomic_flag, atomic_bool might be
  // implemented on top of e.g. mutex. However, it is
  // very unlikely we'll face such situation in Ceph.
  static_assert(std::atomic_bool::is_always_lock_free);

public:
  void lock() {
    ceph::spin_lock(locked);
  }
 
  void unlock() noexcept {
    ceph::spin_unlock(locked);
  }
};

// Free functions:
inline void spin_lock(std::atomic_bool& locked)
{
  bool expected = false;
  if (likely(locked.compare_exchange_weak(expected, true,
                                          std::memory_order_acquire,
                                          std::memory_order_relaxed))) {
    return;
  }

  // The contention part.
  do {
    std::size_t tries = 0;

    // There is no need to constantly try LOCK CMPXCHG and enforce
    // x86 CPUs to do costly mem fencing. The algorithm comes from
    // NPTL's pthread_spin_lock() implementation of glibc.
    do {
      emit_pause();

      if if (++tries == 32) {
        // Oops, things went really bad. There was no state change
        // for many iterations. This could happen when lock holder
        // gets stuck because of e.g. being preempted. Most likely
        // other waiters started spinning as well. The best we can
        // do is to limit our losses and yield CPU. Luckily kernel
        // (it's perfectly unaware about the whole situation) will
        // switch to something useful or, at least, blocked holder
        // will get its time quantum faster.
        std::this_thread::yield();
        tries = 0;
      }
    } while (locked.load(std::memory_order_relaxed));

    // The specification of compare_exchange() does not say a word
    // that the value of `expected` can't change, so let's refresh.
    expected = false;
  } while (!locked.compare_exchange_weak(expected, true,
                                         std::memory_order_acquire,
                                         std::memory_order_relaxed));
}

inline void spin_unlock(std::atomic_bool& locked)
{
  locked.store(false, std::memory_order_release);
}

inline void spin_lock(std::atomic_bool *locked)
{
 spin_lock(*locked);
}

inline void spin_unlock(std::atomic_bool *locked)
{
 spin_unlock(*locked);
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
