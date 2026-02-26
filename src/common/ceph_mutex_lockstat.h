// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM Corp
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_COMMON_MUTEX_LOCKSTAT_H
#define CEPH_COMMON_MUTEX_LOCKSTAT_H

#include <bits/fs_fwd.h>
#include <pthread.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <shared_mutex>
#include <system_error>
#include <thread>

#include "mutex_debug.h"

#include <boost/spirit/home/qi/directive/as.hpp>

#include "include/ceph_assert.h"
#include "include/common_fwd.h"

#include "ceph_time.h"
#include "likely.h"
#include "lockstat.h"

namespace ceph {
namespace lockstat_detail {


template <typename mutex_base>
class mutex_lockstat_impl : public LockStat, public mutex_base {

public:
  static constexpr LockStatTraits::LockStatType LockType =
      LockStatTraits::LockStatType::MUTEX;

  mutex_lockstat_impl(const LockStatTraits* traits) :
    LockStat(LockType, traits), mutex_base()
  {}

  // Mutex is Destructible
  ~mutex_lockstat_impl() = default;

  // Mutex concept is non-Copyable
  mutex_lockstat_impl(const mutex_lockstat_impl&) = delete;
  mutex_lockstat_impl& operator=(const mutex_lockstat_impl&) = delete;

  // Mutex concept is non-Movable
  mutex_lockstat_impl(mutex_lockstat_impl&&) = delete;
  mutex_lockstat_impl& operator=(mutex_lockstat_impl&&) = delete;

  void
  lock()
  {
    const auto wait_start_clock =
        unlikely(lockstat_detail::LockStat::is_lockstat_enabled())
            ? lockstat_clock::now()
            : lockstat_clock::zero();
    mutex_base::lock();
    if (unlikely(wait_start_clock != lockstat_clock::zero())) {
      record_wait_time(lockstat_clock::now() - wait_start_clock, LockMode::WRITE);
    }
  }

  bool
  try_lock()
  {
    const auto wait_start_clock =
        unlikely(
            g_start_cycles.load(std::memory_order_relaxed) != lockstat_clock::zero())
            ? lockstat_clock::now()
            : lockstat_clock::zero();
    if (mutex_base::try_lock()) {
      if (unlikely(wait_start_clock != lockstat_clock::zero())) {
        record_wait_time(
            lockstat_clock::now() - wait_start_clock, LockMode::TRY_WRITE);
      }
      return true;
    } else {
      return false;
    }
  }

  void
  unlock()
  {
    mutex_base::unlock();
  }
};

class shared_mutex_lockstat : public LockStat, public std::shared_mutex {
  using mutex_base = std::shared_mutex;
  static constexpr LockStatTraits::LockStatType LockType =
      LockStatTraits::LockStatType::RW_LOCK;


public:
  shared_mutex_lockstat(const LockStatTraits* traits) :
    LockStat(LockType, traits), std::shared_mutex()
  {}

  ~shared_mutex_lockstat() = default;

  // exclusive locking
  void
  lock()
  {
    const auto wait_start_clock =
        unlikely(lockstat_detail::LockStat::is_lockstat_enabled())
            ? lockstat_clock::now()
            : lockstat_clock::zero();
    mutex_base::lock();
    if (unlikely(wait_start_clock != lockstat_clock::zero())) {
      record_wait_time(lockstat_clock::now() - wait_start_clock, LockMode::WRITE);
    }
  }

  bool
  try_lock()
  {
    const auto wait_start_clock =
        unlikely(
            g_start_cycles.load(std::memory_order_relaxed) != lockstat_clock::zero())
            ? lockstat_clock::now()
            : lockstat_clock::zero();
    if (mutex_base::try_lock()) {
      if (unlikely(wait_start_clock != lockstat_clock::zero())) {
        record_wait_time(
            lockstat_clock::now() - wait_start_clock, LockMode::TRY_WRITE);
      }
      return true;
    } else {
      return false;
    }
  }

  void
  unlock()
  {
    mutex_base::unlock();
  }

  // shared locking
  void
  lock_shared()
  {
    const auto wait_start_clock =
        unlikely(lockstat_detail::LockStat::is_lockstat_enabled())
            ? lockstat_clock::now()
            : lockstat_clock::zero();
    mutex_base::lock();
    if (unlikely(wait_start_clock != lockstat_clock::zero())) {
      record_wait_time(lockstat_clock::now() - wait_start_clock, LockMode::READ);
    }
  }

  bool
  try_lock_shared()
  {
    const auto wait_start_clock =
        unlikely(
            g_start_cycles.load(std::memory_order_relaxed) != lockstat_clock::zero())
            ? lockstat_clock::now()
            : lockstat_clock::zero();
    if (mutex_base::try_lock()) {
      if (unlikely(wait_start_clock != lockstat_clock::zero())) {
        record_wait_time(
            lockstat_clock::now() - wait_start_clock, LockMode::TRY_READ);
      }
      return true;
    } else {
      return false;
    }
  }

  void
  unlock_shared()
  {
    mutex_base::unlock_shared();
  }
};

class condition_variable_lockstat {
  using mutex_lockstat = lockstat_detail::mutex_lockstat_impl<std::mutex>;

  pthread_cond_t cond;
  mutex_lockstat* waiter_mutex;

  condition_variable_lockstat& operator=(
      const condition_variable_lockstat&) = delete;
  condition_variable_lockstat(const condition_variable_lockstat&) = delete;

public:
  condition_variable_lockstat() :
    waiter_mutex{nullptr}
  {
    int r = pthread_cond_init(&cond, nullptr);
    if (r) {
      throw std::system_error(r, std::generic_category());
    }
  }

  ~condition_variable_lockstat() { pthread_cond_destroy(&cond); }

  void
  wait(std::unique_lock<mutex_lockstat>& lock)
  {
    // make sure this cond is used with one mutex only
    ceph_assert(waiter_mutex == nullptr || waiter_mutex == lock.mutex());
    waiter_mutex = lock.mutex();
    if (int r = pthread_cond_wait(&cond, waiter_mutex->native_handle());
        r != 0) {
      throw std::system_error(r, std::generic_category());
    }
  }

  template <class Predicate>
  void
  wait(std::unique_lock<mutex_lockstat>& lock, Predicate pred)
  {
    while (!pred()) {
      wait(lock);
    }
  }

  template <class Clock, class Duration>
  std::cv_status
  wait_until(
      std::unique_lock<mutex_lockstat>& lock,
      const std::chrono::time_point<Clock, Duration>& when)
  {
    if constexpr (Clock::is_steady) {
      // convert from lockstat_clock to real_clock
      auto real_when = ceph::real_clock::now();
      const auto delta = when - Clock::now();
      real_when += std::chrono::ceil<typename Clock::duration>(delta);
      timespec ts = ceph::real_clock::to_timespec(real_when);
      return _wait_until(lock.mutex(), &ts);
    } else {
      timespec ts = Clock::to_timespec(when);
      return _wait_until(lock.mutex(), &ts);
    }
  }

  template <class Rep, class Period>
  std::cv_status
  wait_for(
      std::unique_lock<mutex_lockstat>& lock,
      const std::chrono::duration<Rep, Period>& awhile)
  {
    ceph::real_time when{ceph::real_clock::now()};
    when += awhile;
    timespec ts = ceph::real_clock::to_timespec(when);
    return _wait_until(lock.mutex(), &ts);
  }

  template <class Rep, class Period, class Pred>
  bool
  wait_for(
      std::unique_lock<mutex_lockstat>& lock,
      const std::chrono::duration<Rep, Period>& awhile,
      Pred pred)
  {
    ceph::real_time when{ceph::real_clock::now()};
    when += awhile;
    timespec ts = ceph::real_clock::to_timespec(when);
    while (!pred()) {
      if (_wait_until(lock.mutex(), &ts) == std::cv_status::timeout) {
        return pred();
      }
    }
    return true;
  }

  void
  notify_one()
  {

    if (int r = pthread_cond_signal(&cond); r != 0) {
      throw std::system_error(r, std::generic_category());
    }
  }

  void
  notify_all()
  {

    if (int r = pthread_cond_broadcast(&cond); r != 0) {
      throw std::system_error(r, std::generic_category());
    }
  }

private:
  std::cv_status
  _wait_until(mutex_lockstat* mutex, timespec* ts)
  {
    // make sure this cond is used with one mutex only
    ceph_assert(waiter_mutex == nullptr || waiter_mutex == mutex);
    waiter_mutex = mutex;

    int r = pthread_cond_timedwait(&cond, waiter_mutex->native_handle(), ts);
    switch (r) {
    case 0:
      return std::cv_status::no_timeout;
    case ETIMEDOUT:
      return std::cv_status::timeout;
    default:
      throw std::system_error(r, std::generic_category());
    }
  }
};

} // namespace lockstat_detail

using mutex_lockstat = lockstat_detail::mutex_lockstat_impl<std::mutex>;
using mutex_recursive_lockstat =
    lockstat_detail::mutex_lockstat_impl<std::recursive_mutex>;
using shared_mutex_lockstat = lockstat_detail::shared_mutex_lockstat;
} // namespace ceph

#endif // CEPH_COMMON_MUTEX_LOCKSTAT_H
