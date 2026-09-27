// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <condition_variable>
#include <mutex>

#include "acconfig.h"

#ifdef HAVE_PTHREAD_MUTEX_ADAPTIVE_NP

#include <pthread.h>

#include <ctime>
#include <system_error>

#include "common/ceph_time.h"
#include "common/likely.h"
#include "include/ceph_assert.h"

namespace ceph {

// pthread adaptive mutex primitive. Public ceph::adaptive_mutex is aliased in
// ceph_mutex.h (plain, lockstat-wrapped, or debug) depending on build mode.
class adaptive_mutex_impl {
  pthread_mutex_t m;

  void
  _init()
  {
    pthread_mutexattr_t a;
    int r = pthread_mutexattr_init(&a);
    ceph_assert(r == 0);
    r = pthread_mutexattr_settype(&a, PTHREAD_MUTEX_ADAPTIVE_NP);
    ceph_assert(r == 0);
    r = pthread_mutex_init(&m, &a);
    ceph_assert(r == 0);
    pthread_mutexattr_destroy(&a);
  }

public:
  adaptive_mutex_impl() { _init(); }

  // Accept and discard naming / lockdep args used by make_mutex factories.
  template <typename... Args>
  explicit adaptive_mutex_impl(Args&&...)
  {
    _init();
  }

  ~adaptive_mutex_impl()
  {
    int r = pthread_mutex_destroy(&m);
    ceph_assert(r == 0);
  }

  adaptive_mutex_impl(const adaptive_mutex_impl&) = delete;
  adaptive_mutex_impl& operator=(const adaptive_mutex_impl&) = delete;
  adaptive_mutex_impl(adaptive_mutex_impl&&) = delete;
  adaptive_mutex_impl& operator=(adaptive_mutex_impl&&) = delete;

  void
  lock()
  {
    int r = pthread_mutex_lock(&m);
    if (unlikely(r == EPERM || r == EDEADLK || r == EBUSY)) {
      throw std::system_error(r, std::generic_category());
    }
    ceph_assert(r == 0);
  }

  bool
  try_lock()
  {
    int r = pthread_mutex_trylock(&m);
    switch (r) {
    case 0:
      return true;
    case EBUSY:
      return false;
    default:
      throw std::system_error(r, std::generic_category());
    }
  }

  void
  unlock() noexcept
  {
    int r = pthread_mutex_unlock(&m);
    ceph_assert(r == 0);
  }

  pthread_mutex_t*
  native_handle()
  {
    return &m;
  }
};

class adaptive_condition_variable_impl {
  pthread_cond_t cond;

  adaptive_condition_variable_impl& operator=(
      const adaptive_condition_variable_impl&) = delete;
  adaptive_condition_variable_impl(
      const adaptive_condition_variable_impl&) = delete;

  std::cv_status
  _wait_until(adaptive_mutex_impl* mutex, timespec* ts)
  {
    int r = pthread_cond_timedwait(&cond, mutex->native_handle(), ts);
    switch (r) {
    case 0:
      return std::cv_status::no_timeout;
    case ETIMEDOUT:
      return std::cv_status::timeout;
    default:
      throw std::system_error(r, std::generic_category());
    }
  }

public:
  adaptive_condition_variable_impl()
  {
    int r = pthread_cond_init(&cond, nullptr);
    if (r) {
      throw std::system_error(r, std::generic_category());
    }
  }

  ~adaptive_condition_variable_impl() { pthread_cond_destroy(&cond); }

  void
  wait(std::unique_lock<adaptive_mutex_impl>& lock)
  {
    if (int r = pthread_cond_wait(&cond, lock.mutex()->native_handle());
        r != 0) {
      throw std::system_error(r, std::generic_category());
    }
  }

  template <class Predicate>
  void
  wait(std::unique_lock<adaptive_mutex_impl>& lock, Predicate pred)
  {
    while (!pred()) {
      wait(lock);
    }
  }

  template <class Clock, class Duration>
  std::cv_status
  wait_until(
      std::unique_lock<adaptive_mutex_impl>& lock,
      const std::chrono::time_point<Clock, Duration>& when)
  {
    if constexpr (Clock::is_steady) {
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
      std::unique_lock<adaptive_mutex_impl>& lock,
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
      std::unique_lock<adaptive_mutex_impl>& lock,
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
  notify_one() noexcept
  {
    pthread_cond_signal(&cond);
  }

  void
  notify_all() noexcept
  {
    pthread_cond_broadcast(&cond);
  }
};

} // namespace ceph

#else // !HAVE_PTHREAD_MUTEX_ADAPTIVE_NP

namespace ceph {
using adaptive_mutex_impl = std::mutex;
using adaptive_condition_variable_impl = std::condition_variable;
} // namespace ceph

#endif // HAVE_PTHREAD_MUTEX_ADAPTIVE_NP
