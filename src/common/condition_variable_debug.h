// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <pthread.h>

#include <condition_variable>
#include <ctime>

#include "common/ceph_time.h"

#include "acconfig.h"

namespace ceph {

namespace mutex_debug_detail {
template <bool Recursive, bool Adaptive>
class mutex_debug_impl;
}

template <bool Adaptive = false>
class condition_variable_debug_impl {
  using mutex_type = mutex_debug_detail::mutex_debug_impl<false, Adaptive>;

  pthread_cond_t cond;
  mutex_type* waiter_mutex;

  condition_variable_debug_impl& operator=(
      const condition_variable_debug_impl&) = delete;
  condition_variable_debug_impl(const condition_variable_debug_impl&) = delete;

public:
  condition_variable_debug_impl();
  ~condition_variable_debug_impl();
  void wait(std::unique_lock<mutex_type>& lock);

  template <class Predicate>
  void
  wait(std::unique_lock<mutex_type>& lock, Predicate pred)
  {
    while (!pred()) {
      wait(lock);
    }
  }

  template <class Clock, class Duration>
  std::cv_status
  wait_until(
      std::unique_lock<mutex_type>& lock,
      const std::chrono::time_point<Clock, Duration>& when)
  {
    if constexpr (Clock::is_steady) {
      // convert from mono_clock to real_clock
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
      std::unique_lock<mutex_type>& lock,
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
      std::unique_lock<mutex_type>& lock,
      const std::chrono::duration<Rep, Period>& awhile,
      Pred pred)
  {
    ceph::real_time when{ceph::real_clock::now()};
    when += awhile;
    timespec ts = ceph::real_clock::to_timespec(when);
    while (!pred()) {
      if ( _wait_until(lock.mutex(), &ts) == std::cv_status::timeout) {
        return pred();
      }
    }
    return true;
  }
  void notify_one();
  void notify_all(bool sloppy = false);
private:
  std::cv_status _wait_until(mutex_type* mutex, timespec* ts);
};

using condition_variable_debug = condition_variable_debug_impl<false>;
#ifdef HAVE_PTHREAD_MUTEX_ADAPTIVE_NP
using condition_variable_adaptive_debug = condition_variable_debug_impl<true>;
#else
using condition_variable_adaptive_debug = condition_variable_debug;
#endif

} // namespace ceph
