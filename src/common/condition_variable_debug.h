// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <condition_variable>
#include <ctime>
#include <pthread.h>
#include "common/ceph_time.h"

namespace ceph {

namespace mutex_debug_detail {
  template<bool> class mutex_debug_impl;
}

class condition_variable_debug {
  using mutex_debug = mutex_debug_detail::mutex_debug_impl<false>;

  pthread_cond_t cond;
  mutex_debug* waiter_mutex;

  condition_variable_debug&
  operator=(const condition_variable_debug&) = delete;
  condition_variable_debug(const condition_variable_debug&) = delete;

public:
  condition_variable_debug();
  ~condition_variable_debug();
  void wait(std::unique_lock<mutex_debug>& lock);
  template<class Predicate>
  void wait(std::unique_lock<mutex_debug>& lock, Predicate pred) {
    while (!pred()) {
      wait(lock);
    }
  }
  template<class Clock, class Duration>
  std::cv_status wait_until(
    std::unique_lock<mutex_debug>& lock,
    const std::chrono::time_point<Clock, Duration>& when) {
    timespec ts = when.to_timespec(when);
    return _wait_until(lock.mutex(), &ts);
  }
  template<class Rep, class Period>
  std::cv_status wait_for(
    std::unique_lock<mutex_debug>& lock,
    const std::chrono::duration<Rep, Period>& awhile) {
    ceph::real_time when{ceph::real_clock::now()};
    when += awhile;
    timespec ts = ceph::real_clock::to_timespec(when);
    return _wait_until(lock.mutex(), &ts);
  }
  template<class Rep, class Period, class Pred>
  bool wait_for(
    std::unique_lock<mutex_debug>& lock,
    const std::chrono::duration<Rep, Period>& awhile,
    Pred pred) {
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
  std::cv_status _wait_until(mutex_debug* mutex, timespec* ts);
};

} // namespace ceph
