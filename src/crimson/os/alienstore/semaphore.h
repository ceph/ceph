// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab
#pragma once

#include <semaphore.h>
#include <ctime>
#include <cerrno>
#include <exception>
#include <chrono>

namespace crimson {

// an implementation of std::counting_semaphore<> in C++17 using the POSIX
// semaphore.
//
// LeastMaxValue is ignored, as we don't have different backends optimized
// for different LeastMaxValues
template<unsigned LeastMaxValue = 64>
class counting_semaphore {
  using clock_t = std::chrono::system_clock;
public:
  explicit counting_semaphore(unsigned count) noexcept {
    sem_init(&sem, 0, count);
  }

  counting_semaphore(const counting_semaphore&) = delete;
  counting_semaphore& operator=(const counting_semaphore&) = delete;

  ~counting_semaphore() {
    sem_destroy(&sem);
  }

  void acquire() noexcept {
    for (;;) {
      int err = sem_wait(&sem);
      if (err != 0) {
        if (errno == EINTR) {
          continue;
        } else {
          std::terminate();
        }
      } else {
        break;
      }
    }
  }

  void release(unsigned update = 1) {
    for (; update != 0; --update) {
      int err = sem_post(&sem);
      if (err != 0) {
        std::terminate();
      }
    }
  }

  template<typename Clock, typename Duration>
  bool try_acquire_until(const std::chrono::time_point<Clock, Duration>& abs_time) noexcept {
    auto s = std::chrono::time_point_cast<std::chrono::seconds>(abs_time);
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(abs_time - s);
    struct timespec ts = {
      static_cast<std::time_t>(s.time_since_epoch().count()),
      static_cast<long>(ns.count())
    };
    for (;;) {
      if (int err = sem_timedwait(&sem, &ts); err) {
        if (errno == EINTR) {
          continue;
        } else if (errno == ETIMEDOUT || errno == EINVAL) {
          return false;
        } else {
          std::terminate();
        }
      } else {
        break;
      }
    }
    return true;
  }

  template<typename Rep, typename Period>
  bool try_acquire_for(const std::chrono::duration<Rep, Period>& rel_time) {
    return try_acquire_until(clock_t::now() + rel_time);
  }

private:
  sem_t sem;
};

}
