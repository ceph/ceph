// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LOG_CLOCK_H
#define CEPH_LOG_CLOCK_H

#include <cstdio>
#include <chrono>
#include <ctime>
#include <sys/time.h>

#include "include/assert.h"
#include "common/ceph_time.h"

namespace ceph {
namespace logging {
class log_clock {
public:
  using duration = timespan;
  using rep = duration::rep;
  using period = duration::period;
  // The second template parameter defaults to the clock's duration
  // type.
  using time_point = std::chrono::time_point<log_clock>;
  static constexpr const bool is_steady = false;

  void coarsen() {
    appropriate_now = coarse_now;
  }

  void refine() {
    appropriate_now = fine_now;
  }

  time_point now() noexcept {
    return appropriate_now();
  }

  static bool is_zero(const time_point& t) {
    return (t == time_point::min());
  }

  // Allow conversion to/from any clock with the same interface as
  // std::chrono::system_clock)
  template<typename Clock, typename Duration>
  static time_point to_system_time_point(
    const std::chrono::time_point<Clock, Duration>& t) {
    return time_point(seconds(Clock::to_time_t(t)) +
		      std::chrono::duration_cast<duration>(
			t.time_since_epoch() % std::chrono::seconds(1)));
  }
  template<typename Clock, typename Duration>
  static std::chrono::time_point<Clock, Duration> to_system_time_point(
    const time_point& t) {
    return (Clock::from_time_t(to_time_t(t)) +
	    std::chrono::duration_cast<Duration>(t.time_since_epoch() %
						 std::chrono::seconds(1)));
  }

  static time_t to_time_t(const time_point& t) noexcept {
    return std::chrono::duration_cast<std::chrono::seconds>(
      t.time_since_epoch()).count();
  }
  static time_point from_time_t(const time_t& t) noexcept {
    return time_point(std::chrono::seconds(t));
  }

  static void to_timespec(const time_point& t, struct timespec& ts) {
    ts.tv_sec = to_time_t(t);
    ts.tv_nsec = (t.time_since_epoch() % std::chrono::seconds(1)).count();
  }
  static struct timespec to_timespec(const time_point& t) {
    struct timespec ts;
    to_timespec(t, ts);
    return ts;
  }
  static time_point from_timespec(const struct timespec& ts) {
    return time_point(std::chrono::seconds(ts.tv_sec) +
		      std::chrono::nanoseconds(ts.tv_nsec));
  }

  static void to_ceph_timespec(const time_point& t,
			       struct ceph_timespec& ts);
  static struct ceph_timespec to_ceph_timespec(const time_point& t);
  static time_point from_ceph_timespec(const struct ceph_timespec& ts);

  static void to_timeval(const time_point& t, struct timeval& tv) {
    tv.tv_sec = to_time_t(t);
    tv.tv_usec = std::chrono::duration_cast<std::chrono::microseconds>(
      t.time_since_epoch() % std::chrono::seconds(1)).count();
  }
  static struct timeval to_timeval(const time_point& t) {
    struct timeval tv;
    to_timeval(t, tv);
    return tv;
  }
  static time_point from_timeval(const struct timeval& tv) {
    return time_point(std::chrono::seconds(tv.tv_sec) +
		      std::chrono::microseconds(tv.tv_usec));
  }

  static double to_double(const time_point& t) {
    return std::chrono::duration<double>(t.time_since_epoch()).count();
  }
  static time_point from_double(const double d) {
    return time_point(std::chrono::duration_cast<duration>(
			std::chrono::duration<double>(d)));
  }
private:
  static time_point coarse_now() {
    return time_point(coarse_real_clock::now().time_since_epoch());
  }
  static time_point fine_now() {
    return time_point(real_clock::now().time_since_epoch());
  }
  time_point(*appropriate_now)() = coarse_now;
};
using log_time = log_clock::time_point;
inline int append_time(const log_time& t, char *out, int outlen) {
  auto tv = log_clock::to_timeval(t);
  std::tm bdt;
  localtime_r(&tv.tv_sec, &bdt);

  auto r = std::snprintf(out, outlen,
			 "%04d-%02d-%02d %02d:%02d:%02d.%06ld",
			 bdt.tm_year + 1900, bdt.tm_mon + 1, bdt.tm_mday,
			 bdt.tm_hour, bdt.tm_min, bdt.tm_sec, tv.tv_usec);
  // Since our caller just adds the return value to something without
  // checking it…
  ceph_assert(r >= 0);
  return r;
}
}
}

#endif
