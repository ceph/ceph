// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#ifndef COMMON_CEPH_TIME_H
#define COMMON_CEPH_TIME_H

#include <chrono>
#include <iostream>
#include <string>
#include <optional>
#include <fmt/chrono.h>
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif
#include <sys/time.h>

#if defined(__APPLE__)
#include <sys/_types/_timespec.h>

#define CLOCK_REALTIME_COARSE CLOCK_REALTIME
#define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC

int clock_gettime(int clk_id, struct timespec *tp);
#endif

#ifdef _WIN32
// Clock precision:
// mingw < 8.0.1:
//   * CLOCK_REALTIME: ~10-55ms (GetSystemTimeAsFileTime)
// mingw >= 8.0.1:
//   * CLOCK_REALTIME: <1us (GetSystemTimePreciseAsFileTime)
//   * CLOCK_REALTIME_COARSE: ~10-55ms (GetSystemTimeAsFileTime)
//
// * CLOCK_MONOTONIC: <1us if TSC is usable, ~10-55ms otherwise
//                    (QueryPerformanceCounter)
// https://github.com/mirror/mingw-w64/commit/dcd990ed423381cf35702df9495d44f1979ebe50
#ifndef CLOCK_REALTIME_COARSE
  #define CLOCK_REALTIME_COARSE CLOCK_REALTIME
#endif
#ifndef CLOCK_MONOTONIC_COARSE
  #define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC
#endif
#endif

struct ceph_timespec;

namespace ceph {
// Currently we use a 64-bit count of nanoseconds.

// We could, if we wished, use a struct holding a uint64_t count
// of seconds and a uint32_t count of nanoseconds.

// At least this way we can change it to something else if we
// want.
typedef uint64_t rep;


// duration is the concrete time representation for our code in the
// case that we are only interested in durations between now and the
// future. Using it means we don't have to have EVERY function that
// deals with a duration be a template. We can do so for user-facing
// APIs, however.
typedef std::chrono::duration<rep, std::nano> timespan;


// Like the above but signed.
typedef int64_t signed_rep;

// Similar to the above but for durations that can specify
// differences between now and a time point in the past.
typedef std::chrono::duration<signed_rep, std::nano> signedspan;

template<typename Duration>
struct timeval to_timeval(Duration d) {
  struct timeval tv;
  auto sec = std::chrono::duration_cast<std::chrono::seconds>(d);
  tv.tv_sec = sec.count();
  auto usec = std::chrono::duration_cast<std::chrono::microseconds>(d-sec);
  tv.tv_usec = usec.count();
  return tv;
}

// We define our own clocks so we can have our choice of all time
// sources supported by the operating system. With the standard
// library the resolution and cost are unspecified. (For example,
// the libc++ system_clock class gives only microsecond
// resolution.)

// One potential issue is that we should accept system_clock
// timepoints in user-facing APIs alongside (or instead of)
// ceph::real_clock times.

// High-resolution real-time clock
class real_clock {
public:
  typedef timespan duration;
  typedef duration::rep rep;
  typedef duration::period period;
  // The second template parameter defaults to the clock's duration
  // type.
  typedef std::chrono::time_point<real_clock> time_point;
  static constexpr const bool is_steady = false;

  static time_point now() noexcept {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return from_timespec(ts);
  }

  static bool is_zero(const time_point& t) {
    return (t == zero());
  }

  static time_point zero() {
    return time_point();
  }

  // Allow conversion to/from any clock with the same interface as
  // std::chrono::system_clock)
  template<typename Clock, typename Duration>
  static time_point to_system_time_point(
    const std::chrono::time_point<Clock, Duration>& t) {
    return time_point(seconds(Clock::to_time_t(t)) +
		      std::chrono::duration_cast<duration>(t.time_since_epoch() %
							   std::chrono::seconds(1)));
  }
  template<typename Clock, typename Duration>
  static std::chrono::time_point<Clock, Duration> to_system_time_point(
    const time_point& t) {
    return (Clock::from_time_t(to_time_t(t)) +
	    std::chrono::duration_cast<Duration>(t.time_since_epoch() %
						 std::chrono::seconds(1)));
  }

  static time_t to_time_t(const time_point& t) noexcept {
    return std::chrono::duration_cast<std::chrono::seconds>(t.time_since_epoch()).count();
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
};

// Low-resolution but preusmably faster real-time clock
class coarse_real_clock {
public:
  typedef timespan duration;
  typedef duration::rep rep;
  typedef duration::period period;
  // The second template parameter defaults to the clock's duration
  // type.
  typedef std::chrono::time_point<coarse_real_clock> time_point;
  static constexpr const bool is_steady = false;

  static time_point now() noexcept {
    struct timespec ts;
#if defined(CLOCK_REALTIME_COARSE)
    // Linux systems have _COARSE clocks.
    clock_gettime(CLOCK_REALTIME_COARSE, &ts);
#elif defined(CLOCK_REALTIME_FAST)
    // BSD systems have _FAST clocks.
    clock_gettime(CLOCK_REALTIME_FAST, &ts);
#else
    // And if we find neither, you may wish to consult your system's
    // documentation.
#warning Falling back to CLOCK_REALTIME, may be slow.
    clock_gettime(CLOCK_REALTIME, &ts);
#endif
    return from_timespec(ts);
  }

  static bool is_zero(const time_point& t) {
    return (t == zero());
  }

  static time_point zero() {
    return time_point();
  }

  static time_t to_time_t(const time_point& t) noexcept {
    return std::chrono::duration_cast<std::chrono::seconds>(
      t.time_since_epoch()).count();
  }
  static time_point from_time_t(const time_t t) noexcept {
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
};

// High-resolution monotonic clock
class mono_clock {
public:
  typedef signedspan duration;
  typedef duration::rep rep;
  typedef duration::period period;
  typedef std::chrono::time_point<mono_clock> time_point;
  static constexpr const bool is_steady = true;

  static time_point now() noexcept {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return time_point(std::chrono::seconds(ts.tv_sec) +
		      std::chrono::nanoseconds(ts.tv_nsec));
  }

  static bool is_zero(const time_point& t) {
    return (t == zero());
  }

  static time_point zero() {
    return time_point();
  }
};

// Low-resolution but, I would hope or there's no point, faster
// monotonic clock
class coarse_mono_clock {
public:
  typedef signedspan duration;
  typedef duration::rep rep;
  typedef duration::period period;
  typedef std::chrono::time_point<coarse_mono_clock> time_point;
  static constexpr const bool is_steady = true;

  static time_point now() noexcept {
    struct timespec ts;
#if defined(CLOCK_MONOTONIC_COARSE)
    // Linux systems have _COARSE clocks.
    clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
#elif defined(CLOCK_MONOTONIC_FAST)
    // BSD systems have _FAST clocks.
    clock_gettime(CLOCK_MONOTONIC_FAST, &ts);
#else
    // And if we find neither, you may wish to consult your system's
    // documentation.
#warning Falling back to CLOCK_MONOTONIC, may be slow.
    clock_gettime(CLOCK_MONOTONIC, &ts);
#endif
    return time_point(std::chrono::seconds(ts.tv_sec) +
		      std::chrono::nanoseconds(ts.tv_nsec));
  }

  static bool is_zero(const time_point& t) {
    return (t == zero());
  }

  static time_point zero() {
    return time_point();
  }
};

namespace time_detail {
// So that our subtractions produce negative spans rather than
// arithmetic underflow.
template<typename Rep1, typename Period1, typename Rep2,
	 typename Period2>
inline auto difference(std::chrono::duration<Rep1, Period1> minuend,
		       std::chrono::duration<Rep2, Period2> subtrahend)
  -> typename std::common_type<
  std::chrono::duration<typename std::make_signed<Rep1>::type,
			Period1>,
  std::chrono::duration<typename std::make_signed<Rep2>::type,
			Period2> >::type {
  // Foo.
  using srep =
    typename std::common_type<
      std::chrono::duration<typename std::make_signed<Rep1>::type,
			    Period1>,
    std::chrono::duration<typename std::make_signed<Rep2>::type,
			  Period2> >::type;
  return srep(srep(minuend).count() - srep(subtrahend).count());
}

template<typename Clock, typename Duration1, typename Duration2>
inline auto difference(
  typename std::chrono::time_point<Clock, Duration1> minuend,
  typename std::chrono::time_point<Clock, Duration2> subtrahend)
  -> typename std::common_type<
  std::chrono::duration<typename std::make_signed<
			  typename Duration1::rep>::type,
			typename Duration1::period>,
  std::chrono::duration<typename std::make_signed<
			  typename Duration2::rep>::type,
			typename Duration2::period> >::type {
  return difference(minuend.time_since_epoch(),
		    subtrahend.time_since_epoch());
}
}

// Please note that the coarse clocks are disjoint. You cannot
// subtract a real_clock timepoint from a coarse_real_clock
// timepoint as, from C++'s perspective, they are disjoint types.

// This is not necessarily bad. If I sample a mono_clock and then a
// coarse_mono_clock, the coarse_mono_clock's time could potentially
// be previous to the mono_clock's time (just due to differing
// resolution) which would be Incorrect.

// This is not horrible, though, since you can use an idiom like
// mono_clock::timepoint(coarsepoint.time_since_epoch()) to unwrap
// and rewrap if you know what you're doing.


// Actual wall-clock times
typedef real_clock::time_point real_time;
typedef coarse_real_clock::time_point coarse_real_time;

// Monotonic times should never be serialized or communicated
// between machines, since they are incomparable. Thus we also don't
// make any provision for converting between
// std::chrono::steady_clock time and ceph::mono_clock time.
typedef mono_clock::time_point mono_time;
typedef coarse_mono_clock::time_point coarse_mono_time;

template<typename Rep1, typename Ratio1, typename Rep2, typename Ratio2>
auto floor(const std::chrono::duration<Rep1, Ratio1>& duration,
	   const std::chrono::duration<Rep2, Ratio2>& precision) ->
  typename std::common_type<std::chrono::duration<Rep1, Ratio1>,
			    std::chrono::duration<Rep2, Ratio2> >::type {
  return duration - (duration % precision);
}

template<typename Rep1, typename Ratio1, typename Rep2, typename Ratio2>
auto ceil(const std::chrono::duration<Rep1, Ratio1>& duration,
	  const std::chrono::duration<Rep2, Ratio2>& precision) ->
  typename std::common_type<std::chrono::duration<Rep1, Ratio1>,
			    std::chrono::duration<Rep2, Ratio2> >::type {
  auto tmod = duration % precision;
  return duration - tmod + (tmod > tmod.zero() ? 1 : 0) * precision;
}

template<typename Clock, typename Duration, typename Rep, typename Ratio>
auto floor(const std::chrono::time_point<Clock, Duration>& timepoint,
	   const std::chrono::duration<Rep, Ratio>& precision) ->
  std::chrono::time_point<Clock,
			  typename std::common_type<
			    Duration, std::chrono::duration<Rep, Ratio>
			    >::type> {
  return std::chrono::time_point<
    Clock, typename std::common_type<
      Duration, std::chrono::duration<Rep, Ratio> >::type>(
	floor(timepoint.time_since_epoch(), precision));
}
template<typename Clock, typename Duration, typename Rep, typename Ratio>
auto ceil(const std::chrono::time_point<Clock, Duration>& timepoint,
	  const std::chrono::duration<Rep, Ratio>& precision) ->
  std::chrono::time_point<Clock,
			  typename std::common_type<
			    Duration,
			    std::chrono::duration<Rep, Ratio> >::type> {
  return std::chrono::time_point<
    Clock, typename std::common_type<
      Duration, std::chrono::duration<Rep, Ratio> >::type>(
	ceil(timepoint.time_since_epoch(), precision));
}

inline signedspan make_timespan(const double d) {
  return std::chrono::duration_cast<signedspan>(
    std::chrono::duration<double>(d));
}
inline std::optional<signedspan> maybe_timespan(const double d) {
  return d ? std::make_optional(make_timespan(d)) : std::nullopt;
}

template<typename Clock,
	 typename std::enable_if<!Clock::is_steady>::type* = nullptr>
std::ostream& operator<<(std::ostream& m,
			 const std::chrono::time_point<Clock>& t);
template<typename Clock,
	 typename std::enable_if<Clock::is_steady>::type* = nullptr>
std::ostream& operator<<(std::ostream& m,
			 const std::chrono::time_point<Clock>& t);

// The way std::chrono handles the return type of subtraction is not
// wonderful. The difference of two unsigned types SHOULD be signed.

inline signedspan operator -(real_time minuend,
			     real_time subtrahend) {
  return time_detail::difference(minuend, subtrahend);
}

inline signedspan operator -(coarse_real_time minuend,
			     coarse_real_time subtrahend) {
  return time_detail::difference(minuend, subtrahend);
}

inline signedspan operator -(mono_time minuend,
			     mono_time subtrahend) {
  return time_detail::difference(minuend, subtrahend);
}

inline signedspan operator -(coarse_mono_time minuend,
			     coarse_mono_time subtrahend) {
  return time_detail::difference(minuend, subtrahend);
}

// We could add specializations of time_point - duration and
// time_point + duration to assert on overflow, but I don't think we
// should.
inline timespan abs(signedspan z) {
  return z > signedspan::zero() ?
    std::chrono::duration_cast<timespan>(z) :
    timespan(-z.count());
}
inline timespan to_timespan(signedspan z) {
  if (z < signedspan::zero()) {
    //ceph_assert(z >= signedspan::zero());
    // There is a kernel bug that seems to be triggering this assert.  We've
    // seen it in:
    //   centos 8.1: 4.18.0-147.el8.x86_64
    //   debian 10.3: 4.19.0-8-amd64
    //   debian 10.1: 4.19.67-2+deb10u1
    //   ubuntu 18.04
    // see bugs:
    //   https://tracker.ceph.com/issues/43365
    //   https://tracker.ceph.com/issues/44078
    z = signedspan::zero();
  }
  return std::chrono::duration_cast<timespan>(z);
}

std::string timespan_str(timespan t);
std::string exact_timespan_str(timespan t);
std::chrono::seconds parse_timespan(const std::string& s);

// detects presence of Clock::to_timespec() and from_timespec()
template <typename Clock, typename = std::void_t<>>
struct converts_to_timespec : std::false_type {};

template <typename Clock>
struct converts_to_timespec<Clock, std::void_t<decltype(
  Clock::from_timespec(Clock::to_timespec(
			 std::declval<typename Clock::time_point>()))
  )>> : std::true_type {};

template <typename Clock>
constexpr bool converts_to_timespec_v = converts_to_timespec<Clock>::value;

template <typename Clock>
concept clock_with_timespec = converts_to_timespec_v<Clock>;

template<typename Rep, typename T>
static Rep to_seconds(T t) {
  return std::chrono::duration_cast<
    std::chrono::duration<Rep>>(t).count();
}

template<typename Rep, typename T>
static Rep to_microseconds(T t) {
  return std::chrono::duration_cast<
    std::chrono::duration<
      Rep,
      std::micro>>(t).count();
}

} // namespace ceph

namespace std {
template<typename Rep, typename Period>
ostream& operator<<(ostream& m, const chrono::duration<Rep, Period>& t);
}

// concept helpers for the formatters:

template <typename TimeP>
concept SteadyTimepoint = TimeP::clock::is_steady;

template <typename TimeP>
concept UnsteadyTimepoint = ! TimeP::clock::is_steady;

namespace fmt {
template <UnsteadyTimepoint T>
struct formatter<T> {
  constexpr auto parse(fmt::format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const T& t, FormatContext& ctx) const
  {
    struct tm bdt;
    time_t tt = T::clock::to_time_t(t);
    localtime_r(&tt, &bdt);
    char tz[32] = {0};
    strftime(tz, sizeof(tz), "%z", &bdt);

    return fmt::format_to(
	ctx.out(), "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}:{:06}{}",
	(bdt.tm_year + 1900), (bdt.tm_mon + 1), bdt.tm_mday, bdt.tm_hour,
	bdt.tm_min, bdt.tm_sec,
	duration_cast<std::chrono::microseconds>(
	    t.time_since_epoch() % std::chrono::seconds(1))
	    .count(),
	tz);
  }
};

template <SteadyTimepoint T>
struct formatter<T> {
  constexpr auto parse(fmt::format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const T& t, FormatContext& ctx) const
  {
    return fmt::format_to(
	ctx.out(), "{}s",
	std::chrono::duration<double>(t.time_since_epoch()).count());
  }
};
}  // namespace fmt

#endif // COMMON_CEPH_TIME_H
