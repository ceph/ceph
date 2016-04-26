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
#include <ctime>

#include "include/encoding.h"

class CephContext;
struct ceph_timespec;

namespace ceph {
  namespace time_detail {
    using std::chrono::duration_cast;
    using std::chrono::seconds;
    using std::chrono::microseconds;
    using std::chrono::nanoseconds;
    // Currently we use a 64-bit count of nanoseconds.

    // We could, if we wished, use a struct holding a uint64_t count
    // of seconds and a uint32_t count of nanoseconds.

    // At least this way we can change it to something else if we
    // want.
    typedef uint64_t rep;

    // A concrete duration, unsigned. The timespan Ceph thinks in.
    typedef std::chrono::duration<rep, std::nano> timespan;


    // Like the above but signed.
    typedef int64_t signed_rep;

    typedef std::chrono::duration<signed_rep, std::nano> signedspan;

    // We define our own clocks so we can have our choice of all time
    // sources supported by the operating system. With the standard
    // library the resolution and cost are unspecified. (For example,
    // the libc++ system_clock class gives only microsecond
    // resolution.)

    // One potential issue is that we should accept system_clock
    // timepoints in user-facing APIs alongside (or instead of)
    // ceph::real_clock times.
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
      // We need a version of 'now' that can take a CephContext for
      // introducing configurable clock skew.
      static time_point now(const CephContext* cct) noexcept;

      static bool is_zero(const time_point& t) {
        return (t == time_point::min());
      }

      // Allow conversion to/from any clock with the same interface as
      // std::chrono::system_clock)
      template<typename Clock, typename Duration>
      static time_point to_system_time_point(
	const std::chrono::time_point<Clock, Duration>& t) {
	return time_point(seconds(Clock::to_time_t(t)) +
			  duration_cast<duration>(t.time_since_epoch() %
						  seconds(1)));
      }
      template<typename Clock, typename Duration>
      static std::chrono::time_point<Clock, Duration> to_system_time_point(
	const time_point& t) {
	return (Clock::from_time_t(to_time_t(t)) +
		duration_cast<Duration>(t.time_since_epoch() % seconds(1)));
      }

      static time_t to_time_t(const time_point& t) noexcept {
	return duration_cast<seconds>(t.time_since_epoch()).count();
      }
      static time_point from_time_t(const time_t& t) noexcept {
	return time_point(seconds(t));
      }

      static void to_timespec(const time_point& t, struct timespec& ts) {
	ts.tv_sec = to_time_t(t);
	ts.tv_nsec = (t.time_since_epoch() % seconds(1)).count();
      }
      static struct timespec to_timespec(const time_point& t) {
	struct timespec ts;
	to_timespec(t, ts);
	return ts;
      }
      static time_point from_timespec(const struct timespec& ts) {
	return time_point(seconds(ts.tv_sec) + nanoseconds(ts.tv_nsec));
      }

      static void to_ceph_timespec(const time_point& t,
				   struct ceph_timespec& ts);
      static struct ceph_timespec to_ceph_timespec(const time_point& t);
      static time_point from_ceph_timespec(const struct ceph_timespec& ts);

      static void to_timeval(const time_point& t, struct timeval& tv) {
	tv.tv_sec = to_time_t(t);
	tv.tv_usec = duration_cast<microseconds>(t.time_since_epoch() %
						 seconds(1)).count();
      }
      static struct timeval to_timeval(const time_point& t) {
	struct timeval tv;
	to_timeval(t, tv);
	return tv;
      }
      static time_point from_timeval(const struct timeval& tv) {
	return time_point(seconds(tv.tv_sec) + microseconds(tv.tv_usec));
      }

      static double to_double(const time_point& t) {
	return std::chrono::duration<double>(t.time_since_epoch()).count();
      }
      static time_point from_double(const double d) {
	return time_point(duration_cast<duration>(
			    std::chrono::duration<double>(d)));
      }
    };

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
      static time_point now(const CephContext* cct) noexcept;

      static time_t to_time_t(const time_point& t) noexcept {
	return duration_cast<seconds>(t.time_since_epoch()).count();
      }
      static time_point from_time_t(const time_t t) noexcept {
	return time_point(seconds(t));
      }

      static void to_timespec(const time_point& t, struct timespec& ts) {
	ts.tv_sec = to_time_t(t);
	ts.tv_nsec = (t.time_since_epoch() % seconds(1)).count();
      }
      static struct timespec to_timespec(const time_point& t) {
	struct timespec ts;
	to_timespec(t, ts);
	return ts;
      }
      static time_point from_timespec(const struct timespec& ts) {
	return time_point(seconds(ts.tv_sec) + nanoseconds(ts.tv_nsec));
      }

      static void to_ceph_timespec(const time_point& t,
				   struct ceph_timespec& ts);
      static struct ceph_timespec to_ceph_timespec(const time_point& t);
      static time_point from_ceph_timespec(const struct ceph_timespec& ts);

      static void to_timeval(const time_point& t, struct timeval& tv) {
	tv.tv_sec = to_time_t(t);
	tv.tv_usec = duration_cast<microseconds>(t.time_since_epoch() %
						 seconds(1)).count();
      }
      static struct timeval to_timeval(const time_point& t) {
	struct timeval tv;
	to_timeval(t, tv);
	return tv;
      }
      static time_point from_timeval(const struct timeval& tv) {
	return time_point(seconds(tv.tv_sec) + microseconds(tv.tv_usec));
      }

      static double to_double(const time_point& t) {
	return std::chrono::duration<double>(t.time_since_epoch()).count();
      }
      static time_point from_double(const double d) {
	return time_point(duration_cast<duration>(
			    std::chrono::duration<double>(d)));
      }
    };

    class mono_clock {
    public:
      typedef timespan duration;
      typedef duration::rep rep;
      typedef duration::period period;
      typedef std::chrono::time_point<mono_clock> time_point;
      static constexpr const bool is_steady = true;

      static time_point now() noexcept {
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return time_point(seconds(ts.tv_sec) + nanoseconds(ts.tv_nsec));
      }

      // A monotonic clock's timepoints are only meaningful to the
      // computer on which they were generated. Thus having an
      // optional skew is meaningless.
    };

    class coarse_mono_clock {
    public:
      typedef timespan duration;
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
	return time_point(seconds(ts.tv_sec) + nanoseconds(ts.tv_nsec));
      }
    };

    // So that our subtractions produce negative spans rather than
    // arithmetic underflow.
    namespace {
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
  } // namespace time_detail

  // duration is the concrete time representation for our code in the
  // case that we are only interested in durations between now and the
  // future. Using it means we don't have to have EVERY function that
  // deals with a duration be a template. We can do so for user-facing
  // APIs, however.
  using time_detail::timespan;

  // Similar to the above but for durations that can specify
  // differences between now and a time point in the past.
  using time_detail::signedspan;

  // High-resolution real-time clock
  using time_detail::real_clock;

  // Low-resolution but preusmably faster real-time clock
  using time_detail::coarse_real_clock;


  // High-resolution monotonic clock
  using time_detail::mono_clock;

  // Low-resolution but, I would hope or there's no point, faster
  // monotonic clock
  using time_detail::coarse_mono_clock;

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

  namespace {
    inline timespan make_timespan(const double d) {
      return std::chrono::duration_cast<timespan>(
	std::chrono::duration<double>(d));
    }
  }

  std::ostream& operator<<(std::ostream& m, const timespan& t);
  std::ostream& operator<<(std::ostream& m, const real_time& t);
  std::ostream& operator<<(std::ostream& m, const mono_time& t);

  // The way std::chrono handles the return type of subtraction is not
  // wonderful. The difference of two unsigned types SHOULD be signed.

  namespace {
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
  }

  // We could add specializations of time_point - duration and
  // time_point + duration to assert on overflow, but I don't think we
  // should.

} // namespace ceph

// We need these definitions to be able to hande ::encode/::decode on
// time points.

template<typename Clock, typename Duration>
void encode(const std::chrono::time_point<Clock, Duration>& t,
	    ceph::bufferlist &bl) {
  auto ts = Clock::to_timespec(t);
  // A 32 bit count of seconds causes me vast unhappiness.
  uint32_t s = ts.tv_sec;
  uint32_t ns = ts.tv_nsec;
  ::encode(s, bl);
  ::encode(ns, bl);
}

template<typename Clock, typename Duration>
void decode(std::chrono::time_point<Clock, Duration>& t,
	    bufferlist::iterator& p) {
  uint32_t s;
  uint32_t ns;
  ::decode(s, p);
  ::decode(ns, p);
  struct timespec ts = {
    static_cast<time_t>(s),
    static_cast<long int>(ns)};

  t = Clock::from_timespec(ts);
}

// C++ Overload Resolution requires that our encode/decode functions
// be defined in the same namespace as the type. So we need this
// to handle things like ::encode(std::vector<ceph::real_time // > >)

namespace std {
  namespace chrono {
    template<typename Clock, typename Duration>
    void encode(const time_point<Clock, Duration>& t,
		ceph::bufferlist &bl) {
      ::encode(t, bl);
    }

    template<typename Clock, typename Duration>
    void decode(time_point<Clock, Duration>& t, bufferlist::iterator &p) {
      ::decode(t, p);
    }
  } // namespace chrono

  // An overload of our own
  namespace {
    inline timespan abs(signedspan z) {
      return z > signedspan::zero() ?
	std::chrono::duration_cast<timespan>(z) :
	timespan(-z.count());
    }
  }
} // namespace std

#endif // COMMON_CEPH_TIME_H
