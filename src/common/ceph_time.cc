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

// For ceph_timespec
#include "ceph_time.h"
#include "log/LogClock.h"
#include "config.h"
#include "strtol.h"

#if defined(__APPLE__)
#include <mach/mach.h>
#include <mach/mach_time.h>

#include <ostringstream>

#ifndef NSEC_PER_SEC
#define NSEC_PER_SEC 1000000000ULL
#endif

int clock_gettime(int clk_id, struct timespec *tp)
{
  if (clk_id == CLOCK_REALTIME) {
    // gettimeofday is much faster than clock_get_time
    struct timeval now;
    int ret = gettimeofday(&now, NULL);
    if (ret)
      return ret;
    tp->tv_sec = now.tv_sec;
    tp->tv_nsec = now.tv_usec * 1000L;
  } else {
    uint64_t t = mach_absolute_time();
    static mach_timebase_info_data_t timebase_info;
    if (timebase_info.denom == 0) {
      (void)mach_timebase_info(&timebase_info);
    }
    auto nanos = t * timebase_info.numer / timebase_info.denom;
    tp->tv_sec = nanos / NSEC_PER_SEC;
    tp->tv_nsec = nanos - (tp->tv_sec * NSEC_PER_SEC);
  }
  return 0;
}
#endif

namespace ceph {
  namespace time_detail {
    void real_clock::to_ceph_timespec(const time_point& t,
				      struct ceph_timespec& ts) {
      ts.tv_sec = to_time_t(t);
      ts.tv_nsec = (t.time_since_epoch() % seconds(1)).count();
    }
    struct ceph_timespec real_clock::to_ceph_timespec(const time_point& t) {
      struct ceph_timespec ts;
      to_ceph_timespec(t, ts);
      return ts;
    }
    real_clock::time_point real_clock::from_ceph_timespec(
      const struct ceph_timespec& ts) {
      return time_point(seconds(ts.tv_sec) + nanoseconds(ts.tv_nsec));
    }

    void coarse_real_clock::to_ceph_timespec(const time_point& t,
					     struct ceph_timespec& ts) {
      ts.tv_sec = to_time_t(t);
      ts.tv_nsec = (t.time_since_epoch() % seconds(1)).count();
    }
    struct ceph_timespec coarse_real_clock::to_ceph_timespec(
      const time_point& t) {
      struct ceph_timespec ts;
      to_ceph_timespec(t, ts);
      return ts;
    }
    coarse_real_clock::time_point coarse_real_clock::from_ceph_timespec(
      const struct ceph_timespec& ts) {
      return time_point(seconds(ts.tv_sec) + nanoseconds(ts.tv_nsec));
    }

  }

  using std::chrono::duration_cast;
  using std::chrono::seconds;
  using std::chrono::microseconds;

  template<typename Clock,
	   typename std::enable_if<Clock::is_steady>::type*>
  std::ostream& operator<<(std::ostream& m,
			   const std::chrono::time_point<Clock>& t) {
    return m << std::chrono::duration<double>(t.time_since_epoch()).count()
	     << "s";
  }

  std::ostream& operator<<(std::ostream& m, const timespan& t) {
    return m << std::chrono::duration<double>(t).count() << "s";
  }

  template<typename Clock,
	   typename std::enable_if<!Clock::is_steady>::type*>
  std::ostream& operator<<(std::ostream& m,
			   const std::chrono::time_point<Clock>& t) {
    m.setf(std::ios::right);
    char oldfill = m.fill();
    m.fill('0');
    // localtime.  this looks like an absolute time.
    //  aim for http://en.wikipedia.org/wiki/ISO_8601
    struct tm bdt;
    time_t tt = Clock::to_time_t(t);
    localtime_r(&tt, &bdt);
    m << std::setw(4) << (bdt.tm_year+1900)  // 2007 -> '07'
      << '-' << std::setw(2) << (bdt.tm_mon+1)
      << '-' << std::setw(2) << bdt.tm_mday
      << ' '
      << std::setw(2) << bdt.tm_hour
      << ':' << std::setw(2) << bdt.tm_min
      << ':' << std::setw(2) << bdt.tm_sec
      << "." << std::setw(6) << duration_cast<microseconds>(
	t.time_since_epoch() % seconds(1));
    m.fill(oldfill);
    m.unsetf(std::ios::right);
    return m;
  }

  template std::ostream&
  operator<< <mono_clock>(std::ostream& m, const mono_time& t);
  template std::ostream&
  operator<< <real_clock>(std::ostream& m, const real_time& t);
  template std::ostream&
  operator<< <coarse_mono_clock>(std::ostream& m, const coarse_mono_time& t);
  template std::ostream&
  operator<< <coarse_real_clock>(std::ostream& m, const coarse_real_time& t);

  std::string timespan_str(timespan t)
  {
    // FIXME: somebody pretty please make a version of this function
    // that isn't as lame as this one!
    uint64_t nsec = std::chrono::nanoseconds(t).count();
    ostringstream ss;
    if (nsec < 2000000000) {
      ss << ((float)nsec / 1000000000) << "s";
      return ss.str();
    }
    uint64_t sec = nsec / 1000000000;
    if (sec < 120) {
      ss << sec << "s";
      return ss.str();
    }
    uint64_t min = sec / 60;
    if (min < 120) {
      ss << min << "m";
      return ss.str();
    }
    uint64_t hr = min / 60;
    if (hr < 48) {
      ss << hr << "h";
      return ss.str();
    }
    uint64_t day = hr / 24;
    if (day < 14) {
      ss << day << "d";
      return ss.str();
    }
    uint64_t wk = day / 7;
    if (wk < 12) {
      ss << wk << "w";
      return ss.str();
    }
    uint64_t mn = day / 30;
    if (mn < 24) {
      ss << mn << "M";
      return ss.str();
    }
    uint64_t yr = day / 365;
    ss << yr << "y";
    return ss.str();
  }

  std::string exact_timespan_str(timespan t)
  {
    uint64_t nsec = std::chrono::nanoseconds(t).count();
    uint64_t sec = nsec / 1000000000;
    nsec %= 1000000000;
    uint64_t yr = sec / (60 * 60 * 24 * 365);
    ostringstream ss;
    if (yr) {
      ss << yr << "y";
      sec -= yr * (60 * 60 * 24 * 365);
    }
    uint64_t mn = sec / (60 * 60 * 24 * 30);
    if (mn >= 3) {
      ss << mn << "mo";
      sec -= mn * (60 * 60 * 24 * 30);
    }
    uint64_t wk = sec / (60 * 60 * 24 * 7);
    if (wk >= 2) {
      ss << wk << "w";
      sec -= wk * (60 * 60 * 24 * 7);
    }
    uint64_t day = sec / (60 * 60 * 24);
    if (day >= 2) {
      ss << day << "d";
      sec -= day * (60 * 60 * 24);
    }
    uint64_t hr = sec / (60 * 60);
    if (hr >= 2) {
      ss << hr << "h";
      sec -= hr * (60 * 60);
    }
    uint64_t min = sec / 60;
    if (min >= 2) {
      ss << min << "m";
      sec -= min * 60;
    }
    if (sec) {
      ss << sec;
    }
    if (nsec) {
      ss << ((float)nsec / 1000000000);
    }
    if (sec || nsec) {
      ss << "s";
    }
    return ss.str();
  }

  std::chrono::seconds parse_timespan(const std::string& s)
  {
    static std::map<string,int> units = {
      { "s", 1 },
      { "sec", 1 },
      { "second", 1 },
      { "seconds", 1 },
      { "m", 60 },
      { "min", 60 },
      { "minute", 60 },
      { "minutes", 60 },
      { "h", 60*60 },
      { "hr", 60*60 },
      { "hour", 60*60 },
      { "hours", 60*60 },
      { "d", 24*60*60 },
      { "day", 24*60*60 },
      { "days", 24*60*60 },
      { "w", 7*24*60*60 },
      { "wk", 7*24*60*60 },
      { "week", 7*24*60*60 },
      { "weeks", 7*24*60*60 },
      { "mo", 30*24*60*60 },
      { "month", 30*24*60*60 },
      { "months", 30*24*60*60 },
      { "y", 365*24*60*60 },
      { "yr", 365*24*60*60 },
      { "year", 365*24*60*60 },
      { "years", 365*24*60*60 },
    };

    auto r = 0s;
    auto pos = 0u;
    while (pos < s.size()) {
      // skip whitespace
      while (std::isspace(s[pos])) {
	++pos;
      }
      if (pos >= s.size()) {
	break;
      }

      // consume any digits
      auto val_start = pos;
      while (std::isdigit(s[pos])) {
	++pos;
      }
      if (val_start == pos) {
	throw invalid_argument("expected digit");
      }
      string n = s.substr(val_start, pos - val_start);
      string err;
      auto val = strict_strtoll(n.c_str(), 10, &err);
      if (err.size()) {
	throw invalid_argument(err);
      }

      // skip whitespace
      while (std::isspace(s[pos])) {
	++pos;
      }

      // consume unit
      auto unit_start = pos;
      while (std::isalpha(s[pos])) {
	++pos;
      }
      if (unit_start != pos) {
	string unit = s.substr(unit_start, pos - unit_start);
	auto p = units.find(unit);
	if (p == units.end()) {
	  throw invalid_argument("unrecogized unit '"s + unit + "'");
	}
	val *= p->second;
      } else if (pos < s.size()) {
	throw invalid_argument("unexpected trailing '"s + s.substr(pos) + "'");
      }
      r += chrono::seconds(val);
    }
    return r;
  }

}
