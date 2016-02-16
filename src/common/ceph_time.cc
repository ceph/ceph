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
#include "include/types.h"

#include "ceph_context.h"
#include "ceph_time.h"
#include "config.h"

namespace ceph {
  namespace time_detail {
    real_clock::time_point real_clock::now(const CephContext* cct) noexcept {
      auto t = now();
      if (cct)
	t += make_timespan(cct->_conf->clock_offset);
      return t;
    }

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

    coarse_real_clock::time_point coarse_real_clock::now(
      const CephContext* cct) noexcept {
      auto t = now();
      if (cct)
	t += make_timespan(cct->_conf->clock_offset);
      return t;
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
  };

  using std::chrono::duration_cast;
  using std::chrono::seconds;
  using std::chrono::microseconds;

  std::ostream& operator<<(std::ostream& m, const mono_time& t) {
    return m << std::chrono::duration<double>(t.time_since_epoch()).count()
	     << "s";
  }

  std::ostream& operator<<(std::ostream& m, const timespan& t) {
    return m << std::chrono::duration<double>(t).count() << "s";
  }

  std::ostream& operator<<(std::ostream& m, const real_time& t) {
    m.setf(std::ios::right);
    char oldfill = m.fill();
    m.fill('0');
    // localtime.  this looks like an absolute time.
    //  aim for http://en.wikipedia.org/wiki/ISO_8601
    struct tm bdt;
    time_t tt = ceph::real_clock::to_time_t(t);
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
};
