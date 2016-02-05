
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

#include <ctime>

#include "common/ceph_time.h"
#include "include/rados.h"
#include "gtest/gtest.h"


using ceph::real_clock;
using ceph::real_time;

using ceph::real_clock;
using ceph::real_time;

using ceph::coarse_real_clock;
using ceph::coarse_mono_clock;

using ceph::timespan;
using ceph::signedspan;

using std::chrono::seconds;
using std::chrono::microseconds;
using std::chrono::nanoseconds;

static_assert(!real_clock::is_steady, "ceph::real_clock must not be steady.");
static_assert(!coarse_real_clock::is_steady,
	      "ceph::coarse_real_clock must not be steady.");

static_assert(mono_clock::is_steady, "ceph::mono_clock must be steady.");
static_assert(coarse_mono_clock::is_steady,
	      "ceph::coarse_mono_clock must be steady.");

// Before this file was written.
static constexpr uint32_t bs = 1440701569;
static constexpr uint32_t bns = 123456789;
static constexpr uint32_t bus = 123456;
static constexpr time_t btt = bs;
static constexpr struct timespec bts = { bs, bns };
static constexpr struct ceph_timespec bcts = { bs, bns };
static constexpr struct timeval btv = { bs, bus };
static constexpr double bd = bs + ((double)bns / 1000000000.);

template<typename Clock>
static void system_clock_sanity() {
  static const typename Clock::time_point brt(seconds(bs) + nanoseconds(bns));
  const typename Clock::time_point now(Clock::now());

  ASSERT_GT(now, brt);

  ASSERT_GT(Clock::to_time_t(now), btt);

  ASSERT_GT(Clock::to_timespec(now).tv_sec, bts.tv_sec);
  ASSERT_LT(Clock::to_timespec(now).tv_nsec, 1000000000L);

  ASSERT_GT(Clock::to_ceph_timespec(now).tv_sec, bcts.tv_sec);
  ASSERT_LT(Clock::to_ceph_timespec(now).tv_nsec, 1000000000UL);

  ASSERT_GT(Clock::to_timeval(now).tv_sec, btv.tv_sec);
  ASSERT_LT(Clock::to_timeval(now).tv_usec, 1000000L);
}

template<typename Clock>
static void system_clock_conversions() {
  static constexpr typename Clock::time_point brt(seconds(bs) +
						  nanoseconds(bns));

  ASSERT_EQ(Clock::to_time_t(brt), btt);
  ASSERT_EQ(Clock::from_time_t(btt) + nanoseconds(bns), brt);

  {
    const struct timespec tts = Clock::to_timespec(brt);
    ASSERT_EQ(tts.tv_sec, bts.tv_sec);
    ASSERT_EQ(tts.tv_nsec, bts.tv_nsec);
  }
  ASSERT_EQ(Clock::from_timespec(bts), brt);
  {
    struct timespec tts;
    Clock::to_timespec(brt, tts);
    ASSERT_EQ(tts.tv_sec, bts.tv_sec);
    ASSERT_EQ(tts.tv_nsec, bts.tv_nsec);
  }

  {
    const struct ceph_timespec tcts = Clock::to_ceph_timespec(brt);
    ASSERT_EQ(tcts.tv_sec, bcts.tv_sec);
    ASSERT_EQ(tcts.tv_nsec, bcts.tv_nsec);
  }
  ASSERT_EQ(Clock::from_ceph_timespec(bcts), brt);
  {
    struct ceph_timespec tcts;
    Clock::to_ceph_timespec(brt, tcts);
    ASSERT_EQ(tcts.tv_sec, bcts.tv_sec);
    ASSERT_EQ(tcts.tv_nsec, bcts.tv_nsec);
  }

  {
    const struct timeval ttv = Clock::to_timeval(brt);
    ASSERT_EQ(ttv.tv_sec, btv.tv_sec);
    ASSERT_EQ(ttv.tv_usec, btv.tv_usec);
  }
  ASSERT_EQ(Clock::from_timeval(btv), brt - nanoseconds(bns - bus * 1000));
  {
    struct timeval ttv;
    Clock::to_timeval(brt, ttv);
    ASSERT_EQ(ttv.tv_sec, btv.tv_sec);
    ASSERT_EQ(ttv.tv_usec, btv.tv_usec);
  }

  ASSERT_EQ(Clock::to_double(brt), bd);
  // Fudge factor
  ASSERT_LT(abs((Clock::from_double(bd) - brt).count()), 30);
}

TEST(RealClock, Sanity) {
  system_clock_sanity<real_clock>();
}


TEST(RealClock, Conversions) {
  system_clock_conversions<real_clock>();
}

TEST(CoarseRealClock, Sanity) {
  system_clock_sanity<coarse_real_clock>();
}


TEST(CoarseRealClock, Conversions) {
  system_clock_conversions<coarse_real_clock>();
}
