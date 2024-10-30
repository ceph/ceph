
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
#include "include/stringify.h"

using namespace std;

using ceph::real_clock;
using ceph::real_time;

using ceph::real_clock;
using ceph::real_time;

using ceph::mono_clock;

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
static struct ceph_timespec bcts = { ceph_le32(bs), ceph_le32(bns) };
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
  static typename Clock::time_point brt(seconds(bs) +
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
  ASSERT_LT(std::abs((Clock::from_double(bd) - brt).count()), 30);
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

TEST(TimePoints, SignedSubtraciton) {
  ceph::real_time rta(std::chrono::seconds(3));
  ceph::real_time rtb(std::chrono::seconds(5));

  ceph::coarse_real_time crta(std::chrono::seconds(3));
  ceph::coarse_real_time crtb(std::chrono::seconds(5));

  ceph::mono_time mta(std::chrono::seconds(3));
  ceph::mono_time mtb(std::chrono::seconds(5));

  ceph::coarse_mono_time cmta(std::chrono::seconds(3));
  ceph::coarse_mono_time cmtb(std::chrono::seconds(5));

  ASSERT_LT(rta - rtb, ceph::signedspan::zero());
  ASSERT_LT((rta - rtb).count(), 0);
  ASSERT_GT(rtb - rta, ceph::signedspan::zero());
  ASSERT_GT((rtb - rta).count(), 0);

  ASSERT_LT(crta - crtb, ceph::signedspan::zero());
  ASSERT_LT((crta - crtb).count(), 0);
  ASSERT_GT(crtb - crta, ceph::signedspan::zero());
  ASSERT_GT((crtb - crta).count(), 0);

  ASSERT_LT(mta - mtb, ceph::signedspan::zero());
  ASSERT_LT((mta - mtb).count(), 0);
  ASSERT_GT(mtb - mta, ceph::signedspan::zero());
  ASSERT_GT((mtb - mta).count(), 0);

  ASSERT_LT(cmta - cmtb, ceph::signedspan::zero());
  ASSERT_LT((cmta - cmtb).count(), 0);
  ASSERT_GT(cmtb - cmta, ceph::signedspan::zero());
  ASSERT_GT((cmtb - cmta).count(), 0);
}

TEST(TimePoints, stringify) {
  ceph::real_clock::time_point tp(seconds(1556122013) + nanoseconds(39923122));
  string s = stringify(tp);
  ASSERT_EQ(s.size(), strlen("2019-04-24T11:06:53.039923-0500"));
  ASSERT_TRUE(s[26] == '-' || s[26] == '+');
  ASSERT_EQ(s.substr(0, 9), "2019-04-2");

  ceph::coarse_real_clock::time_point ctp(seconds(1556122013) +
					  nanoseconds(399000000));
  s = stringify(ctp);
  ASSERT_EQ(s.size(), strlen("2019-04-24T11:06:53.399000-0500"));
  ASSERT_TRUE(s[26] == '-' || s[26] == '+');
  ASSERT_EQ(s.substr(0, 9), "2019-04-2");
}

namespace {
  template<typename Rep, typename Period>
  std::string to_string(const chrono::duration<Rep, Period>& t)
  {
    std::ostringstream ss;
    ss << t;
    return ss.str();
  }

  void float_format_eq(string_view lhs,
                       string_view rhs,
                       unsigned precision)
  {
    const float TOLERANCE = 10.0F / pow(10.0F, static_cast<float>(precision));
    ASSERT_FALSE(lhs.empty());
    ASSERT_EQ(lhs.back(), 's');
    float lhs_v = std::stof(string{lhs, 0, lhs.find('s')});
    ASSERT_NE(lhs.npos, lhs.find('.'));
    ASSERT_EQ(precision, lhs.find('s') - lhs.find('.') - 1);

    ASSERT_FALSE(rhs.empty());
    ASSERT_EQ(rhs.back(), 's');
    float rhs_v = std::stof(string{rhs, 0, rhs.find('s')});
    EXPECT_NEAR(lhs_v, rhs_v, TOLERANCE);
    ASSERT_NE(rhs.npos, rhs.find('.'));
    EXPECT_EQ(precision, rhs.find('s') - rhs.find('.') - 1);
  }
}

TEST(TimeDurations, print) {
  float_format_eq("0.123456700s",
                  to_string(std::chrono::duration_cast<ceph::timespan>(0.1234567s)),
                  9);
  float_format_eq("-0.123456700s",
                  to_string(std::chrono::duration_cast<ceph::signedspan>(-0.1234567s)),
                  9);
  EXPECT_EQ("42s", to_string(42s));
  float_format_eq("0.123000000s", to_string(123ms), 9);
}
