// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat <contact@redhat.com>
 *
 * LGPL-2.1 (see COPYING-LGPL2.1) or later
 */

#include <chrono>

#include <gtest/gtest.h>

#include "common/ceph_time.h"
#include "common/iso_8601.h"

using namespace std::literals;

namespace chrono = std::chrono;

using chrono::hours;
using chrono::minutes;
using chrono::seconds;
using chrono::nanoseconds;
using chrono::time_point_cast;

using ceph::from_iso_8601;
using ceph::iso_8601_format;
using ceph::iso_8601_mode;
using ceph::real_clock;
using ceph::real_time;
using ceph::to_iso_8601;

TEST(iso_8601, epoch) {
  const auto epoch = real_clock::from_time_t(0);

  EXPECT_EQ("1970", to_iso_8601(epoch, iso_8601_format::Y));
  EXPECT_EQ("1970-01", to_iso_8601(epoch, iso_8601_format::YM));
  EXPECT_EQ("1970-01-01", to_iso_8601(epoch, iso_8601_format::YMD));
  EXPECT_EQ("1970-01-01T00Z", to_iso_8601(epoch, iso_8601_format::YMDh));
  EXPECT_EQ("1970-01-01T00:00Z", to_iso_8601(epoch, iso_8601_format::YMDhm));
  EXPECT_EQ("1970-01-01T00:00:00Z",
	    to_iso_8601(epoch, iso_8601_format::YMDhms));
  EXPECT_EQ("1970-01-01T00:00:00.000000000Z",
	    to_iso_8601(epoch, iso_8601_format::YMDhmsn));

  EXPECT_EQ(epoch, *from_iso_8601("1970"));
  EXPECT_EQ(epoch, *from_iso_8601("1970-01"));
  EXPECT_EQ(epoch, *from_iso_8601("1970-01-01"));
  EXPECT_EQ(epoch, *from_iso_8601("1970-01-01T00:00Z"));
  EXPECT_EQ(epoch, *from_iso_8601("1970-01-01T00:00:00Z"));
  EXPECT_EQ(epoch, *from_iso_8601("1970-01-01T00:00:00.000000000Z"));
}

TEST(iso_8601, now) {
  const auto now = real_clock::now();

  EXPECT_EQ(real_time(time_point_cast<minutes>(now)),
	    *from_iso_8601(to_iso_8601(now, iso_8601_format::YMDhm)));
  EXPECT_EQ(real_time(time_point_cast<seconds>(now)),
	    *from_iso_8601(
	      to_iso_8601(now, iso_8601_format::YMDhms)));
  EXPECT_EQ(now,
	    *from_iso_8601(
	      to_iso_8601(now, iso_8601_format::YMDhmsn)));
}

TEST(iso_8601, tz) {
  // Smallpox eradicaiton declared: 1980-05-08T16:10+02:00 in Geneva
  const auto ref = ceph::real_clock::from_time_t(326643000);

  EXPECT_EQ(chrono::floor<hours>(ref), *from_iso_8601("1980-05-08T14Z"sv));
  EXPECT_EQ(chrono::floor<hours>(ref), *from_iso_8601("1980-05-08T14-00:00"sv));
  EXPECT_EQ(chrono::floor<hours>(ref), *from_iso_8601("1980-05-08T14+00:00"sv));
  EXPECT_EQ(chrono::floor<hours>(ref), *from_iso_8601("1980-05-08T16+02:00"sv));
  EXPECT_EQ(chrono::floor<hours>(ref), *from_iso_8601("1980-05-08T14-00"sv));
  EXPECT_EQ(chrono::floor<hours>(ref), *from_iso_8601("1980-05-08T14+0000"sv));
  EXPECT_EQ(chrono::floor<hours>(ref), *from_iso_8601("1980-05-08T16+02"sv));
  EXPECT_EQ(chrono::floor<hours>(ref), *from_iso_8601("1980-05-08T16+0200"sv));

  EXPECT_EQ(ref, *from_iso_8601("1980-05-08T14:10Z"sv));
  EXPECT_EQ(ref, *from_iso_8601("1980-05-08T14:10-00"sv));
  EXPECT_EQ(ref, *from_iso_8601("1980-05-08T14:10-00:00"sv));
  EXPECT_EQ(ref, *from_iso_8601("1980-05-08T14:10-0000"sv));
  EXPECT_EQ(ref, *from_iso_8601("1980-05-08T14:10+00"sv));
  EXPECT_EQ(ref, *from_iso_8601("1980-05-08T14:10+00:00"sv));
  EXPECT_EQ(ref, *from_iso_8601("1980-05-08T16:10+02"sv));
  EXPECT_EQ(ref, *from_iso_8601("1980-05-08T16:10+02:00"sv));
  EXPECT_EQ(ref, *from_iso_8601("1980-05-08T16:10+0200"sv));
  EXPECT_EQ(ref, *from_iso_8601("1980-05-08T12:10-02"sv));
  EXPECT_EQ(ref, *from_iso_8601("1980-05-08T12:10-02:00"sv));
  EXPECT_EQ(ref, *from_iso_8601("1980-05-08T12:10-0200"sv));

  EXPECT_EQ(ref + 5s, *from_iso_8601("1980-05-08T14:10:05Z"sv));
  EXPECT_EQ(ref + 5s , *from_iso_8601("1980-05-08T14:10:05-00:00"sv));
  EXPECT_EQ(ref + 5s , *from_iso_8601("1980-05-08T14:10:05-00"sv));
  EXPECT_EQ(ref + 5s , *from_iso_8601("1980-05-08T14:10:05-0000"sv));
  EXPECT_EQ(ref + 5s, *from_iso_8601("1980-05-08T14:10:05+00:00"sv));
  EXPECT_EQ(ref + 5s, *from_iso_8601("1980-05-08T14:10:05+0000"sv));
  EXPECT_EQ(ref + 5s, *from_iso_8601("1980-05-08T14:10:05+00"sv));
  EXPECT_EQ(ref + 5s, *from_iso_8601("1980-05-08T16:10:05+02:00"sv));
  EXPECT_EQ(ref + 5s, *from_iso_8601("1980-05-08T16:10:05+0200"sv));

  EXPECT_EQ(ref + 5s + 30ns, *from_iso_8601("1980-05-08T14:10:05,000000030Z"sv));
  EXPECT_EQ(ref + 5s + 30ns, *from_iso_8601("1980-05-08T14:10:05.000000030Z"sv));
  EXPECT_EQ(ref + 5s + 30ns, *from_iso_8601("1980-05-08T14:10:05,000000030-00:00"sv));
  EXPECT_EQ(ref + 5s + 30ns, *from_iso_8601("1980-05-08T14:10:05,000000030-00"sv));
  EXPECT_EQ(ref + 5s + 30ns, *from_iso_8601("1980-05-08T14:10:05,000000030-0000"sv));
  EXPECT_EQ(ref + 5s + 30ns, *from_iso_8601("1980-05-08T14:10:05.000000030-00:00"sv));
  EXPECT_EQ(ref + 5s + 30ns, *from_iso_8601("1980-05-08T14:10:05,000000030+00:00"sv));
  EXPECT_EQ(ref + 5s + 30ns, *from_iso_8601("1980-05-08T14:10:05,000000030+00"sv));
  EXPECT_EQ(ref + 5s + 30ns, *from_iso_8601("1980-05-08T14:10:05,000000030+0000"sv));
  EXPECT_EQ(ref + 5s + 30ns, *from_iso_8601("1980-05-08T14:10:05.000000030+00:00"sv));
  EXPECT_EQ(ref + 5s + 30ns, *from_iso_8601("1980-05-08T16:10:05,000000030+02:00"sv));
  EXPECT_EQ(ref + 5s + 30ns, *from_iso_8601("1980-05-08T16:10:05,000000030+02"sv));
  EXPECT_EQ(ref + 5s + 30ns, *from_iso_8601("1980-05-08T16:10:05,000000030+0200"sv));
  EXPECT_EQ(ref + 5s + 30ns, *from_iso_8601("1980-05-08T16:10:05.000000030+02:00"sv));
  EXPECT_EQ(ref + 5s + 30ns, *from_iso_8601("1980-05-08T16:10:05.000000030+02"sv));
  EXPECT_EQ(ref + 5s + 30ns, *from_iso_8601("1980-05-08T16:10:05.000000030+0200"sv));

  // Lax

  EXPECT_EQ(ref + 5s + 30ns, *from_iso_8601("1980-05-08T14:10:05,000000030Z"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10:05,000000030z"sv));
  EXPECT_EQ(ref + 5s + 30ns, *from_iso_8601("1980-05-08T14:10:05,000000030z"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08t14:10:05.000000030Z"sv));
  EXPECT_EQ(ref + 5s + 30ns, *from_iso_8601("1980-05-08t14:10:05.000000030Z"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08 14:10:05.000000030Z"sv));
  EXPECT_EQ(ref + 5s + 30ns, *from_iso_8601("1980-05-08 14:10:05.000000030Z"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10:05.000000030z"sv));
  EXPECT_EQ(ref + 5s + 30ns, *from_iso_8601("1980-05-08T14:10:05.000000030z"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10:05.000000030"sv));
  EXPECT_EQ(ref + 5s + 30ns, *from_iso_8601("1980-05-08T14:10:05.000000030"sv, iso_8601_mode::lax));

  EXPECT_EQ(ref + 5s + 30ns, *from_iso_8601("1980-05-08T14:10:05,000000030+00:00"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08t14:10:05,000000030+00:00"sv));
  EXPECT_EQ(ref + 5s + 30ns, *from_iso_8601("1980-05-08t14:10:05,000000030+00:00"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08 14:10:05,000000030+00:00"sv));
  EXPECT_EQ(ref + 5s + 30ns, *from_iso_8601("1980-05-08 14:10:05,000000030+00:00"sv, iso_8601_mode::lax));

  EXPECT_FALSE(from_iso_8601("1980-05-08T14"sv));
  EXPECT_EQ(chrono::floor<hours>(ref), *from_iso_8601("1980-05-08T14"sv, iso_8601_mode::lax));

  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10"sv));
  EXPECT_EQ(ref, *from_iso_8601("1980-05-08T14:10"sv, iso_8601_mode::lax));

  EXPECT_FALSE(from_iso_8601("1980-05-08T14z"sv));
  EXPECT_EQ(chrono::floor<hours>(ref), *from_iso_8601("1980-05-08T14z"sv, iso_8601_mode::lax));

  EXPECT_FALSE(from_iso_8601("1980-05-08t16+02:00"sv));
  EXPECT_EQ(chrono::floor<hours>(ref), *from_iso_8601("1980-05-08t16+02:00"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08 16+02:00"sv));
  EXPECT_EQ(chrono::floor<hours>(ref), *from_iso_8601("1980-05-08 16+02:00"sv, iso_8601_mode::lax));

  // Garbage

  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10+24:00"sv));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10+24:00"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10+15:00"sv));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10+15:00"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10-24:00"sv));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10-24:00"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10-13:00"sv));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10-13:00"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08+02:00"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T+02:00"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14Zgarbage"sv));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14Zgarbage"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14-00:00garbage"sv));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14-00:00garbage"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14+00:00garbage"sv));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14+00:00garbage"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T16+02:00garbage"sv));
  EXPECT_FALSE(from_iso_8601("1980-05-08T16+02:00garbage"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:Z"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10-00:"sv));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10-00:"sv, iso_8601_mode::lax));

  EXPECT_FALSE(from_iso_8601("1980T14:10Z"sv));
  EXPECT_FALSE(from_iso_8601("1980T14:10Z"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05T14:10Z"sv));
  EXPECT_FALSE(from_iso_8601("1980-05T14:10Z"sv, iso_8601_mode::lax));
}

TEST(iso_8601, epoch_timezone)
{
  // Pre-epoch
  EXPECT_FALSE(from_iso_8601("1970-01-01T00:00+05:00"sv));
  EXPECT_FALSE(from_iso_8601("1970-01-01T00:00+05:00"sv, iso_8601_mode::lax));
  EXPECT_EQ(ceph::real_time(0s), *from_iso_8601("1970-01-01T05:00+05:00"sv));

  // Near the end of the epoch
  EXPECT_TRUE(from_iso_8601("2553-12-31T23:59:59.999999999-12:00"sv));
  EXPECT_TRUE(from_iso_8601("2554-01-01"sv));
  EXPECT_EQ(ceph::real_time::max(), *from_iso_8601("2554-07-21T23:34:33.709551615Z"));
  EXPECT_FALSE(from_iso_8601("2554-07-21T23:34:33.709551616Z"));
  EXPECT_FALSE(from_iso_8601("9999-12-31T23:59:59Z"sv));
}

TEST(iso_8601, distinct_fields) {
  const real_time ref{984166567s + 800'000'000ns};
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07,800000000Z"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07,800000000+00"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07,800000000+00:00"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07,800000000+0000"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07,800000000-00"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07,800000000-00:00"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07,800000000-0000"));

  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07.800000000Z"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07.800000000+00"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07.800000000+00:00"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07.800000000+0000"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07.800000000-00"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07.800000000-00:00"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07.800000000-0000"));

  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07,80000000Z"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07,8000000Z"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07,800000Z"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07,80000Z"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07,8000Z"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07,800Z"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07,80Z"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07,8Z"));

  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07.80000000Z"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07.8000000Z"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07.800000Z"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07.80000Z"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07.8000Z"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07.800Z"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07.80Z"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07.8Z"));

  EXPECT_EQ(ref, *from_iso_8601("2001-03-10T05:06:07.80000000+09:30"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-10T05:06:07.8+09:30"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-10T05:06:07.80000000+0930"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-10T05:06:07.8+0930"));

  EXPECT_EQ(ref, *from_iso_8601("2001-03-10T05:06:07.80000000+09:30", iso_8601_mode::lax));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-10t05:06:07.80000000+09:30", iso_8601_mode::lax));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-10 05:06:07.80000000+09:30", iso_8601_mode::lax));

  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07,800000000Z", iso_8601_mode::lax));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07,800000000z", iso_8601_mode::lax));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07,800000000", iso_8601_mode::lax));

  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07,8Z", iso_8601_mode::lax));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07,8z", iso_8601_mode::lax));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07,8", iso_8601_mode::lax));

  // Rinderpest eradication declared in 2011-06-28
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07,800000000920110628Z"));
  EXPECT_EQ(ref, *from_iso_8601("2001-03-09T19:36:07,800000000920110628Z", iso_8601_mode::lax));
}
