// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat <contact@redhat.com>
 *
 * LGPL2.1 (see COPYING-LGPL2.1) or later
 */

#include <chrono>

#include <gtest/gtest.h>

#include "common/ceph_time.h"
#include "common/iso_8601.h"

using std::chrono::minutes;
using std::chrono::seconds;
using std::chrono::time_point_cast;

using ceph::from_iso_8601;
using ceph::iso_8601_format;
using ceph::real_clock;
using ceph::real_time;
using ceph::to_iso_8601;

TEST(iso_8601, epoch) {
  const auto epoch = real_clock::from_time_t(0);

  ASSERT_EQ("1970", to_iso_8601(epoch, iso_8601_format::Y));
  ASSERT_EQ("1970-01", to_iso_8601(epoch, iso_8601_format::YM));
  ASSERT_EQ("1970-01-01", to_iso_8601(epoch, iso_8601_format::YMD));
  ASSERT_EQ("1970-01-01T00Z", to_iso_8601(epoch, iso_8601_format::YMDh));
  ASSERT_EQ("1970-01-01T00:00Z", to_iso_8601(epoch, iso_8601_format::YMDhm));
  ASSERT_EQ("1970-01-01T00:00:00Z",
	    to_iso_8601(epoch, iso_8601_format::YMDhms));
  ASSERT_EQ("1970-01-01T00:00:00.000000000Z",
	    to_iso_8601(epoch, iso_8601_format::YMDhmsn));

  ASSERT_EQ(epoch, *from_iso_8601("1970"));
  ASSERT_EQ(epoch, *from_iso_8601("1970-01"));
  ASSERT_EQ(epoch, *from_iso_8601("1970-01-01"));
  ASSERT_EQ(epoch, *from_iso_8601("1970-01-01T00:00Z"));
  ASSERT_EQ(epoch, *from_iso_8601("1970-01-01T00:00:00Z"));
  ASSERT_EQ(epoch, *from_iso_8601("1970-01-01T00:00:00.000000000Z"));
}

TEST(iso_8601, now) {
  const auto now = real_clock::now();

  ASSERT_EQ(real_time(time_point_cast<minutes>(now)),
	    *from_iso_8601(to_iso_8601(now, iso_8601_format::YMDhm)));
  ASSERT_EQ(real_time(time_point_cast<seconds>(now)),
	    *from_iso_8601(
	      to_iso_8601(now, iso_8601_format::YMDhms)));
  ASSERT_EQ(now,
	    *from_iso_8601(
	      to_iso_8601(now, iso_8601_format::YMDhmsn)));
}
