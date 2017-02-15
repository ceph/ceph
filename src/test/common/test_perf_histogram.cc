// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 &smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 OVH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/perf_histogram.h"

#include "gtest/gtest.h"

template <int DIM>
class PerfHistogramAccessor : public PerfHistogram<DIM> {
public:
  typedef PerfHistogram<DIM> Base;

  using Base::PerfHistogram;

  static int64_t get_bucket_for_axis(
      int64_t value, const PerfHistogramCommon::axis_config_d& axis_config) {
    return Base::get_bucket_for_axis(value, axis_config);
  }

  static std::vector<std::pair<int64_t, int64_t>> get_axis_bucket_ranges(
      const PerfHistogramCommon::axis_config_d& axis_config) {
    return Base::get_axis_bucket_ranges(axis_config);
  }

  const typename Base::axis_config_d& get_axis_config(int num) {
    return Base::m_axes_config[num];
  }

  template <typename F1, typename F2, typename F3>
  void visit_values(F1 f1, F2 f2, F3 f3) {
    Base::visit_values(f1, f2, f3);
  }
};

TEST(PerfHistogram, GetBucketForAxis) {
  PerfHistogramCommon::axis_config_d linear{
      "", PerfHistogramCommon::SCALE_LINEAR, 100, 3, 4};

  ASSERT_EQ(0, PerfHistogramAccessor<1>::get_bucket_for_axis(-1, linear));
  ASSERT_EQ(0, PerfHistogramAccessor<1>::get_bucket_for_axis(0, linear));
  ASSERT_EQ(0, PerfHistogramAccessor<1>::get_bucket_for_axis(99, linear));
  ASSERT_EQ(1, PerfHistogramAccessor<1>::get_bucket_for_axis(100, linear));
  ASSERT_EQ(1, PerfHistogramAccessor<1>::get_bucket_for_axis(101, linear));
  ASSERT_EQ(1, PerfHistogramAccessor<1>::get_bucket_for_axis(102, linear));
  ASSERT_EQ(2, PerfHistogramAccessor<1>::get_bucket_for_axis(103, linear));
  ASSERT_EQ(2, PerfHistogramAccessor<1>::get_bucket_for_axis(105, linear));
  ASSERT_EQ(3, PerfHistogramAccessor<1>::get_bucket_for_axis(106, linear));
  ASSERT_EQ(3, PerfHistogramAccessor<1>::get_bucket_for_axis(108, linear));
  ASSERT_EQ(3, PerfHistogramAccessor<1>::get_bucket_for_axis(109, linear));

  ASSERT_EQ(0, PerfHistogramAccessor<1>::get_bucket_for_axis(
                   std::numeric_limits<int64_t>::min(), linear));
  ASSERT_EQ(3, PerfHistogramAccessor<1>::get_bucket_for_axis(
                   std::numeric_limits<int64_t>::max(), linear));

  PerfHistogramCommon::axis_config_d logarithmic{
      "", PerfHistogramCommon::SCALE_LOG2, 100, 3, 5};

  ASSERT_EQ(0, PerfHistogramAccessor<1>::get_bucket_for_axis(-1, logarithmic));
  ASSERT_EQ(0, PerfHistogramAccessor<1>::get_bucket_for_axis(0, logarithmic));
  ASSERT_EQ(0, PerfHistogramAccessor<1>::get_bucket_for_axis(99, logarithmic));
  ASSERT_EQ(1, PerfHistogramAccessor<1>::get_bucket_for_axis(100, logarithmic));
  ASSERT_EQ(1, PerfHistogramAccessor<1>::get_bucket_for_axis(101, logarithmic));
  ASSERT_EQ(1, PerfHistogramAccessor<1>::get_bucket_for_axis(102, logarithmic));
  ASSERT_EQ(2, PerfHistogramAccessor<1>::get_bucket_for_axis(103, logarithmic));
  ASSERT_EQ(2, PerfHistogramAccessor<1>::get_bucket_for_axis(105, logarithmic));
  ASSERT_EQ(3, PerfHistogramAccessor<1>::get_bucket_for_axis(106, logarithmic));
  ASSERT_EQ(3, PerfHistogramAccessor<1>::get_bucket_for_axis(111, logarithmic));
  ASSERT_EQ(4, PerfHistogramAccessor<1>::get_bucket_for_axis(112, logarithmic));
  ASSERT_EQ(4, PerfHistogramAccessor<1>::get_bucket_for_axis(124, logarithmic));

  ASSERT_EQ(0, PerfHistogramAccessor<1>::get_bucket_for_axis(
                   std::numeric_limits<int64_t>::min(), logarithmic));
  ASSERT_EQ(4, PerfHistogramAccessor<1>::get_bucket_for_axis(
                   std::numeric_limits<int64_t>::max(), logarithmic));
}

static const int XS = 5;
static const int YS = 7;

static const auto x_axis = PerfHistogramCommon::axis_config_d{
    "x", PerfHistogramCommon::SCALE_LINEAR, 0, 1, XS};
static const auto y_axis = PerfHistogramCommon::axis_config_d{
    "y", PerfHistogramCommon::SCALE_LOG2, 0, 1, YS};

TEST(PerfHistogram, ZeroedInitially) {
  PerfHistogramAccessor<2> h{x_axis, y_axis};
  for (int x = 0; x < XS; ++x) {
    for (int y = 0; y < YS; ++y) {
      ASSERT_EQ(0UL, h.read_bucket(x, y));
    }
  }
}

TEST(PerfHistogram, Copy) {
  PerfHistogramAccessor<2> h1{x_axis, y_axis};
  h1.inc_bucket(1, 1);
  h1.inc_bucket(2, 3);
  h1.inc_bucket(4, 5);

  PerfHistogramAccessor<2> h2 = h1;

  const int cx = 1;
  const int cy = 2;

  h1.inc_bucket(cx, cy);

  // Axes configuration must be equal
  for (int i = 0; i < 2; i++) {
    const auto& ac1 = h1.get_axis_config(i);
    const auto& ac2 = h2.get_axis_config(i);
    ASSERT_EQ(ac1.m_name, ac2.m_name);
    ASSERT_EQ(ac1.m_scale_type, ac2.m_scale_type);
    ASSERT_EQ(ac1.m_min, ac2.m_min);
    ASSERT_EQ(ac1.m_quant_size, ac2.m_quant_size);
    ASSERT_EQ(ac1.m_buckets, ac2.m_buckets);
  }

  // second histogram must have histogram values equal to the first
  // one at the time of copy
  for (int x = 0; x < XS; x++) {
    for (int y = 0; y < YS; y++) {
      if (x == cx && y == cy) {
        ASSERT_NE(h1.read_bucket(x, y), h2.read_bucket(x, y));
      } else {
        ASSERT_EQ(h1.read_bucket(x, y), h2.read_bucket(x, y));
      }
    }
  }
}

TEST(PerfHistogram, SimpleValues) {
  PerfHistogramAccessor<2> h{x_axis, y_axis};
  ASSERT_EQ(0UL, h.read_bucket(1, 1));
  h.inc(0, 0);
  ASSERT_EQ(1UL, h.read_bucket(1, 1));

  ASSERT_EQ(0UL, h.read_bucket(2, 2));
  h.inc(1, 1);
  ASSERT_EQ(1UL, h.read_bucket(2, 2));

  ASSERT_EQ(0UL, h.read_bucket(3, 3));
  h.inc(2, 2);
  ASSERT_EQ(1UL, h.read_bucket(3, 3));

  ASSERT_EQ(0UL, h.read_bucket(4, 3));
  h.inc(3, 3);
  ASSERT_EQ(1UL, h.read_bucket(4, 3));
}

TEST(PerfHistogram, OneBucketRange) {
  auto ranges = PerfHistogramAccessor<1>::get_axis_bucket_ranges(
      PerfHistogramCommon::axis_config_d{"", PerfHistogramCommon::SCALE_LINEAR,
                                         0, 1, 1});

  ASSERT_EQ(1UL, ranges.size());
  ASSERT_EQ(std::numeric_limits<int64_t>::min(), ranges[0].first);
  ASSERT_EQ(std::numeric_limits<int64_t>::max(), ranges[0].second);
}

TEST(PerfHistogram, TwoBucketRange) {
  auto ranges = PerfHistogramAccessor<1>::get_axis_bucket_ranges(
      PerfHistogramCommon::axis_config_d{"", PerfHistogramCommon::SCALE_LINEAR,
                                         0, 1, 2});

  ASSERT_EQ(2UL, ranges.size());
  ASSERT_EQ(std::numeric_limits<int64_t>::min(), ranges[0].first);
  ASSERT_EQ(-1, ranges[0].second);
  ASSERT_EQ(0, ranges[1].first);
  ASSERT_EQ(std::numeric_limits<int64_t>::max(), ranges[1].second);
}

TEST(PerfHistogram, LinearBucketRange) {
  PerfHistogramCommon::axis_config_d ac{"", PerfHistogramCommon::SCALE_LINEAR,
                                        100, 10, 15};
  auto ranges = PerfHistogramAccessor<1>::get_axis_bucket_ranges(ac);

  for (size_t i = 0; i < ranges.size(); ++i) {
    ASSERT_EQ(
      static_cast<long>(i), PerfHistogramAccessor<1>::get_bucket_for_axis(ranges[i].first, ac));
    ASSERT_EQ(
      static_cast<long>(i), PerfHistogramAccessor<1>::get_bucket_for_axis(ranges[i].second, ac));
  }

  for (size_t i = 1; i < ranges.size(); ++i) {
    ASSERT_EQ(ranges[i].first, ranges[i - 1].second + 1);
  }
}

TEST(PerfHistogram, LogarithmicBucketRange) {
  PerfHistogramCommon::axis_config_d ac{"", PerfHistogramCommon::SCALE_LOG2,
                                        100, 10, 15};
  auto ranges = PerfHistogramAccessor<1>::get_axis_bucket_ranges(ac);

  for (size_t i = 0; i < ranges.size(); ++i) {
    ASSERT_EQ(
      static_cast<long>(i), PerfHistogramAccessor<1>::get_bucket_for_axis(ranges[i].first, ac));
    ASSERT_EQ(
      static_cast<long>(i), PerfHistogramAccessor<1>::get_bucket_for_axis(ranges[i].second, ac));
  }

  for (size_t i = 1; i < ranges.size(); ++i) {
    ASSERT_EQ(ranges[i].first, ranges[i - 1].second + 1);
  }
}

TEST(PerfHistogram, AxisAddressing) {
  PerfHistogramCommon::axis_config_d ac1{"", PerfHistogramCommon::SCALE_LINEAR,
                                         0, 1, 7};
  PerfHistogramCommon::axis_config_d ac2{"", PerfHistogramCommon::SCALE_LINEAR,
                                         0, 1, 9};
  PerfHistogramCommon::axis_config_d ac3{"", PerfHistogramCommon::SCALE_LINEAR,
                                         0, 1, 11};

  PerfHistogramAccessor<3> h{ac1, ac2, ac3};

  h.inc(1, 2, 3);  // Should end up in buckets 2, 3, 4
  h.inc_bucket(4, 5, 6);

  std::vector<int64_t> rawValues;
  h.visit_values([](int) {},
                 [&rawValues](int64_t value) { rawValues.push_back(value); },
                 [](int) {});

  for (size_t i = 0; i < rawValues.size(); ++i) {
    switch (i) {
      case 4 + 11 * (3 + 9 * 2):
      case 6 + 11 * (5 + 9 * 4):
        ASSERT_EQ(1, rawValues[i]);
        break;
      default:
        ASSERT_EQ(0, rawValues[i]);
        break;
    }
  }
}
