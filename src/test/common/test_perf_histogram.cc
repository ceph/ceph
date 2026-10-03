// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 &expandtab

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

  std::array<int64_t, 3> custom_bounds = {100, 200, 500};
  PerfHistogramCommon::axis_config_d custom{"", custom_bounds};

  ASSERT_EQ(0, PerfHistogramAccessor<1>::get_bucket_for_axis(-1, custom));
  ASSERT_EQ(1, PerfHistogramAccessor<1>::get_bucket_for_axis(0, custom));
  ASSERT_EQ(1, PerfHistogramAccessor<1>::get_bucket_for_axis(99, custom));
  ASSERT_EQ(1, PerfHistogramAccessor<1>::get_bucket_for_axis(100, custom));
  // note: compared to log2/linear buckets are inclusive and 101 falls
  // into bucket 2 not 1 as above
  ASSERT_EQ(2, PerfHistogramAccessor<1>::get_bucket_for_axis(101, custom));
  ASSERT_EQ(2, PerfHistogramAccessor<1>::get_bucket_for_axis(200, custom));
  ASSERT_EQ(3, PerfHistogramAccessor<1>::get_bucket_for_axis(201, custom));
  ASSERT_EQ(3, PerfHistogramAccessor<1>::get_bucket_for_axis(500, custom));
  ASSERT_EQ(4, PerfHistogramAccessor<1>::get_bucket_for_axis(501, custom));

  ASSERT_EQ(0, PerfHistogramAccessor<1>::get_bucket_for_axis(
                   std::numeric_limits<int64_t>::min(), custom));
  ASSERT_EQ(4, PerfHistogramAccessor<1>::get_bucket_for_axis(
                   std::numeric_limits<int64_t>::max(), custom));
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
  for (int i = 0; i < 2; i++) {
    ASSERT_EQ(h1.get_sum(i), h2.get_sum(i));
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

TEST(PerfHistogram, CustomBucketRange) {
  std::array<int64_t, 3> custom_bounds = {100, 200, 500};
  PerfHistogramCommon::axis_config_d ac{"", custom_bounds};
  auto ranges = PerfHistogramAccessor<1>::get_axis_bucket_ranges(ac);

  ASSERT_EQ(std::size(custom_bounds) + 2, ranges.size());

  for (size_t i = 0; i < ranges.size(); ++i) {
    ASSERT_EQ(
      static_cast<long>(i), PerfHistogramAccessor<1>::get_bucket_for_axis(ranges[i].first, ac));
    ASSERT_EQ(
      static_cast<long>(i), PerfHistogramAccessor<1>::get_bucket_for_axis(ranges[i].second, ac));
  }

  for (size_t i = 1; i < ranges.size(); ++i) {
    ASSERT_EQ(ranges[i].first, ranges[i - 1].second + 1);
  }

  for (size_t i = 0; i < std::size(custom_bounds); ++i) {
    ASSERT_EQ(custom_bounds[i], ranges[i + 1].second);
  }
}

TEST(PerfHistogram, CustomAxisBucketsInclusiveValues) {
  std::array<int64_t, 3> custom_bounds = {100, 200, 500};
  PerfHistogramAccessor<1> h{
      PerfHistogramCommon::axis_config_d{"", custom_bounds}};

  h.inc(100);
  h.inc(101);
  h.inc(1000);

  ASSERT_EQ(0UL, h.read_bucket(0));
  ASSERT_EQ(1UL, h.read_bucket(1));
  ASSERT_EQ(1UL, h.read_bucket(2));
  ASSERT_EQ(0UL, h.read_bucket(3));
  ASSERT_EQ(1UL, h.read_bucket(4));
  ASSERT_EQ(1201UL, h.get_sum(0));
}

TEST(PerfHistogram, WebLatencyBucketRange) {
  auto ac = PerfHistogramCommon::axis_config_d::web_latency("");
  auto ranges = PerfHistogramAccessor<1>::get_axis_bucket_ranges(ac);

  ASSERT_EQ(13, ac.m_buckets);
  ASSERT_EQ(13UL, ranges.size());
  ASSERT_EQ(PerfHistogramCommon::AXIS_UNIT_NANOSECONDS, ac.m_unit);

  // Upper edges land exactly on the Prometheus defaults expressed in
  // nanoseconds, so an exporter emits le="0.005" .. le="10"
  ASSERT_EQ(5'000'000, ranges[1].second);
  ASSERT_EQ(10'000'000, ranges[2].second);
  ASSERT_EQ(25'000'000, ranges[3].second);
  ASSERT_EQ(10'000'000'000, ranges[11].second);
  ASSERT_EQ(std::numeric_limits<int64_t>::max(), ranges[12].second);

  ASSERT_EQ(1, PerfHistogramAccessor<1>::get_bucket_for_axis(5'000'000, ac));
  ASSERT_EQ(2, PerfHistogramAccessor<1>::get_bucket_for_axis(5'000'001, ac));
  ASSERT_EQ(12,
            PerfHistogramAccessor<1>::get_bucket_for_axis(10'000'000'001, ac));
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

TEST(PerfHistogram, SumPerDimensionAndBucketIndependent)
{
  const auto x_axis = PerfHistogramCommon::axis_config_d{
      "x", PerfHistogramCommon::SCALE_LINEAR, 100, 10, 5};
  const auto y_axis = PerfHistogramCommon::axis_config_d{
      "y", PerfHistogramCommon::SCALE_LINEAR, 0, 1, 5};
  PerfHistogramAccessor<2> h{x_axis, y_axis};
  h.inc(50, 1);
  h.inc(105, 2);
  h.inc(108, 3);
  h.inc(10000, 4);

  ASSERT_EQ(h.get_sum(0), 50 + 105 + 108 + 10000);
  ASSERT_EQ(h.get_sum(1), 1 + 2 + 3 + 4);
  h.reset();
  ASSERT_EQ(h.get_sum(0), 0);
  ASSERT_EQ(h.get_sum(1), 0);
}

TEST(PerfHistogram, IncBucketDoesNotSum)
{
  PerfHistogramAccessor<2> h{x_axis, y_axis};
  // inc_bucket increments counts at bucket indices. This means we
  // have no observed values and cannot increment the sum
  h.inc_bucket(1, 1);
  h.inc_bucket(2, 3);

  ASSERT_EQ(1UL, h.read_bucket(1,1));
  ASSERT_EQ(h.get_sum(0), 0);
  ASSERT_EQ(h.get_sum(1), 0);
}
