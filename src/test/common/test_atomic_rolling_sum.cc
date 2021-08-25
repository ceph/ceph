// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include <common/atomic_rolling_sum.h>
#include <gtest/gtest.h>

using namespace std::chrono_literals;

TEST(AtomicRollingSum, add1)
{
  // track 1 32-bit counter at 1-ms interval
  using Counter = ceph::AtomicRollingSum<1, uint32_t, std::milli, 1>;
  auto counter = Counter{};
  EXPECT_EQ(1, counter.add_sum(1, 0ms));
  EXPECT_EQ(2, counter.add_sum(1, 0ms));
  EXPECT_EQ(1, counter.add_sum(1, 1ms)); // expires 2 at t=0
  EXPECT_EQ(1, counter.add_sum(1, 2ms)); // expires 1 at t=1
  EXPECT_EQ(1, counter.add_sum(1, 4ms)); // expires 1 at t=2
  EXPECT_EQ(1, counter.add_sum(1, 5ms)); // expires 1 at t=4
}

TEST(AtomicRollingSum, add2)
{
  // track 2 32-bit counters at 1-ms intervals
  using Counter = ceph::AtomicRollingSum<2, uint32_t, std::milli, 1>;
  // run the test cases with each possible shift from 0-1
  for (auto t = 0ms; t <= 1ms; ++t) {
    auto counter = Counter{};
    EXPECT_EQ(1, counter.add_sum(1, t));
    EXPECT_EQ(2, counter.add_sum(1, t));
    EXPECT_EQ(1, counter.add_sum(1, t + 2ms)); // expires 2 at t+0
    EXPECT_EQ(2, counter.add_sum(1, t + 3ms));
    EXPECT_EQ(2, counter.add_sum(1, t + 4ms)); // expires 1 at t+2
    EXPECT_EQ(1, counter.add_sum(1, t + 6ms)); // expires 2 from t+3-4
    EXPECT_EQ(2, counter.add_sum(1, t + 7ms));
  }
}

TEST(AtomicRollingSum, add3)
{
  // track 3 32-bit counters at 1-ms intervals
  using Counter = ceph::AtomicRollingSum<3, uint32_t, std::milli, 1>;
  // run the test cases with each possible shift from 0-2
  for (auto t = 0ms; t <= 2ms; ++t) {
    auto counter = Counter{};
    EXPECT_EQ(1, counter.add_sum(1, t));
    EXPECT_EQ(2, counter.add_sum(1, t));
    EXPECT_EQ(3, counter.add_sum(1, t + 2ms));
    EXPECT_EQ(2, counter.add_sum(1, t + 3ms)); // expires 2 at t+0
    EXPECT_EQ(2, counter.add_sum(1, t + 5ms)); // expires 1 at t+2
    EXPECT_EQ(1, counter.add_sum(1, t + 8ms)); // expires 2 from t+3-5
    EXPECT_EQ(2, counter.add_sum(1, t + 9ms));
    EXPECT_EQ(3, counter.add_sum(1, t + 10ms));
  }
}

TEST(AtomicRollingSum, add4)
{
  // track 4 32-bit counters at 1-ms intervals
  using Counter = ceph::AtomicRollingSum<4, uint32_t, std::milli, 1>;
  // run the test cases with each possible shift from 0-3
  for (auto t = 0ms; t <= 3ms; ++t) {
    auto counter = Counter{};
    EXPECT_EQ(1, counter.add_sum(1, t));
    EXPECT_EQ(2, counter.add_sum(1, t));
    EXPECT_EQ(3, counter.add_sum(1, t + 2ms));
    EXPECT_EQ(4, counter.add_sum(1, t + 3ms));
    EXPECT_EQ(3, counter.add_sum(1, t + 4ms)); // expires 2 at t+0
    EXPECT_EQ(4, counter.add_sum(1, t + 5ms));
    EXPECT_EQ(4, counter.add_sum(1, t + 6ms)); // expires 1 at t+2
    EXPECT_EQ(2, counter.add_sum(1, t + 9ms)); // expires 3 from t+3-5
    EXPECT_EQ(1, counter.add_sum(1, t + 13ms)); // expires 2 from t+6-9
    EXPECT_EQ(2, counter.add_sum(1, t + 14ms));
    EXPECT_EQ(3, counter.add_sum(1, t + 15ms));
  }
}

TEST(AtomicRollingSum, sub4)
{
  // same test cases with signed ints and negative numbers
  using Counter = ceph::AtomicRollingSum<4, int32_t, std::milli, 1>;
  for (auto t = 0ms; t <= 3ms; ++t) {
    auto counter = Counter{};
    EXPECT_EQ(-1, counter.add_sum(-1, t));
    EXPECT_EQ(-2, counter.add_sum(-1, t));
    EXPECT_EQ(-3, counter.add_sum(-1, t + 2ms));
    EXPECT_EQ(-4, counter.add_sum(-1, t + 3ms));
    EXPECT_EQ(-3, counter.add_sum(-1, t + 4ms)); // expires -2 at t+0
    EXPECT_EQ(-4, counter.add_sum(-1, t + 5ms));
    EXPECT_EQ(-4, counter.add_sum(-1, t + 6ms)); // expires -1 at t+2
    EXPECT_EQ(-2, counter.add_sum(-1, t + 9ms)); // expires -3 from t+3-5
    EXPECT_EQ(-1, counter.add_sum(-1, t + 13ms)); // expires -2 from t+6-9
    EXPECT_EQ(-2, counter.add_sum(-1, t + 14ms));
    EXPECT_EQ(-3, counter.add_sum(-1, t + 15ms));
  }
}

TEST(AtomicRollingSum, out_of_order_add)
{
  // track 4 32-bit counters at 1-ms intervals
  using Counter = ceph::AtomicRollingSum<4, uint32_t, std::milli, 1>;
  for (auto t = 0ms; t <= 3ms; ++t) {
    auto counter = Counter{};
    EXPECT_EQ(1, counter.add_sum(1, t));
    EXPECT_EQ(2, counter.add_sum(1, t));
    EXPECT_EQ(3, counter.add_sum(1, t + 3ms));
    EXPECT_EQ(4, counter.add_sum(1, t + 2ms)); // t+2 still valid
    EXPECT_EQ(3, counter.add_sum(1, t + 4ms)); // expires 2 at t+0
    EXPECT_EQ(3, counter.add_sum(1, t)); // t+0 no longer valid
  }
}

TEST(AtomicRollingSum, out_of_order_sum)
{
  // track 4 32-bit counters at 1-ms intervals
  using Counter = ceph::AtomicRollingSum<4, uint32_t, std::milli, 1>;
  for (auto t = 0ms; t <= 3ms; ++t) {
    auto counter = Counter{};
    EXPECT_EQ(1, counter.add_sum(1, t + 4ms));
    EXPECT_EQ(2, counter.add_sum(1, t + 5ms));
    EXPECT_EQ(3, counter.add_sum(1, t + 6ms));
    EXPECT_EQ(4, counter.add_sum(1, t + 7ms));
    // same sum for all <= t+7
    EXPECT_EQ(4, counter.sum(t));
    EXPECT_EQ(4, counter.sum(t + 1ms));
    EXPECT_EQ(4, counter.sum(t + 2ms));
    EXPECT_EQ(4, counter.sum(t + 3ms));
    EXPECT_EQ(4, counter.sum(t + 4ms));
    EXPECT_EQ(4, counter.sum(t + 5ms));
    EXPECT_EQ(4, counter.sum(t + 6ms));
    EXPECT_EQ(4, counter.sum(t + 7ms));
    // after t+7 some intervals expire
    EXPECT_EQ(3, counter.sum(t + 8ms));
    EXPECT_EQ(2, counter.sum(t + 9ms));
    EXPECT_EQ(1, counter.sum(t + 10ms));
    EXPECT_EQ(0, counter.sum(t + 11ms));
  }
}
