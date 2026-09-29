// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"
#include "mgr/PerfCounterInstance.h"
#include "common/perf_counters.h"

TEST(PerfCounterInstance, ConstructorNormalCounter) {
  PerfCounterInstance counter(PERFCOUNTER_U64);
  ASSERT_EQ(counter.get_data().size(), 0);
}

TEST(PerfCounterInstance, ConstructorAvgCounter) {
  PerfCounterInstance counter(PERFCOUNTER_LONGRUNAVG);
  ASSERT_EQ(counter.get_data_avg().size(), 0);
}

TEST(PerfCounterInstance, PushSingleValue) {
  PerfCounterInstance counter(PERFCOUNTER_U64);
  utime_t t1(100, 0);
  uint64_t v1 = 42;

  counter.push(t1, v1);

  ASSERT_EQ(counter.get_data().size(), 1);
  ASSERT_EQ(counter.get_latest_data().v, v1);
  ASSERT_EQ(counter.get_latest_data().t, t1);
}

TEST(PerfCounterInstance, PushMultipleValues) {
  PerfCounterInstance counter(PERFCOUNTER_U64);

  for (int i = 0; i < 10; i++) {
    utime_t t(100 + i, 0);
    uint64_t v = i * 10;
    counter.push(t, v);
  }

  ASSERT_EQ(counter.get_data().size(), 10);
  ASSERT_EQ(counter.get_latest_data().v, 90);
  ASSERT_EQ(counter.get_latest_data().t.sec(), 109);
}

// The circular buffer capacity is 20; values beyond that evict the oldest.
TEST(PerfCounterInstance, CircularBufferOverflow) {
  PerfCounterInstance counter(PERFCOUNTER_U64);

  for (int i = 0; i < 25; i++) {
    utime_t t(100 + i, 0);
    uint64_t v = i;
    counter.push(t, v);
  }

  ASSERT_EQ(counter.get_data().size(), 20);
  ASSERT_EQ(counter.get_latest_data().v, 24);
  ASSERT_EQ(counter.get_latest_data().t.sec(), 124);
  ASSERT_EQ(counter.get_data().front().v, 5);
  ASSERT_EQ(counter.get_data().front().t.sec(), 105);
}

TEST(PerfCounterInstance, PushAvgSingleValue) {
  PerfCounterInstance counter(PERFCOUNTER_LONGRUNAVG);
  utime_t t1(100, 0);
  uint64_t sum = 1000;
  uint64_t count = 10;

  counter.push_avg(t1, sum, count);

  ASSERT_EQ(counter.get_data_avg().size(), 1);
  ASSERT_EQ(counter.get_latest_data_avg().s, sum);
  ASSERT_EQ(counter.get_latest_data_avg().c, count);
  ASSERT_EQ(counter.get_latest_data_avg().t, t1);
}

TEST(PerfCounterInstance, PushAvgMultipleValues) {
  PerfCounterInstance counter(PERFCOUNTER_LONGRUNAVG);

  for (int i = 0; i < 10; i++) {
    utime_t t(100 + i, 0);
    uint64_t sum = i * 100;
    uint64_t count = i + 1;
    counter.push_avg(t, sum, count);
  }

  ASSERT_EQ(counter.get_data_avg().size(), 10);
  ASSERT_EQ(counter.get_latest_data_avg().s, 900);
  ASSERT_EQ(counter.get_latest_data_avg().c, 10);
  ASSERT_EQ(counter.get_latest_data_avg().t.sec(), 109);
}

// The circular buffer capacity is 20; values beyond that evict the oldest.
TEST(PerfCounterInstance, AvgCircularBufferOverflow) {
  PerfCounterInstance counter(PERFCOUNTER_LONGRUNAVG);

  for (int i = 0; i < 25; i++) {
    utime_t t(100 + i, 0);
    uint64_t sum = i * 10;
    uint64_t count = i + 1;
    counter.push_avg(t, sum, count);
  }

  ASSERT_EQ(counter.get_data_avg().size(), 20);
  ASSERT_EQ(counter.get_latest_data_avg().s, 240);
  ASSERT_EQ(counter.get_latest_data_avg().c, 25);
  ASSERT_EQ(counter.get_latest_data_avg().t.sec(), 124);
  ASSERT_EQ(counter.get_data_avg().front().s, 50);
  ASSERT_EQ(counter.get_data_avg().front().c, 6);
  ASSERT_EQ(counter.get_data_avg().front().t.sec(), 105);
}

TEST(PerfCounterInstance, GetDataReturnsCorrectBuffer) {
  PerfCounterInstance counter(PERFCOUNTER_U64);

  for (int i = 0; i < 5; i++) {
    utime_t t(100 + i, 0);
    counter.push(t, i * 10);
  }

  const auto& data = counter.get_data();
  ASSERT_EQ(data.size(), 5);

  int idx = 0;
  for (const auto& point : data) {
    ASSERT_EQ(point.v, idx * 10);
    ASSERT_EQ(point.t.sec(), 100 + idx);
    idx++;
  }
}

TEST(PerfCounterInstance, GetDataAvgReturnsCorrectBuffer) {
  PerfCounterInstance counter(PERFCOUNTER_LONGRUNAVG);

  for (int i = 0; i < 5; i++) {
    utime_t t(100 + i, 0);
    counter.push_avg(t, i * 100, i + 1);
  }

  const auto& data = counter.get_data_avg();
  ASSERT_EQ(data.size(), 5);

  int idx = 0;
  for (const auto& point : data) {
    ASSERT_EQ(point.s, idx * 100);
    ASSERT_EQ(point.c, idx + 1);
    ASSERT_EQ(point.t.sec(), 100 + idx);
    idx++;
  }
}

TEST(PerfCounterInstance, DifferentCounterTypes) {
  PerfCounterInstance time_counter(PERFCOUNTER_TIME);
  utime_t t1(100, 0);
  time_counter.push(t1, 12345);
  ASSERT_EQ(time_counter.get_data().size(), 1);
  ASSERT_EQ(time_counter.get_latest_data().v, 12345);

  // LONGRUNAVG|TIME is a distinct type combination; test that it routes to the avg buffer
  PerfCounterInstance avg_time_counter(
      static_cast<perfcounter_type_d>(PERFCOUNTER_LONGRUNAVG | PERFCOUNTER_TIME));
  avg_time_counter.push_avg(t1, 5000, 10);
  ASSERT_EQ(avg_time_counter.get_data_avg().size(), 1);
  ASSERT_EQ(avg_time_counter.get_latest_data_avg().s, 5000);
  ASSERT_EQ(avg_time_counter.get_latest_data_avg().c, 10);
}

TEST(PerfCounterInstance, ZeroValues) {
  PerfCounterInstance counter(PERFCOUNTER_U64);
  utime_t t(0, 0);

  counter.push(t, 0);

  ASSERT_EQ(counter.get_data().size(), 1);
  ASSERT_EQ(counter.get_latest_data().v, 0);
  ASSERT_EQ(counter.get_latest_data().t.sec(), 0);
}

TEST(PerfCounterInstance, LargeValues) {
  PerfCounterInstance counter(PERFCOUNTER_U64);
  utime_t t(1000000, 999999);
  uint64_t large_val = UINT64_MAX;

  counter.push(t, large_val);

  ASSERT_EQ(counter.get_data().size(), 1);
  ASSERT_EQ(counter.get_latest_data().v, large_val);
}

TEST(PerfCounterInstance, AvgZeroCount) {
  PerfCounterInstance counter(PERFCOUNTER_LONGRUNAVG);
  utime_t t(100, 0);

  counter.push_avg(t, 1000, 0);

  ASSERT_EQ(counter.get_data_avg().size(), 1);
  ASSERT_EQ(counter.get_latest_data_avg().s, 1000);
  ASSERT_EQ(counter.get_latest_data_avg().c, 0);
}

TEST(PerfCounterInstance, TimeOrdering) {
  PerfCounterInstance counter(PERFCOUNTER_U64);

  for (int i = 0; i < 5; i++) {
    utime_t t(100 + i * 10, i * 1000);
    counter.push(t, i);
  }

  const auto& data = counter.get_data();
  ASSERT_EQ(data.size(), 5);

  utime_t prev_t(0, 0);
  for (const auto& point : data) {
    ASSERT_TRUE(point.t > prev_t);
    prev_t = point.t;
  }
}
