// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "mgr/PerfCounterInstance.h"
#include "gtest/gtest.h"

namespace {
utime_t tp(double s) {
  utime_t t;
  t.set_from_double(s);
  return t;
}
}

TEST(PerfCounterInstance, CurrentAvgEmptyBuffer)
{
  PerfCounterInstance inst{PERFCOUNTER_LONGRUNAVG};
  EXPECT_FALSE(inst.get_current_avg().has_value());
}

TEST(PerfCounterInstance, CurrentAvgSingleSampleFallsBackToCumulative)
{
  // Right after a mgr restart, only the first report is buffered. We
  // fall back to the lifetime cumulative average so the command has
  // something to show instead of a dash.
  PerfCounterInstance inst{PERFCOUNTER_LONGRUNAVG};
  inst.push_avg(tp(1.0), /*sum_ns=*/1'000'000, /*count=*/10);
  auto avg = inst.get_current_avg();
  ASSERT_TRUE(avg.has_value());
  EXPECT_EQ(100'000ULL, *avg); // 1ms / 10 ops = 100us per op
}

TEST(PerfCounterInstance, CurrentAvgSingleSampleZeroCount)
{
  // Lifetime count is zero - no ops ever observed, no meaningful avg.
  PerfCounterInstance inst{PERFCOUNTER_LONGRUNAVG};
  inst.push_avg(tp(1.0), 0, 0);
  EXPECT_FALSE(inst.get_current_avg().has_value());
}

TEST(PerfCounterInstance, CurrentAvgDelta)
{
  PerfCounterInstance inst{PERFCOUNTER_LONGRUNAVG};
  // First observation: 1 ms total across 10 ops.
  inst.push_avg(tp(1.0), 1'000'000, 10);
  // Second observation: 10 new ops took 20 ms total (2 ms each).
  inst.push_avg(tp(2.0), 1'000'000 + 20'000'000, 20);
  auto avg = inst.get_current_avg();
  ASSERT_TRUE(avg.has_value());
  EXPECT_EQ(2'000'000ULL, *avg);
}

TEST(PerfCounterInstance, CurrentAvgNoNewOpsReturnsNullopt)
{
  PerfCounterInstance inst{PERFCOUNTER_LONGRUNAVG};
  // All buffered samples share the same count - no recent activity.
  // The lifetime cumulative would mix in ops from arbitrarily long ago,
  // so the helper returns nullopt and the caller renders a dash.
  inst.push_avg(tp(1.0), 21'000'000, 7);
  inst.push_avg(tp(2.0), 21'000'000, 7);
  inst.push_avg(tp(3.0), 21'000'000, 7);
  EXPECT_FALSE(inst.get_current_avg().has_value());
}

TEST(PerfCounterInstance, CurrentAvgWalksBackPastIdleSamples)
{
  PerfCounterInstance inst{PERFCOUNTER_LONGRUNAVG};
  // Activity happened a few intervals ago; most recent samples are idle.
  // The helper should still report the recent-activity delta rather
  // than the lifetime cumulative, because the cluster just stopped
  // doing IO moments ago.
  inst.push_avg(tp(1.0), 0, 0);
  inst.push_avg(tp(2.0), 10'000'000, 5);       // 2 ms/op during this burst
  inst.push_avg(tp(3.0), 10'000'000, 5);       // idle
  inst.push_avg(tp(4.0), 10'000'000, 5);       // idle
  auto avg = inst.get_current_avg();
  ASSERT_TRUE(avg.has_value());
  EXPECT_EQ(2'000'000ULL, *avg);
}

TEST(PerfCounterInstance, CurrentAvgOnlyUsesLastTwo)
{
  PerfCounterInstance inst{PERFCOUNTER_LONGRUNAVG};
  // Older sample should be ignored once a newer pair is available.
  inst.push_avg(tp(1.0), 0, 0);
  inst.push_avg(tp(2.0), 1'000'000'000, 100); // would yield 10 ms/op
  inst.push_avg(tp(3.0), 1'000'000'000 + 5'000'000, 105); // 1 ms/op since prev
  auto avg = inst.get_current_avg();
  ASSERT_TRUE(avg.has_value());
  EXPECT_EQ(1'000'000ULL, *avg);
}

TEST(PerfCounterInstance, CurrentAvgRespectsMaxAge)
{
  PerfCounterInstance inst{PERFCOUNTER_LONGRUNAVG};
  // A burst of activity 30s ago, then idle. Without max_age the helper
  // would fold the old burst's delta into the displayed value.
  inst.push_avg(tp(0.0), 0, 0);
  inst.push_avg(tp(1.0), 10'000'000, 5);   // 2 ms/op during the burst
  inst.push_avg(tp(31.0), 10'000'000, 5);  // idle 30s later
  // 5s window: prev sample at tp(1.0) is too old, walk-back stops.
  // Buffer has >1 sample, so no lifetime fallback - return nullopt.
  EXPECT_FALSE(inst.get_current_avg(utime_t(5, 0)).has_value());
  // 60s window covers both samples - delta from the burst is reported.
  auto avg = inst.get_current_avg(utime_t(60, 0));
  ASSERT_TRUE(avg.has_value());
  EXPECT_EQ(2'000'000ULL, *avg);
}
