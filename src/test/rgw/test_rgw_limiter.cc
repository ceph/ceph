// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 CLYSO
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <memory>
#include <string>

#include "common/ceph_context.h"
#include "rgw_limiter.h"

namespace rgw::limiter {

using namespace std::chrono_literals;
using Sample = ConcurrencyLimiter::Sample;

static Sample ok(int64_t inflight, std::chrono::nanoseconds rtt = 1ms) {
  return Sample{rtt, inflight, /*dropped=*/false};
}
static Sample dropped(int64_t inflight, std::chrono::nanoseconds rtt = 100ms) {
  return Sample{rtt, inflight, /*dropped=*/true};
}

class ConcurrencyLimiterTest
    : public ::testing::TestWithParam<std::string_view> {
 protected:
  const std::unique_ptr<CephContext> cct;
  const std::unique_ptr<ConcurrencyLimiter> limiter;
  ConcurrencyLimiterTest()
      : cct(new CephContext(CEPH_ENTITY_TYPE_ANY)),
        limiter(create_by_name(cct.get(), GetParam())) {}
};

TEST_P(ConcurrencyLimiterTest, StartsWithPositiveLimit) {
  EXPECT_GT(limiter->limit(), 0);
}

TEST_P(ConcurrencyLimiterTest, LimitIsStableWithoutFeedback) {
  const int64_t first = limiter->limit();
  EXPECT_EQ(first, limiter->limit());
  EXPECT_EQ(first, limiter->limit());
}

TEST_P(ConcurrencyLimiterTest, StaysPositiveUnderHealthyLoad) {
  for (int i = 0; i < 100; i++) {
    limiter->sample(ok(limiter->limit()));
    EXPECT_GT(limiter->limit(), 0);
  }
}

TEST_P(ConcurrencyLimiterTest, NeverCollapsesToZero) {
  for (int i = 0; i < 100; i++) {
    limiter->sample(dropped(limiter->limit()));
    EXPECT_GT(limiter->limit(), 0) << "limit collapsed to " << limiter->limit()
                                   << " after " << i << " drops";
  }
}

TEST_P(ConcurrencyLimiterTest, ToleratesDegenerateSamples) {
  limiter->sample(Sample{std::chrono::nanoseconds{0}, 0, false});
  EXPECT_GT(limiter->limit(), 0);
  limiter->sample(Sample{1h, 0, false});
  EXPECT_GT(limiter->limit(), 0);
  limiter->sample(Sample{1h, 1'000'000, true});
  EXPECT_GT(limiter->limit(), 0);
}

TEST_P(ConcurrencyLimiterTest, LimitStaysBounded) {
  constexpr int64_t sane_ceiling = 1'000'000;
  for (int i = 0; i < 1000; i++) {
    limiter->sample(ok(limiter->limit()));
    ASSERT_GT(limiter->limit(), 0);
    ASSERT_LT(limiter->limit(), sane_ceiling);
  }
}

// Basisc Gradient2 behaviour: Contract on latency
class Gradient2Test : public ::testing::Test {
 protected:
  const std::unique_ptr<CephContext> cct;
  Gradient2 limiter;
  Gradient2Test()
      : cct(new CephContext(CEPH_ENTITY_TYPE_ANY)), limiter(cct.get()) {}

  void feed(std::chrono::nanoseconds rtt, int count) {
    for (int i = 0; i < count; i++) {
      limiter.sample(ok(limiter.limit(), rtt));
    }
  }
};

TEST_F(Gradient2Test, ContractsWhenLatencyDegrades) {
  feed(1ms, 200);
  const int64_t settled = limiter.limit();

  feed(50ms, 200);
  EXPECT_LT(limiter.limit(), settled)
      << "limit stayed at " << limiter.limit() << " after a 50x latency jump";
}

TEST_F(Gradient2Test, AccumulatesFractionalGrowth) {
  const int64_t initial = limiter.limit();

  feed(1ms, 2);
  EXPECT_GT(limiter.limit(), initial)
      << "fractional growth was discarded between samples";
}

INSTANTIATE_TEST_SUITE_P(Implementations, ConcurrencyLimiterTest,
    ::testing::Values("static", "gradient2"),
    [](const ::testing::TestParamInfo<std::string_view>& info) {
      return std::string(info.param);
    });

}  // namespace rgw::limiter
