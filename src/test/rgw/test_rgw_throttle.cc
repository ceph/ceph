// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw/rgw_aio_throttle.h"
#include "rgw/rgw_rados.h"

#include "include/rados/librados.hpp"

#include <gtest/gtest.h>

struct RadosEnv : public ::testing::Environment {
 public:
  static constexpr auto poolname = "ceph_test_rgw_throttle";

  static std::optional<RGWSI_RADOS> rados;

  void SetUp() override {
    rados.emplace(g_ceph_context);
    ASSERT_EQ(0, rados->start());
    int r = rados->pool({poolname}).create();
    if (r == -EEXIST)
      r = 0;
    ASSERT_EQ(0, r);
  }
  void TearDown() {
    rados.reset();
  }
};
std::optional<RGWSI_RADOS> RadosEnv::rados;

auto *const rados_env = ::testing::AddGlobalTestEnvironment(new RadosEnv);

// test fixture for global setup/teardown
class RadosFixture : public ::testing::Test {
 protected:
  RGWSI_RADOS::Obj make_obj(const std::string& oid) {
    auto obj = RadosEnv::rados->obj({{RadosEnv::poolname}, oid});
    ceph_assert_always(0 == obj.open());
    return obj;
  }
};

using Aio_Throttle = RadosFixture;

namespace rgw {

TEST_F(Aio_Throttle, NoThrottleUpToMax)
{
  AioThrottle throttle(4);
  auto obj = make_obj(__PRETTY_FUNCTION__);
  {
    librados::ObjectWriteOperation op1;
    auto c1 = throttle.submit(obj, &op1, 1, 0);
    EXPECT_TRUE(c1.empty());
    librados::ObjectWriteOperation op2;
    auto c2 = throttle.submit(obj, &op2, 1, 0);
    EXPECT_TRUE(c2.empty());
    librados::ObjectWriteOperation op3;
    auto c3 = throttle.submit(obj, &op3, 1, 0);
    EXPECT_TRUE(c3.empty());
    librados::ObjectWriteOperation op4;
    auto c4 = throttle.submit(obj, &op4, 1, 0);
    EXPECT_TRUE(c4.empty());
    // no completions because no ops had to wait
    auto c5 = throttle.poll();
  }
  auto completions = throttle.drain();
  ASSERT_EQ(4u, completions.size());
  for (auto& c : completions) {
    EXPECT_EQ(-EINVAL, c.result);
  }
}

TEST_F(Aio_Throttle, CostOverWindow)
{
  AioThrottle throttle(4);
  auto obj = make_obj(__PRETTY_FUNCTION__);

  librados::ObjectWriteOperation op;
  auto c = throttle.submit(obj, &op, 8, 0);
  ASSERT_EQ(1u, c.size());
  EXPECT_EQ(-EDEADLK, c.front().result);
}

TEST_F(Aio_Throttle, ThrottleOverMax)
{
  constexpr uint64_t window = 4;
  AioThrottle throttle(window);

  auto obj = make_obj(__PRETTY_FUNCTION__);

  // issue 32 writes, and verify that max_outstanding <= window
  constexpr uint64_t total = 32;
  uint64_t max_outstanding = 0;
  uint64_t outstanding = 0;

  for (uint64_t i = 0; i < total; i++) {
    librados::ObjectWriteOperation op;
    auto c = throttle.submit(obj, &op, 1, 0);
    outstanding++;
    outstanding -= c.size();
    if (max_outstanding < outstanding) {
      max_outstanding = outstanding;
    }
  }
  auto c = throttle.drain();
  outstanding -= c.size();
  EXPECT_EQ(0u, outstanding);
  EXPECT_EQ(window, max_outstanding);
}

} // namespace rgw
