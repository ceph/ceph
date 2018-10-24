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

#include "rgw/rgw_putobj_throttle.h"
#include "rgw/rgw_rados.h"

#include "include/rados/librados.hpp"

#include <gtest/gtest.h>

struct RadosEnv : public ::testing::Environment {
 public:
  static constexpr auto poolname = "ceph_test_rgw_throttle";

  static librados::Rados rados;
  static librados::IoCtx io;

  void SetUp() override {
    ASSERT_EQ(0, rados.init_with_context(g_ceph_context));
    ASSERT_EQ(0, rados.connect());
    // open/create test pool
    int r = rados.ioctx_create(poolname, io);
    if (r == -ENOENT) {
      r = rados.pool_create(poolname);
      if (r == -EEXIST) {
        r = 0;
      } else if (r == 0) {
        r = rados.ioctx_create(poolname, io);
      }
    }
    ASSERT_EQ(0, r);
  }
  void TearDown() {
    rados.shutdown();
  }
};
librados::Rados RadosEnv::rados;
librados::IoCtx RadosEnv::io;

auto *const rados_env = ::testing::AddGlobalTestEnvironment(new RadosEnv);

// test fixture for global setup/teardown
class RadosFixture : public ::testing::Test {
 protected:
  librados::IoCtx& io;

  rgw_raw_obj make_obj(const std::string& oid) {
    return {{RadosEnv::poolname}, oid};
  }
  rgw_rados_ref make_ref(const rgw_raw_obj& obj) {
    return {obj.pool, obj.oid, "", io};
  }
 public:
  RadosFixture() : io(RadosEnv::io) {}
};

using PutObj_Throttle = RadosFixture;

namespace rgw::putobj {

inline bool operator==(const Result& lhs, const Result& rhs) {
  return lhs.obj == rhs.obj && lhs.result == rhs.result;
}
std::ostream& operator<<(std::ostream& out, const Result& r) {
  return out << "{r=" << r.result << " obj='" << r.obj << "'";
}

TEST_F(PutObj_Throttle, NoThrottleUpToMax)
{
  AioThrottle throttle(4);
  auto obj = make_obj(__PRETTY_FUNCTION__);
  auto ref = make_ref(obj);
  {
    librados::ObjectWriteOperation op1;
    auto c1 = throttle.submit(ref, obj, &op1, 1);
    EXPECT_TRUE(c1.empty());
    librados::ObjectWriteOperation op2;
    auto c2 = throttle.submit(ref, obj, &op2, 1);
    EXPECT_TRUE(c2.empty());
    librados::ObjectWriteOperation op3;
    auto c3 = throttle.submit(ref, obj, &op3, 1);
    EXPECT_TRUE(c3.empty());
    librados::ObjectWriteOperation op4;
    auto c4 = throttle.submit(ref, obj, &op4, 1);
    EXPECT_TRUE(c4.empty());
    // no completions because no ops had to wait
    auto c5 = throttle.poll();
  }
  auto completions = throttle.drain();
  ASSERT_EQ(4u, completions.size());
  for (auto& c : completions) {
    EXPECT_EQ(Result({obj, -EINVAL}), c);
  }
}

TEST_F(PutObj_Throttle, CostOverWindow)
{
  AioThrottle throttle(4);
  auto obj = make_obj(__PRETTY_FUNCTION__);
  auto ref = make_ref(obj);

  librados::ObjectWriteOperation op;
  auto c = throttle.submit(ref, obj, &op, 8);
  ASSERT_EQ(1u, c.size());
  EXPECT_EQ(Result({obj, -EDEADLK}), c.front());
}

TEST_F(PutObj_Throttle, AioThrottleOverMax)
{
  constexpr uint64_t window = 4;
  AioThrottle throttle(window);

  auto obj = make_obj(__PRETTY_FUNCTION__);
  auto ref = make_ref(obj);

  // issue 32 writes, and verify that max_outstanding <= window
  constexpr uint64_t total = 32;
  uint64_t max_outstanding = 0;
  uint64_t outstanding = 0;

  for (uint64_t i = 0; i < total; i++) {
    librados::ObjectWriteOperation op;
    auto c = throttle.submit(ref, obj, &op, 1);
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

} // namespace rgw::putobj
