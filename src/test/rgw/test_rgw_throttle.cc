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

#include "rgw_aio_throttle.h"

#include <optional>
#include <thread>
#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include "include/scope_guard.h"

#include <spawn/spawn.hpp>
#include <gtest/gtest.h>

static rgw_raw_obj make_obj(const std::string& oid)
{
  return {{"testpool"}, oid};
}

namespace rgw {

struct scoped_completion {
  Aio* aio = nullptr;
  AioResult* result = nullptr;
  ~scoped_completion() { if (aio) { complete(-ECANCELED); } }
  void complete(int r) {
    result->result = r;
    aio->put(*result);
    aio = nullptr;
  }
};

auto wait_on(scoped_completion& c) {
  return [&c] (Aio* aio, AioResult& r) { c.aio = aio; c.result = &r; };
}

auto wait_for(boost::asio::io_context& context, ceph::timespan duration) {
  return [&context, duration] (Aio* aio, AioResult& r) {
    using Clock = ceph::coarse_mono_clock;
    using Timer = boost::asio::basic_waitable_timer<Clock>;
    auto t = std::make_unique<Timer>(context);
    t->expires_after(duration);
    t->async_wait([aio, &r, t=std::move(t)] (boost::system::error_code ec) {
        if (ec != boost::asio::error::operation_aborted) {
          aio->put(r);
        }
      });
  };
}

TEST(Aio_Throttle, NoThrottleUpToMax)
{
  BlockingAioThrottle throttle(4);
  auto obj = make_obj(__PRETTY_FUNCTION__);
  {
    scoped_completion op1;
    auto c1 = throttle.get(obj, wait_on(op1), 1, 0);
    EXPECT_TRUE(c1.empty());
    scoped_completion op2;
    auto c2 = throttle.get(obj, wait_on(op2), 1, 0);
    EXPECT_TRUE(c2.empty());
    scoped_completion op3;
    auto c3 = throttle.get(obj, wait_on(op3), 1, 0);
    EXPECT_TRUE(c3.empty());
    scoped_completion op4;
    auto c4 = throttle.get(obj, wait_on(op4), 1, 0);
    EXPECT_TRUE(c4.empty());
    // no completions because no ops had to wait
    auto c5 = throttle.poll();
    EXPECT_TRUE(c5.empty());
  }
  auto completions = throttle.drain();
  ASSERT_EQ(4u, completions.size());
  for (auto& c : completions) {
    EXPECT_EQ(-ECANCELED, c.result);
  }
}

TEST(Aio_Throttle, CostOverWindow)
{
  BlockingAioThrottle throttle(4);
  auto obj = make_obj(__PRETTY_FUNCTION__);

  scoped_completion op;
  auto c = throttle.get(obj, wait_on(op), 8, 0);
  ASSERT_EQ(1u, c.size());
  EXPECT_EQ(-EDEADLK, c.front().result);
}

TEST(Aio_Throttle, ThrottleOverMax)
{
  constexpr uint64_t window = 4;
  BlockingAioThrottle throttle(window);

  auto obj = make_obj(__PRETTY_FUNCTION__);

  // issue 32 writes, and verify that max_outstanding <= window
  constexpr uint64_t total = 32;
  uint64_t max_outstanding = 0;
  uint64_t outstanding = 0;

  // timer thread
  boost::asio::io_context context;
  using Executor = boost::asio::io_context::executor_type;
  using Work = boost::asio::executor_work_guard<Executor>;
  std::optional<Work> work(context.get_executor());
  std::thread worker([&context] { context.run(); });
  auto g = make_scope_guard([&work, &worker] {
      work.reset();
      worker.join();
    });

  for (uint64_t i = 0; i < total; i++) {
    using namespace std::chrono_literals;
    auto c = throttle.get(obj, wait_for(context, 10ms), 1, 0);
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

TEST(Aio_Throttle, YieldCostOverWindow)
{
  auto obj = make_obj(__PRETTY_FUNCTION__);

  boost::asio::io_context context;
  spawn::spawn(context,
    [&] (spawn::yield_context yield) {
      YieldingAioThrottle throttle(4, context, yield);
      scoped_completion op;
      auto c = throttle.get(obj, wait_on(op), 8, 0);
      ASSERT_EQ(1u, c.size());
      EXPECT_EQ(-EDEADLK, c.front().result);
    });
  context.run();
}

TEST(Aio_Throttle, YieldingThrottleOverMax)
{
  constexpr uint64_t window = 4;

  auto obj = make_obj(__PRETTY_FUNCTION__);

  // issue 32 writes, and verify that max_outstanding <= window
  constexpr uint64_t total = 32;
  uint64_t max_outstanding = 0;
  uint64_t outstanding = 0;

  boost::asio::io_context context;
  spawn::spawn(context,
    [&] (spawn::yield_context yield) {
      YieldingAioThrottle throttle(window, context, yield);
      for (uint64_t i = 0; i < total; i++) {
        using namespace std::chrono_literals;
        auto c = throttle.get(obj, wait_for(context, 10ms), 1, 0);
        outstanding++;
        outstanding -= c.size();
        if (max_outstanding < outstanding) {
          max_outstanding = outstanding;
        }
      }
      auto c = throttle.drain();
      outstanding -= c.size();
    });
  context.poll(); // run until we block
  EXPECT_EQ(window, outstanding);

  context.run();
  EXPECT_EQ(0u, outstanding);
  EXPECT_EQ(window, max_outstanding);
}

} // namespace rgw
