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

#include "rgw_reshard.h"
#include <boost/asio/spawn.hpp>

#include <gtest/gtest.h>

using namespace std::chrono_literals;
using Clock = RGWReshardWait::Clock;

TEST(ReshardWait, wait_block)
{
  constexpr ceph::timespan wait_duration = 10ms;
  const auto dpp = NoDoutPrefix{g_ceph_context, ceph_subsys_rgw};
  RGWReshardWait waiter(wait_duration);

  const auto start = Clock::now();
  EXPECT_EQ(0, waiter.wait(&dpp, null_yield));
  const ceph::timespan elapsed = Clock::now() - start;

  EXPECT_LE(wait_duration, elapsed); // waited at least 10ms
  waiter.stop();
}

TEST(ReshardWait, stop_block)
{
  constexpr ceph::timespan short_duration = 10ms;
  constexpr ceph::timespan long_duration = 10s;
  const auto dpp = NoDoutPrefix{g_ceph_context, ceph_subsys_rgw};

  RGWReshardWait long_waiter(long_duration);
  RGWReshardWait short_waiter(short_duration);

  const auto start = Clock::now();
  std::thread thread([&dpp, &long_waiter] {
    EXPECT_EQ(-ECANCELED, long_waiter.wait(&dpp, null_yield));
  });

  EXPECT_EQ(0, short_waiter.wait(&dpp, null_yield));

  long_waiter.stop(); // cancel long waiter

  thread.join();
  const ceph::timespan elapsed = Clock::now() - start;

  EXPECT_LE(short_duration, elapsed); // waited at least 10ms
  EXPECT_GT(long_duration, elapsed); // waited less than 10s
  short_waiter.stop();
}

void rethrow(std::exception_ptr eptr) {
  if (eptr) std::rethrow_exception(eptr);
}

TEST(ReshardWait, wait_yield)
{
  constexpr ceph::timespan wait_duration = 50ms;
  const auto dpp = NoDoutPrefix{g_ceph_context, ceph_subsys_rgw};
  RGWReshardWait waiter(wait_duration);

  boost::asio::io_context context;
  boost::asio::spawn(context, [&] (boost::asio::yield_context yield) {
      EXPECT_EQ(0, waiter.wait(&dpp, yield));
    }, rethrow);

  const auto start = Clock::now();
  EXPECT_EQ(1u, context.poll()); // spawn
  EXPECT_FALSE(context.stopped());

  EXPECT_EQ(1u, context.run_one()); // timeout
  EXPECT_TRUE(context.stopped());
  const ceph::timespan elapsed = Clock::now() - start;

  EXPECT_LE(wait_duration, elapsed); // waited at least 10ms
  waiter.stop();
}

TEST(ReshardWait, stop_yield)
{
  constexpr ceph::timespan short_duration = 50ms;
  constexpr ceph::timespan long_duration = 10s;
  const auto dpp = NoDoutPrefix{g_ceph_context, ceph_subsys_rgw};

  RGWReshardWait long_waiter(long_duration);
  RGWReshardWait short_waiter(short_duration);

  boost::asio::io_context context;
  boost::asio::spawn(context,
    [&] (boost::asio::yield_context yield) {
      EXPECT_EQ(-ECANCELED, long_waiter.wait(&dpp, yield));
    }, rethrow);

  const auto start = Clock::now();
  EXPECT_EQ(1u, context.poll()); // spawn
  EXPECT_FALSE(context.stopped());

  EXPECT_EQ(0, short_waiter.wait(&dpp, null_yield));

  long_waiter.stop(); // cancel long waiter

  EXPECT_EQ(1u, context.run_one_for(short_duration)); // timeout
  EXPECT_TRUE(context.stopped());
  const ceph::timespan elapsed = Clock::now() - start;

  EXPECT_LE(short_duration, elapsed); // waited at least 10ms
  EXPECT_GT(long_duration, elapsed); // waited less than 10s
  short_waiter.stop();
}

TEST(ReshardWait, stop_multiple)
{
  constexpr ceph::timespan short_duration = 50ms;
  constexpr ceph::timespan long_duration = 10s;
  const auto dpp = NoDoutPrefix{g_ceph_context, ceph_subsys_rgw};

  RGWReshardWait long_waiter(long_duration);
  RGWReshardWait short_waiter(short_duration);

  // spawn 4 threads
  std::vector<std::thread> threads;
  {
    auto sync_waiter([&dpp, &long_waiter] {
      EXPECT_EQ(-ECANCELED, long_waiter.wait(&dpp, null_yield));
    });
    threads.emplace_back(sync_waiter);
    threads.emplace_back(sync_waiter);
    threads.emplace_back(sync_waiter);
    threads.emplace_back(sync_waiter);
  }
  // spawn 4 coroutines
  boost::asio::io_context context;
  {
    auto async_waiter = [&] (boost::asio::yield_context yield) {
        EXPECT_EQ(-ECANCELED, long_waiter.wait(&dpp, yield));
      };
    boost::asio::spawn(context, async_waiter, rethrow);
    boost::asio::spawn(context, async_waiter, rethrow);
    boost::asio::spawn(context, async_waiter, rethrow);
    boost::asio::spawn(context, async_waiter, rethrow);
  }

  const auto start = Clock::now();
  EXPECT_EQ(4u, context.poll()); // spawn
  EXPECT_FALSE(context.stopped());

  EXPECT_EQ(0, short_waiter.wait(&dpp, null_yield));

  long_waiter.stop(); // cancel long waiter

  EXPECT_EQ(4u, context.run_for(short_duration)); // timeout
  EXPECT_TRUE(context.stopped());

  for (auto& thread : threads) {
    thread.join();
  }
  const ceph::timespan elapsed = Clock::now() - start;

  EXPECT_LE(short_duration, elapsed); // waited at least 10ms
  EXPECT_GT(long_duration, elapsed); // waited less than 10s
  short_waiter.stop();
}
