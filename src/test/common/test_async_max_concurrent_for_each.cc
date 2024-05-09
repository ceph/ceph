// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "common/async/max_concurrent_for_each.h"

#include <chrono>
#include <exception>
#include <optional>
#include <boost/asio/spawn.hpp>
#include <boost/asio/steady_timer.hpp>
#include <gtest/gtest.h>

namespace ceph::async {

namespace asio = boost::asio;

void rethrow(std::exception_ptr eptr)
{
  if (eptr) std::rethrow_exception(eptr);
}

using namespace std::chrono_literals;

void wait_for(std::chrono::milliseconds dur, asio::yield_context yield)
{
  auto timer = asio::steady_timer{yield.get_executor(), dur};
  timer.async_wait(yield);
}

struct null_sentinel {};
bool operator==(const char* c, null_sentinel) { return !*c; }
static_assert(std::sentinel_for<null_sentinel, const char*>);

TEST(iterator_null_yield, empty)
{
  int* end = nullptr;
  auto cr = [] (int, asio::yield_context) {};
  max_concurrent_for_each(end, end, 10, null_yield, cr);
}

TEST(iterator_null_yield, over_limit)
{
  int concurrent = 0;
  int max_concurrent = 0;
  int completed = 0;

  auto cr = [&] (int, asio::yield_context yield) {
    ++concurrent;
    if (max_concurrent < concurrent) {
      max_concurrent = concurrent;
    }

    wait_for(1ms, yield);

    --concurrent;
    ++completed;
  };

  constexpr auto arr = std::array{1,2,3,4,5,6,7,8,9,10};
  max_concurrent_for_each(begin(arr), end(arr), 2, null_yield, cr);

  EXPECT_EQ(0, concurrent);
  EXPECT_EQ(2, max_concurrent);
  EXPECT_EQ(10, completed);
}

TEST(iterator_null_yield, sentinel)
{
  const char* begin = "hello";
  null_sentinel end;

  size_t completed = 0;
  auto cr = [&completed] (char c, asio::yield_context) { ++completed; };
  max_concurrent_for_each(begin, end, 10, null_yield, cr);
  EXPECT_EQ(completed, 5);
}

TEST(range_null_yield, empty)
{
  constexpr std::array<int, 0> arr{};
  auto cr = [] (int, asio::yield_context) {};
  max_concurrent_for_each(arr, 10, null_yield, cr);
}

TEST(range_null_yield, over_limit)
{
  int concurrent = 0;
  int max_concurrent = 0;
  int completed = 0;

  auto cr = [&] (int, asio::yield_context yield) {
    ++concurrent;
    if (max_concurrent < concurrent) {
      max_concurrent = concurrent;
    }

    wait_for(1ms, yield);

    --concurrent;
    ++completed;
  };

  constexpr auto arr = std::array{1,2,3,4,5,6,7,8,9,10};
  max_concurrent_for_each(arr, 2, null_yield, cr);

  EXPECT_EQ(0, concurrent);
  EXPECT_EQ(2, max_concurrent);
  EXPECT_EQ(10, completed);
}


TEST(iterator_yield, empty)
{
  int* end = nullptr;
  auto cr = [] (int, asio::yield_context) {};

  asio::io_context ctx;
  asio::spawn(ctx, [&] (asio::yield_context yield) {
        max_concurrent_for_each(end, end, 10, yield, cr);
      }, rethrow);
  ctx.run();
}

TEST(iterator_yield, over_limit)
{
  int concurrent = 0;
  int max_concurrent = 0;
  int completed = 0;

  auto cr = [&] (int, asio::yield_context yield) {
    ++concurrent;
    if (max_concurrent < concurrent) {
      max_concurrent = concurrent;
    }

    wait_for(1ms, yield);

    --concurrent;
    ++completed;
  };

  asio::io_context ctx;
  asio::spawn(ctx, [&] (asio::yield_context yield) {
        constexpr auto arr = std::array{1,2,3,4,5,6,7,8,9,10};
        max_concurrent_for_each(begin(arr), end(arr), 2, yield, cr);
      }, rethrow);
  ctx.run();

  EXPECT_EQ(0, concurrent);
  EXPECT_EQ(2, max_concurrent);
  EXPECT_EQ(10, completed);
}

TEST(iterator_yield, sentinel)
{
  const char* begin = "hello";
  null_sentinel end;

  size_t completed = 0;
  auto cr = [&completed] (char c, asio::yield_context) { ++completed; };

  asio::io_context ctx;
  asio::spawn(ctx, [&] (asio::yield_context yield) {
        max_concurrent_for_each(begin, end, 10, yield, cr);
      }, rethrow);
  ctx.run();

  EXPECT_EQ(completed, 5);
}

TEST(range_yield, empty)
{
  constexpr std::array<int, 0> arr{};
  auto cr = [] (int, asio::yield_context) {};

  asio::io_context ctx;
  asio::spawn(ctx, [&] (asio::yield_context yield) {
  max_concurrent_for_each(arr, 10, yield, cr);
      }, rethrow);
  ctx.run();
}

TEST(range_yield, over_limit)
{
  int concurrent = 0;
  int max_concurrent = 0;
  int completed = 0;

  auto cr = [&] (int, asio::yield_context yield) {
    ++concurrent;
    if (max_concurrent < concurrent) {
      max_concurrent = concurrent;
    }

    wait_for(1ms, yield);

    --concurrent;
    ++completed;
  };

  asio::io_context ctx;
  asio::spawn(ctx, [&] (asio::yield_context yield) {
        constexpr auto arr = std::array{1,2,3,4,5,6,7,8,9,10};
        max_concurrent_for_each(arr, 2, yield, cr);
      }, rethrow);
  ctx.run();

  EXPECT_EQ(0, concurrent);
  EXPECT_EQ(2, max_concurrent);
  EXPECT_EQ(10, completed);
}

} // namespace ceph::async
