// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 Red Hat <contact@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/async/co_throttle.h"
#include <chrono>
#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/asio/io_context.hpp>
#include <gtest/gtest.h>

namespace ceph::async {

namespace errc = boost::system::errc;
using boost::system::error_code;

using executor_type = boost::asio::io_context::executor_type;

template <typename T>
using awaitable = boost::asio::awaitable<T, executor_type>;
using use_awaitable_t = boost::asio::use_awaitable_t<executor_type>;
static constexpr use_awaitable_t use_awaitable{};

using clock_type = std::chrono::steady_clock;
using timer_type = boost::asio::basic_waitable_timer<clock_type,
     boost::asio::wait_traits<clock_type>, executor_type>;

void rethrow(std::exception_ptr eptr)
{
  if (eptr) std::rethrow_exception(eptr);
}

using namespace std::chrono_literals;

auto worker(std::chrono::milliseconds delay = 20ms)
    -> awaitable<void>
{
  auto timer = timer_type{co_await boost::asio::this_coro::executor, delay};
  co_await timer.async_wait(use_awaitable);
}

auto worker(error_code ec, std::chrono::milliseconds delay = 10ms)
    -> awaitable<error_code>
{
  co_await worker(delay);
  co_return ec;
}

auto worker(std::exception_ptr eptr, std::chrono::milliseconds delay = 10ms)
    -> awaitable<void>
{
  co_await worker(delay);
  std::rethrow_exception(eptr);
}

auto worker(bool& finished, std::chrono::milliseconds delay = 20ms)
    -> awaitable<void>
{
  co_await worker(delay);
  finished = true;
}

auto counting_worker(size_t& count, size_t& max_count)
    -> awaitable<void>
{
  ++count;
  if (max_count < count) {
    max_count = count;
  }
  co_await worker();
  --count;
}

// use a worker that never completes to test cancellation
awaitable<void> lazy_worker()
{
  for (;;) {
    co_await worker();
  }
}

TEST(co_throttle, wait_empty)
{
  constexpr size_t limit = 1;

  boost::asio::io_context ctx;
  auto ex = ctx.get_executor();
  auto throttle = co_throttle{ex, limit};

  std::optional<error_code> ec_wait;

  boost::asio::co_spawn(ex,
      [&] () -> awaitable<void> {
        ec_wait = co_await throttle.wait();
      }, rethrow);

  ctx.poll();
  ASSERT_TRUE(ctx.stopped()); // poll runs to completion

  ASSERT_TRUE(ec_wait); // wait returns immediately if nothing was spawned
  EXPECT_FALSE(*ec_wait);
}

TEST(co_throttle, spawn_over_limit)
{
  constexpr size_t limit = 1;

  size_t count = 0;
  size_t max_count = 0;

  boost::asio::io_context ctx;
  auto ex = ctx.get_executor();
  auto throttle = co_throttle{ex, limit};

  std::optional<error_code> ec_spawn1;
  std::optional<error_code> ec_spawn2;
  std::optional<error_code> ec_wait;

  boost::asio::co_spawn(ex,
      [&] () -> awaitable<void> {
        ec_spawn1 = co_await throttle.spawn(counting_worker(count, max_count));
        ec_spawn2 = co_await throttle.spawn(counting_worker(count, max_count));
        ec_wait = co_await throttle.wait();
      }, rethrow);

  ctx.poll(); // run until spawn2 blocks

  ASSERT_TRUE(ec_spawn1);
  EXPECT_FALSE(*ec_spawn1);
  EXPECT_FALSE(ec_spawn2);

  ctx.run_one(); // wait for spawn1's completion
  ctx.poll(); // run until wait blocks

  ASSERT_TRUE(ec_spawn2);
  EXPECT_FALSE(*ec_spawn2);
  EXPECT_FALSE(ec_wait);

  ctx.run(); // run to completion

  ASSERT_TRUE(ec_wait);
  EXPECT_FALSE(*ec_wait);

  EXPECT_EQ(max_count, limit); // count never exceeds limit
  EXPECT_EQ(count, 0);
}

TEST(co_throttle, spawn_over_smaller_limit)
{
  constexpr size_t limit = 2;
  constexpr size_t smaller_limit = 1;

  size_t count = 0;
  size_t max_count = 0;

  boost::asio::io_context ctx;
  auto ex = ctx.get_executor();
  auto throttle = co_throttle{ex, limit};

  std::optional<error_code> ec_spawn1;
  std::optional<error_code> ec_spawn2;
  std::optional<error_code> ec_wait;

  boost::asio::co_spawn(ex,
      [&] () -> awaitable<void> {
        ec_spawn1 = co_await throttle.spawn(counting_worker(count, max_count));
        ec_spawn2 = co_await throttle.spawn(counting_worker(count, max_count),
                                            smaller_limit);
        ec_wait = co_await throttle.wait();
      }, rethrow);

  ctx.poll(); // run until spawn2 blocks

  ASSERT_TRUE(ec_spawn1);
  EXPECT_FALSE(*ec_spawn1);
  EXPECT_FALSE(ec_spawn2);

  ctx.run_one(); // wait for spawn1's completion
  ctx.poll(); // run until wait blocks

  ASSERT_TRUE(ec_spawn2);
  EXPECT_FALSE(*ec_spawn2);
  EXPECT_FALSE(ec_wait);

  ctx.run(); // run to completion

  ASSERT_TRUE(ec_wait);
  EXPECT_FALSE(*ec_wait);

  EXPECT_EQ(max_count, smaller_limit); // count never exceeds smaller_limit
  EXPECT_EQ(count, 0);
}

TEST(co_throttle, spawn_cancel)
{
  constexpr size_t limit = 1;

  boost::asio::io_context ctx;
  auto ex = ctx.get_executor();
  auto throttle = co_throttle{ex, limit};

  std::optional<error_code> ec_spawn1;
  std::optional<error_code> ec_spawn2;
  std::optional<error_code> ec_wait;

  boost::asio::co_spawn(ex,
      [&] () -> awaitable<void> {
        ec_spawn1 = co_await throttle.spawn(lazy_worker());
        ec_spawn2 = co_await throttle.spawn(lazy_worker());
        ec_wait = co_await throttle.wait();
      }, rethrow);

  ctx.poll(); // run until spawn2 blocks

  ASSERT_TRUE(ec_spawn1);
  EXPECT_FALSE(*ec_spawn1);
  EXPECT_FALSE(ec_spawn2);
  EXPECT_FALSE(ec_wait);

  throttle.cancel();

  ctx.poll();
  ASSERT_TRUE(ctx.stopped()); // poll runs to completion

  ASSERT_TRUE(ec_spawn2);
  EXPECT_EQ(*ec_spawn2, boost::asio::error::operation_aborted);
  ASSERT_TRUE(ec_wait);
  EXPECT_FALSE(*ec_wait); // wait after cancel succeeds immediately
}

TEST(co_throttle, wait_cancel)
{
  constexpr size_t limit = 1;

  boost::asio::io_context ctx;
  auto ex = ctx.get_executor();
  auto throttle = co_throttle{ex, limit};

  std::optional<error_code> ec_spawn;
  std::optional<error_code> ec_wait;

  boost::asio::co_spawn(ex,
      [&] () -> awaitable<void> {
        ec_spawn = co_await throttle.spawn(lazy_worker());
        ec_wait = co_await throttle.wait();
      }, rethrow);

  ctx.poll(); // run until wait blocks

  ASSERT_TRUE(ec_spawn);
  EXPECT_FALSE(*ec_spawn);
  EXPECT_FALSE(ec_wait);

  throttle.cancel();

  ctx.poll();
  ASSERT_TRUE(ctx.stopped()); // poll runs to completion

  ASSERT_TRUE(ec_wait);
  EXPECT_EQ(*ec_wait, boost::asio::error::operation_aborted);
}

TEST(co_throttle, spawn_shutdown)
{
  constexpr size_t limit = 1;

  boost::asio::io_context ctx;
  auto ex = ctx.get_executor();

  std::optional<error_code> ec_spawn1;
  std::optional<error_code> ec_spawn2;

  boost::asio::co_spawn(ex,
      [&] () -> awaitable<void> {
        auto throttle = co_throttle{ex, limit};
        ec_spawn1 = co_await throttle.spawn(lazy_worker());
        ec_spawn2 = co_await throttle.spawn(lazy_worker());
      }, rethrow);

  ctx.run_one(); // call spawn1 and spawn2

  ASSERT_TRUE(ec_spawn1);
  EXPECT_FALSE(*ec_spawn1);
  EXPECT_FALSE(ec_spawn2);

  // shut down io_context before spawn2 unblocks
}

TEST(co_throttle, wait_shutdown)
{
  constexpr size_t limit = 1;

  boost::asio::io_context ctx;
  auto ex = ctx.get_executor();

  std::optional<error_code> ec_spawn;
  std::optional<error_code> ec_wait;

  boost::asio::co_spawn(ex,
      [&] () -> awaitable<void> {
        auto throttle = co_throttle{ex, limit};
        ec_spawn = co_await throttle.spawn(lazy_worker());
        ec_wait = co_await throttle.wait();
      }, rethrow);

  ctx.run_one(); // call spawn and wait

  ASSERT_TRUE(ec_spawn);
  EXPECT_FALSE(*ec_spawn);
  EXPECT_FALSE(ec_wait);

  // shut down io_context before wait unblocks
}

TEST(co_throttle, spawn_destroy)
{
  constexpr size_t limit = 1;

  boost::asio::io_context ctx;
  auto ex = ctx.get_executor();

  std::optional<error_code> ec_spawn1;
  std::optional<error_code> ec_spawn2;

  {
    auto throttle = co_throttle{ex, limit};

    boost::asio::co_spawn(ex,
        [&] () -> awaitable<void> {
          ec_spawn1 = co_await throttle.spawn(lazy_worker());
          ec_spawn2 = co_await throttle.spawn(lazy_worker());
        }, rethrow);

    ctx.poll(); // run until spawn2 blocks

    ASSERT_TRUE(ec_spawn1);
    EXPECT_FALSE(*ec_spawn1);
    EXPECT_FALSE(ec_spawn2);
    // throttle canceled/destroyed
  }

  ctx.poll();
  ASSERT_TRUE(ctx.stopped()); // poll runs to completion

  ASSERT_TRUE(ec_spawn2);
  EXPECT_EQ(*ec_spawn2, boost::asio::error::operation_aborted);
}

TEST(co_throttle, wait_destroy)
{
  constexpr size_t limit = 1;

  boost::asio::io_context ctx;
  auto ex = ctx.get_executor();

  std::optional<error_code> ec_spawn;
  std::optional<error_code> ec_wait;

  {
    auto throttle = co_throttle{ex, limit};

    boost::asio::co_spawn(ex,
        [&] () -> awaitable<void> {
          ec_spawn = co_await throttle.spawn(lazy_worker());
          ec_wait = co_await throttle.wait();
        }, rethrow);

    ctx.poll(); // run until wait blocks

    ASSERT_TRUE(ec_spawn);
    EXPECT_FALSE(*ec_spawn);
    EXPECT_FALSE(ec_wait);
    // throttle canceled/destroyed
  }

  ctx.poll();
  ASSERT_TRUE(ctx.stopped()); // poll runs to completion

  ASSERT_TRUE(ec_wait);
  EXPECT_EQ(*ec_wait, boost::asio::error::operation_aborted);
}

TEST(co_throttle, spawn_error)
{
  constexpr size_t limit = 2;

  boost::asio::io_context ctx;
  auto ex = ctx.get_executor();
  auto throttle = co_throttle{ex, limit};

  std::optional<error_code> ec_spawn1;
  std::optional<error_code> ec_spawn2;
  std::optional<error_code> ec_spawn3;
  std::optional<error_code> ec_wait;
  bool spawn1_finished = false;
  bool spawn3_finished = false;

  boost::asio::co_spawn(ex,
      [&] () -> awaitable<void> {
        ec_spawn1 = co_await throttle.spawn(worker(spawn1_finished));
        auto ec = make_error_code(errc::invalid_argument);
        ec_spawn2 = co_await throttle.spawn(worker(ec));
        ec_spawn3 = co_await throttle.spawn(worker(spawn3_finished));
        ec_wait = co_await throttle.wait();
      }, rethrow);

  ctx.poll(); // run until spawn3 blocks

  ASSERT_TRUE(ec_spawn1);
  EXPECT_FALSE(*ec_spawn1);
  ASSERT_TRUE(ec_spawn2);
  EXPECT_FALSE(*ec_spawn2);
  EXPECT_FALSE(ec_spawn3);
  EXPECT_FALSE(spawn1_finished);
  EXPECT_FALSE(spawn3_finished);

  ctx.run_one(); // wait for spawn2's completion
  ctx.poll(); // run until wait() blocks

  ASSERT_TRUE(ec_spawn3);
  EXPECT_EQ(*ec_spawn3, errc::invalid_argument);
  EXPECT_FALSE(ec_wait);

  ctx.run(); // run to completion

  EXPECT_TRUE(spawn3_finished); // spawn3 isn't canceled by spawn2's error
  ASSERT_TRUE(ec_wait);
  EXPECT_FALSE(*ec_wait);
}

TEST(co_throttle, wait_error)
{
  constexpr size_t limit = 1;

  boost::asio::io_context ctx;
  auto ex = ctx.get_executor();
  auto throttle = co_throttle{ex, limit};

  std::optional<error_code> ec_spawn;
  std::optional<error_code> ec_wait;

  boost::asio::co_spawn(ex,
      [&] () -> awaitable<void> {
        auto ec = make_error_code(errc::invalid_argument);
        ec_spawn = co_await throttle.spawn(worker(ec));
        ec_wait = co_await throttle.wait();
      }, rethrow);

  ctx.poll(); // run until wait blocks

  ASSERT_TRUE(ec_spawn);
  EXPECT_FALSE(*ec_spawn);
  EXPECT_FALSE(ec_wait);

  ctx.run(); // run to completion

  ASSERT_TRUE(ec_wait);
  EXPECT_EQ(*ec_wait, errc::invalid_argument);
}

TEST(co_throttle, spawn_cancel_on_error_after)
{
  constexpr size_t limit = 2;

  boost::asio::io_context ctx;
  auto ex = ctx.get_executor();
  auto throttle = co_throttle{ex, limit, cancel_on_error::after};

  std::optional<error_code> ec_spawn1;
  std::optional<error_code> ec_spawn2;
  std::optional<error_code> ec_spawn3;
  std::optional<error_code> ec_spawn4;
  std::optional<error_code> ec_wait;
  bool spawn1_finished = false;
  bool spawn3_finished = false;
  bool spawn4_finished = false;

  boost::asio::co_spawn(ex,
      [&] () -> awaitable<void> {
        ec_spawn1 = co_await throttle.spawn(worker(spawn1_finished));
        auto ec = make_error_code(errc::invalid_argument);
        ec_spawn2 = co_await throttle.spawn(worker(ec));
        // spawn3 expects invalid_argument error and cancellation
        ec_spawn3 = co_await throttle.spawn(worker(spawn3_finished));
        // spawn4 expects success
        ec_spawn4 = co_await throttle.spawn(worker(spawn4_finished));
        ec_wait = co_await throttle.wait();
      }, rethrow);

  ctx.poll(); // run until spawn3 blocks

  ASSERT_TRUE(ec_spawn1);
  EXPECT_FALSE(*ec_spawn1);
  ASSERT_TRUE(ec_spawn2);
  EXPECT_FALSE(*ec_spawn2);
  EXPECT_FALSE(spawn1_finished);

  ctx.run_one(); // wait for spawn2's completion
  ctx.poll();
  ASSERT_FALSE(ctx.stopped());

  ASSERT_TRUE(ec_spawn3);
  EXPECT_EQ(*ec_spawn3, errc::invalid_argument);
  ASSERT_TRUE(ec_spawn4);
  EXPECT_FALSE(*ec_spawn4);
  EXPECT_FALSE(spawn1_finished);

  ctx.run_one(); // wait for spawn1's completion
  ctx.poll(); // run until wait blocks

  EXPECT_FALSE(ec_wait);
  EXPECT_TRUE(spawn1_finished); // spawn1 not canceled

  ctx.run_one(); // wait for spawn4's completion
  ctx.poll();
  ASSERT_TRUE(ctx.stopped()); // poll runs to completion

  ASSERT_TRUE(ec_wait);
  EXPECT_FALSE(*ec_wait);
  EXPECT_FALSE(spawn3_finished); // spawn3 canceled
  EXPECT_TRUE(spawn4_finished); // spawn4 not canceled
}

TEST(co_throttle, spawn_cancel_on_error_all)
{
  constexpr size_t limit = 2;

  boost::asio::io_context ctx;
  auto ex = ctx.get_executor();
  auto throttle = co_throttle{ex, limit, cancel_on_error::all};

  std::optional<error_code> ec_spawn1;
  std::optional<error_code> ec_spawn2;
  std::optional<error_code> ec_spawn3;
  std::optional<error_code> ec_spawn4;
  std::optional<error_code> ec_wait;
  bool spawn1_finished = false;
  bool spawn3_finished = false;
  bool spawn4_finished = false;

  boost::asio::co_spawn(ex,
      [&] () -> awaitable<void> {
        ec_spawn1 = co_await throttle.spawn(worker(spawn1_finished));
        auto ec = make_error_code(errc::invalid_argument);
        ec_spawn2 = co_await throttle.spawn(worker(ec));
        // spawn3 expects invalid_argument error and cancellation
        ec_spawn3 = co_await throttle.spawn(worker(spawn3_finished));
        // spawn3 expects success
        ec_spawn4 = co_await throttle.spawn(worker(spawn4_finished));
        ec_wait = co_await throttle.wait();
      }, rethrow);

  ctx.poll(); // run until spawn3 blocks

  ASSERT_TRUE(ec_spawn1);
  EXPECT_FALSE(*ec_spawn1);
  ASSERT_TRUE(ec_spawn2);
  EXPECT_FALSE(*ec_spawn2);
  EXPECT_FALSE(ec_spawn3);
  EXPECT_FALSE(ec_spawn4);

  ctx.run_one(); // wait for spawn2's completion
  ctx.poll(); // run until wait blocks

  ASSERT_TRUE(ec_spawn3);
  EXPECT_EQ(*ec_spawn3, errc::invalid_argument);
  ASSERT_TRUE(ec_spawn4);
  EXPECT_FALSE(*ec_spawn4);
  EXPECT_FALSE(ec_wait);
  EXPECT_FALSE(spawn1_finished); // spawn1 canceled

  ctx.run_one(); // wait for spawn4's completion
  ctx.poll();
  ASSERT_TRUE(ctx.stopped()); // poll runs to completion

  ASSERT_TRUE(ec_wait);
  EXPECT_FALSE(*ec_wait);
  EXPECT_FALSE(spawn3_finished); // spawn3 canceled
  EXPECT_TRUE(spawn4_finished); // spawn4 not canceled
}

TEST(co_throttle, spawn_exception)
{
  constexpr size_t limit = 2;

  boost::asio::io_context ctx;
  auto ex = ctx.get_executor();
  auto throttle = co_throttle{ex, limit};

  std::optional<error_code> ec_spawn1;
  std::optional<error_code> ec_spawn2;
  std::optional<error_code> ec_spawn3;
  bool spawn1_finished = false;
  bool spawn3_finished = false;

  boost::asio::co_spawn(ex,
      [&] () -> awaitable<void> {
        ec_spawn1 = co_await throttle.spawn(worker(spawn1_finished));
        auto eptr = std::make_exception_ptr(std::runtime_error{"oops"});
        ec_spawn2 = co_await throttle.spawn(worker(eptr));
        ec_spawn3 = co_await throttle.spawn(worker(spawn3_finished));
      }, rethrow);

  ctx.poll(); // run until spawn3 blocks

  ASSERT_TRUE(ec_spawn1);
  EXPECT_FALSE(*ec_spawn1);
  ASSERT_TRUE(ec_spawn2);
  EXPECT_FALSE(*ec_spawn2);

  EXPECT_THROW(ctx.run_one(), std::runtime_error);

  ASSERT_FALSE(ec_spawn3);
  EXPECT_FALSE(spawn1_finished);
  EXPECT_FALSE(spawn3_finished);
}

TEST(co_throttle, wait_exception)
{
  constexpr size_t limit = 1;

  boost::asio::io_context ctx;
  auto ex = ctx.get_executor();
  auto throttle = co_throttle{ex, limit};

  std::optional<error_code> ec_spawn;
  std::optional<error_code> ec_wait;

  boost::asio::co_spawn(ex,
      [&] () -> awaitable<void> {
        auto eptr = std::make_exception_ptr(std::runtime_error{"oops"});
        ec_spawn = co_await throttle.spawn(worker(eptr));
        ec_wait = co_await throttle.wait();
      }, rethrow);

  ctx.poll(); // run until wait blocks

  ASSERT_TRUE(ec_spawn);
  EXPECT_FALSE(*ec_spawn);

  EXPECT_THROW(ctx.run(), std::runtime_error);

  ASSERT_FALSE(ec_wait);
}

} // namespace ceph::async
