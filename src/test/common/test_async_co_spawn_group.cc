// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/async/co_spawn_group.h"

#include <latch>
#include <optional>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/defer.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/thread_pool.hpp>
#include <gtest/gtest.h>
#include "common/async/co_waiter.h"

namespace ceph::async {

namespace asio = boost::asio;
namespace errc = boost::system::errc;
using boost::system::error_code;

using executor_type = asio::any_io_executor;

template <typename T>
auto capture(std::optional<T>& opt)
{
  return [&opt] (T value) { opt = std::move(value); };
}

template <typename T>
auto capture(asio::cancellation_signal& signal, std::optional<T>& opt)
{
  return asio::bind_cancellation_slot(signal.slot(), capture(opt));
}

TEST(co_spawn_group, spawn_limit)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto group = co_spawn_group{ex, 1};

  auto cr = [] () -> asio::awaitable<void> { co_return; };

  group.spawn(cr());
  EXPECT_THROW(group.spawn(cr()), std::length_error);
}

TEST(co_spawn_group, wait_empty)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto group = co_spawn_group{ex, 1};

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ex, group.wait(), capture(result));

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
}

TEST(co_spawn_group, spawn_shutdown)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto group = co_spawn_group{ex, 10};

  co_waiter<void, executor_type> waiter;
  group.spawn(waiter.get());

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  // shut down before wait()
}

TEST(co_spawn_group, spawn_wait)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto group = co_spawn_group{ex, 10};

  co_waiter<void, executor_type> waiter;
  group.spawn(waiter.get());

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ex, group.wait(), capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  waiter.complete(nullptr);

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
}

TEST(co_spawn_group, spawn_wait_shutdown)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();

  co_waiter<void, executor_type> waiter;
  auto cr = [ex, &waiter] () -> asio::awaitable<void> {
    auto group = co_spawn_group{ex, 1};
    group.spawn(waiter.get());
    co_await group.wait();
  };

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ex, cr(), capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  // shut down before wait() completes
}

TEST(co_spawn_group, spawn_wait_cancel)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();

  co_waiter<void, executor_type> waiter;
  auto cr = [ex, &waiter] () -> asio::awaitable<void> {
    auto group = co_spawn_group{ex, 1};
    group.spawn(waiter.get());
    co_await group.wait();
  };

  asio::cancellation_signal signal;
  std::optional<std::exception_ptr> result;
  asio::co_spawn(ex, cr(), capture(signal, result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  // cancel before wait() completes
  signal.emit(asio::cancellation_type::terminal);

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), asio::error::operation_aborted);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

TEST(co_spawn_group, spawn_wait_exception_order)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto group = co_spawn_group{ex, 2};

  co_waiter<void, executor_type> waiter1;
  group.spawn(waiter1.get());

  co_waiter<void, executor_type> waiter2;
  group.spawn(waiter2.get());

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ex, group.wait(), capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  waiter2.complete(std::make_exception_ptr(std::runtime_error{"oops"}));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  waiter1.complete(std::make_exception_ptr(std::logic_error{"oops"}));

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  EXPECT_THROW(std::rethrow_exception(*result), std::runtime_error);
}

TEST(co_spawn_group, spawn_complete_wait)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto group = co_spawn_group{ex, 2};

  co_waiter<void, executor_type> waiter;
  group.spawn(waiter.get());

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());

  waiter.complete(std::make_exception_ptr(std::runtime_error{"oops"}));

  ctx.poll();
  ASSERT_TRUE(ctx.stopped()); // no waiter means ctx can stop
  ctx.restart();

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ex, group.wait(), capture(result));

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  EXPECT_THROW(std::rethrow_exception(*result), std::runtime_error);
}

TEST(co_spawn_group, spawn_wait_wait)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto group = co_spawn_group{ex, 1};

  co_waiter<void, executor_type> waiter;
  group.spawn(waiter.get());

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ex, group.wait(), capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());

  waiter.complete(std::make_exception_ptr(std::runtime_error{"oops"}));

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  EXPECT_THROW(std::rethrow_exception(*result), std::runtime_error);

  result.reset();
  asio::co_spawn(ex, group.wait(), capture(result));

  ctx.restart();
  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
}

TEST(co_spawn_group, spawn_wait_spawn_wait)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto group = co_spawn_group{ex, 1};

  co_waiter<void, executor_type> waiter;
  group.spawn(waiter.get());

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ex, group.wait(), capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  waiter.complete(nullptr);

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_FALSE(*result);

  group.spawn(waiter.get());

  result.reset();
  asio::co_spawn(ex, group.wait(), capture(result));

  ctx.restart();
  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  waiter.complete(nullptr);

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
}

TEST(co_spawn_group, spawn_cancel_wait_spawn_wait)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto group = co_spawn_group{ex, 1};

  co_waiter<void, executor_type> waiter;
  group.spawn(waiter.get());

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());

  group.cancel();

  ctx.poll();
  ASSERT_TRUE(ctx.stopped()); // no waiter means ctx can stop
  ctx.restart();

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ex, group.wait(), capture(result));

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), asio::error::operation_aborted);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }

  group.spawn(waiter.get());

  result.reset();
  asio::co_spawn(ex, group.wait(), capture(result));

  ctx.restart();
  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  waiter.complete(nullptr);

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
}

TEST(co_spawn_group, spawn_wait_cancel_spawn_wait)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto group = co_spawn_group{ex, 1};

  co_waiter<void, executor_type> waiter;
  group.spawn(waiter.get());

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ex, group.wait(), capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  // cancel before waiter completes
  group.cancel();

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), asio::error::operation_aborted);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }

  group.spawn(waiter.get());

  result.reset();
  asio::co_spawn(ex, group.wait(), capture(result));

  ctx.restart();
  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  waiter.complete(nullptr);

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
}

TEST(co_spawn_group, cancel_on_error_after)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto group = co_spawn_group{ex, 3, cancel_on_error::after};

  co_waiter<void, executor_type> waiter1;
  group.spawn(waiter1.get());

  co_waiter<void, executor_type> waiter2;
  group.spawn(waiter2.get());

  co_waiter<void, executor_type> waiter3;
  group.spawn(waiter3.get());

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ex, group.wait(), capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  waiter2.complete(std::make_exception_ptr(std::runtime_error{"oops"}));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  waiter1.complete(nullptr);

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  EXPECT_THROW(std::rethrow_exception(*result), std::runtime_error);
}

TEST(co_spawn_group, cancel_on_error_all)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto group = co_spawn_group{ex, 3, cancel_on_error::all};

  co_waiter<void, executor_type> waiter1;
  group.spawn(waiter1.get());

  co_waiter<void, executor_type> waiter2;
  group.spawn(waiter2.get());

  co_waiter<void, executor_type> waiter3;
  group.spawn(waiter3.get());

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ex, group.wait(), capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  waiter2.complete(std::make_exception_ptr(std::runtime_error{"oops"}));

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  EXPECT_THROW(std::rethrow_exception(*result), std::runtime_error);
}

TEST(co_spawn_group, cross_thread_cancel)
{
  // run the coroutine in a background thread
  asio::thread_pool ctx{1};
  executor_type ex = ctx.get_executor();

  std::latch waiting{1};

  auto cr = [ex, &waiting] () -> asio::awaitable<void> {
    auto group = co_spawn_group{ex, 1};
    co_waiter<void, executor_type> waiter;
    group.spawn(waiter.get());
    // decrement the latch after group.wait() suspends
    asio::defer(ex, [&waiting] { waiting.count_down(); });
    co_await group.wait();
  };

  asio::cancellation_signal signal;
  std::optional<std::exception_ptr> result;
  // without bind_executor(), tsan identifies a data race on signal.emit()
  asio::co_spawn(ex, cr(), bind_executor(ex, capture(signal, result)));

  waiting.wait(); // wait until we've suspended in group.wait()

  signal.emit(asio::cancellation_type::terminal);

  ctx.join();
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), asio::error::operation_aborted);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

} // namespace ceph::async
