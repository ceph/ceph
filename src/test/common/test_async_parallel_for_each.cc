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

#include "common/async/parallel_for_each.h"

#include <optional>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <gtest/gtest.h>
#include "common/async/co_waiter.h"

namespace ceph::async {

namespace asio = boost::asio;
namespace errc = boost::system::errc;
using boost::system::error_code;

using executor_type = asio::io_context::executor_type;

template <typename T>
using awaitable = asio::awaitable<T, executor_type>;

using void_waiter = co_waiter<void, executor_type>;

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

TEST(parallel_for_each, empty)
{
  asio::io_context ctx;

  int* end = nullptr;
  auto cr = [] (int i) -> awaitable<void> { co_return; };

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, parallel_for_each(end, end, cr), capture(result));

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
}

TEST(parallel_for_each, shutdown)
{
  asio::io_context ctx;

  void_waiter waiters[2];
  auto cr = [] (void_waiter& w) -> awaitable<void> { return w.get(); };

  asio::cancellation_signal signal;
  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, parallel_for_each(waiters, cr), capture(signal, result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  // shut down before any waiters complete
}

TEST(parallel_for_each, cancel)
{
  asio::io_context ctx;

  void_waiter waiters[2];
  auto cr = [] (void_waiter& w) -> awaitable<void> { return w.get(); };

  asio::cancellation_signal signal;
  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, parallel_for_each(waiters, cr), capture(signal, result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  // cancel before any waiters complete
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

TEST(parallel_for_each, complete_shutdown)
{
  asio::io_context ctx;

  void_waiter waiters[2];
  auto cr = [] (void_waiter& w) -> awaitable<void> { return w.get(); };

  asio::cancellation_signal signal;
  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, parallel_for_each(waiters, cr), capture(signal, result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  waiters[0].complete(nullptr);

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  // shut down before final waiter completes
}

TEST(parallel_for_each, complete_cancel)
{
  asio::io_context ctx;

  void_waiter waiters[2];
  auto cr = [] (void_waiter& w) -> awaitable<void> { return w.get(); };

  asio::cancellation_signal signal;
  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, parallel_for_each(waiters, cr), capture(signal, result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  waiters[0].complete(nullptr);

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  // cancel before final waiter completes
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

TEST(parallel_for_each, complete_complete)
{
  asio::io_context ctx;

  void_waiter waiters[2];
  auto cr = [] (void_waiter& w) -> awaitable<void> { return w.get(); };

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, parallel_for_each(waiters, cr), capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  waiters[0].complete(nullptr);

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  waiters[1].complete(nullptr);

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
}

struct null_sentinel {};
bool operator==(const char* c, null_sentinel) { return !*c; }
static_assert(std::sentinel_for<null_sentinel, const char*>);

TEST(parallel_for_each, sentinel)
{
  asio::io_context ctx;

  const char* begin = "hello";
  null_sentinel end;

  size_t count = 0;
  auto cr = [&count] (char c) -> awaitable<void> {
    ++count;
    co_return;
  };

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, parallel_for_each(begin, end, cr), capture(result));

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
  EXPECT_EQ(count, 5);
}

TEST(parallel_for_each, move_iterator)
{
  asio::io_context ctx;

  using value_type = std::unique_ptr<int>;
  value_type values[] = {
    std::make_unique<int>(42),
    std::make_unique<int>(43),
  };

  auto begin = std::make_move_iterator(std::begin(values));
  auto end = std::make_move_iterator(std::end(values));

  auto cr = [] (value_type v) -> awaitable<void> {
    if (!v) {
      throw std::invalid_argument("empty");
    }
    co_return;
  };

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, parallel_for_each(begin, end, cr), capture(result));

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);

  EXPECT_FALSE(values[0]);
  EXPECT_FALSE(values[1]);
}

} // namespace ceph::async
