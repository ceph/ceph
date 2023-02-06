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
#include <optional>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
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
using use_awaitable_t = asio::use_awaitable_t<executor_type>;
static constexpr use_awaitable_t use_awaitable{};

using void_waiter = co_waiter<void, executor_type>;

auto capture(std::optional<std::exception_ptr>& eptr)
{
  return [&eptr] (std::exception_ptr e) { eptr = e; };
}

auto capture(asio::cancellation_signal& signal,
             std::optional<std::exception_ptr>& eptr)
{
  return asio::bind_cancellation_slot(signal.slot(), capture(eptr));
}

awaitable<void> wait(void_waiter& waiter, bool& completed)
{
  co_await waiter.get();
  completed = true;
}

TEST(co_throttle, wait_empty)
{
  constexpr size_t limit = 1;
  asio::io_context ctx;

  auto cr = [&] () -> awaitable<void> {
    auto throttle = co_throttle{co_await asio::this_coro::executor, limit};
    co_await throttle.wait();
  };

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, cr(), capture(result));

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
}

TEST(co_throttle, spawn_over_limit)
{
  constexpr size_t limit = 1;
  asio::io_context ctx;

  void_waiter waiter1;
  void_waiter waiter2;
  bool spawn1_completed = false;
  bool spawn2_completed = false;

  auto cr = [&] () -> awaitable<void> {
    auto throttle = co_throttle{co_await asio::this_coro::executor, limit};
    co_await throttle.spawn(waiter1.get());
    spawn1_completed = true;
    co_await throttle.spawn(waiter2.get());
    spawn2_completed = true;
    co_await throttle.wait();
  };

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, cr(), capture(result));

  ctx.poll(); // run until spawn2 blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_TRUE(spawn1_completed);
  EXPECT_FALSE(spawn2_completed);

  waiter1.complete(nullptr);

  ctx.poll(); // run until wait blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  EXPECT_TRUE(spawn2_completed);

  waiter2.complete(nullptr);

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
}

TEST(co_throttle, spawn_over_smaller_limit)
{
  constexpr size_t limit = 2;
  constexpr size_t smaller_limit = 1;
  asio::io_context ctx;

  void_waiter waiter1;
  void_waiter waiter2;
  bool spawn1_completed = false;
  bool spawn2_completed = false;

  auto cr = [&] () -> awaitable<void> {
    auto throttle = co_throttle{co_await asio::this_coro::executor, limit};
    co_await throttle.spawn(waiter1.get());
    spawn1_completed = true;
    co_await throttle.spawn(waiter2.get(), smaller_limit);
    spawn2_completed = true;
    co_await throttle.wait();
  };

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, cr(), capture(result));

  ctx.poll(); // run until spawn2 blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_TRUE(spawn1_completed);
  EXPECT_FALSE(spawn2_completed);

  waiter1.complete(nullptr);

  ctx.poll(); // run until wait blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_TRUE(spawn2_completed);

  waiter2.complete(nullptr);

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
}

TEST(co_throttle, spawn_cancel)
{
  constexpr size_t limit = 1;
  asio::io_context ctx;

  void_waiter waiter1;
  void_waiter waiter2;
  bool spawn1_completed = false;
  bool spawn2_completed = false;

  auto cr = [&] () -> awaitable<void> {
    auto throttle = co_throttle{co_await asio::this_coro::executor, limit};
    co_await throttle.spawn(waiter1.get());
    spawn1_completed = true;
    co_await throttle.spawn(waiter2.get());
    spawn2_completed = true;
    co_await throttle.wait();
  };

  asio::cancellation_signal signal;
  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, cr(), capture(signal, result));

  ctx.poll(); // run until spawn2 blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_TRUE(spawn1_completed);
  EXPECT_FALSE(spawn2_completed);

  // cancel before spawn2 completes
  signal.emit(asio::cancellation_type::terminal);

  ctx.poll();
  ASSERT_TRUE(ctx.stopped()); // poll runs to completion
  EXPECT_FALSE(spawn2_completed);
  ASSERT_TRUE(result);
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), asio::error::operation_aborted);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

TEST(co_throttle, wait_cancel)
{
  constexpr size_t limit = 1;
  asio::io_context ctx;

  void_waiter waiter;
  bool spawn_completed = false;

  auto cr = [&] () -> awaitable<void> {
    auto throttle = co_throttle{co_await asio::this_coro::executor, limit};
    co_await throttle.spawn(waiter.get());
    spawn_completed = true;
    co_await throttle.wait();
  };

  asio::cancellation_signal signal;
  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, cr(), capture(signal, result));

  ctx.poll(); // run until wait blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_TRUE(spawn_completed);
  EXPECT_FALSE(result);

  // cancel before wait completes
  signal.emit(asio::cancellation_type::terminal);

  ctx.poll();
  ASSERT_TRUE(ctx.stopped()); // poll runs to completion
  ASSERT_TRUE(result);
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), asio::error::operation_aborted);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

TEST(co_throttle, spawn_shutdown)
{
  constexpr size_t limit = 1;
  asio::io_context ctx;

  void_waiter waiter1;
  void_waiter waiter2;
  bool spawn1_completed = false;

  auto cr = [&] () -> awaitable<void> {
    auto throttle = co_throttle{co_await asio::this_coro::executor, limit};
    co_await throttle.spawn(waiter1.get());
    spawn1_completed = true;
    co_await throttle.spawn(waiter2.get());
  };

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, cr(), capture(result));

  ctx.poll(); // run until spawn2 blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_TRUE(spawn1_completed);
  EXPECT_FALSE(result);
  // shut down before spawn2 completes
}

TEST(co_throttle, wait_shutdown)
{
  constexpr size_t limit = 1;
  asio::io_context ctx;

  void_waiter waiter;
  bool spawn_completed = false;

  auto cr = [&] () -> awaitable<void> {
    auto throttle = co_throttle{co_await asio::this_coro::executor, limit};
    co_await throttle.spawn(waiter.get());
    spawn_completed = true;
    co_await throttle.wait();
  };

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, cr(), capture(result));

  ctx.poll(); // run until wait blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_TRUE(spawn_completed);
  EXPECT_FALSE(result);
  // shut down before wait completes
}

TEST(co_throttle, spawn_error)
{
  constexpr size_t limit = 2;
  asio::io_context ctx;

  void_waiter waiter1;
  void_waiter waiter2;
  void_waiter waiter3;
  bool cr1_completed = false;
  bool cr2_completed = false;
  bool cr3_completed = false;
  std::exception_ptr spawn3_eptr;

  auto cr = [&] () -> awaitable<void> {
    auto throttle = co_throttle{co_await asio::this_coro::executor, limit};
    co_await throttle.spawn(wait(waiter1, cr1_completed));
    co_await throttle.spawn(wait(waiter2, cr2_completed));
    try {
      co_await throttle.spawn(wait(waiter3, cr3_completed));
    } catch (const std::exception&) {
      spawn3_eptr = std::current_exception();
    }
    co_await throttle.wait();
  };

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, cr(), capture(result));

  ctx.poll(); // run until spawn3 blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(cr1_completed);
  EXPECT_FALSE(cr2_completed);
  EXPECT_FALSE(cr3_completed);

  waiter2.complete(std::make_exception_ptr(std::runtime_error{"oops"}));

  ctx.poll(); // run until wait blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(spawn3_eptr);
  EXPECT_THROW(std::rethrow_exception(spawn3_eptr), std::runtime_error);
  EXPECT_FALSE(result);

  waiter1.complete(nullptr);

  ctx.poll();
  ASSERT_FALSE(ctx.stopped()); // wait still blocked

  waiter3.complete(nullptr);

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
  EXPECT_TRUE(cr1_completed);
  EXPECT_FALSE(cr2_completed);
  EXPECT_TRUE(cr3_completed); // cr3 isn't canceled by cr2's error
}

TEST(co_throttle, wait_error)
{
  constexpr size_t limit = 1;
  asio::io_context ctx;

  void_waiter waiter;

  auto cr = [&] () -> awaitable<void> {
    auto throttle = co_throttle{co_await asio::this_coro::executor, limit};
    co_await throttle.spawn(waiter.get());
    co_await throttle.wait();
  };

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, cr(), capture(result));

  ctx.poll(); // run until wait blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  waiter.complete(std::make_exception_ptr(std::runtime_error{"oops"}));

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  EXPECT_THROW(std::rethrow_exception(*result), std::runtime_error);
}

TEST(co_throttle, spawn_cancel_on_error_after)
{
  constexpr size_t limit = 2;
  asio::io_context ctx;

  void_waiter waiter1;
  void_waiter waiter2;
  void_waiter waiter3;
  void_waiter waiter4;
  bool cr1_completed = false;
  bool cr2_completed = false;
  bool cr3_completed = false;
  bool cr4_completed = false;
  std::exception_ptr spawn3_eptr;

  auto cr = [&] () -> awaitable<void> {
    auto ex = co_await asio::this_coro::executor;
    auto throttle = co_throttle{ex, limit, cancel_on_error::after};
    co_await throttle.spawn(wait(waiter1, cr1_completed));
    co_await throttle.spawn(wait(waiter2, cr2_completed));
    try {
      co_await throttle.spawn(wait(waiter3, cr3_completed));
    } catch (const std::exception&) {
      spawn3_eptr = std::current_exception();
    }
    co_await throttle.spawn(wait(waiter4, cr4_completed));
    co_await throttle.wait();
  };

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, cr(), capture(result));

  ctx.poll(); // run until spawn3 blocks
  ASSERT_FALSE(ctx.stopped());

  waiter2.complete(std::make_exception_ptr(std::runtime_error{"oops"}));

  ctx.poll(); // run until wait blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(cr1_completed);
  ASSERT_TRUE(spawn3_eptr);
  EXPECT_THROW(std::rethrow_exception(spawn3_eptr), std::runtime_error);

  waiter1.complete(nullptr);

  ctx.poll();
  ASSERT_FALSE(ctx.stopped()); // wait still blocked
  EXPECT_FALSE(result);
  EXPECT_TRUE(cr1_completed);
  EXPECT_FALSE(cr4_completed);

  waiter4.complete(nullptr);

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
  EXPECT_FALSE(cr2_completed); // exited by exception
  EXPECT_FALSE(cr3_completed); // cr3 canceled
  EXPECT_TRUE(cr4_completed); // cr4 not canceled
}

TEST(co_throttle, spawn_cancel_on_error_all)
{
  constexpr size_t limit = 2;
  asio::io_context ctx;

  void_waiter waiter1;
  void_waiter waiter2;
  void_waiter waiter3;
  void_waiter waiter4;
  bool cr1_completed = false;
  bool cr2_completed = false;
  bool cr3_completed = false;
  bool cr4_completed = false;
  std::exception_ptr spawn3_eptr;

  auto cr = [&] () -> awaitable<void> {
    auto ex = co_await asio::this_coro::executor;
    auto throttle = co_throttle{ex, limit, cancel_on_error::all};
    co_await throttle.spawn(wait(waiter1, cr1_completed));
    co_await throttle.spawn(wait(waiter2, cr2_completed));
    try {
      co_await throttle.spawn(wait(waiter3, cr3_completed));
    } catch (const std::exception&) {
      spawn3_eptr = std::current_exception();
    }
    co_await throttle.spawn(wait(waiter4, cr4_completed));
    co_await throttle.wait();
  };

  std::optional<std::exception_ptr> result;
  asio::co_spawn(ctx, cr(), capture(result));

  ctx.poll(); // run until spawn3 blocks
  ASSERT_FALSE(ctx.stopped());

  waiter2.complete(std::make_exception_ptr(std::runtime_error{"oops"}));

  ctx.poll(); // run until wait blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(spawn3_eptr);
  EXPECT_THROW(std::rethrow_exception(spawn3_eptr), std::runtime_error);
  EXPECT_FALSE(cr4_completed);

  waiter4.complete(nullptr);

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
  EXPECT_FALSE(cr1_completed); // cr1 canceled
  EXPECT_FALSE(cr2_completed); // exited by exception
  EXPECT_FALSE(cr3_completed); // cr3 canceled
  EXPECT_TRUE(cr4_completed); // cr4 not canceled
}

} // namespace ceph::async
