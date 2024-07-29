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

#include "common/async/yield_waiter.h"
#include <exception>
#include <memory>
#include <optional>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>
#include <gtest/gtest.h>

namespace ceph::async {

namespace asio = boost::asio;
using error_code = boost::system::error_code;

void rethrow(std::exception_ptr eptr)
{
  if (eptr) std::rethrow_exception(eptr);
}

auto capture(std::optional<std::exception_ptr>& eptr)
{
  return [&eptr] (std::exception_ptr e) { eptr = e; };
}


TEST(YieldWaiterVoid, wait_shutdown)
{
  asio::io_context ctx;
  yield_waiter<void> waiter;

  asio::spawn(ctx, [&waiter] (asio::yield_context yield) {
        waiter.async_wait(yield);
      }, rethrow);

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
}

TEST(YieldWaiterVoid, wait_complete)
{
  asio::io_context ctx;
  yield_waiter<void> waiter;

  asio::spawn(ctx, [&waiter] (asio::yield_context yield) {
        waiter.async_wait(yield);
      }, rethrow);

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());

  ASSERT_TRUE(waiter);
  waiter.complete(error_code{});
  EXPECT_FALSE(waiter);

  ctx.poll();
  EXPECT_TRUE(ctx.stopped());
}

TEST(YieldWaiterVoid, wait_error)
{
  asio::io_context ctx;
  yield_waiter<void> waiter;
  std::optional<std::exception_ptr> eptr;

  asio::spawn(ctx, [&waiter] (asio::yield_context yield) {
        waiter.async_wait(yield);
      }, capture(eptr));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());

  ASSERT_TRUE(waiter);
  waiter.complete(make_error_code(asio::error::operation_aborted));
  EXPECT_FALSE(waiter);

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(eptr);
  ASSERT_TRUE(*eptr);
  try {
    std::rethrow_exception(*eptr);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), asio::error::operation_aborted);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}


TEST(YieldWaiterInt, wait_shutdown)
{
  asio::io_context ctx;
  yield_waiter<int> waiter;

  asio::spawn(ctx, [&waiter] (asio::yield_context yield) {
        waiter.async_wait(yield);
      }, rethrow);

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
}

TEST(YieldWaiterInt, wait_complete)
{
  asio::io_context ctx;
  yield_waiter<int> waiter;
  std::optional<int> result;

  asio::spawn(ctx, [&waiter, &result] (asio::yield_context yield) {
        result = waiter.async_wait(yield);
      }, rethrow);

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());

  ASSERT_TRUE(waiter);
  waiter.complete(error_code{}, 42);
  EXPECT_FALSE(waiter);

  ctx.poll();
  EXPECT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_EQ(42, *result);
}

TEST(YieldWaiterInt, wait_error)
{
  asio::io_context ctx;
  yield_waiter<int> waiter;
  std::optional<int> result;
  std::optional<std::exception_ptr> eptr;

  asio::spawn(ctx, [&waiter, &result] (asio::yield_context yield) {
        result = waiter.async_wait(yield);
      }, capture(eptr));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());

  ASSERT_TRUE(waiter);
  waiter.complete(make_error_code(std::errc::no_such_file_or_directory), 0);
  EXPECT_FALSE(waiter);

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_TRUE(eptr);
  ASSERT_TRUE(*eptr);
  try {
    std::rethrow_exception(*eptr);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), std::errc::no_such_file_or_directory);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}


// test with move-only value type
TEST(YieldWaiterPtr, wait_shutdown)
{
  asio::io_context ctx;
  yield_waiter<std::unique_ptr<int>> waiter;

  asio::spawn(ctx, [&waiter] (asio::yield_context yield) {
        waiter.async_wait(yield);
      }, rethrow);

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
}

TEST(YieldWaiterPtr, wait_complete)
{
  asio::io_context ctx;
  yield_waiter<std::unique_ptr<int>> waiter;
  std::optional<std::unique_ptr<int>> result;

  asio::spawn(ctx, [&waiter, &result] (asio::yield_context yield) {
        result = waiter.async_wait(yield);
      }, rethrow);

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());

  ASSERT_TRUE(waiter);
  waiter.complete(error_code{}, std::make_unique<int>(42));
  EXPECT_FALSE(waiter);

  ctx.poll();
  EXPECT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  EXPECT_EQ(42, **result);
}

TEST(YieldWaiterPtr, wait_error)
{
  asio::io_context ctx;
  yield_waiter<std::unique_ptr<int>> waiter;
  std::optional<std::unique_ptr<int>> result;
  std::optional<std::exception_ptr> eptr;

  asio::spawn(ctx, [&waiter, &result] (asio::yield_context yield) {
        result = waiter.async_wait(yield);
      }, capture(eptr));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());

  ASSERT_TRUE(waiter);
  waiter.complete(make_error_code(std::errc::no_such_file_or_directory), nullptr);
  EXPECT_FALSE(waiter);

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_TRUE(eptr);
  ASSERT_TRUE(*eptr);
  try {
    std::rethrow_exception(*eptr);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), std::errc::no_such_file_or_directory);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

} // namespace ceph::async
