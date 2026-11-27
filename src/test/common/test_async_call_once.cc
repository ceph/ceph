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

#include "common/async/call_once.h"

#include <exception>
#include <future>
#include <latch>
#include <optional>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>
#include <gtest/gtest.h>
#include "common/async/yield_waiter.h"

void rethrow(std::exception_ptr eptr)
{
  if (eptr) std::rethrow_exception(eptr);
}

auto capture(std::optional<std::exception_ptr>& out) {
  return [&out] (std::exception_ptr eptr) {
    out = std::move(eptr);
  };
}

template <typename T>
auto capture(std::optional<T>& out) {
  return [&out] (std::exception_ptr eptr, T value) {
    rethrow(eptr);
    out = std::move(value);
  };
}

namespace ceph::async {

TEST(CallOnceAsync, call_wait_shutdown)
{
  boost::asio::io_context context;
  // test that memory doesn't leak when both coroutines hold a shared_ptr to
  // the once_result they're waiting on
  auto once = std::make_shared<once_result<int>>();
  yield_waiter<int> waiter;

  boost::asio::spawn(context, [once, &waiter] (boost::asio::yield_context yield) {
        call_once(*once, yield, [&] { return waiter.async_wait(yield); });
      }, rethrow);
  boost::asio::spawn(context, [once] (boost::asio::yield_context yield) {
        call_once(*once, yield, [] { return 0; });
      }, rethrow);

  context.poll();
  EXPECT_FALSE(context.stopped()); // blocked on waiter, never completes
}

TEST(CallOnceAsync, call_immediate_result)
{
  boost::asio::io_context context;
  once_result<int> once;

  std::optional<int> result;
  boost::asio::spawn(context, [&] (boost::asio::yield_context yield) {
        return call_once(once, yield, [] { return 0; });
      }, capture(result));

  context.poll();
  EXPECT_TRUE(context.stopped());
  ASSERT_TRUE(result);
  EXPECT_EQ(*result, 0);
}

TEST(CallOnceAsync, call_immediate_exception)
{
  boost::asio::io_context context;
  once_result<int> once;

  std::optional<std::exception_ptr> eptr;
  boost::asio::spawn(context, [&] (boost::asio::yield_context yield) {
        call_once(once, yield, [] { throw std::bad_alloc(); return 0; });
      }, capture(eptr));

  context.poll();
  EXPECT_TRUE(context.stopped());
  ASSERT_TRUE(eptr);
  EXPECT_THROW(std::rethrow_exception(*eptr), std::bad_alloc);
}

TEST(CallOnceAsync, call_wait_result)
{
  boost::asio::io_context context;
  once_result<int> once;
  yield_waiter<int> waiter;

  std::optional<int> call_result;
  boost::asio::spawn(context, [&] (boost::asio::yield_context yield) {
        return call_once(once, yield, [&] { return waiter.async_wait(yield); });
      }, capture(call_result));

  std::optional<int> wait_result;
  boost::asio::spawn(context, [&] (boost::asio::yield_context yield) {
        return call_once(once, yield, [] { return 0; });
      }, capture(wait_result));

  context.poll();
  EXPECT_FALSE(context.stopped());
  ASSERT_FALSE(call_result);
  ASSERT_FALSE(wait_result);

  waiter.complete(boost::system::error_code{}, 42);

  context.poll();
  EXPECT_TRUE(context.stopped());
  ASSERT_TRUE(call_result);
  EXPECT_EQ(*call_result, 42);
  ASSERT_TRUE(wait_result);
  EXPECT_EQ(*wait_result, 42);

  std::optional<int> cached_result;
  boost::asio::spawn(context, [&] (boost::asio::yield_context yield) {
        return call_once(once, yield, [] { return 0; });
      }, capture(cached_result));

  context.restart();
  context.poll();
  EXPECT_TRUE(context.stopped());
  ASSERT_TRUE(cached_result);
  EXPECT_EQ(*cached_result, 42);
}

TEST(CallOnceAsync, call_wait_exception)
{
  boost::asio::io_context context;
  once_result<int> once;
  yield_waiter<void> waiter;

  std::optional<std::exception_ptr> call_eptr;
  boost::asio::spawn(context, [&] (boost::asio::yield_context yield) {
        call_once(once, yield, [&] {
            waiter.async_wait(yield);
            throw std::bad_alloc();
            return 0;
          });
      }, capture(call_eptr));

  std::optional<std::exception_ptr> wait_eptr;
  boost::asio::spawn(context, [&] (boost::asio::yield_context yield) {
        call_once(once, yield, [] { return 0; });
      }, capture(wait_eptr));

  context.poll();
  EXPECT_FALSE(context.stopped());
  ASSERT_FALSE(call_eptr);
  ASSERT_FALSE(wait_eptr);

  waiter.complete(boost::system::error_code{});

  context.poll();
  EXPECT_TRUE(context.stopped());
  ASSERT_TRUE(call_eptr);
  EXPECT_THROW(std::rethrow_exception(*call_eptr), std::bad_alloc);
  ASSERT_TRUE(wait_eptr);
  EXPECT_THROW(std::rethrow_exception(*wait_eptr), std::bad_alloc);

  std::optional<std::exception_ptr> cached_eptr;
  boost::asio::spawn(context, [&] (boost::asio::yield_context yield) {
        call_once(once, yield, [] { return 0; });
      }, capture(cached_eptr));

  context.restart();
  context.poll();
  EXPECT_TRUE(context.stopped());
  ASSERT_TRUE(cached_eptr);
  EXPECT_THROW(std::rethrow_exception(*cached_eptr), std::bad_alloc);
}

TEST(CallOnceSync, call_immediate_result)
{
  once_result<int> once;

  EXPECT_EQ(call_once(once, null_yield, [] { return 42; }), 42);
}

TEST(CallOnceSync, call_immediate_result_lvalue)
{
  once_result<int> once;

  // test that call_once() properly forwards the Callable so
  // it can be passed by lvalue-ref without copy/move
  struct noncopyable_fn {
   public:
    noncopyable_fn() = default;
    noncopyable_fn(const noncopyable_fn&) = delete;
    noncopyable_fn& operator=(const noncopyable_fn&) = delete;

    int operator()() { return 42; }
  };
  noncopyable_fn fn;

  EXPECT_EQ(call_once(once, null_yield, fn), 42);
}

TEST(CallOnceSync, call_immediate_exception)
{
  once_result<int> once;

  EXPECT_THROW(call_once(once, null_yield, [] { throw std::bad_alloc(); return 0; }), std::bad_alloc);
}

TEST(CallOnceSync, call_wait_result)
{
  once_result<int> once;

  std::future<int> call_future;
  std::future<int> wait_future;
  std::latch call_latch{1};
  std::latch wait_latch{2};

  call_future = std::async(std::launch::async, [&] {
      return call_once(once, null_yield, [&] {
          wait_future = std::async(std::launch::async, [&] {
              wait_latch.count_down();
              return call_once(once, null_yield, [] { return 0; });
            });
          wait_latch.count_down();
          call_latch.wait();
          return 42;
        });
    });

  wait_latch.wait();

  using namespace std::chrono_literals;
  EXPECT_NE(wait_future.wait_for(0s), std::future_status::ready);

  call_latch.count_down(); // let call return

  EXPECT_EQ(call_future.get(), 42);
  EXPECT_EQ(wait_future.get(), 42);

  // return cached result
  EXPECT_EQ(call_once(once, null_yield, [] { return 0; }), 42);
}

TEST(CallOnceSync, call_wait_exception)
{
  once_result<int> once;

  std::future<int> call_future;
  std::future<int> wait_future;
  std::latch call_latch{1};
  std::latch wait_latch{2};

  call_future = std::async(std::launch::async, [&] {
      return call_once(once, null_yield, [&] {
          wait_future = std::async(std::launch::async, [&] {
              wait_latch.count_down();
              return call_once(once, null_yield, [] { return 0; });
            });
          wait_latch.count_down();
          call_latch.wait();
          throw std::bad_alloc();
          return 0;
        });
    });

  wait_latch.wait();

  using namespace std::chrono_literals;
  EXPECT_NE(wait_future.wait_for(0s), std::future_status::ready);

  call_latch.count_down(); // let call return

  EXPECT_THROW(call_future.get(), std::bad_alloc);
  EXPECT_THROW(wait_future.get(), std::bad_alloc);

  // rethrow cached exception
  EXPECT_THROW(call_once(once, null_yield, [] { return 0; }), std::bad_alloc);
}

} // namespace ceph::async
