// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/async/async_call.h"

#include <cerrno>
#include <chrono>
#include <functional>
#include <exception>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <boost/system/error_code.hpp>
#include <boost/system/errc.hpp>
#include <boost/system/system_error.hpp>

#include <gtest/gtest.h>

namespace asio = boost::asio;
namespace chrono = std::chrono;
namespace sys = boost::system;
using namespace std::literals;

using ceph::async::async_dispatch;
using ceph::async::async_post;
using ceph::async::async_defer;

inline constexpr auto dur = 50ms;

template<typename Rep, typename Period>
asio::awaitable<chrono::duration<Rep, Period>>
wait_for(chrono::duration<Rep, Period> dur)
{
  asio::steady_timer timer{co_await asio::this_coro::executor, dur};
  co_await timer.async_wait(asio::use_awaitable);
  co_return dur;
}

TEST(AsyncDispatch, AsyncCall)
{
  asio::io_context c;
  asio::co_spawn(c, [&]() -> asio::awaitable<void> {
      auto strand = asio::make_strand(c.get_executor());
      auto x = co_await async_dispatch(
	strand, [] {return 55;}, asio::use_awaitable);
      EXPECT_EQ(x, 55);
      co_return;
    }, [](std::exception_ptr e) {
      if (e) std::rethrow_exception(e);
    });
  c.run();
}

TEST(AsyncPost, AsyncCall)
{
  asio::io_context c;
  asio::co_spawn(c, [&]() -> asio::awaitable<void> {
      auto strand = asio::make_strand(c.get_executor());
      auto x = co_await async_post(
	strand, [] {return 55;}, asio::use_awaitable);
      EXPECT_EQ(x, 55);
      co_return;
    }, [](std::exception_ptr e) {
      if (e) std::rethrow_exception(e);
    });
  c.run();
}

TEST(AsyncDefer, AsyncCall)
{
  asio::io_context c;
  asio::co_spawn(c, [&]() -> asio::awaitable<void> {
      auto strand = asio::make_strand(c.get_executor());
      auto x = co_await async_defer(
	strand, [] {return 55;}, asio::use_awaitable);
      EXPECT_EQ(x, 55);
      co_return;
    }, [](std::exception_ptr e) {
      if (e) std::rethrow_exception(e);
    });
  c.run();
}

TEST(AsyncDispatchVoid, AsyncCall)
{
  asio::io_context c;
  asio::co_spawn(c, [&]() -> asio::awaitable<void> {
      auto strand = asio::make_strand(c.get_executor());
      co_await async_dispatch(
	strand, [] {return;}, asio::use_awaitable);
      co_return;
    }, [](std::exception_ptr e) {
      if (e) std::rethrow_exception(e);
    });
  c.run();
}

TEST(AsyncPostVoid, AsyncCall)
{
  asio::io_context c;
  asio::co_spawn(c, [&]() -> asio::awaitable<void> {
      auto strand = asio::make_strand(c.get_executor());
      co_await async_post(
	strand, [] {return;}, asio::use_awaitable);
      co_return;
    }, [](std::exception_ptr e) {
      if (e) std::rethrow_exception(e);
    });
  c.run();
}

TEST(AsyncDeferVoid, AsyncCall)
{
  asio::io_context c;
  asio::co_spawn(c, [&]() -> asio::awaitable<void> {
      auto strand = asio::make_strand(c.get_executor());
      co_await async_defer(
	strand, [] {return;}, asio::use_awaitable);
      co_return;
    }, [](std::exception_ptr e) {
      if (e) std::rethrow_exception(e);
    });
  c.run();
}

TEST(AsyncDispatchDeferred, AsyncCall)
{
  asio::io_context c;
  asio::co_spawn(c, [&]() -> asio::awaitable<void> {
      auto strand = asio::make_strand(c.get_executor());
      bool ran = false;
      auto op = async_dispatch(strand, [&ran] {
	ran = true;
	return 55;
      }, asio::deferred);
      EXPECT_FALSE(ran);
      std::move(op)([](int x) {
	EXPECT_EQ(x, 55);
      });
      EXPECT_TRUE(ran);
      co_return;
    }, [](std::exception_ptr e) {
      if (e) std::rethrow_exception(e);
    });
  c.run();
}

TEST(AsyncDispatchDeferredVoid, AsyncCall)
{
  asio::io_context c;
  bool ran = false;
  asio::co_spawn(c, [&]() -> asio::awaitable<void> {
      auto strand = asio::make_strand(c.get_executor());
      auto op = async_dispatch(strand, [&ran] {
	ran = true;
	return;
      }, asio::deferred);
      EXPECT_FALSE(ran);
      std::move(op)([]() {});
      co_return;
    }, [](std::exception_ptr e) {
      if (e) std::rethrow_exception(e);
    });
  c.run();
  EXPECT_TRUE(ran);
}

TEST(AsyncPostDeferred, AsyncCall)
{
  asio::io_context c;
  bool ran = false;
  asio::co_spawn(c, [&]() -> asio::awaitable<void> {
      auto strand = asio::make_strand(c.get_executor());
      auto op = async_post(strand, [&ran] {
	ran = true;
	return 55;
      }, asio::deferred);
      EXPECT_FALSE(ran);
      std::move(op)([](int x) {
	EXPECT_EQ(x, 55);
      });
      co_return;
    }, [](std::exception_ptr e) {
      if (e) std::rethrow_exception(e);
    });
  c.run();
  EXPECT_TRUE(ran);
}

TEST(AsyncPostDeferredVoid, AsyncCall)
{
  asio::io_context c;
  bool ran = false;
  asio::co_spawn(c, [&]() -> asio::awaitable<void> {
      auto strand = asio::make_strand(c.get_executor());
      auto op = async_post(strand, [&ran] {
	ran = true;
	return;
      }, asio::deferred);
      EXPECT_FALSE(ran);
      std::move(op)([]() {});
      co_return;
    }, [](std::exception_ptr e) {
      if (e) std::rethrow_exception(e);
    });
  c.run();
  EXPECT_TRUE(ran);
}

TEST(AsyncDeferDeferred, AsyncCall)
{
  asio::io_context c;
  bool ran = false;
  asio::co_spawn(c, [&]() -> asio::awaitable<void> {
      auto strand = asio::make_strand(c.get_executor());
      auto op = async_defer(strand, [&ran] {
	ran = true;
	return 55;
      }, asio::deferred);
      EXPECT_FALSE(ran);
      std::move(op)([](int x) {
	EXPECT_EQ(x, 55);
      });
      co_return;
    }, [](std::exception_ptr e) {
      if (e) std::rethrow_exception(e);
    });
  c.run();
  EXPECT_TRUE(ran);
}

TEST(AsyncDeferDeferredVoid, AsyncCall)
{
  asio::io_context c;
  bool ran = false;
  asio::co_spawn(c, [&]() -> asio::awaitable<void> {
      auto strand = asio::make_strand(c.get_executor());
      auto op = async_defer(strand, [&ran] {
	ran = true;
	return;
      }, asio::deferred);
      EXPECT_FALSE(ran);
      std::move(op)([]() {});
      co_return;
    }, [](std::exception_ptr e) {
      if (e) std::rethrow_exception(e);
    });
  c.run();
  EXPECT_TRUE(ran);
}
