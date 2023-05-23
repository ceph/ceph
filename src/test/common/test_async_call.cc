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
#include "common/async/async_yield.h"
#include "common/async/coro_aiocomplete.h"

#include <cerrno>
#include <chrono>
#include <functional>
#include <exception>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <boost/system/error_code.hpp>
#include <boost/system/errc.hpp>
#include <boost/system/system_error.hpp>

#include <spawn/spawn.hpp>

#include <gtest/gtest.h>

namespace asio = boost::asio;
namespace chrono = std::chrono;
namespace sys = boost::system;
using namespace std::literals;

using ceph::async::async_dispatch;
using ceph::async::async_post;
using ceph::async::async_defer;
using ceph::async::async_yield;
using ceph::async::coro_aiocomplete;

inline constexpr auto dur = 50ms;

template<typename Rep, typename Period>
asio::awaitable<chrono::duration<Rep, Period>>
wait_for(chrono::duration<Rep, Period> dur)
{
  asio::steady_timer timer{co_await asio::this_coro::executor, dur};
  co_await timer.async_wait(asio::use_awaitable);
  co_return dur;
}

template<typename Rep, typename Period>
chrono::duration<Rep, Period> wait_for(auto executor,
				       chrono::duration<Rep, Period> dur,
				       spawn::yield_context y)
{
  asio::steady_timer timer{executor, dur};
  timer.async_wait(y);
  return dur;
}

TEST(Yield2Coro, Yield2Coro)
{
  asio::io_context c;
  spawn::spawn(c, [&](spawn::yield_context y) {
    auto start = chrono::steady_clock::now();
    auto [eptr, d] = asio::co_spawn(c.get_executor(), wait_for(dur), y);
    auto end = chrono::steady_clock::now();
    ASSERT_GE(end, start + dur);
    ASSERT_EQ(dur, d);
  });
  c.run();
}

TEST(Return, Coro2Yield)
{
  asio::io_context c;
  asio::co_spawn(c, [&]() -> asio::awaitable<void> {
      auto start = chrono::steady_clock::now();
      auto d = co_await async_yield<spawn::yield_context>(
	co_await asio::this_coro::executor,
	[&c](spawn::yield_context y) {
	  return wait_for(c.get_executor(),dur, y);
	}, asio::use_awaitable);
      auto end = chrono::steady_clock::now();
      EXPECT_GE(end, start + dur);
      EXPECT_EQ(dur, d);
      co_return;
    }, [](std::exception_ptr e) {
      if (e) std::rethrow_exception(e);
    });
  c.run();
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

TEST(VoidSucc, AioComplete)
{
  auto lrc = librados::Rados::aio_create_completion();
  asio::io_context c;
  coro_aiocomplete(
    nullptr, c.get_executor(),
    []() -> asio::awaitable<void> {
      co_return;
    }(), lrc);
  c.run();
  lrc->wait_for_complete();
  auto r = lrc->get_return_value();
  ASSERT_EQ(0, r);
}

TEST(VoidExcept, AioComplete)
{
  auto lrc = librados::Rados::aio_create_completion();
  asio::io_context c;
  coro_aiocomplete(
    nullptr, c.get_executor(),
    []() -> asio::awaitable<void> {
      throw sys::system_error{ENOENT, sys::generic_category()};
      co_return;
    }(), lrc);
  c.run();
  lrc->wait_for_complete();
  auto r = lrc->get_return_value();
  ASSERT_EQ(-ENOENT, r);
}

TEST(VoidUnknownExcept, AioComplete)
{
  auto lrc = librados::Rados::aio_create_completion();
  asio::io_context c;
  coro_aiocomplete(
    nullptr, c.get_executor(),
    []() -> asio::awaitable<void> {
      throw std::exception{};
      co_return;
    }(), lrc);
  c.run();
  lrc->wait_for_complete();
  auto r = lrc->get_return_value();
  ASSERT_EQ(-EFAULT, r);
}

TEST(IntSucc, AioComplete)
{
  auto lrc = librados::Rados::aio_create_completion();
  asio::io_context c;
  coro_aiocomplete(
    nullptr, c.get_executor(),
    []() -> asio::awaitable<int> {
      co_return -42;
    }(), lrc);
  c.run();
  lrc->wait_for_complete();
  auto r = lrc->get_return_value();
  ASSERT_EQ(-42, r);
}

TEST(IntExcept, AioComplete)
{
  auto lrc = librados::Rados::aio_create_completion();
  asio::io_context c;
  coro_aiocomplete(
    nullptr, c.get_executor(),
    []() -> asio::awaitable<int> {
      throw sys::system_error{ENOENT, sys::generic_category()};
      co_return -42;
    }(), lrc);
  c.run();
  lrc->wait_for_complete();
  auto r = lrc->get_return_value();
  ASSERT_EQ(-ENOENT, r);
}

TEST(SysSucc, AioComplete)
{
  auto lrc = librados::Rados::aio_create_completion();
  asio::io_context c;
  coro_aiocomplete(
    nullptr, c.get_executor(),
    []() -> asio::awaitable<sys::error_code> {
      co_return sys::error_code{};
    }(), lrc);
  c.run();
  lrc->wait_for_complete();
  auto r = lrc->get_return_value();
  ASSERT_EQ(0, r);
}

TEST(SysFail, AioComplete)
{
  auto lrc = librados::Rados::aio_create_completion();
  asio::io_context c;
  coro_aiocomplete(
    nullptr, c.get_executor(),
    []() -> asio::awaitable<sys::error_code> {
      co_return sys::error_code{ENOENT, sys::generic_category()};
    }(), lrc);
  c.run();
  lrc->wait_for_complete();
  auto r = lrc->get_return_value();
  ASSERT_EQ(-ENOENT, r);
}

TEST(SysExcept, AioComplete)
{
  auto lrc = librados::Rados::aio_create_completion();
  asio::io_context c;
  coro_aiocomplete(
    nullptr, c.get_executor(),
    []() -> asio::awaitable<sys::error_code> {
      throw sys::system_error{ENOENT, sys::generic_category()};
      co_return sys::error_code{EBADF, sys::generic_category()};
    }(), lrc);
  c.run();
  lrc->wait_for_complete();
  auto r = lrc->get_return_value();
  ASSERT_EQ(-ENOENT, r);
}
