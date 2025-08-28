// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */


#include <boost/asio/append.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>

#include <boost/system/error_code.hpp>

#include <gtest/gtest.h>

#include "common/async/blocked_completion.h"
using namespace std::literals;

namespace asio = boost::asio;
namespace sys = boost::system;
namespace async = ceph::async;

class context_thread {
  asio::io_context c;
  asio::executor_work_guard<asio::io_context::executor_type> guard;
  std::thread th;

public:
  context_thread() noexcept
    : guard(asio::make_work_guard(c)),
      th([this]() noexcept { c.run();}) {}

  ~context_thread() {
    guard.reset();
    th.join();
  }

  asio::io_context& io_context() noexcept {
    return c;
  }

  asio::io_context::executor_type get_executor() noexcept {
    return c.get_executor();
  }
};

struct move_only {
  move_only() = default;
  move_only(move_only&&) = default;
  move_only& operator=(move_only&&) = default;
  move_only(const move_only&) = delete;
  move_only& operator=(const move_only&) = delete;
};

struct defaultless {
  int a;
  defaultless(int a) : a(a) {}
};

template<typename Executor, typename CompletionToken, typename... Args>
auto id(const Executor& executor, CompletionToken&& token,
	Args&& ...args)
{
  return asio::async_initiate<CompletionToken, void(Args...)>(
    []<typename ...Args2>(auto handler, Args2&& ...args2) mutable {
      asio::post(asio::append(std::move(handler),
			      std::forward<Args2>(args2)...));
    }, token, std::forward<Args>(args)...);
}

TEST(BlockedCompletion, Void)
{
  context_thread t;

  asio::post(t.get_executor(), async::use_blocked);
}

TEST(BlockedCompletion, Timer)
{
  context_thread t;
  asio::steady_timer timer(t.io_context(), 50ms);
  timer.async_wait(async::use_blocked);
}

TEST(BlockedCompletion, NoError)
{
  context_thread t;
  asio::steady_timer timer(t.io_context(), 1s);
  sys::error_code ec;

  EXPECT_NO_THROW(id(t.get_executor(), async::use_blocked, sys::error_code{}));
  EXPECT_NO_THROW(id(t.get_executor(), async::use_blocked[ec], sys::error_code{}));
  EXPECT_FALSE(ec);

  int i;
  EXPECT_NO_THROW(i = id(t.get_executor(), async::use_blocked,
			 sys::error_code{}, 5));
  ASSERT_EQ(5, i);
  EXPECT_NO_THROW(
      i = id(t.get_executor(), async::use_blocked[ec], sys::error_code{}, 7));
  EXPECT_FALSE(ec);
  ASSERT_EQ(7, i);

  float j;

  EXPECT_NO_THROW(std::tie(i, j) = id(t.get_executor(), async::use_blocked, 9,
				      3.5));
  ASSERT_EQ(9, i);
  ASSERT_EQ(3.5, j);
  EXPECT_NO_THROW(std::tie(i, j) = id(t.get_executor(), async::use_blocked[ec],
				      11, 2.25));
  EXPECT_FALSE(ec);
  ASSERT_EQ(11, i);
  ASSERT_EQ(2.25, j);
}

TEST(BlockedCompletion, AnError)
{
  context_thread t;
  asio::steady_timer timer(t.io_context(), 1s);
  sys::error_code ec;

  EXPECT_THROW(id(t.get_executor(), async::use_blocked,
		  sys::error_code{EDOM, sys::system_category()}),
	       sys::system_error);
  EXPECT_NO_THROW(id(t.get_executor(), async::use_blocked[ec],
		     sys::error_code{EDOM, sys::system_category()}));
  EXPECT_EQ(sys::error_code(EDOM, sys::system_category()), ec);

  EXPECT_THROW(id(t.get_executor(), async::use_blocked,
		  sys::error_code{EDOM, sys::system_category()}, 5),
	       sys::system_error);
  EXPECT_NO_THROW(id(t.get_executor(), async::use_blocked[ec],
		     sys::error_code{EDOM, sys::system_category()}, 5));
  EXPECT_EQ(sys::error_code(EDOM, sys::system_category()), ec);

  EXPECT_THROW(id(t.get_executor(), async::use_blocked,
		  sys::error_code{EDOM, sys::system_category()}, 5, 3),
	       sys::system_error);
  EXPECT_NO_THROW(id(t.get_executor(), async::use_blocked[ec],
		     sys::error_code{EDOM, sys::system_category()}, 5, 3));
  EXPECT_EQ(sys::error_code(EDOM, sys::system_category()), ec);
}

TEST(BlockedCompletion, MoveOnly)
{
  context_thread t;
  asio::steady_timer timer(t.io_context(), 1s);
  sys::error_code ec;


  EXPECT_NO_THROW(id(t.get_executor(), async::use_blocked,
			 sys::error_code{}, move_only{}));
  EXPECT_NO_THROW(id(t.get_executor(), async::use_blocked[ec],
		     sys::error_code{}, move_only{}));
  EXPECT_FALSE(ec);

  {
    auto [i, j] = id(t.get_executor(), async::use_blocked, move_only{}, 5);
    EXPECT_EQ(j, 5);
  }
  {
    auto [i, j] = id(t.get_executor(), async::use_blocked[ec], move_only{}, 5);
    EXPECT_EQ(j, 5);
  }
  EXPECT_FALSE(ec);


  EXPECT_THROW(id(t.get_executor(), async::use_blocked,
		  sys::error_code{EDOM, sys::system_category()}, move_only{}),
	       sys::system_error);
  EXPECT_NO_THROW(id(t.get_executor(), async::use_blocked[ec],
		     sys::error_code{EDOM, sys::system_category()}, move_only{}));
  EXPECT_EQ(sys::error_code(EDOM, sys::system_category()), ec);

  EXPECT_THROW(id(t.get_executor(), async::use_blocked,
		  sys::error_code{EDOM, sys::system_category()}, move_only{}, 3),
	       sys::system_error);
  EXPECT_NO_THROW(id(t.get_executor(), async::use_blocked[ec],
		     sys::error_code{EDOM, sys::system_category()},
		     move_only{}, 3));
  EXPECT_EQ(sys::error_code(EDOM, sys::system_category()), ec);
}

TEST(BlockedCompletion, DefaultLess)
{
  context_thread t;
  asio::steady_timer timer(t.io_context(), 1s);
  sys::error_code ec;


  {
    auto l = id(t.get_executor(), async::use_blocked, sys::error_code{}, defaultless{5});
    EXPECT_EQ(5, l.a);
  }
  {
    auto l = id(t.get_executor(), async::use_blocked[ec], sys::error_code{}, defaultless{7});
    EXPECT_EQ(7, l.a);
  }

  {
    auto [i, j] = id(t.get_executor(), async::use_blocked, defaultless{3}, 5);
    EXPECT_EQ(i.a, 3);
    EXPECT_EQ(j, 5);
  }
  {
    auto [i, j] = id(t.get_executor(), async::use_blocked[ec], defaultless{3}, 5);
    EXPECT_EQ(i.a, 3);
    EXPECT_EQ(j, 5);
  }
  EXPECT_FALSE(ec);

  EXPECT_THROW(id(t.get_executor(), async::use_blocked,
		  sys::error_code{EDOM, sys::system_category()}, move_only{}),
	       sys::system_error);
  EXPECT_NO_THROW(id(t.get_executor(), async::use_blocked[ec],
		     sys::error_code{EDOM, sys::system_category()}, move_only{}));
  EXPECT_EQ(sys::error_code(EDOM, sys::system_category()), ec);

  EXPECT_THROW(id(t.get_executor(), async::use_blocked,
		  sys::error_code{EDOM, sys::system_category()}, move_only{}, 3),
	       sys::system_error);
  EXPECT_NO_THROW(id(t.get_executor(), async::use_blocked[ec],
		     sys::error_code{EDOM, sys::system_category()},
		     move_only{}, 3));
  EXPECT_EQ(sys::error_code(EDOM, sys::system_category()), ec);
}
