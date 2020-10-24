// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */


#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>

#include <gtest/gtest.h>

#include "common/async/bind_handler.h"
#include "common/async/forward_handler.h"
#include "common/async/yield_context.h"

using namespace std::literals;

namespace ba = boost::asio;
namespace bs = boost::system;
namespace ca = ceph::async;
namespace s = spawn;

class context_thread {
  ba::io_context c;
  ba::executor_work_guard<ba::io_context::executor_type> guard;
  std::thread th;

public:
  context_thread() noexcept
    : guard(ba::make_work_guard(c)),
      th([this]() noexcept { c.run();}) {}

  ~context_thread() {
    guard.reset();
    th.join();
  }

  ba::io_context& io_context() noexcept {
    return c;
  }

  ba::io_context::executor_type get_executor() noexcept {
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
  ba::async_completion<CompletionToken, void(Args...)> init(token);
  auto a = ba::get_associated_allocator(init.completion_handler);
  executor.post(ca::forward_handler(
		  ca::bind_handler(std::move(init.completion_handler),
				   std::forward<Args>(args)...)),
		a);
  return init.result.get();
}

TEST(NullYieldCompletion, Void)
{
  context_thread t;

  ba::post(t.get_executor(), null_yield);
}

TEST(NullYieldCompletion, Timer)
{
  context_thread t;
  ba::steady_timer timer(t.io_context(), 50ms);
  timer.async_wait(null_yield);
}

TEST(NullYieldCompletion, NoError)
{
  context_thread t;
  ba::steady_timer timer(t.io_context(), 1s);
  bs::error_code ec;

  EXPECT_NO_THROW(id(t.get_executor(), null_yield, bs::error_code{}));
  EXPECT_NO_THROW(id(t.get_executor(), null_yield[ec], bs::error_code{}));
  EXPECT_FALSE(ec);

  int i;

  EXPECT_NO_THROW(i = id(t.get_executor(), null_yield,
			 bs::error_code{}, 5));
  ASSERT_EQ(5, i);
  EXPECT_NO_THROW(i = id(t.get_executor(), null_yield[ec],
			 bs::error_code{}, 7));
  EXPECT_FALSE(ec);
  ASSERT_EQ(7, i);

  float j;

  EXPECT_NO_THROW(std::tie(i, j) = id(t.get_executor(), null_yield, 9,
				      3.5));
  ASSERT_EQ(9, i);
  ASSERT_EQ(3.5, j);
  EXPECT_NO_THROW(std::tie(i, j) = id(t.get_executor(), null_yield[ec],
				      11, 2.25));
  EXPECT_FALSE(ec);
  ASSERT_EQ(11, i);
  ASSERT_EQ(2.25, j);
}

TEST(NullYieldCompletion, AnError)
{
  context_thread t;
  ba::steady_timer timer(t.io_context(), 1s);
  bs::error_code ec;

  EXPECT_THROW(id(t.get_executor(), null_yield,
		  bs::error_code{EDOM, bs::system_category()}),
	       bs::system_error);
  EXPECT_NO_THROW(id(t.get_executor(), null_yield[ec],
		     bs::error_code{EDOM, bs::system_category()}));
  EXPECT_EQ(bs::error_code(EDOM, bs::system_category()), ec);

  EXPECT_THROW(id(t.get_executor(), null_yield,
		  bs::error_code{EDOM, bs::system_category()}, 5),
	       bs::system_error);
  EXPECT_NO_THROW(id(t.get_executor(), null_yield[ec],
		     bs::error_code{EDOM, bs::system_category()}, 5));
  EXPECT_EQ(bs::error_code(EDOM, bs::system_category()), ec);

  EXPECT_THROW(id(t.get_executor(), null_yield,
		  bs::error_code{EDOM, bs::system_category()}, 5, 3),
	       bs::system_error);
  EXPECT_NO_THROW(id(t.get_executor(), null_yield[ec],
		     bs::error_code{EDOM, bs::system_category()}, 5, 3));
  EXPECT_EQ(bs::error_code(EDOM, bs::system_category()), ec);
}

TEST(NullYieldCompletion, MoveOnly)
{
  context_thread t;
  ba::steady_timer timer(t.io_context(), 1s);
  bs::error_code ec;


  EXPECT_NO_THROW(id(t.get_executor(), null_yield,
			 bs::error_code{}, move_only{}));
  EXPECT_NO_THROW(id(t.get_executor(), null_yield[ec],
		     bs::error_code{}, move_only{}));
  EXPECT_FALSE(ec);

  {
    auto [i, j] = id(t.get_executor(), null_yield, move_only{}, 5);
    EXPECT_EQ(j, 5);
  }
  {
    auto [i, j] = id(t.get_executor(), null_yield[ec], move_only{}, 5);
    EXPECT_EQ(j, 5);
  }
  EXPECT_FALSE(ec);


  EXPECT_THROW(id(t.get_executor(), null_yield,
		  bs::error_code{EDOM, bs::system_category()}, move_only{}),
	       bs::system_error);
  EXPECT_NO_THROW(id(t.get_executor(), null_yield[ec],
		     bs::error_code{EDOM, bs::system_category()}, move_only{}));
  EXPECT_EQ(bs::error_code(EDOM, bs::system_category()), ec);

  EXPECT_THROW(id(t.get_executor(), null_yield,
		  bs::error_code{EDOM, bs::system_category()}, move_only{}, 3),
	       bs::system_error);
  EXPECT_NO_THROW(id(t.get_executor(), null_yield[ec],
		     bs::error_code{EDOM, bs::system_category()},
		     move_only{}, 3));
  EXPECT_EQ(bs::error_code(EDOM, bs::system_category()), ec);
}

TEST(NullYieldCompletion, DefaultLess)
{
  context_thread t;
  ba::steady_timer timer(t.io_context(), 1s);
  bs::error_code ec;


  {
    auto l = id(t.get_executor(), null_yield, bs::error_code{}, defaultless{5});
    EXPECT_EQ(5, l.a);
  }
  {
    auto l = id(t.get_executor(), null_yield[ec], bs::error_code{}, defaultless{7});
    EXPECT_EQ(7, l.a);
  }

  {
    auto [i, j] = id(t.get_executor(), null_yield, defaultless{3}, 5);
    EXPECT_EQ(i.a, 3);
    EXPECT_EQ(j, 5);
  }
  {
    auto [i, j] = id(t.get_executor(), null_yield[ec], defaultless{3}, 5);
    EXPECT_EQ(i.a, 3);
    EXPECT_EQ(j, 5);
  }
  EXPECT_FALSE(ec);

  EXPECT_THROW(id(t.get_executor(), null_yield,
		  bs::error_code{EDOM, bs::system_category()}, move_only{}),
	       bs::system_error);
  EXPECT_NO_THROW(id(t.get_executor(), null_yield[ec],
		     bs::error_code{EDOM, bs::system_category()}, move_only{}));
  EXPECT_EQ(bs::error_code(EDOM, bs::system_category()), ec);

  EXPECT_THROW(id(t.get_executor(), null_yield,
		  bs::error_code{EDOM, bs::system_category()}, move_only{}, 3),
	       bs::system_error);
  EXPECT_NO_THROW(id(t.get_executor(), null_yield[ec],
		     bs::error_code{EDOM, bs::system_category()},
		     move_only{}, 3));
  EXPECT_EQ(bs::error_code(EDOM, bs::system_category()), ec);
}



TEST(NonEmptyOptionalYieldCompletion, Void)
{
  ba::io_context c;
  s::spawn(c, [&](s::yield_context _y) {
    optional_yield y(c, _y);
    ba::post(c, y);
  });
  c.run();
}

TEST(NonEmptyOptionalYieldCompletion, Timer)
{
  ba::io_context c;
  s::spawn(c, [&](s::yield_context _y) {
    optional_yield y(c, _y);
    ba::steady_timer timer(c, 50ms);
    timer.async_wait(y);
  });
  c.run();
}

TEST(NonEmptyOptionalYieldCompletion, NoError)
{
  ba::io_context c;
  s::spawn(c, [&](s::yield_context _y) {
    optional_yield y(c, _y);
    ba::steady_timer timer(c, 1s);
    bs::error_code ec;

    EXPECT_NO_THROW(id(c.get_executor(), y, bs::error_code{}));
    EXPECT_NO_THROW(id(c.get_executor(), y[ec], bs::error_code{}));
    EXPECT_FALSE(ec);

    int i;

    EXPECT_NO_THROW(i = id(c.get_executor(), y,
			   bs::error_code{}, 5));
    ASSERT_EQ(5, i);
    EXPECT_NO_THROW(i = id(c.get_executor(), y[ec],
			   bs::error_code{}, 7));
    EXPECT_FALSE(ec);
    ASSERT_EQ(7, i);

    float j;

    EXPECT_NO_THROW(std::tie(i, j) = id(c.get_executor(), y, 9,
					3.5));
    ASSERT_EQ(9, i);
    ASSERT_EQ(3.5, j);
    EXPECT_NO_THROW(std::tie(i, j) = id(c.get_executor(), y[ec],
					11, 2.25));
    EXPECT_FALSE(ec);
    ASSERT_EQ(11, i);
    ASSERT_EQ(2.25, j);
  });
  c.run();
}

TEST(NonEmptyOptionalYieldCompletion, AnError)
{
  ba::io_context c;
  s::spawn(c, [&](s::yield_context _y) {
    optional_yield y(c, _y);
    ba::steady_timer timer(c, 1s);
    bs::error_code ec;

    EXPECT_THROW(id(c.get_executor(), y,
		    bs::error_code{EDOM, bs::system_category()}),
		 bs::system_error);
    EXPECT_NO_THROW(id(c.get_executor(), y[ec],
		       bs::error_code{EDOM, bs::system_category()}));
    EXPECT_EQ(bs::error_code(EDOM, bs::system_category()), ec);

    EXPECT_THROW(id(c.get_executor(), y,
		    bs::error_code{EDOM, bs::system_category()}, 5),
		 bs::system_error);
    EXPECT_NO_THROW(id(c.get_executor(), y[ec],
		       bs::error_code{EDOM, bs::system_category()}, 5));
    EXPECT_EQ(bs::error_code(EDOM, bs::system_category()), ec);

    EXPECT_THROW(id(c.get_executor(), y,
		    bs::error_code{EDOM, bs::system_category()}, 5, 3),
		 bs::system_error);
    EXPECT_NO_THROW(id(c.get_executor(), y[ec],
		       bs::error_code{EDOM, bs::system_category()}, 5, 3));
    EXPECT_EQ(bs::error_code(EDOM, bs::system_category()), ec);
  });
  c.run();
}

TEST(NonEmptyOptionalYieldCompletion, MoveOnly)
{
  ba::io_context c;
  s::spawn(c, [&](s::yield_context _y) {
    optional_yield y(c, _y);
    ba::steady_timer timer(c, 1s);
    bs::error_code ec;


    EXPECT_NO_THROW(id(c.get_executor(), y,
		       bs::error_code{}, move_only{}));
    EXPECT_NO_THROW(id(c.get_executor(), y[ec],
		       bs::error_code{}, move_only{}));
    EXPECT_FALSE(ec);

    {
      auto [i, j] = id(c.get_executor(), y, move_only{}, 5);
      EXPECT_EQ(j, 5);
    }
    {
      auto [i, j] = id(c.get_executor(), y[ec], move_only{}, 5);
      EXPECT_EQ(j, 5);
    }
    EXPECT_FALSE(ec);


    EXPECT_THROW(id(c.get_executor(), y,
		    bs::error_code{EDOM, bs::system_category()}, move_only{}),
		 bs::system_error);
    EXPECT_NO_THROW(id(c.get_executor(), y[ec],
		       bs::error_code{EDOM, bs::system_category()}, move_only{}));
    EXPECT_EQ(bs::error_code(EDOM, bs::system_category()), ec);

    EXPECT_THROW(id(c.get_executor(), y,
		    bs::error_code{EDOM, bs::system_category()}, move_only{}, 3),
		 bs::system_error);
    EXPECT_NO_THROW(id(c.get_executor(), y[ec],
		       bs::error_code{EDOM, bs::system_category()},
		       move_only{}, 3));
    EXPECT_EQ(bs::error_code(EDOM, bs::system_category()), ec);
  });
  c.run();
}

TEST(NonEmptyOptionalYieldCompletion, DefaultLess)
{
  ba::io_context c;
  s::spawn(c, [&](s::yield_context _y) {
    optional_yield y(c, _y);
    ba::steady_timer timer(c, 1s);
    bs::error_code ec;

    {
      auto l = id(c.get_executor(), y, bs::error_code{}, defaultless{5});
      EXPECT_EQ(5, l.a);
    }
    {
      auto l = id(c.get_executor(), y[ec], bs::error_code{}, defaultless{7});
      EXPECT_EQ(7, l.a);
    }

    {
      auto [i, j] = id(c.get_executor(), y, defaultless{3}, 5);
      EXPECT_EQ(i.a, 3);
      EXPECT_EQ(j, 5);
    }
    {
      auto [i, j] = id(c.get_executor(), y[ec], defaultless{3}, 5);
      EXPECT_EQ(i.a, 3);
      EXPECT_EQ(j, 5);
    }
    EXPECT_FALSE(ec);

    EXPECT_THROW(id(c.get_executor(), y,
		    bs::error_code{EDOM, bs::system_category()}, move_only{}),
		 bs::system_error);
    EXPECT_NO_THROW(id(c.get_executor(), y[ec],
		       bs::error_code{EDOM, bs::system_category()}, move_only{}));
    EXPECT_EQ(bs::error_code(EDOM, bs::system_category()), ec);

    EXPECT_THROW(id(c.get_executor(), y,
		    bs::error_code{EDOM, bs::system_category()}, move_only{}, 3),
		 bs::system_error);
    EXPECT_NO_THROW(id(c.get_executor(), y[ec],
		       bs::error_code{EDOM, bs::system_category()},
		       move_only{}, 3));
    EXPECT_EQ(bs::error_code(EDOM, bs::system_category()), ec);
  });
}

TEST(OptionalYield, Check) {
  bool triggered = false;

  optional_yield::set_checker(
    [&triggered]() {
      triggered = true;
    });
  {
    context_thread t;
    ASSERT_FALSE(triggered);
    ba::post(t.get_executor(), null_yield);
    EXPECT_TRUE(triggered);
  }
  triggered = false;
  {
    ba::io_context c;
    s::spawn(c, [&](s::yield_context _y) {
      optional_yield y(c, _y);
      ASSERT_FALSE(triggered);
      ba::post(c.get_executor(), y);
      EXPECT_FALSE(triggered);
    });
    c.run();
  }
  optional_yield::set_checker(nullptr);
}
