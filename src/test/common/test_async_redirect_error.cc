// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 */

// Based on libs/asio/test/redirect_error.cpp which is
// Copyright (c) 2003-2024 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See
// accompanying copy at http://www.boost.org/LICENSE_1_0.txt)

#include "common/async/redirect_error.h"

#include <exception>
#include <future>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/system_timer.hpp>
#include <boost/asio/use_future.hpp>

#include <gtest/gtest.h>

struct redirect_error_handler
{
  int* count;

  explicit redirect_error_handler(int* c)
    : count(c) {}

  void operator ()()
  {
    ++(*count);
  }
};

TEST(RedirectError, RedirectError)
{
  boost::asio::io_context io1;
  boost::asio::io_context io2;
  boost::asio::system_timer timer1(io1);
  boost::system::error_code ec = boost::asio::error::would_block;
  int count = 0;

  timer1.expires_after(boost::asio::chrono::seconds(0));
  timer1.async_wait(
      ceph::async::redirect_error(
        boost::asio::bind_executor(io2.get_executor(),
          redirect_error_handler(&count)), ec));

  ASSERT_EQ(boost::asio::error::would_block, ec);
  ASSERT_EQ(0, count);

  io1.run();

  ASSERT_EQ(boost::asio::error::would_block, ec);
  ASSERT_EQ(0, count);

  io2.run();

  ASSERT_FALSE(ec);
  ASSERT_EQ(1, count);

  ec = boost::asio::error::would_block;
  timer1.async_wait(
      ceph::async::redirect_error(
        boost::asio::bind_executor(io2.get_executor(),
          boost::asio::deferred), ec))(redirect_error_handler(&count));

  ASSERT_EQ(boost::asio::error::would_block, ec);
  ASSERT_EQ(1, count);

  io1.restart();
  io1.run();

  ASSERT_EQ(boost::asio::error::would_block, ec);
  ASSERT_EQ(1, count);

  io2.restart();
  io2.run();

  ASSERT_FALSE(ec);
  ASSERT_EQ(2, count);

  ec = boost::asio::error::would_block;
  std::future<void> f = timer1.async_wait(
      ceph::async::redirect_error(
        boost::asio::bind_executor(io2.get_executor(),
          boost::asio::use_future), ec));

  ASSERT_EQ(boost::asio::error::would_block, ec);
  ASSERT_EQ(std::future_status::timeout, f.wait_for(std::chrono::seconds(0)));

  io1.restart();
  io1.run();

  ASSERT_EQ(boost::asio::error::would_block, ec);
  ASSERT_EQ(std::future_status::timeout, f.wait_for(std::chrono::seconds(0)));

  io2.restart();
  io2.run();

  ASSERT_FALSE(ec);
  ASSERT_EQ(std::future_status::ready, f.wait_for(std::chrono::seconds(0)));
}

TEST(RedirectError, RedirectErrorExceptionPtr)
{
  boost::asio::io_context io;
  std::exception_ptr eptr = nullptr;

  int count = 0;

  boost::asio::co_spawn(
    io, []() -> boost::asio::awaitable<void> {
      throw std::exception{};
      co_return;
    },
    ceph::async::redirect_error(
      boost::asio::bind_executor(io.get_executor(),
				 redirect_error_handler(&count)), eptr));

  ASSERT_FALSE(eptr);
  ASSERT_EQ(0, count);

  io.run();

  ASSERT_TRUE(eptr);
  ASSERT_EQ(1, count);

  boost::asio::co_spawn(
    io, []() -> boost::asio::awaitable<void> {
      co_return;
    },
    ceph::async::redirect_error(
      boost::asio::bind_executor(io.get_executor(),
				 redirect_error_handler(&count)), eptr));
  ASSERT_TRUE(eptr);
  ASSERT_EQ(1, count);

  io.restart();
  io.run();

  ASSERT_FALSE(eptr);
  ASSERT_EQ(2, count);
}

TEST(RedirectError, PartialRedirectError)
{
  boost::asio::io_context io1;
  boost::asio::io_context io2;
  boost::asio::system_timer timer1(io1);
  boost::system::error_code ec = boost::asio::error::would_block;
  int count = 0;

  timer1.expires_after(boost::asio::chrono::seconds(0));
  timer1.async_wait(ceph::async::redirect_error(ec))(
      boost::asio::bind_executor(io2.get_executor(),
        redirect_error_handler(&count)));

  ASSERT_EQ(boost::asio::error::would_block, ec);
  ASSERT_EQ(0, count);

  io1.run();

  ASSERT_EQ(boost::asio::error::would_block, ec);
  ASSERT_EQ(0, count);

  io2.run();

  ASSERT_FALSE(ec);
  ASSERT_EQ(1, count);

  ec = boost::asio::error::would_block;
  timer1.async_wait(ceph::async::redirect_error(ec))(
      boost::asio::bind_executor(io2.get_executor(),
        boost::asio::deferred))(redirect_error_handler(&count));

  ASSERT_EQ(boost::asio::error::would_block, ec);
  ASSERT_EQ(1, count);

  io1.restart();
  io1.run();

  ASSERT_EQ(boost::asio::error::would_block, ec);
  ASSERT_EQ(1, count);

  io2.restart();
  io2.run();

  ASSERT_FALSE(ec);
  ASSERT_EQ(2, count);

  ec = boost::asio::error::would_block;
  timer1.async_wait()(ceph::async::redirect_error(ec))(
      boost::asio::bind_executor(io2.get_executor(),
        boost::asio::deferred))(redirect_error_handler(&count));

  ASSERT_EQ(boost::asio::error::would_block, ec);
  ASSERT_EQ(2, count);

  io1.restart();
  io1.run();

  ASSERT_EQ(boost::asio::error::would_block, ec);
  ASSERT_EQ(2, count);

  io2.restart();
  io2.run();

  ASSERT_FALSE(ec);
  ASSERT_EQ(3, count);

  ec = boost::asio::error::would_block;
  std::future<void> f = timer1.async_wait(ceph::async::redirect_error(ec))(
      boost::asio::bind_executor(io2.get_executor(), boost::asio::use_future));

  ASSERT_EQ(boost::asio::error::would_block, ec);
  EXPECT_EQ(std::future_status::timeout, f.wait_for(std::chrono::seconds(0)));

  io1.restart();
  io1.run();

  EXPECT_EQ(boost::asio::error::would_block, ec);
  EXPECT_EQ(std::future_status::timeout, f.wait_for(std::chrono::seconds(0)));

  io2.restart();
  io2.run();

  EXPECT_FALSE(ec);
  EXPECT_EQ(std::future_status::ready, f.wait_for(std::chrono::seconds(0)));
}

TEST(RedirectError, PartialRedirectErrorExceptionPtr)
{
  boost::asio::io_context io;
  std::exception_ptr eptr = nullptr;

  int count = 0;

  boost::asio::co_spawn(
    io, []() -> boost::asio::awaitable<void> {
      throw std::exception{};
      co_return;
    },
    ceph::async::redirect_error(eptr)(boost::asio::bind_executor(
					io.get_executor(),
					redirect_error_handler(&count))));

  ASSERT_FALSE(eptr);
  ASSERT_EQ(0, count);

  io.run();

  ASSERT_TRUE(eptr);
  ASSERT_EQ(1, count);

  boost::asio::co_spawn(
    io, []() -> boost::asio::awaitable<void> {
      co_return;
    },
    ceph::async::redirect_error(eptr)(boost::asio::bind_executor(
					io.get_executor(),
					redirect_error_handler(&count))));
  ASSERT_TRUE(eptr);
  ASSERT_EQ(1, count);

  io.restart();
  io.run();

  ASSERT_FALSE(eptr);
  ASSERT_EQ(2, count);
}
