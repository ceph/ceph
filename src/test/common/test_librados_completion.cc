// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2024 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/async/librados_completion.h"

#include <boost/system/detail/errc.hpp>
#include <gtest/gtest.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <boost/system/error_code.hpp>
#include <boost/system/errc.hpp>
#include <boost/system/system_error.hpp>

#include "include/rados/librados.hpp"

#include "common/async/async_call.h"

namespace asio = boost::asio;
namespace sys = boost::system;
namespace async = ceph::async;

TEST(CoroSucc, AioComplete)
{
  auto lrc = librados::Rados::aio_create_completion();
  asio::io_context c;
  asio::co_spawn(c.get_executor(),
                 []() -> asio::awaitable<void> {
                   co_return;
                 }(), lrc);
  c.run();
  lrc->wait_for_complete();
  auto r = lrc->get_return_value();
  ASSERT_EQ(0, r);
}

TEST(CoroExcept, AioComplete)
{
  auto lrc = librados::Rados::aio_create_completion();
  asio::io_context c;
  asio::co_spawn(c.get_executor(),
                 []() -> asio::awaitable<void> {
                   throw sys::system_error{ENOENT, sys::generic_category()};
                   co_return;
                 }(), lrc);
  c.run();
  lrc->wait_for_complete();
  auto r = lrc->get_return_value();
  ASSERT_EQ(-ENOENT, r);
}

TEST(CoroUnknownExcept, AioComplete)
{
  auto lrc = librados::Rados::aio_create_completion();
  asio::io_context c;
  asio::co_spawn(c.get_executor(),
                 []() -> asio::awaitable<void> {
                   throw std::exception{};
                   co_return;
                 }(), lrc);
  c.run();
  lrc->wait_for_complete();
  auto r = lrc->get_return_value();
  ASSERT_EQ(-EIO, r);
}

TEST(Int, AioComplete)
{
  auto lrc = librados::Rados::aio_create_completion();
  asio::io_context c;
  async::async_dispatch(c.get_executor(),
                        []() {
                         return -42;
                       }, lrc);
  c.run();
  lrc->wait_for_complete();
  auto r = lrc->get_return_value();
  ASSERT_EQ(-42, r);
}

TEST(EC, AioComplete)
{
  auto lrc = librados::Rados::aio_create_completion();
  asio::io_context c;
  async::async_dispatch(c.get_executor(),
                        []() {
                          return sys::error_code(ENOENT,
                                                 sys::generic_category());
                        }, lrc);
  c.run();
  lrc->wait_for_complete();
  auto r = lrc->get_return_value();
  ASSERT_EQ(-ENOENT, r);
}
