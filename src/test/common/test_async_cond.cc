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

#include "common/async/async_cond.h"

#include <coroutine>

#include <boost/asio/io_context.hpp>

#include <gtest/gtest.h>

namespace asio = boost::asio;
namespace sys = boost::system;

namespace async = ceph::async;

enum response : int {
  error, success
};

std::mutex m;

struct waiter {
  std::unique_lock<std::mutex> l{m};
  int* i;

  waiter(int* i) : i(i) {}

  void operator ()(sys::error_code ec) {
    EXPECT_TRUE(l.owns_lock());
    *i = ec ? error : success;
    l.unlock();
    delete this;
  }
};


TEST(async_cond, lambdata)
{
  asio::io_context io_context;
  async::async_cond cond(io_context.get_executor());
  std::array<int, 5> data;
  data.fill(0xdeadbeef);


  for (auto i = 0; i < std::ssize(data); ++i) {
    auto c = new waiter(data.data() + i);
    cond.wait(c->l, std::ref(*c));
  }
  cond.notify();
  io_context.run();
  for (const auto& d : data) {
    ASSERT_EQ(success, d);
  }
}

TEST(async_cond, lambdataReset)
{
  asio::io_context io_context;
  async::async_cond cond(io_context.get_executor());
  std::array<int, 5> data;
  data.fill(0xdeadbeef);

  for (auto i = 0; i < std::ssize(data); ++i) {
    auto c = new waiter(data.data() + i);
    cond.wait(c->l, std::ref(*c));
  }
  cond.reset();
  io_context.run();
  for (const auto& d : data) {
    ASSERT_EQ(error, d);
  }
}

TEST(async_cond, lambdataAlreadyComplete)
{
  asio::io_context io_context;
  async::async_cond cond(io_context.get_executor());
  std::array<int, 5> data;
  data.fill(0xdeadbeef);

  cond.notify();
  for (auto i = 0; i < std::ssize(data); ++i) {
    auto c = new waiter(data.data() + i);
    cond.wait(c->l, std::ref(*c));
  }
  io_context.run();
  for (const auto& d : data) {
    ASSERT_EQ(success, d);
  }
}
