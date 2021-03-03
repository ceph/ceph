// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#define BOOST_COROUTINES_NO_DEPRECATION_WARNING
#include <chrono>
#include <future>

#include <boost/range/begin.hpp>
#include <boost/range/end.hpp>

#include <boost/asio.hpp>
#include <boost/asio/spawn.hpp>
#include <gtest/gtest.h>

#include "common/async/context_pool.h"
#include "common/async/gather_completion.h"
using namespace std::literals;

namespace ca = ceph::async;
namespace ba = boost::asio;

TEST(GatherCompletion, Future)
{
  ca::io_context_pool c(1);
  auto gc = ca::make_gather_completion(c.get_executor(), ba::use_future);

  auto c1 = gc.completion_minor();
  auto c2 = gc.completion_minor();
  auto c3 = gc.completion_minor();
  auto f = gc.complete();

  ASSERT_TRUE(f.valid());
  ASSERT_NE(std::future_status::ready, f.wait_for(1ns));
  c1();

  ASSERT_TRUE(f.valid());
  ASSERT_NE(std::future_status::ready, f.wait_for(1ns));
  c2();

  ASSERT_TRUE(f.valid());
  ASSERT_NE(std::future_status::ready, f.wait_for(1ns));
  c3();

  ASSERT_TRUE(f.valid());

  f.get();
  c.finish();
}

TEST(GatherCompletion, Future2)
{
  ca::io_context_pool c(1);
  auto gc = ca::make_gather_completion(c.get_executor(), ba::use_future);
  auto c1 = gc.completion_minor();
  auto c2 = gc.completion_minor();
  auto c3 = gc.completion_minor();
  c1();
  c2();
  c3();
  auto f = gc.complete();
  f.get();
  c.finish();
}

TEST(GatherCompletion, Future3)
{
  ca::io_context_pool c(1);
  auto gc = ca::make_gather_completion<void(bool, const std::string&)>(
    c.get_executor(), ba::use_future,
    [](int n, bool b, const std::string& s) {
      if (b)
	return int(n + s.length());
      else
	return n;
    }, int(0));
  auto c1 = gc.completion_minor();
  auto c2 = gc.completion_minor();
  auto c3 = gc.completion_minor();
  c1(true, "abc"s);
  c2(false, "def"s);
  c3(true, "ghi"s);
  auto f = gc.complete();
  auto n = f.get();
  ASSERT_EQ(6, n);
  c.finish();
}

TEST(GatherCompletion, Coroutine)
{
  ba::io_context c;
  ba::spawn(
    [&c](ba::yield_context y) {
      auto gc = ca::make_gather_completion(c.get_executor(), y);
      auto c1 = gc.completion_minor();
      auto c2 = gc.completion_minor();
      auto c3 = gc.completion_minor();
      c1();
      c2();
      c3();
      gc.complete();
    });
  c.run();
}

namespace {
int sum(int a, int b) {
  return a + b;
}
}

TEST(GatherCompletion, Coroutine2)
{
  ba::io_context c;
  ba::spawn(
    [&c](ba::yield_context y) {
      auto gc = ca::make_gather_completion(c.get_executor(), y, &sum, 0);
      auto c1 = gc.completion_minor();
      auto c2 = gc.completion_minor();
      auto c3 = gc.completion_minor();
      c1(1);
      c2(2);
      c3(3);
      auto n = gc.complete();
      ASSERT_EQ(6, n);
    });
  c.run();
}
