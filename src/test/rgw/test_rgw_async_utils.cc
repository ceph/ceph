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

#include "rgw/async_utils.h"

#include <cerrno>
#include <memory>
#include <string>
#include <tuple>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/spawn.hpp>

#include <boost/system/generic_category.hpp>
#include <boost/system/system_error.hpp>

#include <gtest/gtest.h>

#include "common/async/context_pool.h"
#include "common/async/yield_context.h"

#include <common/ceph_context.h>
#include <common/dout.h>

namespace asio = boost::asio;
namespace async = ceph::async;
namespace sys = boost::system;

static auto cct = std::make_unique<CephContext>(CEPH_ENTITY_TYPE_ANY);

static NoDoutPrefix dp(cct.get(), ceph_subsys_rgw);

TEST(CoSpawn, CoSpawn) {
  async::io_context_pool pool{3};

  auto maybethrow = [](int code) -> asio::awaitable<void> {
    if (code != 0) {
      throw sys::system_error{code, sys::generic_category()};
    }
    co_return;
  };

  int r = 0;
  r = rgw::run_coro(&dp, pool, maybethrow(ENOENT), nullptr);
  ASSERT_EQ(-ENOENT, r);

  r = rgw::run_coro(&dp, pool, maybethrow(0), nullptr);
  ASSERT_EQ(0, r);

  asio::spawn(pool,
              [&](asio::yield_context y) -> void {
                r = rgw::run_coro(&dp, pool, maybethrow(ENOENT), "yielding",
                                  y);
              },
              async::use_blocked);
  ASSERT_EQ(-ENOENT, r);

  asio::spawn(pool,
              [&](asio::yield_context y) -> void {
                r = rgw::run_coro(&dp, pool, maybethrow(0), "yielding",
                                  y);
              },
              async::use_blocked);
  ASSERT_EQ(0, r);

  r = rgw::run_coro(&dp, pool, maybethrow(ENOENT), "blocking",
                    null_yield);
  ASSERT_EQ(-ENOENT, r);

  r = rgw::run_coro(&dp, pool, maybethrow(0), "blocking",
                    null_yield);
  ASSERT_EQ(0, r);

  auto maybethrowv = []<typename V>(int code, V v) -> asio::awaitable<V> {
    if (code != 0) {
      throw sys::system_error{code, sys::generic_category()};
    }
    co_return std::move(v);
  };

  const std::string instr("foo");
  std::string s;

  r = rgw::run_coro(&dp, pool, maybethrowv(ENOENT, instr),
                    s, nullptr);
  ASSERT_EQ(-ENOENT, r);
  ASSERT_TRUE(s.empty());

  r = rgw::run_coro(&dp, pool, maybethrowv(0, instr), s, nullptr);
  ASSERT_EQ(0, r);
  ASSERT_EQ(instr, s);

  s.clear();

  asio::spawn(pool,
              [&](asio::yield_context y) -> void {
                r = rgw::run_coro(&dp, pool, maybethrowv(ENOENT, instr),
                                  s, "yielding", y);
              },
              async::use_blocked);
  ASSERT_EQ(-ENOENT, r);
  ASSERT_TRUE(s.empty());

  asio::spawn(pool,
              [&](asio::yield_context y) -> void {
                r = rgw::run_coro(&dp, pool, maybethrowv(0, instr),
                                  s, "yielding", y);
              },
              async::use_blocked);
  ASSERT_EQ(0, r);
  ASSERT_EQ(instr, s);

  s.clear();
  r = rgw::run_coro(&dp, pool, maybethrowv(ENOENT, instr), s,
                    "blocking", null_yield);
  ASSERT_EQ(-ENOENT, r);
  ASSERT_TRUE(s.empty());

  r = rgw::run_coro(&dp, pool, maybethrowv(0, instr), s,
                    "blocking", null_yield);
  ASSERT_EQ(0, r);
  ASSERT_EQ(instr, s);

  auto maybethrowvs = []<typename ...Vs>(int code, Vs ...vs)
    -> asio::awaitable<std::tuple<Vs...>> {
    if (code != 0) {
      throw sys::system_error{code, sys::generic_category()};
    }
    co_return std::make_tuple(std::move(vs)...);
  };

  s.clear();
  std::unique_ptr<int> p;

  r = rgw::run_coro(&dp, pool, maybethrowvs(ENOENT, instr,
					    std::make_unique<int>(5)),
                    std::tie(s, p), nullptr);
  ASSERT_EQ(-ENOENT, r);
  ASSERT_TRUE(s.empty());
  ASSERT_FALSE(p);

  r = rgw::run_coro(&dp, pool, maybethrowvs(0, instr,
					    std::make_unique<int>(5)),
                    std::tie(s, p), nullptr);
  ASSERT_EQ(0, r);
  ASSERT_EQ(instr, s);
  ASSERT_TRUE(p);
  ASSERT_EQ(5, *p);

  s.clear();
  p.reset();

  asio::spawn(pool,
              [&](asio::yield_context y) -> void {
                r = rgw::run_coro(&dp, pool,
				  maybethrowvs(ENOENT, instr,
					       std::make_unique<int>(5)),
                                  std::tie(s, p), "yielding", y);
              },
              async::use_blocked);
  ASSERT_EQ(-ENOENT, r);
  ASSERT_TRUE(s.empty());
  ASSERT_FALSE(p);

  asio::spawn(pool,
              [&](asio::yield_context y) -> void {
                r = rgw::run_coro(&dp, pool,
				  maybethrowvs(0, instr,
					       std::make_unique<int>(5)),
                                  std::tie(s, p), "yielding", y);
              },
              async::use_blocked);
  ASSERT_EQ(0, r);
  ASSERT_EQ(instr, s);
  ASSERT_TRUE(p);
  ASSERT_EQ(5, *p);

  s.clear();
  p.reset();
  r = rgw::run_coro(&dp, pool, maybethrowvs(ENOENT, instr,
					    std::make_unique<int>(5)),
		    std::tie(s, p), "blocking", null_yield);
  ASSERT_EQ(-ENOENT, r);
  ASSERT_TRUE(s.empty());
  ASSERT_FALSE(p);

  r = rgw::run_coro(&dp, pool, maybethrowvs(0, instr,
					    std::make_unique<int>(5)),
		    std::tie(s, p), "blocking", null_yield);
  ASSERT_EQ(0, r);
  ASSERT_EQ(instr, s);
  ASSERT_TRUE(p);
  ASSERT_EQ(5, *p);
}
