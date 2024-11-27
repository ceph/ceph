// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM
 *
 * See file COPYING for license information.
 *
 */

#include <coroutine>
#include <cstdint>
#include <tuple>
#include <utility>
#include <vector>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <boost/container/flat_set.hpp>

#include <fmt/format.h>

#include "include/neorados/RADOS.hpp"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

namespace asio = boost::asio;
namespace container = boost::container;

using namespace std::literals;

using neorados::Cursor;
using neorados::IOContext;
using neorados::WriteOp;

using Entries = std::vector<neorados::Entry>;
using REntries = container::flat_set<neorados::Entry>;

CORO_TEST_F(NeoradosList, ListObjects, NeoRadosTest) {
  static constexpr auto oid = "foo";
  co_await execute(oid, WriteOp{}.create(true));
  auto [entries, cursor] = co_await
    rados().enumerate_objects(pool(), Cursor::begin(), Cursor::end(), 1'000, {},
			      asio::use_awaitable);

  EXPECT_EQ(1, entries.size());
  EXPECT_EQ(oid, entries.front().oid);
  co_return;
}


asio::awaitable<void> populate(neorados::RADOS& rados, const IOContext& pool,
			       const REntries& entries) {
  for (const auto& entry : entries) {
    co_await ::create_obj(rados, entry.oid, pool, asio::use_awaitable);
  }
  co_return;
};

void compare(const REntries& ref, const Entries& res) {
  EXPECT_EQ(ref.size(), res.size());
  for (const auto& e : res) {
    EXPECT_TRUE(ref.contains(e));
  }
  return;
};

CORO_TEST_F(NeoradosList, ListObjectsNS, NeoRadosTest) {
  auto pdef = pool();
  IOContext p1{pool().get_pool(), "ns1"};
  IOContext p2{pool().get_pool(), "ns2"};
  IOContext pall{pool().get_pool(), neorados::all_nspaces};

  neorados::Entry meow{.oid="foo1"s};
  REntries def{
    {.oid = "foo1"s},
    {.oid = "foo2"s},
    {.oid = "foo3"s}
  };
  REntries ns1{
    {.nspace = "ns1"s, .oid = "foo1"s},
    {.nspace = "ns1"s, .oid = "foo4"s},
    {.nspace = "ns1"s, .oid = "foo5"s},
    {.nspace = "ns1"s, .oid = "foo6"s},
    {.nspace = "ns1"s, .oid = "foo7"s}
  };
  REntries ns2{
    {.nspace = "ns2"s, .oid = "foo6"s},
    {.nspace = "ns2"s, .oid = "foo7"s}
  };
  REntries all{def};
  all.insert(ns1.begin(), ns1.end());
  all.insert(ns2.begin(), ns2.end());

  co_await populate(rados(), pdef, def);
  co_await populate(rados(), p1, ns1);
  co_await populate(rados(), p2, ns2);

  auto [resdef, cdef] = co_await
    rados().enumerate_objects(pdef, Cursor::begin(), Cursor::end(), 1'000, {},
			      asio::use_awaitable);
  auto [res1, c1] = co_await
    rados().enumerate_objects(p1, Cursor::begin(), Cursor::end(), 1'000, {},
			      asio::use_awaitable);
  auto [res2, c2] = co_await
    rados().enumerate_objects(p2, Cursor::begin(), Cursor::end(), 1'000, {},
			      asio::use_awaitable);
  auto [resall, call] = co_await
    rados().enumerate_objects(pall, Cursor::begin(), Cursor::end(), 1'000, {},
			      asio::use_awaitable);

  compare(def, resdef);
  compare(ns1, res1);
  compare(ns2, res2);
  compare(all, resall);

  co_return;
}

CORO_TEST_F(NeoradosList, ListObjectsMany, NeoRadosTest) {
  REntries ref;
  for (auto i = 0u; i < 512; ++i) {
    ref.insert({.oid = fmt::format("{:0>3}", i)});
  }
  co_await populate(rados(), pool(), ref);
  REntries res;
  {
    Cursor c;
    Entries e;
    static constexpr auto per = 10;
    e.reserve(per);
    while (c != Cursor::end()) {
      std::tie(e, c) = co_await
	rados().enumerate_objects(pool(), c, Cursor::end(), per, {},
				  asio::use_awaitable);
      for (auto&& n : e) {
	res.insert(std::move(n));
      }
      e.clear();
    }
  }
  EXPECT_EQ(ref, res);

  co_return;
}

// Sadly I don't think there's a good way to templatize testcases over
// fixture.

