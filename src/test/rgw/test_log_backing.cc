// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "rgw_log_backing.h"

#include <coroutine>
#include <iostream>
#include <string_view>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <boost/system/errc.hpp>
#include <boost/system/error_code.hpp>

#include <fmt/format.h>

#include "include/neorados/RADOS.hpp"

#include "neorados/cls/fifo.h"
#include "neorados/cls/log.h"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

namespace asio = boost::asio;
namespace buffer = ceph::buffer;

namespace fifo = neorados::cls::fifo;
namespace logn = neorados::cls::log;

namespace {
inline constexpr int SHARDS = 3;
std::string get_oid(uint64_t gen_id, int i) {
  return (gen_id > 0 ?
	  fmt::format("shard@G{}.{}", gen_id, i) :
	  fmt::format("shard.{}", i));
}

asio::awaitable<void> make_omap(neorados::RADOS& rados,
				const neorados::IOContext& loc) {
  for (int i = 0; i < SHARDS; ++i) {
    using ceph::encode;
    neorados::WriteOp op;
    buffer::list bl;
    encode(i, bl);
    op.exec(logn::add(ceph::real_clock::now(), {}, "meow", std::move(bl)));
    co_await rados.execute(get_oid(0, i), loc, std::move(op),
			   asio::use_awaitable);
  }
  co_return;
}

asio::awaitable<void> make_fifo(const DoutPrefixProvider* dpp,
				neorados::RADOS& rados,
                                const neorados::IOContext& loc) {
  for (int i = 0; i < SHARDS; ++i) {
    auto fifo = co_await fifo::FIFO::create(dpp, rados, get_oid(0, i), loc,
					    asio::use_awaitable);
    EXPECT_TRUE(fifo);
  }
}
}

CORO_TEST_F(LogBacking, TestOmap, NeoRadosTest)
{
  co_await make_omap(rados(), pool());
  auto stat = co_await log_backing_type(
    dpp(), rados(), pool(), log_type::fifo, SHARDS,
    [](int shard){ return get_oid(0, shard); });
  EXPECT_EQ(log_type::omap, stat);
}


CORO_TEST_F(LogBacking, TestOmapEmpty, NeoRadosTest)
{
  auto stat = co_await log_backing_type(
    dpp(), rados(), pool(), log_type::omap, SHARDS,
    [](int shard){ return get_oid(0, shard); });
  EXPECT_EQ(log_type::omap, stat);
}

CORO_TEST_F(LogBacking, TestFIFO, NeoRadosTest)
{
  co_await make_fifo(dpp(), rados(), pool());
  auto stat = co_await log_backing_type(
    dpp(), rados(), pool(), log_type::fifo, SHARDS,
    [](int shard){ return get_oid(0, shard); });
  EXPECT_EQ(log_type::fifo, stat);
}

CORO_TEST_F(LogBacking, TestFIFOEmpty, NeoRadosTest)
{
  auto stat = co_await log_backing_type(
    dpp(), rados(), pool(), log_type::fifo, SHARDS,
    [](int shard){ return get_oid(0, shard); });
  EXPECT_EQ(log_type::fifo, stat);
}

TEST(CursorGen, RoundTrip) {
  const std::string_view pcurs = "fded";
  {
    auto gc = gencursor(0, pcurs);
    ASSERT_EQ(pcurs, gc);
    auto [gen, cursor] = cursorgen(gc);
    ASSERT_EQ(0, gen);
    ASSERT_EQ(pcurs, cursor);
  }
  {
    auto gc = gencursor(53, pcurs);
    ASSERT_NE(pcurs, gc);
    auto [gen, cursor] = cursorgen(gc);
    ASSERT_EQ(53, gen);
    ASSERT_EQ(pcurs, cursor);
  }
}

class generations final : public logback_generations {
public:

  entries_t got_entries;
  std::optional<uint64_t> tail;

  using logback_generations::logback_generations;

  void handle_init(entries_t e) override {
    got_entries = e;
  }

  void handle_new_gens(entries_t e) override {
    got_entries = e;
  }

  void handle_empty_to(uint64_t new_tail) override {
    tail = new_tail;
  }
};

CORO_TEST_F(LogBacking, GenerationSingle, NeoRadosTest) {
  auto lg = co_await logback_generations::init<generations>(
    dpp(), rados(), "foobar", pool(), &get_oid, SHARDS, log_type::fifo);

  EXPECT_FALSE(lg->got_entries.empty());
  EXPECT_EQ(0, lg->got_entries.begin()->first);

  EXPECT_EQ(0, lg->got_entries[0].gen_id);
  EXPECT_EQ(log_type::fifo, lg->got_entries[0].type);
  EXPECT_FALSE(lg->got_entries[0].pruned);

  EXPECT_THROW({
      co_await lg->empty_to(dpp(), 0);
    }, sys::system_error);


  lg.reset();

  lg = co_await logback_generations::init<generations>(
    dpp(), rados(), "foobar", pool(), &get_oid, SHARDS, log_type::fifo);

  EXPECT_EQ(0, lg->got_entries.begin()->first);

  EXPECT_EQ(0, lg->got_entries[0].gen_id);
  EXPECT_EQ(log_type::fifo, lg->got_entries[0].type);
  EXPECT_FALSE(lg->got_entries[0].pruned);

  lg->got_entries.clear();

  co_await lg->new_backing(dpp(), log_type::omap);

  EXPECT_EQ(1, lg->got_entries.size());
  EXPECT_EQ(1, lg->got_entries[1].gen_id);
  EXPECT_EQ(log_type::omap, lg->got_entries[1].type);
  EXPECT_FALSE(lg->got_entries[1].pruned);

  lg.reset();

  lg = co_await logback_generations::init<generations>(
    dpp(), rados(), "foobar", pool(), &get_oid, SHARDS, log_type::fifo);

  EXPECT_EQ(2, lg->got_entries.size());
  EXPECT_EQ(0, lg->got_entries[0].gen_id);
  EXPECT_EQ(log_type::fifo, lg->got_entries[0].type);
  EXPECT_FALSE(lg->got_entries[0].pruned);

  EXPECT_EQ(1, lg->got_entries[1].gen_id);
  EXPECT_EQ(log_type::omap, lg->got_entries[1].type);
  EXPECT_FALSE(lg->got_entries[1].pruned);

  co_await lg->empty_to(dpp(), 0);

  EXPECT_EQ(0, *lg->tail);

  lg.reset();

  lg = co_await logback_generations::init<generations>(
    dpp(), rados(), "foobar", pool(), &get_oid, SHARDS, log_type::fifo);

  EXPECT_EQ(1, lg->got_entries.size());
  EXPECT_EQ(1, lg->got_entries[1].gen_id);
  EXPECT_EQ(log_type::omap, lg->got_entries[1].type);
  EXPECT_FALSE(lg->got_entries[1].pruned);
}

CORO_TEST_F(LogBacking, GenerationWN, NeoRadosTest) {
  auto lg1 = co_await logback_generations::init<generations>(
    dpp(), rados(), "foobar", pool(), &get_oid, SHARDS, log_type::fifo);

  co_await lg1->new_backing(dpp(), log_type::omap);

  EXPECT_EQ(1, lg1->got_entries.size());
  EXPECT_EQ(1, lg1->got_entries[1].gen_id);
  EXPECT_EQ(log_type::omap, lg1->got_entries[1].type);
  EXPECT_FALSE(lg1->got_entries[1].pruned);

  lg1->got_entries.clear();

  auto rados2 = co_await neorados::RADOS::Builder{}
    .build(asio_context, boost::asio::use_awaitable);

  auto lg2 = co_await logback_generations::init<generations>(
    dpp(), rados2, "foobar", pool(), &get_oid, SHARDS, log_type::fifo);

  EXPECT_EQ(2, lg2->got_entries.size());

  EXPECT_EQ(0, lg2->got_entries[0].gen_id);
  EXPECT_EQ(log_type::fifo, lg2->got_entries[0].type);
  EXPECT_FALSE(lg2->got_entries[0].pruned);

  EXPECT_EQ(1, lg2->got_entries[1].gen_id);
  EXPECT_EQ(log_type::omap, lg2->got_entries[1].type);
  EXPECT_FALSE(lg2->got_entries[1].pruned);

  lg2->got_entries.clear();

  co_await lg1->new_backing(dpp(), log_type::fifo);

  EXPECT_EQ(1, lg1->got_entries.size());
  EXPECT_EQ(2, lg1->got_entries[2].gen_id);
  EXPECT_EQ(log_type::fifo, lg1->got_entries[2].type);
  EXPECT_FALSE(lg1->got_entries[2].pruned);

  EXPECT_EQ(1, lg2->got_entries.size());
  EXPECT_EQ(2, lg2->got_entries[2].gen_id);
  EXPECT_EQ(log_type::fifo, lg2->got_entries[2].type);
  EXPECT_FALSE(lg2->got_entries[2].pruned);

  lg1->got_entries.clear();
  lg2->got_entries.clear();

  co_await lg2->empty_to(dpp(), 1);

  EXPECT_EQ(1, *lg1->tail);
  EXPECT_EQ(1, *lg2->tail);

  lg1->tail.reset();
  lg2->tail.reset();
}
