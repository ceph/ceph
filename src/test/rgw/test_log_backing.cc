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

#include <cerrno>
#include <iostream>
#include <string_view>

#include <fmt/format.h>

#include "include/types.h"
#include "include/rados/librados.hpp"

#include "test/librados/test_cxx.h"
#include "global/global_context.h"

#include "cls/log/cls_log_client.h"

#include "rgw_tools.h"
#include "cls_fifo_legacy.h"

#include "gtest/gtest.h"

namespace lr = librados;
namespace cb = ceph::buffer;
namespace fifo = rados::cls::fifo;
namespace RCf = rgw::cls::fifo;

auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);
const DoutPrefix dp(cct, 1, "test log backing: ");

class LogBacking : public testing::Test {
protected:
  static constexpr int SHARDS = 3;
  const std::string pool_name = get_temp_pool_name();
  lr::Rados rados;
  lr::IoCtx ioctx;
  lr::Rados rados2;
  lr::IoCtx ioctx2;

  void SetUp() override {
    ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
    ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));
    connect_cluster_pp(rados2);
    ASSERT_EQ(0, rados2.ioctx_create(pool_name.c_str(), ioctx2));
  }
  void TearDown() override {
    destroy_one_pool_pp(pool_name, rados);
  }

  std::string get_oid(uint64_t gen_id, int i) const {
    return (gen_id > 0 ?
	    fmt::format("shard@G{}.{}", gen_id, i) :
	    fmt::format("shard.{}", i));
  }

  void make_omap() {
    for (int i = 0; i < SHARDS; ++i) {
      using ceph::encode;
      lr::ObjectWriteOperation op;
      cb::list bl;
      encode(i, bl);
      cls_log_add(op, ceph_clock_now(), {}, "meow", bl);
      auto r = rgw_rados_operate(&dp, ioctx, get_oid(0, i), &op, null_yield);
      ASSERT_GE(r, 0);
    }
  }

  void add_omap(int i) {
    using ceph::encode;
    lr::ObjectWriteOperation op;
    cb::list bl;
    encode(i, bl);
    cls_log_add(op, ceph_clock_now(), {}, "meow", bl);
    auto r = rgw_rados_operate(&dp, ioctx, get_oid(0, i), &op, null_yield);
    ASSERT_GE(r, 0);
  }

  void empty_omap() {
    for (int i = 0; i < SHARDS; ++i) {
      auto oid = get_oid(0, i);
      std::string to_marker;
      {
	lr::ObjectReadOperation op;
	std::vector<cls_log_entry> entries;
	bool truncated = false;
	cls_log_list(op, {}, {}, {}, 1, entries, &to_marker, &truncated);
	auto r = rgw_rados_operate(&dp, ioctx, oid, &op, nullptr, null_yield);
	ASSERT_GE(r, 0);
	ASSERT_FALSE(entries.empty());
      }
      {
	lr::ObjectWriteOperation op;
	cls_log_trim(op, {}, {}, {}, to_marker);
	auto r = rgw_rados_operate(&dp, ioctx, oid, &op, null_yield);
	ASSERT_GE(r, 0);
      }
      {
	lr::ObjectReadOperation op;
	std::vector<cls_log_entry> entries;
	bool truncated = false;
	cls_log_list(op, {}, {}, {}, 1, entries, &to_marker, &truncated);
	auto r = rgw_rados_operate(&dp, ioctx, oid, &op, nullptr, null_yield);
	ASSERT_GE(r, 0);
	ASSERT_TRUE(entries.empty());
      }
    }
  }

  void make_fifo()
    {
      for (int i = 0; i < SHARDS; ++i) {
	std::unique_ptr<RCf::FIFO> fifo;
	auto r = RCf::FIFO::create(&dp, ioctx, get_oid(0, i), &fifo, null_yield);
	ASSERT_EQ(0, r);
	ASSERT_TRUE(fifo);
      }
    }

  void add_fifo(int i)
    {
      using ceph::encode;
      std::unique_ptr<RCf::FIFO> fifo;
      auto r = RCf::FIFO::open(&dp, ioctx, get_oid(0, i), &fifo, null_yield);
      ASSERT_GE(0, r);
      ASSERT_TRUE(fifo);
      cb::list bl;
      encode(i, bl);
      r = fifo->push(&dp, bl, null_yield);
      ASSERT_GE(0, r);
    }

  void assert_empty() {
    std::vector<lr::ObjectItem> result;
    lr::ObjectCursor next;
    auto r = ioctx.object_list(ioctx.object_list_begin(), ioctx.object_list_end(),
			       100, {}, &result, &next);
    ASSERT_GE(r, 0);
    ASSERT_TRUE(result.empty());
  }
};

TEST_F(LogBacking, TestOmap)
{
  make_omap();
  auto stat = log_backing_type(&dp, ioctx, log_type::fifo, SHARDS,
			       [this](int shard){ return get_oid(0, shard); },
			       null_yield);
  ASSERT_EQ(log_type::omap, *stat);
}

TEST_F(LogBacking, TestOmapEmpty)
{
  auto stat = log_backing_type(&dp, ioctx, log_type::omap, SHARDS,
			       [this](int shard){ return get_oid(0, shard); },
			       null_yield);
  ASSERT_EQ(log_type::omap, *stat);
}

TEST_F(LogBacking, TestFIFO)
{
  make_fifo();
  auto stat = log_backing_type(&dp, ioctx, log_type::fifo, SHARDS,
			       [this](int shard){ return get_oid(0, shard); },
			       null_yield);
  ASSERT_EQ(log_type::fifo, *stat);
}

TEST_F(LogBacking, TestFIFOEmpty)
{
  auto stat = log_backing_type(&dp, ioctx, log_type::fifo, SHARDS,
			       [this](int shard){ return get_oid(0, shard); },
			       null_yield);
  ASSERT_EQ(log_type::fifo, *stat);
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

  bs::error_code handle_init(entries_t e) noexcept {
    got_entries = e;
    return {};
  }

  bs::error_code handle_new_gens(entries_t e) noexcept  {
    got_entries = e;
    return {};
  }

  bs::error_code handle_empty_to(uint64_t new_tail) noexcept {
    tail = new_tail;
    return {};
  }
};

TEST_F(LogBacking, GenerationSingle)
{
  auto lgr = logback_generations::init<generations>(
    &dp, ioctx, "foobar", [this](uint64_t gen_id, int shard) {
      return get_oid(gen_id, shard);
    }, SHARDS, log_type::fifo, null_yield);
  ASSERT_TRUE(lgr);

  auto lg = std::move(*lgr);

  ASSERT_EQ(0, lg->got_entries.begin()->first);

  ASSERT_EQ(0, lg->got_entries[0].gen_id);
  ASSERT_EQ(log_type::fifo, lg->got_entries[0].type);
  ASSERT_FALSE(lg->got_entries[0].pruned);

  auto ec = lg->empty_to(&dp, 0, null_yield);
  ASSERT_TRUE(ec);

  lg.reset();

  lg = *logback_generations::init<generations>(
    &dp, ioctx, "foobar", [this](uint64_t gen_id, int shard) {
      return get_oid(gen_id, shard);
    }, SHARDS, log_type::fifo, null_yield);

  ASSERT_EQ(0, lg->got_entries.begin()->first);

  ASSERT_EQ(0, lg->got_entries[0].gen_id);
  ASSERT_EQ(log_type::fifo, lg->got_entries[0].type);
  ASSERT_FALSE(lg->got_entries[0].pruned);

  lg->got_entries.clear();

  ec = lg->new_backing(&dp, log_type::omap, null_yield);
  ASSERT_FALSE(ec);

  ASSERT_EQ(1, lg->got_entries.size());
  ASSERT_EQ(1, lg->got_entries[1].gen_id);
  ASSERT_EQ(log_type::omap, lg->got_entries[1].type);
  ASSERT_FALSE(lg->got_entries[1].pruned);

  lg.reset();

  lg = *logback_generations::init<generations>(
    &dp, ioctx, "foobar", [this](uint64_t gen_id, int shard) {
      return get_oid(gen_id, shard);
    }, SHARDS, log_type::fifo, null_yield);

  ASSERT_EQ(2, lg->got_entries.size());
  ASSERT_EQ(0, lg->got_entries[0].gen_id);
  ASSERT_EQ(log_type::fifo, lg->got_entries[0].type);
  ASSERT_FALSE(lg->got_entries[0].pruned);

  ASSERT_EQ(1, lg->got_entries[1].gen_id);
  ASSERT_EQ(log_type::omap, lg->got_entries[1].type);
  ASSERT_FALSE(lg->got_entries[1].pruned);

  ec = lg->empty_to(&dp, 0, null_yield);
  ASSERT_FALSE(ec);

  ASSERT_EQ(0, *lg->tail);

  lg.reset();

  lg = *logback_generations::init<generations>(
    &dp, ioctx, "foobar", [this](uint64_t gen_id, int shard) {
      return get_oid(gen_id, shard);
    }, SHARDS, log_type::fifo, null_yield);

  ASSERT_EQ(1, lg->got_entries.size());
  ASSERT_EQ(1, lg->got_entries[1].gen_id);
  ASSERT_EQ(log_type::omap, lg->got_entries[1].type);
  ASSERT_FALSE(lg->got_entries[1].pruned);
}

TEST_F(LogBacking, GenerationWN)
{
  auto lg1 = *logback_generations::init<generations>(
    &dp, ioctx, "foobar", [this](uint64_t gen_id, int shard) {
      return get_oid(gen_id, shard);
    }, SHARDS, log_type::fifo, null_yield);

  auto ec = lg1->new_backing(&dp, log_type::omap, null_yield);
  ASSERT_FALSE(ec);

  ASSERT_EQ(1, lg1->got_entries.size());
  ASSERT_EQ(1, lg1->got_entries[1].gen_id);
  ASSERT_EQ(log_type::omap, lg1->got_entries[1].type);
  ASSERT_FALSE(lg1->got_entries[1].pruned);

  lg1->got_entries.clear();

  auto lg2 = *logback_generations::init<generations>(
    &dp, ioctx2, "foobar", [this](uint64_t gen_id, int shard) {
      return get_oid(gen_id, shard);
    }, SHARDS, log_type::fifo, null_yield);

  ASSERT_EQ(2, lg2->got_entries.size());

  ASSERT_EQ(0, lg2->got_entries[0].gen_id);
  ASSERT_EQ(log_type::fifo, lg2->got_entries[0].type);
  ASSERT_FALSE(lg2->got_entries[0].pruned);

  ASSERT_EQ(1, lg2->got_entries[1].gen_id);
  ASSERT_EQ(log_type::omap, lg2->got_entries[1].type);
  ASSERT_FALSE(lg2->got_entries[1].pruned);

  lg2->got_entries.clear();

  ec = lg1->new_backing(&dp, log_type::fifo, null_yield);
  ASSERT_FALSE(ec);

  ASSERT_EQ(1, lg1->got_entries.size());
  ASSERT_EQ(2, lg1->got_entries[2].gen_id);
  ASSERT_EQ(log_type::fifo, lg1->got_entries[2].type);
  ASSERT_FALSE(lg1->got_entries[2].pruned);

  ASSERT_EQ(1, lg2->got_entries.size());
  ASSERT_EQ(2, lg2->got_entries[2].gen_id);
  ASSERT_EQ(log_type::fifo, lg2->got_entries[2].type);
  ASSERT_FALSE(lg2->got_entries[2].pruned);

  lg1->got_entries.clear();
  lg2->got_entries.clear();

  ec = lg2->empty_to(&dp, 1, null_yield);
  ASSERT_FALSE(ec);

  ASSERT_EQ(1, *lg1->tail);
  ASSERT_EQ(1, *lg2->tail);

  lg1->tail.reset();
  lg2->tail.reset();
}
