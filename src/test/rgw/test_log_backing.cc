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

#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include <fmt/format.h>

#include "include/types.h"
#include "include/rados/librados.hpp"

#include "test/librados/test_cxx.h"
#include "global/global_context.h"

#include "cls/log/cls_log_client.h"

#include "rgw/rgw_tools.h"
#include "rgw/cls_fifo_legacy.h"

#include "gtest/gtest.h"

namespace lr = librados;
namespace cb = ceph::buffer;
namespace fifo = rados::cls::fifo;
namespace RCf = rgw::cls::fifo;

class LogBacking : public testing::Test {
protected:
  static constexpr int SHARDS = 3;
  const std::string pool_name = get_temp_pool_name();
  lr::Rados rados;
  lr::IoCtx ioctx;

  void SetUp() override {
    ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
    ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));
  }
  void TearDown() override {
    destroy_one_pool_pp(pool_name, rados);
  }

  static std::string get_oid(int i) {
    return fmt::format("shard.{}", i);
  }

  void make_omap() {
    for (int i = 0; i < SHARDS; ++i) {
      using ceph::encode;
      lr::ObjectWriteOperation op;
      cb::list bl;
      encode(i, bl);
      cls_log_add(op, ceph_clock_now(), {}, "meow", bl);
      auto r = rgw_rados_operate(ioctx, get_oid(i), &op, null_yield);
      ASSERT_GE(r, 0);
    }
  }

  void add_omap(int i) {
    using ceph::encode;
    lr::ObjectWriteOperation op;
    cb::list bl;
    encode(i, bl);
    cls_log_add(op, ceph_clock_now(), {}, "meow", bl);
    auto r = rgw_rados_operate(ioctx, get_oid(i), &op, null_yield);
    ASSERT_GE(r, 0);
  }

  void empty_omap() {
    for (int i = 0; i < SHARDS; ++i) {
      auto oid = get_oid(i);
      std::string to_marker;
      {
	lr::ObjectReadOperation op;
	std::list<cls_log_entry> entries;
	bool truncated = false;
	cls_log_list(op, {}, {}, {}, 1, entries, &to_marker, &truncated);
	auto r = rgw_rados_operate(ioctx, oid, &op, nullptr, null_yield);
	ASSERT_GE(r, 0);
	ASSERT_FALSE(entries.empty());
      }
      {
	lr::ObjectWriteOperation op;
	cls_log_trim(op, {}, {}, {}, to_marker);
	auto r = rgw_rados_operate(ioctx, oid, &op, null_yield);
	ASSERT_GE(r, 0);
      }
      {
	lr::ObjectReadOperation op;
	std::list<cls_log_entry> entries;
	bool truncated = false;
	cls_log_list(op, {}, {}, {}, 1, entries, &to_marker, &truncated);
	auto r = rgw_rados_operate(ioctx, oid, &op, nullptr, null_yield);
	ASSERT_GE(r, 0);
	ASSERT_TRUE(entries.empty());
      }
    }
  }

  void make_fifo()
    {
      for (int i = 0; i < SHARDS; ++i) {
	std::unique_ptr<RCf::FIFO> fifo;
	auto r = RCf::FIFO::create(ioctx, get_oid(i), &fifo, null_yield);
	ASSERT_EQ(0, r);
	ASSERT_TRUE(fifo);
      }
    }

  void add_fifo(int i)
    {
      using ceph::encode;
      std::unique_ptr<RCf::FIFO> fifo;
      auto r = RCf::FIFO::open(ioctx, get_oid(i), &fifo, null_yield);
      ASSERT_GE(0, r);
      ASSERT_TRUE(fifo);
      cb::list bl;
      encode(i, bl);
      r = fifo->push(bl, null_yield);
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
  auto stat = log_backing_type(ioctx, log_type::fifo, SHARDS,
			       get_oid, null_yield);
  ASSERT_EQ(log_type::omap, *stat);
}

TEST_F(LogBacking, TestOmapEmpty)
{
  auto stat = log_backing_type(ioctx, log_type::omap, SHARDS,
			       get_oid, null_yield);
  ASSERT_EQ(log_type::omap, *stat);
}

TEST_F(LogBacking, TestFIFO)
{
  make_fifo();
  auto stat = log_backing_type(ioctx, log_type::fifo, SHARDS,
			       get_oid, null_yield);
  ASSERT_EQ(log_type::fifo, *stat);
}

TEST_F(LogBacking, TestFIFOEmpty)
{
  auto stat = log_backing_type(ioctx, log_type::fifo, SHARDS,
			       get_oid, null_yield);
  ASSERT_EQ(log_type::fifo, *stat);
}

TEST(CursorGen, RoundTrip) {
  const auto pcurs = "fded"sv;
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
