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

#include <cerrno>
#include <iostream>
#include <string_view>

#include "include/scope_guard.h"
#include "include/types.h"
#include "include/rados/librados.hpp"

#include "cls/fifo/cls_fifo_ops.h"
#include "test/librados/test_cxx.h"
#include "global/global_context.h"

#include "rgw/rgw_tools.h"
#include "rgw/cls_fifo_legacy.h"

#include "gtest/gtest.h"

namespace R = librados;
namespace cb = ceph::buffer;
namespace fifo = rados::cls::fifo;
namespace RCf = rgw::cls::fifo;

namespace {
int fifo_create(R::IoCtx& ioctx,
		const std::string& oid,
		std::string_view id,
		optional_yield y,
		std::optional<fifo::objv> objv = std::nullopt,
		std::optional<std::string_view> oid_prefix = std::nullopt,
		bool exclusive = false,
		std::uint64_t max_part_size = RCf::default_max_part_size,
		std::uint64_t max_entry_size = RCf::default_max_entry_size)
{
  R::ObjectWriteOperation op;
  RCf::create_meta(&op, id, objv, oid_prefix, exclusive, max_part_size,
		   max_entry_size);
  return rgw_rados_operate(ioctx, oid, &op, y);
}
}

class LegacyFIFO : public testing::Test {
protected:
  const std::string pool_name = get_temp_pool_name();
  const std::string fifo_id = "fifo";
  R::Rados rados;
  librados::IoCtx ioctx;

  void SetUp() override {
    ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
    ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));
  }
  void TearDown() override {
    destroy_one_pool_pp(pool_name, rados);
  }
};

using LegacyClsFIFO = LegacyFIFO;

TEST_F(LegacyClsFIFO, TestCreate)
{
  auto r = fifo_create(ioctx, fifo_id, ""s, null_yield);
  EXPECT_EQ(-EINVAL, r);
  r = fifo_create(ioctx, fifo_id, fifo_id, null_yield, std::nullopt,
		  std::nullopt, false, 0);
  EXPECT_EQ(-EINVAL, r);
  r = fifo_create(ioctx, fifo_id, {}, null_yield,
		  std::nullopt, std::nullopt,
		  false, RCf::default_max_part_size, 0);
  EXPECT_EQ(-EINVAL, r);
  r = fifo_create(ioctx, fifo_id, fifo_id, null_yield);
  EXPECT_EQ(0, r);
  std::uint64_t size;
  ioctx.stat(fifo_id, &size, nullptr);
  EXPECT_GT(size, 0);
  /* test idempotency */
  r = fifo_create(ioctx, fifo_id, fifo_id, null_yield);
  EXPECT_EQ(0, r);
  r = fifo_create(ioctx, fifo_id, {}, null_yield, std::nullopt,
		  std::nullopt, false);
  EXPECT_EQ(-EINVAL, r);
  r = fifo_create(ioctx, fifo_id, {}, null_yield, std::nullopt,
		  "myprefix"sv, false);
  EXPECT_EQ(-EINVAL, r);
  r = fifo_create(ioctx, fifo_id, "foo"sv, null_yield,
		  std::nullopt, std::nullopt, false);
  EXPECT_EQ(-EEXIST, r);
}

TEST_F(LegacyClsFIFO, TestGetInfo)
{
  auto r = fifo_create(ioctx, fifo_id, fifo_id, null_yield);
  fifo::info info;
  std::uint32_t part_header_size;
  std::uint32_t part_entry_overhead;
  r = RCf::get_meta(ioctx, fifo_id, std::nullopt, &info, &part_header_size,
		    &part_entry_overhead, null_yield);
  EXPECT_EQ(0, r);
  EXPECT_GT(part_header_size, 0);
  EXPECT_GT(part_entry_overhead, 0);
  EXPECT_FALSE(info.version.instance.empty());

  r = RCf::get_meta(ioctx, fifo_id, info.version, &info, &part_header_size,
		    &part_entry_overhead, null_yield);
  EXPECT_EQ(0, r);
  fifo::objv objv;
  objv.instance = "foo";
  objv.ver = 12;
  r = RCf::get_meta(ioctx, fifo_id, objv, &info, &part_header_size,
			&part_entry_overhead, null_yield);
  EXPECT_EQ(-ECANCELED, r);
}

TEST_F(LegacyFIFO, TestOpenDefault)
{
  std::unique_ptr<RCf::FIFO> fifo;
  auto r = RCf::FIFO::create(ioctx, fifo_id, &fifo, null_yield);
  ASSERT_EQ(0, r);
  // force reading from backend
  r = fifo->read_meta(null_yield);
  EXPECT_EQ(0, r);
  auto info = fifo->meta();
  EXPECT_EQ(info.id, fifo_id);
}

TEST_F(LegacyFIFO, TestOpenParams)
{
  const std::uint64_t max_part_size = 10 * 1024;
  const std::uint64_t max_entry_size = 128;
  auto oid_prefix = "foo.123."sv;
  fifo::objv objv;
  objv.instance = "fooz"s;
  objv.ver = 10;

  /* first successful create */
  std::unique_ptr<RCf::FIFO> f;
  auto r = RCf::FIFO::create(ioctx, fifo_id, &f, null_yield, objv, oid_prefix,
			     false, max_part_size, max_entry_size);
  ASSERT_EQ(0, r);

  /* force reading from backend */
  r = f->read_meta(null_yield);
  auto info = f->meta();
  EXPECT_EQ(info.id, fifo_id);
  EXPECT_EQ(info.params.max_part_size, max_part_size);
  EXPECT_EQ(info.params.max_entry_size, max_entry_size);
  EXPECT_EQ(info.version, objv);
}

namespace {
template<class T>
std::pair<T, std::string> decode_entry(const RCf::list_entry& entry)
{
  T val;
  auto iter = entry.data.cbegin();
  decode(val, iter);
  return std::make_pair(std::move(val), entry.marker);
}
}


TEST_F(LegacyFIFO, TestPushListTrim)
{
  std::unique_ptr<RCf::FIFO> f;
  auto r = RCf::FIFO::create(ioctx, fifo_id, &f, null_yield);
  ASSERT_EQ(0, r);
  static constexpr auto max_entries = 10u;
  for (uint32_t i = 0; i < max_entries; ++i) {
    cb::list bl;
    encode(i, bl);
    r = f->push(bl, null_yield);
    ASSERT_EQ(0, r);
  }

  std::optional<std::string> marker;
  /* get entries one by one */
  std::vector<RCf::list_entry> result;
  bool more = false;
  for (auto i = 0u; i < max_entries; ++i) {

    r = f->list(1, marker, &result, &more, null_yield);
    ASSERT_EQ(0, r);

    bool expected_more = (i != (max_entries - 1));
    ASSERT_EQ(expected_more, more);
    ASSERT_EQ(1, result.size());

    std::uint32_t val;
    std::tie(val, marker) = decode_entry<std::uint32_t>(result.front());

    ASSERT_EQ(i, val);
    result.clear();
  }

  /* get all entries at once */
  std::string markers[max_entries];
  std::uint32_t min_entry = 0;
  r = f->list(max_entries * 10, std::nullopt, &result, &more, null_yield);
  ASSERT_EQ(0, r);

  ASSERT_FALSE(more);
  ASSERT_EQ(max_entries, result.size());
  for (auto i = 0u; i < max_entries; ++i) {
    std::uint32_t val;
    std::tie(val, markers[i]) = decode_entry<std::uint32_t>(result[i]);
    ASSERT_EQ(i, val);
  }

  /* trim one entry */
  r = f->trim(markers[min_entry], null_yield);
  ASSERT_EQ(0, r);
  ++min_entry;

  r = f->list(max_entries * 10, std::nullopt, &result, &more, null_yield);
  ASSERT_EQ(0, r);
  ASSERT_FALSE(more);
  ASSERT_EQ(max_entries - min_entry, result.size());

  for (auto i = min_entry; i < max_entries; ++i) {
    std::uint32_t val;
    std::tie(val, markers[i - min_entry]) =
      decode_entry<std::uint32_t>(result[i - min_entry]);
    EXPECT_EQ(i, val);
  }
}


TEST_F(LegacyFIFO, TestPushTooBig)
{
  static constexpr auto max_part_size = 2048ull;
  static constexpr auto max_entry_size = 128ull;

  std::unique_ptr<RCf::FIFO> f;
  auto r = RCf::FIFO::create(ioctx, fifo_id, &f, null_yield, std::nullopt,
			     std::nullopt, false, max_part_size, max_entry_size);
  ASSERT_EQ(0, r);

  char buf[max_entry_size + 1];
  memset(buf, 0, sizeof(buf));

  cb::list bl;
  bl.append(buf, sizeof(buf));

  r = f->push(bl, null_yield);
  EXPECT_EQ(-E2BIG, r);
}


TEST_F(LegacyFIFO, TestMultipleParts)
{
  static constexpr auto max_part_size = 2048ull;
  static constexpr auto max_entry_size = 128ull;
  std::unique_ptr<RCf::FIFO> f;
  auto r = RCf::FIFO::create(ioctx, fifo_id, &f, null_yield, std::nullopt,
			     std::nullopt, false, max_part_size,
			     max_entry_size);
  ASSERT_EQ(0, r);

  char buf[max_entry_size];
  memset(buf, 0, sizeof(buf));
  const auto [part_header_size, part_entry_overhead] =
    f->get_part_layout_info();
  const auto entries_per_part = ((max_part_size - part_header_size) /
				 (max_entry_size + part_entry_overhead));
  const auto max_entries = entries_per_part * 4 + 1;
  /* push enough entries */
  for (auto i = 0u; i < max_entries; ++i) {
    cb::list bl;
    *(int *)buf = i;
    bl.append(buf, sizeof(buf));
    r = f->push(bl, null_yield);
    ASSERT_EQ(0, r);
  }

  auto info = f->meta();
  ASSERT_EQ(info.id, fifo_id);
  /* head should have advanced */
  ASSERT_GT(info.head_part_num, 0);

  /* list all at once */
  std::vector<RCf::list_entry> result;
  bool more = false;
  r = f->list(max_entries, std::nullopt, &result, &more, null_yield);
  ASSERT_EQ(0, r);
  EXPECT_EQ(false, more);
  ASSERT_EQ(max_entries, result.size());

  for (auto i = 0u; i < max_entries; ++i) {
    auto& bl = result[i].data;
    ASSERT_EQ(i, *(int *)bl.c_str());
  }

  std::optional<std::string> marker;
  /* get entries one by one */

  for (auto i = 0u; i < max_entries; ++i) {
    r = f->list(1, marker, &result, &more, null_yield);
    ASSERT_EQ(0, r);
    ASSERT_EQ(result.size(), 1);
    const bool expected_more = (i != (max_entries - 1));
    ASSERT_EQ(expected_more, more);

    std::uint32_t val;
    std::tie(val, marker) = decode_entry<std::uint32_t>(result.front());

    auto& entry = result.front();
    auto& bl = entry.data;
    ASSERT_EQ(i, *(int *)bl.c_str());
    marker = entry.marker;
  }

  /* trim one at a time */
  marker.reset();
  for (auto i = 0u; i < max_entries; ++i) {
    /* read single entry */
    r = f->list(1, marker, &result, &more, null_yield);
    ASSERT_EQ(0, r);
    ASSERT_EQ(result.size(), 1);
    const bool expected_more = (i != (max_entries - 1));
    ASSERT_EQ(expected_more, more);

    marker = result.front().marker;
    r = f->trim(*marker, null_yield);
    ASSERT_EQ(0, r);

    /* check tail */
    info = f->meta();
    ASSERT_EQ(info.tail_part_num, i / entries_per_part);

    /* try to read all again, see how many entries left */
    r = f->list(max_entries, marker, &result, &more, null_yield);
    ASSERT_EQ(max_entries - i - 1, result.size());
    ASSERT_EQ(false, more);
  }

  /* tail now should point at head */
  info = f->meta();
  ASSERT_EQ(info.head_part_num, info.tail_part_num);

  RCf::part_info partinfo;
  /* check old tails are removed */
  for (auto i = 0; i < info.tail_part_num; ++i) {
    r = f->get_part_info(i, &partinfo, null_yield);
    ASSERT_EQ(-ENOENT, r);
  }
  /* check current tail exists */
  r = f->get_part_info(info.tail_part_num, &partinfo, null_yield);
  ASSERT_EQ(0, r);
}

TEST_F(LegacyFIFO, TestTwoPushers)
{
  static constexpr auto max_part_size = 2048ull;
  static constexpr auto max_entry_size = 128ull;

  std::unique_ptr<RCf::FIFO> f;
  auto r = RCf::FIFO::create(ioctx, fifo_id, &f, null_yield, std::nullopt,
			     std::nullopt, false, max_part_size,
			     max_entry_size);
  ASSERT_EQ(0, r);
  char buf[max_entry_size];
  memset(buf, 0, sizeof(buf));

  auto [part_header_size, part_entry_overhead] = f->get_part_layout_info();
  const auto entries_per_part = ((max_part_size - part_header_size) /
				 (max_entry_size + part_entry_overhead));
  const auto max_entries = entries_per_part * 4 + 1;
  std::unique_ptr<RCf::FIFO> f2;
  r = RCf::FIFO::open(ioctx, fifo_id, &f2, null_yield);
  std::vector fifos{&f, &f2};

  for (auto i = 0u; i < max_entries; ++i) {
    cb::list bl;
    *(int *)buf = i;
    bl.append(buf, sizeof(buf));
    auto& f = *fifos[i % fifos.size()];
    r = f->push(bl, null_yield);
    ASSERT_EQ(0, r);
  }

  /* list all by both */
  std::vector<RCf::list_entry> result;
  bool more = false;
  r = f2->list(max_entries, std::nullopt, &result, &more, null_yield);
  ASSERT_EQ(0, r);
  ASSERT_EQ(false, more);
  ASSERT_EQ(max_entries, result.size());

  r = f2->list(max_entries, std::nullopt, &result, &more, null_yield);
  ASSERT_EQ(0, r);
  ASSERT_EQ(false, more);
  ASSERT_EQ(max_entries, result.size());

  for (auto i = 0u; i < max_entries; ++i) {
    auto& bl = result[i].data;
    ASSERT_EQ(i, *(int *)bl.c_str());
  }
}

TEST_F(LegacyFIFO, TestTwoPushersTrim)
{
  static constexpr auto max_part_size = 2048ull;
  static constexpr auto max_entry_size = 128ull;
  std::unique_ptr<RCf::FIFO> f1;
  auto r = RCf::FIFO::create(ioctx, fifo_id, &f1, null_yield, std::nullopt,
			     std::nullopt, false, max_part_size,
			     max_entry_size);
  ASSERT_EQ(0, r);

  char buf[max_entry_size];
  memset(buf, 0, sizeof(buf));

  auto [part_header_size, part_entry_overhead] = f1->get_part_layout_info();
  const auto entries_per_part = ((max_part_size - part_header_size) /
				 (max_entry_size + part_entry_overhead));
  const auto max_entries = entries_per_part * 4 + 1;

  std::unique_ptr<RCf::FIFO> f2;
  r = RCf::FIFO::open(ioctx, fifo_id, &f2, null_yield);
  ASSERT_EQ(0, r);

  /* push one entry to f2 and the rest to f1 */
  for (auto i = 0u; i < max_entries; ++i) {
    cb::list bl;
    *(int *)buf = i;
    bl.append(buf, sizeof(buf));
    auto& f = (i < 1 ? f2 : f1);
    r = f->push(bl, null_yield);
    ASSERT_EQ(0, r);
  }

  /* trim half by fifo1 */
  auto num = max_entries / 2;
  std::string marker;
  std::vector<RCf::list_entry> result;
  bool more = false;
  r = f1->list(num, std::nullopt, &result, &more, null_yield);
  ASSERT_EQ(0, r);
  ASSERT_EQ(true, more);
  ASSERT_EQ(num, result.size());

  for (auto i = 0u; i < num; ++i) {
    auto& bl = result[i].data;
    ASSERT_EQ(i, *(int *)bl.c_str());
  }

  auto& entry = result[num - 1];
  marker = entry.marker;
  r = f1->trim(marker, null_yield);
  /* list what's left by fifo2 */

  const auto left = max_entries - num;
  f2->list(left, marker, &result, &more, null_yield);
  ASSERT_EQ(left, result.size());
  ASSERT_EQ(false, more);

  for (auto i = num; i < max_entries; ++i) {
    auto& bl = result[i - num].data;
    ASSERT_EQ(i, *(int *)bl.c_str());
  }
}

TEST_F(LegacyFIFO, TestPushBatch)
{
  static constexpr auto max_part_size = 2048ull;
  static constexpr auto max_entry_size = 128ull;

  std::unique_ptr<RCf::FIFO> f;
  auto r = RCf::FIFO::create(ioctx, fifo_id, &f, null_yield, std::nullopt,
			     std::nullopt, false, max_part_size,
			     max_entry_size);
  ASSERT_EQ(0, r);

  char buf[max_entry_size];
  memset(buf, 0, sizeof(buf));
  auto [part_header_size, part_entry_overhead] = f->get_part_layout_info();
  auto entries_per_part = ((max_part_size - part_header_size) /
			   (max_entry_size + part_entry_overhead));
  auto max_entries = entries_per_part * 4 + 1; /* enough entries to span multiple parts */
  std::vector<cb::list> bufs;
  for (auto i = 0u; i < max_entries; ++i) {
    cb::list bl;
    *(int *)buf = i;
    bl.append(buf, sizeof(buf));
    bufs.push_back(bl);
  }

  r = f->push(bufs, null_yield);
  ASSERT_EQ(0, r);

  /* list all */

  std::vector<RCf::list_entry> result;
  bool more = false;
  r = f->list(max_entries, std::nullopt, &result, &more, null_yield);
  ASSERT_EQ(0, r);
  ASSERT_EQ(false, more);
  ASSERT_EQ(max_entries, result.size());
  for (auto i = 0u; i < max_entries; ++i) {
    auto& bl = result[i].data;
    ASSERT_EQ(i, *(int *)bl.c_str());
  }
  auto& info = f->meta();
  ASSERT_EQ(info.head_part_num, 4);
}

TEST_F(LegacyFIFO, TestAioTrim)
{
  static constexpr auto max_part_size = 2048ull;
  static constexpr auto max_entry_size = 128ull;
  std::unique_ptr<RCf::FIFO> f;
  auto r = RCf::FIFO::create(ioctx, fifo_id, &f, null_yield, std::nullopt,
			     std::nullopt, false, max_part_size,
			     max_entry_size);
  ASSERT_EQ(0, r);

  char buf[max_entry_size];
  memset(buf, 0, sizeof(buf));
  const auto [part_header_size, part_entry_overhead] =
    f->get_part_layout_info();
  const auto entries_per_part = ((max_part_size - part_header_size) /
				 (max_entry_size + part_entry_overhead));
  const auto max_entries = entries_per_part * 4 + 1;
  /* push enough entries */
  std::vector<cb::list> bufs;
  for (auto i = 0u; i < max_entries; ++i) {
    cb::list bl;
    *(int *)buf = i;
    bl.append(buf, sizeof(buf));
    bufs.push_back(std::move(bl));
  }

  r = f->push(bufs, null_yield);
  ASSERT_EQ(0, r);

  auto info = f->meta();
  ASSERT_EQ(info.id, fifo_id);
  /* head should have advanced */
  ASSERT_GT(info.head_part_num, 0);

  /* list all at once */
  std::vector<RCf::list_entry> result;
  bool more = false;
  std::optional<std::string> marker;
  /* trim one at a time */
  marker.reset();
  for (auto i = 0u; i < max_entries; ++i) {
    /* read single entry */
    r = f->list(1, marker, &result, &more, null_yield);
    ASSERT_EQ(0, r);
    ASSERT_EQ(result.size(), 1);
    const bool expected_more = (i != (max_entries - 1));
    ASSERT_EQ(expected_more, more);

    marker = result.front().marker;
    std::unique_ptr<R::AioCompletion> c(rados.aio_create_completion(nullptr,
								    nullptr));
    r = f->trim(*marker, c.get());
    ASSERT_EQ(0, r);
    c->wait_for_complete();
    r = c->get_return_value();
    ASSERT_EQ(0, r);

    /* check tail */
    info = f->meta();
    ASSERT_EQ(info.tail_part_num, i / entries_per_part);

    /* try to read all again, see how many entries left */
    r = f->list(max_entries, marker, &result, &more, null_yield);
    ASSERT_EQ(max_entries - i - 1, result.size());
    ASSERT_EQ(false, more);
  }

  /* tail now should point at head */
  info = f->meta();
  ASSERT_EQ(info.head_part_num, info.tail_part_num);

  RCf::part_info partinfo;
  /* check old tails are removed */
  for (auto i = 0; i < info.tail_part_num; ++i) {
    r = f->get_part_info(i, &partinfo, null_yield);
    ASSERT_EQ(-ENOENT, r);
  }
  /* check current tail exists */
  r = f->get_part_info(info.tail_part_num, &partinfo, null_yield);
  ASSERT_EQ(0, r);
}
