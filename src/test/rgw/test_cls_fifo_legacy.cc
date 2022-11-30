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
#include "common/ceph_context.h"

#include "cls/fifo/cls_fifo_ops.h"
#include "test/librados/test_cxx.h"
#include "global/global_context.h"

#include "rgw_tools.h"
#include "cls_fifo_legacy.h"

#include "gtest/gtest.h"

using namespace std::literals;
using namespace std::string_literals;

namespace R = librados;
namespace cb = ceph::buffer;
namespace fifo = rados::cls::fifo;
namespace RCf = rgw::cls::fifo;

auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);
const DoutPrefix dp(cct, 1, "test legacy cls fifo: ");

namespace {
int fifo_create(const DoutPrefixProvider *dpp, R::IoCtx& ioctx,
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
  return rgw_rados_operate(dpp, ioctx, oid, &op, y);
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
using AioLegacyFIFO = LegacyFIFO;


TEST_F(LegacyClsFIFO, TestCreate)
{
  auto r = fifo_create(&dp, ioctx, fifo_id, ""s, null_yield);
  EXPECT_EQ(-EINVAL, r);
  r = fifo_create(&dp, ioctx, fifo_id, fifo_id, null_yield, std::nullopt,
		  std::nullopt, false, 0);
  EXPECT_EQ(-EINVAL, r);
  r = fifo_create(&dp, ioctx, fifo_id, {}, null_yield,
		  std::nullopt, std::nullopt,
		  false, RCf::default_max_part_size, 0);
  EXPECT_EQ(-EINVAL, r);
  r = fifo_create(&dp, ioctx, fifo_id, fifo_id, null_yield);
  EXPECT_EQ(0, r);
  std::uint64_t size;
  ioctx.stat(fifo_id, &size, nullptr);
  EXPECT_GT(size, 0);
  /* test idempotency */
  r = fifo_create(&dp, ioctx, fifo_id, fifo_id, null_yield);
  EXPECT_EQ(0, r);
  r = fifo_create(&dp, ioctx, fifo_id, {}, null_yield, std::nullopt,
		  std::nullopt, false);
  EXPECT_EQ(-EINVAL, r);
  r = fifo_create(&dp, ioctx, fifo_id, {}, null_yield, std::nullopt,
		  "myprefix"sv, false);
  EXPECT_EQ(-EINVAL, r);
  r = fifo_create(&dp, ioctx, fifo_id, "foo"sv, null_yield,
		  std::nullopt, std::nullopt, false);
  EXPECT_EQ(-EEXIST, r);
}

TEST_F(LegacyClsFIFO, TestGetInfo)
{
  auto r = fifo_create(&dp, ioctx, fifo_id, fifo_id, null_yield);
  fifo::info info;
  std::uint32_t part_header_size;
  std::uint32_t part_entry_overhead;
  r = RCf::get_meta(&dp, ioctx, fifo_id, std::nullopt, &info, &part_header_size,
		    &part_entry_overhead, 0, null_yield);
  EXPECT_EQ(0, r);
  EXPECT_GT(part_header_size, 0);
  EXPECT_GT(part_entry_overhead, 0);
  EXPECT_FALSE(info.version.instance.empty());

  r = RCf::get_meta(&dp, ioctx, fifo_id, info.version, &info, &part_header_size,
		    &part_entry_overhead, 0, null_yield);
  EXPECT_EQ(0, r);
  fifo::objv objv;
  objv.instance = "foo";
  objv.ver = 12;
  r = RCf::get_meta(&dp, ioctx, fifo_id, objv, &info, &part_header_size,
		    &part_entry_overhead, 0, null_yield);
  EXPECT_EQ(-ECANCELED, r);
}

TEST_F(LegacyFIFO, TestOpenDefault)
{
  std::unique_ptr<RCf::FIFO> fifo;
  auto r = RCf::FIFO::create(&dp, ioctx, fifo_id, &fifo, null_yield);
  ASSERT_EQ(0, r);
  // force reading from backend
  r = fifo->read_meta(&dp, null_yield);
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
  auto r = RCf::FIFO::create(&dp, ioctx, fifo_id, &f, null_yield, objv, oid_prefix,
			     false, max_part_size, max_entry_size);
  ASSERT_EQ(0, r);

  /* force reading from backend */
  r = f->read_meta(&dp, null_yield);
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
  auto r = RCf::FIFO::create(&dp, ioctx, fifo_id, &f, null_yield);
  ASSERT_EQ(0, r);
  static constexpr auto max_entries = 10u;
  for (uint32_t i = 0; i < max_entries; ++i) {
    cb::list bl;
    encode(i, bl);
    r = f->push(&dp, bl, null_yield);
    ASSERT_EQ(0, r);
  }

  std::optional<std::string> marker;
  /* get entries one by one */
  std::vector<RCf::list_entry> result;
  bool more = false;
  for (auto i = 0u; i < max_entries; ++i) {

    r = f->list(&dp, 1, marker, &result, &more, null_yield);
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
  r = f->list(&dp, max_entries * 10, std::nullopt, &result, &more, null_yield);
  ASSERT_EQ(0, r);

  ASSERT_FALSE(more);
  ASSERT_EQ(max_entries, result.size());
  for (auto i = 0u; i < max_entries; ++i) {
    std::uint32_t val;
    std::tie(val, markers[i]) = decode_entry<std::uint32_t>(result[i]);
    ASSERT_EQ(i, val);
  }

  /* trim one entry */
  r = f->trim(&dp, markers[min_entry], false, null_yield);
  ASSERT_EQ(0, r);
  ++min_entry;

  r = f->list(&dp, max_entries * 10, std::nullopt, &result, &more, null_yield);
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
  auto r = RCf::FIFO::create(&dp, ioctx, fifo_id, &f, null_yield, std::nullopt,
			     std::nullopt, false, max_part_size, max_entry_size);
  ASSERT_EQ(0, r);

  char buf[max_entry_size + 1];
  memset(buf, 0, sizeof(buf));

  cb::list bl;
  bl.append(buf, sizeof(buf));

  r = f->push(&dp, bl, null_yield);
  EXPECT_EQ(-E2BIG, r);
}


TEST_F(LegacyFIFO, TestMultipleParts)
{
  static constexpr auto max_part_size = 2048ull;
  static constexpr auto max_entry_size = 128ull;
  std::unique_ptr<RCf::FIFO> f;
  auto r = RCf::FIFO::create(&dp, ioctx, fifo_id, &f, null_yield, std::nullopt,
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
    r = f->push(&dp, bl, null_yield);
    ASSERT_EQ(0, r);
  }

  auto info = f->meta();
  ASSERT_EQ(info.id, fifo_id);
  /* head should have advanced */
  ASSERT_GT(info.head_part_num, 0);

  /* list all at once */
  std::vector<RCf::list_entry> result;
  bool more = false;
  r = f->list(&dp, max_entries, std::nullopt, &result, &more, null_yield);
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
    r = f->list(&dp, 1, marker, &result, &more, null_yield);
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
    r = f->list(&dp, 1, marker, &result, &more, null_yield);
    ASSERT_EQ(0, r);
    ASSERT_EQ(result.size(), 1);
    const bool expected_more = (i != (max_entries - 1));
    ASSERT_EQ(expected_more, more);

    marker = result.front().marker;
    r = f->trim(&dp, *marker, false, null_yield);
    ASSERT_EQ(0, r);

    /* check tail */
    info = f->meta();
    ASSERT_EQ(info.tail_part_num, i / entries_per_part);

    /* try to read all again, see how many entries left */
    r = f->list(&dp, max_entries, marker, &result, &more, null_yield);
    ASSERT_EQ(max_entries - i - 1, result.size());
    ASSERT_EQ(false, more);
  }

  /* tail now should point at head */
  info = f->meta();
  ASSERT_EQ(info.head_part_num, info.tail_part_num);

  RCf::part_info partinfo;
  /* check old tails are removed */
  for (auto i = 0; i < info.tail_part_num; ++i) {
    r = f->get_part_info(&dp, i, &partinfo, null_yield);
    ASSERT_EQ(-ENOENT, r);
  }
  /* check current tail exists */
  r = f->get_part_info(&dp, info.tail_part_num, &partinfo, null_yield);
  ASSERT_EQ(0, r);
}

TEST_F(LegacyFIFO, TestTwoPushers)
{
  static constexpr auto max_part_size = 2048ull;
  static constexpr auto max_entry_size = 128ull;

  std::unique_ptr<RCf::FIFO> f;
  auto r = RCf::FIFO::create(&dp, ioctx, fifo_id, &f, null_yield, std::nullopt,
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
  r = RCf::FIFO::open(&dp, ioctx, fifo_id, &f2, null_yield);
  std::vector fifos{&f, &f2};

  for (auto i = 0u; i < max_entries; ++i) {
    cb::list bl;
    *(int *)buf = i;
    bl.append(buf, sizeof(buf));
    auto& f = *fifos[i % fifos.size()];
    r = f->push(&dp, bl, null_yield);
    ASSERT_EQ(0, r);
  }

  /* list all by both */
  std::vector<RCf::list_entry> result;
  bool more = false;
  r = f2->list(&dp, max_entries, std::nullopt, &result, &more, null_yield);
  ASSERT_EQ(0, r);
  ASSERT_EQ(false, more);
  ASSERT_EQ(max_entries, result.size());

  r = f2->list(&dp, max_entries, std::nullopt, &result, &more, null_yield);
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
  auto r = RCf::FIFO::create(&dp, ioctx, fifo_id, &f1, null_yield, std::nullopt,
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
  r = RCf::FIFO::open(&dp, ioctx, fifo_id, &f2, null_yield);
  ASSERT_EQ(0, r);

  /* push one entry to f2 and the rest to f1 */
  for (auto i = 0u; i < max_entries; ++i) {
    cb::list bl;
    *(int *)buf = i;
    bl.append(buf, sizeof(buf));
    auto& f = (i < 1 ? f2 : f1);
    r = f->push(&dp, bl, null_yield);
    ASSERT_EQ(0, r);
  }

  /* trim half by fifo1 */
  auto num = max_entries / 2;
  std::string marker;
  std::vector<RCf::list_entry> result;
  bool more = false;
  r = f1->list(&dp, num, std::nullopt, &result, &more, null_yield);
  ASSERT_EQ(0, r);
  ASSERT_EQ(true, more);
  ASSERT_EQ(num, result.size());

  for (auto i = 0u; i < num; ++i) {
    auto& bl = result[i].data;
    ASSERT_EQ(i, *(int *)bl.c_str());
  }

  auto& entry = result[num - 1];
  marker = entry.marker;
  r = f1->trim(&dp, marker, false, null_yield);
  /* list what's left by fifo2 */

  const auto left = max_entries - num;
  f2->list(&dp, left, marker, &result, &more, null_yield);
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
  auto r = RCf::FIFO::create(&dp, ioctx, fifo_id, &f, null_yield, std::nullopt,
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
  ASSERT_EQ(max_entries, bufs.size());

  r = f->push(&dp, bufs, null_yield);
  ASSERT_EQ(0, r);

  /* list all */

  std::vector<RCf::list_entry> result;
  bool more = false;
  r = f->list(&dp, max_entries, std::nullopt, &result, &more, null_yield);
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
  auto r = RCf::FIFO::create(&dp, ioctx, fifo_id, &f, null_yield, std::nullopt,
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
  ASSERT_EQ(max_entries, bufs.size());

  r = f->push(&dp, bufs, null_yield);
  ASSERT_EQ(0, r);

  auto info = f->meta();
  ASSERT_EQ(info.id, fifo_id);
  /* head should have advanced */
  ASSERT_GT(info.head_part_num, 0);

  /* list all at once */
  std::vector<RCf::list_entry> result;
  bool more = false;
  r = f->list(&dp, max_entries, std::nullopt, &result, &more, null_yield);
  ASSERT_EQ(0, r);
  ASSERT_EQ(false, more);
  ASSERT_EQ(max_entries, result.size());

  std::optional<std::string> marker;
  /* trim one at a time */
  result.clear();
  more = false;
  marker.reset();
  for (auto i = 0u; i < max_entries; ++i) {
    /* read single entry */
    r = f->list(&dp, 1, marker, &result, &more, null_yield);
    ASSERT_EQ(0, r);
    ASSERT_EQ(result.size(), 1);
    const bool expected_more = (i != (max_entries - 1));
    ASSERT_EQ(expected_more, more);

    marker = result.front().marker;
    std::unique_ptr<R::AioCompletion> c(rados.aio_create_completion(nullptr,
								    nullptr));
    f->trim(&dp, *marker, false, c.get());
    c->wait_for_complete();
    r = c->get_return_value();
    ASSERT_EQ(0, r);

    /* check tail */
    info = f->meta();
    ASSERT_EQ(info.tail_part_num, i / entries_per_part);

    /* try to read all again, see how many entries left */
    r = f->list(&dp, max_entries, marker, &result, &more, null_yield);
    ASSERT_EQ(max_entries - i - 1, result.size());
    ASSERT_EQ(false, more);
  }

  /* tail now should point at head */
  info = f->meta();
  ASSERT_EQ(info.head_part_num, info.tail_part_num);

  RCf::part_info partinfo;
  /* check old tails are removed */
  for (auto i = 0; i < info.tail_part_num; ++i) {
    r = f->get_part_info(&dp, i, &partinfo, null_yield);
    ASSERT_EQ(-ENOENT, r);
  }
  /* check current tail exists */
  r = f->get_part_info(&dp, info.tail_part_num, &partinfo, null_yield);
  ASSERT_EQ(0, r);
}

TEST_F(LegacyFIFO, TestTrimExclusive) {
  std::unique_ptr<RCf::FIFO> f;
  auto r = RCf::FIFO::create(&dp, ioctx, fifo_id, &f, null_yield);
  ASSERT_EQ(0, r);
  std::vector<RCf::list_entry> result;
  bool more = false;

  static constexpr auto max_entries = 10u;
  for (uint32_t i = 0; i < max_entries; ++i) {
    cb::list bl;
    encode(i, bl);
    f->push(&dp, bl, null_yield);
  }

  f->list(&dp, 1, std::nullopt, &result, &more, null_yield);
  auto [val, marker] = decode_entry<std::uint32_t>(result.front());
  ASSERT_EQ(0, val);
  f->trim(&dp, marker, true, null_yield);

  result.clear();
  f->list(&dp, max_entries, std::nullopt, &result, &more, null_yield);
  std::tie(val, marker) = decode_entry<std::uint32_t>(result.front());
  ASSERT_EQ(0, val);
  f->trim(&dp, result[4].marker, true, null_yield);

  result.clear();
  f->list(&dp, max_entries, std::nullopt, &result, &more, null_yield);
  std::tie(val, marker) = decode_entry<std::uint32_t>(result.front());
  ASSERT_EQ(4, val);
  f->trim(&dp, result.back().marker, true, null_yield);

  result.clear();
  f->list(&dp, max_entries, std::nullopt, &result, &more, null_yield);
  std::tie(val, marker) = decode_entry<std::uint32_t>(result.front());
  ASSERT_EQ(result.size(), 1);
  ASSERT_EQ(max_entries - 1, val);
}

TEST_F(AioLegacyFIFO, TestPushListTrim)
{
  std::unique_ptr<RCf::FIFO> f;
  auto r = RCf::FIFO::create(&dp, ioctx, fifo_id, &f, null_yield);
  ASSERT_EQ(0, r);
  static constexpr auto max_entries = 10u;
  for (uint32_t i = 0; i < max_entries; ++i) {
    cb::list bl;
    encode(i, bl);
    auto c = R::Rados::aio_create_completion();
    f->push(&dp, bl, c);
    c->wait_for_complete();
    r = c->get_return_value();
    c->release();
    ASSERT_EQ(0, r);
  }

  std::optional<std::string> marker;
  /* get entries one by one */
  std::vector<RCf::list_entry> result;
  bool more = false;
  for (auto i = 0u; i < max_entries; ++i) {
    auto c = R::Rados::aio_create_completion();
    f->list(&dp, 1, marker, &result, &more, c);
    c->wait_for_complete();
    r = c->get_return_value();
    c->release();
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
  auto c = R::Rados::aio_create_completion();
  f->list(&dp, max_entries * 10, std::nullopt, &result, &more, c);
  c->wait_for_complete();
  r = c->get_return_value();
  c->release();
  ASSERT_EQ(0, r);

  ASSERT_FALSE(more);
  ASSERT_EQ(max_entries, result.size());
  for (auto i = 0u; i < max_entries; ++i) {
    std::uint32_t val;
    std::tie(val, markers[i]) = decode_entry<std::uint32_t>(result[i]);
    ASSERT_EQ(i, val);
  }

  /* trim one entry */
  c = R::Rados::aio_create_completion();
  f->trim(&dp, markers[min_entry], false, c);
  c->wait_for_complete();
  r = c->get_return_value();
  c->release();
  ASSERT_EQ(0, r);
  ++min_entry;

  c = R::Rados::aio_create_completion();
  f->list(&dp, max_entries * 10, std::nullopt, &result, &more, c);
  c->wait_for_complete();
  r = c->get_return_value();
  c->release();
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


TEST_F(AioLegacyFIFO, TestPushTooBig)
{
  static constexpr auto max_part_size = 2048ull;
  static constexpr auto max_entry_size = 128ull;

  std::unique_ptr<RCf::FIFO> f;
  auto r = RCf::FIFO::create(&dp, ioctx, fifo_id, &f, null_yield, std::nullopt,
			     std::nullopt, false, max_part_size, max_entry_size);
  ASSERT_EQ(0, r);

  char buf[max_entry_size + 1];
  memset(buf, 0, sizeof(buf));

  cb::list bl;
  bl.append(buf, sizeof(buf));

  auto c = R::Rados::aio_create_completion();
  f->push(&dp, bl, c);
  c->wait_for_complete();
  r = c->get_return_value();
  ASSERT_EQ(-E2BIG, r);
  c->release();

  c = R::Rados::aio_create_completion();
  f->push(&dp, std::vector<cb::list>{}, c);
  c->wait_for_complete();
  r = c->get_return_value();
  c->release();
  EXPECT_EQ(0, r);
}


TEST_F(AioLegacyFIFO, TestMultipleParts)
{
  static constexpr auto max_part_size = 2048ull;
  static constexpr auto max_entry_size = 128ull;
  std::unique_ptr<RCf::FIFO> f;
  auto r = RCf::FIFO::create(&dp, ioctx, fifo_id, &f, null_yield, std::nullopt,
			     std::nullopt, false, max_part_size,
			     max_entry_size);
  ASSERT_EQ(0, r);

  {
    auto c = R::Rados::aio_create_completion();
    f->get_head_info(&dp, [&](int r, RCf::part_info&& p) {
      ASSERT_EQ(0, p.magic);
      ASSERT_EQ(0, p.min_ofs);
      ASSERT_EQ(0, p.last_ofs);
      ASSERT_EQ(0, p.next_ofs);
      ASSERT_EQ(0, p.min_index);
      ASSERT_EQ(0, p.max_index);
      ASSERT_EQ(ceph::real_time{}, p.max_time);
    }, c);
    c->wait_for_complete();
    r = c->get_return_value();
    c->release();
  }

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
    auto c = R::Rados::aio_create_completion();
    f->push(&dp, bl, c);
    c->wait_for_complete();
    r = c->get_return_value();
    c->release();
    EXPECT_EQ(0, r);
  }

  auto info = f->meta();
  ASSERT_EQ(info.id, fifo_id);
  /* head should have advanced */
  ASSERT_GT(info.head_part_num, 0);

  /* list all at once */
  std::vector<RCf::list_entry> result;
  bool more = false;
  auto c = R::Rados::aio_create_completion();
  f->list(&dp, max_entries, std::nullopt, &result, &more, c);
  c->wait_for_complete();
  r = c->get_return_value();
  c->release();
  EXPECT_EQ(0, r);
  EXPECT_EQ(false, more);
  ASSERT_EQ(max_entries, result.size());

  for (auto i = 0u; i < max_entries; ++i) {
    auto& bl = result[i].data;
    ASSERT_EQ(i, *(int *)bl.c_str());
  }

  std::optional<std::string> marker;
  /* get entries one by one */

  for (auto i = 0u; i < max_entries; ++i) {
    c = R::Rados::aio_create_completion();
    f->list(&dp, 1, marker, &result, &more, c);
    c->wait_for_complete();
    r = c->get_return_value();
    c->release();
    EXPECT_EQ(0, r);
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
    c = R::Rados::aio_create_completion();
    f->list(&dp, 1, marker, &result, &more, c);
    c->wait_for_complete();
    r = c->get_return_value();
    c->release();
    EXPECT_EQ(0, r);
    ASSERT_EQ(result.size(), 1);
    const bool expected_more = (i != (max_entries - 1));
    ASSERT_EQ(expected_more, more);

    marker = result.front().marker;
    c = R::Rados::aio_create_completion();
    f->trim(&dp, *marker, false, c);
    c->wait_for_complete();
    r = c->get_return_value();
    c->release();
    EXPECT_EQ(0, r);
    ASSERT_EQ(result.size(), 1);

    /* check tail */
    info = f->meta();
    ASSERT_EQ(info.tail_part_num, i / entries_per_part);

    /* try to read all again, see how many entries left */
    c = R::Rados::aio_create_completion();
    f->list(&dp, max_entries, marker, &result, &more, c);
    c->wait_for_complete();
    r = c->get_return_value();
    c->release();
    EXPECT_EQ(0, r);
    ASSERT_EQ(max_entries - i - 1, result.size());
    ASSERT_EQ(false, more);
  }

  /* tail now should point at head */
  info = f->meta();
  ASSERT_EQ(info.head_part_num, info.tail_part_num);

  /* check old tails are removed */
  for (auto i = 0; i < info.tail_part_num; ++i) {
    c = R::Rados::aio_create_completion();
    RCf::part_info partinfo;
    f->get_part_info(i, &partinfo, c);
    c->wait_for_complete();
    r = c->get_return_value();
    c->release();
    ASSERT_EQ(-ENOENT, r);
  }
  /* check current tail exists */
  std::uint64_t next_ofs;
  {
    c = R::Rados::aio_create_completion();
    RCf::part_info partinfo;
    f->get_part_info(info.tail_part_num, &partinfo, c);
    c->wait_for_complete();
    r = c->get_return_value();
    c->release();
    next_ofs = partinfo.next_ofs;
  }
  ASSERT_EQ(0, r);

  c = R::Rados::aio_create_completion();
  f->get_head_info(&dp, [&](int r, RCf::part_info&& p) {
    ASSERT_EQ(next_ofs, p.next_ofs);
  }, c);
  c->wait_for_complete();
  r = c->get_return_value();
  c->release();
  ASSERT_EQ(0, r);
}

TEST_F(AioLegacyFIFO, TestTwoPushers)
{
  static constexpr auto max_part_size = 2048ull;
  static constexpr auto max_entry_size = 128ull;

  std::unique_ptr<RCf::FIFO> f;
  auto r = RCf::FIFO::create(&dp, ioctx, fifo_id, &f, null_yield, std::nullopt,
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
  r = RCf::FIFO::open(&dp, ioctx, fifo_id, &f2, null_yield);
  std::vector fifos{&f, &f2};

  for (auto i = 0u; i < max_entries; ++i) {
    cb::list bl;
    *(int *)buf = i;
    bl.append(buf, sizeof(buf));
    auto& f = *fifos[i % fifos.size()];
    auto c = R::Rados::aio_create_completion();
    f->push(&dp, bl, c);
    c->wait_for_complete();
    r = c->get_return_value();
    c->release();
    ASSERT_EQ(0, r);
  }

  /* list all by both */
  std::vector<RCf::list_entry> result;
  bool more = false;
  auto c = R::Rados::aio_create_completion();
  f2->list(&dp, max_entries, std::nullopt, &result, &more, c);
  c->wait_for_complete();
  r = c->get_return_value();
  c->release();
  ASSERT_EQ(0, r);
  ASSERT_EQ(false, more);
  ASSERT_EQ(max_entries, result.size());

  c = R::Rados::aio_create_completion();
  f2->list(&dp, max_entries, std::nullopt, &result, &more, c);
  c->wait_for_complete();
  r = c->get_return_value();
  c->release();
  ASSERT_EQ(0, r);
  ASSERT_EQ(false, more);
  ASSERT_EQ(max_entries, result.size());

  for (auto i = 0u; i < max_entries; ++i) {
    auto& bl = result[i].data;
    ASSERT_EQ(i, *(int *)bl.c_str());
  }
}

TEST_F(AioLegacyFIFO, TestTwoPushersTrim)
{
  static constexpr auto max_part_size = 2048ull;
  static constexpr auto max_entry_size = 128ull;
  std::unique_ptr<RCf::FIFO> f1;
  auto r = RCf::FIFO::create(&dp, ioctx, fifo_id, &f1, null_yield, std::nullopt,
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
  r = RCf::FIFO::open(&dp, ioctx, fifo_id, &f2, null_yield);
  ASSERT_EQ(0, r);

  /* push one entry to f2 and the rest to f1 */
  for (auto i = 0u; i < max_entries; ++i) {
    cb::list bl;
    *(int *)buf = i;
    bl.append(buf, sizeof(buf));
    auto& f = (i < 1 ? f2 : f1);
    auto c = R::Rados::aio_create_completion();
    f->push(&dp, bl, c);
    c->wait_for_complete();
    r = c->get_return_value();
    c->release();
    ASSERT_EQ(0, r);
  }

  /* trim half by fifo1 */
  auto num = max_entries / 2;
  std::string marker;
  std::vector<RCf::list_entry> result;
  bool more = false;
  auto c = R::Rados::aio_create_completion();
  f1->list(&dp, num, std::nullopt, &result, &more, c);
  c->wait_for_complete();
  r = c->get_return_value();
  c->release();
  ASSERT_EQ(0, r);
  ASSERT_EQ(true, more);
  ASSERT_EQ(num, result.size());

  for (auto i = 0u; i < num; ++i) {
    auto& bl = result[i].data;
    ASSERT_EQ(i, *(int *)bl.c_str());
  }

  auto& entry = result[num - 1];
  marker = entry.marker;
  c = R::Rados::aio_create_completion();
  f1->trim(&dp, marker, false, c);
  c->wait_for_complete();
  r = c->get_return_value();
  c->release();
  ASSERT_EQ(0, r);
  /* list what's left by fifo2 */

  const auto left = max_entries - num;
  c = R::Rados::aio_create_completion();
  f2->list(&dp, left, marker, &result, &more, c);
  c->wait_for_complete();
  r = c->get_return_value();
  c->release();
  ASSERT_EQ(0, r);
  ASSERT_EQ(left, result.size());
  ASSERT_EQ(false, more);

  for (auto i = num; i < max_entries; ++i) {
    auto& bl = result[i - num].data;
    ASSERT_EQ(i, *(int *)bl.c_str());
  }
}

TEST_F(AioLegacyFIFO, TestPushBatch)
{
  static constexpr auto max_part_size = 2048ull;
  static constexpr auto max_entry_size = 128ull;

  std::unique_ptr<RCf::FIFO> f;
  auto r = RCf::FIFO::create(&dp, ioctx, fifo_id, &f, null_yield, std::nullopt,
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
  ASSERT_EQ(max_entries, bufs.size());

  auto c = R::Rados::aio_create_completion();
  f->push(&dp, bufs, c);
  c->wait_for_complete();
  r = c->get_return_value();
  c->release();
  ASSERT_EQ(0, r);

  /* list all */

  std::vector<RCf::list_entry> result;
  bool more = false;
  c = R::Rados::aio_create_completion();
  f->list(&dp, max_entries, std::nullopt, &result, &more, c);
  c->wait_for_complete();
  r = c->get_return_value();
  c->release();
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

TEST_F(LegacyFIFO, TrimAll)
{
  std::unique_ptr<RCf::FIFO> f;
  auto r = RCf::FIFO::create(&dp, ioctx, fifo_id, &f, null_yield);
  ASSERT_EQ(0, r);
  static constexpr auto max_entries = 10u;
  for (uint32_t i = 0; i < max_entries; ++i) {
    cb::list bl;
    encode(i, bl);
    r = f->push(&dp, bl, null_yield);
    ASSERT_EQ(0, r);
  }

  /* trim one entry */
  r = f->trim(&dp, RCf::marker::max().to_string(), false, null_yield);
  ASSERT_EQ(-ENODATA, r);

  std::vector<RCf::list_entry> result;
  bool more;
  r = f->list(&dp, 1, std::nullopt, &result, &more, null_yield);
  ASSERT_EQ(0, r);
  ASSERT_TRUE(result.empty());
}

TEST_F(LegacyFIFO, AioTrimAll)
{
  std::unique_ptr<RCf::FIFO> f;
  auto r = RCf::FIFO::create(&dp, ioctx, fifo_id, &f, null_yield);
  ASSERT_EQ(0, r);
  static constexpr auto max_entries = 10u;
  for (uint32_t i = 0; i < max_entries; ++i) {
    cb::list bl;
    encode(i, bl);
    r = f->push(&dp, bl, null_yield);
    ASSERT_EQ(0, r);
  }

  auto c = R::Rados::aio_create_completion();
  f->trim(&dp, RCf::marker::max().to_string(), false, c);
  c->wait_for_complete();
  r = c->get_return_value();
  c->release();
  ASSERT_EQ(-ENODATA, r);

  std::vector<RCf::list_entry> result;
  bool more;
  r = f->list(&dp, 1, std::nullopt, &result, &more, null_yield);
  ASSERT_EQ(0, r);
  ASSERT_TRUE(result.empty());
}
