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

#include "neorados/cls/fifo.h"

#include <array>
#include <boost/system/detail/errc.hpp>
#include <coroutine>
#include <memory>
#include <new>
#include <string_view>
#include <utility>

#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <boost/system/errc.hpp>
#include <boost/system/error_code.hpp>

#include "include/neorados/RADOS.hpp"

#include "cls/version/cls_version_types.h"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

namespace asio = boost::asio;
namespace sys = boost::system;
namespace buffer = ceph::buffer;
namespace fifo = neorados::cls::fifo;

using namespace std::literals;

namespace neorados::cls::fifo {
class FIFOtest {
public:
  template<typename... Args>
  static auto create_meta(Args&&... args) {
    return FIFO::create_meta(std::forward<Args>(args)...);
  }

  template<typename... Args>
  static auto get_meta(Args&&... args) {
    return FIFO::get_meta(std::forward<Args>(args)...);
  }

  template<typename... Args>
  static auto read_meta(fifo::FIFO& f, Args&&... args) {
    return f.read_meta(std::forward<Args>(args)...);
  }

  static auto meta(fifo::FIFO& f) {
    return f.info;
  }

  template<typename... Args>
  static auto get_part_info(fifo::FIFO& f, Args&&... args) {
    return f.get_part_info(std::forward<Args>(args)...);
  }

  static auto get_part_layout_info(fifo::FIFO& f) {
    return std::make_tuple(f.part_header_size, f.part_entry_overhead);
  }

};
}

using fifo::FIFOtest;

auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);
const DoutPrefix dp(cct, 1, "test legacy cls fifo: ");

CORO_TEST_F(cls_fifo, create, NeoRadosTest)
{
  std::string_view fifo_id = "fifo";


  sys::error_code ec;
  co_await FIFOtest::create_meta(rados(), fifo_id, pool(), std::nullopt,
                                 std::nullopt, false, 0,
                                 fifo::FIFO::default_max_entry_size,
                                 asio::redirect_error(asio::use_awaitable, ec));
  EXPECT_EQ(sys::errc::invalid_argument, ec);
  co_await FIFOtest::create_meta(rados(), fifo_id, pool(), std::nullopt,
                                 std::nullopt, false,
                                 fifo::FIFO::default_max_part_size, 0,
                                 asio::redirect_error(asio::use_awaitable, ec));

  EXPECT_EQ(sys::errc::invalid_argument, ec);
  co_await FIFOtest::create_meta(rados(), fifo_id, pool(), std::nullopt,
                                 std::nullopt, false,
                                 fifo::FIFO::default_max_part_size,
                                 fifo::FIFO::default_max_entry_size,
                                 asio::redirect_error(asio::use_awaitable, ec));
  EXPECT_FALSE(ec);
  neorados::ReadOp op;
  std::uint64_t size;
  op.stat(&size, nullptr);
  co_await execute(fifo_id, std::move(op), nullptr);
  EXPECT_GT(size, 0);
  /* test idempotency */
  co_await FIFOtest::create_meta(rados(), fifo_id, pool(), std::nullopt,
                                 std::nullopt, false,
                                 fifo::FIFO::default_max_part_size,
                                 fifo::FIFO::default_max_entry_size,
                                 asio::redirect_error(asio::use_awaitable, ec));
  EXPECT_FALSE(ec);
}

CORO_TEST_F(cls_fifo, get_info, NeoRadosTest)
{
  std::string_view fifo_id = "fifo";

  co_await FIFOtest::create_meta(rados(), fifo_id, pool(), std::nullopt,
                                 std::nullopt, false,
                                 fifo::FIFO::default_max_part_size,
                                 fifo::FIFO::default_max_entry_size,
                                 asio::use_awaitable);
  auto [info, part_header_size, part_entry_overhead] =
    co_await FIFOtest::get_meta(rados(), fifo_id, pool(), std::nullopt,
                                asio::use_awaitable);
  EXPECT_GT(part_header_size, 0);
  EXPECT_GT(part_entry_overhead, 0);
  EXPECT_FALSE(info.version.instance.empty());

  std::tie(info, part_header_size, part_entry_overhead) =
    co_await FIFOtest::get_meta(rados(), fifo_id, pool(), info.version,
                                asio::use_awaitable);


  decltype(info.version) objv;
  objv.instance = "foo";
  objv.ver = 12;

  sys::error_code ec;
  std::tie(info, part_header_size, part_entry_overhead) =
    co_await FIFOtest::get_meta(rados(), fifo_id, pool(), objv,
                                asio::redirect_error(asio::use_awaitable, ec));

  EXPECT_EQ(sys::errc::operation_canceled, ec);
}

CORO_TEST_F(fifo, open_default, NeoRadosTest)
{
  std::string_view fifo_id = "fifo";

  auto f = co_await fifo::FIFO::create(&dp, rados(), fifo_id, pool(),
                                       asio::use_awaitable);
  // force reading from backend
  co_await FIFOtest::read_meta(*f, &dp, asio::use_awaitable);
  auto info = FIFOtest::meta(*f);
  EXPECT_EQ(info.id, fifo_id);
}

CORO_TEST_F(fifo, open_params, NeoRadosTest)
{
  std::string_view fifo_id = "fifo";

  const std::uint64_t max_part_size = 10 * 1024;
  const std::uint64_t max_entry_size = 128;
  auto oid_prefix = "foo.123."s;
  rados::cls::fifo::objv objv;
  objv.instance = "fooz"s;
  objv.ver = 10;

  auto f = co_await fifo::FIFO::create(&dp, rados(), fifo_id, pool(),
                                       asio::use_awaitable,
                                       objv, oid_prefix, false,
                                       max_part_size, max_entry_size);

  // force reading from backend
  co_await FIFOtest::read_meta(*f, &dp, asio::use_awaitable);
  auto info = FIFOtest::meta(*f);
  EXPECT_EQ(info.id, fifo_id);
  EXPECT_EQ(info.params.max_part_size, max_part_size);
  EXPECT_EQ(info.params.max_entry_size, max_entry_size);
  EXPECT_EQ(info.version, objv);
}

template<class T>
inline T decode_entry(const fifo::entry& entry)
{
  T val;
  auto iter = entry.data.cbegin();
  decode(val, iter);
  return std::move(val);
}

CORO_TEST_F(fifo, push_list_trim, NeoRadosTest)
{
  std::string_view fifo_id = "fifo";

  auto f = co_await fifo::FIFO::create(&dp, rados(), fifo_id, pool(),
                                       asio::use_awaitable);
  static constexpr auto max_entries = 10u;
  for (auto i = 0u; i < max_entries; ++i) {
    buffer::list bl;
    encode(i, bl);
    co_await f->push(&dp, std::move(bl), asio::use_awaitable);
  }

  std::array<fifo::entry, max_entries> entries;
  /* get entries one by one */
  std::optional<std::string> inmark;
  for (auto i = 0u; i < max_entries; ++i) {
    auto [result, marker] =
      co_await f->list(&dp, inmark, std::span{entries}.first(1),
		       asio::use_awaitable);
    bool expected_marker = (i != (max_entries - 1));
    EXPECT_EQ(expected_marker, !!marker);
    EXPECT_EQ(1, result.size());

    auto val = decode_entry<std::uint32_t>(result.front());

    EXPECT_EQ(i, val);
    inmark = marker;
  }

  /* get all entries at once */
  std::array<std::string, max_entries> markers;

  std::uint32_t min_entry = 0;
  auto [res, marker] = co_await f->list(&dp, {}, entries,
                                        asio::use_awaitable);

  EXPECT_FALSE(marker);
  EXPECT_EQ(max_entries, res.size());
  for (auto i = 0u; i < max_entries; ++i) {
    auto val = decode_entry<std::uint32_t>(res[i]);
    markers[i] = res[i].marker;
    EXPECT_EQ(i, val);
  }

  /* trim one entry */
  co_await f->trim(&dp, markers[min_entry], false, asio::use_awaitable);
  ++min_entry;

  std::tie(res, marker) = co_await f->list(&dp, {}, entries,
                                           asio::use_awaitable);

  EXPECT_FALSE(marker);
  EXPECT_EQ(max_entries - min_entry, res.size());

  for (auto i = min_entry; i < max_entries; ++i) {
    auto val = decode_entry<std::uint32_t>(res[i - min_entry]);
    markers[i - min_entry] = res[i - min_entry].marker;
    EXPECT_EQ(i, val);
  }
}

CORO_TEST_F(fifo, push_too_big, NeoRadosTest)
{
  static constexpr auto max_part_size = 2048ull;
  static constexpr auto max_entry_size = 128ull;
  std::string_view fifo_id = "fifo";

  auto f = co_await fifo::FIFO::create(&dp, rados(), fifo_id, pool(),
                                       asio::use_awaitable, std::nullopt,
				       std::nullopt, false, max_part_size,
				       max_entry_size);

  std::array<char, max_entry_size + 1> buf;
  buf.fill('\0');
  buffer::list bl;
  bl.append(buf.data(), sizeof(buf));

  sys::error_code ec;
  co_await f->push(&dp, bl, asio::redirect_error(asio::use_awaitable, ec));
  EXPECT_EQ(sys::errc::argument_list_too_long, ec);
}

CORO_TEST_F(fifo, multiple_parts, NeoRadosTest)
{
  static constexpr auto max_part_size = 2048ull;
  static constexpr auto max_entry_size = 128ull;
  std::string_view fifo_id = "fifo";

  auto f = co_await fifo::FIFO::create(&dp, rados(), fifo_id, pool(),
                                       asio::use_awaitable, std::nullopt,
				       std::nullopt, false, max_part_size,
				       max_entry_size);
  std::array<char, max_entry_size> buf;
  buf.fill('\0');
  const auto [part_header_size, part_entry_overhead] =
    FIFOtest::get_part_layout_info(*f);
  const auto entries_per_part = ((max_part_size - part_header_size) /
				 (max_entry_size + part_entry_overhead));
  const auto max_entries = entries_per_part * 4 + 1;
  /* push enough entries */
  for (auto i = 0u; i < max_entries; ++i) {
    buffer::list bl;
    *std::launder(reinterpret_cast<int*>(buf.data())) = i;
    bl.append(buf.data(), sizeof(buf));
    co_await f->push(&dp, bl, asio::use_awaitable);
  }

  auto info = FIFOtest::meta(*f);
  EXPECT_EQ(info.id, fifo_id);
  /* head should have advanced */
  EXPECT_GT(info.head_part_num, 0);

  std::vector<fifo::entry> entries{max_entries};
  {
    /* list all at once */
    auto [result, marker] = co_await f->list(&dp, {}, entries,
					     asio::use_awaitable);

    EXPECT_FALSE(marker);
    EXPECT_EQ(max_entries, result.size());

    for (auto i = 0u; i < max_entries; ++i) {
      auto& bl = result[i].data;
      EXPECT_EQ(i, *std::launder(reinterpret_cast<int*>(bl.c_str())));
    }
  }

  std::optional<std::string> marker;
  /* get entries one by one */
  for (auto i = 0u; i < max_entries; ++i) {
    std::span<fifo::entry> result;
    std::tie(result, marker) = co_await f->list(&dp, marker,
						std::span(entries).first(1),
						asio::use_awaitable);

    EXPECT_EQ(1, result.size());
    const bool expected_more = (i != (max_entries - 1));
    EXPECT_EQ(expected_more, !!marker);

    auto& e = result.front();
    auto& bl = e.data;
    EXPECT_EQ(i, *std::launder(reinterpret_cast<int*>(bl.c_str())));
  }

  /* trim one at a time */
  marker.reset();
  for (auto i = 0u; i < max_entries; ++i) {
    /* read single entry */
    std::span<fifo::entry> result;
    std::tie(result, marker) = co_await f->list(&dp, {},
						std::span(entries).first(1),
						asio::use_awaitable);
    EXPECT_EQ(result.size(), 1);
    const bool expected_more = (i != (max_entries - 1));
    EXPECT_EQ(expected_more, !!marker);

    /* trim one entry */
    co_await f->trim(&dp, result.front().marker, false, asio::use_awaitable);

    /* check tail */
    info = FIFOtest::meta(*f);

    EXPECT_EQ(info.tail_part_num, i / entries_per_part);

    /* try to read all again, see how many entries left */
    std::tie(result, marker) = co_await f->list(&dp, marker, entries,
						asio::use_awaitable);
    EXPECT_EQ(max_entries - i - 1, result.size());
    EXPECT_FALSE(!!marker);
  }

  /* tail now should point at head */
  info = FIFOtest::meta(*f);
  EXPECT_EQ(info.head_part_num, info.tail_part_num);

  /* check old tails are removed */
  for (auto i = 0; i < info.tail_part_num; ++i) {
    sys::error_code ec;
    co_await FIFOtest::get_part_info(*f, i, asio::redirect_error(
				       asio::use_awaitable, ec));
    EXPECT_EQ(sys::errc::no_such_file_or_directory, ec);
  }
  /* check current tail exists */
  co_await FIFOtest::get_part_info(*f, info.tail_part_num, asio::use_awaitable);
}

CORO_TEST_F(fifo, two_pushers, NeoRadosTest)
{
  static constexpr auto max_part_size = 2048ull;
  static constexpr auto max_entry_size = 128ull;
  std::string_view fifo_id = "fifo";

  auto f1 = co_await fifo::FIFO::create(&dp, rados(), fifo_id, pool(),
					asio::use_awaitable, std::nullopt,
					std::nullopt, false, max_part_size,
					max_entry_size);
  std::array<char, max_entry_size> buf;
  buf.fill('\0');
  const auto [part_header_size, part_entry_overhead] =
    FIFOtest::get_part_layout_info(*f1);
  const auto entries_per_part = ((max_part_size - part_header_size) /
				 (max_entry_size + part_entry_overhead));
  const auto max_entries = entries_per_part * 4 + 1;


  auto f2 = co_await fifo::FIFO::open(&dp, rados(), fifo_id, pool(),
				      asio::use_awaitable);
  std::vector fifos{f1.get(), f2.get()};

  for (auto i = 0u; i < max_entries; ++i) {
    buffer::list bl;
    *std::launder(reinterpret_cast<int*>(buf.data())) = i;
    bl.append(buf.data(), sizeof(buf));
    auto f = fifos[i % fifos.size()];
    co_await f->push(&dp, bl, asio::use_awaitable);
  }

  /* list all by both */
  std::vector<fifo::entry> entries{max_entries};
  auto [result, marker] = co_await f1->list(&dp, std::nullopt, entries,
					    asio::use_awaitable);
  EXPECT_FALSE(marker);
  EXPECT_EQ(max_entries, result.size());

  std::tie(result, marker) = co_await f2->list(&dp, std::nullopt, entries,
					       asio::use_awaitable);
  EXPECT_FALSE(marker);
  EXPECT_EQ(max_entries, result.size());

  for (auto i = 0u; i < max_entries; ++i) {
    auto& bl = result[i].data;
    EXPECT_EQ(i, *std::launder(reinterpret_cast<int*>(bl.c_str())));
  }
}

CORO_TEST_F(fifo, two_pushers_trim, NeoRadosTest)
{
  static constexpr auto max_part_size = 2048ull;
  static constexpr auto max_entry_size = 128ull;
  std::string_view fifo_id = "fifo";

  auto f1 = co_await fifo::FIFO::create(&dp, rados(), fifo_id, pool(),
					asio::use_awaitable, std::nullopt,
					std::nullopt, false, max_part_size,
					max_entry_size);
  std::array<char, max_entry_size> buf;
  buf.fill('\0');
  const auto [part_header_size, part_entry_overhead] =
    FIFOtest::get_part_layout_info(*f1);
  const auto entries_per_part = ((max_part_size - part_header_size) /
				 (max_entry_size + part_entry_overhead));
  const auto max_entries = entries_per_part * 4 + 1;


  auto f2 = co_await fifo::FIFO::open(&dp, rados(), fifo_id, pool(),
				      asio::use_awaitable);
  /* push one entry to f2 and the rest to f1 */
  for (auto i = 0u; i < max_entries; ++i) {
    buffer::list bl;
    *std::launder(reinterpret_cast<int*>(buf.data())) = i;
    bl.append(buf.data(), sizeof(buf));
    auto& f = (i < 1 ? f2 : f1);
    co_await f->push(&dp, bl, asio::use_awaitable);
  }

  /* trim half by fifo1 */
  auto num = max_entries / 2;
  std::vector<fifo::entry> entries{max_entries};
  auto [result, marker] = co_await f1->list(&dp, std::nullopt,
					    std::span(entries).first(num),
					    asio::use_awaitable);
  EXPECT_TRUE(marker);
  EXPECT_EQ(num, result.size());

  for (auto i = 0u; i < num; ++i) {
    auto& bl = result[i].data;
    EXPECT_EQ(i, *std::launder(reinterpret_cast<int*>(bl.c_str())));
  }

  auto& entry = result[num - 1];
  EXPECT_EQ(marker, entry.marker);
  co_await f1->trim(&dp, *marker, false, asio::use_awaitable);
  /* list what's left by fifo2 */

  const auto left = max_entries - num;
  std::tie(result, marker) = co_await f2->list(&dp, *marker,
					       std::span(entries).first(left),
					       asio::use_awaitable);

  EXPECT_EQ(left, result.size());
  EXPECT_FALSE(marker);

  for (auto i = num; i < max_entries; ++i) {
    auto& bl = result[i - num].data;
    EXPECT_EQ(i, *std::launder(reinterpret_cast<int*>(bl.c_str())));
  }
}

CORO_TEST_F(fifo, push_batch, NeoRadosTest)
{
  static constexpr auto max_part_size = 2048ull;
  static constexpr auto max_entry_size = 128ull;
  std::string_view fifo_id = "fifo";

  auto f = co_await fifo::FIFO::create(&dp, rados(), fifo_id, pool(),
				       asio::use_awaitable, std::nullopt,
				       std::nullopt, false, max_part_size,
				       max_entry_size);
  std::array<char, max_entry_size> buf;
  buf.fill('\0');
  const auto [part_header_size, part_entry_overhead] =
    FIFOtest::get_part_layout_info(*f);
  const auto entries_per_part = ((max_part_size - part_header_size) /
				 (max_entry_size + part_entry_overhead));
  const auto max_entries = entries_per_part * 4 + 1;


  std::deque<buffer::list> bufs;
  for (auto i = 0u; i < max_entries; ++i) {
    buffer::list bl;
    *std::launder(reinterpret_cast<int*>(buf.data())) = i;
    bl.append(buf.data(), sizeof(buf));
    bufs.push_back(bl);
  }
  EXPECT_EQ(max_entries, bufs.size());
  co_await f->push(&dp, std::move(bufs), asio::use_awaitable);

  /* list all */
  std::vector<fifo::entry> entries{max_entries};
  auto [result, marker] = co_await f->list(&dp, std::nullopt, entries,
					   asio::use_awaitable);
  EXPECT_FALSE(marker);
  EXPECT_EQ(max_entries, result.size());
  for (auto i = 0u; i < max_entries; ++i) {
    auto& bl = result[i].data;
    EXPECT_EQ(i, *std::launder(reinterpret_cast<int*>(bl.c_str())));
  }
  auto info = FIFOtest::meta(*f);
  EXPECT_EQ(info.head_part_num, 4);
}

CORO_TEST_F(fifo, trim_exclusive, NeoRadosTest)
{
  std::string_view fifo_id = "fifo";
  auto f = co_await fifo::FIFO::create(&dp, rados(), fifo_id, pool(),
				       asio::use_awaitable);

  static constexpr auto max_entries = 10u;
  for (auto i = 0u; i < max_entries; ++i) {
    buffer::list bl;
    encode(i, bl);
    co_await f->push(&dp, std::move(bl), asio::use_awaitable);
  }

  std::array<fifo::entry, max_entries> entries;
  auto [result, marker] = co_await f->list(&dp, std::nullopt,
					   std::span{entries}.first(1),
					   asio::use_awaitable);
  auto val = decode_entry<std::uint32_t>(result.front());
  EXPECT_EQ(0, val);
  EXPECT_EQ(*marker, result.front().marker);

  co_await f->trim(&dp, *marker, true, asio::use_awaitable);

  std::tie(result, marker) = co_await f->list(&dp, std::nullopt, entries,
					      asio::use_awaitable);
  val = decode_entry<std::uint32_t>(result.front());
  EXPECT_EQ(0, val);
  co_await f->trim(&dp, result[4].marker, true, asio::use_awaitable);

  std::tie(result, marker) = co_await f->list(&dp, std::nullopt, entries,
					      asio::use_awaitable);
  val = decode_entry<std::uint32_t>(result.front());
  EXPECT_EQ(4, val);
  co_await f->trim(&dp, result.back().marker, true, asio::use_awaitable);

  std::tie(result, marker) = co_await f->list(&dp, std::nullopt, entries,
					      asio::use_awaitable);
  val = decode_entry<std::uint32_t>(result.front());
  EXPECT_EQ(1, result.size());
  EXPECT_EQ(max_entries - 1, val);
}

CORO_TEST_F(fifo, trim_all, NeoRadosTest)
{
  std::string_view fifo_id = "fifo";
  auto f = co_await fifo::FIFO::create(&dp, rados(), fifo_id, pool(),
				       asio::use_awaitable);

  static constexpr auto max_entries = 10u;
  for (auto i = 0u; i < max_entries; ++i) {
    buffer::list bl;
    encode(i, bl);
    co_await f->push(&dp, std::move(bl), asio::use_awaitable);
  }

  sys::error_code ec;
  co_await f->trim(&dp, f->max_marker(), false,
		   asio::redirect_error(asio::use_awaitable, ec));
  EXPECT_EQ(sys::errc::no_message_available, ec);

  std::array<fifo::entry, max_entries> entries;
  auto [result, marker] = co_await f->list(&dp, std::nullopt, entries,
					   asio::use_awaitable);
  EXPECT_TRUE(result.empty());
  EXPECT_FALSE(marker);
}

TEST(neocls_fifo_bare, lambdata)
{
  asio::io_context c;

  std::optional<neorados::RADOS> rados;
  neorados::IOContext pool;
  std::string_view fifo_id = "fifo";
  std::unique_ptr<fifo::FIFO> f;
  static constexpr auto max_entries = 10u;
  std::array<fifo::entry, max_entries> list_entries;
  bool completed = false;
  neorados::RADOS::Builder{}.build(
    c,
    [&](sys::error_code ec, neorados::RADOS r_) {
      ASSERT_FALSE(ec);
      rados = std::move(r_);
      create_pool(
	*rados, get_temp_pool_name(),
	[&](sys::error_code ec, int64_t poolid) {
	  ASSERT_FALSE(ec);
	  pool.set_pool(poolid);
	  fifo::FIFO::create(
	    &dp, *rados, fifo_id, pool,
	    [&](sys::error_code ec, std::unique_ptr<fifo::FIFO> f_) {
	      ASSERT_FALSE(ec);
	      f = std::move(f_);
	      std::array<buffer::list, max_entries> entries;
	      for (auto i = 0u; i < max_entries; ++i) {
		encode(i, entries[i]);
	      }
	      f->push(
		&dp, entries,
		[&](sys::error_code ec) {
		  ASSERT_FALSE(ec);
		  f->list(
		    &dp, std::nullopt, list_entries,
		    [&](sys::error_code ec, std::span<fifo::entry> result,
			std::optional<std::string> marker) {
		      ASSERT_FALSE(ec);
		      ASSERT_EQ(max_entries, result.size());
		      ASSERT_FALSE(marker);
		      for (auto i = 0u; i < max_entries; ++i) {
			auto val = decode_entry<std::uint32_t>(result[i]);
			EXPECT_EQ(i, val);
		      }
		      f->trim(
			&dp, f->max_marker(), false,
			[&](sys::error_code ec) {
			  ASSERT_EQ(sys::errc::no_message_available, ec);
			  f->list(
			    &dp, std::nullopt, list_entries,
			    [&](sys::error_code ec, std::span<fifo::entry> result,
				std::optional<std::string> marker) {
			      ASSERT_FALSE(ec);
			      ASSERT_TRUE(result.empty());
			      ASSERT_FALSE(marker);
			      completed = true;
			    });
			});
		    });
		});
	    });
	});
    });
  c.run();
  ASSERT_TRUE(completed);
}
