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

#include "neorados/cls/log.h"

#include <coroutine>
#include <chrono>
#include <map>
#include <string>
#include <string_view>

#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/redirect_error.hpp>

#include <boost/system/errc.hpp>
#include <boost/system/error_code.hpp>

#include <fmt/format.h>

#include "include/neorados/RADOS.hpp"

#include "cls/version/cls_version_types.h"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

using namespace std::literals;

namespace chrono = std::chrono;

namespace asio = boost::asio;

namespace buffer = ceph::buffer;

using boost::system::error_code;
using boost::system::system_error;
using boost::system::errc::no_message_available;
using ceph::real_clock;
using ceph::real_time;

using neorados::RADOS;
using neorados::IOContext;
using neorados::Object;
using neorados::WriteOp;
using neorados::ReadOp;

namespace l = neorados::cls::log;

inline constexpr auto section = "global"s;
inline constexpr auto oid = "obj"sv;

template<typename T>
auto encode(const T& v)
{
  using ceph::encode;
  buffer::list bl;
  encode(v, bl);
  return bl;
}

template<typename T>
auto decode(const buffer::list& bl)
{
  using ceph::decode;
  T v;
  auto bi = bl.cbegin();
  decode(v, bi);
  return v;
}

auto get_time(real_time start_time, chrono::seconds i, bool modify_time)
{
  return modify_time ? start_time + i : start_time;
}

auto get_name(int i)
{
  static constexpr auto prefix = "data-source-"sv;
  return fmt::format("{}{}", prefix, i);
}

template<boost::asio::completion_token_for<void(error_code)> CompletionToken>
auto generate_log(RADOS& r, Object oid, IOContext ioc,
		  int max, real_time start_time,
		  bool modify_time, CompletionToken&& token)
{
// In this case, the warning is spurious as the 'mismatched' `operator
// new` calls directly into the matching `operator new`, returning its
// result.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmismatched-new-delete"
  return asio::async_initiate<CompletionToken, void(error_code)>
    (asio::experimental::co_composed<void(error_code)>
     ([](auto state, RADOS& r, Object oid, IOContext ioc,
	 int max, real_time start_time, bool modify_time) -> void {
       static constexpr auto maxops = 50;
       try {
	 for (auto i = 0; i < max;) {
	   std::vector<l::entry> entries;
	   for (auto ops = 0; (ops < maxops) && (i < max); ++i, ++ops) {
	     entries.emplace_back(get_time(start_time, i * 1s, modify_time),
				  section, get_name(i), encode(i));
	   }
	   co_await r.execute(oid, ioc, WriteOp{}.exec(l::add(std::move(entries))),
			      asio::deferred);
	 }
       } catch (const system_error& e) {
	 co_return {e.code()};
       }
       co_return {error_code{}};
     }, r.get_executor()),
     token, std::ref(r), std::move(oid),
     std::move(ioc), max, start_time, modify_time);
#pragma GCC diagnostic pop
}

void check_entry(const l::entry& entry, real_time start_time,
		 int i, bool modified_time)
{
  auto name = get_name(i);
  auto ts = get_time(start_time, i * 1s, modified_time);

  ASSERT_EQ(section, entry.section);
  ASSERT_EQ(name, entry.name);
  ASSERT_EQ(ts, entry.timestamp);
}

template<boost::asio::completion_token_for<void(error_code)> CompletionToken>
auto check_log(RADOS& r, Object oid, IOContext ioc, real_time start_time,
	       int max, CompletionToken&& token)
{
// In this case, the warning is spurious as the 'mismatched' `operator
// new` calls directly into the matching `operator new`, returning its
// result.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmismatched-new-delete"
  return asio::async_initiate<CompletionToken, void(error_code)>
    (asio::experimental::co_composed<void(error_code)>
     ([](auto state, RADOS& rados, Object oid, IOContext ioc,
	 real_time start_time, int max) -> void {
       try {
	 std::vector<l::entry> entries{neorados::cls::log::max_list_entries};
	 std::optional<std::string> marker;
	 int i = 0;
	 do {
	   std::span<l::entry> result;
	   std::tie(result, marker) =
	     co_await neorados::cls::log::list(rados, oid, ioc, {}, {},
					       marker, entries,
					       asio::deferred);
	   for (const auto& entry : result) {
	     auto num = decode<int>(entry.data);
	     EXPECT_EQ(i, num);
	     check_entry(entry, start_time, i, true);
	     ++i;
	   }
	 } while (marker);
	 EXPECT_EQ(i, max);
       } catch (const system_error& e) {
	 co_return {e.code()};
       }
       co_return {error_code{}};
     }, r.get_executor()),
     token, std::ref(r), std::move(oid),
     std::move(ioc), start_time, max);
#pragma GCC diagnostic pop
}

template<typename CompletionToken>
auto trim(RADOS& rados, Object oid, const IOContext ioc,
	  real_time from, real_time to, CompletionToken&& token)
{
  return rados.execute(std::move(oid), std::move(ioc),
		       WriteOp{}.exec(l::trim(from, to)),
		       std::forward<CompletionToken>(token));
}

template<typename CompletionToken>
auto trim(RADOS& rados, Object oid, IOContext ioc,
	  std::string from, std::string to, CompletionToken&& token)
{
  return rados.execute(std::move(oid), std::move(ioc),
		       WriteOp{}.exec(l::trim(std::move(from), std::move(to))),
		       std::forward<CompletionToken>(token));
}

template<typename CompletionToken>
auto list(RADOS& rados, Object oid, IOContext ioc,
	  std::span<l::entry> entries, std::span<l::entry>* result,
	  CompletionToken&& token)
{
  return rados.execute(oid, ioc,
		       ReadOp{}.exec(l::list({}, {}, {}, entries, result, nullptr)),
		       nullptr, asio::use_awaitable);
}

template<typename CompletionToken>
auto list(RADOS& rados, Object oid, IOContext ioc,
	  real_time from, real_time to,
	  std::optional<std::string> in_marker,
	  std::span<l::entry> entries, std::span<l::entry>* result,
	  std::optional<std::string>* marker, CompletionToken&& token)
{
  return rados.execute(
    oid, ioc,
    ReadOp{}.exec(l::list(from, to, std::move(in_marker), entries, result, marker)),
    nullptr, asio::use_awaitable);
}

CORO_TEST_F(neocls_log, test_log_add_same_time, NeoRadosTest)
{
  co_await create_obj(oid);

  auto start_time = real_clock::now();
  auto to_time = start_time + 1s;
  co_await generate_log(rados(), oid, pool(), 10, start_time, false,
			asio::use_awaitable);

  std::vector<l::entry> entries{neorados::cls::log::max_list_entries};
  auto [res, marker] =
    co_await neorados::cls::log::list(rados(), oid, pool(), start_time, to_time,
				      {}, entries, asio::use_awaitable);
  EXPECT_EQ(10u, res.size());
  EXPECT_FALSE(marker);

  /* need to sort returned entries, all were using the same time as key */
  std::map<int, l::entry> check_ents;

  for (const auto& entry : res) {
    auto num = decode<int>(entry.data);
    check_ents[num] = entry;
  }

  EXPECT_EQ(10u, check_ents.size());

  decltype(check_ents)::iterator ei;
  int i;

  for (i = 0, ei = check_ents.begin(); i < 10; i++, ++ei) {
    const auto& entry = ei->second;

    EXPECT_EQ(i, ei->first);
    check_entry(entry, start_time, i, false);
  }


  res = std::span{entries}.first(1);
  co_await list(rados(), oid, pool(), start_time, to_time, {},
		res, &res, &marker, asio::use_awaitable);

  EXPECT_EQ(1u, res.size());
  EXPECT_TRUE(marker);
}

CORO_TEST_F(neocls_log, test_log_add_different_time, NeoRadosTest)
{
  co_await create_obj(oid);

  /* generate log */
  auto start_time = real_clock::now();
  co_await generate_log(rados(), oid, pool(), 10, start_time, true,
			asio::use_awaitable);

  std::vector<l::entry> entries{neorados::cls::log::max_list_entries};
  std::optional<std::string> marker;
  std::span<l::entry> result;

  auto to_time = start_time + (10 * 1s);

  {
    /* check list */
    std::tie(result, marker) =
      co_await neorados::cls::log::list(rados(), oid, pool(), start_time,
					to_time, {}, entries,
					asio::use_awaitable);
    EXPECT_EQ(10u, result.size());
    EXPECT_FALSE(marker);
  }

  decltype(result)::iterator iter;
  int i;

  for (i = 0, iter = result.begin(); iter != result.end(); ++iter, ++i) {
    auto& entry = *iter;
    auto num = decode<int>(entry.data);
    EXPECT_EQ(i, num);
    check_entry(entry, start_time, i, true);
  }

  /* check list again with shifted time */
  {
    auto next_time = get_time(start_time, 1s, true);
    std::tie(result, marker) =
      co_await neorados::cls::log::list(rados(), oid, pool(), next_time,
					to_time, {}, entries,
					asio::use_awaitable);

    EXPECT_EQ(9u, result.size());
    EXPECT_FALSE(marker);
  }

  i = 0;
  marker.reset();
  do {
    auto old_marker = std::move(marker);
    std::tie(result, marker) =
      co_await neorados::cls::log::list(rados(), oid, pool(), start_time, to_time,
					old_marker, std::span{entries}.first(1),
					asio::use_awaitable);
    EXPECT_NE(old_marker, marker);
    EXPECT_EQ(1u, result.size());

    ++i;
    EXPECT_GE(10, i);
  } while (marker);

  EXPECT_EQ(10, i);
}

CORO_TEST_F(neocls_log, trim_by_time, NeoRadosTest)
{
  co_await create_obj(oid);

  /* generate log */
  auto start_time = real_clock::now();
  co_await generate_log(rados(), oid, pool(), 10, start_time, true,
			asio::use_awaitable);

  std::vector<l::entry> entries{neorados::cls::log::max_list_entries};
  std::optional<std::string> marker;

  /* trim */
  auto to_time = get_time(start_time, 10s, true);

  for (int i = 0; i < 10; i++) {
    auto trim_time = get_time(start_time, i * 1s, true);
    co_await trim(rados(), oid, pool(), {}, trim_time, asio::use_awaitable);
    error_code ec;
    co_await trim(rados(), oid, pool(), {}, trim_time,
		  asio::redirect_error(asio::use_awaitable, ec));
    EXPECT_EQ(no_message_available, ec);

    std::span<l::entry> result;
    co_await list(rados(), oid, pool(), start_time, to_time, {},
		  entries, &result, &marker, asio::use_awaitable);
    EXPECT_EQ(9u - i, result.size());
    EXPECT_FALSE(marker);
  }
}

CORO_TEST_F(neocls_log, trim_by_marker, NeoRadosTest)
{
  co_await create_obj(oid);

  auto start_time = real_clock::now();
  co_await generate_log(rados(), oid, pool(), 10, start_time, true,
			asio::use_awaitable);
  std::vector<l::entry> log1;
  {
    std::vector<l::entry> entries{neorados::cls::log::max_list_entries};
    std::span<l::entry> result;
    co_await list(rados(), oid, pool(), entries, &result, asio::use_awaitable);
    EXPECT_EQ(10u, result.size());
    log1.assign(std::make_move_iterator(result.begin()),
                std::make_move_iterator(result.end()));
  }
  // trim front of log
  {
    const std::string from = neorados::cls::log::begin_marker;
    const std::string to = log1[0].id;
    co_await trim(rados(), oid, pool(), from, to, asio::use_awaitable);

    std::vector<l::entry> entries{neorados::cls::log::max_list_entries};
    std::span<l::entry> result;
    co_await list(rados(), oid, pool(), entries, &result, asio::use_awaitable);

    EXPECT_EQ(9u, result.size());
    EXPECT_EQ(log1[1].id, result.begin()->id);

    error_code ec;
    co_await trim(rados(), oid, pool(), from, to,
		  asio::redirect_error(asio::use_awaitable, ec));
    EXPECT_EQ(no_message_available, ec);
  }
  // trim back of log
  {
    const std::string from = log1[8].id;
    const std::string to = neorados::cls::log::end_marker;
    co_await trim(rados(), oid, pool(), from, to, asio::use_awaitable);

    std::vector<l::entry> entries{neorados::cls::log::max_list_entries};
    std::span<l::entry> result;
    co_await list(rados(), oid, pool(), entries, &result, asio::use_awaitable);
    EXPECT_EQ(8u, result.size());
    EXPECT_EQ(log1[8].id, result.rbegin()->id);

    error_code ec;
    co_await trim(rados(), oid, pool(), from, to,
		  asio::redirect_error(asio::use_awaitable, ec));
    EXPECT_EQ(no_message_available, ec);
  }
  // trim a key from the middle
  {
    const std::string from = log1[3].id;
    const std::string to = log1[4].id;
    co_await trim(rados(), oid, pool(), from, to, asio::use_awaitable);

    std::vector<l::entry> entries{neorados::cls::log::max_list_entries};
    std::span<l::entry> result;

    EXPECT_EQ(7u, result.size());

    error_code ec;
    co_await trim(rados(), oid, pool(), from, to,
		  asio::redirect_error(asio::use_awaitable, ec));
    EXPECT_EQ(no_message_available, ec);
  }
  // trim full log
  {
    const std::string from = neorados::cls::log::begin_marker;
    const std::string to = neorados::cls::log::end_marker;
    co_await trim(rados(), oid, pool(), from, to, asio::use_awaitable);

    std::vector<l::entry> entries{neorados::cls::log::max_list_entries};
    std::span<l::entry> result;
    co_await list(rados(), oid, pool(), entries, &result, asio::use_awaitable);
    EXPECT_EQ(0u, result.size());

    error_code ec;
    co_await trim(rados(), oid, pool(), from, to,
		  asio::redirect_error(asio::use_awaitable, ec));
    EXPECT_EQ(no_message_available, ec);
  }
}

TEST(neocls_log_bare, lambdata)
{
  asio::io_context c;

  std::string_view oid = "obj";

  const auto now = real_clock::now();
  static constexpr auto max = 10'000;

  std::optional<neorados::RADOS> rados;
  neorados::IOContext pool;
  std::vector<l::entry> entries{neorados::cls::log::max_list_entries};

  bool completed = false;
  neorados::RADOS::Builder{}.build(c, [&](error_code ec, neorados::RADOS r_) {
    ASSERT_FALSE(ec);
    rados = std::move(r_);
    create_pool(
      *rados, get_temp_pool_name(),
      [&](error_code ec, int64_t poolid) {
	ASSERT_FALSE(ec);
	pool.set_pool(poolid);
	generate_log(
	  *rados, oid, pool, max, now, true,
	  [&](error_code ec) {
	    ASSERT_FALSE(ec);
	    check_log(
	      *rados, oid, pool, now, max,
	      [&](error_code ec) {
		ASSERT_FALSE(ec);
		neorados::cls::log::trim(
		  *rados, oid, pool, neorados::cls::log::begin_marker,
		  neorados::cls::log::end_marker,
		  [&](error_code ec) {
		    ASSERT_FALSE(ec);
		    l::list(
		      *rados, oid, pool, {}, {}, {}, entries,
		      [&](error_code ec,
			  std::span<l::entry> result,
			  std::optional<std::string> marker) {
			ASSERT_FALSE(ec);
			ASSERT_FALSE(marker);
			ASSERT_EQ(0u, result.size());
			completed = true;
		      });
		  });
	      });
	  });
      });
  });
  c.run();
  ASSERT_TRUE(completed);
}
