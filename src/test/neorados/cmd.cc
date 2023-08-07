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

#include <array>
#include <coroutine>

#include <fmt/format.h>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <boost/system/errc.hpp>

#include "include/neorados/RADOS.hpp"

#include "include/stringify.h"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

namespace asio = boost::asio;
namespace sys = boost::system;
namespace buffer = ceph::buffer;

using neorados::ReadOp;

using namespace std::literals;

CORO_TEST_F(NeoRadosCmd, MonDescribe, NeoRadosTest) {
  std::string outs;
  buffer::list outbl;
  std::vector arg({R"({"prefix": "get_command_descriptions"})"s});
  co_await rados().mon_command(std::move(arg), {}, &outs, &outbl,
			       asio::use_awaitable);
  EXPECT_LT(0u, outbl.length());
  EXPECT_LE(0u, outs.length());
  co_return;
}

CORO_TEST_F(NeoRadosCmd, OSDCmd, NeoRadosTest) {
  {
    std::vector arg({R"(asdfasdf)"s});
    co_await expect_error_code(
      rados().osd_command(0, std::move(arg),
			  {}, asio::use_awaitable),
      sys::errc::invalid_argument, sys::errc::no_such_device_or_address);
  }

  {
    std::vector arg({R"(version)"s});
    co_await expect_error_code(
      rados().osd_command(0, std::move(arg),
			  {}, asio::use_awaitable),
      sys::errc::invalid_argument, sys::errc::no_such_device_or_address);
  }

  {
    std::vector arg({R"({"prefix":"version"})"s});
    auto [ec, outs, outbl] = co_await
      rados().osd_command(0, std::move(arg), {},
			  asio::as_tuple(asio::use_awaitable));

    EXPECT_TRUE((!ec && outbl.length() > 0) ||
		(ec == sys::errc::no_such_device_or_address &&
		 outbl.length() == 0));

  }
  co_return;
}

CORO_TEST_F(NeoRadosCmd, PGCmd, NeoRadosTest) {
  const neorados::PG pgid{uint64_t(pool().get_pool()), 0};

  {
    std::vector arg({R"(asdfasdf)"s});
    // note: tolerate NXIO here in case the cluster is thrashing out underneath us.
    co_await expect_error_code(
      rados().pg_command(pgid, std::move(arg),
			 {}, asio::use_awaitable),
      sys::errc::invalid_argument, sys::errc::no_such_device_or_address);
  }

  // make sure the pg exists on the osd before we query it
  for (auto i = 0; i < 100; ++i) {
    co_await expect_error_code(
      rados().execute(fmt::format("obj{}", i), pool(),
		      ReadOp{}.assert_exists(), nullptr,
		      asio::use_awaitable),
      sys::errc::no_such_file_or_directory);
  }

  {
    std::vector arg({fmt::format(R"({{"prefix":"pg", "cmd":"query", "pgid":"{}.{}"}})",
				 pgid.pool, pgid.seed)});
    // Working around a bug in GCC.
    auto coro = rados().pg_command(pgid, std::move(arg),
				   {}, asio::as_tuple(asio::use_awaitable));
    auto [ec, outs, outbl] = co_await std::move(coro);

    EXPECT_TRUE(!ec || ec == sys::errc::no_such_file_or_directory ||
		ec == sys::errc::no_such_device_or_address);

    EXPECT_LT(0u, outbl.length());
  }

  co_return;
}
