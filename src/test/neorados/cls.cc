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
#include <memory>
#include <string_view>
#include <utility>

#include <boost/asio/use_awaitable.hpp>

#include <boost/system/errc.hpp>

#include "include/neorados/RADOS.hpp"

#include "common/ceph_json.h"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

namespace asio = boost::asio;
namespace sys = boost::system;

using namespace std::literals;

using neorados::ReadOp;
using neorados::WriteOp;

CORO_TEST_F(NeoRadosCls, DNE, NeoRadosTest)
{
  std::string_view oid = "obj";
  co_await execute(oid, WriteOp{}.create(true));
  // Call a bogus class
  co_await expect_error_code(
    execute(oid, ReadOp{}.exec("doesnotexistasdfasdf", "method", {})),
    sys::errc::operation_not_supported);

  // Call a bogus method on an existent class
  co_await expect_error_code(
    execute(oid, ReadOp{}.exec("lock", "doesnotexistasdfasdfasdf", {})),
    sys::errc::operation_not_supported);
  co_return;
}

CORO_TEST_F(NeoRadosCls, RemoteReads, NeoRadosTest)
{
  SKIP_IF_CRIMSON();
  static constexpr std::size_t object_size = 4096;
  static constexpr std::array oids{"src_object.1"sv, "src_object.2"sv,
				   "src_object.3"sv};

  std::array<char, object_size> buf;
  buf.fill(1);

  for (const auto& oid : oids) {
    buffer::list in;
    in.append(buf.data(), buf.size());
    co_await execute(oid, WriteOp{}.write_full(std::move(in)));
  }

  // Construct JSON request passed to "test_gather" method, and in
  // turn, to "test_read" method
  buffer::list in;
  {
    auto formatter = std::make_unique<JSONFormatter>(true);
    formatter->open_object_section("foo");
    encode_json("src_objects", oids, formatter.get());
    encode_json("cls", "test_remote_reads", formatter.get());
    encode_json("method", "test_read", formatter.get());
    encode_json("pool", pool_name(), formatter.get());
    formatter->close_section();
    formatter->flush(in);
  }

  static const auto target = "tgt_object"s;

  // Create target object by combining data gathered from source
  // objects using "test_read" method
  co_await execute(target,
		   WriteOp{}.exec("test_remote_reads", "test_gather", in));


  // Read target object and check its size.
  buffer::list out;
  co_await execute(target, ReadOp{}.read(0, 0, &out));
  EXPECT_EQ(3 * object_size, out.length());

  co_return;
}
