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
#include <cstdint>
#include <limits>
#include <utility>

#include <fmt/format.h>

#include <boost/asio/use_awaitable.hpp>

#include <boost/container/flat_map.hpp>

#include <boost/system/errc.hpp>

#include "include/neorados/RADOS.hpp"

#include "include/buffer.h"
#include "include/stringify.h"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

namespace asio = boost::asio;
namespace buffer = ceph::buffer;
namespace container = boost::container;
namespace sys = boost::system;

using namespace std::literals;

using neorados::ReadOp;
using neorados::WriteOp;

static constexpr auto oid = "oid"sv;

CORO_TEST_F(NeoRadosECIo, SimpleWrite, NeoRadosECTest) {
  co_return;
  static constexpr auto nspace = "nspace";
  auto pool2 = pool();
  const auto bl = filled_buffer_list(0xcc, 128);

  pool2.set_ns(nspace);
  EXPECT_EQ(nspace, pool2.get_ns());
  sleep(10);

  {
    co_await execute(oid, WriteOp().write(0, bl));
    auto resbl = co_await read(oid);
    EXPECT_EQ(bl, resbl);
  }

  {
    co_await execute(oid, WriteOp().write(0, bl), pool2);
    auto resbl = co_await read(oid, pool2);
    EXPECT_EQ(bl, resbl);
  }

  co_return;
}

CORO_TEST_F(NeoRadosECIo, ReadOp, NeoRadosECTest) {
  const auto refbl = filled_buffer_list(0xcc, 128);

  co_await execute(oid, WriteOp{}.write_full(refbl));
  {
    buffer::list op_bl;
    co_await rados().execute(oid, pool(),
			     ReadOp().read(0, refbl.length(), nullptr),
			     &op_bl, asio::use_awaitable);
    EXPECT_EQ(refbl, op_bl);
  }
  {
    buffer::list op_bl;
    // 0 means read the whole object data.
    co_await rados().execute(oid, pool(),
			     ReadOp().read(0, 0, nullptr),
			     &op_bl, asio::use_awaitable);
    EXPECT_EQ(refbl, op_bl);
  }
  {
    buffer::list read_bl, op_bl;
    co_await rados().execute(oid, pool(),
			     ReadOp().read(0, refbl.length(), &read_bl),
			     &op_bl, asio::use_awaitable);
    EXPECT_EQ(refbl, read_bl);
    EXPECT_EQ(refbl, op_bl);
  }
  {
    buffer::list read_bl, op_bl;
    // 0 means read the whole object data.
    co_await rados().execute(oid, pool(),
			     ReadOp().read(0, 0, &read_bl),
			     &op_bl, asio::use_awaitable);
    EXPECT_EQ(refbl, read_bl);
    EXPECT_EQ(refbl, op_bl);
  }

  {
    buffer::list read_bl, read_bl2, op_bl;
    // 0 means read the whole object data.
    co_await rados().execute(oid, pool(), ReadOp{}
			     .read(0, 0, &read_bl)
			     .read(0, 0, &read_bl2),
			     &op_bl, asio::use_awaitable);
    EXPECT_EQ(refbl, read_bl);
    EXPECT_EQ(refbl, read_bl2);
    buffer::list bl2;
    bl2.append(refbl);
    bl2.append(refbl);
    EXPECT_EQ(bl2, op_bl);
  }
  {
    // Read into buffer with a cached crc
    auto op_bl = filled_buffer_list('z', refbl.length());
    EXPECT_NE(refbl.crc32c(0), op_bl.crc32c(0));  // cache 'x' crc

    co_await rados().execute(oid, pool(),
			     ReadOp().read(0, refbl.length(), nullptr),
			     &op_bl, asio::use_awaitable);
    EXPECT_EQ(refbl, op_bl);
    EXPECT_EQ(refbl.crc32c(0), op_bl.crc32c(0));  // cache 'x' crc
  }

  co_return;
}
