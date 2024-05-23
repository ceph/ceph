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

CORO_TEST_F(NeoRadosIo, Limits, NeoRadosTest) {
  SKIP_IF_CRIMSON(); // See: https://tracker.ceph.com/issues/64040
  co_await expect_error_code(
    execute(oid, WriteOp{}
	    .write(std::numeric_limits<std::uint64_t>::max(), {})),
    sys::errc::file_too_large);
  co_await expect_error_code(
    execute(oid, WriteOp{}
	    .writesame(0, std::numeric_limits<std::uint64_t>::max(), {})),
    sys::errc::invalid_argument);
  co_return;
}

CORO_TEST_F(NeoRadosIo, SimpleWrite, NeoRadosTest) {
  static constexpr auto nspace = "nspace";
  auto pool2 = pool();
  const auto bl = filled_buffer_list(0xcc, 128);

  pool2.set_ns(nspace);
  EXPECT_EQ(nspace, pool2.get_ns());

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

CORO_TEST_F(NeoRadosIo, ReadOp, NeoRadosTest) {
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

void expect_eq_sparse(
    const buffer::list& expected,
    const std::vector<std::pair<std::uint64_t, std::uint64_t>>& extents,
    const buffer::list& actual)
{
  auto i = expected.begin();
  auto p = actual.begin();
  uint64_t pos = 0;
  for (auto extent : extents) {
    const uint64_t start = extent.first;
    const uint64_t end = start + extent.second;
    for (; pos < end; ++i, ++pos) {
      EXPECT_FALSE(i.end());
      if (pos < start) {
        // check the hole
        EXPECT_EQ('\0', *i);
      } else {
        // then the extent
        EXPECT_EQ(*i, *p);
        ++p;
      }
    }
  }
  EXPECT_EQ(expected.length(), pos);
}


CORO_TEST_F(NeoRadosIo, SparseRead, NeoRadosTest) {
  {
    const auto refbl = filled_buffer_list(0xcc, 4'096);
    co_await execute(oid, WriteOp{}.write_full(refbl));

    std::vector<std::pair<std::uint64_t, std::uint64_t>> extents;
    buffer::list readbl;
    co_await execute(oid, ReadOp{}.sparse_read(0, refbl.length(), &readbl, &extents));
    expect_eq_sparse(refbl, extents, readbl);
    EXPECT_EQ(refbl, readbl);
  }
  {
    buffer::list refbl;
    refbl.append(filled_buffer_list(0xcc, 4'096));
    refbl.append(filled_buffer_list(0x00, 4'096));
    refbl.append(filled_buffer_list(0xdd, 4'096));
    refbl.append(filled_buffer_list(0x00, 4'096));
    refbl.append(filled_buffer_list(0xee, 4'096));
    co_await execute(oid, WriteOp{}.write_full(refbl));

    std::vector<std::pair<std::uint64_t, std::uint64_t>> extents;
    buffer::list readbl;
    co_await execute(oid, ReadOp{}
		     .sparse_read(0, refbl.length(), &readbl, &extents));
    expect_eq_sparse(refbl, extents, readbl);
  }
}

CORO_TEST_F(NeoRadosIo, RoundTrip, NeoRadosTest) {
  const auto refbl = filled_buffer_list(0xcc, 128);
  co_await execute(oid, WriteOp{}.write_full(refbl));
  {
    auto bl = co_await read(oid);
    EXPECT_EQ(refbl, bl);
  }
  {
    buffer::list bl;
    ReadOp op;
    op.read(0, 0, & bl)
      .set_fadvise_nocache()
      .set_fadvise_random();
    co_await execute(oid, std::move(op));
    EXPECT_EQ(refbl, bl);
  }
  co_return;
}

CORO_TEST_F(NeoRadosIo, ReadIntoBuufferlist, NeoRadosTest) {
  auto refbl = filled_buffer_list(0xcc, 128);
  co_await execute(oid, WriteOp{}.write_full(refbl));
  {
    // here we test reading into a non-empty bufferlist referencing existing
    // buffers
    std::array<char, 128> buf;
    buf.fill(0xbb);
    buffer::list bl2;
    bl2.append(buffer::create_static(buf.size(), buf.data()));
    co_await rados().execute(oid, pool(),
			     ReadOp().read(0, refbl.length(), nullptr),
			     &bl2, asio::use_awaitable);
    EXPECT_EQ(refbl, bl2);
    EXPECT_EQ(0, memcmp(refbl.c_str(), buf.data(), buf.size()));
  }
  co_return;
}

CORO_TEST_F(NeoRadosIo, OverlappingWriteRoundTrip, NeoRadosTest) {
  const auto buf1 = filled_buffer_list(0xcc, 128);
  const auto buf2 = filled_buffer_list(0xdd, 64);
  co_await execute(oid, WriteOp{}.write(0, buf1));
  co_await execute(oid, WriteOp{}.write(0, buf2));

  buffer::list buf3 = buf2;
  buf3.append(filled_buffer_list(0xcc, 64));
  auto resbl = co_await read(oid);
  EXPECT_EQ(buf3, resbl);
  co_return;
}


CORO_TEST_F(NeoRadosIo, WriteFullRoundTrip, NeoRadosTest) {
  {
    const auto buf1 = filled_buffer_list(0xcc, 128);
    const auto buf2 = filled_buffer_list(0xdd, 64);
    co_await execute(oid, WriteOp{}.write_full(buf1));
    co_await execute(oid, WriteOp{}.write_full(buf2));
    auto resbl = co_await read(oid);
    EXPECT_EQ(buf2, resbl);
  }
  {
    const auto bl = to_buffer_list("ceph");
    co_await execute(oid, WriteOp()
		     .write_full(bl)
		     .set_fadvise_nocache());

    buffer::list resbl;
    co_await execute(oid, ReadOp()
		     .read(0, 0, &resbl).balance_reads()
		     .set_fadvise_dontneed()
		     .set_fadvise_random());
    EXPECT_EQ(bl, resbl);
  }
  co_return;
}

CORO_TEST_F(NeoRadosIo, AppendRoundTrip, NeoRadosTest) {
  const auto buf1 = filled_buffer_list(0xde, 64);
  const auto buf2 = filled_buffer_list(0xad, 64);
  co_await execute(oid, WriteOp{}.append(buf1));
  co_await execute(oid, WriteOp{}.append(buf2));
  auto resbl = co_await read(oid);
  auto cmpbl = buf1;
  cmpbl.append(buf2);
  EXPECT_EQ(cmpbl, resbl);
  co_return;
}

CORO_TEST_F(NeoRadosIo, Trunc, NeoRadosTest) {
  const auto buf = filled_buffer_list(0xaa, 128);
  co_await execute(oid, WriteOp{}.append(buf));
  co_await execute(oid, WriteOp{}.truncate(buf.length() / 2));
  const auto resbl = co_await read(oid);
  buffer::list cmpbl;
  cmpbl.substr_of(buf, 0, buf.length() / 2);
  EXPECT_EQ(cmpbl, resbl);
  co_return;
}

CORO_TEST_F(NeoRadosIo, Remove, NeoRadosTest) {
  co_await execute(oid, WriteOp{}.create(true));
  co_await execute(oid, ReadOp{}.stat(nullptr, nullptr));
  co_await execute(oid, WriteOp{}.remove());
  co_await expect_error_code(execute(oid, WriteOp{}.remove()),
			     sys::errc::no_such_file_or_directory);
  co_return;
}

CORO_TEST_F(NeoRadosIo, XattrsRoundTrip, NeoRadosTest) {
  const auto obj_buf = filled_buffer_list(0xaa, 128);
  const auto attrkey = "attr1"sv;
  const auto attrval = to_buffer_list("foo bar baz");
  co_await execute(oid, WriteOp{}.append(obj_buf));
  buffer::list attrval_res;

  co_await expect_error_code(execute(oid,
				     ReadOp{}.get_xattr(attrkey, &attrval_res)),
			     sys::errc::no_message_available);
  EXPECT_EQ(0, attrval_res.length());

  co_await execute(oid, WriteOp{}.setxattr(attrkey, attrval));
  co_await execute(oid, ReadOp{}.get_xattr(attrkey, &attrval_res));
  co_return;
}

CORO_TEST_F(NeoRadosIo, RmXattr, NeoRadosTest) {
  const auto objbl= filled_buffer_list(0xaa, 128);
  const auto attrkey = "attr1"sv;
  const auto attrval = to_buffer_list("foo bar baz");

  co_await execute(oid, WriteOp{}.append(objbl));

  co_await expect_error_code(execute(oid,
				     ReadOp{}.get_xattr(attrkey, nullptr)),
			     sys::errc::no_message_available);
  co_await execute(oid, WriteOp{}.setxattr(attrkey, attrval));
  co_await execute(oid, ReadOp{}.get_xattr(attrkey, nullptr));

  co_await execute(oid, WriteOp{}.rmxattr(attrkey));
  co_await expect_error_code(execute(oid,
				     ReadOp{}.get_xattr(attrkey, nullptr)),
			     sys::errc::no_message_available);

  // Test rmxattr of a removed object
  co_await execute(oid, WriteOp{}.remove());
  co_await expect_error_code(execute(oid,
				     WriteOp{}.rmxattr(attrkey)),
			     sys::errc::no_such_file_or_directory);

  co_return;
}

CORO_TEST_F(NeoRadosIo, GetXattrs, NeoRadosTest) {
  const auto objbl= filled_buffer_list(0xaa, 128);
  const auto attrkey1 = "attr1"s;
  const auto attrval1 = to_buffer_list("foo bar baz");
  const auto attrkey2 = "attr2"s;
  std::array<char, 256> attrbuf2;
  for (auto i = 0u; i < attrbuf2.size(); ++i) {
    attrbuf2[i] = i % 0xff;
  }
  buffer::list attrval2;
  attrval2.append(attrbuf2.data(), attrbuf2.size());

  co_await execute(oid, WriteOp{}
		   .append(objbl)
		   .setxattr(attrkey1, attrval1)
		   .setxattr(attrkey2, attrval2));

  container::flat_map<std::string, buffer::list> attrset;
  co_await execute(oid, ReadOp{}.get_xattrs(&attrset));
  EXPECT_EQ(2, attrset.size());
  EXPECT_EQ(attrval1, attrset[attrkey1]);
  EXPECT_EQ(attrval2, attrset[attrkey2]);

  co_return;
}
