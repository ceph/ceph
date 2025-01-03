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

#include <coroutine>
#include <cstdint>
#include <utility>

#include <boost/asio/use_awaitable.hpp>

#include <boost/system/errc.hpp>

#include "include/neorados/RADOS.hpp"

#include "include/buffer.h"

#include "common/ceph_context.h"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

namespace asio = boost::asio;
namespace buffer = ceph::buffer;
namespace sys = boost::system;

using namespace std::literals;

using neorados::ReadOp;
using neorados::WriteOp;

static constexpr auto oid = "oid"sv;

CORO_TEST_F(NeoRadosMisc, Version, NeoRadosTest) {
  [[maybe_unused]] auto [major, minor, point] = neorados::RADOS::version();
  co_return;
}

CORO_TEST_F(NeoRadosMisc, WaitOSDMap, NeoRadosTest) {
  co_await rados().wait_for_latest_osd_map(asio::use_awaitable);
  co_return;
}

CORO_TEST_F(NeoRadosMisc, LongName, NeoRadosTest) {
  const auto maxlen = rados().cct()->_conf->osd_max_object_name_len;
  const auto bl = to_buffer_list("content"sv);
  co_await execute(std::string(maxlen / 2, 'a'),
                   WriteOp{}.write_full(bl));
  co_await execute(std::string(maxlen - 1, 'a'),
                   WriteOp{}.write_full(bl));
  co_await execute(std::string(maxlen, 'a'),
                   WriteOp{}.write_full(bl));

  co_await expect_error_code(execute(std::string(maxlen + 1, 'a'),
                                     WriteOp{}.write_full(bl)),
                             sys::errc::filename_too_long);
  co_await expect_error_code(execute(std::string(maxlen * 2, 'a'),
                                     WriteOp{}.write_full(bl)),
                             sys::errc::filename_too_long);
  co_return;
}

CORO_TEST_F(NeoRadosMisc, LongLocator, NeoRadosTest) {
  SKIP_IF_CRIMSON();
  const auto maxlen = rados().cct()->_conf->osd_max_object_name_len;
  const auto bl = to_buffer_list("content"sv);
  {
    auto p = pool();
    p.set_key(std::string(maxlen / 2, 'a'));
    co_await execute(oid,
                     WriteOp{}.write_full(bl), p);
  }
  {
    auto p = pool();
    p.set_key(std::string(maxlen - 1, 'a'));
    co_await execute(oid,
                     WriteOp{}.write_full(bl), p);
  }
  {
    auto p = pool();
    p.set_key(std::string(maxlen, 'a'));
    co_await execute(oid,
                     WriteOp{}.write_full(bl), p);
  }
  {
    auto p = pool();
    p.set_key(std::string(maxlen + 1, 'a'));
    co_await expect_error_code(execute(oid,
				       WriteOp{}.write_full(bl), p),
			       sys::errc::filename_too_long);
  }
  {
    auto p = pool();
    p.set_key(std::string(maxlen * 2, 'a'));
    co_await expect_error_code(execute(oid,
				       WriteOp{}.write_full(bl), p),
			       sys::errc::filename_too_long);
  }

  co_return;
}

CORO_TEST_F(NeoRadosMisc, LongNamespace, NeoRadosTest) {
  const auto maxlen = rados().cct()->_conf->osd_max_object_namespace_len;
  const auto bl = to_buffer_list("content"sv);
  {
    auto p = pool();
    p.set_ns(std::string(maxlen / 2, 'a'));
    co_await execute(oid,
                     WriteOp{}.write_full(bl), p);
  }
  {
    auto p = pool();
    p.set_ns(std::string(maxlen - 1, 'a'));
    co_await execute(oid,
                     WriteOp{}.write_full(bl), p);
  }
  {
    auto p = pool();
    p.set_ns(std::string(maxlen, 'a'));
    co_await execute(oid,
                     WriteOp{}.write_full(bl), p);
  }
  {
    auto p = pool();
    p.set_ns(std::string(maxlen + 1, 'a'));
    co_await expect_error_code(execute(oid,
				       WriteOp{}.write_full(bl), p),
			       sys::errc::filename_too_long);
  }
  {
    auto p = pool();
    p.set_ns(std::string(maxlen * 2, 'a'));
    co_await expect_error_code(execute(oid,
				       WriteOp{}.write_full(bl), p),
			       sys::errc::filename_too_long);
  }

  co_return;
}

CORO_TEST_F(NeoRadosMisc, LongAttrName, NeoRadosTest) {
  const auto maxlen = rados().cct()->_conf->osd_max_attr_name_len;
  const auto bl = to_buffer_list("content"sv);

  co_await execute(oid, WriteOp{}.setxattr(std::string(maxlen / 2, 'a'), bl));
  co_await execute(oid, WriteOp{}.setxattr(std::string(maxlen - 1, 'a'), bl));
  co_await execute(oid, WriteOp{}.setxattr(std::string(maxlen, 'a'), bl));

  co_await expect_error_code(
    execute(oid, WriteOp{}.setxattr(std::string(maxlen + 1, 'a'), bl)),
    sys::errc::filename_too_long);
  co_await expect_error_code(
    execute(oid, WriteOp{}.setxattr(std::string(maxlen * 2, 'a'), bl)),
    sys::errc::filename_too_long);
  co_return;
}

CORO_TEST_F(NeoRadosMisc, Exec, NeoRadosTest) {
  buffer::list out;
  co_await execute(oid, WriteOp{}.create(true));
  co_await execute(oid,
		   ReadOp{}.exec("rbd"sv, "get_all_features"sv, {}, &out));
  auto features = from_buffer_list<std::uint64_t>(out);
  // make sure *some* features are specified; don't care which ones
  EXPECT_NE(0, features);

  co_return;
}

CORO_TEST_F(NeoRadosMisc, Operate1, NeoRadosTest) {
  static constexpr auto key1 = "key1"sv;
  const auto val1 = to_buffer_list("val1\0"sv);
  {
    WriteOp op;
    op.write(0, {})
      .setxattr(key1, val1)
      // Should not affect xattr
      .clear_omap();
    co_await execute(oid, std::move(op));
  }

  // Op is empty now
  co_await execute(oid, WriteOp{});
  {
    buffer::list bl;
    co_await execute(oid, ReadOp{}.get_xattr(key1, &bl));
    EXPECT_EQ(val1, bl);
  }
  // Comparisons differing in NUL termination.
  const auto notval1 = to_buffer_list("val1"sv);
  co_await expect_error_code(
    execute(oid, WriteOp{}
	    .cmpxattr(key1, neorados::cmp_op::eq, notval1)
	    .rmxattr(key1)),
    sys::errc::operation_canceled);
  co_await expect_error_code(
    execute(oid, WriteOp{}.cmpxattr(key1, neorados::cmp_op::eq, notval1)),
    sys::errc::operation_canceled);

  co_return;
}

CORO_TEST_F(NeoRadosMisc, Operate2, NeoRadosTest) {
  static constexpr auto key1 = "key1"sv;
  const auto val1 = to_buffer_list("val1\0"sv);
  WriteOp op;
  op.write(0, to_buffer_list("abcdefg"sv))
    .setxattr(key1, val1)
    .truncate(0);
  co_await execute(oid, std::move(op));
  std::uint64_t size;
  co_await execute(oid, ReadOp{}.stat(&size, nullptr));
  EXPECT_EQ(0, size);
  co_return;
}

CORO_TEST_F(NeoRadosMisc, BigObject, NeoRadosTest) {
  const auto data = to_buffer_list("abcdefg"sv);
  co_await execute(oid, WriteOp{}.write(0, data));

  co_await expect_error_code(execute(oid, WriteOp{}.truncate(500000000000ull)),
			     sys::errc::file_too_large);
  co_await expect_error_code(execute(oid, WriteOp{}.zero(500000000000ull, 1)),
			     sys::errc::file_too_large);
  co_await expect_error_code(execute(oid, WriteOp{}.zero(1, 500000000000ull)),
			     sys::errc::file_too_large);
  co_await expect_error_code(execute(oid, WriteOp{}.zero(500000000000ull,
							 500000000000ull)),
			     sys::errc::file_too_large);
#ifdef __LP64__
  co_await expect_error_code(execute(oid, WriteOp{}.write(500000000000ull, data)),
			     sys::errc::file_too_large);
#endif // __LP64__
  co_return;
}

CORO_TEST_F(NeoRadosMisc, BigAttr, NeoRadosTest) {
  const auto maxlen = rados().cct()->_conf->osd_max_attr_size;
  if (maxlen > 0) {
    buffer::list attrval;
    attrval.append(buffer::create(maxlen));
    co_await execute(oid, WriteOp{}.setxattr("one"sv, attrval));

    attrval.clear();
    attrval.append(buffer::create(maxlen + 1));
    co_await expect_error_code(execute(oid, WriteOp()
				       .setxattr("one"sv, attrval)),
			       sys::errc::file_too_large);
  } else {
    SUCCEED() << "osd_max_attr_size == 0, skipping test." << std::endl;
  }

  co_return;
}

CORO_TEST_F(NeoRadosMisc, WriteSame, NeoRadosTest) {
  SKIP_IF_CRIMSON(); // See: https://tracker.ceph.com/issues/64040
  static constexpr auto patlen = 128u;
  static constexpr auto samelen = patlen * 4;
  static constexpr char fill = 0xcc;
  const auto patbl = filled_buffer_list(fill, patlen);
  const auto refbl = filled_buffer_list(fill, samelen);

  // Zero the full range before using `writesame`
  co_await execute(oid, WriteOp{}.zero(0, patlen));

  // Write the same pattern four times
  co_await execute(oid, WriteOp{}.writesame(0, samelen, patbl));
  auto resbl = co_await read(oid);
  EXPECT_EQ(refbl, resbl);

  // Write length must be a multiple of the pattern length
  co_await expect_error_code(execute(oid, WriteOp{}
				     .writesame(0, samelen - 1, patbl)),
			     sys::errc::invalid_argument);

  // Write length is the same as pattern length (same as write)
  co_await execute(oid, WriteOp{}.truncate(0));
  co_await execute(oid, WriteOp{}.writesame(0, patbl.length(), patbl));
  resbl = co_await read(oid);
  EXPECT_EQ(patbl, resbl);


  co_return;
}

// We already have tests for cmpext and checksum. The rest uses
// currently unimplemented functionality.
