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
#include <cstring>
#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>

#include <boost/asio/use_awaitable.hpp>

#include <boost/system/error_code.hpp>
#include <boost/system/errc.hpp>

#include <fmt/format.h>

#include "include/neorados/RADOS.hpp"

#include "osd/error_code.h"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

namespace sys = boost::system;

using namespace std::literals;

using neorados::cmp_op;
using neorados::ReadOp;
using neorados::WriteOp;

constexpr auto oid = "test"sv;

CORO_TEST_F(NeoRadosWriteOps, AssertExists, NeoRadosTest) {
  co_await expect_error_code(execute(oid, WriteOp{}.assert_exists()),
			     sys::errc::no_such_file_or_directory);
  co_await execute(oid, WriteOp{}.create(true));
  co_await execute(oid, WriteOp{}.assert_exists());
  co_return;
}

CORO_TEST_F(NeoRadosWriteOps, AssertVersion, NeoRadosTest) {
  co_await execute(oid, WriteOp{}.create(true));
  std::uint64_t v;
  // Write to the object a second time to guarantee that its
  // version number is greater than 0
  co_await execute(oid, WriteOp{}.write_full(to_buffer_list("hi")), &v);

  co_await expect_error_code(execute(oid, WriteOp{}.assert_version(v + 1)),
			     sys::errc::value_too_large);
  co_await expect_error_code(execute(oid, WriteOp{}.assert_version(v - 1)),
			     sys::errc::result_out_of_range);
  co_await execute(oid, WriteOp{}.assert_version(v));
  co_return;
}

CORO_TEST_F(NeoRadosWriteOps, Xattrs, NeoRadosTest) {
  // Hey, the compiler won't check that I don't have typos in my strings…
  static constexpr auto key = "key"sv;
  const auto value = to_buffer_list("value");
  // Create an object with an xattr
  co_await execute(oid, WriteOp{}
		   .create(true)
		   .setxattr(key, value));
  // Check that xattr exists, if it does, delete it.
  co_await execute(oid, WriteOp{}
		   .cmpxattr("key", cmp_op::eq, value)
		   .rmxattr(key));

  // Check the xattr exits, if it does, add it again (will fail)
  co_await expect_error_code(execute(oid, WriteOp{}
				     .cmpxattr(key, cmp_op::eq, value)
				     .setxattr(key, value)),
			     sys::errc::operation_canceled);

  co_return;
}

CORO_TEST_F(NeoRadosWriteOps, Write, NeoRadosTest) {
  // Create an object, write and write full to it
  {
    const auto value = to_buffer_list("hi");
    co_await execute(oid, WriteOp{}
		     .write(0, to_buffer_list("four"))
		     .write_full(value));
    auto bl = co_await read(oid);
    EXPECT_EQ(value, bl);
  }
  // Create write op with I/O hint
  {
    const auto value = to_buffer_list("ceph");
    co_await execute(oid, WriteOp{}
		     .write_full(value)
		     .set_fadvise_nocache());
    auto bl = co_await read(oid);
    EXPECT_EQ(value, bl);
  }
  // Truncate and append
  {
    co_await execute(oid, WriteOp{}
		     .truncate(1)
		     .append(to_buffer_list("hi")));
    auto bl = co_await read(oid);
    EXPECT_EQ(to_buffer_list("chi"), bl);
  }
  // Zero and remove
  {
    co_await execute(oid, WriteOp{}
		     .zero(0, 3)
		     .remove());
    co_await expect_error_code(execute(oid, ReadOp{}.read(0, 0, nullptr)),
			       sys::errc::no_such_file_or_directory);
  }

  co_return;
}

CORO_TEST_F(NeoRadosWriteOps, Exec, NeoRadosTest) {
  co_await execute(oid, WriteOp{}
		   .exec("hello"sv, "record_hello"sv,
			 to_buffer_list("test")));
  const auto bl = co_await read(oid);
  EXPECT_EQ(to_buffer_list("Hello, test!"), bl);
  co_return;
}

CORO_TEST_F(NeoRadosWriteOps, WriteSame, NeoRadosTest) {
  co_await execute(oid, WriteOp{}
		   .writesame(0, 4 * 4, // Total bytes, not total copies
			      to_buffer_list("four")));
  const auto bl = co_await read(oid);
  EXPECT_EQ(to_buffer_list("fourfourfourfour"), bl);
  co_return;
}

CORO_TEST_F(NeoRadosWriteOps, CmpExt, NeoRadosTest) {
  static const auto four = to_buffer_list("four");
  static const auto five = to_buffer_list("five");
  static const auto six = to_buffer_list("six");

  // Create an object, write to it
  {
    co_await execute(oid, WriteOp{}
		     .create(true)
		     .write_full(four));
    const auto bl = co_await read(oid);
    EXPECT_EQ(four, bl);
  }
  // Compare and overwrite on (expected) match
  {
    uint64_t unmatch = 0;
    co_await execute(oid, WriteOp{}
		     .cmpext(0, four, &unmatch)
		     .write(0, five));
    const auto bl = co_await read(oid);
    EXPECT_EQ(five, bl);
    EXPECT_EQ(-1, unmatch);
  }
  // check offset return error value
  {
    uint64_t unmatch = -2;
    co_await expect_error_code(execute(oid, WriteOp()
				       .cmpext(0, four, &unmatch)
				       .write(0, six)
				       .returnvec()),
			       osd_errc::cmpext_mismatch);
    // 'four' mistmatches 'five' on character 1.
    EXPECT_EQ(1, unmatch);
  }
  // Compare and bail before write due to mismatch. Do it a thousand
  // times to make sure we are hitting some socket injection
  for (auto i = 0; i < 1000; ++i) {
    uint64_t unmatch = -2;
    co_await expect_error_code(execute(fmt::format("test_{}", i), WriteOp()
				       .cmpext(0, four, &unmatch)
				       .write(0, six)
				       .returnvec()),
			       osd_errc::cmpext_mismatch);
    EXPECT_EQ(0, unmatch);
    EXPECT_EQ(0, unmatch);
  }
  co_return;
}
