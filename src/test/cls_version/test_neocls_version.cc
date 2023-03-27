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

#include "neorados/cls/version.h"

#include <boost/asio/post.hpp>
#include <coroutine>
#include <memory>
#include <string_view>
#include <utility>

#include <boost/asio/use_awaitable.hpp>

#include <boost/system/errc.hpp>
#include <boost/system/error_code.hpp>

#include "include/neorados/RADOS.hpp"

#include "cls/version/cls_version_types.h"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

namespace asio = boost::asio;
namespace version = neorados::cls::version;
using neorados::ReadOp;
using neorados::WriteOp;

using boost::system::error_code;
using boost::system::errc::operation_canceled;

CORO_TEST_F(neocls_version, test_version_inc_read, NeoRadosTest)
{
  std::string_view oid = "obj";
  co_await create_obj(rados, oid, pool, asio::use_awaitable);

  auto ver = co_await version::read(rados, oid, pool, asio::use_awaitable);
  EXPECT_EQ(0u, ver.ver);
  EXPECT_EQ(0u, ver.tag.size());

  // Increment version
  co_await rados.execute(oid, pool, WriteOp{}.exec(version::inc()),
			 asio::use_awaitable);

  ver = co_await version::read(rados, oid, pool, asio::use_awaitable);
  EXPECT_GT(ver.ver, 0u);
  EXPECT_NE(0u, ver.tag.size());

  co_await rados.execute(oid, pool, WriteOp{}.exec(version::inc()),
			 asio::use_awaitable);

  auto ver2 = co_await version::read(rados, oid, pool, asio::use_awaitable);

  EXPECT_GT(ver2.ver, ver.ver);
  EXPECT_EQ(0u, ver2.tag.compare(ver.tag));

  obj_version ver3;
  co_await rados.execute(oid, pool, ReadOp{}.exec(version::read(&ver3)),
			 nullptr, asio::use_awaitable);
  EXPECT_EQ(ver2.ver, ver3.ver);
  EXPECT_EQ(1u, ver2.compare(&ver3));
  co_return;
}


CORO_TEST_F(neocls_version, test_version_set, NeoRadosTest)
{
  std::string_view oid = "obj";
  co_await create_obj(rados, oid, pool, asio::use_awaitable);

  auto ver = co_await version::read(rados, oid, pool, asio::use_awaitable);
  EXPECT_EQ(0u, ver.ver);
  EXPECT_EQ(0u, ver.tag.size());

  ver.ver = 123;
  ver.tag = "foo";

  // Set version
  co_await rados.execute(oid, pool, WriteOp{}.exec(version::set(ver)),
			 asio::use_awaitable);

  auto ver2 = co_await version::read(rados, oid, pool, asio::use_awaitable);

  EXPECT_EQ(ver2.ver, ver.ver);
  EXPECT_EQ(0, ver2.tag.compare(ver.tag));
}

CORO_TEST_F(neocls_version, test_version_inc_cond, NeoRadosTest)
{
  std::string_view oid = "obj";
  co_await create_obj(rados, oid, pool, asio::use_awaitable);

  auto ver = co_await version::read(rados, oid, pool, asio::use_awaitable);

  EXPECT_EQ(0u, ver.ver);
  EXPECT_EQ(0u, ver.tag.size());

  // Increment version
  co_await rados.execute(oid, pool, WriteOp{}.exec(version::inc()),
			 asio::use_awaitable);

  ver = co_await version::read(rados, oid, pool, asio::use_awaitable);
  EXPECT_GT(ver.ver, 0u);
  EXPECT_NE(0, ver.tag.size());

  auto cond_ver = ver;

  
  co_await rados.execute(oid, pool, WriteOp{}.exec(version::inc()),
			 asio::use_awaitable);

  auto ver2 = co_await version::read(rados, oid, pool, asio::use_awaitable);
  EXPECT_GT(ver2.ver, ver.ver);
  EXPECT_EQ(0u, ver2.tag.compare(ver.tag));

  // Now check various condition tests
  co_await rados.execute(oid, pool,
			 WriteOp{}.exec(version::inc(cond_ver, VER_COND_NONE)),
			 asio::use_awaitable);

  ver2 = co_await version::read(rados, oid, pool, asio::use_awaitable);
  EXPECT_GT(ver2.ver, ver.ver);
  EXPECT_EQ(0u, ver2.tag.compare(ver.tag));

  // A bunch of conditions that should fail
  co_await expect_error_code(
    rados.execute(oid, pool,
		  WriteOp{}.exec(version::inc(cond_ver, VER_COND_EQ)),
		  asio::use_awaitable),
    operation_canceled);

  co_await expect_error_code(
    rados.execute(oid, pool,
		  WriteOp{}.exec(version::inc(cond_ver, VER_COND_LT)),
		  asio::use_awaitable),
    operation_canceled);

  co_await expect_error_code(
    rados.execute(oid, pool,
		  WriteOp{}.exec(version::inc(cond_ver, VER_COND_LE)),
		  asio::use_awaitable),
    operation_canceled);

  co_await expect_error_code(
    rados.execute(oid, pool,
		  WriteOp{}.exec(version::inc(cond_ver, VER_COND_TAG_NE)),
		  asio::use_awaitable),
    operation_canceled);

  ver2 = co_await version::read(rados, oid, pool, asio::use_awaitable);
  EXPECT_GT(ver2.ver, ver.ver);
  EXPECT_EQ(0u, ver2.tag.compare(ver.tag));

  /* a bunch of conditions that should succeed */
  co_await rados.execute(oid, pool,
			 WriteOp{}.exec(version::inc(ver2, VER_COND_EQ)),
			 asio::use_awaitable);

  co_await rados.execute(oid, pool,
			 WriteOp{}.exec(version::inc(cond_ver, VER_COND_GT)),
			 asio::use_awaitable);

  co_await rados.execute(oid, pool,
			 WriteOp{}.exec(version::inc(cond_ver, VER_COND_GE)),
			 asio::use_awaitable);

  co_await rados.execute(oid, pool,
			 WriteOp{}.exec(version::inc(cond_ver, VER_COND_TAG_EQ)),
			 asio::use_awaitable);
}

CORO_TEST_F(neocls_version, test_version_inc_check, NeoRadosTest)
{
  std::string_view oid = "obj";
  co_await create_obj(rados, oid, pool, asio::use_awaitable);

  auto ver = co_await version::read(rados, oid, pool, asio::use_awaitable);
  EXPECT_EQ(0u, ver.ver);
  EXPECT_EQ(0u, ver.tag.size());

  // Increment version
  co_await rados.execute(oid, pool, WriteOp{}.exec(version::inc()),
			 asio::use_awaitable);

  ver = co_await version::read(rados, oid, pool, asio::use_awaitable);
  EXPECT_GT(ver.ver, 0u);
  EXPECT_NE(0u, ver.tag.size());

  obj_version cond_ver = ver;

  // a bunch of conditions that should succeed
  co_await rados.execute(oid, pool,
			 ReadOp{}.exec(version::check(cond_ver, VER_COND_EQ)),
			 nullptr, asio::use_awaitable);

  co_await rados.execute(oid, pool,
			 ReadOp{}.exec(version::check(cond_ver, VER_COND_GE)),
			 nullptr, asio::use_awaitable);

  co_await rados.execute(oid, pool,
			 ReadOp{}.exec(version::check(cond_ver, VER_COND_LE)),
			 nullptr, asio::use_awaitable);

  co_await rados.execute(oid, pool,
			 ReadOp{}.exec(version::check(cond_ver, VER_COND_TAG_EQ)),
			 nullptr, asio::use_awaitable);

  co_await rados.execute(oid, pool, WriteOp{}.exec(version::inc()),
			 asio::use_awaitable);

  auto ver2 = co_await version::read(rados, oid, pool, asio::use_awaitable);
  EXPECT_GT(ver2.ver, ver.ver);
  EXPECT_EQ(0, ver2.tag.compare(ver.tag));

  // A bunch of conditions that should fail
  co_await expect_error_code(
    rados.execute(oid, pool, ReadOp{}.exec(version::check(ver, VER_COND_LT)),
		  nullptr, asio::use_awaitable),
    operation_canceled);

  co_await expect_error_code(
    rados.execute(oid, pool, ReadOp{}.exec(version::check(ver, VER_COND_LE)),
		  nullptr, asio::use_awaitable),
    operation_canceled);

  
  co_await expect_error_code(
    rados.execute(oid, pool, ReadOp{}.exec(version::check(ver, VER_COND_TAG_NE)),
		  nullptr, asio::use_awaitable),
    operation_canceled);
}

TEST(neocls_version_bare, lambdata)
{
  asio::io_context c;

  std::string_view oid = "obj";

  obj_version iver{123, "foo"};
  obj_version ever;

  std::optional<neorados::RADOS> rados;
  neorados::IOContext pool;
  neorados::RADOS::Builder{}.build(c, [&](error_code ec, neorados::RADOS r_) {
    ASSERT_FALSE(ec);
    rados = std::move(r_);
    create_pool(*rados, get_temp_pool_name(),
		[&](error_code ec, int64_t poolid) {
		  ASSERT_FALSE(ec);
		  pool.pool(poolid);
		  neorados::WriteOp op;
		  op.create(true);
		  op.exec(version::set(iver));
		  rados->execute(oid, pool, std::move(op), [&](error_code ec) {
		    ASSERT_FALSE(ec);
		    version::read(*rados, oid, pool,
				  [&](error_code ec, obj_version over) {
				    ASSERT_FALSE(ec);
				    ASSERT_EQ(iver, over);
				    ever = over;
				  });
		  });
		});
  });
  c.run();
  ASSERT_EQ(iver, ever);
}
