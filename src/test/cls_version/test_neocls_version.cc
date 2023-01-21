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
#include <boost/asio/redirect_error.hpp>

#include <boost/system/errc.hpp>
#include <boost/system/error_code.hpp>

#include "include/neorados/RADOS.hpp"

#include "cls/version/cls_version_types.h"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

namespace asio = boost::asio;
namespace version = neorados::cls::version;

using boost::system::error_code;
using boost::system::errc::operation_canceled;

CORO_TEST_F(neocls_version, test_version_inc_read, NeoRadosTest)
{
  std::string_view oid = "obj";
  co_await create_obj(rados, oid, pool, asio::use_awaitable);

  auto ver = co_await version::read(rados, oid, pool, asio::use_awaitable);
  EXPECT_EQ(0u, ver.ver);
  EXPECT_EQ(0u, ver.tag.size());

  /* inc version */
  neorados::WriteOp op;
  version::inc(op);
  co_await rados.execute(oid, pool, std::move(op), asio::use_awaitable);

  ver = co_await version::read(rados, oid, pool, asio::use_awaitable);
  EXPECT_GT(ver.ver, 0u);
  EXPECT_NE(0u, ver.tag.size());

  op = neorados::WriteOp();
  version::inc(op);
  co_await rados.execute(oid, pool, std::move(op), asio::use_awaitable);

  auto ver2 = co_await version::read(rados, oid, pool, asio::use_awaitable);

  EXPECT_GT(ver2.ver, ver.ver);
  EXPECT_EQ(0u, ver2.tag.compare(ver.tag));

  obj_version ver3;

  neorados::ReadOp rop;
  version::read(rop, &ver3);
  co_await rados.execute(oid, pool, std::move(rop), nullptr,
			 asio::use_awaitable);
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

  /* set version */
  neorados::WriteOp op;
  version::set(op, ver);
  co_await rados.execute(oid, pool, std::move(op), asio::use_awaitable);

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

  /* inc version */
  neorados::WriteOp op;
  version::inc(op);
  co_await rados.execute(oid, pool, std::move(op), asio::use_awaitable);

  ver = co_await version::read(rados, oid, pool, asio::use_awaitable);
  EXPECT_GT(ver.ver, 0u);
  EXPECT_NE(0, ver.tag.size());

  auto cond_ver = ver;

  op = neorados::WriteOp();
  version::inc(op);
  co_await rados.execute(oid, pool, std::move(op), asio::use_awaitable);

  auto ver2 = co_await version::read(rados, oid, pool, asio::use_awaitable);
  EXPECT_GT(ver2.ver, ver.ver);
  EXPECT_EQ(0u, ver2.tag.compare(ver.tag));

  /* now check various condition tests */
  op = neorados::WriteOp();
  version::inc(op, cond_ver, VER_COND_NONE);
  co_await rados.execute(oid, pool, std::move(op), asio::use_awaitable);

  ver2 = co_await version::read(rados, oid, pool, asio::use_awaitable);
  EXPECT_GT(ver2.ver, ver.ver);
  EXPECT_EQ(0u, ver2.tag.compare(ver.tag));

  /* a bunch of conditions that should fail */
  op = neorados::WriteOp();
  version::inc(op, cond_ver, VER_COND_EQ);
  error_code ec;
  co_await rados.execute(oid, pool, std::move(op),
			 asio::redirect_error(asio::use_awaitable, ec));
  EXPECT_EQ(operation_canceled, ec);

  op = neorados::WriteOp();
  version::inc(op, cond_ver, VER_COND_LT);
  co_await rados.execute(oid, pool, std::move(op),
			 asio::redirect_error(asio::use_awaitable, ec));
  EXPECT_EQ(operation_canceled, ec);

  op = neorados::WriteOp();
  version::inc(op, cond_ver, VER_COND_LE);
  co_await rados.execute(oid, pool, std::move(op),
			 asio::redirect_error(asio::use_awaitable, ec));
  EXPECT_EQ(operation_canceled, ec);

  op = neorados::WriteOp();
  version::inc(op, cond_ver, VER_COND_TAG_NE);
  co_await rados.execute(oid, pool, std::move(op),
			 asio::redirect_error(asio::use_awaitable, ec));
  EXPECT_EQ(operation_canceled, ec);

  ver2 = co_await version::read(rados, oid, pool, asio::use_awaitable);
  EXPECT_GT(ver2.ver, ver.ver);
  EXPECT_EQ(0u, ver2.tag.compare(ver.tag));

  /* a bunch of conditions that should succeed */
  op = neorados::WriteOp();
  version::inc(op, ver2, VER_COND_EQ);
  co_await rados.execute(oid, pool, std::move(op), asio::use_awaitable);

  op = neorados::WriteOp();
  version::inc(op, cond_ver, VER_COND_GT);
  co_await rados.execute(oid, pool, std::move(op), asio::use_awaitable);

  op = neorados::WriteOp();
  version::inc(op, cond_ver, VER_COND_GE);
  co_await rados.execute(oid, pool, std::move(op), asio::use_awaitable);

  op = neorados::WriteOp();
  version::inc(op, cond_ver, VER_COND_TAG_EQ);
  co_await rados.execute(oid, pool, std::move(op), asio::use_awaitable);
}

CORO_TEST_F(neocls_version, test_version_inc_check, NeoRadosTest)
{
  std::string_view oid = "obj";
  co_await create_obj(rados, oid, pool, asio::use_awaitable);

  auto ver = co_await version::read(rados, oid, pool, asio::use_awaitable);
  EXPECT_EQ(0u, ver.ver);
  EXPECT_EQ(0u, ver.tag.size());

  /* inc version */
  neorados::WriteOp op;
  version::inc(op);
  co_await rados.execute(oid, pool, std::move(op), asio::use_awaitable);

  ver = co_await version::read(rados, oid, pool, asio::use_awaitable);
  EXPECT_GT(ver.ver, 0u);
  EXPECT_NE(0u, ver.tag.size());

  obj_version cond_ver = ver;

  /* a bunch of conditions that should succeed */
  neorados::ReadOp rop;
  version::check(rop, cond_ver, VER_COND_EQ);
  co_await rados.execute(oid, pool, std::move(rop), nullptr,
			 asio::use_awaitable);

  rop = neorados::ReadOp();
  version::check(rop, cond_ver, VER_COND_GE);
  co_await rados.execute(oid, pool, std::move(rop), nullptr,
			 asio::use_awaitable);

  rop = neorados::ReadOp();
  version::check(rop, cond_ver, VER_COND_LE);
  co_await rados.execute(oid, pool, std::move(rop), nullptr,
			 asio::use_awaitable);

  rop = neorados::ReadOp();
  version::check(rop, cond_ver, VER_COND_TAG_EQ);
  co_await rados.execute(oid, pool, std::move(rop), nullptr,
			 asio::use_awaitable);

  op = neorados::WriteOp();
  version::inc(op);
  co_await rados.execute(oid, pool, std::move(op), asio::use_awaitable);

  auto ver2 = co_await version::read(rados, oid, pool, asio::use_awaitable);
  EXPECT_GT(ver2.ver, ver.ver);
  EXPECT_EQ(0, ver2.tag.compare(ver.tag));

  /* a bunch of conditions that should fail */
  rop = neorados::ReadOp();
  error_code ec;
  version::check(rop, ver, VER_COND_LT);
  co_await rados.execute(oid, pool, std::move(rop), nullptr,
			 asio::redirect_error(asio::use_awaitable, ec));
  EXPECT_EQ(operation_canceled, ec);

  rop = neorados::ReadOp();
  version::check(rop, ver, VER_COND_LE);
  co_await rados.execute(oid, pool, std::move(rop), nullptr,
			 asio::redirect_error(asio::use_awaitable, ec));
  EXPECT_EQ(operation_canceled, ec);

  rop = neorados::ReadOp();
  version::check(rop, ver, VER_COND_TAG_NE);
  co_await rados.execute(oid, pool, std::move(rop), nullptr,
			 asio::redirect_error(asio::use_awaitable, ec));
  EXPECT_EQ(operation_canceled, ec);
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
		  version::set(op, iver);
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
