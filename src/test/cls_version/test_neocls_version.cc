// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "neorados/cls/version.h"

#include <string_view>

#include <boost/asio/io_context.hpp>
#include <boost/system/error_code.hpp>

#include <spawn/spawn.hpp>

#include "include/neorados/RADOS.hpp"

#include "cls/version/cls_version_types.h"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

namespace ba = boost::asio;
namespace bs = boost::system;
namespace cb = ceph::buffer;
namespace nr = neorados;
namespace nrcv = neorados::cls::version;
namespace s = spawn;

TEST(neocls_rgw, test_version_inc_read)
{
  ba::io_context c;
  std::string_view oid = "obj";

  s::spawn(c, [&](s::yield_context y) {
    auto r = nr::RADOS::Builder{}.build(c, y);
    auto pool = create_pool(r, get_temp_pool_name(), y);
    auto sg = make_scope_guard(
      [&] {
	r.delete_pool(pool, y);
      });
    nr::IOContext ioc(pool);
    create_obj(r, oid, ioc, y);
    obj_version ver = nrcv::read(r, oid, ioc, y);
    ASSERT_EQ(0u, ver.ver);
    ASSERT_EQ(0u, ver.tag.size());


    /* inc version */
    nr::WriteOp op;
    nrcv::inc(op);
    r.execute(oid, ioc, std::move(op), y);

    ver = nrcv::read(r, oid, ioc, y);
    ASSERT_GT(ver.ver, 0u);
    ASSERT_NE(0u, ver.tag.size());

    op = nr::WriteOp();
    nrcv::inc(op);
    r.execute(oid, ioc, std::move(op), y);

    auto ver2 = nrcv::read(r, oid, ioc, y);

    ASSERT_GT(ver2.ver, ver.ver);
    ASSERT_EQ(0u, ver2.tag.compare(ver.tag));

    obj_version ver3;

    nr::ReadOp rop;
    nrcv::read(rop, &ver3);
    r.execute(oid, ioc, std::move(rop), nullptr, y);
    ASSERT_EQ(ver2.ver, ver3.ver);
    ASSERT_EQ(1u, ver2.compare(&ver3));
  });
  c.run();
}


TEST(neocls_rgw, test_version_set)
{
  ba::io_context c;
  std::string_view oid = "obj";

  s::spawn(c, [&](s::yield_context y) {
    auto r = nr::RADOS::Builder{}.build(c, y);
    auto pool = create_pool(r, get_temp_pool_name(), y);
    auto sg = make_scope_guard(
      [&] {
	r.delete_pool(pool, y);
      });
    nr::IOContext ioc(pool);
    create_obj(r, oid, ioc, y);

    auto ver = nrcv::read(r, oid, ioc, y);
    ASSERT_EQ(0u, ver.ver);
    ASSERT_EQ(0u, ver.tag.size());

    ver.ver = 123;
    ver.tag = "foo";

    /* set version */
    nr::WriteOp op;
    nrcv::set(op, ver);
    r.execute(oid, ioc, std::move(op), y);

    auto ver2 = nrcv::read(r, oid, ioc, y);

    ASSERT_EQ(ver2.ver, ver.ver);
    ASSERT_EQ(0, ver2.tag.compare(ver.tag));
  });
  c.run();
}

TEST(neocls_rgw, test_version_inc_cond)
{
  ba::io_context c;
  std::string_view oid = "obj";

  s::spawn(c, [&](s::yield_context y) {
    auto r = nr::RADOS::Builder{}.build(c, y);
    auto pool = create_pool(r, get_temp_pool_name(), y);
    auto sg = make_scope_guard(
      [&] {
	r.delete_pool(pool, y);
      });
    nr::IOContext ioc(pool);
    create_obj(r, oid, ioc, y);

    auto ver = nrcv::read(r, oid, ioc, y);

    ASSERT_EQ(0u, ver.ver);
    ASSERT_EQ(0u, ver.tag.size());

    /* inc version */
    nr::WriteOp op;
    nrcv::inc(op);
    r.execute(oid, ioc, std::move(op), y);

    ver = nrcv::read(r, oid, ioc, y);
    ASSERT_GT(ver.ver, 0u);
    ASSERT_NE(0, ver.tag.size());

    auto cond_ver = ver;

    op = nr::WriteOp();
    nrcv::inc(op);
    r.execute(oid, ioc, std::move(op), y);

    auto ver2 = nrcv::read(r, oid, ioc, y);
    ASSERT_GT(ver2.ver, ver.ver);
    ASSERT_EQ(0u, ver2.tag.compare(ver.tag));

    /* now check various condition tests */
    op = nr::WriteOp();
    nrcv::inc(op, cond_ver, VER_COND_NONE);
    r.execute(oid, ioc, std::move(op), y);

    ver2 = nrcv::read(r, oid, ioc, y);
    ASSERT_GT(ver2.ver, ver.ver);
    ASSERT_EQ(0u, ver2.tag.compare(ver.tag));

    /* a bunch of conditions that should fail */
    op = nr::WriteOp();
    nrcv::inc(op, cond_ver, VER_COND_EQ);
    bs::error_code ec;
    r.execute(oid, ioc, std::move(op), y[ec]);
    ASSERT_EQ(bs::errc::operation_canceled, ec);

    op = nr::WriteOp();
    nrcv::inc(op, cond_ver, VER_COND_LT);
    r.execute(oid, ioc, std::move(op), y[ec]);
    ASSERT_EQ(bs::errc::operation_canceled, ec);

    op = nr::WriteOp();
    nrcv::inc(op, cond_ver, VER_COND_LE);
    r.execute(oid, ioc, std::move(op), y[ec]);
    ASSERT_EQ(bs::errc::operation_canceled, ec);

    op = nr::WriteOp();
    nrcv::inc(op, cond_ver, VER_COND_TAG_NE);
    r.execute(oid, ioc, std::move(op), y[ec]);
    ASSERT_EQ(bs::errc::operation_canceled, ec);

    ver2 = nrcv::read(r, oid, ioc, y);
    ASSERT_GT(ver2.ver, ver.ver);
    ASSERT_EQ(0u, ver2.tag.compare(ver.tag));

    /* a bunch of conditions that should succeed */
    op = nr::WriteOp();
    nrcv::inc(op, ver2, VER_COND_EQ);
    r.execute(oid, ioc, std::move(op), y);

    op = nr::WriteOp();
    nrcv::inc(op, cond_ver, VER_COND_GT);
    r.execute(oid, ioc, std::move(op), y);

    op = nr::WriteOp();
    nrcv::inc(op, cond_ver, VER_COND_GE);
    r.execute(oid, ioc, std::move(op), y);

    op = nr::WriteOp();
    nrcv::inc(op, cond_ver, VER_COND_TAG_EQ);
    r.execute(oid, ioc, std::move(op), y);
  });
  c.run();
}

TEST(neocls_rgw, test_version_inc_check)
{
  ba::io_context c;
  std::string_view oid = "obj";

  s::spawn(c, [&](s::yield_context y) {
    auto r = nr::RADOS::Builder{}.build(c, y);
    auto pool = create_pool(r, get_temp_pool_name(), y);
    auto sg = make_scope_guard(
      [&] {
	r.delete_pool(pool, y);
      });
    nr::IOContext ioc(pool);
    create_obj(r, oid, ioc, y);

    auto ver = nrcv::read(r, oid, ioc, y);
    ASSERT_EQ(0u, ver.ver);
    ASSERT_EQ(0u, ver.tag.size());

    /* inc version */
    nr::WriteOp op;
    nrcv::inc(op);
    r.execute(oid, ioc, std::move(op), y);

    ver = nrcv::read(r, oid, ioc, y);
    ASSERT_GT(ver.ver, 0u);
    ASSERT_NE(0u, ver.tag.size());

    obj_version cond_ver = ver;

    /* a bunch of conditions that should succeed */
    nr::ReadOp rop;
    nrcv::check(rop, cond_ver, VER_COND_EQ);
    r.execute(oid, ioc, std::move(rop), nullptr, y);

    rop = nr::ReadOp();
    nrcv::check(rop, cond_ver, VER_COND_GE);
    r.execute(oid, ioc, std::move(rop), nullptr, y);

    rop = nr::ReadOp();
    nrcv::check(rop, cond_ver, VER_COND_LE);
    r.execute(oid, ioc, std::move(rop), nullptr, y);

    rop = nr::ReadOp();
    nrcv::check(rop, cond_ver, VER_COND_TAG_EQ);
    r.execute(oid, ioc, std::move(rop), nullptr, y);

    op = nr::WriteOp();
    nrcv::inc(op);
    r.execute(oid, ioc, std::move(op), y);

    auto ver2 = nrcv::read(r, oid, ioc, y);
    ASSERT_GT(ver2.ver, ver.ver);
    ASSERT_EQ(0, ver2.tag.compare(ver.tag));

    /* a bunch of conditions that should fail */
    rop = nr::ReadOp();
    bs::error_code ec;
    nrcv::check(rop, ver, VER_COND_LT);
    r.execute(oid, ioc, std::move(rop), nullptr, y[ec]);
    ASSERT_EQ(bs::errc::operation_canceled, ec);

    rop = nr::ReadOp();
    nrcv::check(rop, ver, VER_COND_LE);
    r.execute(oid, ioc, std::move(rop), nullptr, y[ec]);
    ASSERT_EQ(bs::errc::operation_canceled, ec);

    rop = nr::ReadOp();
    nrcv::check(rop, ver, VER_COND_TAG_NE);
    r.execute(oid, ioc, std::move(rop), nullptr, y[ec]);
    ASSERT_EQ(bs::errc::operation_canceled, ec);
  });
  c.run();
}
