// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "librados/librados_asio.h"

#include <optional>
#include <gtest/gtest.h>

#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "common/errno.h"
#include "global/global_init.h"

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/asio/use_future.hpp>

#include <optional>

#define dout_subsys ceph_subsys_rados
#define dout_context g_ceph_context

// test fixture for global setup/teardown
class AsioRados : public ::testing::Test {
  static constexpr auto poolname = "ceph_test_rados_api_asio";

 protected:
  static librados::Rados rados;
  static librados::IoCtx io;
  // writes to snapio fail immediately with -EROFS. this is used to test errors
  // that come from inside the initiating function, rather than passed to the
  // AioCompletion callback
  static librados::IoCtx snapio;

 public:
  static void SetUpTestCase() {
    ASSERT_EQ(0, rados.init_with_context(g_ceph_context));
    ASSERT_EQ(0, rados.connect());
    // open/create test pool
    int r = rados.ioctx_create(poolname, io);
    if (r == -ENOENT) {
      r = rados.pool_create(poolname);
      if (r == -EEXIST) {
        r = 0;
      } else if (r == 0) {
        r = rados.ioctx_create(poolname, io);
      }
    }
    ASSERT_EQ(0, r);
    ASSERT_EQ(0, rados.ioctx_create(poolname, snapio));
    snapio.snap_set_read(1);
    // initialize the "exist" object
    bufferlist bl;
    bl.append("hello");
    ASSERT_EQ(0, io.write_full("exist", bl));
  }

  static void TearDownTestCase() {
    rados.shutdown();
  }
};
librados::Rados AsioRados::rados;
librados::IoCtx AsioRados::io;
librados::IoCtx AsioRados::snapio;

using boost::system::error_code;
using read_result = std::tuple<version_t, bufferlist>;

void rethrow(std::exception_ptr eptr) {
  if (eptr) std::rethrow_exception(eptr);
}

auto capture(std::optional<error_code>& out) {
  return [&out] (error_code ec, ...) { out = ec; };
}

auto capture(boost::asio::cancellation_signal& signal,
             std::optional<error_code>& out) {
  return boost::asio::bind_cancellation_slot(signal.slot(), capture(out));
}

TEST_F(AsioRados, AsyncReadCallback)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto success_cb = [&] (error_code ec, version_t ver, bufferlist bl) {
    EXPECT_FALSE(ec);
    EXPECT_LT(0, ver);
    EXPECT_EQ("hello", bl.to_str());
  };
  librados::async_read(ex, io, "exist", 256, 0, success_cb);

  auto failure_cb = [&] (error_code ec, version_t ver, bufferlist bl) {
    EXPECT_EQ(boost::system::errc::no_such_file_or_directory, ec);
    EXPECT_EQ(0, ver);
    EXPECT_EQ(0, bl.length());
  };
  librados::async_read(ex, io, "noexist", 256, 0, failure_cb);

  service.run();
}

TEST_F(AsioRados, AsyncReadDeferred)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto init1 = [&] { // pass local variables that go out of scope
    librados::IoCtx ioc = io;
    std::string oid = "exist";
    return librados::async_read(ex, ioc, oid, 256, 0, boost::asio::deferred);
  }();
  std::move(init1)([] (error_code ec, version_t ver, bufferlist bl) {
    EXPECT_FALSE(ec);
    EXPECT_LT(0, ver);
    EXPECT_EQ("hello", bl.to_str());
  });

  auto init2 = [&] {
    librados::IoCtx ioc = io;
    std::string oid = "noexist";
    return librados::async_read(ex, ioc, oid, 256, 0, boost::asio::deferred);
  }();
  std::move(init2)([] (error_code ec, version_t ver, bufferlist bl) {
    EXPECT_EQ(boost::system::errc::no_such_file_or_directory, ec);
    EXPECT_EQ(0, ver);
    EXPECT_EQ(0, bl.length());
  });

  service.run();
}

TEST_F(AsioRados, AsyncReadFuture)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto f1 = librados::async_read(ex, io, "exist", 256,
                                 0, boost::asio::use_future);
  auto f2 = librados::async_read(ex, io, "noexist", 256,
                                 0, boost::asio::use_future);

  service.run();

  auto [ver, bl] = f1.get();
  EXPECT_LT(0, ver);
  EXPECT_EQ("hello", bl.to_str());

  EXPECT_THROW(f2.get(), boost::system::system_error);
}

TEST_F(AsioRados, AsyncReadYield)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto success_cr = [&] (boost::asio::yield_context yield) {
    error_code ec;
    auto [ver, bl] = librados::async_read(ex, io, "exist", 256,
                                          0, yield[ec]);
    EXPECT_FALSE(ec);
    EXPECT_LT(0, ver);
    EXPECT_EQ("hello", bl.to_str());
  };
  boost::asio::spawn(ex, success_cr, rethrow);

  auto failure_cr = [&] (boost::asio::yield_context yield) {
    error_code ec;
    auto [ver, bl] = librados::async_read(ex, io, "noexist", 256,
                                          0, yield[ec]);
    EXPECT_EQ(boost::system::errc::no_such_file_or_directory, ec);
    EXPECT_EQ(0, ver);
    EXPECT_EQ(0, bl.length());
  };
  boost::asio::spawn(ex, failure_cr, rethrow);

  service.run();
}

template <typename ...Args>
auto capture(std::optional<std::tuple<std::exception_ptr, Args...>>& out) {
  return [&out] (std::exception_ptr eptr, std::tuple<Args...> args) {
    out = std::tuple_cat(std::make_tuple(eptr), std::move(args));
  };
}

TEST_F(AsioRados, AsyncReadAwaitable)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();

  using result_tuple = std::tuple<std::exception_ptr, version_t, bufferlist>;

  std::optional<result_tuple> result1;
  boost::asio::co_spawn(ex,
                        librados::async_read(ex, io, "exist", 256, 0,
                                             boost::asio::use_awaitable),
                        capture(result1));

  std::optional<result_tuple> result2;
  boost::asio::co_spawn(ex,
                        librados::async_read(ex, io, "noexist", 256, 0,
                                             boost::asio::use_awaitable),
                        capture(result2));

  service.run();

  {
    ASSERT_TRUE(result1);
    auto [eptr, ver, bl] = std::move(*result1);
    EXPECT_FALSE(eptr);
    EXPECT_LT(0, ver);
    EXPECT_EQ("hello", bl.to_str());
  }
  {
    ASSERT_TRUE(result2);
    auto [eptr, ver, bl] = std::move(*result2);
    ASSERT_TRUE(eptr);
    try {
      std::rethrow_exception(eptr);
    } catch (const boost::system::system_error& e) {
      EXPECT_EQ(e.code(), std::errc::no_such_file_or_directory);
    } catch (const std::exception&) {
      EXPECT_THROW(throw, boost::system::system_error);
    }
    EXPECT_EQ(0, ver);
    EXPECT_EQ(0, bl.length());
  }
}

TEST_F(AsioRados, AsyncWriteCallback)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();

  bufferlist bl;
  bl.append("hello");

  auto success_cb = [&] (error_code ec, version_t ver) {
    EXPECT_FALSE(ec);
    EXPECT_LT(0, ver);
  };
  librados::async_write(ex, io, "exist", bl, bl.length(), 0,
                        success_cb);

  auto failure_cb = [&] (error_code ec, version_t ver) {
    EXPECT_EQ(boost::system::errc::read_only_file_system, ec);
    EXPECT_EQ(0, ver);
  };
  librados::async_write(ex, snapio, "exist", bl, bl.length(), 0,
                        failure_cb);

  service.run();
}

TEST_F(AsioRados, AsyncWriteDeferred)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto init1 = [&] { // pass local variables that go out of scope
    librados::IoCtx ioc = io;
    std::string oid = "exist";
    bufferlist bl;
    bl.append("hello");
    return librados::async_write(ex, ioc, oid, bl, bl.length(), 0,
                                 boost::asio::deferred);
  }();
  std::move(init1)([] (error_code ec, version_t ver) {
    EXPECT_FALSE(ec);
    EXPECT_LT(0, ver);
  });

  auto init2 = [&] {
    librados::IoCtx ioc = snapio;
    std::string oid = "exist";
    bufferlist bl;
    bl.append("hello");
    return librados::async_write(ex, ioc, oid, bl, bl.length(), 0,
                                 boost::asio::deferred);
  }();
  std::move(init2)([] (error_code ec, version_t ver) {
    EXPECT_EQ(boost::system::errc::read_only_file_system, ec);
    EXPECT_EQ(0, ver);
  });

  service.run();
}

TEST_F(AsioRados, AsyncWriteFuture)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();

  bufferlist bl;
  bl.append("hello");

  auto f1 = librados::async_write(ex, io, "exist", bl, bl.length(), 0,
                                  boost::asio::use_future);
  auto f2 = librados::async_write(ex, snapio, "exist", bl, bl.length(), 0,
                                  boost::asio::use_future);

  service.run();

  EXPECT_LT(0, f1.get());
  EXPECT_THROW(f2.get(), boost::system::system_error);
}

TEST_F(AsioRados, AsyncWriteYield)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();

  bufferlist bl;
  bl.append("hello");

  auto success_cr = [&] (boost::asio::yield_context yield) {
    error_code ec;
    auto ver = librados::async_write(ex, io, "exist", bl,
                                     bl.length(), 0, yield[ec]);
    EXPECT_FALSE(ec);
    EXPECT_LT(0, ver);
    EXPECT_EQ("hello", bl.to_str());
  };
  boost::asio::spawn(ex, success_cr, rethrow);

  auto failure_cr = [&] (boost::asio::yield_context yield) {
    error_code ec;
    auto ver = librados::async_write(ex, snapio, "exist", bl,
                                     bl.length(), 0, yield[ec]);
    EXPECT_EQ(boost::system::errc::read_only_file_system, ec);
    EXPECT_EQ(0, ver);
  };
  boost::asio::spawn(ex, failure_cr, rethrow);

  service.run();
}

TEST_F(AsioRados, AsyncWriteAwaitable)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();

  bufferlist bl;
  bl.append("hello");

  using result_tuple = std::tuple<std::exception_ptr, version_t>;

  std::optional<result_tuple> result1;
  boost::asio::co_spawn(ex,
                        librados::async_write(ex, io, "exist", bl, bl.length(),
                                              0, boost::asio::use_awaitable),
                        capture(result1));

  std::optional<result_tuple> result2;
  boost::asio::co_spawn(ex,
                        librados::async_write(ex, snapio, "exist", bl, bl.length(),
                                              0, boost::asio::use_awaitable),
                        capture(result2));

  service.run();

  {
    ASSERT_TRUE(result1);
    auto [eptr, ver] = std::move(*result1);
    EXPECT_FALSE(eptr);
    EXPECT_LT(0, ver);
  }
  {
    ASSERT_TRUE(result2);
    auto [eptr, ver] = std::move(*result2);
    ASSERT_TRUE(eptr);
    try {
      std::rethrow_exception(eptr);
    } catch (const boost::system::system_error& e) {
      EXPECT_EQ(e.code(), std::errc::read_only_file_system);
    } catch (const std::exception&) {
      EXPECT_THROW(throw, boost::system::system_error);
    }
    EXPECT_EQ(0, ver);
  }
}

TEST_F(AsioRados, AsyncReadOperationCallback)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();
  {
    librados::ObjectReadOperation op;
    op.read(0, 0, nullptr, nullptr);
    auto success_cb = [&] (error_code ec, version_t ver, bufferlist bl) {
      EXPECT_FALSE(ec);
      EXPECT_LT(0, ver);
      EXPECT_EQ("hello", bl.to_str());
    };
    librados::async_operate(ex, io, "exist", std::move(op),
                            0, nullptr, success_cb);
  }
  {
    librados::ObjectReadOperation op;
    op.read(0, 0, nullptr, nullptr);
    auto failure_cb = [&] (error_code ec, version_t ver, bufferlist bl) {
      EXPECT_EQ(boost::system::errc::no_such_file_or_directory, ec);
      EXPECT_EQ(0, ver);
      EXPECT_EQ(0, bl.length());
    };
    librados::async_operate(ex, io, "noexist", std::move(op),
                            0, nullptr, failure_cb);
  }
  service.run();
}

TEST_F(AsioRados, AsyncReadOperationDeferred)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto init1 = [&] { // pass local variables that go out of scope
    librados::IoCtx ioc = io;
    std::string oid = "exist";
    librados::ObjectReadOperation op;
    op.read(0, 0, nullptr, nullptr);
    return librados::async_operate(ex, ioc, oid, std::move(op),
                                   0, nullptr, boost::asio::deferred);
  }();
  std::move(init1)([] (error_code ec, version_t ver, bufferlist bl) {
    EXPECT_FALSE(ec);
    EXPECT_LT(0, ver);
    EXPECT_EQ("hello", bl.to_str());
  });

  auto init2 = [&] {
    librados::IoCtx ioc = io;
    std::string oid = "noexist";
    librados::ObjectReadOperation op;
    op.read(0, 0, nullptr, nullptr);
    return librados::async_operate(ex, ioc, oid, std::move(op),
                                   0, nullptr, boost::asio::deferred);
  }();
  std::move(init2)([] (error_code ec, version_t ver, bufferlist bl) {
    EXPECT_EQ(boost::system::errc::no_such_file_or_directory, ec);
    EXPECT_EQ(0, ver);
    EXPECT_EQ(0, bl.length());
  });

  service.run();
}

TEST_F(AsioRados, AsyncReadOperationFuture)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();
  std::future<read_result> f1;
  {
    librados::ObjectReadOperation op;
    op.read(0, 0, nullptr, nullptr);
    f1 = librados::async_operate(ex, io, "exist", std::move(op),
                                 0, nullptr, boost::asio::use_future);
  }
  std::future<read_result> f2;
  {
    librados::ObjectReadOperation op;
    op.read(0, 0, nullptr, nullptr);
    f2 = librados::async_operate(ex, io, "noexist", std::move(op),
                                 0, nullptr, boost::asio::use_future);
  }
  service.run();

  auto [ver, bl] = f1.get();
  EXPECT_LT(0, ver);
  EXPECT_EQ("hello", bl.to_str());

  EXPECT_THROW(f2.get(), boost::system::system_error);
}

TEST_F(AsioRados, AsyncReadOperationYield)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto success_cr = [&] (boost::asio::yield_context yield) {
    librados::ObjectReadOperation op;
    op.read(0, 0, nullptr, nullptr);
    error_code ec;
    auto [ver, bl] = librados::async_operate(ex, io, "exist", std::move(op),
                                             0, nullptr, yield[ec]);
    EXPECT_FALSE(ec);
    EXPECT_LT(0, ver);
    EXPECT_EQ("hello", bl.to_str());
  };
  boost::asio::spawn(ex, success_cr, rethrow);

  auto failure_cr = [&] (boost::asio::yield_context yield) {
    librados::ObjectReadOperation op;
    op.read(0, 0, nullptr, nullptr);
    error_code ec;
    auto [ver, bl] = librados::async_operate(ex, io, "noexist", std::move(op),
                                             0, nullptr, yield[ec]);
    EXPECT_EQ(boost::system::errc::no_such_file_or_directory, ec);
    EXPECT_EQ(0, ver);
    EXPECT_EQ(0, bl.length());
  };
  boost::asio::spawn(ex, failure_cr, rethrow);

  service.run();
}

TEST_F(AsioRados, AsyncReadOperationAwaitable)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();

  using result_tuple = std::tuple<std::exception_ptr, version_t, bufferlist>;

  std::optional<result_tuple> result1;
  librados::ObjectReadOperation op1;
  op1.read(0, 0, nullptr, nullptr);
  boost::asio::co_spawn(
      ex,
      librados::async_operate(ex, io, "exist", std::move(op1),
                              0, nullptr, boost::asio::use_awaitable),
      capture(result1));

  std::optional<result_tuple> result2;
  librados::ObjectReadOperation op2;
  op2.read(0, 0, nullptr, nullptr);
  boost::asio::co_spawn(
      ex,
      librados::async_operate(ex, io, "noexist", std::move(op2),
                              0, nullptr, boost::asio::use_awaitable),
      capture(result2));

  service.run();

  {
    ASSERT_TRUE(result1);
    auto [eptr, ver, bl] = std::move(*result1);
    EXPECT_FALSE(eptr);
    EXPECT_LT(0, ver);
    EXPECT_EQ("hello", bl.to_str());
  }
  {
    ASSERT_TRUE(result2);
    auto [eptr, ver, bl] = std::move(*result2);
    ASSERT_TRUE(eptr);
    try {
      std::rethrow_exception(eptr);
    } catch (const boost::system::system_error& e) {
      EXPECT_EQ(e.code(), std::errc::no_such_file_or_directory);
    } catch (const std::exception&) {
      EXPECT_THROW(throw, boost::system::system_error);
    }
    EXPECT_EQ(0, ver);
    EXPECT_EQ(0, bl.length());
  }
}

TEST_F(AsioRados, AsyncWriteOperationCallback)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();

  bufferlist bl;
  bl.append("hello");

  {
    librados::ObjectWriteOperation op;
    op.write_full(bl);
    auto success_cb = [&] (error_code ec, version_t ver) {
      EXPECT_FALSE(ec);
      EXPECT_LT(0, ver);
    };
    librados::async_operate(ex, io, "exist", std::move(op),
                            0, nullptr, success_cb);
  }
  {
    librados::ObjectWriteOperation op;
    op.write_full(bl);
    auto failure_cb = [&] (error_code ec, version_t ver) {
      EXPECT_EQ(boost::system::errc::read_only_file_system, ec);
      EXPECT_EQ(0, ver);
    };
    librados::async_operate(ex, snapio, "exist", std::move(op),
                            0, nullptr, failure_cb);
  }
  service.run();
}

TEST_F(AsioRados, AsyncWriteOperationDeferred)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto init1 = [&] { // pass local variables that go out of scope
    librados::IoCtx ioc = io;
    std::string oid = "exist";
    bufferlist bl;
    bl.append("hello");
    librados::ObjectWriteOperation op;
    op.write_full(bl);
    return librados::async_operate(ex, ioc, oid, std::move(op),
                                   0, nullptr, boost::asio::deferred);
  }();
  std::move(init1)([] (error_code ec, version_t ver) {
    EXPECT_FALSE(ec);
    EXPECT_LT(0, ver);
  });

  auto init2 = [&] {
    librados::IoCtx ioc = snapio;
    std::string oid = "exist";
    bufferlist bl;
    bl.append("hello");
    librados::ObjectWriteOperation op;
    op.write_full(bl);
    return librados::async_operate(ex, ioc, oid, std::move(op),
                                   0, nullptr, boost::asio::deferred);
  }();
  std::move(init2)([] (error_code ec, version_t ver) {
    EXPECT_EQ(boost::system::errc::read_only_file_system, ec);
    EXPECT_EQ(0, ver);
  });

  service.run();
}

TEST_F(AsioRados, AsyncWriteOperationFuture)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();

  bufferlist bl;
  bl.append("hello");

  std::future<version_t> f1;
  {
    librados::ObjectWriteOperation op;
    op.write_full(bl);
    f1 = librados::async_operate(ex, io, "exist", std::move(op),
                                 0, nullptr, boost::asio::use_future);
  }
  std::future<version_t> f2;
  {
    librados::ObjectWriteOperation op;
    op.write_full(bl);
    f2 = librados::async_operate(ex, snapio, "exist", std::move(op),
                                 0, nullptr, boost::asio::use_future);
  }
  service.run();

  EXPECT_LT(0, f1.get());
  EXPECT_THROW(f2.get(), boost::system::system_error);
}

TEST_F(AsioRados, AsyncWriteOperationYield)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();

  bufferlist bl;
  bl.append("hello");

  auto success_cr = [&] (boost::asio::yield_context yield) {
    librados::ObjectWriteOperation op;
    op.write_full(bl);
    error_code ec;
    auto ver = librados::async_operate(ex, io, "exist", std::move(op),
                                       0, nullptr, yield[ec]);
    EXPECT_FALSE(ec);
    EXPECT_LT(0, ver);
  };
  boost::asio::spawn(ex, success_cr, rethrow);

  auto failure_cr = [&] (boost::asio::yield_context yield) {
    librados::ObjectWriteOperation op;
    op.write_full(bl);
    error_code ec;
    auto ver = librados::async_operate(ex, snapio, "exist", std::move(op),
                                       0, nullptr, yield[ec]);
    EXPECT_EQ(boost::system::errc::read_only_file_system, ec);
    EXPECT_EQ(0, ver);
  };
  boost::asio::spawn(ex, failure_cr, rethrow);

  service.run();
}

TEST_F(AsioRados, AsyncWriteOperationAwaitable)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();

  bufferlist bl;
  bl.append("hello");

  using result_tuple = std::tuple<std::exception_ptr, version_t>;

  std::optional<result_tuple> result1;
  librados::ObjectWriteOperation op1;
  op1.write_full(bl);
  boost::asio::co_spawn(
      ex,
      librados::async_operate(ex, io, "exist", std::move(op1),
                              0, nullptr, boost::asio::use_awaitable),
      capture(result1));

  std::optional<result_tuple> result2;
  librados::ObjectWriteOperation op2;
  op2.write_full(bl);
  boost::asio::co_spawn(
      ex,
      librados::async_operate(ex, snapio, "exist", std::move(op2),
                              0, nullptr, boost::asio::use_awaitable),
      capture(result2));

  service.run();

  {
    ASSERT_TRUE(result1);
    auto [eptr, ver] = std::move(*result1);
    EXPECT_FALSE(eptr);
    EXPECT_LT(0, ver);
  }
  {
    ASSERT_TRUE(result2);
    auto [eptr, ver] = std::move(*result2);
    ASSERT_TRUE(eptr);
    try {
      std::rethrow_exception(eptr);
    } catch (const boost::system::system_error& e) {
      EXPECT_EQ(e.code(), std::errc::read_only_file_system);
    } catch (const std::exception&) {
      EXPECT_THROW(throw, boost::system::system_error);
    }
    EXPECT_EQ(0, ver);
  }
}

TEST_F(AsioRados, AsyncNotifyCallback)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();

  bufferlist bl;
  bl.append("hello");
  constexpr uint64_t timeout = 0;

  auto success_cb = [&] (error_code ec, version_t ver, bufferlist reply) {
    EXPECT_FALSE(ec);
    EXPECT_LT(0, ver);
    std::vector<librados::notify_ack_t> acks;
    std::vector<librados::notify_timeout_t> timeouts;
    io.decode_notify_response(reply, &acks, &timeouts);
  };
  librados::async_notify(ex, io, "exist", bl, timeout, success_cb);

  auto failure_cb = [] (error_code ec, version_t ver, bufferlist reply) {
    EXPECT_EQ(boost::system::errc::no_such_file_or_directory, ec);
    EXPECT_EQ(0, ver);
    EXPECT_EQ(0, reply.length());
  };
  librados::async_notify(ex, io, "noexist", bl, timeout, failure_cb);

  service.run();
}

TEST_F(AsioRados, AsyncNotifyDeferred)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();

  constexpr uint64_t timeout = 0;

  auto init1 = [&] { // pass local variables that go out of scope
    librados::IoCtx ioc = io;
    std::string oid = "exist";
    bufferlist bl;
    bl.append("hello");
    return librados::async_notify(ex, ioc, oid, bl, timeout,
                                  boost::asio::deferred);
  }();
  std::move(init1)([&] (error_code ec, version_t ver, bufferlist reply) {
    EXPECT_FALSE(ec);
    EXPECT_LT(0, ver);
    std::vector<librados::notify_ack_t> acks;
    std::vector<librados::notify_timeout_t> timeouts;
    io.decode_notify_response(reply, &acks, &timeouts);
  });

  auto init2 = [&] {
    librados::IoCtx ioc = io;
    std::string oid = "noexist";
    bufferlist bl;
    bl.append("hello");
    return librados::async_notify(ex, ioc, oid, bl, timeout,
                                  boost::asio::deferred);
  }();
  std::move(init2)([] (error_code ec, version_t ver, bufferlist reply) {
    EXPECT_EQ(boost::system::errc::no_such_file_or_directory, ec);
    EXPECT_EQ(0, ver);
    EXPECT_EQ(0, reply.length());
  });

  service.run();
}

TEST_F(AsioRados, AsyncNotifyFuture)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();

  bufferlist bl;
  bl.append("hello");
  constexpr uint64_t timeout = 0;

  auto f1 = librados::async_notify(ex, io, "exist", bl, timeout,
                                   boost::asio::use_future);
  auto f2 = librados::async_notify(ex, io, "noexist", bl, timeout,
                                   boost::asio::use_future);

  service.run();

  auto [ver, reply] = f1.get();
  EXPECT_LT(0, ver);
  std::vector<librados::notify_ack_t> acks;
  std::vector<librados::notify_timeout_t> timeouts;
  io.decode_notify_response(reply, &acks, &timeouts);

  EXPECT_THROW(f2.get(), boost::system::system_error);
}

TEST_F(AsioRados, AsyncNotifyYield)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();

  bufferlist bl;
  bl.append("hello");
  constexpr uint64_t timeout = 0;

  auto success_cr = [&] (boost::asio::yield_context yield) {
    error_code ec;
    auto [ver, reply] = librados::async_notify(ex, io, "exist", bl,
                                               timeout, yield[ec]);
    EXPECT_FALSE(ec);
    EXPECT_LT(0, ver);
    std::vector<librados::notify_ack_t> acks;
    std::vector<librados::notify_timeout_t> timeouts;
    io.decode_notify_response(reply, &acks, &timeouts);
  };
  boost::asio::spawn(ex, success_cr, rethrow);

  auto failure_cr = [&] (boost::asio::yield_context yield) {
    error_code ec;
    auto [ver, reply] = librados::async_notify(ex, io, "noexist", bl,
                                               timeout, yield[ec]);
    EXPECT_EQ(boost::system::errc::no_such_file_or_directory, ec);
    EXPECT_EQ(0, ver);
    EXPECT_EQ(0, reply.length());
  };
  boost::asio::spawn(ex, failure_cr, rethrow);

  service.run();
}

TEST_F(AsioRados, AsyncNotifyAwaitable)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();

  bufferlist bl;
  bl.append("hello");
  constexpr uint64_t timeout = 0;

  using result_tuple = std::tuple<std::exception_ptr, version_t, bufferlist>;

  std::optional<result_tuple> result1;
  boost::asio::co_spawn(ex,
                        librados::async_notify(ex, io, "exist", bl, timeout,
                                               boost::asio::use_awaitable),
                        capture(result1));

  std::optional<result_tuple> result2;
  boost::asio::co_spawn(ex,
                        librados::async_notify(ex, io, "noexist", bl, timeout,
                                               boost::asio::use_awaitable),
                        capture(result2));

  service.run();

  {
    ASSERT_TRUE(result1);
    auto [eptr, ver, reply] = std::move(*result1);
    EXPECT_FALSE(eptr);
    EXPECT_LT(0, ver);
    std::vector<librados::notify_ack_t> acks;
    std::vector<librados::notify_timeout_t> timeouts;
    io.decode_notify_response(reply, &acks, &timeouts);
  }
  {
    ASSERT_TRUE(result2);
    auto [eptr, ver, reply] = std::move(*result2);
    ASSERT_TRUE(eptr);
    try {
      std::rethrow_exception(eptr);
    } catch (const boost::system::system_error& e) {
      EXPECT_EQ(e.code(), std::errc::no_such_file_or_directory);
    } catch (const std::exception&) {
      EXPECT_THROW(throw, boost::system::system_error);
    }
    EXPECT_EQ(0, ver);
    EXPECT_EQ(0, reply.length());
  }
}

// FIXME: this crashes on windows with:
// Thread 1 received signal SIGILL, Illegal instruction.
#ifndef _WIN32

TEST_F(AsioRados, AsyncReadOperationCancelTerminal)
{
  // cancellation tests are racy, so retry if completion beats the cancellation
  boost::system::error_code ec;
  int tries = 10;
  do {
    boost::asio::io_context service;
    auto ex = service.get_executor();
    boost::asio::cancellation_signal signal;
    std::optional<error_code> result;

    librados::ObjectReadOperation op;
    op.assert_exists();
    librados::async_operate(ex, io, "noexist", std::move(op), 0, nullptr,
                            capture(signal, result));

    service.poll();
    EXPECT_FALSE(service.stopped());
    EXPECT_FALSE(result);

    signal.emit(boost::asio::cancellation_type::terminal);

    service.run();
    ASSERT_TRUE(result);
    ec = *result;

    signal.emit(boost::asio::cancellation_type::all); // noop
  } while (ec == std::errc::no_such_file_or_directory && --tries);

  EXPECT_EQ(ec, boost::asio::error::operation_aborted);
}

TEST_F(AsioRados, AsyncReadOperationCancelTotal)
{
  // cancellation tests are racy, so retry if completion beats the cancellation
  boost::system::error_code ec;
  int tries = 10;
  do {
    boost::asio::io_context service;
    auto ex = service.get_executor();
    boost::asio::cancellation_signal signal;
    std::optional<error_code> result;

    librados::ObjectReadOperation op;
    op.assert_exists();
    librados::async_operate(ex, io, "noexist", std::move(op), 0, nullptr,
                            capture(signal, result));

    service.poll();
    EXPECT_FALSE(service.stopped());
    EXPECT_FALSE(result);

    signal.emit(boost::asio::cancellation_type::total);

    service.run();
    ASSERT_TRUE(result);
    ec = *result;

    signal.emit(boost::asio::cancellation_type::all); // noop
  } while (ec == std::errc::no_such_file_or_directory && --tries);

  EXPECT_EQ(ec, boost::asio::error::operation_aborted);
}

TEST_F(AsioRados, AsyncWriteOperationCancelTerminal)
{
  // cancellation tests are racy, so retry if completion beats the cancellation
  boost::system::error_code ec;
  int tries = 10;
  do {
    boost::asio::io_context service;
    auto ex = service.get_executor();
    boost::asio::cancellation_signal signal;
    std::optional<error_code> result;

    librados::ObjectWriteOperation op;
    op.assert_exists();
    librados::async_operate(ex, io, "noexist", std::move(op), 0, nullptr,
                            capture(signal, result));

    service.poll();
    EXPECT_FALSE(service.stopped());
    EXPECT_FALSE(result);

    signal.emit(boost::asio::cancellation_type::terminal);

    service.run();
    ASSERT_TRUE(result);
    ec = *result;

    signal.emit(boost::asio::cancellation_type::all); // noop
  } while (ec == std::errc::no_such_file_or_directory && --tries);

  EXPECT_EQ(ec, boost::asio::error::operation_aborted);
}

TEST_F(AsioRados, AsyncWriteOperationCancelTotal)
{
  boost::asio::io_context service;
  auto ex = service.get_executor();
  boost::asio::cancellation_signal signal;
  std::optional<error_code> ec;

  librados::ObjectWriteOperation op;
  op.assert_exists();
  librados::async_operate(ex, io, "noexist", std::move(op), 0, nullptr,
                          capture(signal, ec));

  service.poll();
  EXPECT_FALSE(service.stopped());
  EXPECT_FALSE(ec);

  // noop, write only supports terminal
  signal.emit(boost::asio::cancellation_type::total);

  service.run();
  ASSERT_TRUE(ec);
  EXPECT_EQ(ec, std::errc::no_such_file_or_directory);

  signal.emit(boost::asio::cancellation_type::all); // noop
}

#endif // not _WIN32

int main(int argc, char **argv)
{
  auto args = argv_to_vec(argc, argv);
  env_to_vec(args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct.get());

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
