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
#include <gtest/gtest.h>

#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "common/errno.h"
#include "global/global_init.h"

#include <boost/range/begin.hpp>
#include <boost/range/end.hpp>
#include <spawn/spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#define dout_subsys ceph_subsys_rados
#define dout_context g_ceph_context

using namespace std;

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

TEST_F(AsioRados, AsyncReadCallback)
{
  boost::asio::io_context service;

  auto success_cb = [&] (boost::system::error_code ec, bufferlist bl) {
    EXPECT_FALSE(ec);
    EXPECT_EQ("hello", bl.to_str());
  };
  librados::async_read(service, io, "exist", 256, 0, success_cb);

  auto failure_cb = [&] (boost::system::error_code ec, bufferlist bl) {
    EXPECT_EQ(boost::system::errc::no_such_file_or_directory, ec);
  };
  librados::async_read(service, io, "noexist", 256, 0, failure_cb);

  service.run();
}

TEST_F(AsioRados, AsyncReadFuture)
{
  boost::asio::io_context service;

  std::future<bufferlist> f1 = librados::async_read(service, io, "exist", 256,
                                                    0, boost::asio::use_future);
  std::future<bufferlist> f2 = librados::async_read(service, io, "noexist", 256,
                                                    0, boost::asio::use_future);

  service.run();

  EXPECT_NO_THROW({
    auto bl = f1.get();
    EXPECT_EQ("hello", bl.to_str());
  });
  EXPECT_THROW(f2.get(), boost::system::system_error);
}

TEST_F(AsioRados, AsyncReadYield)
{
  boost::asio::io_context service;

  auto success_cr = [&] (spawn::yield_context yield) {
    boost::system::error_code ec;
    auto bl = librados::async_read(service, io, "exist", 256, 0, yield[ec]);
    EXPECT_FALSE(ec);
    EXPECT_EQ("hello", bl.to_str());
  };
  spawn::spawn(service, success_cr);

  auto failure_cr = [&] (spawn::yield_context yield) {
    boost::system::error_code ec;
    auto bl = librados::async_read(service, io, "noexist", 256, 0, yield[ec]);
    EXPECT_EQ(boost::system::errc::no_such_file_or_directory, ec);
  };
  spawn::spawn(service, failure_cr);

  service.run();
}

TEST_F(AsioRados, AsyncWriteCallback)
{
  boost::asio::io_context service;

  bufferlist bl;
  bl.append("hello");

  auto success_cb = [&] (boost::system::error_code ec) {
    EXPECT_FALSE(ec);
  };
  librados::async_write(service, io, "exist", bl, bl.length(), 0,
                        success_cb);

  auto failure_cb = [&] (boost::system::error_code ec) {
    EXPECT_EQ(boost::system::errc::read_only_file_system, ec);
  };
  librados::async_write(service, snapio, "exist", bl, bl.length(), 0,
                        failure_cb);

  service.run();
}

TEST_F(AsioRados, AsyncWriteFuture)
{
  boost::asio::io_context service;

  bufferlist bl;
  bl.append("hello");

  auto f1 = librados::async_write(service, io, "exist", bl, bl.length(), 0,
                                  boost::asio::use_future);
  auto f2 = librados::async_write(service, snapio, "exist", bl, bl.length(), 0,
                                  boost::asio::use_future);

  service.run();

  EXPECT_NO_THROW(f1.get());
  EXPECT_THROW(f2.get(), boost::system::system_error);
}

TEST_F(AsioRados, AsyncWriteYield)
{
  boost::asio::io_context service;

  bufferlist bl;
  bl.append("hello");

  auto success_cr = [&] (spawn::yield_context yield) {
    boost::system::error_code ec;
    librados::async_write(service, io, "exist", bl, bl.length(), 0,
                          yield[ec]);
    EXPECT_FALSE(ec);
    EXPECT_EQ("hello", bl.to_str());
  };
  spawn::spawn(service, success_cr);

  auto failure_cr = [&] (spawn::yield_context yield) {
    boost::system::error_code ec;
    librados::async_write(service, snapio, "exist", bl, bl.length(), 0,
                          yield[ec]);
    EXPECT_EQ(boost::system::errc::read_only_file_system, ec);
  };
  spawn::spawn(service, failure_cr);

  service.run();
}

TEST_F(AsioRados, AsyncReadOperationCallback)
{
  boost::asio::io_context service;
  {
    librados::ObjectReadOperation op;
    op.read(0, 0, nullptr, nullptr);
    auto success_cb = [&] (boost::system::error_code ec, bufferlist bl) {
      EXPECT_FALSE(ec);
      EXPECT_EQ("hello", bl.to_str());
    };
    librados::async_operate(service, io, "exist", &op, 0, success_cb);
  }
  {
    librados::ObjectReadOperation op;
    op.read(0, 0, nullptr, nullptr);
    auto failure_cb = [&] (boost::system::error_code ec, bufferlist bl) {
      EXPECT_EQ(boost::system::errc::no_such_file_or_directory, ec);
    };
    librados::async_operate(service, io, "noexist", &op, 0, failure_cb);
  }
  service.run();
}

TEST_F(AsioRados, AsyncReadOperationFuture)
{
  boost::asio::io_context service;
  std::future<bufferlist> f1;
  {
    librados::ObjectReadOperation op;
    op.read(0, 0, nullptr, nullptr);
    f1 = librados::async_operate(service, io, "exist", &op, 0,
                                 boost::asio::use_future);
  }
  std::future<bufferlist> f2;
  {
    librados::ObjectReadOperation op;
    op.read(0, 0, nullptr, nullptr);
    f2 = librados::async_operate(service, io, "noexist", &op, 0,
                                 boost::asio::use_future);
  }
  service.run();

  EXPECT_NO_THROW({
    auto bl = f1.get();
    EXPECT_EQ("hello", bl.to_str());
  });
  EXPECT_THROW(f2.get(), boost::system::system_error);
}

TEST_F(AsioRados, AsyncReadOperationYield)
{
  boost::asio::io_context service;

  auto success_cr = [&] (spawn::yield_context yield) {
    librados::ObjectReadOperation op;
    op.read(0, 0, nullptr, nullptr);
    boost::system::error_code ec;
    auto bl = librados::async_operate(service, io, "exist", &op, 0,
                                      yield[ec]);
    EXPECT_FALSE(ec);
    EXPECT_EQ("hello", bl.to_str());
  };
  spawn::spawn(service, success_cr);

  auto failure_cr = [&] (spawn::yield_context yield) {
    librados::ObjectReadOperation op;
    op.read(0, 0, nullptr, nullptr);
    boost::system::error_code ec;
    auto bl = librados::async_operate(service, io, "noexist", &op, 0,
                                      yield[ec]);
    EXPECT_EQ(boost::system::errc::no_such_file_or_directory, ec);
  };
  spawn::spawn(service, failure_cr);

  service.run();
}

TEST_F(AsioRados, AsyncWriteOperationCallback)
{
  boost::asio::io_context service;

  bufferlist bl;
  bl.append("hello");

  {
    librados::ObjectWriteOperation op;
    op.write_full(bl);
    auto success_cb = [&] (boost::system::error_code ec) {
      EXPECT_FALSE(ec);
    };
    librados::async_operate(service, io, "exist", &op, 0, success_cb);
  }
  {
    librados::ObjectWriteOperation op;
    op.write_full(bl);
    auto failure_cb = [&] (boost::system::error_code ec) {
      EXPECT_EQ(boost::system::errc::read_only_file_system, ec);
    };
    librados::async_operate(service, snapio, "exist", &op, 0, failure_cb);
  }
  service.run();
}

TEST_F(AsioRados, AsyncWriteOperationFuture)
{
  boost::asio::io_context service;

  bufferlist bl;
  bl.append("hello");

  std::future<void> f1;
  {
    librados::ObjectWriteOperation op;
    op.write_full(bl);
    f1 = librados::async_operate(service, io, "exist", &op, 0,
                                 boost::asio::use_future);
  }
  std::future<void> f2;
  {
    librados::ObjectWriteOperation op;
    op.write_full(bl);
    f2 = librados::async_operate(service, snapio, "exist", &op, 0,
                                 boost::asio::use_future);
  }
  service.run();

  EXPECT_NO_THROW(f1.get());
  EXPECT_THROW(f2.get(), boost::system::system_error);
}

TEST_F(AsioRados, AsyncWriteOperationYield)
{
  boost::asio::io_context service;

  bufferlist bl;
  bl.append("hello");

  auto success_cr = [&] (spawn::yield_context yield) {
    librados::ObjectWriteOperation op;
    op.write_full(bl);
    boost::system::error_code ec;
    librados::async_operate(service, io, "exist", &op, 0, yield[ec]);
    EXPECT_FALSE(ec);
  };
  spawn::spawn(service, success_cr);

  auto failure_cr = [&] (spawn::yield_context yield) {
    librados::ObjectWriteOperation op;
    op.write_full(bl);
    boost::system::error_code ec;
    librados::async_operate(service, snapio, "exist", &op, 0, yield[ec]);
    EXPECT_EQ(boost::system::errc::read_only_file_system, ec);
  };
  spawn::spawn(service, failure_cr);

  service.run();
}

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
