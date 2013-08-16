// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <iostream>
#include <errno.h>

#include "include/rados/librados.hpp"
#include "test/librados/test.h"
#include "gtest/gtest.h"

using namespace librados;

TEST(ClsHello, SayHello) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist in, out;
  ASSERT_EQ(-ENOENT, ioctx.exec("myobject", "hello", "say_hello", in, out));
  ASSERT_EQ(0, ioctx.write_full("myobject", in));
  ASSERT_EQ(0, ioctx.exec("myobject", "hello", "say_hello", in, out));
  ASSERT_EQ(std::string("Hello, world!"), std::string(out.c_str(), out.length()));

  out.clear();
  in.append("Tester");
  ASSERT_EQ(0, ioctx.exec("myobject", "hello", "say_hello", in, out));
  ASSERT_EQ(std::string("Hello, Tester!"), std::string(out.c_str(), out.length()));

  out.clear();
  in.clear();
  char buf[4096];
  memset(buf, 1, sizeof(buf));
  in.append(buf, sizeof(buf));
  ASSERT_EQ(-EINVAL, ioctx.exec("myobject", "hello", "say_hello", in, out));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsHello, RecordHello) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist in, out;
  ASSERT_EQ(0, ioctx.exec("myobject", "hello", "record_hello", in, out));
  ASSERT_EQ(-EEXIST, ioctx.exec("myobject", "hello", "record_hello", in, out));

  in.append("Tester");
  ASSERT_EQ(0, ioctx.exec("myobject2", "hello", "record_hello", in, out));
  ASSERT_EQ(-EEXIST, ioctx.exec("myobject2", "hello", "record_hello", in, out));
  ASSERT_EQ(0u, out.length());

  in.clear();
  out.clear();
  ASSERT_EQ(0, ioctx.exec("myobject", "hello", "replay", in, out));
  ASSERT_EQ(std::string("Hello, world!"), std::string(out.c_str(), out.length()));
  out.clear();
  ASSERT_EQ(0, ioctx.exec("myobject2", "hello", "replay", in, out));
  ASSERT_EQ(std::string("Hello, Tester!"), std::string(out.c_str(), out.length()));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsHello, WriteReturnData) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist in, out;
  ASSERT_EQ(0, ioctx.exec("myobject", "hello", "writes_dont_return_data", in, out));
  ASSERT_EQ(std::string(), std::string(out.c_str(), out.length()));

  char buf[4096];
  memset(buf, 1, sizeof(buf));
  in.append(buf, sizeof(buf));
  ASSERT_EQ(-EINVAL, ioctx.exec("myobject2", "hello", "writes_dont_return_data", in, out));
  ASSERT_EQ(std::string("too much input data!"), std::string(out.c_str(), out.length()));
  ASSERT_EQ(-ENOENT, ioctx.getxattr("myobject2", "foo", out));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsHello, Loud) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist in, out;
  ASSERT_EQ(0, ioctx.exec("myobject", "hello", "record_hello", in, out));
  ASSERT_EQ(0, ioctx.exec("myobject", "hello", "replay", in, out));
  ASSERT_EQ(std::string("Hello, world!"), std::string(out.c_str(), out.length()));

  ASSERT_EQ(0, ioctx.exec("myobject", "hello", "turn_it_to_11", in, out));
  ASSERT_EQ(0, ioctx.exec("myobject", "hello", "replay", in, out));
  ASSERT_EQ(std::string("HELLO, WORLD!"), std::string(out.c_str(), out.length()));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsHello, BadMethods) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist in, out;

  ASSERT_EQ(0, ioctx.write_full("myobject", in));
  ASSERT_EQ(-EIO, ioctx.exec("myobject", "hello", "bad_reader", in, out));
  ASSERT_EQ(-EIO, ioctx.exec("myobject", "hello", "bad_writer", in, out));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}
