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
#include <string>

#include "include/rados/librados.hpp"
#include "include/encoding.h"
#include "test/librados/test_cxx.h"
#include "gtest/gtest.h"
#include "json_spirit/json_spirit.h"

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

static std::string _get_required_osd_release(Rados& cluster)
{
  bufferlist inbl;
  std::string cmd = std::string("{\"prefix\": \"osd dump\",\"format\":\"json\"}");
  bufferlist outbl;
  int r = cluster.mon_command(cmd, inbl, &outbl, NULL);
  ceph_assert(r >= 0);
  std::string outstr(outbl.c_str(), outbl.length());
  json_spirit::Value v;
  if (!json_spirit::read(outstr, v)) {
    std::cerr <<" unable to parse json " << outstr << std::endl;
    return "";
  }

  json_spirit::Object& o = v.get_obj();
  for (json_spirit::Object::size_type i=0; i<o.size(); i++) {
    json_spirit::Pair& p = o[i];
    if (p.name_ == "require_osd_release") {
      std::cout << "require_osd_release = " << p.value_.get_str() << std::endl;
      return p.value_.get_str();
    }
  }
  std::cerr << "didn't find require_osd_release in " << outstr << std::endl;
  return "";
}

TEST(ClsHello, WriteReturnData) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  // skip test if not yet mimic
  if (_get_required_osd_release(cluster) < "octopus") {
    std::cout << "cluster is not yet octopus, skipping test" << std::endl;
    return;
  }

  // this will return nothing -- no flag is set
  bufferlist in, out;
  ASSERT_EQ(0, ioctx.exec("myobject", "hello", "write_return_data", in, out));
  ASSERT_EQ(std::string(), std::string(out.c_str(), out.length()));

  // this will return an error due to unexpected input.
  // note that we set the RETURNVEC flag here so that we *reliably* get the
  // "too much input data!" return data string.  do it lots of times so we are
  // more likely to resend a request and hit the dup op handling path.
  char buf[4096];
  memset(buf, 1, sizeof(buf));
  for (unsigned i=0; i<1000; ++i) {
    std::cout << i << std::endl;
    in.clear();
    in.append(buf, sizeof(buf));
    int rval;
    ObjectWriteOperation o;
    o.exec("hello", "write_return_data", in, &out, &rval);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate("foo", completion, &o,
				   librados::OPERATION_RETURNVEC));
    completion->wait_for_complete();
    ASSERT_EQ(-EINVAL, completion->get_return_value());
    ASSERT_EQ(-EINVAL, rval);
    ASSERT_EQ(std::string("too much input data!"), std::string(out.c_str(), out.length()));
  }
  ASSERT_EQ(-ENOENT, ioctx.getxattr("myobject2", "foo", out));

  // this *will* return data due to the RETURNVEC flag
  // using a-sync call
  {
    in.clear();
    out.clear();
    int rval;
    ObjectWriteOperation o;
    o.exec("hello", "write_return_data", in, &out, &rval);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate("foo", completion, &o,
				 librados::OPERATION_RETURNVEC));
    completion->wait_for_complete();
    ASSERT_EQ(42, completion->get_return_value());
    ASSERT_EQ(42, rval);
    out.hexdump(std::cout);
    ASSERT_EQ("you might see this", std::string(out.c_str(), out.length()));
  }
  // using sync call
  {
    in.clear();
    out.clear();
    int rval;
    ObjectWriteOperation o;
    o.exec("hello", "write_return_data", in, &out, &rval);
    ASSERT_EQ(42, ioctx.operate("foo", &o,
				 librados::OPERATION_RETURNVEC));
    ASSERT_EQ(42, rval);
    out.hexdump(std::cout);
    ASSERT_EQ("you might see this", std::string(out.c_str(), out.length()));
  }

  // this will overflow because the return data is too big
  {
    in.clear();
    out.clear();
    int rval;
    ObjectWriteOperation o;
    o.exec("hello", "write_too_much_return_data", in, &out, &rval);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate("foo", completion, &o,
				   librados::OPERATION_RETURNVEC));
    completion->wait_for_complete();
    ASSERT_EQ(-EOVERFLOW, completion->get_return_value());
    ASSERT_EQ(-EOVERFLOW, rval);
    ASSERT_EQ("", std::string(out.c_str(), out.length()));
  }

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

TEST(ClsHello, Filter) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist obj_content;
  obj_content.append(buf, sizeof(buf));

  std::string target_str = "content";

  // Write xattr bare, no ::encod'ing
  bufferlist target_val;
  target_val.append(target_str);
  bufferlist nontarget_val;
  nontarget_val.append("rhubarb");

  ASSERT_EQ(0, ioctx.write("has_xattr", obj_content, obj_content.length(), 0));
  ASSERT_EQ(0, ioctx.write("has_wrong_xattr", obj_content, obj_content.length(), 0));
  ASSERT_EQ(0, ioctx.write("no_xattr", obj_content, obj_content.length(), 0));

  ASSERT_EQ(0, ioctx.setxattr("has_xattr", "theattr", target_val));
  ASSERT_EQ(0, ioctx.setxattr("has_wrong_xattr", "theattr", nontarget_val));

  bufferlist filter_bl;
  std::string filter_name = "hello.hello";
  encode(filter_name, filter_bl);
  encode("_theattr", filter_bl);
  encode(target_str, filter_bl);

  NObjectIterator iter(ioctx.nobjects_begin(filter_bl));
  bool foundit = false;
  int k = 0;
  while (iter != ioctx.nobjects_end()) {
    foundit = true;
    // We should only see the object that matches the filter
    ASSERT_EQ((*iter).get_oid(), "has_xattr");
    // We should only see it once
    ASSERT_EQ(k, 0);
    ++iter;
    ++k;
  }
  ASSERT_TRUE(foundit);

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

