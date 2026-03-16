/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 CERN
 *
 * Author: Joaquim Rocha <joaquim.rocha@cern.ch>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include <iostream>
#include <errno.h>
#include <set>
#include <sstream>
#include <string>

#include "cls/numops/cls_numops_client.h"
#include "gtest/gtest.h"
#include "include/rados/librados.hpp"
#include "test/librados/test_cxx.h"
#include "test/librados/test_pool_types.h"

using namespace librados;
using ceph::test::PoolType;
using ceph::test::pool_type_name;
using ceph::test::create_pool_by_type;
using ceph::test::destroy_pool_by_type;

class TestClsNumOps : public ceph::test::ClsTestFixture {
  // Inherits: rados, ioctx, pool_name, pool_type, SetUp(), TearDown()
};

TEST_P(TestClsNumOps, Add) {
  // exec numops add method with an empty bufferlist

  bufferlist in, out;

  ASSERT_EQ(-EINVAL, ioctx.exec("myobject", "numops", "add", in, out));

  // add a number to a non-existing key

  std::string key = "my-key";
  double value_in = 0.5;

  std::stringstream stream;
  stream << value_in;

  ASSERT_EQ(0, rados::cls::numops::add(&ioctx, "myobject", key, value_in));

  // check that the omap entry was set and the value matches

  std::set<std::string> keys;
  std::map<std::string, bufferlist> omap;
  keys.insert(key);

  ASSERT_EQ(0, ioctx.omap_get_vals_by_keys("myobject", keys, &omap));

  std::map<std::string, bufferlist>::iterator it = omap.find(key);

  ASSERT_NE(omap.end(), it);

  bufferlist bl = (*it).second;
  std::string value_out(bl.c_str(), bl.length());

  EXPECT_EQ(stream.str(), value_out);

  // add another value to the existing one

  double new_value_in = 3.001;

  ASSERT_EQ(0, rados::cls::numops::add(&ioctx, "myobject", key, new_value_in));

  // check that the omap entry's value matches

  omap.clear();

  ASSERT_EQ(0, ioctx.omap_get_vals_by_keys("myobject", keys, &omap));

  it = omap.find(key);

  ASSERT_NE(omap.end(), it);

  bl = (*it).second;
  value_out.assign(bl.c_str(), bl.length());

  stream.str("");
  stream << (value_in + new_value_in);

  EXPECT_EQ(stream.str(), value_out);

  // set the omap entry with some non-numeric value

  omap.clear();

  std::string non_numeric_value("some-non-numeric-text");
  omap[key].append(non_numeric_value);

  ASSERT_EQ(0, ioctx.omap_set("myobject", omap));

  // check that adding a number does not succeed

  omap.clear();

  ASSERT_EQ(-EBADMSG, rados::cls::numops::add(&ioctx, "myobject", key, 2.0));

  // check that the omap entry was not changed

  ASSERT_EQ(0, ioctx.omap_get_vals_by_keys("myobject", keys, &omap));

  it = omap.find(key);

  ASSERT_NE(omap.end(), it);

  bl = (*it).second;
  value_out.assign(bl.c_str(), bl.length());

  EXPECT_EQ(non_numeric_value, value_out);
}

TEST_P(TestClsNumOps, Sub) {
  // subtract a number from a non-existing key

  std::string key = "my-key";
  double value_in = 0.5;

  std::stringstream stream;
  stream << value_in;

  ASSERT_EQ(0, rados::cls::numops::sub(&ioctx, "myobject", key, value_in));

  // check that the omap entry was set and the value matches

  std::set<std::string> keys;
  std::map<std::string, bufferlist> omap;
  keys.insert(key);

  ASSERT_EQ(0, ioctx.omap_get_vals_by_keys("myobject", keys, &omap));

  std::map<std::string, bufferlist>::iterator it = omap.find(key);

  ASSERT_NE(omap.end(), it);

  bufferlist bl = (*it).second;
  std::string value_out(bl.c_str(), bl.length());

  EXPECT_EQ("-" + stream.str(), value_out);

  // subtract another value to the existing one

  double new_value_in = 3.001;

  ASSERT_EQ(0, rados::cls::numops::sub(&ioctx, "myobject", key, new_value_in));

  // check that the omap entry's value matches

  omap.clear();

  ASSERT_EQ(0, ioctx.omap_get_vals_by_keys("myobject", keys, &omap));

  it = omap.find(key);

  ASSERT_NE(omap.end(), it);

  bl = (*it).second;
  value_out.assign(bl.c_str(), bl.length());

  stream.str("");
  stream << -(value_in + new_value_in);

  EXPECT_EQ(stream.str(), value_out);

  // set the omap entry with some non-numeric value

  omap.clear();

  std::string non_numeric_value("some-non-numeric-text");
  omap[key].append(non_numeric_value);

  ASSERT_EQ(0, ioctx.omap_set("myobject", omap));

  // check that subtracting a number does not succeed

  omap.clear();

  ASSERT_EQ(-EBADMSG, rados::cls::numops::sub(&ioctx, "myobject", key, 2.0));

  // check that the omap entry was not changed

  ASSERT_EQ(0, ioctx.omap_get_vals_by_keys("myobject", keys, &omap));

  it = omap.find(key);

  ASSERT_NE(omap.end(), it);

  bl = (*it).second;
  value_out.assign(bl.c_str(), bl.length());

  EXPECT_EQ(non_numeric_value, value_out);
}

TEST_P(TestClsNumOps, Mul) {
  // exec numops mul method with an empty bufferlist

  bufferlist in, out;

  ASSERT_EQ(-EINVAL, ioctx.exec("myobject", "numops", "mul", in, out));

  // multiply a number to a non-existing key

  std::string key = "my-key";
  double value_in = 0.5;

  std::stringstream stream;
  stream << value_in;

  ASSERT_EQ(0, rados::cls::numops::mul(&ioctx, "myobject", key, value_in));

  // check that the omap entry was set and the value is zero

  std::set<std::string> keys;
  std::map<std::string, bufferlist> omap;
  keys.insert(key);

  ASSERT_EQ(0, ioctx.omap_get_vals_by_keys("myobject", keys, &omap));

  std::map<std::string, bufferlist>::iterator it = omap.find(key);

  ASSERT_NE(omap.end(), it);

  bufferlist bl = (*it).second;
  std::string value_out(bl.c_str(), bl.length());

  EXPECT_EQ("0", value_out);

  // set a non-zero value so we can effectively test multiplications

  omap.clear();

  omap[key].append(stream.str());

  ASSERT_EQ(0, ioctx.omap_set("myobject", omap));

  // multiply another value to the existing one

  double new_value_in = 3.001;

  ASSERT_EQ(0, rados::cls::numops::mul(&ioctx, "myobject", key, new_value_in));

  // check that the omap entry's value matches

  omap.clear();

  ASSERT_EQ(0, ioctx.omap_get_vals_by_keys("myobject", keys, &omap));

  it = omap.find(key);

  ASSERT_NE(omap.end(), it);

  bl = (*it).second;
  value_out.assign(bl.c_str(), bl.length());

  stream.str("");
  stream << (value_in * new_value_in);

  EXPECT_EQ(stream.str(), value_out);

  // set the omap entry with some non-numeric value

  omap.clear();

  std::string non_numeric_value("some-non-numeric-text");
  omap[key].append(non_numeric_value);

  ASSERT_EQ(0, ioctx.omap_set("myobject", omap));

  // check that adding a number does not succeed

  ASSERT_EQ(-EBADMSG, rados::cls::numops::mul(&ioctx, "myobject", key, 2.0));

  // check that the omap entry was not changed

  omap.clear();

  ASSERT_EQ(0, ioctx.omap_get_vals_by_keys("myobject", keys, &omap));

  it = omap.find(key);

  ASSERT_NE(omap.end(), it);

  bl = (*it).second;
  value_out.assign(bl.c_str(), bl.length());

  EXPECT_EQ(non_numeric_value, value_out);
}

TEST_P(TestClsNumOps, Div) {
  // divide a non-existing key by a number

  std::string key = "my-key";
  double value_in = 0.5;

  std::stringstream stream;
  stream << value_in;

  ASSERT_EQ(0, rados::cls::numops::div(&ioctx, "myobject", key, value_in));

  // check that the omap entry was set and the value is zero

  std::set<std::string> keys;
  std::map<std::string, bufferlist> omap;
  keys.insert(key);

  ASSERT_EQ(0, ioctx.omap_get_vals_by_keys("myobject", keys, &omap));

  std::map<std::string, bufferlist>::iterator it = omap.find(key);

  ASSERT_NE(omap.end(), it);

  bufferlist bl = (*it).second;
  std::string value_out(bl.c_str(), bl.length());

  EXPECT_EQ("0", value_out);

  // check that division by zero is not allowed

  ASSERT_EQ(-EINVAL, rados::cls::numops::div(&ioctx, "myobject", key, 0));

  // set a non-zero value so we can effectively test divisions

  omap.clear();

  omap[key].append(stream.str());

  ASSERT_EQ(0, ioctx.omap_set("myobject", omap));

  // divide another value to the existing one

  double new_value_in = 3.001;

  ASSERT_EQ(0, rados::cls::numops::div(&ioctx, "myobject", key, new_value_in));

  // check that the omap entry's value matches

  omap.clear();

  ASSERT_EQ(0, ioctx.omap_get_vals_by_keys("myobject", keys, &omap));

  it = omap.find(key);

  ASSERT_NE(omap.end(), it);

  bl = (*it).second;
  value_out.assign(bl.c_str(), bl.length());

  stream.str("");
  stream << (value_in / new_value_in);

  EXPECT_EQ(stream.str(), value_out);

  omap.clear();

  // set the omap entry with some non-numeric value

  std::string non_numeric_value("some-non-numeric-text");
  omap[key].append(non_numeric_value);

  ASSERT_EQ(0, ioctx.omap_set("myobject", omap));

  // check that adding a number does not succeed

  ASSERT_EQ(-EBADMSG, rados::cls::numops::div(&ioctx, "myobject", key, 2.0));

  // check that the omap entry was not changed

  omap.clear();

  ASSERT_EQ(0, ioctx.omap_get_vals_by_keys("myobject", keys, &omap));

  it = omap.find(key);

  ASSERT_NE(omap.end(), it);

  bl = (*it).second;
  value_out.assign(bl.c_str(), bl.length());

  EXPECT_EQ(non_numeric_value, value_out);
}

INSTANTIATE_TEST_SUITE_P(, TestClsNumOps,
    ::testing::Values(PoolType::REPLICATED, PoolType::FAST_EC),
    [](const ::testing::TestParamInfo<PoolType>& info) {
      return pool_type_name(info.param);
    }
);
