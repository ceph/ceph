// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "cls/cmpxattr/client.h"
#include "test/librados/test_cxx.h"
#include "gtest/gtest.h"

#include <optional>

// create/destroy a pool that's shared by all tests in the process
struct RadosEnv : public ::testing::Environment {
  static std::optional<std::string> pool_name;
public:
  static librados::Rados rados;
  static librados::IoCtx ioctx;

  void SetUp() override {
    // create pool
    std::string name = get_temp_pool_name();
    ASSERT_EQ("", create_one_pool_pp(name, rados));
    pool_name = name;
    ASSERT_EQ(rados.ioctx_create(name.c_str(), ioctx), 0);
  }
  void TearDown() override {
    ioctx.close();
    if (pool_name) {
      ASSERT_EQ(destroy_one_pool_pp(*pool_name, rados), 0);
    }
  }
};
std::optional<std::string> RadosEnv::pool_name;
librados::Rados RadosEnv::rados;
librados::IoCtx RadosEnv::ioctx;

auto *const rados_env = ::testing::AddGlobalTestEnvironment(new RadosEnv);

namespace cls::cmpxattr {

  // test fixture with helper functions
  class CmpXattr : public ::testing::Test {
  protected:
    librados::IoCtx& ioctx = RadosEnv::ioctx;

    //---------------------------------------------------------------------------
    int do_cmp_vals_set_vals(const std::string& oid,
			     Mode mode,
			     Op comparison,
			     const ComparisonMap& cmp_pairs,
			     const std::map<std::string, bufferlist>& set_pairs)
    {
      librados::ObjectWriteOperation op;
      int ret = cmp_vals_set_vals(op, mode, comparison, cmp_pairs, set_pairs);
      if (ret < 0) {
	return ret;
      }
      return ioctx.operate(oid, &op);
    }

  };

  //---------------------------------------------------------------------------
  TEST_F(CmpXattr, cmp_set_vals_str)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    const bufferlist val1 = string_buffer("bbb");
    const bufferlist val2 = string_buffer("ccc");
    {
      librados::ObjectWriteOperation op;
      op.setxattr("eq",  val1);
      op.setxattr("ne",  val1);
      op.setxattr("gt",  val1);
      op.setxattr("gte", val1);
      op.setxattr("lt",  val1);
      op.setxattr("lte", val1);
      ASSERT_EQ(0, ioctx.operate(oid, &op));
    }

    Mode mode = Mode::String;
    do_cmp_vals_set_vals(oid, mode, Op::EQ,  {{"eq", val2}},  {{"eq", val2}});
    do_cmp_vals_set_vals(oid, mode, Op::NE,  {{"ne", val2}},  {{"ne", val2}});
    do_cmp_vals_set_vals(oid, mode, Op::GT,  {{"gt", val2}},  {{"gt", val2}});
    do_cmp_vals_set_vals(oid, mode, Op::GTE, {{"gte", val2}}, {{"gte", val2}});
    do_cmp_vals_set_vals(oid, mode, Op::LT,  {{"lt", val2}},  {{"lt", val2}});
    do_cmp_vals_set_vals(oid, mode, Op::LTE, {{"lte", val2}}, {{"lte", val2}});

    {
      std::map<std::string, bufferlist> vals;
      ASSERT_EQ(0, ioctx.getxattrs(oid, vals));
      ASSERT_EQ(vals.size(), 6);
      EXPECT_EQ(val1, vals["eq"]);
      EXPECT_EQ(val2, vals["ne"]);
      EXPECT_EQ(val2, vals["gt"]);
      EXPECT_EQ(val2, vals["gte"]);
      EXPECT_EQ(val1, vals["lt"]);
      EXPECT_EQ(val1, vals["lte"]);
    }
  }

  //---------------------------------------------------------------------------
  TEST_F(CmpXattr, cmp_set_vals_u64)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    const bufferlist val1 = u64_buffer(17);
    const bufferlist val2 = u64_buffer(41);
    {
      librados::ObjectWriteOperation op;
      op.setxattr("eq",  val1);
      op.setxattr("ne",  val1);
      op.setxattr("gt",  val1);
      op.setxattr("gte", val1);
      op.setxattr("lt",  val1);
      op.setxattr("lte", val1);
      ASSERT_EQ(0, ioctx.operate(oid, &op));
    }

    Mode mode = Mode::U64;
    do_cmp_vals_set_vals(oid, mode, Op::EQ,  {{"eq", val2}},  {{"eq", val2}});
    do_cmp_vals_set_vals(oid, mode, Op::NE,  {{"ne", val2}},  {{"ne", val2}});
    do_cmp_vals_set_vals(oid, mode, Op::GT,  {{"gt", val2}},  {{"gt", val2}});
    do_cmp_vals_set_vals(oid, mode, Op::GTE, {{"gte", val2}}, {{"gte", val2}});
    do_cmp_vals_set_vals(oid, mode, Op::LT,  {{"lt", val2}},  {{"lt", val2}});
    do_cmp_vals_set_vals(oid, mode, Op::LTE, {{"lte", val2}}, {{"lte", val2}});

    {
      std::map<std::string, bufferlist> vals;
      ASSERT_EQ(0, ioctx.getxattrs(oid, vals));
      ASSERT_EQ(vals.size(), 6);
      EXPECT_EQ(val1, vals["eq"]);
      EXPECT_EQ(val2, vals["ne"]);
      EXPECT_EQ(val2, vals["gt"]);
      EXPECT_EQ(val2, vals["gte"]);
      EXPECT_EQ(val1, vals["lt"]);
      EXPECT_EQ(val1, vals["lte"]);
    }
  }

  //---------------------------------------------------------------------------
  TEST_F(CmpXattr, cmp_set_vals_u64_single_set)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    ComparisonMap cmp_pairs;
    std::map<std::string, bufferlist> set_pairs;
    librados::ObjectWriteOperation op;
    for (uint32_t i = 0; i < max_keys; i++) {
      std::string key = std::to_string(i);
      const bufferlist val1 = u64_buffer(i);
      const bufferlist val2 = u64_buffer(i*2);
      cmp_pairs.emplace(key, val1);
      set_pairs.emplace(key, val2);
      op.setxattr(key.c_str(), val1);
    }
    ASSERT_EQ(0, ioctx.operate(oid, &op));
    {
      std::map<std::string, bufferlist> vals;
      ASSERT_EQ(0, ioctx.getxattrs(oid, vals));
      ASSERT_EQ(vals.size(), cmp_pairs.size());
      for (uint32_t i = 0; i < cmp_pairs.size(); i++) {
	std::string key = std::to_string(i);
	EXPECT_EQ(cmp_pairs[key], vals[key]);
      }
    }

    ASSERT_EQ(0, do_cmp_vals_set_vals(oid, Mode::U64, Op::EQ, cmp_pairs, set_pairs));
    {
      std::map<std::string, bufferlist> vals;
      ASSERT_EQ(0, ioctx.getxattrs(oid, vals));
      ASSERT_EQ(vals.size(), cmp_pairs.size());
      for (uint32_t i = 0; i < set_pairs.size(); i++) {
	std::string key = std::to_string(i);
	EXPECT_EQ(set_pairs[key], vals[key]);
      }
    }
  }

  //---------------------------------------------------------------------------
  TEST_F(CmpXattr, cmp_set_vals_u64_two_sets)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    ComparisonMap cmp_pairs;
    std::map<std::string, bufferlist> set_pairs;
    librados::ObjectWriteOperation op;
    for (uint32_t i = 0; i < max_keys; i++) {
      std::string key1 = std::to_string(i);
      std::string key2 = std::to_string(i+max_keys);
      const bufferlist val1 = u64_buffer(i);
      const bufferlist val2 = u64_buffer(i*5);
      cmp_pairs.emplace(key1, val1);
      set_pairs.emplace(key2, val2);
      op.setxattr(key1.c_str(), val1);
    }
    ASSERT_EQ(0, ioctx.operate(oid, &op));
    {
      std::map<std::string, bufferlist> vals;
      ASSERT_EQ(0, ioctx.getxattrs(oid, vals));
      ASSERT_EQ(vals.size(), cmp_pairs.size());
      for (uint32_t i = 0; i < cmp_pairs.size(); i++) {
	std::string key = std::to_string(i);
	EXPECT_EQ(cmp_pairs[key], vals[key]);
      }
    }

    ASSERT_EQ(0, do_cmp_vals_set_vals(oid, Mode::U64, Op::EQ, cmp_pairs, set_pairs));
    {
      std::map<std::string, bufferlist> vals;
      ASSERT_EQ(0, ioctx.getxattrs(oid, vals));
      ASSERT_EQ(vals.size(), cmp_pairs.size() + set_pairs.size());
      for (uint32_t i = 0; i < set_pairs.size(); i++) {
	std::string key1 = std::to_string(i);
	std::string key2 = std::to_string(i+max_keys);
	EXPECT_EQ(cmp_pairs[key1], vals[key1]);
	EXPECT_EQ(set_pairs[key2], vals[key2]);
      }
    }
  }

  //---------------------------------------------------------------------------
  TEST_F(CmpXattr, cmp_set_vals_str_einval)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    const std::string key = "key";
    bufferlist val1 = string_buffer("ccc");
    bufferlist val2 = u64_buffer(17);

    ASSERT_EQ(ioctx.setxattr(oid, key.c_str(), val1), 0);
    ASSERT_EQ(do_cmp_vals_set_vals(oid, Mode::String, Op::EQ, {{key, val2}},
				   {{key, val2}}), -1);

    {
      std::map<std::string, bufferlist> vals;
      ASSERT_EQ(0, ioctx.getxattrs(oid, vals));
      ASSERT_EQ(vals.size(), 1);
      EXPECT_EQ(val1, vals[key]);
    }
  }

  //---------------------------------------------------------------------------
  TEST_F(CmpXattr, cmp_set_vals_str_eio)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    const std::string key = "key";
    bufferlist val1 = u64_buffer(17);
    bufferlist val2 = string_buffer("ccc");

    ASSERT_EQ(ioctx.setxattr(oid, key.c_str(), val1), 0);
    ASSERT_EQ(do_cmp_vals_set_vals(oid, Mode::String, Op::EQ, {{key, val2}},
				   {{key, val2}}), -1);

    {
      std::map<std::string, bufferlist> vals;
      ASSERT_EQ(0, ioctx.getxattrs(oid, vals));
      ASSERT_EQ(vals.size(), 1);
      EXPECT_EQ(val1, vals[key]);
    }
  }

  //---------------------------------------------------------------------------
  TEST_F(CmpXattr, cmp_set_vals_u64_einval)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    const std::string key = "key";
    bufferlist val1 = u64_buffer(17);
    bufferlist val2 = string_buffer("ccc");

    ASSERT_EQ(ioctx.setxattr(oid, key.c_str(), val1), 0);
    ASSERT_EQ(do_cmp_vals_set_vals(oid, Mode::U64, Op::EQ, {{key, val2}},
				   {{key, val2}}), -EINVAL);
  }

  //---------------------------------------------------------------------------
  TEST_F(CmpXattr, cmp_set_vals_u64_eio)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    const std::string key = "key";
    bufferlist val1 = string_buffer("ccc");
    bufferlist val2 = u64_buffer(17);

    ASSERT_EQ(ioctx.setxattr(oid, key.c_str(), val1), 0);
    ASSERT_EQ(do_cmp_vals_set_vals(oid, Mode::U64, Op::EQ, {{key, val2}},
				   {{key, val2}}), -EIO);

    {
      std::map<std::string, bufferlist> vals;
      ASSERT_EQ(0, ioctx.getxattrs(oid, vals));
      ASSERT_EQ(vals.size(), 1);
      EXPECT_EQ(val1, vals[key]);
    }
  }

  //---------------------------------------------------------------------------
  TEST_F(CmpXattr, cmp_set_vals_at_max_keys)
  {
    ComparisonMap cmp_pairs;
    std::map<std::string, bufferlist> set_pairs;
    const bufferlist value = u64_buffer(0);
    for (uint32_t i = 0; i < max_keys; i++) {
      cmp_pairs.emplace(std::to_string(i), value);
      set_pairs.emplace(std::to_string(i), value);
    }

    librados::ObjectWriteOperation op;
    EXPECT_EQ(cmp_vals_set_vals(op, Mode::U64, Op::EQ, cmp_pairs, set_pairs), 0);
  }

  //---------------------------------------------------------------------------
  TEST_F(CmpXattr, cmp_set_vals_over_max_keys)
  {
    ComparisonMap cmp_pairs;
    std::map<std::string, bufferlist> set_pairs;
    const bufferlist value = u64_buffer(0);
    for (uint32_t i = 0; i < max_keys + 1; i++) {
      cmp_pairs.emplace(std::to_string(i), value);
      set_pairs.emplace(std::to_string(i), value);
    }

    librados::ObjectWriteOperation op;
    EXPECT_EQ(cmp_vals_set_vals(op, Mode::U64, Op::EQ, cmp_pairs, set_pairs), -E2BIG);
  }

} // namespace cls::cmpxattr
