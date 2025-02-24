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
#if 0
    const std::string oid = __PRETTY_FUNCTION__;
    const bufferlist value1 = string_buffer("bbb");
    const bufferlist value2 = string_buffer("ccc");
    {
      std::map<std::string, bufferlist> vals = {
	{"eq", value1},
	{"ne", value1},
	{"gt", value1},
	{"gte", value1},
	{"lt", value1},
	{"lte", value1},
      };
      ASSERT_EQ(ioctx.omap_set(oid, vals), 0);
    }

    ASSERT_EQ(do_cmp_set_vals(oid, Mode::String, Op::EQ, {{"eq", value2}}), 0);
    ASSERT_EQ(do_cmp_set_vals(oid, Mode::String, Op::NE, {{"ne", value2}}), 0);
    ASSERT_EQ(do_cmp_set_vals(oid, Mode::String, Op::GT, {{"gt", value2}}), 0);
    ASSERT_EQ(do_cmp_set_vals(oid, Mode::String, Op::GTE, {{"gte", value2}}), 0);
    ASSERT_EQ(do_cmp_set_vals(oid, Mode::String, Op::LT, {{"lt", value2}}), 0);
    ASSERT_EQ(do_cmp_set_vals(oid, Mode::String, Op::LTE, {{"lte", value2}}), 0);

    {
      std::map<std::string, bufferlist> vals;
      ASSERT_EQ(get_vals(oid, &vals), 0);
      ASSERT_EQ(vals.size(), 6);
      EXPECT_EQ(value1, vals["eq"]);
      EXPECT_EQ(value2, vals["ne"]);
      EXPECT_EQ(value2, vals["gt"]);
      EXPECT_EQ(value2, vals["gte"]);
      EXPECT_EQ(value1, vals["lt"]);
      EXPECT_EQ(value1, vals["lte"]);
    }
#endif
  }

#if 0
  //=======================================================================
  TEST_F(CmpXattr, cmp_set_vals_noexist_str)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    const bufferlist value = string_buffer("bbb");

    EXPECT_EQ(do_cmp_set_vals(oid, Mode::String, Op::EQ, {{"eq", value}}), 0);
    EXPECT_EQ(do_cmp_set_vals(oid, Mode::String, Op::NE, {{"ne", value}}), 0);
    EXPECT_EQ(do_cmp_set_vals(oid, Mode::String, Op::GT, {{"gt", value}}), 0);
    EXPECT_EQ(do_cmp_set_vals(oid, Mode::String, Op::GTE, {{"gte", value}}), 0);
    EXPECT_EQ(do_cmp_set_vals(oid, Mode::String, Op::LT, {{"lt", value}}), 0);
    EXPECT_EQ(do_cmp_set_vals(oid, Mode::String, Op::LTE, {{"lte", value}}), 0);

    std::map<std::string, bufferlist> vals;
    EXPECT_EQ(get_vals(oid, &vals), -ENOENT); // never got created
  }

  TEST_F(CmpXattr, cmp_set_vals_noexist_str_default)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    const bufferlist value = string_buffer("bbb");
    const bufferlist def;

    EXPECT_EQ(do_cmp_set_vals(oid, Mode::String, Op::EQ, {{"eq", value}}, def), 0);
    EXPECT_EQ(do_cmp_set_vals(oid, Mode::String, Op::NE, {{"ne", value}}, def), 0);
    EXPECT_EQ(do_cmp_set_vals(oid, Mode::String, Op::GT, {{"gt", value}}, def), 0);
    EXPECT_EQ(do_cmp_set_vals(oid, Mode::String, Op::GTE, {{"gte", value}}, def), 0);
    EXPECT_EQ(do_cmp_set_vals(oid, Mode::String, Op::LT, {{"lt", value}}, def), 0);
    EXPECT_EQ(do_cmp_set_vals(oid, Mode::String, Op::LTE, {{"lte", value}}, def), 0);

    std::map<std::string, bufferlist> vals;
    ASSERT_EQ(get_vals(oid, &vals), 0);
    EXPECT_EQ(vals.count("eq"), 0);
    EXPECT_EQ(vals.count("ne"), 1);
    EXPECT_EQ(vals.count("gt"), 1);
    EXPECT_EQ(vals.count("gte"), 1);
    EXPECT_EQ(vals.count("lt"), 0);
    EXPECT_EQ(vals.count("lte"), 0);
  }

  TEST_F(CmpXattr, cmp_set_vals_noexist_u64)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    const bufferlist value = u64_buffer(0);

    EXPECT_EQ(do_cmp_set_vals(oid, Mode::U64, Op::EQ, {{"eq", value}}), 0);
    EXPECT_EQ(do_cmp_set_vals(oid, Mode::U64, Op::NE, {{"ne", value}}), 0);
    EXPECT_EQ(do_cmp_set_vals(oid, Mode::U64, Op::GT, {{"gt", value}}), 0);
    EXPECT_EQ(do_cmp_set_vals(oid, Mode::U64, Op::GTE, {{"gte", value}}), 0);
    EXPECT_EQ(do_cmp_set_vals(oid, Mode::U64, Op::LT, {{"lt", value}}), 0);
    EXPECT_EQ(do_cmp_set_vals(oid, Mode::U64, Op::LTE, {{"lte", value}}), 0);

    std::map<std::string, bufferlist> vals;
    ASSERT_EQ(get_vals(oid, &vals), -ENOENT);
  }

  TEST_F(CmpXattr, cmp_set_vals_noexist_u64_default)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    const bufferlist value = u64_buffer(0);
    const bufferlist def = u64_buffer(0);

    EXPECT_EQ(do_cmp_set_vals(oid, Mode::U64, Op::EQ, {{"eq", value}}, def), 0);
    EXPECT_EQ(do_cmp_set_vals(oid, Mode::U64, Op::NE, {{"ne", value}}, def), 0);
    EXPECT_EQ(do_cmp_set_vals(oid, Mode::U64, Op::GT, {{"gt", value}}, def), 0);
    EXPECT_EQ(do_cmp_set_vals(oid, Mode::U64, Op::GTE, {{"gte", value}}, def), 0);
    EXPECT_EQ(do_cmp_set_vals(oid, Mode::U64, Op::LT, {{"lt", value}}, def), 0);
    EXPECT_EQ(do_cmp_set_vals(oid, Mode::U64, Op::LTE, {{"lte", value}}, def), 0);

    std::map<std::string, bufferlist> vals;
    ASSERT_EQ(get_vals(oid, &vals), 0);
    EXPECT_EQ(vals.count("eq"), 1);
    EXPECT_EQ(vals.count("ne"), 0);
    EXPECT_EQ(vals.count("gt"), 0);
    EXPECT_EQ(vals.count("gte"), 1);
    EXPECT_EQ(vals.count("lt"), 0);
    EXPECT_EQ(vals.count("lte"), 1);
  }

  TEST_F(CmpXattr, cmp_set_vals_u64)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    const bufferlist value1 = u64_buffer(0);
    const bufferlist value2 = u64_buffer(42);
    {
      std::map<std::string, bufferlist> vals = {
	{"eq", value1},
	{"ne", value1},
	{"gt", value1},
	{"gte", value1},
	{"lt", value1},
	{"lte", value1},
      };
      ASSERT_EQ(ioctx.omap_set(oid, vals), 0);
    }

    ASSERT_EQ(do_cmp_set_vals(oid, Mode::U64, Op::EQ, {{"eq", value2}}), 0);
    ASSERT_EQ(do_cmp_set_vals(oid, Mode::U64, Op::NE, {{"ne", value2}}), 0);
    ASSERT_EQ(do_cmp_set_vals(oid, Mode::U64, Op::GT, {{"gt", value2}}), 0);
    ASSERT_EQ(do_cmp_set_vals(oid, Mode::U64, Op::GTE, {{"gte", value2}}), 0);
    ASSERT_EQ(do_cmp_set_vals(oid, Mode::U64, Op::LT, {{"lt", value2}}), 0);
    ASSERT_EQ(do_cmp_set_vals(oid, Mode::U64, Op::LTE, {{"lte", value2}}), 0);

    {
      std::map<std::string, bufferlist> vals;
      ASSERT_EQ(get_vals(oid, &vals), 0);
      ASSERT_EQ(vals.size(), 6);
      EXPECT_EQ(value1, vals["eq"]);
      EXPECT_EQ(value2, vals["ne"]);
      EXPECT_EQ(value2, vals["gt"]);
      EXPECT_EQ(value2, vals["gte"]);
      EXPECT_EQ(value1, vals["lt"]);
      EXPECT_EQ(value1, vals["lte"]);
    }
  }

  TEST_F(CmpXattr, cmp_set_vals_u64_einval)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    const std::string key = "key";
    const bufferlist value1 = u64_buffer(0);
    const bufferlist value2 = string_buffer("ccc");
    ASSERT_EQ(ioctx.omap_set(oid, {{key, value1}}), 0);
    ASSERT_EQ(do_cmp_set_vals(oid, Mode::U64, Op::EQ, {{key, value2}}), -EINVAL);
  }

  TEST_F(CmpXattr, cmp_set_vals_u64_eio)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    const std::string key = "key";
    const bufferlist value1 = string_buffer("ccc");
    const bufferlist value2 = u64_buffer(0);
    ASSERT_EQ(ioctx.omap_set(oid, {{key, value1}}), 0);
    ASSERT_EQ(do_cmp_set_vals(oid, Mode::U64, Op::EQ, {{key, value2}}), 0);
    {
      std::map<std::string, bufferlist> vals;
      ASSERT_EQ(get_vals(oid, &vals), 0);
      ASSERT_EQ(vals.size(), 1);
      EXPECT_EQ(value1, vals[key]);
    }
  }

  TEST_F(CmpXattr, cmp_set_vals_at_max_keys)
  {
    ComparisonMap comparisons;
    const bufferlist value = u64_buffer(0);
    for (uint32_t i = 0; i < max_keys; i++) {
      comparisons.emplace(std::to_string(i), value);
    }
    librados::ObjectWriteOperation op;
    EXPECT_EQ(cmp_set_vals(op, Mode::U64, Op::EQ, std::move(comparisons), std::nullopt), 0);
  }

  TEST_F(CmpXattr, cmp_set_vals_over_max_keys)
  {
    ComparisonMap comparisons;
    const bufferlist value = u64_buffer(0);
    for (uint32_t i = 0; i < max_keys + 1; i++) {
      comparisons.emplace(std::to_string(i), value);
    }
    librados::ObjectWriteOperation op;
    EXPECT_EQ(cmp_set_vals(op, Mode::U64, Op::EQ, std::move(comparisons), std::nullopt), -E2BIG);
  }
#endif

} // namespace cls::cmpxattr
