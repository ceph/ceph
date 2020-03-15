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

#include "cls/cmpomap/client.h"
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

namespace cls::cmpomap {

// test fixture with helper functions
class CmpOmap : public ::testing::Test {
 protected:
  librados::IoCtx& ioctx = RadosEnv::ioctx;

  int do_cmp_vals(const std::string& oid, Mode mode,
                  Op comparison, ComparisonMap values,
                  std::optional<bufferlist> def = std::nullopt)
  {
    librados::ObjectReadOperation op;
    int ret = cmp_vals(op, mode, comparison,
                       std::move(values), std::move(def));
    if (ret < 0) {
      return ret;
    }
    return ioctx.operate(oid, &op, nullptr);
  }

  int do_cmp_set_vals(const std::string& oid, Mode mode,
                      Op comparison, ComparisonMap values,
                      std::optional<bufferlist> def = std::nullopt)
  {
    librados::ObjectWriteOperation op;
    int ret = cmp_set_vals(op, mode, comparison,
                           std::move(values), std::move(def));
    if (ret < 0) {
      return ret;
    }
    return ioctx.operate(oid, &op);
  }

  int do_cmp_rm_keys(const std::string& oid, Mode mode,
                     Op comparison, ComparisonMap values)
  {
    librados::ObjectWriteOperation op;
    int ret = cmp_rm_keys(op, mode, comparison, std::move(values));
    if (ret < 0) {
      return ret;
    }
    return ioctx.operate(oid, &op);
  }

  int get_vals(const std::string& oid, std::map<std::string, bufferlist>* vals)
  {
    std::string marker;
    bool more = false;
    do {
      std::map<std::string, bufferlist> tmp;
      int r = ioctx.omap_get_vals2(oid, marker, 1000, &tmp, &more);
      if (r < 0) {
        return r;
      }
      if (!tmp.empty()) {
        marker = tmp.rbegin()->first;
        vals->merge(std::move(tmp));
      }
    } while (more);
    return 0;
  }
};

TEST_F(CmpOmap, cmp_vals_noexist_str)
{
  const std::string oid = __PRETTY_FUNCTION__;
  ASSERT_EQ(ioctx.create(oid, true), 0);
  const std::string key = "key";

  // compare a nonempty value against a missing key with no default
  const bufferlist input = string_buffer("a");
  EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::EQ, {{key, input}}), -ECANCELED);
  EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::NE, {{key, input}}), -ECANCELED);
  EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::GT, {{key, input}}), -ECANCELED);
  EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::GTE, {{key, input}}), -ECANCELED);
  EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::LT, {{key, input}}), -ECANCELED);
  EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::LTE, {{key, input}}), -ECANCELED);
}

TEST_F(CmpOmap, cmp_vals_noexist_str_default)
{
  const std::string oid = __PRETTY_FUNCTION__;
  ASSERT_EQ(ioctx.create(oid, true), 0);
  const std::string key = "key";

  // compare a nonempty value against a missing key with nonempty default
  const bufferlist input = string_buffer("a");
  const bufferlist def = string_buffer("b");
  EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::EQ, {{key, input}}, def), -ECANCELED);
  EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::NE, {{key, input}}, def), 0);
  EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::GT, {{key, input}}, def), -ECANCELED);
  EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::GTE, {{key, input}}, def), -ECANCELED);
  EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::LT, {{key, input}}, def), 0);
  EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::LTE, {{key, input}}, def), 0);
}

TEST_F(CmpOmap, cmp_vals_noexist_u64)
{
  const std::string oid = __PRETTY_FUNCTION__;
  ASSERT_EQ(ioctx.create(oid, true), 0);
  const std::string key = "key";

  // 0 != nullopt
  const bufferlist input = u64_buffer(0);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::EQ, {{key, input}}), -ECANCELED);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::NE, {{key, input}}), -ECANCELED);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::GT, {{key, input}}), -ECANCELED);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::GTE, {{key, input}}), -ECANCELED);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::LT, {{key, input}}), -ECANCELED);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::LTE, {{key, input}}), -ECANCELED);
}

TEST_F(CmpOmap, cmp_vals_noexist_u64_default)
{
  const std::string oid = __PRETTY_FUNCTION__;
  ASSERT_EQ(ioctx.create(oid, true), 0);
  const std::string key = "key";

  // 1 == noexist
  const bufferlist input = u64_buffer(1);
  const bufferlist def = u64_buffer(2);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::EQ, {{key, input}}, def), -ECANCELED);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::NE, {{key, input}}, def), 0);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::GT, {{key, input}}, def), -ECANCELED);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::GTE, {{key, input}}, def), -ECANCELED);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::LT, {{key, input}}, def), 0);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::LTE, {{key, input}}, def), 0);
}

TEST_F(CmpOmap, cmp_vals_str)
{
  const std::string oid = __PRETTY_FUNCTION__;
  const std::string key = "key";
  ASSERT_EQ(ioctx.omap_set(oid, {{key, string_buffer("bbb")}}), 0);
  {
    // empty < existing
    const bufferlist empty;
    EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::EQ, {{key, empty}}), -ECANCELED);
    EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::NE, {{key, empty}}), 0);
    EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::GT, {{key, empty}}), -ECANCELED);
    EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::GTE, {{key, empty}}), -ECANCELED);
    EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::LT, {{key, empty}}), 0);
    EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::LTE, {{key, empty}}), 0);
  }
  {
    // value < existing
    const bufferlist value = string_buffer("aaa");
    EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::EQ, {{key, value}}), -ECANCELED);
    EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::NE, {{key, value}}), 0);
    EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::GT, {{key, value}}), -ECANCELED);
    EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::GTE, {{key, value}}), -ECANCELED);
    EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::LT, {{key, value}}), 0);
    EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::LTE, {{key, value}}), 0);
  }
  {
    // value > existing
    const bufferlist value = string_buffer("bbbb");
    EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::EQ, {{key, value}}), -ECANCELED);
    EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::NE, {{key, value}}), 0);
    EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::GT, {{key, value}}), 0);
    EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::GTE, {{key, value}}), 0);
    EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::LT, {{key, value}}), -ECANCELED);
    EXPECT_EQ(do_cmp_vals(oid, Mode::String, Op::LTE, {{key, value}}), -ECANCELED);
  }
}

TEST_F(CmpOmap, cmp_vals_u64)
{
  const std::string oid = __PRETTY_FUNCTION__;
  const std::string key = "key";
  ASSERT_EQ(ioctx.omap_set(oid, {{key, u64_buffer(42)}}), 0);
  {
    // 0 < existing
    const bufferlist value = u64_buffer(0);
    EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::EQ, {{key, value}}), -ECANCELED);
    EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::NE, {{key, value}}), 0);
    EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::GT, {{key, value}}), -ECANCELED);
    EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::GTE, {{key, value}}), -ECANCELED);
    EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::LT, {{key, value}}), 0);
    EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::LTE, {{key, value}}), 0);
  }
  {
    // 42 == existing
    const bufferlist value = u64_buffer(42);
    EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::EQ, {{key, value}}), 0);
    EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::NE, {{key, value}}), -ECANCELED);
    EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::GT, {{key, value}}), -ECANCELED);
    EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::GTE, {{key, value}}), 0);
    EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::LT, {{key, value}}), -ECANCELED);
    EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::LTE, {{key, value}}), 0);
  }
  {
    // uint64-max > existing
    uint64_t v = std::numeric_limits<uint64_t>::max();
    const bufferlist value = u64_buffer(v);
    EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::EQ, {{key, value}}), -ECANCELED);
    EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::NE, {{key, value}}), 0);
    EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::GT, {{key, value}}), 0);
    EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::GTE, {{key, value}}), 0);
    EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::LT, {{key, value}}), -ECANCELED);
    EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::LTE, {{key, value}}), -ECANCELED);
  }
}

TEST_F(CmpOmap, cmp_vals_u64_invalid_input)
{
  const std::string oid = __PRETTY_FUNCTION__;
  ASSERT_EQ(ioctx.create(oid, true), 0);
  const std::string key = "key";
  const bufferlist empty; // empty buffer can't be decoded as u64
  const bufferlist def = u64_buffer(0);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::EQ, {{key, empty}}, def), -EINVAL);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::NE, {{key, empty}}, def), -EINVAL);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::GT, {{key, empty}}, def), -EINVAL);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::GTE, {{key, empty}}, def), -EINVAL);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::LT, {{key, empty}}, def), -EINVAL);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::LTE, {{key, empty}}, def), -EINVAL);
}

TEST_F(CmpOmap, cmp_vals_u64_invalid_default)
{
  const std::string oid = __PRETTY_FUNCTION__;
  ASSERT_EQ(ioctx.create(oid, true), 0);
  const std::string key = "key";
  const bufferlist input = u64_buffer(0);
  const bufferlist def; // empty buffer can't be decoded as u64
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::EQ, {{key, input}}, def), -EIO);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::NE, {{key, input}}, def), -EIO);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::GT, {{key, input}}, def), -EIO);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::GTE, {{key, input}}, def), -EIO);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::LT, {{key, input}}, def), -EIO);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::LTE, {{key, input}}, def), -EIO);
}

TEST_F(CmpOmap, cmp_vals_u64_invalid_value)
{
  const std::string oid = __PRETTY_FUNCTION__;
  ASSERT_EQ(ioctx.create(oid, true), 0);
  const std::string key = "key";
  ASSERT_EQ(ioctx.omap_set(oid, {{key, string_buffer("bbb")}}), 0);
  const bufferlist input = u64_buffer(0);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::EQ, {{key, input}}), -EIO);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::NE, {{key, input}}), -EIO);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::GT, {{key, input}}), -EIO);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::GTE, {{key, input}}), -EIO);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::LT, {{key, input}}), -EIO);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::LTE, {{key, input}}), -EIO);
}

TEST_F(CmpOmap, cmp_vals_at_max_keys)
{
  ComparisonMap comparisons;
  const bufferlist empty;
  for (uint32_t i = 0; i < max_keys; i++) {
    comparisons.emplace(std::to_string(i), empty);
  }
  librados::ObjectReadOperation op;
  EXPECT_EQ(cmp_vals(op, Mode::String, Op::EQ, std::move(comparisons), empty), 0);
}

TEST_F(CmpOmap, cmp_vals_over_max_keys)
{
  ComparisonMap comparisons;
  const bufferlist empty;
  for (uint32_t i = 0; i < max_keys + 1; i++) {
    comparisons.emplace(std::to_string(i), empty);
  }
  librados::ObjectReadOperation op;
  EXPECT_EQ(cmp_vals(op, Mode::String, Op::EQ, std::move(comparisons), empty), -E2BIG);
}

TEST_F(CmpOmap, cmp_set_vals_noexist_str)
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

TEST_F(CmpOmap, cmp_set_vals_noexist_str_default)
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

TEST_F(CmpOmap, cmp_set_vals_noexist_u64)
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

TEST_F(CmpOmap, cmp_set_vals_noexist_u64_default)
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

TEST_F(CmpOmap, cmp_set_vals_str)
{
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
}

TEST_F(CmpOmap, cmp_set_vals_u64)
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

TEST_F(CmpOmap, cmp_set_vals_u64_einval)
{
  const std::string oid = __PRETTY_FUNCTION__;
  const std::string key = "key";
  const bufferlist value1 = u64_buffer(0);
  const bufferlist value2 = string_buffer("ccc");
  ASSERT_EQ(ioctx.omap_set(oid, {{key, value1}}), 0);
  ASSERT_EQ(do_cmp_set_vals(oid, Mode::U64, Op::EQ, {{key, value2}}), -EINVAL);
}

TEST_F(CmpOmap, cmp_set_vals_u64_eio)
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

TEST_F(CmpOmap, cmp_set_vals_at_max_keys)
{
  ComparisonMap comparisons;
  const bufferlist value = u64_buffer(0);
  for (uint32_t i = 0; i < max_keys; i++) {
    comparisons.emplace(std::to_string(i), value);
  }
  librados::ObjectWriteOperation op;
  EXPECT_EQ(cmp_set_vals(op, Mode::U64, Op::EQ, std::move(comparisons), std::nullopt), 0);
}

TEST_F(CmpOmap, cmp_set_vals_over_max_keys)
{
  ComparisonMap comparisons;
  const bufferlist value = u64_buffer(0);
  for (uint32_t i = 0; i < max_keys + 1; i++) {
    comparisons.emplace(std::to_string(i), value);
  }
  librados::ObjectWriteOperation op;
  EXPECT_EQ(cmp_set_vals(op, Mode::U64, Op::EQ, std::move(comparisons), std::nullopt), -E2BIG);
}

TEST_F(CmpOmap, cmp_rm_keys_noexist_str)
{
  const std::string oid = __PRETTY_FUNCTION__;
  const bufferlist empty;
  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::String, Op::EQ, {{"eq", empty}}), 0);
  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::String, Op::NE, {{"ne", empty}}), 0);
  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::String, Op::GT, {{"gt", empty}}), 0);
  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::String, Op::GTE, {{"gte", empty}}), 0);
  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::String, Op::LT, {{"lt", empty}}), 0);
  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::String, Op::LTE, {{"lte", empty}}), 0);

  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), -ENOENT);
}

TEST_F(CmpOmap, cmp_rm_keys_noexist_u64)
{
  const std::string oid = __PRETTY_FUNCTION__;
  const bufferlist value = u64_buffer(0);
  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::U64, Op::EQ, {{"eq", value}}), 0);
  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::U64, Op::NE, {{"ne", value}}), 0);
  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::U64, Op::GT, {{"gt", value}}), 0);
  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::U64, Op::GTE, {{"gte", value}}), 0);
  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::U64, Op::LT, {{"lt", value}}), 0);
  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::U64, Op::LTE, {{"lte", value}}), 0);

  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), -ENOENT);
}

TEST_F(CmpOmap, cmp_rm_keys_str)
{
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

  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::String, Op::EQ, {{"eq", value2}}), 0);
  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::String, Op::NE, {{"ne", value2}}), 0);
  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::String, Op::GT, {{"gt", value2}}), 0);
  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::String, Op::GTE, {{"gte", value2}}), 0);
  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::String, Op::LT, {{"lt", value2}}), 0);
  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::String, Op::LTE, {{"lte", value2}}), 0);

  {
    std::map<std::string, bufferlist> vals;
    ASSERT_EQ(get_vals(oid, &vals), 0);
    EXPECT_EQ(vals.count("eq"), 1);
    EXPECT_EQ(vals.count("ne"), 0);
    EXPECT_EQ(vals.count("gt"), 0);
    EXPECT_EQ(vals.count("gte"), 0);
    EXPECT_EQ(vals.count("lt"), 1);
    EXPECT_EQ(vals.count("lte"), 1);
  }
}

TEST_F(CmpOmap, cmp_rm_keys_u64)
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

  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::U64, Op::EQ, {{"eq", value2}}), 0);
  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::U64, Op::NE, {{"ne", value2}}), 0);
  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::U64, Op::GT, {{"gt", value2}}), 0);
  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::U64, Op::GTE, {{"gte", value2}}), 0);
  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::U64, Op::LT, {{"lt", value2}}), 0);
  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::U64, Op::LTE, {{"lte", value2}}), 0);

  {
    std::map<std::string, bufferlist> vals;
    ASSERT_EQ(get_vals(oid, &vals), 0);
    EXPECT_EQ(vals.count("eq"), 1);
    EXPECT_EQ(vals.count("ne"), 0);
    EXPECT_EQ(vals.count("gt"), 0);
    EXPECT_EQ(vals.count("gte"), 0);
    EXPECT_EQ(vals.count("lt"), 1);
    EXPECT_EQ(vals.count("lte"), 1);
  }
}

TEST_F(CmpOmap, cmp_rm_keys_u64_einval)
{
  const std::string oid = __PRETTY_FUNCTION__;
  const std::string key = "key";
  const bufferlist value1 = u64_buffer(0);
  const bufferlist value2 = string_buffer("ccc");
  ASSERT_EQ(ioctx.omap_set(oid, {{key, value1}}), 0);
  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::U64, Op::EQ, {{key, value2}}), -EINVAL);
}

TEST_F(CmpOmap, cmp_rm_keys_u64_eio)
{
  const std::string oid = __PRETTY_FUNCTION__;
  const std::string key = "key";
  const bufferlist value1 = string_buffer("ccc");
  const bufferlist value2 = u64_buffer(0);
  ASSERT_EQ(ioctx.omap_set(oid, {{key, value1}}), 0);
  ASSERT_EQ(do_cmp_rm_keys(oid, Mode::U64, Op::EQ, {{key, value2}}), 0);
  {
    std::map<std::string, bufferlist> vals;
    ASSERT_EQ(get_vals(oid, &vals), 0);
    EXPECT_EQ(vals.count(key), 1);
  }
}

TEST_F(CmpOmap, cmp_rm_keys_at_max_keys)
{
  ComparisonMap comparisons;
  const bufferlist value = u64_buffer(0);
  for (uint32_t i = 0; i < max_keys; i++) {
    comparisons.emplace(std::to_string(i), value);
  }
  librados::ObjectWriteOperation op;
  EXPECT_EQ(cmp_rm_keys(op, Mode::U64, Op::EQ, std::move(comparisons)), 0);
}

TEST_F(CmpOmap, cmp_rm_keys_over_max_keys)
{
  ComparisonMap comparisons;
  const bufferlist value = u64_buffer(0);
  for (uint32_t i = 0; i < max_keys + 1; i++) {
    comparisons.emplace(std::to_string(i), value);
  }
  librados::ObjectWriteOperation op;
  EXPECT_EQ(cmp_rm_keys(op, Mode::U64, Op::EQ, std::move(comparisons)), -E2BIG);
}

} // namespace cls::cmpomap
