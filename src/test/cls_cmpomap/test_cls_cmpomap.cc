// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

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

  // Generic helper to wrap operation setup and execution
  // Handles common pattern of: build operation -> check return -> execute with flags
  template<typename OpType, typename Func, typename... Args>
  int execute_op(const std::string& oid, Func&& func,
                 bool need_returnvec, Args&&... args)
  {
    OpType op;
    int ret = func(op, std::forward<Args>(args)...);
    if (ret < 0) {
      return ret;
    }
    if (need_returnvec) {
      return ioctx.operate(oid, &op, librados::OPERATION_RETURNVEC);
    } else {
      return ioctx.operate(oid, &op);
    }
  }

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
    return execute_op<librados::ObjectWriteOperation>(
      oid, cmp_set_vals, false, mode, comparison,
      std::move(values), std::move(def));
  }

  int do_cmp_rm_keys(const std::string& oid, Mode mode,
                     Op comparison, ComparisonMap values)
  {
    return execute_op<librados::ObjectWriteOperation>(
      oid, cmp_rm_keys, false, mode, comparison, std::move(values));
  }

  int do_cmp_set_vals2(const std::string& oid, Mode mode,
                       Op comparison, ComparisonMap cmp_values,
                       std::map<std::string, bufferlist> set_values,
                       std::optional<bufferlist> def = std::nullopt)
  {
    return execute_op<librados::ObjectWriteOperation>(
      oid, cmp_set_vals2, false, mode, comparison,
      std::move(cmp_values), std::move(set_values), std::move(def));
  }

  int do_cmp_rm_keys2(const std::string& oid, Mode mode,
                      Op comparison, ComparisonMap cmp_values,
                      std::set<std::string> rm_keys,
                      std::optional<bufferlist> def = std::nullopt)
  {
    return execute_op<librados::ObjectWriteOperation>(
      oid, cmp_rm_keys2, false, mode, comparison,
      std::move(cmp_values), std::move(rm_keys), std::move(def));
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
  const bufferlist def = string_buffer("bbb"); // can't be decoded as u64
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::EQ, {{key, input}}, def), -EIO);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::NE, {{key, input}}, def), -EIO);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::GT, {{key, input}}, def), -EIO);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::GTE, {{key, input}}, def), -EIO);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::LT, {{key, input}}, def), -EIO);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::LTE, {{key, input}}, def), -EIO);
}

TEST_F(CmpOmap, cmp_vals_u64_empty_default)
{
  const std::string oid = __PRETTY_FUNCTION__;
  ASSERT_EQ(ioctx.create(oid, true), 0);
  const std::string key = "key";
  const bufferlist input = u64_buffer(1);
  const bufferlist def; // empty buffer defaults to 0
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::EQ, {{key, input}}, def), -ECANCELED);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::NE, {{key, input}}, def), 0);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::GT, {{key, input}}, def), 0);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::GTE, {{key, input}}, def), 0);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::LT, {{key, input}}, def), -ECANCELED);
  EXPECT_EQ(do_cmp_vals(oid, Mode::U64, Op::LTE, {{key, input}}, def), -ECANCELED);
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

// test upgrades from empty omap values to u64
TEST_F(CmpOmap, cmp_rm_keys_u64_empty)
{
  const std::string oid = __PRETTY_FUNCTION__;
  const bufferlist value1; // empty buffer
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

// ============================================================================
// Tests for cmp_set_vals2
// ============================================================================

TEST_F(CmpOmap, cmp_set_vals2_basic_success)
{
  const std::string oid = __PRETTY_FUNCTION__;
  // Set up initial values
  ASSERT_EQ(ioctx.omap_set(oid, {{"cmp_key", u64_buffer(10)}}), 0);

  // Compare cmp_key==10, if true set set_key=20
  std::map<std::string, bufferlist> set_vals = {{"set_key", u64_buffer(20)}};
  ASSERT_EQ(do_cmp_set_vals2(oid, Mode::U64, Op::EQ, 
                             {{"cmp_key", u64_buffer(10)}},
                             std::move(set_vals)), 0);

  // Verify results
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  ASSERT_EQ(vals.size(), 2);
  EXPECT_EQ(vals["cmp_key"], u64_buffer(10));
  EXPECT_EQ(vals["set_key"], u64_buffer(20));
}

TEST_F(CmpOmap, cmp_set_vals2_comparison_fails)
{
  const std::string oid = __PRETTY_FUNCTION__;
  // Set up initial values
  ASSERT_EQ(ioctx.omap_set(oid, {{"cmp_key", u64_buffer(10)}}), 0);

  // Compare cmp_key==20 (should fail), don't set set_key
  std::map<std::string, bufferlist> set_vals = {{"set_key", u64_buffer(20)}};
  ASSERT_EQ(do_cmp_set_vals2(oid, Mode::U64, Op::EQ,
                             {{"cmp_key", u64_buffer(20)}},
                             std::move(set_vals)), -ECANCELED);

  // Verify set_key was not created
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  ASSERT_EQ(vals.size(), 1);
  EXPECT_EQ(vals.count("set_key"), 0);
}

TEST_F(CmpOmap, cmp_set_vals2_disjoint_keys)
{
  const std::string oid = __PRETTY_FUNCTION__;
  // Set up initial values - comparison keys are completely separate from set keys
  ASSERT_EQ(ioctx.omap_set(oid, {{"cmp_key1", u64_buffer(10)},
                                  {"cmp_key2", u64_buffer(20)},
                                  {"existing_data", u64_buffer(100)}}), 0);

  // Compare cmp_key1 and cmp_key2, but set completely different keys
  std::map<std::string, bufferlist> set_vals = {
    {"new_key1", u64_buffer(200)},
    {"new_key2", u64_buffer(300)}
  };
  ASSERT_EQ(do_cmp_set_vals2(oid, Mode::U64, Op::GT,
                             {{"cmp_key1", u64_buffer(15)},
                              {"cmp_key2", u64_buffer(25)}},
                             std::move(set_vals)), 0);

  // Verify: cmp keys unchanged, new keys created, existing data unchanged
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  ASSERT_EQ(vals.size(), 5);
  EXPECT_EQ(vals["cmp_key1"], u64_buffer(10));  // unchanged
  EXPECT_EQ(vals["cmp_key2"], u64_buffer(20));  // unchanged
  EXPECT_EQ(vals["existing_data"], u64_buffer(100));  // unchanged
  EXPECT_EQ(vals["new_key1"], u64_buffer(200));  // newly created
  EXPECT_EQ(vals["new_key2"], u64_buffer(300));  // newly created
}

TEST_F(CmpOmap, cmp_set_vals2_partial_overlap)
{
  const std::string oid = __PRETTY_FUNCTION__;
  // Test case where some keys overlap between cmp and set
  ASSERT_EQ(ioctx.omap_set(oid, {{"key1", u64_buffer(10)},
                                  {"key2", u64_buffer(20)},
                                  {"key3", u64_buffer(30)}}), 0);

  // Compare key1 and key2, set key2 and key3
  std::map<std::string, bufferlist> set_vals = {
    {"key2", u64_buffer(99)},  // overwrite key2
    {"key3", u64_buffer(88)}   // overwrite key3
  };
  ASSERT_EQ(do_cmp_set_vals2(oid, Mode::U64, Op::LTE,
                             {{"key1", u64_buffer(5)},
                              {"key2", u64_buffer(15)}},
                             std::move(set_vals)), 0);

  // Verify results
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  ASSERT_EQ(vals.size(), 3);
  EXPECT_EQ(vals["key1"], u64_buffer(10));  // unchanged (only compared)
  EXPECT_EQ(vals["key2"], u64_buffer(99));  // changed (compared and set)
  EXPECT_EQ(vals["key3"], u64_buffer(88));  // changed (only set)
}

TEST_F(CmpOmap, cmp_set_vals2_compare_one_set_many)
{
  const std::string oid = __PRETTY_FUNCTION__;
  // Use case: check a single condition, update many values atomically
  ASSERT_EQ(ioctx.omap_set(oid, {{"lock_flag", u64_buffer(0)}}), 0);

  // If lock_flag==0 (unlocked), set multiple config values
  std::map<std::string, bufferlist> set_vals = {
    {"config1", u64_buffer(100)},
    {"config2", u64_buffer(200)},
    {"config3", u64_buffer(300)},
    {"config4", u64_buffer(400)}
  };
  ASSERT_EQ(do_cmp_set_vals2(oid, Mode::U64, Op::EQ,
                             {{"lock_flag", u64_buffer(0)}},
                             std::move(set_vals)), 0);

  // Verify all configs were set
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  ASSERT_EQ(vals.size(), 5);
  EXPECT_EQ(vals["lock_flag"], u64_buffer(0));  // unchanged
  EXPECT_EQ(vals["config1"], u64_buffer(100));
  EXPECT_EQ(vals["config2"], u64_buffer(200));
  EXPECT_EQ(vals["config3"], u64_buffer(300));
  EXPECT_EQ(vals["config4"], u64_buffer(400));
}

TEST_F(CmpOmap, cmp_set_vals2_compare_many_set_one)
{
  const std::string oid = __PRETTY_FUNCTION__;
  // Use case: check multiple preconditions before single update
  ASSERT_EQ(ioctx.omap_set(oid, {{"version", u64_buffer(5)},
                                  {"state", u64_buffer(1)},
                                  {"permission", u64_buffer(7)},
                                  {"result", u64_buffer(0)}}), 0);

  // Only update result if all preconditions are met
  std::map<std::string, bufferlist> set_vals = {{"result", u64_buffer(42)}};
  ASSERT_EQ(do_cmp_set_vals2(oid, Mode::U64, Op::LTE,
                             {{"version", u64_buffer(5)},
                              {"state", u64_buffer(1)},
                              {"permission", u64_buffer(4)}},
                             std::move(set_vals)), 0);

  // Verify only result changed
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  ASSERT_EQ(vals.size(), 4);
  EXPECT_EQ(vals["version"], u64_buffer(5));
  EXPECT_EQ(vals["state"], u64_buffer(1));
  EXPECT_EQ(vals["permission"], u64_buffer(7));
  EXPECT_EQ(vals["result"], u64_buffer(42));  // only this changed
}

TEST_F(CmpOmap, cmp_set_vals2_multiple_comparisons_all_pass)
{
  const std::string oid = __PRETTY_FUNCTION__;
  // Set up initial values
  ASSERT_EQ(ioctx.omap_set(oid, {{"cmp1", u64_buffer(10)},
                                  {"cmp2", u64_buffer(20)}}), 0);

  // Compare both keys, set multiple values if all pass
  std::map<std::string, bufferlist> set_vals = {{"set1", u64_buffer(100)},
                                                 {"set2", u64_buffer(200)}};
  ASSERT_EQ(do_cmp_set_vals2(oid, Mode::U64, Op::LTE,
                             {{"cmp1", u64_buffer(5)},
                              {"cmp2", u64_buffer(15)}},
                             std::move(set_vals)), 0);

  // Verify all values
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  ASSERT_EQ(vals.size(), 4);
  EXPECT_EQ(vals["set1"], u64_buffer(100));
  EXPECT_EQ(vals["set2"], u64_buffer(200));
}

TEST_F(CmpOmap, cmp_set_vals2_multiple_comparisons_one_fails)
{
  const std::string oid = __PRETTY_FUNCTION__;
  // Set up initial values
  ASSERT_EQ(ioctx.omap_set(oid, {{"cmp1", u64_buffer(10)},
                                  {"cmp2", u64_buffer(20)}}), 0);

  // One comparison fails, so no values should be set
  std::map<std::string, bufferlist> set_vals = {{"set1", u64_buffer(100)}};
  ASSERT_EQ(do_cmp_set_vals2(oid, Mode::U64, Op::LT,
                             {{"cmp1", u64_buffer(5)},   // passes: 5 < 10
                              {"cmp2", u64_buffer(25)}}, // fails: 25 < 20
                             std::move(set_vals)), -ECANCELED);

  // Verify set_vals were not applied
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  ASSERT_EQ(vals.size(), 2);
  EXPECT_EQ(vals.count("set1"), 0);
}

TEST_F(CmpOmap, cmp_set_vals2_with_default_value)
{
  const std::string oid = __PRETTY_FUNCTION__;
  ASSERT_EQ(ioctx.create(oid, true), 0);

  // Compare against nonexistent key with default value
  std::map<std::string, bufferlist> set_vals = {{"set_key", u64_buffer(99)}};
  bufferlist def = u64_buffer(0);
  ASSERT_EQ(do_cmp_set_vals2(oid, Mode::U64, Op::EQ,
                             {{"missing_key", u64_buffer(0)}},
                             std::move(set_vals), def), 0);

  // Verify set_key was created
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  ASSERT_EQ(vals.size(), 1);
  EXPECT_EQ(vals["set_key"], u64_buffer(99));
}

TEST_F(CmpOmap, cmp_set_vals2_missing_key_no_default)
{
  const std::string oid = __PRETTY_FUNCTION__;

  // Compare against nonexistent key without default - should fail
  std::map<std::string, bufferlist> set_vals = {{"set_key", u64_buffer(99)}};
  ASSERT_EQ(do_cmp_set_vals2(oid, Mode::U64, Op::EQ,
                             {{"missing_key", u64_buffer(0)}},
                             std::move(set_vals)), -ECANCELED);

  // Verify nothing was set
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), -ENOENT);
}

TEST_F(CmpOmap, cmp_set_vals2_string_mode)
{
  const std::string oid = __PRETTY_FUNCTION__;
  ASSERT_EQ(ioctx.omap_set(oid, {{"cmp_key", string_buffer("aaa")}}), 0);

  // Compare string, set if match
  std::map<std::string, bufferlist> set_vals = {{"set_key", string_buffer("zzz")}};
  ASSERT_EQ(do_cmp_set_vals2(oid, Mode::String, Op::EQ,
                             {{"cmp_key", string_buffer("aaa")}},
                             std::move(set_vals)), 0);

  // Verify
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  EXPECT_EQ(vals["set_key"], string_buffer("zzz"));
}

TEST_F(CmpOmap, cmp_set_vals2_overwrite_existing)
{
  const std::string oid = __PRETTY_FUNCTION__;
  ASSERT_EQ(ioctx.omap_set(oid, {{"cmp_key", u64_buffer(10)},
                                  {"set_key", u64_buffer(5)}}), 0);

  // Overwrite existing set_key if comparison passes
  std::map<std::string, bufferlist> set_vals = {{"set_key", u64_buffer(50)}};
  ASSERT_EQ(do_cmp_set_vals2(oid, Mode::U64, Op::EQ,
                             {{"cmp_key", u64_buffer(10)}},
                             std::move(set_vals)), 0);

  // Verify overwrite
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  EXPECT_EQ(vals["set_key"], u64_buffer(50));
}

TEST_F(CmpOmap, cmp_set_vals2_u64_invalid_input)
{
  const std::string oid = __PRETTY_FUNCTION__;
  ASSERT_EQ(ioctx.create(oid, true), 0);

  // Invalid input value for U64 mode
  std::map<std::string, bufferlist> set_vals = {{"set_key", u64_buffer(1)}};
  bufferlist def = u64_buffer(0);
  bufferlist invalid = string_buffer("invalid"); // invalid number
  EXPECT_EQ(do_cmp_set_vals2(oid, Mode::U64, Op::EQ,
                             {{"key", invalid}},
                             std::move(set_vals), def), -EINVAL);
}

TEST_F(CmpOmap, cmp_set_vals2_at_max_keys)
{
  ComparisonMap cmp_vals;
  std::map<std::string, bufferlist> set_vals;
  for (uint32_t i = 0; i < max_keys; i++) {
    cmp_vals.emplace(std::to_string(i), u64_buffer(i));
    set_vals.emplace(std::to_string(i+max_keys), u64_buffer(i));
  }
  librados::ObjectWriteOperation op;
  EXPECT_EQ(cmp_set_vals2(op, Mode::U64, Op::GTE, std::move(cmp_vals),
                          std::move(set_vals), std::nullopt), 0);
}

TEST_F(CmpOmap, cmp_set_vals2_over_max_keys_cmp)
{
  ComparisonMap cmp_vals;
  std::map<std::string, bufferlist> set_vals = {{"set1", u64_buffer(1)}};
  for (uint32_t i = 0; i < max_keys + 1; i++) {
    cmp_vals.emplace(std::to_string(i), u64_buffer(i));
  }
  librados::ObjectWriteOperation op;
  EXPECT_EQ(cmp_set_vals2(op, Mode::U64, Op::LTE, std::move(cmp_vals),
                          std::move(set_vals), std::nullopt), -E2BIG);
}

TEST_F(CmpOmap, cmp_set_vals2_over_max_keys_set)
{
  ComparisonMap cmp_vals = {{"cmp1", u64_buffer(1)}};
  std::map<std::string, bufferlist> set_vals;
  for (uint32_t i = 0; i < max_keys + 1; i++) {
    set_vals.emplace(std::to_string(i), u64_buffer(i));
  }
  librados::ObjectWriteOperation op;
  EXPECT_EQ(cmp_set_vals2(op, Mode::U64, Op::LTE, std::move(cmp_vals),
                          std::move(set_vals), std::nullopt), -E2BIG);
}

TEST_F(CmpOmap, cmp_set_vals2_empty_cmp_values)
{
  const std::string oid = __PRETTY_FUNCTION__;
  ASSERT_EQ(ioctx.create(oid, true), 0);

  // Empty comparison map should always succeed
  ComparisonMap empty_cmp;
  std::map<std::string, bufferlist> set_vals = {
    {"key1", u64_buffer(100)},
    {"key2", u64_buffer(200)}
  };
  ASSERT_EQ(do_cmp_set_vals2(oid, Mode::U64, Op::EQ,
                             std::move(empty_cmp),
                             std::move(set_vals)), 0);

  // Verify values were set
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  ASSERT_EQ(vals.size(), 2);
  EXPECT_EQ(vals["key1"], u64_buffer(100));
  EXPECT_EQ(vals["key2"], u64_buffer(200));
}

TEST_F(CmpOmap, cmp_set_vals2_empty_cmp_values_with_existing)
{
  const std::string oid = __PRETTY_FUNCTION__;
  ASSERT_EQ(ioctx.omap_set(oid, {{"existing", u64_buffer(42)}}), 0);

  // Empty comparison map with existing data
  ComparisonMap empty_cmp;
  std::map<std::string, bufferlist> set_vals = {{"new_key", u64_buffer(99)}};
  ASSERT_EQ(do_cmp_set_vals2(oid, Mode::U64, Op::EQ,
                             std::move(empty_cmp),
                             std::move(set_vals)), 0);

  // Verify both keys exist
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  ASSERT_EQ(vals.size(), 2);
  EXPECT_EQ(vals["existing"], u64_buffer(42));
  EXPECT_EQ(vals["new_key"], u64_buffer(99));
}

TEST_F(CmpOmap, cmp_set_vals2_empty_cmp_values_overwrite)
{
  const std::string oid = __PRETTY_FUNCTION__;
  ASSERT_EQ(ioctx.omap_set(oid, {{"key1", u64_buffer(10)}}), 0);

  // Empty comparison map, overwrite existing key
  ComparisonMap empty_cmp;
  std::map<std::string, bufferlist> set_vals = {{"key1", u64_buffer(50)}};
  ASSERT_EQ(do_cmp_set_vals2(oid, Mode::U64, Op::EQ,
                             std::move(empty_cmp),
                             std::move(set_vals)), 0);

  // Verify key was overwritten
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  EXPECT_EQ(vals["key1"], u64_buffer(50));
}

// ============================================================================
// Tests for cmp_rm_keys2
// ============================================================================

TEST_F(CmpOmap, cmp_rm_keys2_basic_success)
{
  const std::string oid = __PRETTY_FUNCTION__;
  // Set up initial values
  ASSERT_EQ(ioctx.omap_set(oid, {{"cmp_key", u64_buffer(10)},
                                  {"rm_key", u64_buffer(20)}}), 0);

  // Compare cmp_key==10, if true remove rm_key
  ASSERT_EQ(do_cmp_rm_keys2(oid, Mode::U64, Op::EQ,
                            {{"cmp_key", u64_buffer(10)}},
                            {"rm_key"}), 0);

  // Verify rm_key was removed
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  ASSERT_EQ(vals.size(), 1);
  EXPECT_EQ(vals.count("cmp_key"), 1);
  EXPECT_EQ(vals.count("rm_key"), 0);
}

TEST_F(CmpOmap, cmp_rm_keys2_comparison_fails)
{
  const std::string oid = __PRETTY_FUNCTION__;
  // Set up initial values
  ASSERT_EQ(ioctx.omap_set(oid, {{"cmp_key", u64_buffer(10)},
                                  {"rm_key", u64_buffer(20)}}), 0);

  // Compare fails, don't remove
  ASSERT_EQ(do_cmp_rm_keys2(oid, Mode::U64, Op::EQ,
                            {{"cmp_key", u64_buffer(99)}},
                            {"rm_key"}), -ECANCELED);

  // Verify rm_key still exists
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  ASSERT_EQ(vals.size(), 2);
  EXPECT_EQ(vals.count("rm_key"), 1);
}

TEST_F(CmpOmap, cmp_rm_keys2_disjoint_keys)
{
  const std::string oid = __PRETTY_FUNCTION__;
  // Compare one set of keys, remove a completely different set
  ASSERT_EQ(ioctx.omap_set(oid, {{"guard1", u64_buffer(10)},
                                  {"guard2", u64_buffer(20)},
                                  {"data1", u64_buffer(100)},
                                  {"data2", u64_buffer(200)},
                                  {"keep", u64_buffer(999)}}), 0);

  // Check guard conditions, then remove data keys (not guard keys)
  ASSERT_EQ(do_cmp_rm_keys2(oid, Mode::U64, Op::GTE,
                            {{"guard1", u64_buffer(15)},
                             {"guard2", u64_buffer(25)}},
                            {"data1", "data2"}), 0);

  // Verify: guards untouched, data removed, keep untouched
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  ASSERT_EQ(vals.size(), 3);
  EXPECT_EQ(vals.count("guard1"), 1);  // unchanged
  EXPECT_EQ(vals.count("guard2"), 1);  // unchanged
  EXPECT_EQ(vals.count("keep"), 1);    // unchanged
  EXPECT_EQ(vals.count("data1"), 0);   // removed
  EXPECT_EQ(vals.count("data2"), 0);   // removed
}

TEST_F(CmpOmap, cmp_rm_keys2_partial_overlap)
{
  const std::string oid = __PRETTY_FUNCTION__;
  // Some keys are both compared and removed
  ASSERT_EQ(ioctx.omap_set(oid, {{"key1", u64_buffer(10)},
                                  {"key2", u64_buffer(20)},
                                  {"key3", u64_buffer(30)}}), 0);

  // Compare key1 and key2, but remove key2 and key3
  ASSERT_EQ(do_cmp_rm_keys2(oid, Mode::U64, Op::GT,
                            {{"key1", u64_buffer(15)},
                             {"key2", u64_buffer(25)}},
                            {"key2", "key3"}), 0);

  // Verify: key1 kept (only compared), key2 and key3 removed
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  ASSERT_EQ(vals.size(), 1);
  EXPECT_EQ(vals.count("key1"), 1);  // kept
  EXPECT_EQ(vals.count("key2"), 0);  // removed (was compared and removed)
  EXPECT_EQ(vals.count("key3"), 0);  // removed (only removed)
}

TEST_F(CmpOmap, cmp_rm_keys2_compare_one_remove_many)
{
  const std::string oid = __PRETTY_FUNCTION__;
  // Use case: check single flag, cleanup many temporary keys
  ASSERT_EQ(ioctx.omap_set(oid, {{"cleanup_flag", u64_buffer(1)},
                                  {"temp1", u64_buffer(1)},
                                  {"temp2", u64_buffer(2)},
                                  {"temp3", u64_buffer(3)},
                                  {"temp4", u64_buffer(4)},
                                  {"permanent", u64_buffer(999)}}), 0);

  // If cleanup_flag==1, remove all temp keys
  ASSERT_EQ(do_cmp_rm_keys2(oid, Mode::U64, Op::EQ,
                            {{"cleanup_flag", u64_buffer(1)}},
                            {"temp1", "temp2", "temp3", "temp4"}), 0);

  // Verify: cleanup_flag and permanent remain, temps removed
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  ASSERT_EQ(vals.size(), 2);
  EXPECT_EQ(vals.count("cleanup_flag"), 1);
  EXPECT_EQ(vals.count("permanent"), 1);
  EXPECT_EQ(vals.count("temp1"), 0);
  EXPECT_EQ(vals.count("temp2"), 0);
  EXPECT_EQ(vals.count("temp3"), 0);
  EXPECT_EQ(vals.count("temp4"), 0);
}

TEST_F(CmpOmap, cmp_rm_keys2_compare_many_remove_one)
{
  const std::string oid = __PRETTY_FUNCTION__;
  // Use case: check multiple conditions before removing critical key
  ASSERT_EQ(ioctx.omap_set(oid, {{"check1", u64_buffer(1)},
                                  {"check2", u64_buffer(2)},
                                  {"check3", u64_buffer(3)},
                                  {"critical_lock", u64_buffer(123)}}), 0);

  // Only remove lock if all checks pass
  ASSERT_EQ(do_cmp_rm_keys2(oid, Mode::U64, Op::LTE,
                            {{"check1", u64_buffer(1)},
                             {"check2", u64_buffer(2)},
                             {"check3", u64_buffer(3)}},
                            {"critical_lock"}), 0);

  // Verify: checks remain, lock removed
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  ASSERT_EQ(vals.size(), 3);
  EXPECT_EQ(vals.count("check1"), 1);
  EXPECT_EQ(vals.count("check2"), 1);
  EXPECT_EQ(vals.count("check3"), 1);
  EXPECT_EQ(vals.count("critical_lock"), 0);  // only this removed
}

TEST_F(CmpOmap, cmp_rm_keys2_multiple_removes)
{
  const std::string oid = __PRETTY_FUNCTION__;
  // Set up initial values
  ASSERT_EQ(ioctx.omap_set(oid, {{"cmp_key", u64_buffer(10)},
                                  {"rm1", u64_buffer(20)},
                                  {"rm2", u64_buffer(30)},
                                  {"keep", u64_buffer(40)}}), 0);

  // Remove multiple keys
  ASSERT_EQ(do_cmp_rm_keys2(oid, Mode::U64, Op::GT,
                            {{"cmp_key", u64_buffer(20)}},
                            {"rm1", "rm2"}), 0);

  // Verify
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  ASSERT_EQ(vals.size(), 2);
  EXPECT_EQ(vals.count("cmp_key"), 1);
  EXPECT_EQ(vals.count("keep"), 1);
  EXPECT_EQ(vals.count("rm1"), 0);
  EXPECT_EQ(vals.count("rm2"), 0);
}

TEST_F(CmpOmap, cmp_rm_keys2_multiple_comparisons)
{
  const std::string oid = __PRETTY_FUNCTION__;
  ASSERT_EQ(ioctx.omap_set(oid, {{"cmp1", u64_buffer(10)},
                                  {"cmp2", u64_buffer(20)},
                                  {"rm_key", u64_buffer(30)}}), 0);

  // All comparisons must pass
  ASSERT_EQ(do_cmp_rm_keys2(oid, Mode::U64, Op::LTE,
                            {{"cmp1", u64_buffer(5)},
                             {"cmp2", u64_buffer(15)}},
                            {"rm_key"}), 0);

  // Verify
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  EXPECT_EQ(vals.count("rm_key"), 0);
}

TEST_F(CmpOmap, cmp_rm_keys2_one_comparison_fails)
{
  const std::string oid = __PRETTY_FUNCTION__;
  ASSERT_EQ(ioctx.omap_set(oid, {{"cmp1", u64_buffer(10)},
                                  {"cmp2", u64_buffer(20)},
                                  {"rm_key", u64_buffer(30)}}), 0);

  // One fails, so nothing is removed
  ASSERT_EQ(do_cmp_rm_keys2(oid, Mode::U64, Op::LT,
                            {{"cmp1", u64_buffer(5)},   // passes
                             {"cmp2", u64_buffer(25)}}, // fails
                            {"rm_key"}), -ECANCELED);

  // Verify rm_key still exists
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  EXPECT_EQ(vals.count("rm_key"), 1);
}

TEST_F(CmpOmap, cmp_rm_keys2_with_default)
{
  const std::string oid = __PRETTY_FUNCTION__;
  ASSERT_EQ(ioctx.omap_set(oid, {{"rm_key", u64_buffer(10)}}), 0);

  // Compare nonexistent key with default
  bufferlist def = u64_buffer(0);
  ASSERT_EQ(do_cmp_rm_keys2(oid, Mode::U64, Op::EQ,
                            {{"missing", u64_buffer(0)}},
                            {"rm_key"}, def), 0);

  // Verify removal
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  ASSERT_EQ(vals.size(), 0); // Key deleted
}

TEST_F(CmpOmap, cmp_rm_keys2_missing_key_no_default)
{
  const std::string oid = __PRETTY_FUNCTION__;
  ASSERT_EQ(ioctx.omap_set(oid, {{"rm_key", u64_buffer(10)}}), 0);

  // Compare nonexistent key without default - fails
  ASSERT_EQ(do_cmp_rm_keys2(oid, Mode::U64, Op::EQ,
                            {{"missing", u64_buffer(0)}},
                            {"rm_key"}), -ECANCELED);

  // Verify not removed
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  EXPECT_EQ(vals.count("rm_key"), 1);
}

TEST_F(CmpOmap, cmp_rm_keys2_string_mode)
{
  const std::string oid = __PRETTY_FUNCTION__;
  ASSERT_EQ(ioctx.omap_set(oid, {{"cmp_key", string_buffer("test")},
                                  {"rm_key", string_buffer("data")}}), 0);

  ASSERT_EQ(do_cmp_rm_keys2(oid, Mode::String, Op::EQ,
                            {{"cmp_key", string_buffer("test")}},
                            {"rm_key"}), 0);

  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  EXPECT_EQ(vals.count("rm_key"), 0);
}

TEST_F(CmpOmap, cmp_rm_keys2_remove_nonexistent_key)
{
  const std::string oid = __PRETTY_FUNCTION__;
  ASSERT_EQ(ioctx.omap_set(oid, {{"cmp_key", u64_buffer(10)}}), 0);

  // Try to remove nonexistent key - should succeed (no-op)
  ASSERT_EQ(do_cmp_rm_keys2(oid, Mode::U64, Op::EQ,
                            {{"cmp_key", u64_buffer(10)}},
                            {"nonexistent"}), 0);
}

TEST_F(CmpOmap, cmp_rm_keys2_at_max_keys)
{
  ComparisonMap cmp_vals;
  std::set<std::string> rm_keys;
  for (uint32_t i = 0; i < max_keys; i++) {
    cmp_vals.emplace(std::to_string(i), u64_buffer(i));
    rm_keys.insert(std::to_string(i+max_keys));
  }
  librados::ObjectWriteOperation op;
  EXPECT_EQ(cmp_rm_keys2(op, Mode::U64, Op::LTE, std::move(cmp_vals),
                         std::move(rm_keys), std::nullopt), 0);
}

TEST_F(CmpOmap, cmp_rm_keys2_over_max_keys_cmp)
{
  ComparisonMap cmp_vals;
  std::set<std::string> rm_keys = {"rm1"};
  for (uint32_t i = 0; i < max_keys + 1; i++) {
    cmp_vals.emplace(std::to_string(i), u64_buffer(i));
  }
  librados::ObjectWriteOperation op;
  EXPECT_EQ(cmp_rm_keys2(op, Mode::U64, Op::LTE, std::move(cmp_vals),
                         std::move(rm_keys), std::nullopt), -E2BIG);
}

TEST_F(CmpOmap, cmp_rm_keys2_over_max_keys_rm)
{
  ComparisonMap cmp_vals = {{"cmp1", u64_buffer(1)}};
  std::set<std::string> rm_keys;
  for (uint32_t i = 0; i < max_keys + 1; i++) {
    rm_keys.insert(std::to_string(i));
  }
  librados::ObjectWriteOperation op;
  EXPECT_EQ(cmp_rm_keys2(op, Mode::U64, Op::LTE, std::move(cmp_vals),
                         std::move(rm_keys), std::nullopt), -E2BIG);
}

TEST_F(CmpOmap, cmp_rm_keys2_empty_cmp_values)
{
  const std::string oid = __PRETTY_FUNCTION__;
  ASSERT_EQ(ioctx.omap_set(oid, {{"key1", u64_buffer(10)},
                                  {"key2", u64_buffer(20)},
                                  {"key3", u64_buffer(30)}}), 0);

  // Empty comparison map should always succeed
  ComparisonMap empty_cmp;
  ASSERT_EQ(do_cmp_rm_keys2(oid, Mode::U64, Op::EQ,
                            std::move(empty_cmp),
                            {"key1", "key2"}), 0);

  // Verify key1 and key2 were removed, key3 remains
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  ASSERT_EQ(vals.size(), 1);
  EXPECT_EQ(vals.count("key1"), 0);
  EXPECT_EQ(vals.count("key2"), 0);
  EXPECT_EQ(vals.count("key3"), 1);
}

TEST_F(CmpOmap, cmp_rm_keys2_empty_cmp_values_remove_all)
{
  const std::string oid = __PRETTY_FUNCTION__;
  ASSERT_EQ(ioctx.omap_set(oid, {{"key1", u64_buffer(10)},
                                  {"key2", u64_buffer(20)}}), 0);

  // Empty comparison map, remove all keys
  ComparisonMap empty_cmp;
  ASSERT_EQ(do_cmp_rm_keys2(oid, Mode::U64, Op::EQ,
                            std::move(empty_cmp),
                            {"key1", "key2"}), 0);

  // Verify all keys were removed
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  ASSERT_EQ(vals.size(), 0);
}

TEST_F(CmpOmap, cmp_rm_keys2_empty_cmp_values_remove_nonexistent)
{
  const std::string oid = __PRETTY_FUNCTION__;
  ASSERT_EQ(ioctx.omap_set(oid, {{"existing", u64_buffer(42)}}), 0);

  // Empty comparison map, try to remove nonexistent key (should succeed as no-op)
  ComparisonMap empty_cmp;
  ASSERT_EQ(do_cmp_rm_keys2(oid, Mode::U64, Op::EQ,
                            std::move(empty_cmp),
                            {"nonexistent"}), 0);

  // Verify existing key still there
  std::map<std::string, bufferlist> vals;
  ASSERT_EQ(get_vals(oid, &vals), 0);
  ASSERT_EQ(vals.size(), 1);
  EXPECT_EQ(vals.count("existing"), 1);
}

} // namespace cls::cmpomap
