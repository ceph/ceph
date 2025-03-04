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
#include "common/errno.h"
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
    bool do_cmp_vals_set_vals(const std::string& oid,
			      Mode mode,
			      Op comparison,
			      const ComparisonMap cmp_pairs,
			      const std::map<std::string, bufferlist> set_pairs,
			      int *p_ret = nullptr,
			      std::string *p_key = nullptr)
    {
      bufferlist err_bl;
      librados::ObjectWriteOperation op;
      int ret = cmp_vals_set_vals(op, mode, comparison, std::move(cmp_pairs),
				  std::move(set_pairs), &err_bl);
      if (ret < 0) {
	return ret;
      }
      ret = ioctx.operate(oid, &op, librados::OPERATION_RETURNVEC);
      if (p_ret) {
	*p_ret = ret;
      }

      if (ret == 0 && err_bl.length() && p_key) {
	try {
	  //std::string key;
	  auto bl_iter = err_bl.cbegin();
	  using ceph::decode;
	  decode(*p_key, bl_iter);
	} catch (buffer::error& err) {
	  std::cerr << __func__ << "::ERROR: unable to decode err_bl" << std::endl;
	}
      }

      return (ret == 0 && err_bl.length() == 0);
    }

  };

  //---------------------------------------------------------------------------
  TEST_F(CmpXattr, cmp_set_vals_str_wrong_val_for_key)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    const std::string key = "key_to_fail";
    bufferlist val1 = string_buffer("val1");
    bufferlist val2 = string_buffer("val2");

    ASSERT_EQ(ioctx.setxattr(oid, key.c_str(), val1), 0);

    std::string failed_key;
    bool success = do_cmp_vals_set_vals(oid, Mode::String, Op::EQ, {{key, val2}}, {{key, val2}},
					nullptr, &failed_key);
    EXPECT_EQ(success, false);
    EXPECT_EQ(key, failed_key);

    {
      std::map<std::string, bufferlist> vals;
      ASSERT_EQ(0, ioctx.getxattrs(oid, vals));
      ASSERT_EQ(vals.size(), 1);
      EXPECT_EQ(val1, vals[key]);
    }
  }

  //---------------------------------------------------------------------------
  TEST_F(CmpXattr, cmp_set_vals_u64_wrong_val_for_key)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    const std::string key = "key_to_fail";
    bufferlist val1 = u64_buffer(17);
    bufferlist val2 = u64_buffer(29);

    ASSERT_EQ(ioctx.setxattr(oid, key.c_str(), val1), 0);

    std::string failed_key;
    bool success = do_cmp_vals_set_vals(oid, Mode::U64, Op::EQ, {{key, val2}}, {{key, val2}},
					nullptr, &failed_key);
    EXPECT_EQ(success, false);
    EXPECT_EQ(key, failed_key);

    {
      std::map<std::string, bufferlist> vals;
      ASSERT_EQ(0, ioctx.getxattrs(oid, vals));
      ASSERT_EQ(vals.size(), 1);
      EXPECT_EQ(val1, vals[key]);
    }
  }

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
    EXPECT_EQ(do_cmp_vals_set_vals(oid, mode, Op::EQ,  {{"eq", val2}},  {{"eq", val2}}),  false);
    EXPECT_EQ(do_cmp_vals_set_vals(oid, mode, Op::NE,  {{"ne", val2}},  {{"ne", val2}}),  true);
    EXPECT_EQ(do_cmp_vals_set_vals(oid, mode, Op::GT,  {{"gt", val2}},  {{"gt", val2}}),  true);
    EXPECT_EQ(do_cmp_vals_set_vals(oid, mode, Op::GTE, {{"gte", val2}}, {{"gte", val2}}), true);
    EXPECT_EQ(do_cmp_vals_set_vals(oid, mode, Op::LT,  {{"lt", val2}},  {{"lt", val2}}),  false);
    EXPECT_EQ(do_cmp_vals_set_vals(oid, mode, Op::LTE, {{"lte", val2}}, {{"lte", val2}}), false);

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
    EXPECT_EQ(do_cmp_vals_set_vals(oid, mode, Op::EQ,  {{"eq", val2}},  {{"eq", val2}}),  false);
    EXPECT_EQ(do_cmp_vals_set_vals(oid, mode, Op::NE,  {{"ne", val2}},  {{"ne", val2}}),  true);
    EXPECT_EQ(do_cmp_vals_set_vals(oid, mode, Op::GT,  {{"gt", val2}},  {{"gt", val2}}),  true);
    EXPECT_EQ(do_cmp_vals_set_vals(oid, mode, Op::GTE, {{"gte", val2}}, {{"gte", val2}}), true);
    EXPECT_EQ(do_cmp_vals_set_vals(oid, mode, Op::LT,  {{"lt", val2}},  {{"lt", val2}}),  false);
    EXPECT_EQ(do_cmp_vals_set_vals(oid, mode, Op::LTE, {{"lte", val2}}, {{"lte", val2}}), false);

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

    EXPECT_EQ(do_cmp_vals_set_vals(oid, Mode::U64, Op::EQ, cmp_pairs, set_pairs), true);
    {
      std::map<std::string, bufferlist> vals;
      ASSERT_EQ(0, ioctx.getxattrs(oid, vals));
      ASSERT_EQ(vals.size(), set_pairs.size());
      for (uint32_t i = 0; i < set_pairs.size(); i++) {
	std::string key = std::to_string(i);
	EXPECT_EQ(set_pairs[key], vals[key]);
      }
    }
  }

  //---------------------------------------------------------------------------
  TEST_F(CmpXattr, cmp_set_vals_u64_existing_set_nonexisting_set)
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
	EXPECT_EQ(set_pairs.contains(key), false);
      }
    }

    EXPECT_EQ(do_cmp_vals_set_vals(oid, Mode::U64, Op::EQ, cmp_pairs, set_pairs), true);
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
  TEST_F(CmpXattr, cmp_set_vals_u64_two_sets)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    ComparisonMap cmp_pairs;
    std::map<std::string, bufferlist> old_pairs;
    std::map<std::string, bufferlist> set_pairs;
    librados::ObjectWriteOperation op;
    for (uint32_t i = 0; i < max_keys; i++) {
      std::string key1 = std::to_string(i);
      std::string key2 = std::to_string(i+max_keys);
      const bufferlist val1 = u64_buffer(i);
      const bufferlist val2 = u64_buffer(i*5);
      const bufferlist val3 = u64_buffer(i*7);
      cmp_pairs.emplace(key1, val1);
      old_pairs.emplace(key2, val2);
      set_pairs.emplace(key2, val3);

      op.setxattr(key1.c_str(), val1);
      op.setxattr(key2.c_str(), val2);
    }
    ASSERT_EQ(0, ioctx.operate(oid, &op));
    {
      std::map<std::string, bufferlist> vals;
      ASSERT_EQ(0, ioctx.getxattrs(oid, vals));
      ASSERT_EQ(vals.size(), cmp_pairs.size() + old_pairs.size());
      for (uint32_t i = 0; i < cmp_pairs.size(); i++) {
	std::string key1 = std::to_string(i);
	std::string key2 = std::to_string(i+max_keys);
	EXPECT_EQ(cmp_pairs[key1], vals[key1]);
	EXPECT_EQ(old_pairs[key2], vals[key2]);
      }
    }

    EXPECT_EQ(do_cmp_vals_set_vals(oid, Mode::U64, Op::EQ, cmp_pairs, set_pairs), true);
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
  TEST_F(CmpXattr, cmp_set_vals_u64_existing_set_nonexisting_set_fail)
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
	EXPECT_EQ(set_pairs.contains(key), false);
      }
    }

    // randomly select one key from the cmp_set and change it val to a different one
    unsigned idx = std::rand() % cmp_pairs.size();
    std::string key_to_fail = std::to_string(idx);
    const bufferlist wrong_val = u64_buffer(idx+1);
    cmp_pairs[key_to_fail] = wrong_val;

    // the cmp will fail so no new values will; be set
    ASSERT_EQ(do_cmp_vals_set_vals(oid, Mode::U64, Op::EQ, cmp_pairs, set_pairs), false);
    // fix the cmp_pairs
    cmp_pairs[key_to_fail] = u64_buffer(idx);

    // make sure nothing was changed
    {
      std::map<std::string, bufferlist> vals;
      ASSERT_EQ(0, ioctx.getxattrs(oid, vals));
      ASSERT_EQ(vals.size(), cmp_pairs.size());
      for (uint32_t i = 0; i < cmp_pairs.size(); i++) {
	std::string key = std::to_string(i);
	EXPECT_EQ(cmp_pairs[key], vals[key]);
	EXPECT_EQ(set_pairs.contains(key), false);
      }
    }
  }

  //---------------------------------------------------------------------------
  TEST_F(CmpXattr, cmp_set_vals_u64_two_sets_fail)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    ComparisonMap cmp_pairs;
    std::map<std::string, bufferlist> old_pairs;
    std::map<std::string, bufferlist> set_pairs;
    librados::ObjectWriteOperation op;
    for (uint32_t i = 0; i < max_keys; i++) {
      std::string key1 = std::to_string(i);
      std::string key2 = std::to_string(i+max_keys);
      const bufferlist val1 = u64_buffer(i);
      const bufferlist val2 = u64_buffer(i*5);
      const bufferlist val3 = u64_buffer(i*7);
      cmp_pairs.emplace(key1, val1);
      old_pairs.emplace(key2, val2);
      set_pairs.emplace(key2, val3);

      op.setxattr(key1.c_str(), val1);
      op.setxattr(key2.c_str(), val2);
    }
    ASSERT_EQ(0, ioctx.operate(oid, &op));
    {
      std::map<std::string, bufferlist> vals;
      ASSERT_EQ(0, ioctx.getxattrs(oid, vals));
      ASSERT_EQ(vals.size(), cmp_pairs.size() + old_pairs.size());
      for (uint32_t i = 0; i < cmp_pairs.size(); i++) {
	std::string key1 = std::to_string(i);
	std::string key2 = std::to_string(i+max_keys);
	EXPECT_EQ(cmp_pairs[key1], vals[key1]);
	EXPECT_EQ(old_pairs[key2], vals[key2]);
      }
    }

    // randomly select one key from the cmp_set and change it val to a different one
    unsigned idx = std::rand() % cmp_pairs.size();
    std::string key_to_fail = std::to_string(idx);
    const bufferlist wrong_val = u64_buffer(idx+1);
    cmp_pairs[key_to_fail] = wrong_val;

    // the cmp will fail so no new values will; be set
    ASSERT_EQ(do_cmp_vals_set_vals(oid, Mode::U64, Op::EQ, cmp_pairs, set_pairs), false);
    // fix the cmp_pairs
    cmp_pairs[key_to_fail] = u64_buffer(idx);

    // make sure nothing was changed
    {
      std::map<std::string, bufferlist> vals;
      ASSERT_EQ(0, ioctx.getxattrs(oid, vals));
      ASSERT_EQ(vals.size(), cmp_pairs.size() + old_pairs.size());
      for (uint32_t i = 0; i < cmp_pairs.size(); i++) {
	std::string key1 = std::to_string(i);
	std::string key2 = std::to_string(i+max_keys);
	EXPECT_EQ(cmp_pairs[key1], vals[key1]);
	EXPECT_EQ(old_pairs[key2], vals[key2]);
      }
    }

  }

  //---------------------------------------------------------------------------
  TEST_F(CmpXattr, cmp_set_vals_u64_non_existing_vec)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    std::map<std::string, bufferlist> old_pairs;
    std::map<std::string, bufferlist> set_pairs;
    librados::ObjectWriteOperation op;
    for (uint32_t i = 0; i < max_keys; i++) {
      std::string key = std::to_string(i);
      const bufferlist val1 = u64_buffer(i);
      const bufferlist val2 = u64_buffer(i*5);
      old_pairs.emplace(key, val1);
      set_pairs.emplace(key, val2);
      op.setxattr(key.c_str(), val1);
    }
    ASSERT_EQ(0, ioctx.operate(oid, &op));

    bufferlist empty_bl;
    {
      std::map<std::string, bufferlist> vals;
      ASSERT_EQ(0, ioctx.getxattrs(oid, vals));
      ASSERT_EQ(vals.size(), old_pairs.size());

      for (uint32_t i = 0; i < old_pairs.size(); i++) {
	std::string key = std::to_string(i);
	EXPECT_EQ(old_pairs[key], vals[key]);

	// CMP_SET exiting key->val against an empty val should fail
	ComparisonMap cmp_pairs = {{key, empty_bl}}; // -17
	ASSERT_EQ(do_cmp_vals_set_vals(oid, Mode::U64, Op::EQ, cmp_pairs, set_pairs), false);
      }
    }

    std::string new_key = std::to_string(0xFFFFFFFF);
    ASSERT_EQ(old_pairs.contains(new_key), false);
    ASSERT_EQ(set_pairs.contains(new_key), false);

    {
      // verify that nothing has changed because all cmp_set should have failed
      std::map<std::string, bufferlist> vals;
      ASSERT_EQ(0, ioctx.getxattrs(oid, vals));
      ASSERT_EQ(vals.size(), old_pairs.size());
      ASSERT_EQ(vals.contains(new_key), false);
      for (uint32_t i = 0; i < old_pairs.size(); i++) {
	std::string key = std::to_string(i);
	EXPECT_EQ(old_pairs[key], vals[key]);
      }
    }

    // TBD: fix U64 compare with an empty value
    ComparisonMap cmp_pairs = {{new_key, empty_bl}};
    ASSERT_EQ(do_cmp_vals_set_vals(oid, Mode::U64, Op::EQ, cmp_pairs, set_pairs), true);
    {
      std::map<std::string, bufferlist> vals;
      ASSERT_EQ(0, ioctx.getxattrs(oid, vals));
      ASSERT_EQ(vals.size(), set_pairs.size());
      for (uint32_t i = 0; i < set_pairs.size(); i++) {
	std::string key = std::to_string(i);
	EXPECT_EQ(set_pairs[key], vals[key]);
      }
    }
  }

  //---------------------------------------------------------------------------
  TEST_F(CmpXattr, cmp_set_vals_str_non_existing_single)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    std::string key1 = "key1";
    std::string key2 = "key2";
    bufferlist val1 = string_buffer("val1");
    bufferlist val2 = string_buffer("val2");
    bufferlist val3 = string_buffer("val3");

    {
      // first set key1->val1
      bufferlist bl;
      ASSERT_EQ(ioctx.setxattr(oid, key1.c_str(), val1), 0);
      ASSERT_EQ(ioctx.getxattr(oid, key1.c_str(), bl), val1.length());
      ASSERT_EQ(bl, val1);
    }

    {
      // comparing key1 with an empty val should fail
      bufferlist empty_bl, bl;
      ComparisonMap cmp_pairs = {{key1, empty_bl}};
      std::map<std::string, bufferlist> set_pairs = {{key1, val2}}; // -17
      ASSERT_EQ(do_cmp_vals_set_vals(oid, Mode::String, Op::EQ, cmp_pairs, set_pairs), false);
      ASSERT_EQ(ioctx.getxattr(oid, key1.c_str(), bl), val1.length());
      ASSERT_EQ(bl, val1);
    }

    {
      // replace val1 with an empty val
      bufferlist empty_bl, bl;
      ComparisonMap cmp_pairs = {{key1, val1}};
      std::map<std::string, bufferlist> set_pairs = {{key1, empty_bl}};
      ASSERT_EQ(do_cmp_vals_set_vals(oid, Mode::String, Op::EQ, cmp_pairs, set_pairs), true);
      EXPECT_EQ(ioctx.getxattr(oid, key1.c_str(), bl), empty_bl.length());
      EXPECT_EQ(bl, empty_bl);
    }

    {
      // now that key1 has an empty val compare against an empty val should succeed
      bufferlist empty_bl, bl;
      ComparisonMap cmp_pairs = {{key1, empty_bl}};
      std::map<std::string, bufferlist> set_pairs = {{key1, val2}};
      ASSERT_EQ(do_cmp_vals_set_vals(oid, Mode::String, Op::EQ, cmp_pairs, set_pairs), true);
      ASSERT_EQ(ioctx.getxattr(oid, key1.c_str(), bl), val2.length());
      ASSERT_EQ(bl, val2);
    }

    {
      // compare non-existing key2 against an empty val should succeed
      bufferlist empty_bl, bl;
      ComparisonMap cmp_pairs = {{key2, empty_bl}};
      std::map<std::string, bufferlist> set_pairs = {{key2, val3}};
      ASSERT_EQ(do_cmp_vals_set_vals(oid, Mode::String, Op::EQ, cmp_pairs, set_pairs), true);

      std::map<std::string, bufferlist> vals;
      ASSERT_EQ(0, ioctx.getxattrs(oid, vals));
      ASSERT_EQ(vals.size(), 2);
      EXPECT_EQ(val2, vals[key1]);
      EXPECT_EQ(val3, vals[key2]);
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
    ASSERT_EQ(do_cmp_vals_set_vals(oid, Mode::String, Op::EQ, {{key, val2}}, {{key, val2}}), false);

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
    ASSERT_EQ(do_cmp_vals_set_vals(oid, Mode::String, Op::EQ, {{key, val2}}, {{key, val2}}), false);

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
    int ret = 0;
    bool success = do_cmp_vals_set_vals(oid, Mode::U64, Op::EQ, {{key, val2}}, {{key, val2}}, &ret);
    ASSERT_EQ(success, false);
    ASSERT_EQ(ret, -EINVAL);
  }

  //---------------------------------------------------------------------------
  TEST_F(CmpXattr, cmp_set_vals_u64_eio)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    const std::string key = "key";
    bufferlist val1 = string_buffer("ccc");
    bufferlist val2 = u64_buffer(17);

    ASSERT_EQ(ioctx.setxattr(oid, key.c_str(), val1), 0);
    int ret = 0;
    bool success = do_cmp_vals_set_vals(oid, Mode::U64, Op::EQ, {{key, val2}}, {{key, val2}}, &ret);
    ASSERT_EQ(success, false);
    ASSERT_EQ(ret, -EIO);

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
    bufferlist err_bl;
    librados::ObjectWriteOperation op;
    EXPECT_EQ(cmp_vals_set_vals(op, Mode::U64, Op::EQ, std::move(cmp_pairs),
				std::move(set_pairs), &err_bl), 0);
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
    bufferlist err_bl;
    librados::ObjectWriteOperation op;
    EXPECT_EQ(cmp_vals_set_vals(op, Mode::U64, Op::EQ, std::move(cmp_pairs),
				std::move(set_pairs), &err_bl), -E2BIG);
  }

  //---------------------------------------------------------------------------
  TEST_F(CmpXattr, cmp_set_vals_empty_pairs)
  {
    const std::string key = "key";
    bufferlist val1 = string_buffer("val1");
    bufferlist val2 = string_buffer("val2");

    ComparisonMap empty_cmp_pairs;
    ComparisonMap cmp_pairs = {{key, val1}};
    std::map<std::string, bufferlist> empty_set_pairs;
    std::map<std::string, bufferlist> set_pairs = {{key, val2}};

    bufferlist err_bl;
    librados::ObjectWriteOperation op;
    EXPECT_EQ(cmp_vals_set_vals(op, Mode::String, Op::EQ, std::move(empty_cmp_pairs),
				std::move(empty_set_pairs), &err_bl), -EINVAL);
    EXPECT_EQ(cmp_vals_set_vals(op, Mode::String, Op::EQ, std::move(empty_cmp_pairs),
				std::move(set_pairs), &err_bl), -EINVAL);
    EXPECT_EQ(cmp_vals_set_vals(op, Mode::String, Op::EQ, std::move(cmp_pairs),
				std::move(empty_set_pairs), &err_bl), -EINVAL);
    EXPECT_EQ(cmp_vals_set_vals(op, Mode::String, Op::EQ, std::move(cmp_pairs),
				std::move(set_pairs), &err_bl), 0);
  }

} // namespace cls::cmpxattr
