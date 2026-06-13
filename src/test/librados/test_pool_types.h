// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <string>
#include "include/rados/librados.hpp"
#include "test/librados/test_cxx.h"
#include "gtest/gtest.h"

namespace ceph {
namespace test {

// Pool type enumeration for parameterized tests
enum class PoolType {
  REPLICATED,
  FAST_EC,
  LEGACY_EC
};

// Convert pool type to string for test naming
inline std::string pool_type_name(PoolType type) {
  switch (type) {
    case PoolType::REPLICATED:
      return "Replicated";
    case PoolType::FAST_EC:
      return "FastEC";
    case PoolType::LEGACY_EC:
      return "LegacyEC";
    default:
      return "Unknown";
  }
}

// Create pool based on type
inline std::string create_pool_by_type(
    const std::string& pool_name,
    librados::Rados& cluster,
    PoolType type) {
  switch (type) {
    case PoolType::REPLICATED:
      return create_one_pool_pp(pool_name, cluster);
    case PoolType::FAST_EC: {
      std::string result = create_one_ec_pool_pp(pool_name, cluster, true, true);
      if (result != "") {
        return result;
      }
      result = set_allow_ec_overwrites_pp(pool_name, cluster, true);
      cluster.wait_for_latest_osdmap();
      return result;
    }
    case PoolType::LEGACY_EC:
      return create_one_ec_pool_pp(pool_name, cluster, false, false);
    default:
      return "Unknown pool type";
  }
}

// Destroy pool based on type
inline int destroy_pool_by_type(
    const std::string& pool_name,
    librados::Rados& cluster,
    PoolType type) {
  switch (type) {
    case PoolType::REPLICATED:
      return destroy_one_pool_pp(pool_name, cluster);
    case PoolType::FAST_EC:
    case PoolType::LEGACY_EC:
      return destroy_one_ec_pool_pp(pool_name, cluster);
    default:
      return -EINVAL;
  }
}

// Generic base class for parameterized pool type tests
// Can be used for any test that needs to run on multiple pool types
class PoolTypeTestFixture : public ::testing::TestWithParam<PoolType> {
 protected:
  static librados::Rados rados;
  librados::IoCtx ioctx;
  std::string pool_name;
  PoolType pool_type;

  void SetUp() override {
    pool_type = GetParam();
    pool_name = get_temp_pool_name();
    ASSERT_EQ("", create_pool_by_type(pool_name, rados, pool_type));
    ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));
  }
  
  void TearDown() override {
    ioctx.close();
    ASSERT_EQ(0, destroy_pool_by_type(pool_name, rados, pool_type));
  }
};

// Base class for EC-only tests
class ECOnlyTestFixture : public ::testing::Test {
 protected:
  static librados::Rados rados;
  librados::IoCtx ioctx;
  std::string pool_name;

  void SetUp() override {
    pool_name = get_temp_pool_name();
    ASSERT_EQ("", create_pool_by_type(pool_name, rados, PoolType::FAST_EC));
    ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));
  }
  
  void TearDown() override {
    ioctx.close();
    ASSERT_EQ(0, destroy_pool_by_type(pool_name, rados, PoolType::FAST_EC));
  }
};

} // namespace test
} // namespace ceph
