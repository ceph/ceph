// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <chrono>
#include <map>
#include <string>
#include <vector>

#include "include/rados/librados.hpp"
#include "include/scope_guard.h"
#include "test/librados/test_cxx.h"
#include "gtest/gtest.h"

namespace ceph::messaging::osd {
struct OSDMapReply;
}

namespace ceph::test {

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
      return create_pool_pp(pool_name, cluster);
    case PoolType::FAST_EC: {
      std::string result = create_ec_pool_pp(pool_name, cluster, true);
      if (result != "") {
        return result;
      }
      result = set_allow_ec_overwrites_pp(pool_name, cluster, true);
      cluster.wait_for_latest_osdmap();
      return result;
    }
    case PoolType::LEGACY_EC:
      return create_ec_pool_pp(pool_name, cluster, false);
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
      return destroy_pool_pp(pool_name, cluster);
    case PoolType::FAST_EC:
    case PoolType::LEGACY_EC:
      return destroy_ec_pool_pp(pool_name, cluster);
    default:
      return -EINVAL;
  }
}

// Generic base class for parameterized pool type tests
// Can be used for any test that needs to run on multiple pool types
class PoolTypeTestFixture : public ::testing::TestWithParam<PoolType> {
 protected:
  static librados::Rados rados;
  static std::map<PoolType, std::string> pool_names;
  librados::IoCtx ioctx;
  std::string pool_name;
  std::string nspace;
  PoolType pool_type;

  static std::vector<PoolType> get_supported_pool_types() {
    return {PoolType::REPLICATED, PoolType::FAST_EC};
  }

  static std::string pool_name_prefix() {
    return "pool_type_test_";
  }

  static void after_pool_create(PoolType type,
                                const std::string& pool_name,
                                librados::Rados& cluster) {
  }

  static void cleanup_namespace(librados::Rados& cluster,
                                librados::IoCtx& ioctx,
                                const std::string& ns);

  void turn_balancing_off();
  void turn_balancing_on();

  int request_osd_map(
      const std::string& oid,
      ceph::messaging::osd::OSDMapReply* reply);

  int set_osd_upmap(
      const std::string& pgid,
      const std::vector<int>& up_osds);

  int wait_for_upmap(
      const std::string& oid,
      int desired_primary,
      std::chrono::seconds timeout);

  void print_osd_map(const std::string& message, const std::vector<int>& osd_vec);

  void setup_and_trigger_recovery(
      const std::string& oid,
      int& new_primary,
      std::chrono::seconds timeout = std::chrono::seconds(30));

  static void SetUpTestSuite();
  static void TearDownTestSuite();
  void SetUp() override;
  void TearDown() override;

  bool balancing_disabled = false;
};

using ClsTestFixture = PoolTypeTestFixture;

// Base class for EC-only tests
class ECOnlyTestFixture : public ::testing::Test {
 protected:
  static librados::Rados rados;
  static std::string static_pool_name;
  librados::IoCtx ioctx;
  std::string pool_name;
  std::string nspace;

  static std::string pool_name_prefix() {
    return "ec_only_test_";
  }

  static void after_pool_create(const std::string& pool_name,
                                librados::Rados& cluster) {
  }

  static void cleanup_namespace(librados::Rados& cluster,
                                librados::IoCtx& ioctx,
                                const std::string& ns) {
    ioctx.snap_set_read(librados::SNAP_HEAD);
    ioctx.set_namespace(ns);

    int tries = 20;
    while (--tries) {
      int got_enoent = 0;
      for (librados::NObjectIterator it = ioctx.nobjects_begin();
           it != ioctx.nobjects_end(); ++it) {
        ioctx.locator_set_key(it->get_locator());
        librados::ObjectWriteOperation op;
        op.remove();
        librados::AioCompletion* completion = cluster.aio_create_completion();
        auto sg = make_scope_guard([&] { completion->release(); });
        ASSERT_EQ(0, ioctx.aio_operate(it->get_oid(), completion, &op,
                                       librados::OPERATION_IGNORE_CACHE));
        completion->wait_for_complete();
        if (completion->get_return_value() == -ENOENT) {
          ++got_enoent;
        } else {
          ASSERT_EQ(0, completion->get_return_value());
        }
      }
      if (!got_enoent) {
        break;
      }
      sleep(1);
    }
  }

  static void SetUpTestSuite() {
    ASSERT_EQ("", connect_cluster_pp(rados));

    static_pool_name = get_temp_pool_name(
      pool_name_prefix() + pool_type_name(PoolType::FAST_EC) + "_");
    ASSERT_EQ("", create_pool_by_type(static_pool_name, rados, PoolType::FAST_EC));
    after_pool_create(static_pool_name, rados);
  }

  static void TearDownTestSuite() {
    ASSERT_EQ(0, destroy_pool_by_type(static_pool_name, rados, PoolType::FAST_EC));
    static_pool_name.clear();
    rados.shutdown();
  }

  void SetUp() override;
  void TearDown() override;
};

using ClsTestFixtureEC = ECOnlyTestFixture;

} // namespace ceph::test
