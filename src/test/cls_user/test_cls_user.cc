// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "cls/user/cls_user_client.h"
#include "test/librados/test_cxx.h"
#include "gtest/gtest.h"

#include <optional>
#include <system_error>
#include "include/expected.hpp"

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

// test fixture with helper functions
class ClsAccount : public ::testing::Test {
 protected:
  librados::IoCtx& ioctx = RadosEnv::ioctx;

  int add(const std::string& oid, const cls_user_account_resource& entry,
          bool exclusive, uint32_t limit)
  {
    librados::ObjectWriteOperation op;
    cls_user_account_resource_add(op, entry, exclusive, limit);
    return ioctx.operate(oid, &op);
  }

  auto get(const std::string& oid, std::string_view name)
      -> tl::expected<cls_user_account_resource, int>
  {
    librados::ObjectReadOperation op;
    cls_user_account_resource resource;
    int r2 = 0;
    cls_user_account_resource_get(op, name, resource, &r2);

    int r1 = ioctx.operate(oid, &op, nullptr);
    if (r1 < 0) return tl::unexpected(r1);
    if (r2 < 0) return tl::unexpected(r2);
    return resource;
  }

  int rm(const std::string& oid, std::string_view name)
  {
    librados::ObjectWriteOperation op;
    cls_user_account_resource_rm(op, name);
    return ioctx.operate(oid, &op);
  }

  int list(const std::string& oid, std::string_view marker,
           std::string_view path_prefix, uint32_t max_entries,
           std::vector<cls_user_account_resource>& entries, bool& truncated,
           std::string& next_marker, int& ret)
  {
    librados::ObjectReadOperation op;
    cls_user_account_resource_list(op, marker, path_prefix, max_entries,
                                   entries, &truncated, &next_marker, &ret);
    return ioctx.operate(oid, &op, nullptr);
  }

  auto list_all(const std::string& oid,
                std::string_view path_prefix = "",
                uint32_t max_chunk = 1000)
    -> std::vector<cls_user_account_resource>
  {
    std::vector<cls_user_account_resource> all_entries;
    std::string marker;
    bool truncated = true;

    while (truncated) {
      std::vector<cls_user_account_resource> entries;
      std::string next_marker;
      int r2 = 0;
      int r1 = list(oid, marker, path_prefix, max_chunk,
                    entries, truncated, next_marker, r2);
      if (r1 < 0) throw std::system_error(r1, std::system_category());
      if (r2 < 0) throw std::system_error(r2, std::system_category());
      marker = std::move(next_marker);
      std::move(entries.begin(), entries.end(),
                std::back_inserter(all_entries));
    }
    return all_entries;
  }
};

template <typename ...Args>
std::vector<cls_user_account_resource> make_list(Args&& ...args)
{
  return {std::forward<Args>(args)...};
}

bool operator==(const cls_user_account_resource& lhs,
                const cls_user_account_resource& rhs)
{
  if (lhs.name != rhs.name) {
    return false;
  }
  return lhs.path == rhs.path;
  // ignore metadata
}
std::ostream& operator<<(std::ostream& out, const cls_user_account_resource& r)
{
  return out << r.path << r.name;
}

TEST_F(ClsAccount, add)
{
  const std::string oid = __PRETTY_FUNCTION__;
  const auto u1 = cls_user_account_resource{.name = "user1"};
  const auto u2 = cls_user_account_resource{.name = "user2"};
  const auto u3 = cls_user_account_resource{.name = "USER2"};
  EXPECT_EQ(-EUSERS, add(oid, u1, true, 0));
  EXPECT_EQ(0, add(oid, u1, true, 1));
  EXPECT_EQ(-EUSERS, add(oid, u2, true, 1));
  EXPECT_EQ(-EEXIST, add(oid, u1, true, 1));
  EXPECT_EQ(0, add(oid, u1, false, 1)); // allow overwrite at limit
  EXPECT_EQ(0, add(oid, u2, true, 2));
  EXPECT_EQ(-EEXIST, add(oid, u3, true, 2)); // case-insensitive match
}

TEST_F(ClsAccount, get)
{
  const std::string oid = __PRETTY_FUNCTION__;
  const auto u1 = cls_user_account_resource{.name = "user1", .path = "A"};
  const auto u2 = cls_user_account_resource{.name = "USER1"};
  EXPECT_EQ(tl::unexpected(-ENOENT), get(oid, u1.name));
  EXPECT_EQ(-EUSERS, add(oid, u1, true, 0));
  EXPECT_EQ(tl::unexpected(-ENOENT), get(oid, u1.name));
  EXPECT_EQ(0, add(oid, u1, true, 1));
  EXPECT_EQ(u1, get(oid, u1.name));
  EXPECT_EQ(0, add(oid, u2, false, 1)); // overwrite with different case
  EXPECT_EQ(u2, get(oid, u1.name)); // accessible by the original name
}

TEST_F(ClsAccount, rm)
{
  const std::string oid = __PRETTY_FUNCTION__;
  const auto u1 = cls_user_account_resource{.name = "user1"};
  const auto u2 = cls_user_account_resource{.name = "USER1"};
  EXPECT_EQ(-ENOENT, rm(oid, u1.name));
  ASSERT_EQ(0, add(oid, u1, true, 1));
  ASSERT_EQ(0, rm(oid, u1.name));
  EXPECT_EQ(-ENOENT, rm(oid, u1.name));
  ASSERT_EQ(0, add(oid, u1, true, 1));
  ASSERT_EQ(0, rm(oid, u2.name)); // case-insensitive match
}

TEST_F(ClsAccount, list)
{
  const std::string oid = __PRETTY_FUNCTION__;
  const auto u1 = cls_user_account_resource{.name = "user1", .path = ""};
  const auto u2 = cls_user_account_resource{.name = "User2", .path = "A"};
  const auto u3 = cls_user_account_resource{.name = "user3", .path = "AA"};
  const auto u4 = cls_user_account_resource{.name = "User4", .path = ""};
  const auto u5 = cls_user_account_resource{.name = "USER1", .path = "z"};
  constexpr uint32_t max_users = 1024;

  ASSERT_EQ(0, ioctx.create(oid, true));
  ASSERT_EQ(make_list(), list_all(oid));
  ASSERT_EQ(0, add(oid, u1, true, max_users));
  EXPECT_EQ(make_list(u1), list_all(oid));
  ASSERT_EQ(0, add(oid, u2, true, max_users));
  ASSERT_EQ(0, add(oid, u3, true, max_users));
  ASSERT_EQ(0, add(oid, u4, true, max_users));
  EXPECT_EQ(make_list(u1, u2, u3, u4), list_all(oid, ""));
  EXPECT_EQ(make_list(u1, u2, u3, u4), list_all(oid, "", 1)); // paginated
  EXPECT_EQ(make_list(u2, u3), list_all(oid, "A"));
  EXPECT_EQ(make_list(u2, u3), list_all(oid, "A", 1)); // paginated
  EXPECT_EQ(make_list(u3), list_all(oid, "AA"));
  EXPECT_EQ(make_list(u3), list_all(oid, "AA", 1)); // paginated
  EXPECT_EQ(make_list(), list_all(oid, "AAu")); // don't match AAuser3
  ASSERT_EQ(0, rm(oid, u2.name));
  EXPECT_EQ(make_list(u1, u3, u4), list_all(oid, ""));
  EXPECT_EQ(make_list(u1, u3, u4), list_all(oid, "", 1)); // paginated
  ASSERT_EQ(0, add(oid, u5, false, max_users)); // overwrite u1
  EXPECT_EQ(make_list(u5, u3, u4), list_all(oid, ""));
}

// ============================================================================
// USER STORAGE CLASS STATS std::optional TESTS
// ============================================================================

// Helper functions for user header operations
static int write_user_header_test(librados::IoCtx& ioctx, const std::string& oid,
                                   const cls_user_header& header)
{
  librados::ObjectWriteOperation op;
  bufferlist bl;
  encode(header, bl);
  op.omap_set_header(bl);
  return ioctx.operate(oid, &op);
}

static int read_user_header_test(librados::IoCtx& ioctx, const std::string& oid,
                                  cls_user_header& header)
{
  librados::ObjectReadOperation op;
  bufferlist bl;
  int rc;
  
  op.omap_get_header(&bl, &rc);
  
  int ret = ioctx.operate(oid, &op, nullptr);
  if (ret < 0) return ret;
  if (rc < 0) return rc;
  
  if (bl.length() > 0) {
    auto iter = bl.cbegin();
    decode(header, iter);
  }
  
  return 0;
}

// Create a test fixture for user storage class stats
class ClsUserStorageClass : public ::testing::Test {
  static librados::Rados rados;
  static std::string pool_name;
protected:
  static librados::IoCtx ioctx;

  static void SetUpTestCase() {
    pool_name = get_temp_pool_name();
    ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
    ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));
  }
  
  static void TearDownTestCase() {
    ioctx.close();
    ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
  }
};

librados::Rados ClsUserStorageClass::rados;
std::string ClsUserStorageClass::pool_name;
librados::IoCtx ClsUserStorageClass::ioctx;

TEST_F(ClsUserStorageClass, BasicInit) {
  std::string oid = "user.basic";
  
  cls_user_header header;
  header.storage_class_stats = std::unordered_map<std::string, cls_user_stats>();
  
  ASSERT_EQ(0, write_user_header_test(ioctx, oid, header));
  
  cls_user_header read_hdr;
  ASSERT_EQ(0, read_user_header_test(ioctx, oid, read_hdr));
  
  ASSERT_TRUE(read_hdr.storage_class_stats.has_value());
  ASSERT_TRUE(read_hdr.storage_class_stats->empty());
}

TEST_F(ClsUserStorageClass, LegacyToConverted) {
  std::string oid = "user.legacy";
  
  // Start with nullopt (legacy user)
  cls_user_header header;
  header.storage_class_stats = std::nullopt;
  
  ASSERT_EQ(0, write_user_header_test(ioctx, oid, header));
  
  // Verify nullopt
  cls_user_header read_hdr;
  ASSERT_EQ(0, read_user_header_test(ioctx, oid, read_hdr));
  ASSERT_FALSE(read_hdr.storage_class_stats.has_value());
  
  // Convert to has_value
  read_hdr.storage_class_stats = std::unordered_map<std::string, cls_user_stats>();
  cls_user_stats stats;
  stats.total_entries = 10;
  stats.total_bytes = 10240;
  stats.total_bytes_rounded = 10240;
  (*read_hdr.storage_class_stats)["default::STANDARD"] = stats;
  
  ASSERT_EQ(0, write_user_header_test(ioctx, oid, read_hdr));
  
  // Verify converted
  cls_user_header final_hdr;
  ASSERT_EQ(0, read_user_header_test(ioctx, oid, final_hdr));
  ASSERT_TRUE(final_hdr.storage_class_stats.has_value());
  EXPECT_EQ((*final_hdr.storage_class_stats)["default::STANDARD"].total_entries, 10);
}

TEST_F(ClsUserStorageClass, MultipleClasses) {
  std::string oid = "user.multi";
  
  cls_user_header header;
  header.storage_class_stats = std::unordered_map<std::string, cls_user_stats>();
  
  cls_user_stats std_stats;
  std_stats.total_entries = 100;
  std_stats.total_bytes = 102400;
  std_stats.total_bytes_rounded = 102400;
  
  cls_user_stats hdd_stats;
  hdd_stats.total_entries = 200;
  hdd_stats.total_bytes = 204800;
  hdd_stats.total_bytes_rounded = 204800;
  
  (*header.storage_class_stats)["default::STANDARD"] = std_stats;
  (*header.storage_class_stats)["default::HDD"] = hdd_stats;
  
  ASSERT_EQ(0, write_user_header_test(ioctx, oid, header));
  
  cls_user_header read_hdr;
  ASSERT_EQ(0, read_user_header_test(ioctx, oid, read_hdr));
  
  ASSERT_TRUE(read_hdr.storage_class_stats.has_value());
  EXPECT_EQ(read_hdr.storage_class_stats->size(), 2);
  EXPECT_EQ((*read_hdr.storage_class_stats)["default::STANDARD"].total_entries, 100);
  EXPECT_EQ((*read_hdr.storage_class_stats)["default::HDD"].total_entries, 200);
}

