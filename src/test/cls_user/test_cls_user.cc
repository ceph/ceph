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
#include "test/librados/test_pool_types.h"
#include "gtest/gtest.h"

#include <optional>
#include <system_error>
#include "include/expected.hpp"
using ceph::test::PoolType;
using ceph::test::pool_type_name;
using ceph::test::create_pool_by_type;
using ceph::test::destroy_pool_by_type;

// test fixture with helper functions
class TestClsAccount : public ceph::test::ClsTestFixture {
  // Inherits: rados, ioctx, pool_name, pool_type, SetUp(), TearDown()
 protected:
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

TEST_P(TestClsAccount, add)
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

TEST_P(TestClsAccount, get)
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

TEST_P(TestClsAccount, rm)
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

TEST_P(TestClsAccount, list)
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


INSTANTIATE_TEST_SUITE_P(, TestClsAccount,
  ::testing::Values(PoolType::REPLICATED, PoolType::FAST_EC),
  [](const ::testing::TestParamInfo<PoolType>& info) {
  return pool_type_name(info.param);
  }
);

// test fixture for the user header stats
class TestClsUserStats : public ceph::test::ClsTestFixture {
 protected:
  // add 'count' buckets with one 4KiB object each
  int add_buckets(const std::string& oid, unsigned first, unsigned count)
  {
    std::list<cls_user_bucket_entry> entries;
    for (unsigned i = first; i < first + count; i++) {
      cls_user_bucket_entry e;
      e.bucket.name = "bucket" + std::to_string(i);
      e.size = 4096;
      e.size_rounded = 4096;
      e.count = 1;
      entries.push_back(std::move(e));
    }
    librados::ObjectWriteOperation op;
    cls_user_set_buckets(op, entries, true);
    return ioctx.operate(oid, &op);
  }

  int get_header(const std::string& oid, cls_user_header& header)
  {
    librados::ObjectReadOperation op;
    int r2 = 0;
    cls_user_get_header(op, &header, &r2);
    int r1 = ioctx.operate(oid, &op, nullptr);
    if (r1 < 0) return r1;
    return r2;
  }

  int write_header(const std::string& oid, const cls_user_header& header)
  {
    bufferlist bl;
    encode(header, bl);
    return ioctx.omap_set_header(oid, bl);
  }

  // same loop as reset_stats() in rgw/driver/rados/buckets.cc
  int reset_stats(const std::string& oid)
  {
    cls_user_reset_stats2_op call;
    cls_user_reset_stats2_ret ret;
    do {
      bufferlist in, out;
      librados::ObjectWriteOperation op;
      int rval = 0;

      call.time = ceph::real_clock::now();
      ret.update_call(call);

      encode(call, in);
      op.exec(cls::user::method::reset_user_stats2, in, &out, &rval);
      int r = ioctx.operate(oid, &op, librados::OPERATION_RETURNVEC);
      if (r < 0) return r;
      auto p = out.cbegin();
      decode(ret, p);
    } while (ret.truncated);
    return 0;
  }
};

TEST_P(TestClsUserStats, reset_stats_multiple_calls)
{
  const std::string oid = __PRETTY_FUNCTION__;
  // reset_user_stats2 reads up to 1000 bucket entries per call,
  // so 2500 buckets need three calls
  constexpr unsigned num_buckets = 2500;
  for (unsigned i = 0; i < num_buckets; i += 500) {
    ASSERT_EQ(0, add_buckets(oid, i, 500));
  }

  cls_user_header header;
  ASSERT_EQ(0, get_header(oid, header));
  EXPECT_EQ(num_buckets, header.stats.total_entries);

  // clear the header stats, then rebuild them from the bucket entries
  ASSERT_EQ(0, write_header(oid, cls_user_header{}));
  ASSERT_EQ(0, reset_stats(oid));

  ASSERT_EQ(0, get_header(oid, header));
  EXPECT_EQ(num_buckets, header.stats.total_entries);
  EXPECT_EQ(num_buckets * 4096, header.stats.total_bytes);
  EXPECT_EQ(num_buckets * 4096, header.stats.total_bytes_rounded);
}

INSTANTIATE_TEST_SUITE_P(, TestClsUserStats,
  ::testing::Values(PoolType::REPLICATED, PoolType::FAST_EC),
  [](const ::testing::TestParamInfo<PoolType>& info) {
  return pool_type_name(info.param);
  }
);
