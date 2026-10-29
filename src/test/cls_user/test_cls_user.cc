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

// ============================================================================
// USER STORAGE CLASS STATS std::optional TESTS
// ============================================================================

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
  cls_user_get_header(op, &header, nullptr);
  return ioctx.operate(oid, &op, nullptr);
}

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

// Helper: call cls_user_set_buckets() with a single bucket entry
// This exercises cls_user.cc:cls_user_set_buckets_info()
static void set_bucket(librados::IoCtx& ioctx, const std::string& oid,
                       cls_user_bucket_entry& entry, bool add)
{
  std::list<cls_user_bucket_entry> entries = {entry};
  librados::ObjectWriteOperation op;
  cls_user_set_buckets(op, entries, add);
  ASSERT_EQ(0, ioctx.operate(oid, &op));
}

TEST_F(ClsUserStorageClass, LegacyToConverted) {
  std::string oid = "user.legacy-sc";

  // Start with nullopt (legacy user header)
  cls_user_header header;
  header.storage_class_stats = std::nullopt;
  ASSERT_EQ(0, write_user_header_test(ioctx, oid, header));

  // Confirm nullopt
  cls_user_header read_hdr;
  ASSERT_EQ(0, read_user_header_test(ioctx, oid, read_hdr));
  ASSERT_FALSE(read_hdr.storage_class_stats.has_value());

  // add data in user header
  cls_user_bucket_entry entry;
  entry.bucket.name = "test-bucket";
  entry.size = 5120;
  entry.size_rounded = 5120;
  entry.count = 1;
  entry.storage_class_stats = std::unordered_map<std::string, cls_user_bucket_entry>();
  cls_user_bucket_entry sc_entry;
  sc_entry.size = 5120;
  sc_entry.size_rounded = 5120;
  sc_entry.count = 1;
  (*entry.storage_class_stats)["STANDARD"] = sc_entry;

  set_bucket(ioctx, oid, entry, true);

  // ensure storage class still nullopt
  ASSERT_EQ(0, read_user_header_test(ioctx, oid, read_hdr));
  ASSERT_FALSE(read_hdr.storage_class_stats.has_value());
  EXPECT_EQ(read_hdr.stats.total_entries, 1u);

  // removes data from user header and check its storage class initialized
  cls_user_bucket_entry remove_entry;
  remove_entry.bucket.name = "test-bucket";
  remove_entry.size = 0;
  remove_entry.size_rounded = 0;
  remove_entry.count = 0;
  set_bucket(ioctx, oid, remove_entry, false);
  ASSERT_EQ(0, read_user_header_test(ioctx, oid, read_hdr));
  ASSERT_TRUE(read_hdr.storage_class_stats.has_value());
  EXPECT_EQ(read_hdr.stats.total_entries, 0u);

  // re-sync the bucket; its storage-class stats now accumulate
  set_bucket(ioctx, oid, entry, false);

  // storage_class_stats must now be consistent with stats
  ASSERT_EQ(0, read_user_header_test(ioctx, oid, read_hdr));
  EXPECT_TRUE(read_hdr.storage_class_stats->count("STANDARD") > 0);
  EXPECT_EQ((*read_hdr.storage_class_stats)["STANDARD"].total_entries, 1u);
  EXPECT_EQ((*read_hdr.storage_class_stats)["STANDARD"].total_bytes, 5120u);
}

TEST_F(ClsUserStorageClass, LegacyBucketConversion) {
  // nonzero stats + uninitialized storage_class_stats,
  // remove bucket (decrement to zero), add bucket with SC,
  // verify storage_class_stats initialized by that add
  std::string oid = "user.legacy-convert";

  // Add 3 bucket entries to get nonzero stats
  for (int i = 0; i < 3; i++) {
    cls_user_bucket_entry entry;
    entry.bucket.name = "bucket" + std::to_string(i);
    entry.size = 1024;
    entry.size_rounded = 1024;
    entry.count = 1;
    entry.storage_class_stats = std::unordered_map<std::string, cls_user_bucket_entry>();
    cls_user_bucket_entry sc_entry;
    sc_entry.size = 1024;
    sc_entry.size_rounded = 1024;
    sc_entry.count = 1;
    (*entry.storage_class_stats)["STANDARD"] = sc_entry;
    set_bucket(ioctx, oid, entry, true);
  }

  // Verify nonzero stats, then clear storage_class_stats (legacy simulation)
  cls_user_header header;
  ASSERT_EQ(0, read_user_header_test(ioctx, oid, header));
  ASSERT_GT(header.stats.total_entries, 0u);
  header.storage_class_stats = std::nullopt;
  ASSERT_EQ(0, write_user_header_test(ioctx, oid, header));

  // Confirm: nonzero stats, nullopt storage_class_stats
  ASSERT_EQ(0, read_user_header_test(ioctx, oid, header));
  ASSERT_FALSE(header.storage_class_stats.has_value());
  ASSERT_EQ(header.stats.total_entries, 3u);

  // remove data
  for (int i = 0; i < 3; i++) {
    cls_user_bucket_entry entry;
    entry.bucket.name = "bucket" + std::to_string(i);
    entry.size = 0;
    entry.size_rounded = 0;
    entry.count = 0;
    set_bucket(ioctx, oid, entry, false);
  }
  ASSERT_EQ(0, read_user_header_test(ioctx, oid, header));
  EXPECT_EQ(header.stats.total_entries, 0u);
  ASSERT_TRUE(header.storage_class_stats.has_value());

  // Now add a new bucket with a different storage class
  cls_user_bucket_entry new_entry;
  new_entry.bucket.name = "new-bucket";
  new_entry.size = 2048;
  new_entry.size_rounded = 2048;
  new_entry.count = 1;
  new_entry.storage_class_stats = std::unordered_map<std::string, cls_user_bucket_entry>();
  cls_user_bucket_entry hdd_entry;
  hdd_entry.size = 2048;
  hdd_entry.size_rounded = 2048;
  hdd_entry.count = 1;
  (*new_entry.storage_class_stats)["HDD"] = hdd_entry;
  set_bucket(ioctx, oid, new_entry, true);

  // storage_class_stats must be initialized by that add
  ASSERT_EQ(0, read_user_header_test(ioctx, oid, header));
  ASSERT_TRUE(header.storage_class_stats.has_value());
  EXPECT_TRUE(header.storage_class_stats->count("HDD") > 0);
  EXPECT_EQ((*header.storage_class_stats)["HDD"].total_entries, 1u);
  EXPECT_EQ((*header.storage_class_stats)["HDD"].total_bytes, 2048u);
}

TEST_F(ClsUserStorageClass, MultipleClasses) {
  std::string oid = "user.multi-sc";

  cls_user_header header;
  header.storage_class_stats.emplace();
  ASSERT_EQ(0, write_user_header_test(ioctx, oid, header));

  // Add buckets with different storage classes via cls_user_set_buckets()
  struct { const char* sc; const char* bucket; uint64_t size; } data[] = {
    {"STANDARD", "bucket-std", 1024},
    {"HDD",      "bucket-hdd", 2048},
  };

  for (auto& d : data) {
    cls_user_bucket_entry entry;
    entry.bucket.name = d.bucket;
    entry.size = d.size;
    entry.size_rounded = d.size;
    entry.count = 1;
    entry.storage_class_stats = std::unordered_map<std::string, cls_user_bucket_entry>();
    cls_user_bucket_entry sc_entry;
    sc_entry.size = d.size;
    sc_entry.size_rounded = d.size;
    sc_entry.count = 1;
    (*entry.storage_class_stats)[d.sc] = sc_entry;
    set_bucket(ioctx, oid, entry, true);
  }


  ASSERT_EQ(0, read_user_header_test(ioctx, oid, header));
  ASSERT_TRUE(header.storage_class_stats.has_value());
  EXPECT_EQ(header.storage_class_stats->size(), 2u);
  EXPECT_EQ((*header.storage_class_stats)["STANDARD"].total_entries, 1u);
  EXPECT_EQ((*header.storage_class_stats)["STANDARD"].total_bytes, 1024u);
  EXPECT_EQ((*header.storage_class_stats)["HDD"].total_entries, 1u);
  EXPECT_EQ((*header.storage_class_stats)["HDD"].total_bytes, 2048u);
}

static void add_buckets_with_sc(librados::IoCtx& ioctx, const std::string& oid,
                                int num_buckets, int legacy_index)
{
  int chunk = 250;
  std::string classes[] = {"STANDARD", "HDD"};
  for (int base = 0; base < num_buckets; base += chunk) {
    std::list<cls_user_bucket_entry> entries;
    for (int i = base; i < base + chunk && i < num_buckets; i++) {
      cls_user_bucket_entry entry;
      entry.bucket.name = "bucket-" + std::to_string(i);
      entry.size = 1024;
      entry.size_rounded = 1024;
      entry.count = 1;
      if (i == legacy_index) {
        entry.storage_class_stats = std::nullopt;
      } else {
        entry.storage_class_stats.emplace();
        cls_user_bucket_entry sc_entry;
        sc_entry.size = 1024;
        sc_entry.size_rounded = 1024;
        sc_entry.count = 1;
        (*entry.storage_class_stats)[classes[i%2]] = sc_entry;
      }
      entries.push_back(entry);
    }
    librados::ObjectWriteOperation op;
    cls_user_set_buckets(op, entries, true);
    ASSERT_EQ(0, ioctx.operate(oid, &op));
  }
}

// emulate the reset_stats method in src/rgw/driver/rados/buckets.cc,
static void reset_user_stats_test(librados::IoCtx& ioctx, const std::string& oid)
{
  cls_user_reset_stats2_op call;
  cls_user_reset_stats2_ret ret;
  ret.storage_class_stats.emplace();
  do {
    call.time = ceph::real_clock::now();
    ret.update_call(call);
    bufferlist in, out;
    encode(call, in);
    librados::ObjectWriteOperation op;
    int rval;
    op.exec(cls::user::method::reset_user_stats2, in, &out, &rval);
    ASSERT_EQ(0, ioctx.operate(oid, &op, librados::OPERATION_RETURNVEC));
    try {
      auto bliter = out.cbegin();
      decode(ret, bliter);
    } catch (ceph::buffer::error& err) {
      FAIL() << "failed to decode cls_user_reset_stats2_ret";
    }
  } while (ret.truncated);
}

TEST_F(ClsUserStorageClass, ResetStatsPagination) {
  std::string oid = "user.reset-sc-pagination";

  int num_buckets = 1005;
  add_buckets_with_sc(ioctx, oid, num_buckets, -1); // no legacy buckets
  reset_user_stats_test(ioctx, oid);

  cls_user_header header;
  ASSERT_EQ(0, read_user_header_test(ioctx, oid, header));
  ASSERT_TRUE(header.storage_class_stats.has_value());
  EXPECT_EQ(header.storage_class_stats->size(), 2u);
  // 503 even-indexed buckets in STANDARD, 502 odd-indexed in HDD
  EXPECT_EQ((*header.storage_class_stats)["STANDARD"].total_entries, 503u);
  EXPECT_EQ((*header.storage_class_stats)["STANDARD"].total_bytes, 503u * 1024u);
  EXPECT_EQ((*header.storage_class_stats)["HDD"].total_entries, 502u);
  EXPECT_EQ((*header.storage_class_stats)["HDD"].total_bytes, 502u * 1024u);
  EXPECT_EQ(header.stats.total_entries, 1005u);
}

TEST_F(ClsUserStorageClass, ResetStatsPaginationLegacyFirstPage) {
  std::string oid = "user.reset-sc-legacy-first";

  int num_buckets = 1003;
  add_buckets_with_sc(ioctx, oid, num_buckets, 0); // legacy is on the first page

  reset_user_stats_test(ioctx, oid);

  cls_user_header header;
  ASSERT_EQ(0, read_user_header_test(ioctx, oid, header));
  EXPECT_EQ(header.stats.total_entries, 1003u);
  // should be empty as a legacy bucket exists
  ASSERT_FALSE(header.storage_class_stats.has_value());
}


TEST_F(ClsUserStorageClass, ResetStatsPaginationLegacyLastPage) {
  std::string oid = "user.reset-sc-legacy-last";

  int num_buckets = 1002;
  add_buckets_with_sc(ioctx, oid, num_buckets, num_buckets - 1); // legacy on last page
  reset_user_stats_test(ioctx, oid);

  cls_user_header header;
  ASSERT_EQ(0, read_user_header_test(ioctx, oid, header));
  EXPECT_EQ(header.stats.total_entries, 1002u);
  // should be empty as a legacy bucket exists
  ASSERT_FALSE(header.storage_class_stats.has_value());
}

// bucket entry with all objects in one storage class
static cls_user_bucket_entry sc_bucket_entry(const std::string& name,
                                             const std::string& storage_class,
                                             uint64_t count, uint64_t size)
{
  cls_user_bucket_entry entry;
  entry.bucket.name = name;
  entry.count = count;
  entry.size = size;
  entry.size_rounded = size;
  entry.storage_class_stats.emplace();
  cls_user_bucket_entry& sc = (*entry.storage_class_stats)[storage_class];
  sc.count = count;
  sc.size = size;
  sc.size_rounded = size;
  return entry;
}

// bucket entry from an older client: no per-storage-class stats
static cls_user_bucket_entry legacy_bucket_entry(const std::string& name,
                                                 uint64_t count, uint64_t size)
{
  cls_user_bucket_entry entry;
  entry.bucket.name = name;
  entry.count = count;
  entry.size = size;
  entry.size_rounded = size;
  entry.storage_class_stats = std::nullopt;
  return entry;
}

TEST_F(ClsUserStorageClass, OldClientSyncMakesScUnknown) {
  std::string oid = "user.old-client-sync";
  cls_user_header header;
  header.storage_class_stats.emplace();
  ASSERT_EQ(0, write_user_header_test(ioctx, oid, header));

  auto entry = sc_bucket_entry("bucket", "COLD", 3, 3072);
  set_bucket(ioctx, oid, entry, true); // link and sync
  ASSERT_EQ(0, read_user_header_test(ioctx, oid, header));
  ASSERT_TRUE(header.storage_class_stats.has_value());
  EXPECT_EQ((*header.storage_class_stats)["COLD"].total_entries, 3u);

  // an older client syncs the same bucket without per-storage-class stats
  auto legacy = legacy_bucket_entry("bucket", 3, 3072);
  set_bucket(ioctx, oid, legacy, false);

  // totals stay right, the per-storage-class stats become unknown (not zero)
  ASSERT_EQ(0, read_user_header_test(ioctx, oid, header));
  EXPECT_EQ(header.stats.total_entries, 3u);
  EXPECT_EQ(header.stats.total_bytes, 3072u);
  EXPECT_FALSE(header.storage_class_stats.has_value());
}

TEST_F(ClsUserStorageClass, NewUserOldClientSync) {
  // user created during an upgrade: empty header, bucket written and synced
  // by an older client
  std::string oid = "user.new-user-old-client";
  cls_user_bucket_entry link; // as rgwrados::buckets::add() sends it
  link.bucket.name = "bucket";
  set_bucket(ioctx, oid, link, true);

  cls_user_header header;
  ASSERT_EQ(0, read_user_header_test(ioctx, oid, header));
  ASSERT_TRUE(header.storage_class_stats.has_value()); // no objects yet

  auto legacy = legacy_bucket_entry("bucket", 15, 15360);
  set_bucket(ioctx, oid, legacy, false);

  ASSERT_EQ(0, read_user_header_test(ioctx, oid, header));
  EXPECT_EQ(header.stats.total_entries, 15u);
  EXPECT_FALSE(header.storage_class_stats.has_value());
}

TEST_F(ClsUserStorageClass, LinkKeepsSc) {
  std::string oid = "user.link-keeps-sc";
  cls_user_header header;
  header.storage_class_stats.emplace();
  ASSERT_EQ(0, write_user_header_test(ioctx, oid, header));

  auto entry = sc_bucket_entry("bucket1", "COLD", 2, 2048);
  set_bucket(ioctx, oid, entry, true); // link and sync

  // link a new, empty bucket, as rgwrados::buckets::add() does
  cls_user_bucket_entry link;
  link.bucket.name = "bucket2";
  set_bucket(ioctx, oid, link, true);

  // link the existing bucket again (e.g. radosgw-admin bucket link)
  cls_user_bucket_entry relink;
  relink.bucket.name = "bucket1";
  set_bucket(ioctx, oid, relink, true);

  ASSERT_EQ(0, read_user_header_test(ioctx, oid, header));
  EXPECT_EQ(header.stats.total_entries, 2u);
  ASSERT_TRUE(header.storage_class_stats.has_value());
  EXPECT_EQ((*header.storage_class_stats)["COLD"].total_entries, 2u);
  EXPECT_EQ((*header.storage_class_stats)["COLD"].total_bytes, 2048u);
}

TEST_F(ClsUserStorageClass, ResetStats2EmptyLinkedBucket) {
  // an empty bucket that was just linked has no per-storage-class stats, but
  // it must not make them unknown
  std::string oid = "user.reset2-empty-linked";
  add_buckets_with_sc(ioctx, oid, 10, -1);
  cls_user_bucket_entry link;
  link.bucket.name = "bucket-empty";
  set_bucket(ioctx, oid, link, true);
  ASSERT_EQ(0, write_user_header_test(ioctx, oid, cls_user_header{}));

  reset_user_stats_test(ioctx, oid);

  cls_user_header header;
  ASSERT_EQ(0, read_user_header_test(ioctx, oid, header));
  EXPECT_EQ(header.stats.total_entries, 10u);
  ASSERT_TRUE(header.storage_class_stats.has_value());
  EXPECT_EQ((*header.storage_class_stats)["STANDARD"].total_entries, 5u);
  EXPECT_EQ((*header.storage_class_stats)["HDD"].total_entries, 5u);
}

TEST_F(ClsUserStorageClass, ResetStatsV1) {
  // the older reset_user_stats method, with an empty linked bucket
  std::string oid = "user.reset-sc-v1";
  add_buckets_with_sc(ioctx, oid, 10, -1);
  cls_user_bucket_entry link;
  link.bucket.name = "bucket-empty";
  set_bucket(ioctx, oid, link, true);
  ASSERT_EQ(0, write_user_header_test(ioctx, oid, cls_user_header{}));

  librados::ObjectWriteOperation op;
  cls_user_reset_stats(op);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  cls_user_header header;
  ASSERT_EQ(0, read_user_header_test(ioctx, oid, header));
  EXPECT_EQ(header.stats.total_entries, 10u);
  ASSERT_TRUE(header.storage_class_stats.has_value());
  EXPECT_EQ((*header.storage_class_stats)["STANDARD"].total_entries, 5u);
  EXPECT_EQ((*header.storage_class_stats)["HDD"].total_entries, 5u);
}

TEST_F(ClsUserStorageClass, ResetStatsThreeCalls) {
  // 2500 buckets need three reset_user_stats2 calls (up to 1000 entries
  // each). an empty bucket that was just linked has no per-storage-class
  // stats, but it must not make them unknown
  std::string oid = "user.reset-sc-three-calls";
  add_buckets_with_sc(ioctx, oid, 2500, -1);
  cls_user_bucket_entry link;
  link.bucket.name = "bucket-empty";
  set_bucket(ioctx, oid, link, true);

  // clear the header, then rebuild it
  ASSERT_EQ(0, write_user_header_test(ioctx, oid, cls_user_header{}));
  reset_user_stats_test(ioctx, oid);

  cls_user_header header;
  ASSERT_EQ(0, read_user_header_test(ioctx, oid, header));
  EXPECT_EQ(header.stats.total_entries, 2500u);
  EXPECT_EQ(header.stats.total_bytes, 2500u * 1024u);
  ASSERT_TRUE(header.storage_class_stats.has_value());
  EXPECT_EQ((*header.storage_class_stats)["STANDARD"].total_entries, 1250u);
  EXPECT_EQ((*header.storage_class_stats)["HDD"].total_entries, 1250u);
  EXPECT_EQ((*header.storage_class_stats)["HDD"].total_bytes, 1250u * 1024u);
}

TEST_F(ClsUserStorageClass, RemoveBucketDecrementsSc) {
  // e.g. radosgw-admin bucket unlink of a bucket with objects
  std::string oid = "user.remove-bucket-sc";
  cls_user_header header;
  header.storage_class_stats.emplace();
  ASSERT_EQ(0, write_user_header_test(ioctx, oid, header));

  auto b1 = sc_bucket_entry("bucket1", "COLD", 3, 3072);
  auto b2 = sc_bucket_entry("bucket2", "STANDARD", 2, 2048);
  set_bucket(ioctx, oid, b1, true);
  set_bucket(ioctx, oid, b2, true);

  librados::ObjectWriteOperation op;
  cls_user_remove_bucket(op, b1.bucket);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  ASSERT_EQ(0, read_user_header_test(ioctx, oid, header));
  EXPECT_EQ(header.stats.total_entries, 2u);
  ASSERT_TRUE(header.storage_class_stats.has_value());
  EXPECT_EQ((*header.storage_class_stats)["COLD"].total_entries, 0u);
  EXPECT_EQ((*header.storage_class_stats)["COLD"].total_bytes, 0u);
  EXPECT_EQ((*header.storage_class_stats)["STANDARD"].total_entries, 2u);

  // link and sync it again: no double counting
  set_bucket(ioctx, oid, b1, true);
  ASSERT_EQ(0, read_user_header_test(ioctx, oid, header));
  EXPECT_EQ(header.stats.total_entries, 5u);
  EXPECT_EQ((*header.storage_class_stats)["COLD"].total_entries, 3u);
  EXPECT_EQ((*header.storage_class_stats)["STANDARD"].total_entries, 2u);
}
