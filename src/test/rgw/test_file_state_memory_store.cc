// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "file_state/change_feed.h"
#include "file_state/memory_store.h"
#include "rgw_s3files_errors.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <thread>
#include <unordered_map>

#include <gtest/gtest.h>

using namespace rgw::file_state;
using rgw::s3files::ERR_INVALID_BUCKET_ARN;
using rgw::s3files::ERR_INVALID_ROLE_ARN;
using rgw::s3files::ERR_INVALID_POLICY_DOCUMENT;
using rgw::s3files::ERR_INVALID_POSIX_USER;
using rgw::s3files::ERR_BUCKET_ALREADY_IN_USE;
using rgw::s3files::ERR_FILE_SYSTEM_NOT_FOUND;
using rgw::s3files::ERR_FILE_SYSTEM_HAS_CHILDREN;
using rgw::s3files::ERR_FILE_SYSTEM_IN_INVALID_STATE;
using rgw::s3files::ERR_ACCESS_POINT_NOT_FOUND;
using rgw::s3files::ERR_MOUNT_TARGET_NOT_FOUND;
using rgw::s3files::ERR_MOUNT_TARGET_ALREADY_IN_ZONE;
using rgw::s3files::ERR_POLICY_NOT_FOUND;
using rgw::s3files::ERR_INVALID_ZONE_ID;

namespace {

constexpr std::string_view kAccount = "1234567890";
constexpr std::string_view kBucket  = "arn:aws:s3:::test-bucket";
constexpr std::string_view kRole    = "arn:aws:iam::1234567890:role/test-role";
constexpr std::string_view kZone    = "00000000000000000000000000000001";

CreateFileSystemRequest minimum_fs_req() {
  CreateFileSystemRequest req;
  req.owner_account_id = kAccount;
  req.bucket_arn = kBucket;
  req.role_arn = kRole;
  return req;
}

}  // namespace

// =================================================================
// FileSystem
// =================================================================

TEST(MemoryStore, CreateFileSystem_Minimum) {
  MemoryStore s;
  auto r = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(r)) << error(r).error_code;
  const auto& fs = value(r);
  EXPECT_FALSE(fs.spec.id.empty());
  EXPECT_EQ(fs.spec.bucket_arn, kBucket);
  EXPECT_EQ(fs.status.state, LifecycleState::Available);
  EXPECT_TRUE(fs.spec.arn.find(fs.spec.id) != std::string::npos);
}

TEST(MemoryStore, CreateFileSystem_MissingBucket) {
  MemoryStore s;
  auto req = minimum_fs_req();
  req.bucket_arn.clear();
  auto r = s.create_file_system(req);
  ASSERT_FALSE(ok(r));
  EXPECT_EQ(error(r).kind, StoreError::Kind::InvalidArgument);
  EXPECT_EQ(error(r).error_code, ERR_INVALID_BUCKET_ARN);
}

TEST(MemoryStore, CreateFileSystem_MissingRoleArn) {
  MemoryStore s;
  auto req = minimum_fs_req();
  req.role_arn.clear();
  auto r = s.create_file_system(req);
  ASSERT_FALSE(ok(r));
  EXPECT_EQ(error(r).error_code, ERR_INVALID_ROLE_ARN);
}

TEST(MemoryStore, CreateFileSystem_BucketAlreadyInUse) {
  MemoryStore s;
  ASSERT_TRUE(ok(s.create_file_system(minimum_fs_req())));
  auto r = s.create_file_system(minimum_fs_req());
  ASSERT_FALSE(ok(r));
  EXPECT_EQ(error(r).kind, StoreError::Kind::Conflict);
  EXPECT_EQ(error(r).error_code, ERR_BUCKET_ALREADY_IN_USE);
}

TEST(MemoryStore, CreateFileSystem_IdempotentClientToken) {
  MemoryStore s;
  auto req = minimum_fs_req();
  req.client_token = "tok-1";
  auto a = s.create_file_system(req);
  ASSERT_TRUE(ok(a));
  auto b = s.create_file_system(req);
  ASSERT_TRUE(ok(b));
  EXPECT_EQ(value(a).spec.id, value(b).spec.id);
}

TEST(MemoryStore, GetFileSystem_NotFound) {
  MemoryStore s;
  auto r = s.get_file_system(kAccount, "fs-doesnotexist");
  ASSERT_FALSE(ok(r));
  EXPECT_EQ(error(r).kind, StoreError::Kind::NotFound);
  EXPECT_EQ(error(r).error_code, ERR_FILE_SYSTEM_NOT_FOUND);
}

TEST(MemoryStore, GetFileSystem_WrongAccount) {
  MemoryStore s;
  auto created = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(created));
  auto r = s.get_file_system("9999999999", value(created).spec.id);
  ASSERT_FALSE(ok(r));
  EXPECT_EQ(error(r).error_code, ERR_FILE_SYSTEM_NOT_FOUND);
}

TEST(MemoryStore, ListFileSystems_FiltersByAccount) {
  MemoryStore s;
  auto a = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(a));

  auto req2 = minimum_fs_req();
  req2.owner_account_id = "9999999999";
  req2.bucket_arn = "arn:aws:s3:::other-bucket";
  ASSERT_TRUE(ok(s.create_file_system(req2)));

  auto r = s.list_file_systems(kAccount, ListOptions{});
  ASSERT_TRUE(ok(r));
  EXPECT_EQ(value(r).items.size(), 1u);
  EXPECT_EQ(value(r).items.front().spec.id, value(a).spec.id);
}

TEST(MemoryStore, DeleteFileSystem_RoundTrip) {
  MemoryStore s;
  auto fs = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(fs));
  ASSERT_TRUE(ok(s.delete_file_system(kAccount, value(fs).spec.id)));
  auto r = s.get_file_system(kAccount, value(fs).spec.id);
  ASSERT_FALSE(ok(r));
  EXPECT_EQ(error(r).error_code, ERR_FILE_SYSTEM_NOT_FOUND);
}

TEST(MemoryStore, DeleteFileSystem_NotFound) {
  MemoryStore s;
  auto r = s.delete_file_system(kAccount, "fs-doesnotexist");
  ASSERT_FALSE(ok(r));
  EXPECT_EQ(error(r).error_code, ERR_FILE_SYSTEM_NOT_FOUND);
}

// =================================================================
// FileSystem policy
// =================================================================

TEST(MemoryStore, FileSystemPolicy_PutGetDelete) {
  MemoryStore s;
  auto fs = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(fs));
  const auto& fs_id = value(fs).spec.id;

  // No policy initially.
  auto r1 = s.get_file_system_policy(kAccount, fs_id);
  ASSERT_FALSE(ok(r1));
  EXPECT_EQ(error(r1).error_code, ERR_POLICY_NOT_FOUND);

  // Put.
  std::string policy = R"({"Version":"2012-10-17"})";
  ASSERT_TRUE(ok(s.put_file_system_policy(kAccount, fs_id, policy)));

  // Get returns the same.
  auto r2 = s.get_file_system_policy(kAccount, fs_id);
  ASSERT_TRUE(ok(r2));
  EXPECT_EQ(value(r2), policy);

  // Delete.
  ASSERT_TRUE(ok(s.delete_file_system_policy(kAccount, fs_id)));

  // Now NotFound again.
  auto r3 = s.get_file_system_policy(kAccount, fs_id);
  ASSERT_FALSE(ok(r3));
  EXPECT_EQ(error(r3).error_code, ERR_POLICY_NOT_FOUND);
}

TEST(MemoryStore, FileSystemPolicy_InvalidJson) {
  MemoryStore s;
  auto fs = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(fs));
  auto r = s.put_file_system_policy(kAccount, value(fs).spec.id, "not-json{");
  ASSERT_FALSE(ok(r));
  EXPECT_EQ(error(r).error_code, ERR_INVALID_POLICY_DOCUMENT);
}

// =================================================================
// SynchronizationConfiguration
// =================================================================

TEST(MemoryStore, SyncConfig_PutGet_VersionIncrements) {
  MemoryStore s;
  auto fs = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(fs));
  const auto& fs_id = value(fs).spec.id;

  SyncConfig cfg;
  cfg.import_rules.push_back({"", "ON_FILE_ACCESS", 1024});
  cfg.expiration_rules.push_back({30});

  ASSERT_TRUE(ok(s.put_synchronization_configuration(
      kAccount, fs_id, cfg, std::nullopt)));

  auto r1 = s.get_synchronization_configuration(kAccount, fs_id);
  ASSERT_TRUE(ok(r1));
  EXPECT_EQ(value(r1).latest_version_number, 1);

  // Subsequent put with matching expected_version succeeds and
  // increments the version.
  ASSERT_TRUE(ok(s.put_synchronization_configuration(
      kAccount, fs_id, cfg, /*expected_version=*/1)));
  auto r2 = s.get_synchronization_configuration(kAccount, fs_id);
  ASSERT_TRUE(ok(r2));
  EXPECT_EQ(value(r2).latest_version_number, 2);
}

TEST(MemoryStore, SyncConfig_VersionMismatch) {
  MemoryStore s;
  auto fs = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(fs));
  SyncConfig cfg;
  cfg.import_rules.push_back({"", "ON_FILE_ACCESS", 1024});
  cfg.expiration_rules.push_back({30});
  ASSERT_TRUE(ok(s.put_synchronization_configuration(
      kAccount, value(fs).spec.id, cfg, std::nullopt)));

  auto r = s.put_synchronization_configuration(
      kAccount, value(fs).spec.id, cfg, /*expected_version=*/999);
  ASSERT_FALSE(ok(r));
  EXPECT_EQ(error(r).kind, StoreError::Kind::Conflict);
}

// =================================================================
// AccessPoint
// =================================================================

TEST(MemoryStore, CreateAccessPoint_Minimum) {
  MemoryStore s;
  auto fs = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(fs));

  CreateAccessPointRequest req;
  req.owner_account_id = kAccount;
  req.filesystem_id = value(fs).spec.id;

  auto r = s.create_access_point(req);
  ASSERT_TRUE(ok(r));
  EXPECT_FALSE(value(r).spec.id.empty());
  EXPECT_EQ(value(r).spec.parent_filesystem_id, value(fs).spec.id);
  EXPECT_EQ(value(r).status.state, LifecycleState::Available);
}

TEST(MemoryStore, CreateAccessPoint_NonexistentFs) {
  MemoryStore s;
  CreateAccessPointRequest req;
  req.owner_account_id = kAccount;
  req.filesystem_id = "fs-doesnotexist";
  auto r = s.create_access_point(req);
  ASSERT_FALSE(ok(r));
  EXPECT_EQ(error(r).error_code, ERR_FILE_SYSTEM_NOT_FOUND);
}

TEST(MemoryStore, CreateAccessPoint_InvalidPosixUser) {
  MemoryStore s;
  auto fs = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(fs));

  CreateAccessPointRequest req;
  req.owner_account_id = kAccount;
  req.filesystem_id = value(fs).spec.id;
  PosixUser pu;
  pu.uid = -1;  // invalid
  pu.gid = 0;
  req.posix_user = pu;

  auto r = s.create_access_point(req);
  ASSERT_FALSE(ok(r));
  EXPECT_EQ(error(r).error_code, ERR_INVALID_POSIX_USER);
}

TEST(MemoryStore, ListAccessPoints_FiltersByParentFs) {
  MemoryStore s;
  auto fs = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(fs));

  CreateAccessPointRequest req;
  req.owner_account_id = kAccount;
  req.filesystem_id = value(fs).spec.id;
  ASSERT_TRUE(ok(s.create_access_point(req)));
  ASSERT_TRUE(ok(s.create_access_point(req)));

  auto r = s.list_access_points(kAccount, value(fs).spec.id, ListOptions{});
  ASSERT_TRUE(ok(r));
  EXPECT_EQ(value(r).items.size(), 2u);
}

// =================================================================
// MountTarget
// =================================================================

TEST(MemoryStore, CreateMountTarget_Minimum) {
  MemoryStore s;
  auto fs = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(fs));

  CreateMountTargetRequest req;
  req.owner_account_id = kAccount;
  req.filesystem_id = value(fs).spec.id;
  req.zone_id = std::string(kZone);

  auto r = s.create_mount_target(req);
  ASSERT_TRUE(ok(r));
  EXPECT_EQ(value(r).spec.zone_id, kZone);
  EXPECT_EQ(value(r).spec.ip_address_type, "IPV4_ONLY");
}

TEST(MemoryStore, CreateMountTarget_OnePerZone) {
  MemoryStore s;
  auto fs = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(fs));

  CreateMountTargetRequest req;
  req.owner_account_id = kAccount;
  req.filesystem_id = value(fs).spec.id;
  req.zone_id = std::string(kZone);
  ASSERT_TRUE(ok(s.create_mount_target(req)));

  auto r = s.create_mount_target(req);
  ASSERT_FALSE(ok(r));
  EXPECT_EQ(error(r).kind, StoreError::Kind::Conflict);
  EXPECT_EQ(error(r).error_code, ERR_MOUNT_TARGET_ALREADY_IN_ZONE);
}

TEST(MemoryStore, CreateMountTarget_MissingZone) {
  MemoryStore s;
  auto fs = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(fs));

  CreateMountTargetRequest req;
  req.owner_account_id = kAccount;
  req.filesystem_id = value(fs).spec.id;

  auto r = s.create_mount_target(req);
  ASSERT_FALSE(ok(r));
  EXPECT_EQ(error(r).error_code, ERR_INVALID_ZONE_ID);
}

TEST(MemoryStore, UpdateMountTarget_SecurityGroups) {
  MemoryStore s;
  auto fs = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(fs));
  CreateMountTargetRequest mreq;
  mreq.owner_account_id = kAccount;
  mreq.filesystem_id = value(fs).spec.id;
  mreq.zone_id = std::string(kZone);
  auto mt = s.create_mount_target(mreq);
  ASSERT_TRUE(ok(mt));

  UpdateMountTargetRequest ureq;
  ureq.id = value(mt).spec.id;
  ureq.security_groups = {"sg-1", "sg-2"};
  auto upd = s.update_mount_target(kAccount, ureq);
  ASSERT_TRUE(ok(upd));
  EXPECT_EQ(value(upd).spec.security_groups,
            (std::vector<std::string>{"sg-1", "sg-2"}));

  auto got = s.get_mount_target(kAccount, value(mt).spec.id);
  ASSERT_TRUE(ok(got));
  EXPECT_EQ(value(got).spec.security_groups,
            (std::vector<std::string>{"sg-1", "sg-2"}));
}

TEST(MemoryStore, ListMountTargets_FilteredByFs) {
  MemoryStore s;
  auto fs1 = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(fs1));

  auto fs2_req = minimum_fs_req();
  fs2_req.bucket_arn = "arn:aws:s3:::other-bucket";
  auto fs2 = s.create_file_system(fs2_req);
  ASSERT_TRUE(ok(fs2));

  CreateMountTargetRequest mt1;
  mt1.owner_account_id = kAccount;
  mt1.filesystem_id = value(fs1).spec.id;
  mt1.zone_id = std::string(kZone);
  ASSERT_TRUE(ok(s.create_mount_target(mt1)));

  CreateMountTargetRequest mt2;
  mt2.owner_account_id = kAccount;
  mt2.filesystem_id = value(fs2).spec.id;
  mt2.zone_id = std::string(kZone);
  ASSERT_TRUE(ok(s.create_mount_target(mt2)));

  auto r = s.list_mount_targets(
      kAccount, std::optional<std::string_view>{value(fs1).spec.id},
      std::nullopt, ListOptions{});
  ASSERT_TRUE(ok(r));
  EXPECT_EQ(value(r).items.size(), 1u);
  EXPECT_EQ(value(r).items.front().spec.parent_filesystem_id,
            value(fs1).spec.id);
}

// =================================================================
// Cascade
// =================================================================

TEST(MemoryStore, DeleteFileSystem_RejectsWithChildren) {
  MemoryStore s;
  auto fs = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(fs));
  CreateAccessPointRequest req;
  req.owner_account_id = kAccount;
  req.filesystem_id = value(fs).spec.id;
  auto ap = s.create_access_point(req);
  ASSERT_TRUE(ok(ap));

  auto r = s.delete_file_system(kAccount, value(fs).spec.id);
  ASSERT_FALSE(ok(r));
  EXPECT_EQ(error(r).kind, StoreError::Kind::Conflict);
  EXPECT_EQ(error(r).error_code, ERR_FILE_SYSTEM_HAS_CHILDREN);

  // Cleaning up the AP allows the FS delete to succeed.
  ASSERT_TRUE(ok(s.delete_access_point(kAccount, value(ap).spec.id)));
  ASSERT_TRUE(ok(s.delete_file_system(kAccount, value(fs).spec.id)));
}

// =================================================================
// Tagging
// =================================================================

TEST(MemoryStore, Tagging_RoundTrip) {
  MemoryStore s;
  auto fs = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(fs));
  const auto& fs_id = value(fs).spec.id;

  std::vector<Tag> tags = {{"env", "ci"}, {"Name", "test"}};
  ASSERT_TRUE(ok(s.tag_resource(kAccount, fs_id, tags)));

  auto listed = s.list_tags_for_resource(kAccount, fs_id, ListOptions{});
  ASSERT_TRUE(ok(listed));
  EXPECT_EQ(value(listed).items.size(), 2u);

  // A second tag_resource with the same key replaces the value.
  ASSERT_TRUE(ok(s.tag_resource(kAccount, fs_id, {{"env", "prod"}})));
  auto listed2 = s.list_tags_for_resource(kAccount, fs_id, ListOptions{});
  ASSERT_TRUE(ok(listed2));
  bool found = false;
  for (const auto& t : value(listed2).items) {
    if (t.key == "env") {
      EXPECT_EQ(t.value, "prod");
      found = true;
    }
  }
  EXPECT_TRUE(found);

  // Untag removes by key.
  ASSERT_TRUE(ok(s.untag_resource(kAccount, fs_id, {"env"})));
  auto listed3 = s.list_tags_for_resource(kAccount, fs_id, ListOptions{});
  ASSERT_TRUE(ok(listed3));
  for (const auto& t : value(listed3).items) {
    EXPECT_NE(t.key, "env");
  }
}

TEST(MemoryStore, Tagging_NameTagDrivesView) {
  MemoryStore s;
  auto req = minimum_fs_req();
  req.tags = {{"Name", "the-name"}};
  auto r = s.create_file_system(req);
  ASSERT_TRUE(ok(r));
  EXPECT_EQ(value(r).name(), "the-name");
}

TEST(MemoryStore, Tagging_NotFound) {
  MemoryStore s;
  auto r = s.tag_resource(kAccount, "fs-doesnotexist", {{"k", "v"}});
  ASSERT_FALSE(ok(r));
  EXPECT_EQ(error(r).kind, StoreError::Kind::NotFound);
}

// =================================================================
// ChangeFeed
// =================================================================

TEST(ChangeFeed, InProcess_FiresAllSubscribers) {
  InProcessChangeFeed feed;
  std::atomic<int> a{0}, b{0};
  feed.subscribe([&]{ a++; });
  feed.subscribe([&]{ b++; });
  feed.fire();
  feed.fire();
  EXPECT_EQ(a.load(), 2);
  EXPECT_EQ(b.load(), 2);
}

TEST(ChangeFeed, InProcess_UnsubscribeStopsCallback) {
  InProcessChangeFeed feed;
  std::atomic<int> a{0}, b{0};
  auto h_a = feed.subscribe([&]{ a++; });
  feed.subscribe([&]{ b++; });
  feed.fire();
  feed.unsubscribe(h_a);
  feed.fire();
  EXPECT_EQ(a.load(), 1);
  EXPECT_EQ(b.load(), 2);
}

TEST(ChangeFeed, Noop_NeverFires) {
  NoopChangeFeed feed;
  std::atomic<int> n{0};
  feed.subscribe([&]{ n++; });  // accepted but never invoked
  // No fire() — interface has none. The reconciler relies on the
  // safety-net timer when paired with NoopChangeFeed.
  EXPECT_EQ(n.load(), 0);
}

// =================================================================
// MemoryStore + ChangeFeed wiring
// =================================================================

TEST(MemoryStore, OnChange_FiresAfterMutation) {
  MemoryStore s;
  std::atomic<int> n{0};
  s.set_on_change([&]{ n++; });

  // Mutations fire.
  auto r = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(r));
  EXPECT_EQ(n.load(), 1);

  s.tag_resource(kAccount, value(r).spec.id, {{"k", "v"}});
  EXPECT_EQ(n.load(), 2);

  s.delete_file_system(kAccount, value(r).spec.id);
  EXPECT_EQ(n.load(), 3);
}

TEST(MemoryStore, OnChange_DoesNotFireOnReads) {
  MemoryStore s;
  auto r = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(r));

  // Wire the callback only AFTER the create so we can count
  // reads in isolation.
  std::atomic<int> n{0};
  s.set_on_change([&]{ n++; });

  s.get_file_system(kAccount, value(r).spec.id);
  ListOptions opts;
  s.list_file_systems(kAccount, opts);
  EXPECT_EQ(n.load(), 0);

  s.delete_file_system(kAccount, value(r).spec.id);  // mutation
  EXPECT_EQ(n.load(), 1);
}

TEST(MemoryStore, OnChange_DoesNotFireOnFailedMutation) {
  MemoryStore s;
  std::atomic<int> n{0};
  s.set_on_change([&]{ n++; });

  // Invalid request: missing required field. Should not fire.
  CreateFileSystemRequest bad;
  bad.owner_account_id = kAccount;
  // bucket_arn intentionally empty
  bad.role_arn = kRole;
  auto r = s.create_file_system(bad);
  ASSERT_FALSE(ok(r));
  EXPECT_EQ(n.load(), 0);
}

TEST(MemoryStore, OnChange_FiresThroughInProcessFeed) {
  // The classic wiring: a feed subscribes, MemoryStore fires
  // through it, downstream observer wakes.
  MemoryStore s;
  InProcessChangeFeed feed;
  s.set_on_change([&feed]{ feed.fire(); });

  std::atomic<int> reconciler_wakeups{0};
  feed.subscribe([&]{ reconciler_wakeups++; });

  auto r1 = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(r1));
  CreateAccessPointRequest apreq;
  apreq.owner_account_id = kAccount;
  apreq.filesystem_id = value(r1).spec.id;
  auto r2 = s.create_access_point(apreq);
  ASSERT_TRUE(ok(r2));
  s.delete_access_point(kAccount, value(r2).spec.id);

  EXPECT_EQ(reconciler_wakeups.load(), 3);
}
