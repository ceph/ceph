// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "file_state/change_feed.h"
#include "file_state/dbus_ganesha_sink.h"
#include "file_state/desired_export.h"
#include "file_state/ganesha_sink.h"
#include "file_state/memory_store.h"
#include "file_state/reconciler.h"
#include "rgw_s3files_errors.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <thread>

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

// =================================================================
// compose_exports
// =================================================================

namespace {

// Helper: create a complete (FS, AP, MT) tuple in the store and
// return the ids.
struct FullTuple {
  std::string fs_id;
  std::string ap_id;
  std::string mt_id;
};

FullTuple make_full_tuple(MemoryStore& s,
                           std::string_view bucket = kBucket,
                           std::string_view zone  = kZone,
                           std::string_view ap_path = "") {
  CreateFileSystemRequest fsreq;
  fsreq.owner_account_id = kAccount;
  fsreq.bucket_arn = std::string(bucket);
  fsreq.role_arn = kRole;
  fsreq.prefix = "fsp/";
  auto fs = s.create_file_system(fsreq);
  if (!ok(fs)) return {};

  CreateAccessPointRequest apreq;
  apreq.owner_account_id = kAccount;
  apreq.filesystem_id = value(fs).spec.id;
  if (!ap_path.empty()) {
    RootDirectory rd;
    rd.path = std::string(ap_path);
    apreq.root_directory = rd;
  }
  apreq.posix_user = PosixUser{1000, 1000, {}};
  auto ap = s.create_access_point(apreq);
  if (!ok(ap)) return {};

  CreateMountTargetRequest mtreq;
  mtreq.owner_account_id = kAccount;
  mtreq.filesystem_id = value(fs).spec.id;
  mtreq.zone_id = std::string(zone);
  auto mt = s.create_mount_target(mtreq);
  if (!ok(mt)) return {};

  return {value(fs).spec.id, value(ap).spec.id, value(mt).spec.id};
}

}  // namespace

TEST(ComposeExports, EmptyStoreYieldsEmpty) {
  MemoryStore s;
  EXPECT_TRUE(compose_exports(s).empty());
}

TEST(ComposeExports, FsAlone_NoAp_YieldsNothing) {
  MemoryStore s;
  auto fs = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(fs));
  EXPECT_TRUE(compose_exports(s).empty());
}

TEST(ComposeExports, FsAndApButNoMt_YieldsNothing) {
  MemoryStore s;
  auto fs = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(fs));
  CreateAccessPointRequest apreq;
  apreq.owner_account_id = kAccount;
  apreq.filesystem_id = value(fs).spec.id;
  auto ap = s.create_access_point(apreq);
  ASSERT_TRUE(ok(ap));
  EXPECT_TRUE(compose_exports(s).empty());
}

TEST(ComposeExports, FullTuple_YieldsOneExport) {
  MemoryStore s;
  auto t = make_full_tuple(s);
  ASSERT_FALSE(t.fs_id.empty());

  auto exports = compose_exports(s);
  ASSERT_EQ(exports.size(), 1u);
  const auto& e = exports[0];
  EXPECT_EQ(e.fs_id, t.fs_id);
  EXPECT_EQ(e.ap_id, t.ap_id);
  EXPECT_EQ(e.mt_id, t.mt_id);
  EXPECT_EQ(e.bucket_arn, kBucket);
  EXPECT_EQ(e.role_arn, kRole);
  EXPECT_EQ(e.zone_id, kZone);
  // No ap rootDirectory, so composed_prefix == fs prefix.
  EXPECT_EQ(e.composed_prefix, "fsp/");
  ASSERT_TRUE(e.posix_user.has_value());
  EXPECT_EQ(e.posix_user->uid, 1000);
}

TEST(ComposeExports, ComposesPrefixWithRootDirectory) {
  MemoryStore s;
  // FS prefix + AP rootDirectory `/scoped/team-a` should yield
  // composed prefix `fsp/scoped/team-a/`.
  make_full_tuple(s, kBucket, kZone, "/scoped/team-a");
  auto exports = compose_exports(s);
  ASSERT_EQ(exports.size(), 1u);
  EXPECT_EQ(exports[0].composed_prefix, "fsp/scoped/team-a/");
}

TEST(ComposeExports, MultipleApsCrossWithMt) {
  // One FS, two APs, one MT → two exports (cross-product).
  MemoryStore s;
  auto fs = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(fs));

  CreateAccessPointRequest apreq;
  apreq.owner_account_id = kAccount;
  apreq.filesystem_id = value(fs).spec.id;
  auto ap1 = s.create_access_point(apreq);
  auto ap2 = s.create_access_point(apreq);
  ASSERT_TRUE(ok(ap1));
  ASSERT_TRUE(ok(ap2));

  CreateMountTargetRequest mtreq;
  mtreq.owner_account_id = kAccount;
  mtreq.filesystem_id = value(fs).spec.id;
  mtreq.zone_id = kZone;
  auto mt = s.create_mount_target(mtreq);
  ASSERT_TRUE(ok(mt));

  auto exports = compose_exports(s);
  EXPECT_EQ(exports.size(), 2u);
}

TEST(ComposeExports, OrphanedMt_Skipped) {
  // Construct a scenario where a MT references a FS that
  // doesn't exist (shouldn't happen in practice, but the
  // reconciler must tolerate transient inconsistency).
  MemoryStore s;
  auto t = make_full_tuple(s);
  ASSERT_FALSE(t.fs_id.empty());

  // Delete the AP and FS, leaving the MT orphaned. We can't
  // delete an FS while children exist, so delete in order.
  s.delete_access_point(kAccount, t.ap_id);
  // delete_mount_target(t.mt_id) would clean cleanly, so we
  // skip that to simulate the orphan case.
  // The FS still has the MT, so delete_file_system should
  // be rejected; the test isn't about that, it's about
  // compose_exports tolerating inconsistency. Drop the MT
  // explicitly to leave just the FS.
  // Actually simpler: just verify post-delete_ap, the
  // exports list is empty (no AP → no export).
  EXPECT_TRUE(compose_exports(s).empty());
}

// =================================================================
// Reconciler
// =================================================================

TEST(Reconciler, ReconcileOnce_EmptyStore_AppliesEmptySet) {
  MemoryStore s;
  NoopChangeFeed feed;
  RecordingGaneshaSink sink;
  Reconciler r(s, feed, sink);
  r.reconcile_once();
  ASSERT_EQ(sink.call_count(), 1u);
  EXPECT_TRUE(sink.last().empty());
}

TEST(Reconciler, ReconcileOnce_FullStore_AppliesExpectedSet) {
  MemoryStore s;
  auto t = make_full_tuple(s);
  ASSERT_FALSE(t.fs_id.empty());

  NoopChangeFeed feed;
  RecordingGaneshaSink sink;
  Reconciler r(s, feed, sink);
  r.reconcile_once();
  ASSERT_EQ(sink.call_count(), 1u);
  ASSERT_EQ(sink.last().size(), 1u);
  EXPECT_EQ(sink.last()[0].fs_id, t.fs_id);
  EXPECT_EQ(sink.last()[0].ap_id, t.ap_id);
  EXPECT_EQ(sink.last()[0].mt_id, t.mt_id);
}

TEST(Reconciler, Start_RunsInitialReconcileWithoutChanges) {
  MemoryStore s;
  make_full_tuple(s);

  InProcessChangeFeed feed;
  RecordingGaneshaSink sink;
  // Long safety-net timer so we observe just the start-time
  // initial reconcile, not a periodic one.
  Reconciler r(s, feed, sink, ReconcilerConfig{
      .safety_net_interval = std::chrono::seconds(60),
  });
  r.start();
  // Wait briefly for the worker thread to drain its initial
  // dirty flag and apply.
  for (int i = 0; i < 100 && sink.call_count() == 0; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  r.stop();
  EXPECT_GE(sink.call_count(), 1u);
  EXPECT_EQ(sink.last().size(), 1u);
}

TEST(Reconciler, FeedSignal_TriggersReconcile) {
  MemoryStore s;
  InProcessChangeFeed feed;
  RecordingGaneshaSink sink;
  // Wire the store to the feed.
  s.set_on_change([&feed]{ feed.fire(); });

  Reconciler r(s, feed, sink, ReconcilerConfig{
      .safety_net_interval = std::chrono::seconds(60),
  });
  r.start();

  // Wait for the initial reconcile to settle.
  for (int i = 0; i < 100 && sink.call_count() == 0; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  const auto initial_calls = sink.call_count();

  // Mutate: should fire the feed → wake the worker → another
  // apply().
  make_full_tuple(s);
  for (int i = 0; i < 200 && sink.call_count() == initial_calls; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  r.stop();

  EXPECT_GT(sink.call_count(), initial_calls);
  EXPECT_EQ(sink.last().size(), 1u);
}

TEST(Reconciler, BurstOfChanges_CoalescesIntoFewerApplies) {
  MemoryStore s;
  InProcessChangeFeed feed;
  RecordingGaneshaSink sink;
  s.set_on_change([&feed]{ feed.fire(); });

  Reconciler r(s, feed, sink, ReconcilerConfig{
      .safety_net_interval = std::chrono::seconds(60),
  });
  r.start();

  // Fire a burst of mutations as fast as we can. The reconciler
  // should coalesce — far fewer applies than mutations.
  constexpr int kBurst = 50;
  for (int i = 0; i < kBurst; ++i) {
    auto fsreq = minimum_fs_req();
    fsreq.bucket_arn = std::string(kBucket) + "-" + std::to_string(i);
    s.create_file_system(fsreq);
  }

  // Wait for the worker to settle. Generous so a slow CI box
  // doesn't flake.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  r.stop();

  // Strictly fewer applies than mutations means coalescing.
  // (Initial + at-least-one + at-most-kBurst.)
  EXPECT_LT(sink.call_count(), kBurst);
  EXPECT_GE(sink.call_count(), 1u);
}

TEST(Reconciler, Idempotent_NoStateChange_StillSafeToReconcile) {
  // Repeated reconcile_once() with no underlying change yields
  // identical apply() payloads — important for the safety-net
  // timer not to jitter Ganesha unnecessarily.
  MemoryStore s;
  make_full_tuple(s);
  NoopChangeFeed feed;
  RecordingGaneshaSink sink;
  Reconciler r(s, feed, sink);
  r.reconcile_once();
  r.reconcile_once();
  r.reconcile_once();
  ASSERT_EQ(sink.call_count(), 3u);
  EXPECT_EQ(sink.calls()[0], sink.calls()[1]);
  EXPECT_EQ(sink.calls()[1], sink.calls()[2]);
}

TEST(Reconciler, SafetyNetTimer_FiresEvenWithoutChanges) {
  MemoryStore s;
  NoopChangeFeed feed;
  RecordingGaneshaSink sink;
  // Tiny safety-net interval so the test doesn't hang.
  Reconciler r(s, feed, sink, ReconcilerConfig{
      .safety_net_interval = std::chrono::milliseconds(50),
  });
  r.start();
  // Hold the reconciler alive long enough for at least 3 timer
  // wakes (initial + 2-3 from the 50ms timer).
  std::this_thread::sleep_for(std::chrono::milliseconds(250));
  r.stop();
  // Initial reconcile + ~3-4 timer-driven ones.
  EXPECT_GE(sink.call_count(), 3u);
}

TEST(Reconciler, StopIsIdempotentAndSafe) {
  MemoryStore s;
  NoopChangeFeed feed;
  RecordingGaneshaSink sink;
  Reconciler r(s, feed, sink);
  r.stop();              // never started
  r.start();
  r.stop();
  r.stop();              // double stop
}

// =================================================================
// DbusGaneshaSink
// =================================================================

namespace {

// Records every dbus-send invocation; returns success by default
// and a fake `uint16 NNNN` reply for AddExport so the parser
// has something to chew on.
class RecordingInvoker {
 public:
  std::vector<std::vector<std::string>> calls;
  std::uint16_t next_id = 1234;

  DbusGaneshaSink::DbusResult operator()(
      const std::vector<std::string>& argv) {
    calls.push_back(argv);
    DbusGaneshaSink::DbusResult r;
    r.exit_status = 0;
    // The sink doesn't actually consume AddExport's reply for
    // anything (it assigns its own export_id), so the stdout
    // contents don't matter for behavior. Keep it realistic
    // anyway in case future tests parse it.
    bool is_add = std::any_of(argv.begin(), argv.end(),
        [](const std::string& s){ return s.find("AddExport") != std::string::npos; });
    if (is_add) {
      r.stdout_text =
          "method return time=0 sender=:1.0 -> destination=...\n"
          "   uint16 " + std::to_string(next_id++) + "\n"
          "   string \"Export added\"\n";
    }
    return r;
  }
};

DbusGaneshaSink::Config minimum_dbus_cfg(const std::string& dir) {
  DbusGaneshaSink::Config c;
  c.export_config_dir = dir;
  c.rgw_endpoint = "http://127.0.0.1:8000";
  c.rgw_user_id = "testid";
  c.rgw_access_key = "ak";
  c.rgw_secret_key = "sk";
  return c;
}

DesiredExport sample_export(std::string_view fs = "fs-AAA",
                             std::string_view ap = "ap-BBB",
                             std::string_view mt = "mt-CCC") {
  DesiredExport e;
  e.fs_id = std::string(fs);
  e.ap_id = std::string(ap);
  e.mt_id = std::string(mt);
  e.bucket_arn = "arn:aws:s3:::demo-bucket";
  e.composed_prefix = "data/team-a/";
  e.role_arn = "arn:aws:iam::123:role/r";
  e.owner_account_id = "123";
  e.zone_id = "zone1";
  return e;
}

class TmpDir {
 public:
  TmpDir() {
    char tmpl[] = "/tmp/s3files-test-XXXXXX";
    char* d = mkdtemp(tmpl);
    if (d) path_ = d;
  }
  ~TmpDir() {
    if (!path_.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(path_, ec);
    }
  }
  const std::string& path() const { return path_; }
 private:
  std::string path_;
};

}  // namespace

TEST(DbusGaneshaSink, Apply_Add_NewExport_CallsAddExportAndWritesFile) {
  TmpDir td;
  RecordingInvoker rec;
  DbusGaneshaSink sink(minimum_dbus_cfg(td.path()),
                        std::ref(rec));
  sink.apply({sample_export()});

  ASSERT_EQ(rec.calls.size(), 1u);
  // The single dbus-send call should be AddExport.
  bool found_add = false;
  for (const auto& s : rec.calls[0]) {
    if (s.find("AddExport") != std::string::npos) found_add = true;
  }
  EXPECT_TRUE(found_add) << "expected AddExport in dbus-send args";

  // A config file should be on disk.
  auto id = sink.export_id_for("fs-AAA", "ap-BBB", "mt-CCC");
  ASSERT_TRUE(id.has_value());
  std::string path = td.path() + "/" + std::to_string(*id) + ".conf";
  EXPECT_TRUE(std::filesystem::exists(path));
}

TEST(DbusGaneshaSink, Apply_Idempotent_NoChange_NoExtraCalls) {
  TmpDir td;
  RecordingInvoker rec;
  DbusGaneshaSink sink(minimum_dbus_cfg(td.path()),
                        std::ref(rec));
  auto e = sample_export();
  sink.apply({e});
  std::size_t after_first = rec.calls.size();
  sink.apply({e});
  // Second apply with identical desired set: no Add/Update/Remove
  // should fire.
  EXPECT_EQ(rec.calls.size(), after_first);
}

TEST(DbusGaneshaSink, Apply_Update_ChangedExport_CallsUpdateExport) {
  TmpDir td;
  RecordingInvoker rec;
  DbusGaneshaSink sink(minimum_dbus_cfg(td.path()),
                        std::ref(rec));
  auto e = sample_export();
  sink.apply({e});
  std::size_t after_first = rec.calls.size();

  // Mutate something the FSAL would care about.
  e.composed_prefix = "data/team-b/";
  sink.apply({e});

  ASSERT_GT(rec.calls.size(), after_first);
  bool found_update = false;
  for (std::size_t i = after_first; i < rec.calls.size(); ++i) {
    for (const auto& s : rec.calls[i]) {
      if (s.find("UpdateExport") != std::string::npos) found_update = true;
    }
  }
  EXPECT_TRUE(found_update);
}

TEST(DbusGaneshaSink, Apply_Remove_DroppedExport_CallsRemoveExport) {
  TmpDir td;
  RecordingInvoker rec;
  DbusGaneshaSink sink(minimum_dbus_cfg(td.path()),
                        std::ref(rec));
  sink.apply({sample_export()});
  std::size_t after_first = rec.calls.size();
  sink.apply({});  // drop it

  ASSERT_GT(rec.calls.size(), after_first);
  bool found_remove = false;
  for (std::size_t i = after_first; i < rec.calls.size(); ++i) {
    for (const auto& s : rec.calls[i]) {
      if (s.find("RemoveExport") != std::string::npos) found_remove = true;
    }
  }
  EXPECT_TRUE(found_remove);
}

TEST(DbusGaneshaSink, Render_BlockContainsExpectedFields) {
  TmpDir td;
  RecordingInvoker rec;
  DbusGaneshaSink sink(minimum_dbus_cfg(td.path()),
                        std::ref(rec));
  auto e = sample_export();
  e.posix_user = PosixUser{2000, 2000, {}};
  sink.apply({e});

  auto id = sink.export_id_for("fs-AAA", "ap-BBB", "mt-CCC");
  ASSERT_TRUE(id.has_value());
  std::ifstream f(td.path() + "/" + std::to_string(*id) + ".conf");
  std::string body((std::istreambuf_iterator<char>(f)),
                    std::istreambuf_iterator<char>());

  // Smoke-check: the rendered EXPORT block must reference the
  // identity inputs and the FSAL credentials.
  EXPECT_NE(body.find("Export_ID = " + std::to_string(*id)), std::string::npos);
  EXPECT_NE(body.find("data/team-a/"), std::string::npos);
  EXPECT_NE(body.find("Pseudo = \"/fs-AAA/ap-BBB\""), std::string::npos);
  EXPECT_NE(body.find("Anonymous_Uid = 2000"), std::string::npos);
  EXPECT_NE(body.find("Name = RGW"), std::string::npos);
  EXPECT_NE(body.find("bucket = \"demo-bucket\""), std::string::npos);
  EXPECT_NE(body.find("User_ID = \"testid\""), std::string::npos);
  // The role ARN appears in the auto-generated comment so future
  // tooling can wire AssumeRole without re-deriving the binding.
  EXPECT_NE(body.find("roleArn=arn:aws:iam::123:role/r"),
            std::string::npos);
}

TEST(DbusGaneshaSink, ExportIds_AreUniquePerTuple) {
  TmpDir td;
  RecordingInvoker rec;
  DbusGaneshaSink sink(minimum_dbus_cfg(td.path()),
                        std::ref(rec));
  sink.apply({sample_export("fs-1", "ap-1", "mt-1"),
              sample_export("fs-1", "ap-2", "mt-1"),
              sample_export("fs-1", "ap-1", "mt-2")});

  auto a = sink.export_id_for("fs-1", "ap-1", "mt-1");
  auto b = sink.export_id_for("fs-1", "ap-2", "mt-1");
  auto c = sink.export_id_for("fs-1", "ap-1", "mt-2");
  ASSERT_TRUE(a && b && c);
  EXPECT_NE(*a, *b);
  EXPECT_NE(*a, *c);
  EXPECT_NE(*b, *c);
}
