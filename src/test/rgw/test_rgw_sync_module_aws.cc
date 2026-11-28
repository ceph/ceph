// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * The destination paths the cloud sync module builds, in
 * AWSSyncConfig::get_path() and get_target().
 *
 * A path is target_path with ${bucket} and ${owner} substituted, followed by
 * the object key. get_target() then splits it at the first '/' to get the
 * destination bucket name. Two things have to hold: two different source
 * buckets must never produce the same path, and the tenant of a tenanted
 * bucket must stay out of the destination bucket name.
 */

#include <gtest/gtest.h>

#include "common/ceph_context.h"
#include "rgw_common.h"

#include "driver/rados/rgw_sync_module_aws.cc"

namespace {

/* get_path() logs on the rejection path, so it needs a real context. */
auto cct = std::make_unique<CephContext>(CEPH_ENTITY_TYPE_ANY);
NoDoutPrefix dp(cct.get(), ceph_subsys_rgw);

std::shared_ptr<AWSSyncConfig_Profile> profile(const string& target_path)
{
  auto p = std::make_shared<AWSSyncConfig_Profile>();
  p->target_path = target_path;
  return p;
}

RGWBucketInfo bucket(const string& tenant, const string& name)
{
  RGWBucketInfo info;
  info.bucket.tenant = tenant;
  info.bucket.name = name;
  return info;
}

/* get_path() for a bucket, or the error it returned. */
int path_of(const string& target_path, const RGWBucketInfo& info,
            const string& key, string *out)
{
  AWSSyncConfig conf;
  auto p = profile(target_path);
  return conf.get_path(&dp, p, info, rgw_obj_key(key), out);
}

} // namespace

/* Tenanted proj1/data and untenanted proj1-data must land on different paths. */
TEST(AWSSyncPath, TenantedAndUntenantedDoNotCollide)
{
  const string target = "dest/${bucket}";

  string tenanted, untenanted;
  ASSERT_EQ(0, path_of(target, bucket("proj1", "data"), "secret.txt", &tenanted));
  ASSERT_EQ(0, path_of(target, bucket("", "proj1-data"), "secret.txt", &untenanted));

  EXPECT_NE(tenanted, untenanted);
  EXPECT_EQ("dest/proj1/data/secret.txt", tenanted);
  EXPECT_EQ("dest/proj1-data/secret.txt", untenanted);
}

TEST(AWSSyncPath, UntenantedBucketIsUnchanged)
{
  string path;
  ASSERT_EQ(0, path_of("dest/${bucket}", bucket("", "data"), "obj", &path));
  EXPECT_EQ("dest/data/obj", path);
}

/* The default we ship puts ${bucket} in the key, so tenanted buckets work. */
TEST(AWSSyncPath, DefaultTargetPathAcceptsTenantedBucket)
{
  string path;
  ASSERT_EQ(0, path_of(default_target_path, bucket("proj1", "data"), "obj", &path));
  EXPECT_EQ("rgw-${zonegroup}-${sid}/proj1/data/obj", path);
}

/* A tenanted bucket with ${bucket} at the front of target_path is refused. */
TEST(AWSSyncPath, TenantedBucketRejectedWhenBucketIsTheDestinationBucketName)
{
  string path;
  EXPECT_EQ(-EINVAL,
            path_of("${bucket}", bucket("proj1", "data"), "obj", &path));
  EXPECT_EQ(-EINVAL,
            path_of("${bucket}/sub", bucket("proj1", "data"), "obj", &path));
}

/* The same target_path is fine for an untenanted bucket. */
TEST(AWSSyncPath, UntenantedBucketAllowedAsDestinationBucketName)
{
  string path;
  ASSERT_EQ(0, path_of("${bucket}", bucket("", "data"), "obj", &path));
  EXPECT_EQ("data/obj", path);
}

/* ${owner} still joins tenant and user with a '-'. */
TEST(AWSSyncPath, OwnerKeepsHyphenForm)
{
  RGWBucketInfo info = bucket("proj1", "data");
  info.owner = parse_owner("alice");

  string path;
  ASSERT_EQ(0, path_of("dest/${owner}/${bucket}", info, "obj", &path));
  EXPECT_EQ("dest/proj1-alice/proj1/data/obj", path);
}

/* get_target() puts the tenant in the key, not in the destination bucket. */
TEST(AWSSyncPath, GetTargetSplitsBucketAndKey)
{
  AWSSyncConfig conf;
  auto p = profile("dest/${bucket}");

  string bucket_name, obj_name;
  ASSERT_EQ(0, conf.get_target(&dp, p, bucket("proj1", "data"),
                               rgw_obj_key("secret.txt"),
                               &bucket_name, &obj_name));
  EXPECT_EQ("dest", bucket_name);
  EXPECT_EQ("proj1/data/secret.txt", obj_name);
}
