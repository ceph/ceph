// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "gtest/gtest.h"

#include "common/dout.h"
#include "driver/rados/rgw_sal_rados.h"
#include "global/global_context.h"
#include "rgw_acl.h"
#include "rgw_acl_swift.h"
#include "rgw_auth.h"

// Referrer grants don't look up users, so no driver is needed.
static rgw::sal::Driver* const no_driver = nullptr;

static std::pair<std::string, std::string>
format_container_acls(const RGWAccessControlPolicy& policy)
{
  std::string read, write;
  rgw::swift::format_container_acls(policy, read, write);
  return {read, write};
}

// Parsing a user grant would need a driver.
static ACLGrant user_grant(const char* id, uint32_t perm)
{
  ACLGrant grant;
  grant.set_canon(rgw_user{id}, "", perm);
  return grant;
}

class SwiftAcl : public ::testing::Test {
protected:
  NoDoutPrefix dpp{g_ceph_context, ceph_subsys_rgw};
  ACLOwner owner{rgw_user{"owner"}, "Owner"};
  RGWAccessControlPolicy policy;
  uint32_t rw_mask = 0;

  int create_container_policy(const char* read, const char* write) {
    return rgw::swift::create_container_policy(&dpp,
                                               no_driver,
                                               owner,
                                               read,
                                               write,
                                               rw_mask,
                                               policy);
  }

  // As RGW does on a POST: parse the sent headers, then merge policy in.
  std::pair<std::string, std::string>
  update_container_policy(const char* read, const char* write) {
    RGWAccessControlPolicy updated;
    uint32_t mask = 0;
    EXPECT_EQ(0, rgw::swift::create_container_policy(&dpp,
                                                     no_driver,
                                                     owner,
                                                     read,
                                                     write,
                                                     mask,
                                                     updated));
    rgw::swift::merge_policy(mask, policy, updated);
    return format_container_acls(updated);
  }

  // As verify_object_permission_no_policy() checks a Swift object read.
  bool anonymous_read(const char* referer) {
    // SwiftAnonymousEngine's identity; the user only carries the id.
    const rgw::auth::LocalApplier anonymous(
        g_ceph_context,
        std::make_unique<rgw::sal::RadosUser>(nullptr,
                                              rgw_user{RGW_USER_ANON_ID}),
        std::nullopt, {}, rgw::auth::LocalApplier::NO_SUBUSER,
        std::nullopt, rgw::auth::LocalApplier::NO_ACCESS_KEY);
    return policy.verify_permission(&dpp,
                                    anonymous,
                                    RGW_PERM_READ_OBJS,
                                    RGW_PERM_READ_OBJS,
                                    referer);
  }

  int create_account_policy(const char* acl) {
    return rgw::swift::create_account_policy(&dpp,
                                             no_driver,
                                             owner,
                                             acl,
                                             policy);
  }
};

TEST_F(SwiftAcl, ReferrerReadRoundTrip)
{
  ASSERT_EQ(0, create_container_policy(
      ".r:*,.r:.example.com,.r:-bad.example.com", nullptr));
  EXPECT_EQ(RGW_PERM_READ_OBJS, rw_mask);
  auto [read, write] = format_container_acls(policy);
  EXPECT_EQ(".r:*,.r:.example.com,.r:-bad.example.com", read);
  EXPECT_EQ("", write);
}

TEST_F(SwiftAcl, ReferrerAliases)
{
  ASSERT_EQ(0, create_container_policy(
      ".ref:a.example,.referer:b.example,.referrer:c.example", nullptr));
  EXPECT_EQ(".r:a.example,.r:b.example,.r:c.example",
            format_container_acls(policy).first);
}

TEST_F(SwiftAcl, ReferrerLeadingStarStripped)
{
  ASSERT_EQ(0, create_container_policy(
      ".r:*.example.com,.r:-*.bad.example.com", nullptr));
  EXPECT_EQ(".r:.example.com,.r:-.bad.example.com",
            format_container_acls(policy).first);
}

TEST_F(SwiftAcl, ReferrerStarPermissions)
{
  ASSERT_EQ(0, create_container_policy(
      ".r:-a.example.com,.r:*,.r:-bad.example.com", nullptr));
  EXPECT_TRUE(anonymous_read(nullptr));
  // Each matches two grants; the last one wins.
  EXPECT_TRUE(anonymous_read("http://a.example.com/"));
  EXPECT_FALSE(anonymous_read("http://bad.example.com/"));
}

TEST_F(SwiftAcl, ReferrerDomainPermissions)
{
  ASSERT_EQ(0, create_container_policy(".r:.example.com", nullptr));
  EXPECT_TRUE(anonymous_read("http://a.example.com/"));
  EXPECT_FALSE(anonymous_read("http://example.com/"));
  EXPECT_FALSE(anonymous_read(nullptr));
}

TEST_F(SwiftAcl, MergeKeepsUnchangedReadAcl)
{
  ASSERT_EQ(0, create_container_policy(
      ".r:a.example,.r:-bad.example", nullptr));
  policy.get_acl().add_grant(user_grant("bob", RGW_PERM_WRITE_OBJS));
  auto [read, write] = update_container_policy(nullptr, "");
  EXPECT_EQ(".r:a.example,.r:-bad.example", read);
  EXPECT_EQ("", write);
}

TEST_F(SwiftAcl, MergeKeepsUnchangedWriteAcl)
{
  ASSERT_EQ(0, create_container_policy(
      ".r:a.example,.r:-bad.example", nullptr));
  policy.get_acl().add_grant(user_grant("bob", RGW_PERM_WRITE_OBJS));
  auto [read, write] = update_container_policy("", nullptr);
  EXPECT_EQ("", read);
  EXPECT_EQ("bob", write);
}

TEST_F(SwiftAcl, ReferrerWriteRejected)
{
  EXPECT_EQ(-EINVAL, create_container_policy(nullptr, ".r:*"));
}

TEST_F(SwiftAcl, InvalidGrantsRejected)
{
  EXPECT_EQ(-EINVAL, create_container_policy(".foo:bar", nullptr));
  EXPECT_EQ(-EINVAL, create_container_policy(".r:", nullptr));
  EXPECT_EQ(-EINVAL, create_container_policy(".r:-", nullptr));
  EXPECT_EQ(-EINVAL, create_container_policy(".r:.", nullptr));
  EXPECT_EQ(-EINVAL, create_container_policy(".r:a.example,.foo:bar", nullptr));
}

TEST_F(SwiftAcl, AccountEmptyAclFormatsToNullopt)
{
  ASSERT_EQ(0, create_account_policy("{}"));
  EXPECT_EQ(std::nullopt, rgw::swift::format_account_acl(policy));
}

TEST_F(SwiftAcl, AccountGrantsPartitionedByPermission)
{
  // Adds the owner's grant, which isn't listed.
  policy.create_default(owner.id, owner.display_name);
  auto& acl = policy.get_acl();
  acl.add_grant(user_grant("admin", RGW_PERM_FULL_CONTROL));
  acl.add_grant(user_grant("rw", RGW_PERM_READ_OBJS | RGW_PERM_WRITE_OBJS));
  acl.add_grant(user_grant("ro", RGW_PERM_READ_OBJS));
  EXPECT_EQ(R"({"read-only":["ro"],"read-write":["rw"],"admin":["admin"]})",
            rgw::swift::format_account_acl(policy));
}

TEST_F(SwiftAcl, AccountInvalidJsonRejected)
{
  EXPECT_EQ(-EINVAL, create_account_policy("{not json"));
}
