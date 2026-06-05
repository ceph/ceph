// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <list>
#include <string>

#include <gtest/gtest.h>

#include "rgw_auth.h"
#include "rgw_auth_keystone.h"
#include "rgw_common.h"
#include "rgw_keystone.h"

namespace {

constexpr const char* TENANT_ID   = "11111111111111111111111111111111";
constexpr const char* TENANT_NAME = "shared";
constexpr const char* USER_ID     = "22222222222222222222222222222222";
constexpr const char* USER_NAME   = "consumer1";

rgw::auth::RemoteApplier::acl_strategy_t make_strategy_no_roles()
{
  return rgw::auth::keystone::make_keystone_acl_strategy(
      TENANT_ID, TENANT_NAME, USER_ID, USER_NAME, {});
}

rgw::auth::Identity::aclspec_t single_grant(const std::string& key, uint32_t perm)
{
  rgw::auth::Identity::aclspec_t spec;
  spec[key] = perm;
  return spec;
}

} // namespace

TEST(KeystoneAclStrategy, MatchesProjectIdUserId)
{
  auto strategy = make_strategy_no_roles();
  const auto spec = single_grant(std::string{TENANT_ID} + ":" + USER_ID,
                                 RGW_OP_TYPE_READ);
  EXPECT_EQ(RGW_OP_TYPE_READ, strategy(spec));
}

TEST(KeystoneAclStrategy, MatchesProjectNameUserName)
{
  auto strategy = make_strategy_no_roles();
  const auto spec = single_grant(std::string{TENANT_NAME} + ":" + USER_NAME,
                                 RGW_OP_TYPE_WRITE);
  EXPECT_EQ(RGW_OP_TYPE_WRITE, strategy(spec));
}

TEST(KeystoneAclStrategy, MatchesProjectIdWildcardUser)
{
  auto strategy = make_strategy_no_roles();
  const auto spec = single_grant(std::string{TENANT_ID} + ":*",
                                 RGW_OP_TYPE_READ);
  EXPECT_EQ(RGW_OP_TYPE_READ, strategy(spec));
}

TEST(KeystoneAclStrategy, MatchesProjectNameWildcardUser)
{
  auto strategy = make_strategy_no_roles();
  const auto spec = single_grant(std::string{TENANT_NAME} + ":*",
                                 RGW_OP_TYPE_READ);
  EXPECT_EQ(RGW_OP_TYPE_READ, strategy(spec));
}

TEST(KeystoneAclStrategy, MatchesWildcardTenantUserId)
{
  auto strategy = make_strategy_no_roles();
  const auto spec = single_grant(std::string{"*:"} + USER_ID,
                                 RGW_OP_TYPE_READ);
  EXPECT_EQ(RGW_OP_TYPE_READ, strategy(spec));
}

TEST(KeystoneAclStrategy, MatchesWildcardTenantUserName)
{
  auto strategy = make_strategy_no_roles();
  const auto spec = single_grant(std::string{"*:"} + USER_NAME,
                                 RGW_OP_TYPE_READ);
  EXPECT_EQ(RGW_OP_TYPE_READ, strategy(spec));
}

TEST(KeystoneAclStrategy, MatchesAuthenticatedWildcard)
{
  auto strategy = make_strategy_no_roles();
  const auto spec = single_grant("*:*", RGW_OP_TYPE_READ);
  EXPECT_EQ(RGW_OP_TYPE_READ, strategy(spec));
}

TEST(KeystoneAclStrategy, OrsMultipleMatchingGrants)
{
  auto strategy = make_strategy_no_roles();
  rgw::auth::Identity::aclspec_t spec;
  spec[std::string{TENANT_ID} + ":" + USER_ID] = RGW_OP_TYPE_READ;
  spec[std::string{TENANT_ID} + ":*"] = RGW_OP_TYPE_WRITE;
  EXPECT_EQ(RGW_OP_TYPE_READ | RGW_OP_TYPE_WRITE, strategy(spec));
}

TEST(KeystoneAclStrategy, NoMatchReturnsZero)
{
  auto strategy = make_strategy_no_roles();
  const auto spec = single_grant("other_tenant:other_user", RGW_OP_TYPE_READ);
  EXPECT_EQ(0u, strategy(spec));
}

TEST(KeystoneAclStrategy, EmptyAclspecReturnsZero)
{
  auto strategy = make_strategy_no_roles();
  rgw::auth::Identity::aclspec_t spec;
  EXPECT_EQ(0u, strategy(spec));
}

TEST(KeystoneAclStrategy, BareRoleNameIsNotHonored)
{
  /* Bare role-name ACL elements are intentionally unsupported until the
   * acl_strategy_t callback can see the bucket owner; otherwise a role
   * shared across projects would leak access. See PR #68795 commit message. */
  rgw::keystone::TokenEnvelope::Role member;
  member.name = "Member";
  std::list<rgw::keystone::TokenEnvelope::Role> roles{member};

  auto strategy = rgw::auth::keystone::make_keystone_acl_strategy(
      TENANT_ID, TENANT_NAME, USER_ID, USER_NAME, std::move(roles));
  const auto spec = single_grant("Member", RGW_OP_TYPE_READ);
  EXPECT_EQ(0u, strategy(spec));
}

TEST(KeystoneAclStrategy, SystemReaderAdminGrantsRead)
{
  rgw::keystone::TokenEnvelope::Role role;
  role.name = "system-reader";
  role.is_reader = true;
  role.is_admin = true;
  std::list<rgw::keystone::TokenEnvelope::Role> roles{role};

  auto strategy = rgw::auth::keystone::make_keystone_acl_strategy(
      TENANT_ID, TENANT_NAME, USER_ID, USER_NAME, std::move(roles));

  rgw::auth::Identity::aclspec_t empty;
  EXPECT_EQ(static_cast<uint32_t>(RGW_OP_TYPE_READ), strategy(empty));
}

TEST(KeystoneAclStrategy, ReaderWithoutAdminDoesNotGrant)
{
  rgw::keystone::TokenEnvelope::Role role;
  role.name = "reader";
  role.is_reader = true;
  role.is_admin = false;
  std::list<rgw::keystone::TokenEnvelope::Role> roles{role};

  auto strategy = rgw::auth::keystone::make_keystone_acl_strategy(
      TENANT_ID, TENANT_NAME, USER_ID, USER_NAME, std::move(roles));

  rgw::auth::Identity::aclspec_t empty;
  EXPECT_EQ(0u, strategy(empty));
}

TEST(KeystoneAclStrategy, SystemReaderAddsToAclMatchedPerm)
{
  rgw::keystone::TokenEnvelope::Role role;
  role.name = "system-reader";
  role.is_reader = true;
  role.is_admin = true;
  std::list<rgw::keystone::TokenEnvelope::Role> roles{role};

  auto strategy = rgw::auth::keystone::make_keystone_acl_strategy(
      TENANT_ID, TENANT_NAME, USER_ID, USER_NAME, std::move(roles));
  const auto spec = single_grant(std::string{TENANT_ID} + ":" + USER_ID,
                                 RGW_OP_TYPE_WRITE);
  EXPECT_EQ(RGW_OP_TYPE_READ | RGW_OP_TYPE_WRITE, strategy(spec));
}
