// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "rgw_common.h"

#include <cerrno>
#include <gtest/gtest.h>

TEST(RGWApplyTenantToUid, EmptyTenantIsNoop)
{
  rgw_user uid("user1");
  std::string err_msg;
  ASSERT_EQ(0, rgw_apply_tenant_to_uid("", "user1", uid, err_msg));
  EXPECT_EQ(rgw_user("user1"), uid);
}

TEST(RGWApplyTenantToUid, EmptyTenantEmptyUidIsNoop)
{
  rgw_user uid("");
  std::string err_msg;
  ASSERT_EQ(0, rgw_apply_tenant_to_uid("", "", uid, err_msg));
  EXPECT_TRUE(uid.empty());
}

TEST(RGWApplyTenantToUid, TenantRequiresUid)
{
  rgw_user uid("");
  std::string err_msg;
  ASSERT_EQ(-EINVAL, rgw_apply_tenant_to_uid("tenanta", "", uid, err_msg));
  EXPECT_FALSE(err_msg.empty());
}

TEST(RGWApplyTenantToUid, TenantAppliedToBareUid)
{
  rgw_user uid("user1");
  std::string err_msg;
  ASSERT_EQ(0, rgw_apply_tenant_to_uid("tenanta", "user1", uid, err_msg));
  EXPECT_EQ("tenanta", uid.tenant);
  EXPECT_EQ("user1", uid.id);
}

TEST(RGWApplyTenantToUid, MatchingEmbeddedTenantAccepted)
{
  rgw_user uid("tenanta$user1");
  std::string err_msg;
  ASSERT_EQ(0, rgw_apply_tenant_to_uid("tenanta", "tenanta$user1", uid,
                                       err_msg));
  EXPECT_EQ(rgw_user("tenanta$user1"), uid);
}

TEST(RGWApplyTenantToUid, ConflictingEmbeddedTenantRejected)
{
  rgw_user uid("tenanta$user1");
  std::string err_msg;
  ASSERT_EQ(-EINVAL, rgw_apply_tenant_to_uid("tenantb", "tenanta$user1", uid,
                                             err_msg));
  EXPECT_FALSE(err_msg.empty());
  EXPECT_EQ(rgw_user("tenanta$user1"), uid);
}

// rgw_user::from_str() parses "$user1" as an empty tenant, so a tenant
// parameter applies to it the same as to a bare uid.
TEST(RGWApplyTenantToUid, LeadingDollarParsesAsEmptyTenant)
{
  rgw_user uid("$user1");
  ASSERT_TRUE(uid.tenant.empty());
  ASSERT_EQ("user1", uid.id);
  std::string err_msg;
  ASSERT_EQ(0, rgw_apply_tenant_to_uid("tenanta", "$user1", uid, err_msg));
  EXPECT_EQ("tenanta", uid.tenant);
  EXPECT_EQ("user1", uid.id);
}

// A tenant value that is itself a combined "tenant$uid" string gets no
// special handling: it is compared verbatim against the embedded tenant.
TEST(RGWApplyTenantToUid, CombinedStringAsTenantIsNotSpecialCased)
{
  rgw_user uid("tenanta$user1");
  std::string err_msg;
  ASSERT_EQ(-EINVAL, rgw_apply_tenant_to_uid("tenanta$user1", "tenanta$user1",
                                             uid, err_msg));
  EXPECT_FALSE(err_msg.empty());
}
