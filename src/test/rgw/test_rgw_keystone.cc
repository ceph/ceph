// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include <initializer_list>
#include <optional>
#include <string>

#include <boost/intrusive_ptr.hpp>
#include <boost/optional.hpp>

#include <gtest/gtest.h>

#include "common/ceph_context.h"
#include "common/ceph_json.h"
#include "common/dout.h"
#include "rgw_acl_types.h"
#include "rgw_auth.h"
#include "rgw_common.h"
#include "rgw_iam_policy.h"
#include "rgw_keystone.h"

using rgw::auth::Identity;
using rgw::auth::Principal;

// ---------------------------------------------------------------------
// Part 1: from roles to permission mask.
//
// These tests check how the roles on a Keystone token become a
// permission mask. update_roles() resolves the token's roles to a single
// permission tier (TokenEnvelope::perm_tier): a normal user gets full
// control; a user whose only accepted role is the reader role gets
// read-only; a user whose only accepted role is an implicit-deny role gets
// no permissions at all (mask 0); a user with no accepted role is not
// admitted at all. effective_perm_mask() returns that tier (or
// RGW_PERM_NONE), and admitted() reports whether any accepted role was
// present. A role that is in no list must have no effect.
// ---------------------------------------------------------------------

// Make a test token with the given role names, then flag the roles
// with fixed config lists, like the auth engines do.
static rgw::keystone::TokenEnvelope make_keystone_token(
    std::initializer_list<std::string> role_names)
{
  rgw::keystone::TokenEnvelope t;
  for (const auto& name : role_names) {
    rgw::keystone::TokenEnvelope::Role r;
    r.name = name;
    t.roles.push_back(std::move(r));
  }
  // same lists the auth engines read from the config; an admin role
  // also counts as accepted
  t.update_roles({"member", "objectstore_viewer", "objectstore_authed",
                  "admin"},                                  // plain
                 {"admin"},                                  // admin
                 {},                                         // system_reader
                 {"objectstore_viewer"},                     // project_reader
                 {"objectstore_authed"});                    // implicit_deny
  return t;
}

TEST(KeystoneProjectReader, UpdateRolesFlags)
{
  auto t = make_keystone_token({"objectstore_viewer", "member", "admin",
                                "objectstore_authed", "unlisted_role"});
  // Only the admin/system-reader personas survive as per-role flags; the
  // permission tier is now a single token-level result.
  for (const auto& r : t.roles) {
    EXPECT_EQ(r.name == "admin", r.is_admin);
    EXPECT_FALSE(r.is_system_reader);   // no system_reader role in the lists
  }
  // the token carries accepted roles (member, admin) -> admitted, full control
  EXPECT_TRUE(t.admitted());
  EXPECT_EQ(RGW_PERM_FULL_CONTROL, t.effective_perm_mask());
}

TEST(KeystoneProjectReader, ReaderOnly)
{
  // only the reader role: the user is read-only (data + config reads)
  EXPECT_EQ(RGW_PERM_READ | RGW_PERM_READ_ACP,
            make_keystone_token({"objectstore_viewer"}).effective_perm_mask());
}

TEST(KeystoneProjectReader, UnrelatedRoleIgnored)
{
  // an extra unknown role must not turn off the read-only cap
  EXPECT_EQ(RGW_PERM_READ | RGW_PERM_READ_ACP,
            make_keystone_token({"objectstore_viewer", "unlisted_role"})
                .effective_perm_mask());
}

TEST(KeystoneProjectReader, MemberWins)
{
  // member is a normal accepted role: it gives full control, so the
  // reader cap does not apply
  EXPECT_EQ(RGW_PERM_FULL_CONTROL,
            make_keystone_token({"objectstore_viewer", "member"})
                .effective_perm_mask());
}

TEST(KeystoneProjectReader, AdminWins)
{
  EXPECT_EQ(RGW_PERM_FULL_CONTROL,
            make_keystone_token({"objectstore_viewer", "admin"})
                .effective_perm_mask());
}

TEST(KeystoneProjectReader, NoReaderRole)
{
  // a plain accepted role (member) grants full control
  auto member = make_keystone_token({"member"});
  EXPECT_TRUE(member.admitted());
  EXPECT_EQ(RGW_PERM_FULL_CONTROL, member.effective_perm_mask());
  // No accepted role (or no roles at all) -> not admitted, zero permissions.
  // We fail closed: deny by default, never full control. In a real request a
  // user with no accepted role is already rejected at login before this runs,
  // so this is just a safety net.
  auto unlisted = make_keystone_token({"unlisted_role"});
  EXPECT_FALSE(unlisted.admitted());
  EXPECT_EQ(RGW_PERM_NONE, unlisted.effective_perm_mask());
  auto none = make_keystone_token({});
  EXPECT_FALSE(none.admitted());
  EXPECT_EQ(RGW_PERM_NONE, none.effective_perm_mask());
}

TEST(KeystoneImplicitDeny, AuthedOnly)
{
  // only the implicit-deny role: admitted (it is an accepted role) but with
  // no permissions at all -- distinct from an unadmitted token.
  auto t = make_keystone_token({"objectstore_authed"});
  EXPECT_TRUE(t.admitted());
  EXPECT_EQ(RGW_PERM_NONE, t.effective_perm_mask());
}

TEST(KeystoneImplicitDeny, UnrelatedRoleIgnored)
{
  // an extra unknown role must not lift the cap
  EXPECT_EQ(RGW_PERM_NONE,
            make_keystone_token({"objectstore_authed", "unlisted_role"})
                .effective_perm_mask());
}

TEST(KeystoneImplicitDeny, ReaderWins)
{
  // the most permissive accepted role wins: reader beats implicit-deny
  EXPECT_EQ(RGW_PERM_READ | RGW_PERM_READ_ACP,
            make_keystone_token({"objectstore_authed", "objectstore_viewer"})
                .effective_perm_mask());
}

TEST(KeystoneImplicitDeny, MemberWins)
{
  EXPECT_EQ(RGW_PERM_FULL_CONTROL,
            make_keystone_token({"objectstore_authed", "member"})
                .effective_perm_mask());
}

TEST(KeystoneImplicitDeny, AdminWins)
{
  EXPECT_EQ(RGW_PERM_FULL_CONTROL,
            make_keystone_token({"objectstore_authed", "admin"})
                .effective_perm_mask());
}

// ---------------------------------------------------------------------
// Part 2: enforcement of the mask.
//
// Part 1 proved which mask a token gets. The tests below prove what
// that mask does during a request, using the real
// verify_bucket_permission(): the bucket policy is checked first and
// can override the mask; without a policy match the mask limits what
// the ACL can give.
// ---------------------------------------------------------------------

// A fake user identity, like the one the Keystone auth code builds for
// a capped user: the type is Keystone, the perm_mask is fixed, and it
// is not an admin and not an owner. The Identity interface requires
// every method below. Only three of them matter for the tests:
//   get_perm_mask()        -> the cap we are testing
//   get_identity_type()    -> Keystone by default; overridable so a
//                             non-Keystone identity can be tested too
//   get_perms_from_aclspec -> what a bucket ACL gives this user
class CappedKeystoneIdentity : public Identity {
  const std::string id;
  const uint32_t perm_mask;
  const uint32_t itype;
  const bool owns;
public:
  CappedKeystoneIdentity(std::string id, uint32_t perm_mask,
                         uint32_t itype = TYPE_KEYSTONE, bool owns = false)
    : id(std::move(id)), perm_mask(perm_mask), itype(itype), owns(owns) {}

  ACLOwner get_aclowner() const override { return {}; }
  uint32_t get_perms_from_aclspec(const DoutPrefixProvider*,
                                  const aclspec_t& aclspec) const override {
    const auto iter = aclspec.find(id);
    return (iter != aclspec.end()) ? iter->second : 0;
  }
  bool is_admin() const override { return false; }
  bool is_owner_of(const rgw_owner&) const override { return owns; }
  bool is_root() const override { return false; }
  uint32_t get_perm_mask() const override { return perm_mask; }
  void to_str(std::ostream& out) const override { out << id; }
  bool is_identity(const Principal& p) const override {
    return p.is_wildcard();
  }
  uint32_t get_identity_type() const override { return itype; }
  std::optional<rgw::ARN> get_caller_identity() const override {
    return std::nullopt;
  }
  std::string get_acct_name() const override { return {}; }
  std::string get_subuser() const override { return {}; }
  const std::string& get_tenant() const override {
    static const std::string no_tenant;
    return no_tenant;
  }
  const std::optional<RGWAccountInfo>& get_account() const override {
    static const std::optional<RGWAccountInfo> no_account;
    return no_account;
  }
};

// Shared setup for the tests below: a CephContext and a helper that
// runs one PutObject permission check through the real
// verify_bucket_permission(). Each test picks its own inputs.
class KeystoneCapEnforcement : public ::testing::Test {
protected:
  static constexpr const char* USERID = "6d2c1863b0a94379b4f5ab5d78546f45";

  boost::intrusive_ptr<CephContext> cct;
  const std::string arbitrary_tenant;
  RGWBucketInfo bucket_info;  // requester_pays is false by default

  KeystoneCapEnforcement() {
    cct.reset(new CephContext(CEPH_ENTITY_TYPE_CLIENT), false);
  }

  // bucket policy: allow s3:PutObject on testbucket/* only for USERID
  rgw::IAM::Policy allow_put_for_userid() {
    static const std::string text = R"({
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Principal": "*",
        "Action": "s3:PutObject",
        "Resource": "arn:aws:s3:::testbucket/*",
        "Condition": {"StringEquals": {"keystone:userid": ")"
        + std::string(USERID) + R"("}}
      }]
    })";
    return rgw::IAM::Policy(cct.get(), &arbitrary_tenant, text, true);
  }

  // Check if PutObject on testbucket/obj is allowed with the given
  // mask, request environment, bucket policy and bucket ACL.
  // perm_state holds the request data the permission code reads; in
  // the real server it is filled in after authentication.
  bool put_allowed(uint32_t perm_mask, const rgw::IAM::Environment& env,
                   const boost::optional<rgw::IAM::Policy>& bucket_policy,
                   const RGWAccessControlPolicy& bucket_acl = {}) {
    CappedKeystoneIdentity identity(USERID, perm_mask);
    perm_state ps(cct.get(), env, &identity, bucket_info,
                  rgw::s3::ObjectOwnership::ObjectWriter,
                  perm_mask, false /* defer_to_bucket_acls */,
                  nullptr /* referer */, false /* request_payer */);
    const NoDoutPrefix ndp(cct.get(), ceph_subsys_rgw);
    rgw_bucket b;
    b.name = "testbucket";
    return verify_bucket_permission(&ndp, &ps, rgw::ARN(b, "obj"), false,
                                    {} /* user_acl */, bucket_acl,
                                    bucket_policy, {}, {},
                                    rgw::IAM::s3PutObject);
  }
};

TEST_F(KeystoneCapEnforcement, MaskDeniesWriteWithoutPolicy)
{
  const rgw::IAM::Environment env = {{"keystone:userid", USERID}};
  // no policy: the read-only mask blocks the write, and the
  // implicit-deny mask (0) blocks it too
  EXPECT_FALSE(put_allowed(RGW_PERM_READ, env, boost::none));
  EXPECT_FALSE(put_allowed(RGW_PERM_NONE, env, boost::none));
}

TEST_F(KeystoneCapEnforcement, PolicyAllowOverridesMask)
{
  const rgw::IAM::Environment env = {{"keystone:userid", USERID}};
  // the policy allows this user, so the write works even though the
  // mask is read-only: the policy is checked before the mask
  EXPECT_TRUE(put_allowed(RGW_PERM_READ, env, allow_put_for_userid()));
  EXPECT_TRUE(put_allowed(RGW_PERM_NONE, env, allow_put_for_userid()));
}

TEST_F(KeystoneCapEnforcement, PolicyGrantIsScopedToNamedUser)
{
  // a different user id: the policy does not match, so the mask blocks
  // the write as usual
  const rgw::IAM::Environment env = {{"keystone:userid", "someone-else"}};
  EXPECT_FALSE(put_allowed(RGW_PERM_READ, env, allow_put_for_userid()));
  EXPECT_FALSE(put_allowed(RGW_PERM_NONE, env, allow_put_for_userid()));
}

TEST_F(KeystoneCapEnforcement, AclGrantCannotExceedMask)
{
  const rgw::IAM::Environment env = {{"keystone:userid", USERID}};
  // an ACL that grants full control must not beat the read-only mask
  RGWAccessControlPolicy bucket_acl;
  ACLGrant grant;
  grant.set_canon(rgw_user(USERID), "display", RGW_PERM_FULL_CONTROL);
  bucket_acl.get_acl().add_grant(grant);
  EXPECT_FALSE(put_allowed(RGW_PERM_READ, env, boost::none, bucket_acl));
  EXPECT_FALSE(put_allowed(RGW_PERM_NONE, env, boost::none, bucket_acl));
  // sanity check: with a full mask the same ACL does allow the write
  EXPECT_TRUE(put_allowed(RGW_PERM_FULL_CONTROL, env, boost::none,
                          bucket_acl));
}

// The account-scoped default-allow path: verify_user_permission_no_policy()
// returns true for an empty user ACL (the "you own the account" shortcut that
// CreateBucket and Swift account-metadata writes rely on). The
// is_capped_keystone_identity() gate must block a capped Keystone identity
// there, and must NOT touch a non-Keystone identity.
TEST_F(KeystoneCapEnforcement, GateBlocksAccountScopedWriteForCappedKeystone)
{
  CappedKeystoneIdentity reader(USERID, RGW_PERM_READ, TYPE_KEYSTONE);
  const rgw::IAM::Environment env;
  perm_state ps(cct.get(), env, &reader, bucket_info,
                rgw::s3::ObjectOwnership::ObjectWriter,
                RGW_PERM_READ, false, nullptr, false);
  const NoDoutPrefix ndp(cct.get(), ceph_subsys_rgw);
  const RGWAccessControlPolicy empty_user_acl;
  // a write the read-only mask lacks is denied even with an empty user ACL
  EXPECT_FALSE(verify_user_permission_no_policy(&ndp, &ps, empty_user_acl,
                                                RGW_PERM_WRITE));
}

TEST_F(KeystoneCapEnforcement, NonKeystoneReducedMaskNotCapped)
{
  // Non-regression: the cap is scoped to Keystone identities. A non-Keystone
  // identity with a reduced mask (e.g. a Swift read-only subuser of a local
  // user) must still pass the account-scoped shortcut -- this is what the
  // TYPE_KEYSTONE check in is_capped_keystone_identity() protects.
  CappedKeystoneIdentity local_subuser(USERID, RGW_PERM_READ, TYPE_RGW);
  const rgw::IAM::Environment env;
  perm_state ps(cct.get(), env, &local_subuser, bucket_info,
                rgw::s3::ObjectOwnership::ObjectWriter,
                RGW_PERM_READ, false, nullptr, false);
  const NoDoutPrefix ndp(cct.get(), ceph_subsys_rgw);
  const RGWAccessControlPolicy empty_user_acl;
  EXPECT_TRUE(verify_user_permission_no_policy(&ndp, &ps, empty_user_acl,
                                               RGW_PERM_WRITE));
}

// ---------------------------------------------------------------------
// Part 3: the shared owner-grant primitive, verify_owner_permission().
//
// A capped identity owns its project's resources, so ownership alone must
// not let it perform a write while read-class operations still pass. This
// is the single place an owner-based grant consults the cap; the SNS topic
// path (verify_topic_permission) and bucket mdsearch both funnel through
// it, so these tests cover the SNS Set/Delete/CreateTopic-overwrite bypass
// and the mdsearch owner gates at once.
// ---------------------------------------------------------------------

// op_to_perm() must classify the SNS actions that reach the owner grant,
// otherwise they would fall through to RGW_PERM_INVALID and the primitive
// could not tell a topic read from a topic write.
TEST(KeystoneOwnerCap, SnsOpsClassified)
{
  using namespace rgw::IAM;
  EXPECT_EQ(RGW_PERM_READ,  op_to_perm(snsGetTopicAttributes));
  EXPECT_EQ(RGW_PERM_READ,  op_to_perm(snsListTopics));
  EXPECT_EQ(RGW_PERM_WRITE, op_to_perm(snsCreateTopic));
  EXPECT_EQ(RGW_PERM_WRITE, op_to_perm(snsSetTopicAttributes));
  EXPECT_EQ(RGW_PERM_WRITE, op_to_perm(snsDeleteTopic));
  EXPECT_EQ(RGW_PERM_WRITE, op_to_perm(snsPublish));
}

TEST(KeystoneOwnerCap, ReaderOwnerReadAllowedWriteDenied)
{
  using namespace rgw::IAM;
  const rgw_owner owner;
  const uint32_t mask = RGW_PERM_READ | RGW_PERM_READ_ACP;
  // a project reader (Keystone, read-only mask) that owns the resource
  CappedKeystoneIdentity reader("u", mask, TYPE_KEYSTONE, true /* owns */);

  // reads through ownership pass ...
  EXPECT_TRUE(verify_owner_permission(reader, mask, owner,
                                      op_to_perm(snsGetTopicAttributes)));
  // ... writes do not: this is the SNS owner-fallback bypass being fixed
  EXPECT_FALSE(verify_owner_permission(reader, mask, owner,
                                       op_to_perm(snsSetTopicAttributes)));
  EXPECT_FALSE(verify_owner_permission(reader, mask, owner,
                                       op_to_perm(snsDeleteTopic)));
  EXPECT_FALSE(verify_owner_permission(reader, mask, owner,
                                       op_to_perm(snsCreateTopic)));
  // Publish is a write too: a read-only owner may not publish through the
  // topic owner fallback (verify_topic_permission gates the publish
  // default-allow on the cap).
  EXPECT_FALSE(verify_owner_permission(reader, mask, owner,
                                       op_to_perm(snsPublish)));
}

TEST(KeystoneOwnerCap, NonOwnerDenied)
{
  const rgw_owner owner;
  const uint32_t mask = RGW_PERM_READ | RGW_PERM_READ_ACP;
  // the same reader, but it does not own the resource: denied even a read
  CappedKeystoneIdentity reader("u", mask, TYPE_KEYSTONE, false /* owns */);
  EXPECT_FALSE(verify_owner_permission(reader, mask, owner, RGW_PERM_READ));
}

TEST(KeystoneOwnerCap, FullControlOwnerWriteAllowed)
{
  const rgw_owner owner;
  // a normal (uncapped) owner: full control, so the write passes
  CappedKeystoneIdentity user("u", RGW_PERM_FULL_CONTROL, TYPE_KEYSTONE,
                              true /* owns */);
  EXPECT_TRUE(verify_owner_permission(user, RGW_PERM_FULL_CONTROL, owner,
                                      RGW_PERM_WRITE));
}

TEST(KeystoneOwnerCap, ImplicitDenyOwnerGetsNothing)
{
  const rgw_owner owner;
  // implicit-deny mask (0): even a read is refused through ownership alone
  CappedKeystoneIdentity authed("u", RGW_PERM_NONE, TYPE_KEYSTONE,
                                true /* owns */);
  EXPECT_FALSE(verify_owner_permission(authed, RGW_PERM_NONE, owner,
                                       RGW_PERM_READ));
}

TEST(KeystoneOwnerCap, NonKeystoneReducedMaskOwnerNotCapped)
{
  const rgw_owner owner;
  // non-regression: the cap is scoped to Keystone identities, so a
  // non-Keystone owner with a reduced mask still grants by ownership
  CappedKeystoneIdentity local("u", RGW_PERM_READ, TYPE_RGW, true /* owns */);
  EXPECT_TRUE(verify_owner_permission(local, RGW_PERM_READ, owner,
                                      RGW_PERM_WRITE));
}
