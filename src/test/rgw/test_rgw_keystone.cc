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
#include "include/timegm.h"
#include "rgw_auth_keystone.h"
#include "rgw_b64.h"
#include <thread>
#include <atomic>
#include <cstring>
#include <set>
#include <sstream>
#include <stdexcept>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/read.hpp>
#include <sys/socket.h>
#include <sys/time.h>
#include <boost/asio/write.hpp>
#include <boost/asio/streambuf.hpp>
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "rgw_process_env.h"
#include "rgw_auth_s3.h"
#include "rgw_auth_registry.h"

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
                                    rgw::IAM::s3PutObject,
                                    false /* cross_account */);
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

namespace {
using rgw::auth::keystone::SigningKeyCache;
using rgw::auth::keystone::S3ProofResult;
using rgw::auth::keystone::decode_s3_proof_response;
using rgw::auth::keystone::validate_s3_v4_proof;

const std::string proof_identity = R"({
  "user":{"id":"user","name":"User","domain":{"id":"domain","name":"Domain"}},
  "project":{"id":"project","name":"Project","domain":{"id":"domain","name":"Domain"}},
  "roles":[{"id":"role","name":"member"}],"methods":["ec2credential"],
  "credential_id":"opaque-credential-not-a-token","consumer":"s3",
  "expires_at":"2030-01-02T03:04:05Z"
})";

SigningKeyCache::Key sample_scope()
{
  return {"access", "20300102", "region", "s3", "https://keystone.example/"};
}

std::string key_response(const std::string& identity = proof_identity,
                         const std::string& encoded = std::string(43, 'A') + "=")
{
  return "{\"authentication\":" + identity +
         ",\"scope\":{\"date\":\"20300102\",\"region\":\"region\",\"service\":\"s3\"}," +
         "\"signing_key\":\"" + encoded + "\",\"cache_until\":\"2030-01-02T03:04:05Z\"}";
}

std::string replaced(std::string source, const std::string& before,
                      const std::string& after)
{
  auto pos = source.find(before);
  if (pos == std::string::npos) {
    ADD_FAILURE() << "test fixture substitution missing";
    return source;
  }
  source.replace(pos, before.size(), after);
  return source;
}

TEST(KeystoneS3Proof, IdentityIsNotABearerToken)
{
  S3ProofResult result;
  ASSERT_EQ(0, decode_s3_proof_response(key_response(), true, sample_scope(), result));
  ASSERT_TRUE(result.signing_key);
  EXPECT_TRUE(result.identity.token.id.empty());
  EXPECT_FALSE(result.identity.app_cred);
  EXPECT_EQ("user", result.identity.get_user_id());
  EXPECT_EQ("project", result.identity.get_project_id());
  EXPECT_EQ("domain", result.identity.user.domain.id);
  EXPECT_EQ("opaque-credential-not-a-token", result.credential_id);
  EXPECT_EQ(result.cache_until, result.identity.get_expires());
  result.identity.update_roles({"member"}, {}, {}, {}, {});
  EXPECT_TRUE(result.identity.admitted());
  EXPECT_EQ(RGW_PERM_FULL_CONTROL, result.identity.effective_perm_mask());
}

TEST(KeystoneS3Proof, BinaryKeyPreservesNulAndHighBits)
{
  std::string binary(32, '\0');
  binary[1] = '\xff';
  binary[31] = '\x80';
  S3ProofResult result;
  ASSERT_EQ(0, decode_s3_proof_response(key_response(proof_identity,
      rgw::to_base64(binary)), true, sample_scope(), result));
  ASSERT_TRUE(result.signing_key);
  EXPECT_EQ(0, std::memcmp(result.signing_key->v, binary.data(), 32));
}

TEST(KeystoneS3Proof, RejectsWrongScalarDuplicateAndMissingDomain)
{
  const std::vector<std::string> bad = {
    replaced(proof_identity, "\"id\":\"user\"", "\"id\":42"),
    replaced(proof_identity, "\"id\":\"user\"", "\"id\":\"user\",\"id\":\"other\""),
    replaced(proof_identity, "\"name\":\"User\",\"domain\":{\"id\":\"domain\",\"name\":\"Domain\"}", "\"name\":\"User\""),
    replaced(proof_identity, "[\"ec2credential\"]", "[\"password\"]"),
    replaced(proof_identity, "[\"ec2credential\"]", "[\"ec2credential\",\"password\"]"),
    replaced(proof_identity, "\"consumer\":\"s3\"", "\"consumer\":\"heat-cfn\""),
    replaced(proof_identity, "\"consumer\":\"s3\"", "\"consumer\":\"s3\",\"token\":\"bearer\""),
    replaced(proof_identity, "\"consumer\":\"s3\"", "\"consumer\":\"s3\",\"application_credential\":{}"),
    replaced(proof_identity, "\"roles\":[{\"id\":\"role\",\"name\":\"member\"}]", "\"roles\":{}"),
    replaced(proof_identity, "2030-01-02T03:04:05Z", "2030-02-30T03:04:05Z")
  };
  for (const auto& identity : bad) {
    S3ProofResult result;
    EXPECT_LT(decode_s3_proof_response(key_response(identity), true, sample_scope(), result), 0);
  }
}

TEST(KeystoneS3Proof, EveryStringFieldRejectsJsonScalarCoercion)
{
  const std::vector<std::string> fields = {
    "\"id\":\"user\"", "\"name\":\"User\"", "\"id\":\"project\"",
    "\"name\":\"Project\"", "\"id\":\"domain\"", "\"name\":\"Domain\"",
    "\"id\":\"role\"", "\"name\":\"member\"",
    "\"credential_id\":\"opaque-credential-not-a-token\"",
    "\"consumer\":\"s3\"", "\"expires_at\":\"2030-01-02T03:04:05Z\""
  };
  for (const auto& member : fields) {
    const auto colon = member.find(':');
    for (const auto scalar : {"null", "0", "false", "[]", "{}", "\"\""}) {
      S3ProofResult result;
      EXPECT_LT(decode_s3_proof_response(key_response(replaced(proof_identity,
          member, member.substr(0, colon + 1) + scalar)), true, sample_scope(), result), 0);
    }
  }
}

TEST(KeystoneS3Proof, DecoderFailureClearsPoisonedBearerAndCanRecover)
{
  rgw::keystone::TokenEnvelope identity;
  identity.token.id = "must-not-survive";
  identity.user.id = "wrong-user";
  identity.project.id = "wrong-project";
  identity.perm_tier = RGW_PERM_FULL_CONTROL;
  identity.app_cred.emplace();
  identity.app_cred->id = "delegated";
  std::string credential = "stale";
  JSONParser bad;
  const auto malformed = replaced(proof_identity, "[\"ec2credential\"]", "[\"token\"]");
  ASSERT_TRUE(bad.parse(malformed.data(), malformed.size()));
  EXPECT_EQ(-EINVAL, identity.decode_s3_authentication(&bad, credential));
  EXPECT_TRUE(identity.token.id.empty());
  EXPECT_TRUE(identity.user.id.empty());
  EXPECT_TRUE(identity.project.id.empty());
  EXPECT_TRUE(identity.roles.empty());
  EXPECT_FALSE(identity.app_cred);
  EXPECT_FALSE(identity.perm_tier);
  EXPECT_TRUE(credential.empty());
  JSONParser good;
  ASSERT_TRUE(good.parse(proof_identity.data(), proof_identity.size()));
  EXPECT_EQ(0, identity.decode_s3_authentication(&good, credential));
  EXPECT_EQ("user", identity.get_user_id());
  EXPECT_TRUE(identity.token.id.empty());
}

TEST(KeystoneS3Proof, GregorianUtcDatesAndFractions)
{
  for (const auto date : {"2000-02-29T23:59:59Z", "2024-02-29T01:02:03.123456789Z"}) {
    const auto input = replaced(proof_identity, "2030-01-02T03:04:05Z", date);
    JSONParser parser;
    ASSERT_TRUE(parser.parse(input.data(), input.size()));
    rgw::keystone::TokenEnvelope identity;
    std::string credential;
    EXPECT_EQ(0, identity.decode_s3_authentication(&parser, credential));
  }
  for (const auto date : {"2100-02-29T03:04:05Z", "2030-04-31T03:04:05Z",
       "0000-01-02T03:04:05Z", "2030-01-02T24:04:05Z", "2030-01-02T03:60:05Z",
       "2030-01-02T03:04:60Z", "2030-01-02T03:04:05", "2030-01-02T03:04:05+00:00",
       "2030-01-02T03:04:05.Z", "2030-01-02T03:04:05.1234567890Z",
       "2030-01-02T03:04:05Zjunk", "2030-01-02t03:04:05z"}) {
    S3ProofResult result;
    EXPECT_LT(decode_s3_proof_response(key_response(replaced(proof_identity,
        "2030-01-02T03:04:05Z", date)), true, sample_scope(), result), 0);
  }
}

TEST(KeystoneS3Proof, StrictOuterShapeScopeDeadlineAndBase64)
{
  std::vector<std::string> bad = {
    key_response() + "{}",
    replaced(key_response(), "\"signing_key\":", "\"token\":\"bearer\",\"signing_key\":"),
    replaced(key_response(), "\"region\":\"region\"", "\"region\":\"different\""),
    replaced(key_response(), "\"service\":\"s3\"", "\"service\":\"sts\""),
    replaced(key_response(), "\"cache_until\":\"2030-01-02T03:04:05Z\"", "\"cache_until\":\"2030-01-02T03:04:06Z\""),
    key_response(proof_identity, rgw::to_base64(std::string(31, 'x'))),
    key_response(proof_identity, rgw::to_base64(std::string(33, 'x'))),
    key_response(proof_identity, std::string(43, '_') + "="),
    key_response(proof_identity, std::string(44, 'A'))
  };
  for (const auto& body : bad) {
    S3ProofResult result;
    EXPECT_LT(decode_s3_proof_response(body, true, sample_scope(), result), 0);
  }
}

TEST(KeystoneS3Proof, StrictJsonGrammarRejectsRepeatedAndTrailingCommas)
{
  const std::vector<std::string> bad = {
    replaced(key_response(), "\"methods\":[\"ec2credential\"]", "\"methods\":[\"ec2credential\",,]"),
    replaced(key_response(), "\"methods\":[\"ec2credential\"]", "\"methods\":[\"ec2credential\",]"),
    replaced(key_response(), "\"consumer\":\"s3\",", "\"consumer\":\"s3\",,"),
    replaced(key_response(), "\"cache_until\":\"2030-01-02T03:04:05Z\"}", "\"cache_until\":\"2030-01-02T03:04:05Z\",}"),
    replaced(key_response(), "\"user\":", "user:"),
    replaced(key_response(), "\"credential_id\":", "/* comment */\"credential_id\":"),
    key_response() + " false",
    key_response() + std::string(1, '\0'),
    replaced(key_response(), "opaque-credential-not-a-token", std::string(1, '\xff'))
  };
  for (size_t i = 0; i < bad.size(); ++i) {
    SCOPED_TRACE(i);
    S3ProofResult result;
    EXPECT_LT(decode_s3_proof_response(bad[i], true, sample_scope(), result), 0);
  }
  S3ProofResult result;
  EXPECT_EQ(0, decode_s3_proof_response(key_response(replaced(proof_identity,
      "opaque-credential-not-a-token", "valid-commas,,inside-string")), true, sample_scope(), result));
}

TEST(KeystoneS3Proof, ValidationReplyHasIdentityButNoKeyOrCache)
{
  S3ProofResult result;
  ASSERT_EQ(0, decode_s3_proof_response("{\"authentication\":" + proof_identity + "}",
                                      false, sample_scope(), result));
  EXPECT_FALSE(result.signing_key);
  EXPECT_EQ(0, result.cache_until);
  EXPECT_LT(decode_s3_proof_response(key_response(), false, sample_scope(), result), 0);
}

std::string v4_proof(const std::string& stamp, const std::string& date = "20300102")
{
  return "AWS4-HMAC-SHA256\n" + stamp + "\n" + date +
         "/region/s3/aws4_request\n" + std::string(64, 'a');
}

TEST(KeystoneS3Proof, EveryRequestTimeAndPresignedBoundaries)
{
  std::tm t{};
  ASSERT_TRUE(parse_iso8601("20300102T030405Z", &t, nullptr, false));
  const auto now = internal_timegm(&t);
  auto scope = sample_scope();
  EXPECT_EQ(0, validate_s3_v4_proof(v4_proof("20300102T030405Z"), false, "", now, scope));
  EXPECT_EQ(0, validate_s3_v4_proof(v4_proof("20300102T030905Z"), false, "", now, scope));
  EXPECT_LT(validate_s3_v4_proof(v4_proof("20300102T030906Z"), false, "", now, scope), 0);
  EXPECT_LT(validate_s3_v4_proof(v4_proof("20300102T025904Z"), false, "", now, scope), 0);
  EXPECT_EQ(0, validate_s3_v4_proof(v4_proof("20291227T030405Z", "20291227"), true, "604800", now, scope));
  EXPECT_LT(validate_s3_v4_proof(v4_proof("20291226T030405Z", "20291226"), true, "604800", now, scope), 0);
  for (const auto expiry : {"0", "604801", "10junk", "-1", "99999999999999999999999"}) {
    EXPECT_LT(validate_s3_v4_proof(v4_proof("20300102T030405Z"), true, expiry, now, scope), 0);
  }
  EXPECT_LT(validate_s3_v4_proof(v4_proof("20300230T030405Z", "20300230"), true, "600", now, scope), 0);
  EXPECT_LT(validate_s3_v4_proof(replaced(v4_proof("20300102T030405Z"), "/s3/", "/sts/"), false, "", now, scope), 0);
  EXPECT_LT(validate_s3_v4_proof(replaced(v4_proof("20300102T030405Z"), "aws4_request", "aws4_requestjunk"), false, "", now, scope), 0);
}

SigningKeyCache::Value cache_value(time_t expires, SigningKeyCache::mono_time mono)
{
  SigningKeyCache::Value value;
  value.identity.set_expires(expires);
  value.wall_expires = expires;
  value.mono_expires = mono;
  return value;
}

TEST(KeystoneSigningCache, ExpiryEqualityAndClockRollback)
{
  SigningKeyCache cache(2);
  const auto mono = SigningKeyCache::mono_time{};
  ASSERT_TRUE(cache.insert(sample_scope(), cache_value(100, mono + std::chrono::seconds(10)), 90, mono));
  EXPECT_TRUE(cache.find(sample_scope(), 99, mono + std::chrono::seconds(9)));
  EXPECT_FALSE(cache.find(sample_scope(), 80, mono + std::chrono::seconds(10)));
  ASSERT_TRUE(cache.insert(sample_scope(), cache_value(100, mono + std::chrono::seconds(10)), 90, mono));
  EXPECT_FALSE(cache.find(sample_scope(), 100, mono));
  EXPECT_EQ(0, cache.size());
}

TEST(KeystoneSigningCache, CompositeIsolationAndLru)
{
  SigningKeyCache cache(2);
  const auto mono = SigningKeyCache::mono_time{};
  const auto value = cache_value(100, mono + std::chrono::seconds(10));
  auto first = sample_scope();
  ASSERT_TRUE(cache.insert(first, value, 90, mono));
  auto second = first;
  second.region = "other";
  ASSERT_TRUE(cache.insert(second, value, 90, mono));
  EXPECT_TRUE(cache.find(first, 90, mono));
  auto third = first;
  third.endpoint = "https://another.example/";
  ASSERT_TRUE(cache.insert(third, value, 90, mono));
  EXPECT_FALSE(cache.find(second, 90, mono));
  EXPECT_TRUE(cache.find(first, 90, mono));
  for (unsigned n = 0; n < 4; ++n) {
    auto other = first;
    if (n == 0) other.access += "x";
    if (n == 1) other.date += "x";
    if (n == 2) other.service = "sts";
    if (n == 3) other.region += "x";
    EXPECT_FALSE(cache.find(other, 90, mono));
  }
  EXPECT_EQ(2, cache.size());
}

TEST(KeystoneSigningCache, ZeroOffAndConcurrentReplacement)
{
  const auto mono = SigningKeyCache::mono_time{};
  const auto value = cache_value(100, mono + std::chrono::seconds(10));
  SigningKeyCache disabled(0);
  EXPECT_FALSE(disabled.insert(sample_scope(), value, 90, mono));
  SigningKeyCache cache(2);
  std::vector<std::thread> threads;
  for (unsigned n = 0; n < 4; ++n) {
    threads.emplace_back([&] {
      for (unsigned i = 0; i < 100; ++i) {
        cache.insert(sample_scope(), value, 90, mono);
        EXPECT_TRUE(cache.find(sample_scope(), 90, mono));
      }
    });
  }
  for (auto& thread : threads) thread.join();
  EXPECT_EQ(1, cache.size());
}

std::string utc(time_t stamp, const char* format)
{
  std::tm t{};
  gmtime_r(&stamp, &t);
  char text[64];
  std::strftime(text, sizeof(text), format, &t);
  return text;
}

class ProofConfig : public rgw::keystone::Config {
public:
  std::string endpoint;
  static ProofConfig& get_instance() { static ProofConfig config; return config; }
  std::string get_endpoint_url() const noexcept override { return endpoint; }
  std::string_view get_admin_user() const noexcept override { return "caller"; }
  std::string get_admin_password() const noexcept override { return "synthetic-password"; }
  std::string_view get_admin_tenant() const noexcept override { return "project"; }
  std::string_view get_admin_project() const noexcept override { return "project"; }
  std::string_view get_admin_domain() const noexcept override { return "domain"; }
};

// A real loopback HTTP peer. Each accepted client is our bounded RGW client;
// the accept loop is nonblocking and joined, never a detached fixture process.
class ProofServer {
  using tcp = boost::asio::ip::tcp;
  boost::asio::io_context io;
  tcp::acceptor acceptor{io, tcp::endpoint(boost::asio::ip::address_v4::loopback(), 0)};
  const unsigned short port = acceptor.local_endpoint().port();
  std::atomic<bool> stopping{false};
  std::thread thread;
  mutable std::mutex mutex;
public:
  struct RecordedRequest { std::string method, path, body, caller; };
private:
  std::vector<RecordedRequest> requests;
  std::function<void()> after_proof;

  void serve() {
    while (!stopping.load()) {
      tcp::socket socket(io);
      boost::system::error_code ec;
      acceptor.accept(socket, ec);
      if (ec == boost::asio::error::would_block || ec == boost::asio::error::try_again) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        continue;
      }
      if (ec) return;
      timeval timeout{2, 0};
      setsockopt(socket.native_handle(), SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
      try {
        boost::asio::streambuf input;
        boost::asio::read_until(socket, input, "\r\n\r\n");
        std::istream stream(&input);
        std::string method, path, version, line, caller;
        stream >> method >> path >> version;
        std::getline(stream, line);
        size_t length = 0;
        while (std::getline(stream, line) && line != "\r") {
          auto lower = line;
          std::transform(lower.begin(), lower.end(), lower.begin(),
                         [](unsigned char c) { return std::tolower(c); });
          if (lower.starts_with("content-length:")) length = std::stoul(line.substr(15));
          if (lower.starts_with("x-auth-token:")) {
            caller = line.substr(13);
            const auto first = caller.find_first_not_of(" \t");
            const auto last = caller.find_last_not_of(" \t\r\n");
            caller = first == std::string::npos ? "" : caller.substr(first, last - first + 1);
          }
        }
        std::string body(length, '\0');
        size_t have = std::min(length, input.size());
        stream.read(body.data(), have);
        if (have < length) boost::asio::read(socket, boost::asio::buffer(body.data() + have, length - have));
        {
          std::lock_guard guard(mutex);
          requests.push_back({method, path, body, caller});
        }
        if (path == "/v3/s3tokens/signing_key" || path == "/v3/s3tokens/validate") {
          if (method == "POST") {
            JSONParser schema;
            if (!schema.parse(body.data(), body.size())) throw std::runtime_error("proof JSON");
            unsigned root_fields = 0;
            for (auto i = schema.find_first(); !i.end(); ++i) {
              if ((*i)->get_name() != "credentials") throw std::runtime_error("proof wrapper");
              ++root_fields;
            }
            if (root_fields != 1) throw std::runtime_error("duplicate proof wrapper");
            auto credentials = *schema.find_first("credentials");
            std::set<std::string> names;
            for (auto i = credentials->find_first(); !i.end(); ++i) {
              if (!names.insert((*i)->get_name()).second || !(*i)->get_data_val().quoted) {
                throw std::runtime_error("proof fields");
              }
            }
            if (names != std::set<std::string>{"access", "token", "signature"}) {
              throw std::runtime_error("proof aliases are forbidden");
            }
            std::string access;
            JSONDecoder::decode_json("access", access, credentials, true);
            if (access != "access") throw std::runtime_error("unknown synthetic credential");
          }
        }
        unsigned status = 200;
        std::string response, extra;
        if (path.starts_with("/v3/s3tokens/") &&
            caller != "synthetic-cached-caller" && caller != "synthetic-new-caller") {
          status = 401;
        } else if (method == "OPTIONS") {
          ++options;
          status = options_status.load();
          extra = "Allow: POST, OPTIONS\r\n";
        } else if (path == "/v3/auth/tokens") {
          ++caller_tokens;
          status = 201;
          extra = "X-Subject-Token: synthetic-new-caller\r\n";
          auto caller_identity = replaced(proof_identity, "\"id\":\"user\"", "\"id\":\"service-caller\"");
          caller_identity = replaced(caller_identity, "\"id\":\"project\"", "\"id\":\"service-project\"");
          response = "{\"token\":" + replaced(caller_identity, "2030-01-02T03:04:05Z",
              utc(time(nullptr) + 300, "%Y-%m-%dT%H:%M:%SZ")) + "}";
        } else if (path == "/v3/s3tokens/signing_key") {
          ++key_calls;
          status = persistent_fault.load() ? post_status.load() : post_status.exchange(200);
          if (status == 200) {
            JSONParser parser;
            if (!parser.parse(body.data(), body.size())) throw std::runtime_error("JSON request");
            auto credentials = *parser.find_first("credentials");
            std::string encoded, signature;
            JSONDecoder::decode_json("token", encoded, credentials, true);
            JSONDecoder::decode_json("signature", signature, credentials, true);
            std::replace(encoded.begin(), encoded.end(), '-', '+');
            std::replace(encoded.begin(), encoded.end(), '_', '/');
            const auto proof = rgw::from_base64(encoded);
            const auto expected = calc_hmac_sha256(key, proof).to_str();
            if (signature != expected) {
              status = 403;
            } else {
              std::string date = proof.substr(proof.find('\n') + 1, 8);
              const auto expiry = utc(time(nullptr) + 300, "%Y-%m-%dT%H:%M:%SZ");
              response = key_response(replaced(proof_identity,
                  "2030-01-02T03:04:05Z", expiry), rgw::to_base64(
                      std::string_view(reinterpret_cast<const char*>(key.v), 32)));
              response = replaced(response, "\"date\":\"20300102\"", "\"date\":\"" + date + "\"");
              response = replaced(response, "\"cache_until\":\"2030-01-02T03:04:05Z\"", "\"cache_until\":\"" + expiry + "\"");
            }
          }
          if (status >= 300 && status < 400) {
            extra = "Location: " + endpoint() + "must-not-follow\r\n";
          }
          std::function<void()> hook;
          { std::lock_guard guard(mutex); hook = after_proof; }
          if (hook) hook();
        } else if (method == "POST" && path == "/v3/s3tokens") {
          // Existing legacy cache-mismatch behavior revalidates its proof.
          // Only the explicit default-legacy compatibility case expects this.
          ++legacy_calls;
          status = 401;
        } else if (path == "/v3/s3tokens/validate") {
          ++validation_calls;
          JSONParser parser;
          if (!parser.parse(body.data(), body.size())) throw std::runtime_error("JSON request");
          auto credentials = *parser.find_first("credentials");
          std::string encoded, signature;
          JSONDecoder::decode_json("token", encoded, credentials, true);
          JSONDecoder::decode_json("signature", signature, credentials, true);
          std::replace(encoded.begin(), encoded.end(), '-', '+');
          std::replace(encoded.begin(), encoded.end(), '_', '/');
          const auto digest = calc_hmac_sha1("synthetic-v2-secret", rgw::from_base64(encoded));
          const auto expected = rgw::to_base64(std::string_view(
              reinterpret_cast<const char*>(digest.v), digest.SIZE));
          if (signature != expected) {
            status = 403;
          } else {
            response = "{\"authentication\":" + replaced(proof_identity,
                "2030-01-02T03:04:05Z", utc(time(nullptr) + 300, "%Y-%m-%dT%H:%M:%SZ")) + "}";
          }
          std::function<void()> hook;
          { std::lock_guard guard(mutex); hook = after_proof; }
          if (hook) hook();
        } else {
          ++unexpected_calls;
          status = 404;
        }
        std::string wire = "HTTP/1.1 " + std::to_string(status) + " test\r\n" + extra +
          "Content-Type: application/json\r\nCache-Control: private, no-store\r\nConnection: close\r\nContent-Length: " +
          std::to_string(response.size()) + "\r\n\r\n" + response;
        boost::asio::write(socket, boost::asio::buffer(wire));
      } catch (const std::exception&) {
        ++server_errors;
      }
    }
  }
public:
  sha256_digest_t key;
  std::atomic<unsigned> options{0}, key_calls{0}, validation_calls{0}, legacy_calls{0},
      caller_tokens{0}, unexpected_calls{0}, server_errors{0};
  std::atomic<unsigned> post_status{200}, options_status{200};
  std::atomic<bool> persistent_fault{false};
  ProofServer() {
    key.v[0] = 0; key.v[1] = 255; key.v[31] = 128;
    acceptor.non_blocking(true);
    thread = std::thread([this] { serve(); });
  }
  ~ProofServer() { stopping.store(true); thread.join(); }
  static ProofServer& instance() {
    static ProofServer server;
    return server;
  }
  void reset() {
    std::lock_guard guard(mutex);
    requests.clear();
    after_proof = {};
    options = key_calls = validation_calls = legacy_calls = caller_tokens = unexpected_calls = server_errors = 0;
    post_status = options_status = 200;
    persistent_fault = false;
  }
  std::string endpoint() const {
    return "http://127.0.0.1:" + std::to_string(port) + "/";
  }
  void on_proof(std::function<void()> hook) {
    std::lock_guard guard(mutex);
    after_proof = std::move(hook);
  }
  std::vector<std::string> key_proofs() const {
    std::lock_guard guard(mutex);
    std::vector<std::string> result;
    for (const auto& request : requests) {
      if (request.method == "POST" && request.path == "/v3/s3tokens/signing_key") {
        result.push_back(request.body);
      }
    }
    return result;
  }
  std::vector<RecordedRequest> recorded() const {
    std::lock_guard guard(mutex);
    return requests;
  }
};

class ProofFactory : public rgw::auth::RemoteApplier::Factory {
  rgw::auth::ImplicitTenants tenants{g_ceph_context->_conf};
public:
  mutable unsigned grants = 0;
  rgw::auth::IdentityApplier::aplptr_t create_apl_remote(
      CephContext* cct, const req_state*,
      rgw::auth::RemoteApplier::acl_strategy_t&& acl,
      const rgw::auth::RemoteApplier::AuthInfo& info) const override {
    ++grants;
    return std::make_unique<rgw::auth::RemoteApplier>(cct, nullptr,
        std::move(acl), info, tenants, rgw::auth::ImplicitTenants::IMPLICIT_TENANTS_S3);
  }
};

class KeystoneProofHTTP : public ::testing::Test {
protected:
  ProofServer& server = ProofServer::instance();
  rgw::auth::s3::AWSGeneralAbstractor abstractor{g_ceph_context};
  ProofFactory factory;
  std::unique_ptr<rgw::auth::keystone::EC2Engine> engine;
  RGWProcessEnv penv;
  rgw::auth::keystone::S3ProofClock clock;
  std::atomic<time_t> wall_delta{0}, elapsed{0};
  std::atomic<unsigned> mono_reads{0}, advance_on_read{0};
  unsigned expected_legacy_calls = 0;
  void TearDown() override {
    EXPECT_EQ(expected_legacy_calls, server.legacy_calls.load());
    EXPECT_EQ(0u, server.unexpected_calls.load());
    EXPECT_EQ(0u, server.server_errors.load());
  }
  void controlled_clock() {
    const time_t start = time(nullptr);
    clock.wall = [this, start] { return start + wall_delta.load(); };
    clock.mono = [this] {
      const auto count = mono_reads.fetch_add(1) + 1;
      if (advance_on_read.load() && count >= advance_on_read.load()) elapsed.store(400);
      return SigningKeyCache::mono_time{} + std::chrono::seconds(elapsed.load());
    };
  }
  void SetUp() override {
    server.reset();
    auto& config = ProofConfig::get_instance();
    config.endpoint = server.endpoint();
    auto& conf = g_ceph_context->_conf;
    ASSERT_EQ(0, conf.set_val("rgw_keystone_s3_allow_legacy_signatures", "true"));
    ASSERT_EQ(0, conf.set_val("rgw_keystone_signing_key_cache_size", "1024"));
    ASSERT_EQ(0, conf.set_val("rgw_keystone_signing_key_cache_ttl", "300"));
    ASSERT_EQ(0, conf.set_val("rgw_sigv4_insecure", "false"));
    conf.apply_changes(nullptr);
    auto& cache = rgw::keystone::TokenCache::get_instance<ProofConfig>();
    NoDoutPrefix dpp(g_ceph_context, ceph_subsys_rgw);
    cache.invalidate_admin(&dpp);
    rgw::keystone::TokenEnvelope caller;
    caller.token.id = "synthetic-cached-caller";
    caller.set_expires(time(nullptr) + 300);
    cache.add_admin(caller);
    rebuild_engine();
  }
  void rebuild_engine(const rgw::auth::s3::AWSEngine::VersionAbstractor* version = nullptr) {
    engine = std::make_unique<rgw::auth::keystone::EC2Engine>(g_ceph_context,
        version ? version : &abstractor, &factory, ProofConfig::get_instance(),
        rgw::keystone::TokenCache::get_instance<ProofConfig>(),
        rgw::auth::keystone::SecretCache::get_instance(), clock);
  }
  std::unique_ptr<req_state> request(RGWEnv& env, const std::string& path = "/bucket/object", time_t offset = 0) {
    env.init(g_ceph_context);
    const auto date = utc(time(nullptr) + offset, "%Y%m%dT%H%M%SZ");
    const auto scope = date.substr(0, 8) + "/region/s3/aws4_request";
    env.set("REQUEST_METHOD", "GET");
    env.set("REQUEST_URI", path.c_str());
    env.set("SCRIPT_URI", path.c_str());
    env.set("HTTP_HOST", "rgw.example.com");
    env.set("SERVER_PORT", "80");
    env.set("HTTP_X_AMZ_DATE", date.c_str());
    env.set("HTTP_X_AMZ_CONTENT_SHA256", "UNSIGNED-PAYLOAD");
    const auto prefix = "AWS4-HMAC-SHA256 Credential=access/" + scope +
        ",SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=";
    env.set("HTTP_AUTHORIZATION", (prefix + std::string(64, 'a')).c_str());
    auto state = std::make_unique<req_state>(g_ceph_context, penv, &env, 0);
    state->op = OP_GET;
    state->op_type = RGW_OP_GET_OBJ;
    const auto data = abstractor.get_auth_data(state.get());
    env.set("HTTP_AUTHORIZATION", (prefix + calc_hmac_sha256(server.key,
        data.string_to_sign).to_str()).c_str());
    return state;
  }
  std::unique_ptr<req_state> v2_request(RGWEnv& env) {
    env.init(g_ceph_context);
    env.set("REQUEST_METHOD", "GET");
    env.set("REQUEST_URI", "/bucket/object");
    env.set("HTTP_HOST", "rgw.example.com");
    env.set("HTTP_DATE", utc(time(nullptr), "%a, %d %b %Y %H:%M:%S GMT").c_str());
    env.set("HTTP_AUTHORIZATION", "AWS access:synthetic-placeholder");
    auto s = std::make_unique<req_state>(g_ceph_context, penv, &env, 0);
    s->op = OP_GET;
    s->op_type = RGW_OP_GET_OBJ;
    const auto data = abstractor.get_auth_data(s.get());
    const auto digest = calc_hmac_sha1("synthetic-v2-secret", data.string_to_sign);
    const auto signature = rgw::to_base64(std::string_view(
        reinterpret_cast<const char*>(digest.v), digest.SIZE));
    env.set("HTTP_AUTHORIZATION", ("AWS access:" + signature).c_str());
    return s;
  }
  std::unique_ptr<req_state> boto_query(RGWEnv& env) {
    auto s = request(env);
    const std::string date = env.get("HTTP_X_AMZ_DATE", "");
    const auto scope = date.substr(0, 8) + "/region/s3/aws4_request";
    env.remove("HTTP_AUTHORIZATION");
    env.remove("HTTP_X_AMZ_DATE");
    env.remove("HTTP_X_AMZ_CONTENT_SHA256");
    env.set("SERVER_PORT", "8000");
    s->info.args.append("x-amz-algorithm", "AWS4-HMAC-SHA256");
    s->info.args.append("x-amz-credential", "access/" + scope);
    s->info.args.append("x-amz-date", date);
    s->info.args.append("x-amz-expires", "600");
    s->info.args.append("x-amz-signedheaders", "host");
    s->info.args.append("x-amz-signature", std::string(64, '0'));
    s->info.request_params = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=access/" +
        scope + "&X-Amz-Date=" + date + "&X-Amz-Expires=600&X-Amz-SignedHeaders=host&X-Amz-Signature=" +
        std::string(64, '0');
    rgw::auth::s3::AWSGeneralBoto2Abstractor boto(g_ceph_context);
    const auto auth = boto.get_auth_data(s.get());
    const auto signature = calc_hmac_sha256(server.key, auth.string_to_sign).to_str();
    s->info.args.append("x-amz-signature", signature);
    s->info.request_params.replace(s->info.request_params.size() - 64, 64, signature);
    return s;
  }
  auto authenticate(req_state* s) {
    return engine->authenticate(s, s, null_yield);
  }
};

TEST_F(KeystoneProofHTTP, ColdAndHitVerifyActualRequestAndIdentity)
{
  RGWEnv env;
  auto s = request(env);
  auto result = authenticate(s.get());
  ASSERT_EQ(rgw::auth::Engine::result_t::Status::GRANTED, result.get_status());
  auto apl = result.get_applier();
  EXPECT_EQ(TYPE_KEYSTONE, apl->get_identity_type());
  EXPECT_EQ("Project", apl->get_acct_name());
  apl->modify_request_state(s.get(), s.get());
  EXPECT_EQ("user", s->env.find("keystone:userid")->second);
  EXPECT_EQ(1u, server.key_calls.load());
  EXPECT_EQ(2u, server.options.load());
  auto hit = authenticate(s.get());
  EXPECT_EQ(rgw::auth::Engine::result_t::Status::GRANTED, hit.get_status());
  EXPECT_EQ(1u, server.key_calls.load());
  s->info.request_uri = "/tampered";
  s->info.request_uri_aws4 = "/tampered";
  auto tamper = authenticate(s.get());
  EXPECT_NE(rgw::auth::Engine::result_t::Status::GRANTED, tamper.get_status());
  EXPECT_EQ(1u, server.key_calls.load());
  EXPECT_EQ(0u, server.unexpected_calls.load());
  EXPECT_EQ(0u, server.server_errors.load());
}

TEST_F(KeystoneProofHTTP, Caller401RetriesSameProofExactlyOnce)
{
  server.post_status = 401;
  RGWEnv env;
  auto s = request(env);
  auto result = authenticate(s.get());
  ASSERT_EQ(rgw::auth::Engine::result_t::Status::GRANTED, result.get_status());
  EXPECT_EQ(2u, server.key_calls.load());
  EXPECT_EQ(1u, server.caller_tokens.load());
  auto apl = result.get_applier();
  apl->modify_request_state(s.get(), s.get());
  EXPECT_EQ("user", s->env.find("keystone:userid")->second);
  auto proofs = server.key_proofs();
  ASSERT_EQ(2u, proofs.size());
  EXPECT_EQ(proofs[0], proofs[1]);
  std::vector<ProofServer::RecordedRequest> posts;
  for (const auto& request : server.recorded()) {
    if (request.method == "OPTIONS") EXPECT_EQ("synthetic-cached-caller", request.caller);
    if (request.path == "/v3/s3tokens/signing_key" && request.method == "POST") posts.push_back(request);
  }
  ASSERT_EQ(2u, posts.size());
  EXPECT_EQ(posts[0].method, posts[1].method);
  EXPECT_EQ(posts[0].path, posts[1].path);
  EXPECT_EQ("synthetic-cached-caller", posts[0].caller);
  EXPECT_EQ("synthetic-new-caller", posts[1].caller);
  EXPECT_EQ(0u, server.unexpected_calls.load());
}

TEST_F(KeystoneProofHTTP, ForbiddenDoesNotRefreshOrFallback)
{
  server.post_status = 403;
  RGWEnv env;
  auto s = request(env);
  EXPECT_NE(rgw::auth::Engine::result_t::Status::GRANTED, authenticate(s.get()).get_status());
  EXPECT_EQ(1u, server.key_calls.load());
  EXPECT_EQ(0u, server.caller_tokens.load());
  EXPECT_EQ(0u, server.validation_calls.load());
  EXPECT_EQ(0u, server.unexpected_calls.load());
}

TEST_F(KeystoneProofHTTP, RedirectIsNotFollowed)
{
  server.post_status = 307;
  RGWEnv env;
  auto s = request(env);
  EXPECT_NE(rgw::auth::Engine::result_t::Status::GRANTED, authenticate(s.get()).get_status());
  EXPECT_EQ(1u, server.key_calls.load());
  EXPECT_EQ(0u, server.unexpected_calls.load());
  EXPECT_EQ(0u, server.caller_tokens.load());
}

TEST_F(KeystoneProofHTTP, CapabilityDenialIsNotAuthorizationOrDowngrade)
{
  server.options_status = 403;
  RGWEnv env;
  auto s = request(env);
  EXPECT_NE(rgw::auth::Engine::result_t::Status::GRANTED, authenticate(s.get()).get_status());
  EXPECT_EQ(0u, server.key_calls.load());
  EXPECT_EQ(0u, server.caller_tokens.load());
  EXPECT_EQ(0u, server.unexpected_calls.load());
}
TEST_F(KeystoneProofHTTP, SecondCaller401Stops)
{
  server.post_status = 401;
  server.persistent_fault = true;
  RGWEnv env;
  auto s = request(env);
  EXPECT_NE(rgw::auth::Engine::result_t::Status::GRANTED, authenticate(s.get()).get_status());
  EXPECT_EQ(2u, server.key_calls.load());
  EXPECT_EQ(1u, server.caller_tokens.load());
  EXPECT_EQ(0u, server.validation_calls.load());
  EXPECT_EQ(0u, server.unexpected_calls.load());
}

TEST_F(KeystoneProofHTTP, WarmCacheStillRejectsStaleAndFutureSignedDates)
{
  RGWEnv env;
  auto s = request(env);
  ASSERT_EQ(rgw::auth::Engine::result_t::Status::GRANTED, authenticate(s.get()).get_status());
  for (time_t offset : {-301, 600}) {
    RGWEnv other_env;
    auto other = request(other_env, "/bucket/object", offset);
    EXPECT_NE(rgw::auth::Engine::result_t::Status::GRANTED, authenticate(other.get()).get_status());
  }
  EXPECT_EQ(1u, server.key_calls.load());
}

TEST_F(KeystoneProofHTTP, CredentialedCorsNeverSkipsSignatureOnCacheHit)
{
  RGWEnv env;
  auto s = request(env);
  ASSERT_EQ(rgw::auth::Engine::result_t::Status::GRANTED, authenticate(s.get()).get_status());
  s->op = OP_OPTIONS;
  s->op_type = RGW_OP_OPTIONS_CORS;
  env.set("HTTP_ACCESS_CONTROL_REQUEST_METHOD", "GET");
  auto header = std::string(env.get("HTTP_AUTHORIZATION", ""));
  const auto sig = header.find("Signature=");
  ASSERT_NE(std::string::npos, sig);
  header.replace(sig + 10, 64, std::string(64, '0'));
  env.set("HTTP_AUTHORIZATION", header.c_str());
  EXPECT_NE(rgw::auth::Engine::result_t::Status::GRANTED, authenticate(s.get()).get_status());
  EXPECT_EQ(1u, server.key_calls.load());
}

TEST_F(KeystoneProofHTTP, SessionTokenRemainsEligibleForIndependentSTS)
{
  RGWEnv env;
  auto s = request(env);
  env.set("HTTP_X_AMZ_SECURITY_TOKEN", "synthetic-sts-token");
  const auto scope = std::string(env.get("HTTP_X_AMZ_DATE", "")).substr(0, 8) +
      "/region/s3/aws4_request";
  const auto prefix = "AWS4-HMAC-SHA256 Credential=access/" + scope +
      ",SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token,Signature=";
  env.set("HTTP_AUTHORIZATION", (prefix + std::string(64, 'a')).c_str());
  const auto data = abstractor.get_auth_data(s.get());
  ASSERT_EQ("synthetic-sts-token", data.session_token);
  env.set("HTTP_AUTHORIZATION", (prefix + calc_hmac_sha256(server.key,
      data.string_to_sign).to_str()).c_str());
  EXPECT_EQ(rgw::auth::Engine::result_t::Status::DENIED, authenticate(s.get()).get_status());
  EXPECT_EQ(0u, factory.grants);
  EXPECT_EQ(0u, server.options.load());
  EXPECT_EQ(0u, server.key_calls.load());
  EXPECT_EQ(0u, server.validation_calls.load());
}

TEST_F(KeystoneProofHTTP, LateRpcCannotGrantAfterElapsedLocalPolicy)
{
  controlled_clock();
  auto& conf = g_ceph_context->_conf;
  ASSERT_EQ(0, conf.set_val("rgw_keystone_signing_key_cache_ttl", "1"));
  server.on_proof([this] { elapsed.store(2); });
  for (const auto capacity : {"1024", "0"}) {
    elapsed.store(0);
    ASSERT_EQ(0, conf.set_val("rgw_keystone_signing_key_cache_size", capacity));
    conf.apply_changes(nullptr);
    rebuild_engine();
    RGWEnv env;
    auto s = request(env);
    EXPECT_NE(rgw::auth::Engine::result_t::Status::GRANTED, authenticate(s.get()).get_status());
    EXPECT_EQ(0u, factory.grants);
  }
}

TEST_F(KeystoneProofHTTP, CacheDisabledStillEnforcesElapsedIdentityDeadline)
{
  controlled_clock();
  auto& conf = g_ceph_context->_conf;
  ASSERT_EQ(0, conf.set_val("rgw_keystone_signing_key_cache_ttl", "0"));
  ASSERT_EQ(0, conf.set_val("rgw_keystone_signing_key_cache_size", "0"));
  conf.apply_changes(nullptr);
  rebuild_engine();
  // Header remains fresh under the wall rollback, while the server/identity
  // deadline is already exhausted in elapsed time. No sleep or global clock.
  server.on_proof([this] { wall_delta.store(-60); elapsed.store(400); });
  RGWEnv env;
  auto s = request(env);
  EXPECT_NE(rgw::auth::Engine::result_t::Status::GRANTED, authenticate(s.get()).get_status());
  EXPECT_EQ(0u, factory.grants);
}

TEST_F(KeystoneProofHTTP, CacheHitRechecksElapsedDeadlineBeforeApplier)
{
  controlled_clock();
  rebuild_engine();
  RGWEnv env;
  auto s = request(env);
  ASSERT_EQ(rgw::auth::Engine::result_t::Status::GRANTED, authenticate(s.get()).get_status());
  ASSERT_EQ(1u, factory.grants);
  // Advance after lookup has returned its copy, not before lookup can refetch.
  advance_on_read.store(mono_reads.load() + 3);
  EXPECT_NE(rgw::auth::Engine::result_t::Status::GRANTED, authenticate(s.get()).get_status());
  EXPECT_EQ(1u, factory.grants);
  EXPECT_EQ(1u, server.key_calls.load());
}

TEST_F(KeystoneProofHTTP, CacheZeroIsOffNotImmediateAuthenticationExpiry)
{
  controlled_clock();
  auto& conf = g_ceph_context->_conf;
  ASSERT_EQ(0, conf.set_val("rgw_keystone_signing_key_cache_ttl", "0"));
  ASSERT_EQ(0, conf.set_val("rgw_keystone_signing_key_cache_size", "0"));
  conf.apply_changes(nullptr);
  rebuild_engine();
  server.on_proof([this] { elapsed.fetch_add(2); });
  RGWEnv env;
  auto s = request(env);
  EXPECT_EQ(rgw::auth::Engine::result_t::Status::GRANTED, authenticate(s.get()).get_status());
  EXPECT_EQ(rgw::auth::Engine::result_t::Status::GRANTED, authenticate(s.get()).get_status());
  EXPECT_EQ(2u, server.key_calls.load());
}

TEST_F(KeystoneProofHTTP, DisabledLocalCacheStillChecksEveryProof)
{
  auto& conf = g_ceph_context->_conf;
  ASSERT_EQ(0, conf.set_val("rgw_keystone_signing_key_cache_ttl", "0"));
  conf.apply_changes(nullptr);
  rebuild_engine();
  RGWEnv env;
  auto s = request(env);
  EXPECT_EQ(rgw::auth::Engine::result_t::Status::GRANTED, authenticate(s.get()).get_status());
  EXPECT_EQ(rgw::auth::Engine::result_t::Status::GRANTED, authenticate(s.get()).get_status());
  EXPECT_EQ(2u, server.key_calls.load());
}

TEST_F(KeystoneProofHTTP, OldSecretCachePoisonIsIgnored)
{
  rgw::keystone::TokenEnvelope poisoned;
  poisoned.user.id = "wrong-user";
  poisoned.project.id = "wrong-project";
  poisoned.set_expires(time(nullptr) + 300);
  rgw::auth::keystone::SecretCache::get_instance().add("access", poisoned, "wrong-root");
  RGWEnv env;
  auto s = request(env);
  auto result = authenticate(s.get());
  ASSERT_EQ(rgw::auth::Engine::result_t::Status::GRANTED, result.get_status());
  auto apl = result.get_applier();
  apl->modify_request_state(s.get(), s.get());
  EXPECT_EQ("user", s->env.find("keystone:userid")->second);
  EXPECT_EQ(1u, server.key_calls.load());
}

class TaggedPolicyAbstractor : public rgw::auth::s3::AWSEngine::VersionAbstractor {
  ProofKind kind;
public:
  explicit TaggedPolicyAbstractor(ProofKind kind) : kind(kind) {}
  auth_data_t get_auth_data(const req_state*) const override {
    auth_data_t data;
    data.access_key_id = "access";
    data.client_signature = "synthetic-policy-signature";
    data.string_to_sign = "e30=";
    data.proof_kind = kind;
    return data;
  }
};

class LegacyPolicyAbstractor : public rgw::auth::s3::AWSEngine::VersionAbstractor {
public:
  std::string signature;
  auth_data_t get_auth_data(const req_state*) const override {
    return {"legacy-browser-access", signature, {}, "e30=",
            rgw::auth::s3::get_v2_signature,
            [](const boost::optional<std::string>&) {
              return rgw::auth::Completer::cmplptr_t{};
            }, ProofKind::browser_v2, boost::none};
  }
};

TEST_F(KeystoneProofHTTP, DefaultLegacyRetainsRootPolicyCallbacksAndLocalDefaults)
{
  boost::intrusive_ptr<CephContext> cct(new CephContext(CEPH_ENTITY_TYPE_CLIENT), false);
  EXPECT_EQ("legacy", cct->_conf->rgw_keystone_s3_auth_mode);
  EXPECT_TRUE(cct->_conf->rgw_s3_auth_use_rados);
  EXPECT_FALSE(cct->_conf->rgw_s3_auth_use_keystone);
  ASSERT_EQ(0, cct->_conf.set_val("rgw_keystone_accepted_roles", "member"));
  cct->_conf.apply_changes(nullptr);
  common_init_finish(cct.get());
  S3ProofResult proof;
  ASSERT_EQ(0, decode_s3_proof_response(key_response(), true, sample_scope(), proof));
  proof.identity.set_expires(time(nullptr) + 300);
  rgw::auth::keystone::SecretCache::get_instance().add(
      "legacy-browser-access", proof.identity, "synthetic-legacy-root");
  LegacyPolicyAbstractor browser;
  const auto signature = rgw::auth::s3::get_v2_signature(
      cct.get(), "synthetic-legacy-root", "e30=");
  browser.signature.assign(signature.c_str(), signature.size());
  rgw::auth::keystone::EC2Engine legacy(cct.get(), &browser, &factory,
      ProofConfig::get_instance(), rgw::keystone::TokenCache::get_instance<ProofConfig>(),
      rgw::auth::keystone::SecretCache::get_instance());
  RGWEnv env;
  auto s = request(env);
  EXPECT_EQ(rgw::auth::Engine::result_t::Status::GRANTED,
      legacy.authenticate(s.get(), s.get(), null_yield).get_status());
  ASSERT_EQ(0u, server.legacy_calls.load());
  browser.signature = "invalid-signature";
  expected_legacy_calls = 1;
  // Unlike signing_key's local known-key rejection, unchanged legacy mode
  // revalidates remotely. Its explicit HTTP401 maps to signature rejection.
  EXPECT_EQ(rgw::auth::Engine::result_t::Status::REJECTED,
      legacy.authenticate(s.get(), s.get(), null_yield).get_status());
  EXPECT_EQ(1u, server.legacy_calls.load());
  EXPECT_EQ(1u, factory.grants);
  EXPECT_EQ(0u, server.key_calls.load());
  EXPECT_EQ(0u, server.validation_calls.load());
  EXPECT_EQ(0u, server.unexpected_calls.load());
}

TEST_F(KeystoneProofHTTP, TaggedPolicyIsDeniedWithOtherWarmContext)
{
  RGWEnv env;
  auto normal = request(env);
  ASSERT_EQ(rgw::auth::Engine::result_t::Status::GRANTED, authenticate(normal.get()).get_status());
  auto warm_general_context = std::move(engine);
  using Kind = rgw::auth::s3::AWSEngine::VersionAbstractor::ProofKind;
  // Exercise the public version/engine boundary, not private browser storage.
  // Real multipart extraction remains a separate integration gate.
  for (const auto kind : {Kind::browser_v2, Kind::browser_v4}) {
    TaggedPolicyAbstractor browser(kind);
    rebuild_engine(&browser);
    EXPECT_EQ(rgw::auth::Engine::result_t::Status::DENIED, authenticate(normal.get()).get_status());
    engine.reset();
  }
  EXPECT_EQ(1u, server.key_calls.load());
  EXPECT_EQ(0u, server.validation_calls.load());
  EXPECT_EQ(0u, server.unexpected_calls.load());
}

TEST_F(KeystoneProofHTTP, V2NoCacheStillEnforcesElapsedIdentityDeadline)
{
  controlled_clock();
  rebuild_engine();
  server.on_proof([this] { wall_delta.store(-60); elapsed.store(400); });
  RGWEnv env;
  auto s = v2_request(env);
  EXPECT_NE(rgw::auth::Engine::result_t::Status::GRANTED, authenticate(s.get()).get_status());
  EXPECT_EQ(0u, factory.grants);
  EXPECT_EQ(1u, server.validation_calls.load());
}

TEST_F(KeystoneProofHTTP, V2ValidatesPerRequestWithoutExport)
{
  RGWEnv env;
  env.init(g_ceph_context);
  env.set("REQUEST_METHOD", "GET");
  env.set("REQUEST_URI", "/bucket/object");
  env.set("HTTP_HOST", "rgw.example.com");
  env.set("HTTP_DATE", utc(time(nullptr), "%a, %d %b %Y %H:%M:%S GMT").c_str());
  env.set("HTTP_AUTHORIZATION", "AWS access:synthetic-placeholder");
  auto s = std::make_unique<req_state>(g_ceph_context, penv, &env, 0);
  s->op = OP_GET;
  s->op_type = RGW_OP_GET_OBJ;
  const auto data = abstractor.get_auth_data(s.get());
  const auto digest = calc_hmac_sha1("synthetic-v2-secret", data.string_to_sign);
  const auto signature = rgw::to_base64(std::string_view(
      reinterpret_cast<const char*>(digest.v), digest.SIZE));
  env.set("HTTP_AUTHORIZATION", ("AWS access:" + signature).c_str());
  EXPECT_EQ(rgw::auth::Engine::result_t::Status::GRANTED, authenticate(s.get()).get_status());
  EXPECT_EQ(rgw::auth::Engine::result_t::Status::GRANTED, authenticate(s.get()).get_status());
  EXPECT_EQ(2u, server.validation_calls.load());
  EXPECT_EQ(0u, server.key_calls.load());
  auto& conf = g_ceph_context->_conf;
  ASSERT_EQ(0, conf.set_val("rgw_keystone_s3_allow_legacy_signatures", "false"));
  conf.apply_changes(nullptr);
  rebuild_engine();
  EXPECT_NE(rgw::auth::Engine::result_t::Status::GRANTED, authenticate(s.get()).get_status());
  EXPECT_EQ(2u, server.validation_calls.load());
}
TEST_F(KeystoneProofHTTP, FullRegistryCanonicalAlternativesAndDenialCounts)
{
  using Status = rgw::auth::Engine::result_t::Status;
  auto& conf = g_ceph_context->_conf;
  // Restart-only URL/order were configured before common_init_finish().
  // These bool options are runtime-mutable according to Option's actual rules.
  ASSERT_EQ(0, conf.set_val("rgw_s3_auth_use_keystone", "true"));
  ASSERT_EQ(0, conf.set_val("rgw_s3_auth_use_rados", "false"));
  ASSERT_EQ(0, conf.set_val("rgw_s3_auth_use_sts", "false"));
  ASSERT_EQ(0, conf.set_val("rgw_s3_auth_use_ldap", "false"));
  conf.apply_changes(nullptr);
  auto& caller_cache = rgw::keystone::TokenCache::get_instance<rgw::keystone::CephCtxConfig>();
  NoDoutPrefix dpp(g_ceph_context, ceph_subsys_rgw);
  caller_cache.invalidate_admin(&dpp);
  rgw::keystone::TokenEnvelope caller;
  caller.token.id = "synthetic-cached-caller";
  caller.set_expires(time(nullptr) + 300);
  caller_cache.add_admin(caller);
  rgw::auth::ImplicitTenants tenants(conf);
  auto registry = [&] {
    return std::make_unique<rgw::auth::StrategyRegistry>(g_ceph_context, tenants, nullptr);
  };
  // This is the actual production outer plain/boto stack, not a fallback stub.
  // Storage application is deliberately outside this authentication-only test.
  {
    auto auth = registry();
    RGWEnv env;
    auto s = boto_query(env);
    const auto calls = server.key_calls.load(), options = server.options.load();
    EXPECT_EQ(Status::GRANTED, auth->get_s3_main().authenticate(s.get(), s.get(), null_yield).get_status());
    EXPECT_EQ(calls + 2, server.key_calls.load());
    EXPECT_EQ(options + 4, server.options.load());
  }
  {
    auto auth = registry();
    RGWEnv header_env;
    auto header = request(header_env);
    ASSERT_EQ(Status::GRANTED, auth->get_s3_main().authenticate(header.get(), header.get(), null_yield).get_status());
    const auto calls = server.key_calls.load(), options = server.options.load();
    RGWEnv env;
    auto s = boto_query(env);
    EXPECT_EQ(Status::GRANTED, auth->get_s3_main().authenticate(s.get(), s.get(), null_yield).get_status());
    EXPECT_EQ(calls, server.key_calls.load()); // same derived key, local alternative
    EXPECT_EQ(options, server.options.load());
    s->info.args.append("x-amz-signature", std::string(64, '0'));
    s->info.request_params.replace(s->info.request_params.size() - 64, 64, std::string(64, '0'));
    EXPECT_EQ(Status::REJECTED, auth->get_s3_main().authenticate(s.get(), s.get(), null_yield).get_status());
    EXPECT_EQ(calls, server.key_calls.load());
  }
  {
    auto auth = registry();
    RGWEnv env;
    auto s = request(env);
    server.post_status = 403;
    server.persistent_fault = true;
    const auto calls = server.key_calls.load();
    EXPECT_NE(Status::GRANTED, auth->get_s3_main().authenticate(s.get(), s.get(), null_yield).get_status());
    EXPECT_EQ(calls + 1, server.key_calls.load()); // identical header proof is not retried
    server.persistent_fault = false;
    server.post_status = 200;
  }
  {
    auto auth = registry();
    RGWEnv env;
    env.init(g_ceph_context);
    env.set("REQUEST_METHOD", "GET");
    env.set("REQUEST_URI", "/bucket/object");
    env.set("HTTP_HOST", "rgw.example.com");
    env.set("HTTP_DATE", utc(time(nullptr), "%a, %d %b %Y %H:%M:%S GMT").c_str());
    env.set("HTTP_AUTHORIZATION", "AWS access:invalid-signature");
    req_state s(g_ceph_context, penv, &env, 1);
    s.op = OP_GET; s.op_type = RGW_OP_GET_OBJ;
    const auto calls = server.validation_calls.load();
    EXPECT_NE(Status::GRANTED, auth->get_s3_main().authenticate(&s, &s, null_yield).get_status());
    EXPECT_EQ(calls + 1, server.validation_calls.load()); // identical V2 proof too
  }
  {
    auto auth = registry();
    RGWEnv env;
    auto s = boto_query(env);
    s->info.args.append("x-amz-signature", std::string(64, '0'));
    s->info.request_params.replace(s->info.request_params.size() - 64, 64, std::string(64, '0'));
    const auto calls = server.key_calls.load();
    EXPECT_NE(Status::GRANTED, auth->get_s3_main().authenticate(s.get(), s.get(), null_yield).get_status());
    EXPECT_EQ(calls + 2, server.key_calls.load()); // two distinct strict canonical proofs
  }
  EXPECT_EQ(0u, server.caller_tokens.load()); // no403 caller reauthentication
  EXPECT_EQ(0u, server.unexpected_calls.load());
  EXPECT_EQ(0u, server.server_errors.load());
}
} // anonymous namespace

int main(int argc, char** argv)
{
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
      CODE_ENVIRONMENT_UTILITY, CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  // Immutable strings belong to process startup, not a running fixture.
  // Start the process-owned peer first so the real registry sees its fixed URL.
  auto& conf = cct->_conf;
  ceph_assert(conf.set_val("rgw_keystone_s3_auth_mode", "signing_key") == 0);
  ceph_assert(conf.set_val("rgw_keystone_accepted_roles", "member") == 0);
  ceph_assert(conf.set_val("rgw_keystone_accepted_admin_roles", "admin") == 0);
  ceph_assert(conf.set_val("rgw_keystone_url", ProofServer::instance().endpoint()) == 0);
  ceph_assert(conf.set_val("rgw_s3_auth_order", "sts, external, local") == 0);
  conf.apply_changes(nullptr);
  common_init_finish(g_ceph_context);
  rgw_http_client_init(cct.get());
  rgw_setup_saved_curl_handles();
  ::testing::InitGoogleTest(&argc, argv);
  int result = RUN_ALL_TESTS();
  rgw_release_all_curl_handles();
  rgw_http_client_cleanup();
  return result;
}
