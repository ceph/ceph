// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_iam_policy.h"

#include <string>

#include <boost/intrusive_ptr.hpp>
#include <boost/optional.hpp>

#include <gtest/gtest.h>

#include "rgw_sal_store.h"

#include "include/stringify.h"
#include "common/ceph_context.h"
#include "rgw_auth.h"
#include "rgw_iam_managed_policy.h"
#include "rgw_op.h"
#include "rgw_process_env.h"
#include "rgw_sal_config.h"

using std::string;

using boost::container::flat_set;
using boost::intrusive_ptr;
using boost::none;

using rgw::auth::Identity;
using rgw::auth::Principal;
using rgw::auth::PrincipalIdentity;

using rgw::ARN;
using rgw::IAM::Effect;
using rgw::IAM::Environment;
using rgw::Partition;
using rgw::IAM::Policy;
using rgw::IAM::Condition;
using rgw::IAM::s3All;
using rgw::IAM::s3objectlambdaAll;
using rgw::IAM::s3GetAccelerateConfiguration;
using rgw::IAM::s3GetBucketAcl;
using rgw::IAM::s3GetBucketOwnershipControls;
using rgw::IAM::s3GetBucketCORS;
using rgw::IAM::s3GetBucketLocation;
using rgw::IAM::s3GetBucketLogging;
using rgw::IAM::s3GetBucketNotification;
using rgw::IAM::s3GetBucketPolicy;
using rgw::IAM::s3GetBucketPolicyStatus;
using rgw::IAM::s3GetBucketPublicAccessBlock;
using rgw::IAM::s3GetBucketEncryption;
using rgw::IAM::s3GetBucketRequestPayment;
using rgw::IAM::s3GetBucketTagging;
using rgw::IAM::s3GetBucketVersioning;
using rgw::IAM::s3GetBucketWebsite;
using rgw::IAM::s3GetLifecycleConfiguration;
using rgw::IAM::s3GetObject;
using rgw::IAM::s3GetObjectAcl;
using rgw::IAM::s3GetObjectVersionAcl;
using rgw::IAM::s3GetObjectTorrent;
using rgw::IAM::s3GetObjectTagging;
using rgw::IAM::s3GetObjectVersion;
using rgw::IAM::s3GetObjectVersionTagging;
using rgw::IAM::s3GetObjectVersionTorrent;
using rgw::IAM::s3GetObjectAttributes;
using rgw::IAM::s3GetObjectVersionAttributes;
using rgw::IAM::s3GetPublicAccessBlock;
using rgw::IAM::s3GetReplicationConfiguration;
using rgw::IAM::s3GetObjectVersionForReplication;
using rgw::IAM::s3ListAllMyBuckets;
using rgw::IAM::s3ListBucket;
using rgw::IAM::s3ListBucketMultipartUploads;
using rgw::IAM::s3ListBucketVersions;
using rgw::IAM::s3ListMultipartUploadParts;
using rgw::IAM::None;
using rgw::IAM::s3PutBucketAcl;
using rgw::IAM::s3PutBucketPolicy;
using rgw::IAM::s3GetBucketObjectLockConfiguration;
using rgw::IAM::s3GetObjectRetention;
using rgw::IAM::s3GetObjectLegalHold;
using rgw::IAM::s3DescribeJob;
using rgw::IAM::s3GetAccountPublicAccessBlock;
using rgw::IAM::s3objectlambdaGetObject;
using rgw::IAM::s3objectlambdaListBucket;
using rgw::IAM::iamGenerateCredentialReport;
using rgw::IAM::iamGenerateServiceLastAccessedDetails;
using rgw::IAM::iamGetUserPolicy;
using rgw::IAM::iamGetRole;
using rgw::IAM::iamGetRolePolicy;
using rgw::IAM::iamGetOIDCProvider;
using rgw::IAM::iamGetUser;
using rgw::IAM::iamListUserPolicies;
using rgw::IAM::iamListAttachedUserPolicies;
using rgw::IAM::iamListRoles;
using rgw::IAM::iamListRolePolicies;
using rgw::IAM::iamListAttachedRolePolicies;
using rgw::IAM::iamListOIDCProviders;
using rgw::IAM::iamListRoleTags;
using rgw::IAM::iamListUsers;
using rgw::IAM::iamListAccessKeys;
using rgw::IAM::iamGetGroup;
using rgw::IAM::iamListGroups;
using rgw::IAM::iamListGroupsForUser;
using rgw::IAM::iamGetGroupPolicy;
using rgw::IAM::iamListGroupPolicies;
using rgw::IAM::iamListAttachedGroupPolicies;
using rgw::IAM::iamSimulateCustomPolicy;
using rgw::IAM::iamSimulatePrincipalPolicy;
using rgw::IAM::iamGetAccountSummary;
using rgw::IAM::snsGetTopicAttributes;
using rgw::IAM::snsListTopics;
using rgw::Service;
using rgw::IAM::TokenID;
using rgw::IAM::Version;
using rgw::IAM::Action_t;
using rgw::IAM::NotAction_t;
using rgw::IAM::iamCreateRole;
using rgw::IAM::iamDeleteRole;
using rgw::IAM::iamAll;
using rgw::IAM::stsAll;
using rgw::IAM::snsAll;
using rgw::IAM::organizationsAll;
using rgw::IAM::s3vectorsAll;
using rgw::IAM::allCount;

using rgw::IAM::s3AllValue;
using rgw::IAM::s3objectlambdaAllValue;
using rgw::IAM::iamAllValue;
using rgw::IAM::stsAllValue;
using rgw::IAM::snsAllValue;
using rgw::IAM::organizationsAllValue;
using rgw::IAM::allValue;

using rgw::IAM::get_managed_policy;

class PolicyTest : public ::testing::Test {
protected:
  intrusive_ptr<CephContext> cct;
  static const string arbitrary_tenant;
  static string example1;
  static string example2;
  static string example3;
  static string example4;
  static string example5;
  static string example6;
  static string example7;
public:
  PolicyTest() {
    cct.reset(new CephContext(CEPH_ENTITY_TYPE_CLIENT), false);
  }
};

TEST_F(PolicyTest, Parse1) {
  boost::optional<Policy> p;

  ASSERT_NO_THROW(p = Policy(cct.get(), &arbitrary_tenant, example1, true));
  ASSERT_TRUE(p);

  EXPECT_EQ(p->text, example1);
  EXPECT_EQ(p->version, Version::v2012_10_17);
  EXPECT_FALSE(p->id);
  EXPECT_FALSE(p->statements[0].sid);
  EXPECT_FALSE(p->statements.empty());
  EXPECT_EQ(p->statements.size(), 1U);
  EXPECT_TRUE(p->statements[0].princ.empty());
  EXPECT_TRUE(p->statements[0].noprinc.empty());
  EXPECT_EQ(p->statements[0].effect, Effect::Allow);
  Action_t act;
  act[s3ListBucket] = 1;
  EXPECT_EQ(p->statements[0].action, act);
  EXPECT_EQ(p->statements[0].notaction, None);
  ASSERT_FALSE(p->statements[0].resource.empty());
  ASSERT_EQ(p->statements[0].resource.size(), 1U);
  EXPECT_EQ(p->statements[0].resource.begin()->partition, Partition::aws);
  EXPECT_EQ(p->statements[0].resource.begin()->service, Service::s3);
  EXPECT_TRUE(p->statements[0].resource.begin()->region.empty());
  EXPECT_EQ(p->statements[0].resource.begin()->account, arbitrary_tenant);
  EXPECT_EQ(p->statements[0].resource.begin()->resource, "example_bucket");
  EXPECT_TRUE(p->statements[0].notresource.empty());
  EXPECT_TRUE(p->statements[0].conditions.empty());
}

TEST_F(PolicyTest, Eval1) {
  auto p  = Policy(cct.get(), &arbitrary_tenant, example1, true);
  Environment e;
  boost::optional<Principal> principal;

  ARN arn1(Partition::aws, Service::s3,
		       "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(p.eval(e, none, s3ListBucket, arn1, principal),
            Effect::Allow);
  EXPECT_FALSE(principal);

  ARN arn2(Partition::aws, Service::s3,
		       "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(p.eval(e, none, s3PutBucketAcl, arn2, principal),
            Effect::Pass);

  ARN arn3(Partition::aws, Service::s3,
		       "", arbitrary_tenant, "erroneous_bucket");
  EXPECT_EQ(p.eval(e, none, s3ListBucket, arn3, principal),
            Effect::Pass);

}

TEST_F(PolicyTest, Parse2) {
  boost::optional<Policy> p;

  ASSERT_NO_THROW(p = Policy(cct.get(), &arbitrary_tenant, example2, true));
  ASSERT_TRUE(p);

  EXPECT_EQ(p->text, example2);
  EXPECT_EQ(p->version, Version::v2012_10_17);
  EXPECT_EQ(*p->id, "S3-Account-Permissions");
  ASSERT_FALSE(p->statements.empty());
  EXPECT_EQ(p->statements.size(), 1U);
  EXPECT_EQ(*p->statements[0].sid, "1");
  EXPECT_FALSE(p->statements[0].princ.empty());
  EXPECT_EQ(p->statements[0].princ.size(), 1U);
  EXPECT_EQ(*p->statements[0].princ.begin(),
	    Principal::account("ACCOUNT-ID-WITHOUT-HYPHENS"));
  EXPECT_TRUE(p->statements[0].noprinc.empty());
  EXPECT_EQ(p->statements[0].effect, Effect::Allow);
  Action_t act;
  for (auto i = 0ULL; i < s3All; i++)
    act[i] = 1;
  act[s3All] = 1;
  EXPECT_EQ(p->statements[0].action, act);
  EXPECT_EQ(p->statements[0].notaction, None);
  ASSERT_FALSE(p->statements[0].resource.empty());
  ASSERT_EQ(p->statements[0].resource.size(), 2U);
  EXPECT_EQ(p->statements[0].resource.begin()->partition, Partition::aws);
  EXPECT_EQ(p->statements[0].resource.begin()->service, Service::s3);
  EXPECT_TRUE(p->statements[0].resource.begin()->region.empty());
  EXPECT_EQ(p->statements[0].resource.begin()->account, arbitrary_tenant);
  EXPECT_EQ(p->statements[0].resource.begin()->resource, "mybucket");
  EXPECT_EQ((p->statements[0].resource.begin() + 1)->partition,
	    Partition::aws);
  EXPECT_EQ((p->statements[0].resource.begin() + 1)->service,
	    Service::s3);
  EXPECT_TRUE((p->statements[0].resource.begin() + 1)->region.empty());
  EXPECT_EQ((p->statements[0].resource.begin() + 1)->account,
	    arbitrary_tenant);
  EXPECT_EQ((p->statements[0].resource.begin() + 1)->resource, "mybucket/*");
  EXPECT_TRUE(p->statements[0].notresource.empty());
  EXPECT_TRUE(p->statements[0].conditions.empty());
}

TEST_F(PolicyTest, Eval2) {
  auto p  = Policy(cct.get(), &arbitrary_tenant, example2, true);
  Environment e;
  boost::optional<Principal> principal;

  auto trueacct = PrincipalIdentity(
    Principal::account("ACCOUNT-ID-WITHOUT-HYPHENS"));

  auto notacct = PrincipalIdentity(
    Principal::account("some-other-account"));
  for (auto i = 0ULL; i < s3All; ++i) {
    ARN arn1(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "mybucket");
    EXPECT_EQ(p.eval(e, trueacct, i, arn1, principal),
              Effect::Allow);
    ASSERT_TRUE(principal);
    EXPECT_TRUE(principal->is_account());
    ARN arn2(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "mybucket/myobject");
    EXPECT_EQ(p.eval(e, trueacct, i, arn2, principal),
              Effect::Allow);
    ARN arn3(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "mybucket");
    EXPECT_EQ(p.eval(e, notacct, i, arn3, principal),
              Effect::Pass);
    ARN arn4(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "mybucket/myobject");
    EXPECT_EQ(p.eval(e, notacct, i, arn4, principal),
              Effect::Pass);
    ARN arn5(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "notyourbucket");
    EXPECT_EQ(p.eval(e, trueacct, i, arn5, principal),
              Effect::Pass);
    ARN arn6(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "notyourbucket/notyourobject");
    EXPECT_EQ(p.eval(e, trueacct, i, arn6, principal),
              Effect::Pass);

  }
}

TEST_F(PolicyTest, Parse3) {
  boost::optional<Policy> p;

  ASSERT_NO_THROW(p = Policy(cct.get(), &arbitrary_tenant, example3, true));
  ASSERT_TRUE(p);

  EXPECT_EQ(p->text, example3);
  EXPECT_EQ(p->version, Version::v2012_10_17);
  EXPECT_FALSE(p->id);
  ASSERT_FALSE(p->statements.empty());
  EXPECT_EQ(p->statements.size(), 3U);

  EXPECT_EQ(*p->statements[0].sid, "FirstStatement");
  EXPECT_TRUE(p->statements[0].princ.empty());
  EXPECT_TRUE(p->statements[0].noprinc.empty());
  EXPECT_EQ(p->statements[0].effect, Effect::Allow);
  Action_t act;
  act[s3PutBucketPolicy] = 1;
  EXPECT_EQ(p->statements[0].action, act);
  EXPECT_EQ(p->statements[0].notaction, None);
  ASSERT_FALSE(p->statements[0].resource.empty());
  ASSERT_EQ(p->statements[0].resource.size(), 1U);
  EXPECT_EQ(p->statements[0].resource.begin()->partition, Partition::wildcard);
  EXPECT_EQ(p->statements[0].resource.begin()->service, Service::wildcard);
  EXPECT_EQ(p->statements[0].resource.begin()->region, "*");
  EXPECT_EQ(p->statements[0].resource.begin()->account, arbitrary_tenant);
  EXPECT_EQ(p->statements[0].resource.begin()->resource, "*");
  EXPECT_TRUE(p->statements[0].notresource.empty());
  EXPECT_TRUE(p->statements[0].conditions.empty());

  EXPECT_EQ(*p->statements[1].sid, "SecondStatement");
  EXPECT_TRUE(p->statements[1].princ.empty());
  EXPECT_TRUE(p->statements[1].noprinc.empty());
  EXPECT_EQ(p->statements[1].effect, Effect::Allow);
  Action_t act1;
  act1[s3ListAllMyBuckets] = 1;
  EXPECT_EQ(p->statements[1].action, act1);
  EXPECT_EQ(p->statements[1].notaction, None);
  ASSERT_FALSE(p->statements[1].resource.empty());
  ASSERT_EQ(p->statements[1].resource.size(), 1U);
  EXPECT_EQ(p->statements[1].resource.begin()->partition, Partition::wildcard);
  EXPECT_EQ(p->statements[1].resource.begin()->service, Service::wildcard);
  EXPECT_EQ(p->statements[1].resource.begin()->region, "*");
  EXPECT_EQ(p->statements[1].resource.begin()->account, arbitrary_tenant);
  EXPECT_EQ(p->statements[1].resource.begin()->resource, "*");
  EXPECT_TRUE(p->statements[1].notresource.empty());
  EXPECT_TRUE(p->statements[1].conditions.empty());

  EXPECT_EQ(*p->statements[2].sid, "ThirdStatement");
  EXPECT_TRUE(p->statements[2].princ.empty());
  EXPECT_TRUE(p->statements[2].noprinc.empty());
  EXPECT_EQ(p->statements[2].effect, Effect::Allow);
  Action_t act2;
  act2[s3ListMultipartUploadParts] = 1;
  act2[s3ListBucket] = 1;
  act2[s3ListBucketVersions] = 1;
  act2[s3ListAllMyBuckets] = 1;
  act2[s3ListBucketMultipartUploads] = 1;
  act2[s3GetObject] = 1;
  act2[s3GetObjectVersion] = 1;
  act2[s3GetObjectAcl] = 1;
  act2[s3GetObjectVersionAcl] = 1;
  act2[s3GetObjectTorrent] = 1;
  act2[s3GetObjectVersionTorrent] = 1;
  act2[s3GetObjectAttributes] = 1;
  act2[s3GetObjectVersionAttributes] = 1;
  act2[s3GetAccelerateConfiguration] = 1;
  act2[s3GetBucketAcl] = 1;
  act2[s3GetBucketOwnershipControls] = 1;
  act2[s3GetBucketCORS] = 1;
  act2[s3GetBucketVersioning] = 1;
  act2[s3GetBucketRequestPayment] = 1;
  act2[s3GetBucketLocation] = 1;
  act2[s3GetBucketPolicy] = 1;
  act2[s3GetBucketNotification] = 1;
  act2[s3GetBucketLogging] = 1;
  act2[s3GetBucketTagging] = 1;
  act2[s3GetBucketWebsite] = 1;
  act2[s3GetLifecycleConfiguration] = 1;
  act2[s3GetReplicationConfiguration] = 1;
  act2[s3GetObjectTagging] = 1;
  act2[s3GetObjectVersionTagging] = 1;
  act2[s3GetBucketObjectLockConfiguration] = 1;
  act2[s3GetObjectRetention] = 1;
  act2[s3GetObjectLegalHold] = 1;
  act2[s3GetBucketPolicyStatus] = 1;
  act2[s3GetBucketPublicAccessBlock] = 1;
  act2[s3GetPublicAccessBlock] = 1;
  act2[s3GetBucketEncryption] = 1;
  act2[s3GetObjectVersionForReplication] = 1;
  act2[s3GetAccountPublicAccessBlock] = 1;

  EXPECT_EQ(p->statements[2].action, act2);
  EXPECT_EQ(p->statements[2].notaction, None);
  ASSERT_FALSE(p->statements[2].resource.empty());
  ASSERT_EQ(p->statements[2].resource.size(), 2U);
  EXPECT_EQ(p->statements[2].resource.begin()->partition, Partition::aws);
  EXPECT_EQ(p->statements[2].resource.begin()->service, Service::s3);
  EXPECT_TRUE(p->statements[2].resource.begin()->region.empty());
  EXPECT_EQ(p->statements[2].resource.begin()->account, arbitrary_tenant);
  EXPECT_EQ(p->statements[2].resource.begin()->resource, "confidential-data");
  EXPECT_EQ((p->statements[2].resource.begin() + 1)->partition,
	    Partition::aws);
  EXPECT_EQ((p->statements[2].resource.begin() + 1)->service, Service::s3);
  EXPECT_TRUE((p->statements[2].resource.begin() + 1)->region.empty());
  EXPECT_EQ((p->statements[2].resource.begin() + 1)->account,
	    arbitrary_tenant);
  EXPECT_EQ((p->statements[2].resource.begin() + 1)->resource,
	    "confidential-data/*");
  EXPECT_TRUE(p->statements[2].notresource.empty());
  ASSERT_FALSE(p->statements[2].conditions.empty());
  ASSERT_EQ(p->statements[2].conditions.size(), 1U);
  EXPECT_EQ(p->statements[2].conditions[0].op, TokenID::Bool);
  EXPECT_EQ(p->statements[2].conditions[0].key, "aws:MultiFactorAuthPresent");
  EXPECT_FALSE(p->statements[2].conditions[0].ifexists);
  ASSERT_FALSE(p->statements[2].conditions[0].vals.empty());
  EXPECT_EQ(p->statements[2].conditions[0].vals.size(), 1U);
  EXPECT_EQ(p->statements[2].conditions[0].vals[0], "true");
}

TEST_F(PolicyTest, Eval3) {
  auto p  = Policy(cct.get(), &arbitrary_tenant, example3, true);
  Environment em;

  Environment tr = { { "aws:MultiFactorAuthPresent", "true" } };
  Environment fa = { { "aws:MultiFactorAuthPresent", "false" } };
  boost::optional<Principal> principal;

  Action_t s3allow;
  s3allow[s3ListMultipartUploadParts] = 1;
  s3allow[s3ListBucket] = 1;
  s3allow[s3ListBucketVersions] = 1;
  s3allow[s3ListAllMyBuckets] = 1;
  s3allow[s3ListBucketMultipartUploads] = 1;
  s3allow[s3GetObject] = 1;
  s3allow[s3GetObjectVersion] = 1;
  s3allow[s3GetObjectAcl] = 1;
  s3allow[s3GetObjectVersionAcl] = 1;
  s3allow[s3GetObjectAttributes] = 1;
  s3allow[s3GetObjectVersionAttributes] = 1;
  s3allow[s3GetObjectTorrent] = 1;
  s3allow[s3GetObjectVersionTorrent] = 1;
  s3allow[s3GetAccelerateConfiguration] = 1;
  s3allow[s3GetBucketAcl] = 1;
  s3allow[s3GetBucketOwnershipControls] = 1;
  s3allow[s3GetBucketCORS] = 1;
  s3allow[s3GetBucketVersioning] = 1;
  s3allow[s3GetBucketRequestPayment] = 1;
  s3allow[s3GetBucketLocation] = 1;
  s3allow[s3GetBucketPolicy] = 1;
  s3allow[s3GetBucketNotification] = 1;
  s3allow[s3GetBucketLogging] = 1;
  s3allow[s3GetBucketTagging] = 1;
  s3allow[s3GetBucketWebsite] = 1;
  s3allow[s3GetLifecycleConfiguration] = 1;
  s3allow[s3GetReplicationConfiguration] = 1;
  s3allow[s3GetObjectTagging] = 1;
  s3allow[s3GetObjectVersionTagging] = 1;
  s3allow[s3GetBucketObjectLockConfiguration] = 1;
  s3allow[s3GetObjectRetention] = 1;
  s3allow[s3GetObjectLegalHold] = 1;
  s3allow[s3GetBucketPolicyStatus] = 1;
  s3allow[s3GetBucketPublicAccessBlock] = 1;
  s3allow[s3GetPublicAccessBlock] = 1;
  s3allow[s3GetBucketEncryption] = 1;
  s3allow[s3GetObjectVersionForReplication] = 1;
  s3allow[s3GetAccountPublicAccessBlock] = 1;

  ARN arn1(Partition::aws, Service::s3,
		       "", arbitrary_tenant, "mybucket");
  EXPECT_EQ(p.eval(em, none, s3PutBucketPolicy, arn1, principal),
            Effect::Allow);
  EXPECT_FALSE(principal);

  ARN arn2(Partition::aws, Service::s3,
		       "", arbitrary_tenant, "mybucket");
  EXPECT_EQ(p.eval(em, none, s3PutBucketPolicy, arn2, principal),
            Effect::Allow);


  for (auto op = 0ULL; op < s3All; ++op) {
    if ((op == s3ListAllMyBuckets) || (op == s3PutBucketPolicy)) {
      continue;
    }
    ARN arn3(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "confidential-data");
    EXPECT_EQ(p.eval(em, none, op, arn3, principal),
              Effect::Pass);
    ARN arn4(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "confidential-data");
    EXPECT_EQ(p.eval(tr, none, op, arn4, principal),
              s3allow[op] ? Effect::Allow : Effect::Pass);
    ARN arn5(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "confidential-data");
    EXPECT_EQ(p.eval(fa, none, op, arn5, principal),
              Effect::Pass);
    ARN arn6(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "confidential-data/moo");
    EXPECT_EQ(p.eval(em, none, op, arn6, principal),
              Effect::Pass);
    ARN arn7(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "confidential-data/moo");
    EXPECT_EQ(p.eval(tr, none, op, arn7, principal),
              s3allow[op] ? Effect::Allow : Effect::Pass);
    ARN arn8(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "confidential-data/moo");
    EXPECT_EQ(p.eval(fa, none, op, arn8, principal),
              Effect::Pass);
    ARN arn9(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "really-confidential-data");
    EXPECT_EQ(p.eval(em, none, op, arn9, principal),
              Effect::Pass);
    ARN arn10(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "really-confidential-data");
    EXPECT_EQ(p.eval(tr, none, op, arn10, principal),
              Effect::Pass);
    ARN arn11(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "really-confidential-data");
    EXPECT_EQ(p.eval(fa, none, op, arn11, principal),
	      Effect::Pass);
    ARN arn12(Partition::aws, Service::s3,
			 "", arbitrary_tenant,
			 "really-confidential-data/moo");
    EXPECT_EQ(p.eval(em, none, op, arn12, principal), Effect::Pass);
    ARN arn13(Partition::aws, Service::s3,
			 "", arbitrary_tenant,
			 "really-confidential-data/moo");
    EXPECT_EQ(p.eval(tr, none, op, arn13, principal), Effect::Pass);
    ARN arn14(Partition::aws, Service::s3,
			 "", arbitrary_tenant,
			 "really-confidential-data/moo");
    EXPECT_EQ(p.eval(fa, none, op, arn14, principal), Effect::Pass);
  }
}

TEST_F(PolicyTest, Parse4) {
  boost::optional<Policy> p;

  ASSERT_NO_THROW(p = Policy(cct.get(), &arbitrary_tenant, example4, true));
  ASSERT_TRUE(p);

  EXPECT_EQ(p->text, example4);
  EXPECT_EQ(p->version, Version::v2012_10_17);
  EXPECT_FALSE(p->id);
  EXPECT_FALSE(p->statements[0].sid);
  EXPECT_FALSE(p->statements.empty());
  EXPECT_EQ(p->statements.size(), 1U);
  EXPECT_TRUE(p->statements[0].princ.empty());
  EXPECT_TRUE(p->statements[0].noprinc.empty());
  EXPECT_EQ(p->statements[0].effect, Effect::Allow);
  Action_t act;
  act[iamCreateRole] = 1;
  EXPECT_EQ(p->statements[0].action, act);
  EXPECT_EQ(p->statements[0].notaction, None);
  ASSERT_FALSE(p->statements[0].resource.empty());
  ASSERT_EQ(p->statements[0].resource.size(), 1U);
  EXPECT_EQ(p->statements[0].resource.begin()->partition, Partition::wildcard);
  EXPECT_EQ(p->statements[0].resource.begin()->service, Service::wildcard);
  EXPECT_EQ(p->statements[0].resource.begin()->region, "*");
  EXPECT_EQ(p->statements[0].resource.begin()->account, arbitrary_tenant);
  EXPECT_EQ(p->statements[0].resource.begin()->resource, "*");
  EXPECT_TRUE(p->statements[0].notresource.empty());
  EXPECT_TRUE(p->statements[0].conditions.empty());
}

TEST_F(PolicyTest, Eval4) {
  auto p  = Policy(cct.get(), &arbitrary_tenant, example4, true);
  Environment e;
  boost::optional<Principal> principal;

  ARN arn1(Partition::aws, Service::iam,
		       "", arbitrary_tenant, "role/example_role");
  EXPECT_EQ(p.eval(e, none, iamCreateRole, arn1, principal),
            Effect::Allow);
  EXPECT_FALSE(principal);

  ARN arn2(Partition::aws, Service::iam,
		       "", arbitrary_tenant, "role/example_role");
  EXPECT_EQ(p.eval(e, none, iamDeleteRole, arn2, principal),
            Effect::Pass);
}

TEST_F(PolicyTest, Parse5) {
  boost::optional<Policy> p;

  ASSERT_NO_THROW(p = Policy(cct.get(), &arbitrary_tenant, example5, true));
  ASSERT_TRUE(p);
  EXPECT_EQ(p->text, example5);
  EXPECT_EQ(p->version, Version::v2012_10_17);
  EXPECT_FALSE(p->id);
  EXPECT_FALSE(p->statements[0].sid);
  EXPECT_FALSE(p->statements.empty());
  EXPECT_EQ(p->statements.size(), 1U);
  EXPECT_TRUE(p->statements[0].princ.empty());
  EXPECT_TRUE(p->statements[0].noprinc.empty());
  EXPECT_EQ(p->statements[0].effect, Effect::Allow);
  Action_t act;
  for (auto i = s3objectlambdaAll+1; i <= iamAll; i++)
    act[i] = 1;
  EXPECT_EQ(p->statements[0].action, act);
  EXPECT_EQ(p->statements[0].notaction, None);
  ASSERT_FALSE(p->statements[0].resource.empty());
  ASSERT_EQ(p->statements[0].resource.size(), 1U);
  EXPECT_EQ(p->statements[0].resource.begin()->partition, Partition::aws);
  EXPECT_EQ(p->statements[0].resource.begin()->service, Service::iam);
  EXPECT_EQ(p->statements[0].resource.begin()->region, "");
  EXPECT_EQ(p->statements[0].resource.begin()->account, arbitrary_tenant);
  EXPECT_EQ(p->statements[0].resource.begin()->resource, "role/example_role");
  EXPECT_TRUE(p->statements[0].notresource.empty());
  EXPECT_TRUE(p->statements[0].conditions.empty());
}

TEST_F(PolicyTest, Eval5) {
  auto p  = Policy(cct.get(), &arbitrary_tenant, example5, true);
  Environment e;
  boost::optional<Principal> principal;

  ARN arn1(Partition::aws, Service::iam,
		       "", arbitrary_tenant, "role/example_role");
  EXPECT_EQ(p.eval(e, none, iamCreateRole, arn1, principal),
            Effect::Allow);
  EXPECT_FALSE(principal);

  ARN arn2(Partition::aws, Service::iam,
		       "", arbitrary_tenant, "role/example_role");
  EXPECT_EQ(p.eval(e, none, s3ListBucket, arn2, principal),
            Effect::Pass);

  ARN arn3(Partition::aws, Service::iam,
		       "", "", "role/example_role");
  EXPECT_EQ(p.eval(e, none, iamCreateRole, arn3, principal),
            Effect::Pass);
}

TEST_F(PolicyTest, Parse6) {
  boost::optional<Policy> p;

  ASSERT_NO_THROW(p = Policy(cct.get(), &arbitrary_tenant, example6, true));
  ASSERT_TRUE(p);
  EXPECT_EQ(p->text, example6);
  EXPECT_EQ(p->version, Version::v2012_10_17);
  EXPECT_FALSE(p->id);
  EXPECT_FALSE(p->statements[0].sid);
  EXPECT_FALSE(p->statements.empty());
  EXPECT_EQ(p->statements.size(), 1U);
  EXPECT_TRUE(p->statements[0].princ.empty());
  EXPECT_TRUE(p->statements[0].noprinc.empty());
  EXPECT_EQ(p->statements[0].effect, Effect::Allow);
  Action_t act;
  for (auto i = 0U; i <= organizationsAll; i++)
    act[i] = 1;
  for (auto i = 0U; i <= s3vectorsAll; i++)
    act[i] = 1;
  EXPECT_EQ(p->statements[0].action, act);
  EXPECT_EQ(p->statements[0].notaction, None);
  ASSERT_FALSE(p->statements[0].resource.empty());
  ASSERT_EQ(p->statements[0].resource.size(), 1U);
  EXPECT_EQ(p->statements[0].resource.begin()->partition, Partition::aws);
  EXPECT_EQ(p->statements[0].resource.begin()->service, Service::iam);
  EXPECT_EQ(p->statements[0].resource.begin()->region, "");
  EXPECT_EQ(p->statements[0].resource.begin()->account, arbitrary_tenant);
  EXPECT_EQ(p->statements[0].resource.begin()->resource, "user/A");
  EXPECT_TRUE(p->statements[0].notresource.empty());
  EXPECT_TRUE(p->statements[0].conditions.empty());
}

TEST_F(PolicyTest, Eval6) {
  auto p  = Policy(cct.get(), &arbitrary_tenant, example6, true);
  Environment e;
  boost::optional<Principal> principal;

  ARN arn1(Partition::aws, Service::iam,
		       "", arbitrary_tenant, "user/A");
  EXPECT_EQ(p.eval(e, none, iamCreateRole, arn1, principal),
            Effect::Allow);
  EXPECT_FALSE(principal);

  ARN arn2(Partition::aws, Service::iam,
		       "", arbitrary_tenant, "user/A");
  EXPECT_EQ(p.eval(e, none, s3ListBucket, arn2, principal),
            Effect::Allow);
}

TEST_F(PolicyTest, Parse7) {
  boost::optional<Policy> p;

  ASSERT_NO_THROW(p = Policy(cct.get(), &arbitrary_tenant, example7, true));
  ASSERT_TRUE(p);

  EXPECT_EQ(p->text, example7);
  EXPECT_EQ(p->version, Version::v2012_10_17);
  ASSERT_FALSE(p->statements.empty());
  EXPECT_EQ(p->statements.size(), 1U);
  EXPECT_FALSE(p->statements[0].princ.empty());
  EXPECT_EQ(p->statements[0].princ.size(), 1U);
  EXPECT_TRUE(p->statements[0].noprinc.empty());
  EXPECT_EQ(p->statements[0].effect, Effect::Allow);
  Action_t act;
  act[s3ListBucket] = 1;
  EXPECT_EQ(p->statements[0].action, act);
  EXPECT_EQ(p->statements[0].notaction, None);
  ASSERT_FALSE(p->statements[0].resource.empty());
  ASSERT_EQ(p->statements[0].resource.size(), 1U);
  EXPECT_EQ(p->statements[0].resource.begin()->partition, Partition::aws);
  EXPECT_EQ(p->statements[0].resource.begin()->service, Service::s3);
  EXPECT_TRUE(p->statements[0].resource.begin()->region.empty());
  EXPECT_EQ(p->statements[0].resource.begin()->account, arbitrary_tenant);
  EXPECT_EQ(p->statements[0].resource.begin()->resource, "mybucket/*");
  EXPECT_TRUE(p->statements[0].princ.begin()->is_user());
  EXPECT_FALSE(p->statements[0].princ.begin()->is_wildcard());
  EXPECT_EQ(p->statements[0].princ.begin()->get_account(), "");
  EXPECT_EQ(p->statements[0].princ.begin()->get_id(), "A:subA");
  EXPECT_TRUE(p->statements[0].notresource.empty());
  EXPECT_TRUE(p->statements[0].conditions.empty());
}

TEST_F(PolicyTest, Eval7) {
  auto p  = Policy(cct.get(), &arbitrary_tenant, example7, true);
  Environment e;
  boost::optional<Principal> principal;

  auto subacct = PrincipalIdentity(
    Principal::user(std::move(""), "A:subA"));
  auto parentacct = PrincipalIdentity(
    Principal::user(std::move(""), "A"));
  auto sub2acct = PrincipalIdentity(
    Principal::user(std::move(""), "A:sub2A"));

  ARN arn1(Partition::aws, Service::s3,
		       "", arbitrary_tenant, "mybucket/*");
  EXPECT_EQ(p.eval(e, subacct, s3ListBucket, arn1, principal),
            Effect::Allow);
  ASSERT_TRUE(principal);
  EXPECT_TRUE(principal->is_user());

  ARN arn2(Partition::aws, Service::s3,
		       "", arbitrary_tenant, "mybucket/*");
  EXPECT_EQ(p.eval(e, parentacct, s3ListBucket, arn2, principal),
            Effect::Pass);

  ARN arn3(Partition::aws, Service::s3,
		       "", arbitrary_tenant, "mybucket/*");
  EXPECT_EQ(p.eval(e, sub2acct, s3ListBucket, arn3, principal),
            Effect::Pass);
}


class ManagedPolicyTest : public ::testing::Test {
protected:
  intrusive_ptr<CephContext> cct;
public:
  ManagedPolicyTest() {
    cct.reset(new CephContext(CEPH_ENTITY_TYPE_CLIENT), false);
  }
};

TEST_F(ManagedPolicyTest, IAMFullAccess)
{
  auto p = get_managed_policy(cct.get(), "arn:aws:iam::aws:policy/IAMFullAccess");
  ASSERT_TRUE(p);

  Action_t act = iamAllValue | organizationsAllValue;
  act[iamAll] = 1;
  act[organizationsAll] = 1;
  EXPECT_EQ(act, p->statements[0].action);
}

TEST_F(ManagedPolicyTest, IAMReadOnlyAccess)
{
  auto p = get_managed_policy(cct.get(), "arn:aws:iam::aws:policy/IAMReadOnlyAccess");
  ASSERT_TRUE(p);

  Action_t act;
  act[iamGenerateCredentialReport] = 1;
  act[iamGenerateServiceLastAccessedDetails] = 1;
  act[iamGetUserPolicy] = 1;
  act[iamGetRole] = 1;
  act[iamGetRolePolicy] = 1;
  act[iamGetOIDCProvider] = 1;
  act[iamGetUser] = 1;
  act[iamListUserPolicies] = 1;
  act[iamListAttachedUserPolicies] = 1;
  act[iamListRoles] = 1;
  act[iamListRolePolicies] = 1;
  act[iamListAttachedRolePolicies] = 1;
  act[iamListOIDCProviders] = 1;
  act[iamListRoleTags] = 1;
  act[iamListUsers] = 1;
  act[iamListAccessKeys] = 1;
  act[iamGetGroup] = 1;
  act[iamListGroups] = 1;
  act[iamListGroupsForUser] = 1;
  act[iamGetGroupPolicy] = 1;
  act[iamListGroupPolicies] = 1;
  act[iamListAttachedGroupPolicies] = 1;
  act[iamSimulateCustomPolicy] = 1;
  act[iamSimulatePrincipalPolicy] = 1;
  act[iamGetAccountSummary] = 1;

  EXPECT_EQ(act, p->statements[0].action);
}

TEST_F(ManagedPolicyTest, AmazonSNSFullAccess)
{
  auto p = get_managed_policy(cct.get(), "arn:aws:iam::aws:policy/AmazonSNSFullAccess");
  ASSERT_TRUE(p);

  Action_t act = snsAllValue;
  act[snsAll] = 1;
  EXPECT_EQ(act, p->statements[0].action);
}

TEST_F(ManagedPolicyTest, AmazonSNSReadOnlyAccess)
{
  auto p = get_managed_policy(cct.get(), "arn:aws:iam::aws:policy/AmazonSNSReadOnlyAccess");
  ASSERT_TRUE(p);

  Action_t act;
  // sns:GetTopicAttributes
  act[snsGetTopicAttributes] = 1;
  // sns:List*
  act[snsListTopics] = 1;

  EXPECT_EQ(act, p->statements[0].action);
}

TEST_F(ManagedPolicyTest, AmazonS3FullAccess)
{
  auto p = get_managed_policy(cct.get(), "arn:aws:iam::aws:policy/AmazonS3FullAccess");
  ASSERT_TRUE(p);

  Action_t act = s3AllValue | s3objectlambdaAllValue;
  act[s3All] = 1;
  act[s3objectlambdaAll] = 1;
  EXPECT_EQ(act, p->statements[0].action);
}

TEST_F(ManagedPolicyTest, AmazonS3ReadOnlyAccess)
{
  auto p = get_managed_policy(cct.get(), "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess");
  ASSERT_TRUE(p);

  Action_t act;
  // s3:Get*
  act[s3GetObject] = 1;
  act[s3GetObjectVersion] = 1;
  act[s3GetObjectAcl] = 1;
  act[s3GetObjectVersionAcl] = 1;
  act[s3GetObjectTorrent] = 1;
  act[s3GetObjectVersionTorrent] = 1;
  act[s3GetObjectAttributes] = 1;
  act[s3GetObjectVersionAttributes] = 1;
  act[s3GetAccelerateConfiguration] = 1;
  act[s3GetBucketAcl] = 1;
  act[s3GetBucketOwnershipControls] = 1;
  act[s3GetBucketCORS] = 1;
  act[s3GetBucketVersioning] = 1;
  act[s3GetBucketRequestPayment] = 1;
  act[s3GetBucketLocation] = 1;
  act[s3GetBucketPolicy] = 1;
  act[s3GetBucketNotification] = 1;
  act[s3GetBucketLogging] = 1;
  act[s3GetBucketTagging] = 1;
  act[s3GetBucketWebsite] = 1;
  act[s3GetLifecycleConfiguration] = 1;
  act[s3GetReplicationConfiguration] = 1;
  act[s3GetObjectTagging] = 1;
  act[s3GetObjectVersionTagging] = 1;
  act[s3GetBucketObjectLockConfiguration] = 1;
  act[s3GetObjectRetention] = 1;
  act[s3GetObjectLegalHold] = 1;
  act[s3GetBucketPolicyStatus] = 1;
  act[s3GetPublicAccessBlock] = 1;
  act[s3GetBucketPublicAccessBlock] = 1;
  act[s3GetBucketEncryption] = 1;
  act[s3GetObjectVersionForReplication] = 1;
  act[s3GetAccountPublicAccessBlock] = 1;
  // s3:List*
  act[s3ListMultipartUploadParts] = 1;
  act[s3ListBucket] = 1;
  act[s3ListBucketVersions] = 1;
  act[s3ListAllMyBuckets] = 1;
  act[s3ListBucketMultipartUploads] = 1;
  // s3:Describe*
  act[s3DescribeJob] = 1;
  // s3-object-lambda:Get*
  act[s3objectlambdaGetObject] = 1;
  // s3-object-lambda:List*
  act[s3objectlambdaListBucket] = 1;
  act[s3objectlambdaAll] = 1;

  EXPECT_EQ(act, p->statements[0].action);
}

const string PolicyTest::arbitrary_tenant = "arbitrary_tenant";
string PolicyTest::example1 = R"(
{
  "Version": "2012-10-17",
  "Statement": {
    "Effect": "Allow",
    "Action": "s3:ListBucket",
    "Resource": "arn:aws:s3:::example_bucket"
  }
}
)";

string PolicyTest::example2 = R"(
{
  "Version": "2012-10-17",
  "Id": "S3-Account-Permissions",
  "Statement": [{
    "Sid": "1",
    "Effect": "Allow",
    "Principal": {"AWS": ["arn:aws:iam::ACCOUNT-ID-WITHOUT-HYPHENS:root"]},
    "Action": "s3:*",
    "Resource": [
      "arn:aws:s3:::mybucket",
      "arn:aws:s3:::mybucket/*"
    ]
  }]
}
)";

string PolicyTest::example3 = R"(
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "FirstStatement",
      "Effect": "Allow",
      "Action": ["s3:PutBucketPolicy"],
      "Resource": "*"
    },
    {
      "Sid": "SecondStatement",
      "Effect": "Allow",
      "Action": "s3:ListAllMyBuckets",
      "Resource": "*"
    },
    {
      "Sid": "ThirdStatement",
      "Effect": "Allow",
      "Action": [
	"s3:List*",
	"s3:Get*"
      ],
      "Resource": [
	"arn:aws:s3:::confidential-data",
	"arn:aws:s3:::confidential-data/*"
      ],
      "Condition": {"Bool": {"aws:MultiFactorAuthPresent": "true"}}
    }
  ]
}
)";

string PolicyTest::example4 = R"(
{
  "Version": "2012-10-17",
  "Statement": {
    "Effect": "Allow",
    "Action": "iam:CreateRole",
    "Resource": "*"
  }
}
)";

string PolicyTest::example5 = R"(
{
  "Version": "2012-10-17",
  "Statement": {
    "Effect": "Allow",
    "Action": "iam:*",
    "Resource": "arn:aws:iam:::role/example_role"
  }
}
)";

string PolicyTest::example6 = R"(
{
  "Version": "2012-10-17",
  "Statement": {
    "Effect": "Allow",
    "Action": "*",
    "Resource": "arn:aws:iam:::user/A"
  }
}
)";

string PolicyTest::example7 = R"(
{
  "Version": "2012-10-17",
  "Statement": {
    "Effect": "Allow",
    "Principal": {"AWS": ["arn:aws:iam:::user/A:subA"]},
    "Action": "s3:ListBucket",
    "Resource": "arn:aws:s3:::mybucket/*"
  }
}
)";
class IPPolicyTest : public ::testing::Test {
protected:
  intrusive_ptr<CephContext> cct;
  static const string arbitrary_tenant;
  static string ip_address_allow_example;
  static string ip_address_deny_example;
  static string ip_address_full_example;
  // 192.168.1.0/24
  const rgw::IAM::MaskedIP allowedIPv4Range = { false, rgw::IAM::Address("11000000101010000000000100000000"), 24 };
  // 192.168.1.1/32
  const rgw::IAM::MaskedIP blocklistedIPv4 = { false, rgw::IAM::Address("11000000101010000000000100000001"), 32 };
  // 2001:db8:85a3:0:0:8a2e:370:7334/128
  const rgw::IAM::MaskedIP allowedIPv6 = { true, rgw::IAM::Address("00100000000000010000110110111000100001011010001100000000000000000000000000000000100010100010111000000011011100000111001100110100"), 128 };
  // ::1
  const rgw::IAM::MaskedIP blocklistedIPv6 = { true, rgw::IAM::Address(1), 128 };
  // 2001:db8:85a3:0:0:8a2e:370:7330/124
  const rgw::IAM::MaskedIP allowedIPv6Range = { true, rgw::IAM::Address("00100000000000010000110110111000100001011010001100000000000000000000000000000000100010100010111000000011011100000111001100110000"), 124 };
public:
  IPPolicyTest() {
    cct.reset(new CephContext(CEPH_ENTITY_TYPE_CLIENT), false);
  }
};
const string IPPolicyTest::arbitrary_tenant = "arbitrary_tenant";

TEST_F(IPPolicyTest, MaskedIPOperations) {
  EXPECT_EQ(stringify(allowedIPv4Range), "192.168.1.0/24");
  EXPECT_EQ(stringify(blocklistedIPv4), "192.168.1.1/32");
  EXPECT_EQ(stringify(allowedIPv6), "2001:db8:85a3:0:0:8a2e:370:7334/128");
  EXPECT_EQ(stringify(allowedIPv6Range), "2001:db8:85a3:0:0:8a2e:370:7330/124");
  EXPECT_EQ(stringify(blocklistedIPv6), "0:0:0:0:0:0:0:1/128");
  EXPECT_EQ(allowedIPv4Range, blocklistedIPv4);
  EXPECT_EQ(allowedIPv6Range, allowedIPv6);
}

TEST_F(IPPolicyTest, asNetworkIPv4Range) {
  auto actualIPv4Range = rgw::IAM::Condition::as_network("192.168.1.0/24");
  ASSERT_TRUE(actualIPv4Range.is_initialized());
  EXPECT_EQ(*actualIPv4Range, allowedIPv4Range);
}

TEST_F(IPPolicyTest, asNetworkIPv4) {
  auto actualIPv4 = rgw::IAM::Condition::as_network("192.168.1.1");
  ASSERT_TRUE(actualIPv4.is_initialized());
  EXPECT_EQ(*actualIPv4, blocklistedIPv4);
}

TEST_F(IPPolicyTest, asNetworkIPv6Range) {
  auto actualIPv6Range = rgw::IAM::Condition::as_network("2001:db8:85a3:0:0:8a2e:370:7330/124");
  ASSERT_TRUE(actualIPv6Range.is_initialized());
  EXPECT_EQ(*actualIPv6Range, allowedIPv6Range);
}

TEST_F(IPPolicyTest, asNetworkIPv6) {
  auto actualIPv6 = rgw::IAM::Condition::as_network("2001:db8:85a3:0:0:8a2e:370:7334");
  ASSERT_TRUE(actualIPv6.is_initialized());
  EXPECT_EQ(*actualIPv6, allowedIPv6);
}

TEST_F(IPPolicyTest, asNetworkInvalid) {
  EXPECT_FALSE(rgw::IAM::Condition::as_network(""));
  EXPECT_FALSE(rgw::IAM::Condition::as_network("192.168.1.1/33"));
  EXPECT_FALSE(rgw::IAM::Condition::as_network("2001:db8:85a3:0:0:8a2e:370:7334/129"));
  EXPECT_FALSE(rgw::IAM::Condition::as_network("192.168.1.1:"));
  EXPECT_FALSE(rgw::IAM::Condition::as_network("1.2.3.10000"));
}

class DumbUser : public rgw::sal::StoreUser {
  using StoreUser::StoreUser;
  std::unique_ptr<User> clone() {
    return std::make_unique<DumbUser>(*this);
  }
  int read_attrs(const DoutPrefixProvider*, optional_yield) {
    return -ENOTSUP;
  }
  int merge_and_store_attrs(const DoutPrefixProvider*, rgw::sal::Attrs&,
			    optional_yield) {
    return -ENOTSUP;
  }
  int read_usage(const DoutPrefixProvider*, uint64_t, uint64_t, uint32_t,
		 bool*, RGWUsageIter&,
		 std::map<rgw_user_bucket, rgw_usage_log_entry>&) {
    return -ENOTSUP;
  }
  virtual int trim_usage(const DoutPrefixProvider*, uint64_t,
			 uint64_t, optional_yield) {
    return -ENOTSUP;
  }
  int load_user(const DoutPrefixProvider* dpp, optional_yield y) {
    return -ENOTSUP;
  }
  int store_user(const DoutPrefixProvider*, optional_yield, bool, RGWUserInfo*) {
    return -ENOTSUP;
  }
  int remove_user(const DoutPrefixProvider*, optional_yield) {
    return -ENOTSUP;
  }
  int verify_mfa(const std::string&, bool*, const DoutPrefixProvider*,
		 optional_yield) {
    return -ENOTSUP;
  }
  int list_groups(const DoutPrefixProvider*, optional_yield,
		  std::string_view, uint32_t, rgw::sal::GroupList&) {
    return -ENOTSUP;
  }
};

TEST_F(IPPolicyTest, IPEnvironment) {
  RGWProcessEnv penv;
  // Unfortunately RGWCivetWeb is too tightly tied to civetweb to test RGWCivetWeb::init_env.
  RGWEnv rgw_env;
  std::unique_ptr<rgw::sal::User> user = std::make_unique<DumbUser>(rgw_user());
  rgw_env.set("REMOTE_ADDR", "192.168.1.1");
  rgw_env.set("HTTP_HOST", "1.2.3.4");
  req_state rgw_req_state(cct.get(), penv, &rgw_env, 0);
  rgw_req_state.set_user(user);
  rgw_build_iam_environment(&rgw_req_state);
  auto ip = rgw_req_state.env.find("aws:SourceIp");
  ASSERT_NE(ip, rgw_req_state.env.end());
  EXPECT_EQ(ip->second, "192.168.1.1");

  ASSERT_EQ(cct.get()->_conf.set_val("rgw_remote_addr_param", "SOME_VAR"), 0);
  EXPECT_EQ(cct.get()->_conf->rgw_remote_addr_param, "SOME_VAR");
  rgw_req_state.env.clear();
  rgw_build_iam_environment(&rgw_req_state);
  ip = rgw_req_state.env.find("aws:SourceIp");
  EXPECT_EQ(ip, rgw_req_state.env.end());

  rgw_env.set("SOME_VAR", "192.168.1.2");
  rgw_req_state.env.clear();
  rgw_build_iam_environment(&rgw_req_state);
  ip = rgw_req_state.env.find("aws:SourceIp");
  ASSERT_NE(ip, rgw_req_state.env.end());
  EXPECT_EQ(ip->second, "192.168.1.2");

  ASSERT_EQ(cct.get()->_conf.set_val("rgw_remote_addr_param", "HTTP_X_FORWARDED_FOR"), 0);
  rgw_env.set("HTTP_X_FORWARDED_FOR", "192.168.1.3");
  rgw_req_state.env.clear();
  rgw_build_iam_environment(&rgw_req_state);
  ip = rgw_req_state.env.find("aws:SourceIp");
  ASSERT_NE(ip, rgw_req_state.env.end());
  EXPECT_EQ(ip->second, "192.168.1.3");

  rgw_env.set("HTTP_X_FORWARDED_FOR", "192.168.1.4, 4.3.2.1, 2001:db8:85a3:8d3:1319:8a2e:370:7348");
  rgw_req_state.env.clear();
  rgw_build_iam_environment(&rgw_req_state);
  ip = rgw_req_state.env.find("aws:SourceIp");
  ASSERT_NE(ip, rgw_req_state.env.end());
  EXPECT_EQ(ip->second, "192.168.1.4");
}

TEST_F(IPPolicyTest, ParseIPAddress) {
  boost::optional<Policy> p;

  ASSERT_NO_THROW(
    p = Policy(cct.get(), &arbitrary_tenant, ip_address_full_example, true));
  ASSERT_TRUE(p);

  EXPECT_EQ(p->text, ip_address_full_example);
  EXPECT_EQ(p->version, Version::v2012_10_17);
  EXPECT_EQ(*p->id, "S3IPPolicyTest");
  EXPECT_FALSE(p->statements.empty());
  EXPECT_EQ(p->statements.size(), 1U);
  EXPECT_EQ(*p->statements[0].sid, "IPAllow");
  EXPECT_FALSE(p->statements[0].princ.empty());
  EXPECT_EQ(p->statements[0].princ.size(), 1U);
  EXPECT_EQ(*p->statements[0].princ.begin(),
	    Principal::wildcard());
  EXPECT_TRUE(p->statements[0].noprinc.empty());
  EXPECT_EQ(p->statements[0].effect, Effect::Allow);
  Action_t act;
  act[s3ListBucket] = 1;
  EXPECT_EQ(p->statements[0].action, act);
  EXPECT_EQ(p->statements[0].notaction, None);
  ASSERT_FALSE(p->statements[0].resource.empty());
  ASSERT_EQ(p->statements[0].resource.size(), 2U);
  EXPECT_EQ(p->statements[0].resource.begin()->partition, Partition::aws);
  EXPECT_EQ(p->statements[0].resource.begin()->service, Service::s3);
  EXPECT_TRUE(p->statements[0].resource.begin()->region.empty());
  EXPECT_EQ(p->statements[0].resource.begin()->account, arbitrary_tenant);
  EXPECT_EQ(p->statements[0].resource.begin()->resource, "example_bucket");
  EXPECT_EQ((p->statements[0].resource.begin() + 1)->resource, "example_bucket/*");
  EXPECT_TRUE(p->statements[0].notresource.empty());
  ASSERT_FALSE(p->statements[0].conditions.empty());
  ASSERT_EQ(p->statements[0].conditions.size(), 2U);
  EXPECT_EQ(p->statements[0].conditions[0].op, TokenID::IpAddress);
  EXPECT_EQ(p->statements[0].conditions[0].key, "aws:SourceIp");
  ASSERT_FALSE(p->statements[0].conditions[0].vals.empty());
  EXPECT_EQ(p->statements[0].conditions[0].vals.size(), 2U);
  EXPECT_EQ(p->statements[0].conditions[0].vals[0], "192.168.1.0/24");
  EXPECT_EQ(p->statements[0].conditions[0].vals[1], "::1");
  boost::optional<rgw::IAM::MaskedIP> convertedIPv4 = rgw::IAM::Condition::as_network(p->statements[0].conditions[0].vals[0]);
  EXPECT_TRUE(convertedIPv4.is_initialized());
  if (convertedIPv4.is_initialized()) {
    EXPECT_EQ(*convertedIPv4, allowedIPv4Range);
  }

  EXPECT_EQ(p->statements[0].conditions[1].op, TokenID::NotIpAddress);
  EXPECT_EQ(p->statements[0].conditions[1].key, "aws:SourceIp");
  ASSERT_FALSE(p->statements[0].conditions[1].vals.empty());
  EXPECT_EQ(p->statements[0].conditions[1].vals.size(), 2U);
  EXPECT_EQ(p->statements[0].conditions[1].vals[0], "192.168.1.1/32");
  EXPECT_EQ(p->statements[0].conditions[1].vals[1], "2001:0db8:85a3:0000:0000:8a2e:0370:7334");
  boost::optional<rgw::IAM::MaskedIP> convertedIPv6 = rgw::IAM::Condition::as_network(p->statements[0].conditions[1].vals[1]);
  EXPECT_TRUE(convertedIPv6.is_initialized());
  if (convertedIPv6.is_initialized()) {
    EXPECT_EQ(*convertedIPv6, allowedIPv6);
  }
}

TEST_F(IPPolicyTest, EvalIPAddress) {
  auto allowp =
    Policy(cct.get(), &arbitrary_tenant, ip_address_allow_example, true);
  auto denyp =
    Policy(cct.get(), &arbitrary_tenant, ip_address_deny_example, true);
  auto fullp =
    Policy(cct.get(), &arbitrary_tenant, ip_address_full_example, true);
  Environment e;
  Environment allowedIP, blocklistedIP, allowedIPv6, blocklistedIPv6;
  allowedIP.emplace("aws:SourceIp","192.168.1.2");
  allowedIPv6.emplace("aws:SourceIp", "::1");
  blocklistedIP.emplace("aws:SourceIp", "192.168.1.1");
  blocklistedIPv6.emplace("aws:SourceIp", "2001:0db8:85a3:0000:0000:8a2e:0370:7334");
  boost::optional<Principal> principal;

  auto trueacct = PrincipalIdentity(
    Principal::account("ACCOUNT-ID-WITHOUT-HYPHENS"));
  // Without an IP address in the environment then evaluation will always pass
  ARN arn1(Partition::aws, Service::s3,
			    "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(allowp.eval(e, trueacct, s3ListBucket, arn1, principal),
            Effect::Pass);
  ARN arn2(Partition::aws, Service::s3,
      "", arbitrary_tenant, "example_bucket/myobject");
  EXPECT_EQ(fullp.eval(e, trueacct, s3ListBucket, arn2, principal),
            Effect::Pass);

  ARN arn3(Partition::aws, Service::s3,
			    "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(allowp.eval(allowedIP, trueacct, s3ListBucket, arn3, principal),
            Effect::Allow);
  ARN arn4(Partition::aws, Service::s3,
			    "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(allowp.eval(blocklistedIPv6, trueacct, s3ListBucket, arn4, principal),
            Effect::Pass);

  ARN arn5(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(denyp.eval(allowedIP, trueacct, s3ListBucket, arn5, principal),
            Effect::Deny);
  ARN arn6(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket/myobject");
  EXPECT_EQ(denyp.eval(allowedIP, trueacct, s3ListBucket, arn6, principal),
            Effect::Deny);

  ARN arn7(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(denyp.eval(blocklistedIP, trueacct, s3ListBucket, arn7, principal),
            Effect::Pass);
  ARN arn8(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket/myobject");
  EXPECT_EQ(denyp.eval(blocklistedIP, trueacct, s3ListBucket, arn8, principal),
            Effect::Pass);

  ARN arn9(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(denyp.eval(blocklistedIPv6, trueacct, s3ListBucket, arn9, principal),
            Effect::Pass);
  ARN arn10(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket/myobject");
  EXPECT_EQ(denyp.eval(blocklistedIPv6, trueacct, s3ListBucket, arn10, principal),
	    Effect::Pass);
  ARN arn11(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(denyp.eval(allowedIPv6, trueacct, s3ListBucket, arn11, principal),
	    Effect::Deny);
  ARN arn12(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket/myobject");
  EXPECT_EQ(denyp.eval(allowedIPv6, trueacct, s3ListBucket, arn12, principal),
	    Effect::Deny);

  ARN arn13(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(fullp.eval(allowedIP, trueacct, s3ListBucket, arn13, principal),
	    Effect::Allow);
  ARN arn14(Partition::aws, Service::s3,
      "", arbitrary_tenant, "example_bucket/myobject");
  EXPECT_EQ(fullp.eval(allowedIP, trueacct, s3ListBucket, arn14, principal),
	    Effect::Allow);

  ARN arn15(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(fullp.eval(blocklistedIP, trueacct, s3ListBucket, arn15, principal),
	    Effect::Pass);
  ARN arn16(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket/myobject");
  EXPECT_EQ(fullp.eval(blocklistedIP, trueacct, s3ListBucket, arn16, principal),
	    Effect::Pass);

  ARN arn17(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(fullp.eval(allowedIPv6, trueacct, s3ListBucket, arn17, principal),
	    Effect::Allow);
  ARN arn18(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket/myobject");
  EXPECT_EQ(fullp.eval(allowedIPv6, trueacct, s3ListBucket, arn18, principal),
	    Effect::Allow);

  ARN arn19(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(fullp.eval(blocklistedIPv6, trueacct, s3ListBucket, arn19, principal),
	    Effect::Pass);
  ARN arn20(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket/myobject");
  EXPECT_EQ(fullp.eval(blocklistedIPv6, trueacct, s3ListBucket, arn20, principal),
	    Effect::Pass);
}

string IPPolicyTest::ip_address_allow_example = R"(
{
  "Version": "2012-10-17",
  "Id": "S3SimpleIPPolicyTest",
  "Statement": [{
    "Sid": "1",
    "Effect": "Allow",
    "Principal": {"AWS": ["arn:aws:iam::ACCOUNT-ID-WITHOUT-HYPHENS:root"]},
    "Action": "s3:ListBucket",
    "Resource": [
      "arn:aws:s3:::example_bucket"
    ],
    "Condition": {
      "IpAddress": {"aws:SourceIp": "192.168.1.0/24"}
    }
  }]
}
)";

string IPPolicyTest::ip_address_deny_example = R"(
{
  "Version": "2012-10-17",
  "Id": "S3IPPolicyTest",
  "Statement": {
    "Effect": "Deny",
    "Sid": "IPDeny",
    "Action": "s3:ListBucket",
    "Principal": {"AWS": ["arn:aws:iam::ACCOUNT-ID-WITHOUT-HYPHENS:root"]},
    "Resource": [
      "arn:aws:s3:::example_bucket",
      "arn:aws:s3:::example_bucket/*"
    ],
    "Condition": {
      "NotIpAddress": {"aws:SourceIp": ["192.168.1.1/32", "2001:0db8:85a3:0000:0000:8a2e:0370:7334"]}
    }
  }
}
)";

string IPPolicyTest::ip_address_full_example = R"(
{
  "Version": "2012-10-17",
  "Id": "S3IPPolicyTest",
  "Statement": {
    "Effect": "Allow",
    "Sid": "IPAllow",
    "Action": "s3:ListBucket",
    "Principal": "*",
    "Resource": [
      "arn:aws:s3:::example_bucket",
      "arn:aws:s3:::example_bucket/*"
    ],
    "Condition": {
      "IpAddress": {"aws:SourceIp": ["192.168.1.0/24", "::1"]},
      "NotIpAddress": {"aws:SourceIp": ["192.168.1.1/32", "2001:0db8:85a3:0000:0000:8a2e:0370:7334"]}
    }
  }
}
)";

TEST(MatchWildcards, Simple)
{
  EXPECT_TRUE(match_wildcards("", ""));
  EXPECT_TRUE(match_wildcards("", "", MATCH_CASE_INSENSITIVE));
  EXPECT_FALSE(match_wildcards("", "abc"));
  EXPECT_FALSE(match_wildcards("", "abc", MATCH_CASE_INSENSITIVE));
  EXPECT_FALSE(match_wildcards("abc", ""));
  EXPECT_FALSE(match_wildcards("abc", "", MATCH_CASE_INSENSITIVE));
  EXPECT_TRUE(match_wildcards("abc", "abc"));
  EXPECT_TRUE(match_wildcards("abc", "abc", MATCH_CASE_INSENSITIVE));
  EXPECT_FALSE(match_wildcards("abc", "abC"));
  EXPECT_TRUE(match_wildcards("abc", "abC", MATCH_CASE_INSENSITIVE));
  EXPECT_FALSE(match_wildcards("abC", "abc"));
  EXPECT_TRUE(match_wildcards("abC", "abc", MATCH_CASE_INSENSITIVE));
  EXPECT_FALSE(match_wildcards("abc", "abcd"));
  EXPECT_FALSE(match_wildcards("abc", "abcd", MATCH_CASE_INSENSITIVE));
  EXPECT_FALSE(match_wildcards("abcd", "abc"));
  EXPECT_FALSE(match_wildcards("abcd", "abc", MATCH_CASE_INSENSITIVE));
}

TEST(MatchWildcards, QuestionMark)
{
  EXPECT_FALSE(match_wildcards("?", ""));
  EXPECT_FALSE(match_wildcards("?", "", MATCH_CASE_INSENSITIVE));
  EXPECT_TRUE(match_wildcards("?", "a"));
  EXPECT_TRUE(match_wildcards("?", "a", MATCH_CASE_INSENSITIVE));
  EXPECT_TRUE(match_wildcards("?bc", "abc"));
  EXPECT_TRUE(match_wildcards("?bc", "abc", MATCH_CASE_INSENSITIVE));
  EXPECT_TRUE(match_wildcards("a?c", "abc"));
  EXPECT_TRUE(match_wildcards("a?c", "abc", MATCH_CASE_INSENSITIVE));
  EXPECT_FALSE(match_wildcards("abc", "a?c"));
  EXPECT_FALSE(match_wildcards("abc", "a?c", MATCH_CASE_INSENSITIVE));
  EXPECT_FALSE(match_wildcards("a?c", "abC"));
  EXPECT_TRUE(match_wildcards("a?c", "abC", MATCH_CASE_INSENSITIVE));
  EXPECT_TRUE(match_wildcards("ab?", "abc"));
  EXPECT_TRUE(match_wildcards("ab?", "abc", MATCH_CASE_INSENSITIVE));
  EXPECT_TRUE(match_wildcards("a?c?e", "abcde"));
  EXPECT_TRUE(match_wildcards("a?c?e", "abcde", MATCH_CASE_INSENSITIVE));
  EXPECT_TRUE(match_wildcards("???", "abc"));
  EXPECT_TRUE(match_wildcards("???", "abc", MATCH_CASE_INSENSITIVE));
  EXPECT_FALSE(match_wildcards("???", "abcd"));
  EXPECT_FALSE(match_wildcards("???", "abcd", MATCH_CASE_INSENSITIVE));
}

TEST(MatchWildcards, Asterisk)
{
  EXPECT_TRUE(match_wildcards("*", ""));
  EXPECT_TRUE(match_wildcards("*", "", MATCH_CASE_INSENSITIVE));
  EXPECT_FALSE(match_wildcards("", "*"));
  EXPECT_FALSE(match_wildcards("", "*", MATCH_CASE_INSENSITIVE));
  EXPECT_FALSE(match_wildcards("*a", ""));
  EXPECT_FALSE(match_wildcards("*a", "", MATCH_CASE_INSENSITIVE));
  EXPECT_TRUE(match_wildcards("*a", "a"));
  EXPECT_TRUE(match_wildcards("*a", "a", MATCH_CASE_INSENSITIVE));
  EXPECT_TRUE(match_wildcards("a*", "a"));
  EXPECT_TRUE(match_wildcards("a*", "a", MATCH_CASE_INSENSITIVE));
  EXPECT_TRUE(match_wildcards("a*c", "ac"));
  EXPECT_TRUE(match_wildcards("a*c", "ac", MATCH_CASE_INSENSITIVE));
  EXPECT_TRUE(match_wildcards("a*c", "abbc"));
  EXPECT_TRUE(match_wildcards("a*c", "abbc", MATCH_CASE_INSENSITIVE));
  EXPECT_FALSE(match_wildcards("a*c", "abbC"));
  EXPECT_TRUE(match_wildcards("a*c", "abbC", MATCH_CASE_INSENSITIVE));
  EXPECT_TRUE(match_wildcards("a*c*e", "abBce"));
  EXPECT_TRUE(match_wildcards("a*c*e", "abBce", MATCH_CASE_INSENSITIVE));
  EXPECT_TRUE(match_wildcards("http://*.example.com",
                              "http://www.example.com"));
  EXPECT_TRUE(match_wildcards("http://*.example.com",
                              "http://www.example.com", MATCH_CASE_INSENSITIVE));
  EXPECT_FALSE(match_wildcards("http://*.example.com",
                               "http://www.Example.com"));
  EXPECT_TRUE(match_wildcards("http://*.example.com",
                              "http://www.Example.com", MATCH_CASE_INSENSITIVE));
  EXPECT_TRUE(match_wildcards("http://example.com/*",
                              "http://example.com/index.html"));
  EXPECT_TRUE(match_wildcards("http://example.com/*/*.jpg",
                              "http://example.com/fun/smiley.jpg"));
  EXPECT_TRUE(match_wildcards("a*c", "abcc"));
  EXPECT_TRUE(match_wildcards("a*c", "abcc", MATCH_CASE_INSENSITIVE));
}

TEST(MatchPolicy, Action)
{
  constexpr auto flag = MATCH_POLICY_ACTION;
  EXPECT_TRUE(match_policy("a:b:c", "a:b:c", flag));
  EXPECT_TRUE(match_policy("a:b:c", "A:B:C", flag)); // case insensitive
  EXPECT_TRUE(match_policy("a:*:e", "a:bcd:e", flag));
  EXPECT_FALSE(match_policy("a:*", "a:b:c", flag)); // cannot span segments
}

TEST(MatchPolicy, ARN)
{
  constexpr auto flag = MATCH_POLICY_ARN;
  EXPECT_TRUE(match_policy("a:b:c", "a:b:c", flag));
  EXPECT_FALSE(match_policy("a:b:c", "A:B:C", flag)); // case sensitive
  EXPECT_TRUE(match_policy("a:*:e", "a:bcd:e", flag));
  EXPECT_FALSE(match_policy("a:*", "a:b:c", flag)); // cannot span segments
}

Action_t set_range_bits(std::uint64_t start, std::uint64_t end)
{
  Action_t result;
  for (uint64_t i = start; i < end; i++) {
    result.set(i);
  }
  return result;
}

TEST(set_cont_bits, iamconsts)
{
  EXPECT_EQ(s3AllValue, set_range_bits(0, s3All));
  EXPECT_EQ(s3objectlambdaAllValue, set_range_bits(s3All+1, s3objectlambdaAll));
  EXPECT_EQ(iamAllValue, set_range_bits(s3objectlambdaAll+1, iamAll));
  EXPECT_EQ(stsAllValue, set_range_bits(iamAll+1, stsAll));
  EXPECT_EQ(snsAllValue, set_range_bits(stsAll+1, snsAll));
  EXPECT_EQ(organizationsAllValue, set_range_bits(snsAll+1, organizationsAll));
  EXPECT_EQ(allValue , set_range_bits(0, allCount));
}

TEST(Condition, ArnLike)
{
  const std::string key = "aws:SourceArn";
  {
    Condition ArnLike{TokenID::ArnLike, key.data(), key.size(), false};
    ArnLike.vals.push_back("arn:aws:s3:::bucket");

    EXPECT_FALSE(ArnLike.eval({}, nullptr));
    EXPECT_TRUE(ArnLike.eval({{key, "arn:aws:s3:::bucket"}}, nullptr));
    EXPECT_FALSE(ArnLike.eval({{key, "arn:aws:s3:::BUCKET"}}, nullptr));
    EXPECT_FALSE(ArnLike.eval({{key, "arn:aws:s3:::user"}}, nullptr));
  }
  {
    Condition ArnLike{TokenID::ArnLike, key.data(), key.size(), false};
    ArnLike.vals.push_back("arn:aws:s3:::b*");

    EXPECT_FALSE(ArnLike.eval({}, nullptr));
    EXPECT_TRUE(ArnLike.eval({{key, "arn:aws:s3:::b"}}, nullptr));
    EXPECT_TRUE(ArnLike.eval({{key, "arn:aws:s3:::bucket"}}, nullptr));
    EXPECT_FALSE(ArnLike.eval({{key, "arn:aws:s3:::BUCKET"}}, nullptr));
    EXPECT_FALSE(ArnLike.eval({{key, "arn:aws:s3:::user"}}, nullptr));
  }
}


class ConditionTest : public ::testing::Test {
protected:
  intrusive_ptr<CephContext> cct;
  
  ConditionTest() {
    cct.reset(new CephContext(CEPH_ENTITY_TYPE_CLIENT), false);
  }
};

// Test cases for NotEquals condition logic fix
// These tests verify that NotEquals conditions use correct AND logic
// instead of incorrect OR logic (requiring mismatch with ALL values, not ANY)

TEST_F(ConditionTest, StringNotEqualsLogic)
{
  std::string key = "aws:UserName";

  // Test case: value matches one of multiple condition values
  // Should return false because value equals at least one condition value
  {
    Condition stringNotEquals{TokenID::StringNotEquals, key.data(), key.size(), false};
    stringNotEquals.vals.push_back("alice");
    stringNotEquals.vals.push_back("bob");
    stringNotEquals.vals.push_back("charlie");

    // Input "bob" matches second condition value, should return false
    EXPECT_FALSE(stringNotEquals.eval({{key, "bob"}}, nullptr));
    // Input "alice" matches first condition value, should return false
    EXPECT_FALSE(stringNotEquals.eval({{key, "alice"}}, nullptr));
  }

  // Test case: value doesn't match any condition values
  // Should return true because value differs from all condition values
  {
    Condition stringNotEquals{TokenID::StringNotEquals, key.data(), key.size(), false};
    stringNotEquals.vals.push_back("alice");
    stringNotEquals.vals.push_back("bob");
    stringNotEquals.vals.push_back("charlie");

    // Input "david" doesn't match any condition value, should return true
    EXPECT_TRUE(stringNotEquals.eval({{key, "david"}}, nullptr));
  }
}

TEST_F(ConditionTest, NumericNotEqualsLogic)
{
  std::string key = "aws:RequestedRegion";
  
  // Test case: value matches one of multiple condition values
  // Should return false because value equals at least one condition value
  {
    Condition numericNotEquals{TokenID::NumericNotEquals, key.data(), key.size(), false};
    numericNotEquals.vals.push_back("10");
    numericNotEquals.vals.push_back("20");
    numericNotEquals.vals.push_back("30");

    // Input "20" matches second condition value, should return false
    EXPECT_FALSE(numericNotEquals.eval({{key, "20"}}, nullptr));
    // Input "10" matches first condition value, should return false
    EXPECT_FALSE(numericNotEquals.eval({{key, "10"}}, nullptr));
  }

  // Test case: value doesn't match any condition values
  // Should return true because value differs from all condition values
  {
    Condition numericNotEquals{TokenID::NumericNotEquals, key.data(), key.size(), false};
    numericNotEquals.vals.push_back("10");
    numericNotEquals.vals.push_back("20");
    numericNotEquals.vals.push_back("30");

    // Input "40" doesn't match any condition value, should return true
    EXPECT_TRUE(numericNotEquals.eval({{key, "40"}}, nullptr));
  }
}

TEST_F(ConditionTest, DateNotEqualsLogic)
{
  std::string key = "aws:CurrentTime";

  // Test case: value matches one of multiple condition values
  // Should return false because value equals at least one condition value
  {
    Condition dateNotEquals{TokenID::DateNotEquals, key.data(), key.size(), false};
    dateNotEquals.vals.push_back("2023-01-01T00:00:00Z");
    dateNotEquals.vals.push_back("2023-06-01T00:00:00Z");
    dateNotEquals.vals.push_back("2023-12-01T00:00:00Z");

    // Input matches second condition value, should return false
    EXPECT_FALSE(dateNotEquals.eval({{key, "2023-06-01T00:00:00Z"}}, nullptr));
  }

  // Test case: value doesn't match any condition values
  // Should return true because value differs from all condition values
  {
    Condition dateNotEquals{TokenID::DateNotEquals, key.data(), key.size(), false};
    dateNotEquals.vals.push_back("2023-01-01T00:00:00Z");
    dateNotEquals.vals.push_back("2023-06-01T00:00:00Z");
    dateNotEquals.vals.push_back("2023-12-01T00:00:00Z");

    // Input doesn't match any condition value, should return true
    EXPECT_TRUE(dateNotEquals.eval({{key, "2024-01-01T00:00:00Z"}}, nullptr));
  }
}

TEST_F(ConditionTest, NotIpAddressLogic)
{
  std::string key = "aws:SourceIp";

  // Test case: value matches one of multiple condition values
  // Should return false because value equals at least one condition value
  {
    Condition notIpAddress{TokenID::NotIpAddress, key.data(), key.size(), false};
    notIpAddress.vals.push_back("192.168.1.1");
    notIpAddress.vals.push_back("10.0.0.1");
    notIpAddress.vals.push_back("172.16.0.1");

    // Input matches second condition value, should return false
    EXPECT_FALSE(notIpAddress.eval({{key, "10.0.0.1"}}, nullptr));
    // Input matches first condition value, should return false
    EXPECT_FALSE(notIpAddress.eval({{key, "192.168.1.1"}}, nullptr));
  }

  // Test case: value doesn't match any condition values
  // Should return true because value differs from all condition values
  {
    Condition notIpAddress{TokenID::NotIpAddress, key.data(), key.size(), false};
    notIpAddress.vals.push_back("192.168.1.1");
    notIpAddress.vals.push_back("10.0.0.1");
    notIpAddress.vals.push_back("172.16.0.1");

    // Input doesn't match any condition value, should return true
    EXPECT_TRUE(notIpAddress.eval({{key, "8.8.8.8"}}, nullptr));
  }
}

TEST_F(ConditionTest, ArnNotEqualsLogic)
{
  std::string key = "aws:SourceArn";

  // Test case: value matches one of multiple condition values
  // Should return false because value equals at least one condition value
  {
    Condition arnNotEquals{TokenID::ArnNotEquals, key.data(), key.size(), false};
    arnNotEquals.vals.push_back("arn:aws:s3:::bucket1");
    arnNotEquals.vals.push_back("arn:aws:s3:::bucket2");
    arnNotEquals.vals.push_back("arn:aws:s3:::bucket3");

    // Input matches second condition value, should return false
    EXPECT_FALSE(arnNotEquals.eval({{key, "arn:aws:s3:::bucket2"}}, nullptr));
  }

  // Test case: value doesn't match any condition values
  // Should return true because value differs from all condition values
  {
    Condition arnNotEquals{TokenID::ArnNotEquals, key.data(), key.size(), false};
    arnNotEquals.vals.push_back("arn:aws:s3:::bucket1");
    arnNotEquals.vals.push_back("arn:aws:s3:::bucket2");
    arnNotEquals.vals.push_back("arn:aws:s3:::bucket3");

    // Input doesn't match any condition value, should return true
    EXPECT_TRUE(arnNotEquals.eval({{key, "arn:aws:s3:::other-bucket"}},
                                  nullptr));
  }
}

TEST_F(ConditionTest, StringNotLikeLogic)
{
  std::string key = "s3:prefix";

  // Test case: value matches one of multiple condition patterns
  // Should return false because value matches at least one condition pattern
  {
    Condition stringNotLike{TokenID::StringNotLike, key.data(), key.size(), false};
    stringNotLike.vals.push_back("user/*");
    stringNotLike.vals.push_back("admin/*");
    stringNotLike.vals.push_back("temp/*");

    // Input matches second condition pattern, should return false
    EXPECT_FALSE(stringNotLike.eval({{key, "admin/config.txt"}}, nullptr));
    // Input matches first condition pattern, should return false
    EXPECT_FALSE(stringNotLike.eval({{key, "user/profile.jpg"}}, nullptr));
  }

  // Test case: value doesn't match any condition patterns
  // Should return true because value differs from all condition patterns
  {
    Condition stringNotLike{TokenID::StringNotLike, key.data(), key.size(), false};
    stringNotLike.vals.push_back("user/*");
    stringNotLike.vals.push_back("admin/*");
    stringNotLike.vals.push_back("temp/*");

    // Input doesn't match any condition pattern, should return true
    EXPECT_TRUE(stringNotLike.eval({{key, "public/document.pdf"}}, nullptr));
  }
}

TEST_F(ConditionTest, Null)
{
  const std::string key = "s3:prefix";

  {
    // "Null": {"s3:prefix": "true"}
    Condition isNull{TokenID::Null, key.data(), key.size(), false};
    isNull.vals.push_back("true");

    EXPECT_TRUE(isNull.eval({}, nullptr));
    EXPECT_FALSE(isNull.eval({{key, "admin/config.txt"}}, nullptr));
  }

  {
    // "Null": {"s3:prefix": "false"}
    Condition notNull{TokenID::Null, key.data(), key.size(), false};
    notNull.vals.push_back("false");

    EXPECT_FALSE(notNull.eval({}, nullptr));
    EXPECT_TRUE(notNull.eval({{key, "admin/config.txt"}}, nullptr));
  }
}

TEST_F(ConditionTest, KeyStoneRoleStringEquals)
{
  std::string key = "keystone:role";

  Condition cond{TokenID::StringEquals, key.data(), key.size(), false};
  cond.vals.push_back("testrole");

  // No roles
  EXPECT_FALSE(cond.eval({}, nullptr));

  // Single matching role
  EXPECT_TRUE(cond.eval({{key, "testrole"}}, nullptr));

  // Single non-matching role
  EXPECT_FALSE(cond.eval({{key, "member"}}, nullptr));

  //Multiple roles in env,one matches
  Environment multi_env;
  multi_env.emplace(key, "member");
  multi_env.emplace(key, "testrole");
  multi_env.emplace(key, "reader");
  EXPECT_TRUE(cond.eval(multi_env, nullptr));

  //Multiple roles in env, no match
  Environment no_match_env;
  no_match_env.emplace(key, "member");
  no_match_env.emplace(key, "reader");
  EXPECT_FALSE(cond.eval(no_match_env, nullptr));

  // Multiple identical roles (redundancy check)
  Environment duplicate_env;
  duplicate_env.emplace(key, "testrole");
  duplicate_env.emplace(key, "testrole");
  EXPECT_TRUE(cond.eval(duplicate_env, nullptr));
}

TEST_F(ConditionTest, KeyStoneRoleNotStringEquals)
{
  std::string key = "keystone:role";

  Condition cond{TokenID::StringNotEquals, key.data(), key.size(), false};
  cond.vals.push_back("admin");

  // No roles
  EXPECT_FALSE(cond.eval({}, nullptr));

  // Role matches
  EXPECT_FALSE(cond.eval({{key, "admin"}}, nullptr));

  // Role doesn't match
  EXPECT_TRUE(cond.eval({{key, "member"}}, nullptr));

  // Multiple roles in env, one matches -> false
  Environment multi_env;
  multi_env.emplace(key, "member");
  multi_env.emplace(key, "admin");
  EXPECT_FALSE(multi_env.count(key) == 0);
  EXPECT_FALSE(cond.eval(multi_env, nullptr));

  // Multiple roles, none match -> true
  Environment no_match_env;
  no_match_env.emplace(key, "member");
  no_match_env.emplace(key, "reader");
  EXPECT_TRUE(cond.eval(no_match_env, nullptr));
}

TEST_F(ConditionTest, KeyStoneRolePolicyParsing)
{
  string keystone_role_policy = R"(
  {
    "Version": "2012-10-17",
    "Statement": [{
      "Effect" : "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": [
        "arn:aws:s3:::example_bucket/*"
      ],
      "Condition": {
        "StringEquals": {
          "keystone:role": "testrole"
        }
      }
    }]
  }
  )";

  string tenant = "arbitrary_tenant";
  boost::optional<Policy> p;

  ASSERT_NO_THROW(
    p = Policy(cct.get(), &tenant, keystone_role_policy, true));
  ASSERT_TRUE(p);
  EXPECT_EQ(p->statements.size(), 1U);
  EXPECT_EQ(p->statements[0].conditions.size(), 1U);
  EXPECT_EQ(p->statements[0].conditions[0].key, "keystone:role");
  EXPECT_EQ(p->statements[0].conditions[0].vals.size(), 1U);
  EXPECT_EQ(p->statements[0].conditions[0].vals[0], "testrole");

  // Eval with matching role in environment
  Environment match_env;
  match_env.emplace("keystone:role", "testrole");
  EXPECT_TRUE(p->statements[0].conditions[0].eval(match_env, nullptr));

  // Eval with non-matching role
  Environment nomatch_env;
  nomatch_env.emplace("keystone:role", "member");
  EXPECT_FALSE(p->statements[0].conditions[0].eval(nomatch_env, nullptr));

  // Eval with multiple roles, one matching
  Environment multi_env;
  multi_env.emplace("keystone:role", "member");
  multi_env.emplace("keystone:role", "testrole");
  EXPECT_TRUE(p->statements[0].conditions[0].eval(multi_env, nullptr));
}

TEST_F(ConditionTest, KeystoneUserIdStringEquals)
{
  const std::string key = "keystone:userid";
  Condition cond{TokenID::StringEquals, key.data(), key.size(), false};
  cond.vals.push_back("user-123");

  EXPECT_FALSE(cond.eval({}, nullptr));
  EXPECT_TRUE(cond.eval({{key, "user-123"}}, nullptr));
  EXPECT_FALSE(cond.eval({{key, "user-456"}}, nullptr));
}

// Policy variables ("runtime conditions"): condition values containing
// ${key}, substituted from the request context at evaluation time.
//
// Behaviour follows the AWS IAM User Guide, "IAM policy elements:
// Variables and tags",
// https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_variables.html
// Cited below as:
//  [V1] "Variables are marked using a $ prefix followed by a pair of
//       curly braces ({ }) that include the variable name of the value
//       from the request."
//  [V2] "You can use a policy variable for Condition values in any
//       condition that involves the string operators or the ARN
//       operators. ... You can't use a policy variable with other
//       operators, such as Numeric, Date, Boolean, Binary, IP Address,
//       or Null operators."
//  [V3] "Key names are case-insensitive. ... You can use any
//       single-valued condition key as a variable. You can't use a
//       multivalued condition key as a variable."
//  [V4] "When you use a variable with no value in the condition element
//       ... StringEquals or StringLike do not match ... Inverted
//       condition operators like StringNotEquals or StringNotLike do
//       match against a null value".
//  [V5] "If you don't include the Version element and set it to an
//       appropriate version date, variables like ${aws:username} are
//       treated as literal strings in the policy." (2012-10-17 required.)
//  [V6] Special characters ${*}, ${?}, ${$} "can be used in any string
//       where you can use regular policy variables."
//  [V7] Default values: "surround the default value with single quotes
//       (' '), and separate the variable text and the default value with
//       a comma and space (, )", e.g. ${aws:PrincipalTag/team, 'none'}.
//  [V8] Documented example of a variable embedded in a longer value:
//       {"StringLike": {"s3:prefix": ["${aws:PrincipalTag/team}/*"]}}
//
// Tests on the RuntimeConditionTest fixture assert documented AWS
// behaviour. Tests on RuntimeConditionDecision assert choices of ours
// where AWS is silent or where matching AWS is a compatibility
// decision; they are grouped so they can be revisited together.

class RuntimeConditionTest : public ::testing::Test {
protected:
  intrusive_ptr<CephContext> cct;
  const std::string tenant = "tenant";

  RuntimeConditionTest() {
    cct.reset(new CephContext(CEPH_ENTITY_TYPE_CLIENT), false);
  }

  // JSON-quote a string value.
  static std::string q(std::string_view s) {
    return fmt::format(R"("{}")", s);
  }

  // A one-statement policy with a single condition. `vals` is raw JSON:
  // a quoted string or an array of them. An empty `version` omits the
  // Version element altogether.
  static std::string policy_text(std::string_view op, std::string_view key,
                                 std::string_view vals,
                                 std::string_view version = "2012-10-17") {
    std::string s = "{";
    if (!version.empty()) {
      s += fmt::format(R"("Version": "{}", )", version);
    }
    s += fmt::format(
      R"("Statement": [{{"Effect": "Allow", "Action": "s3:ListBucket", )"
      R"("Resource": "arn:aws:s3:::bucket", )"
      R"("Condition": {{"{}": {{"{}": {}}}}}}}]}})",
      op, key, vals);
    return s;
  }

  boost::optional<Policy> parse(const std::string& text) {
    try {
      return Policy(cct.get(), &tenant, text, true);
    } catch (const rgw::IAM::PolicyParseException&) {
      return boost::none;
    }
  }

  bool parses(const std::string& text) {
    return bool(parse(text));
  }

  // Evaluate the single condition of a one-statement policy. A parse
  // failure is a test failure and evaluates false.
  bool eval(const std::string& text, const Environment& env) {
    auto p = parse(text);
    if (!p) {
      ADD_FAILURE() << "failed to parse: " << text;
      return false;
    }
    if (p->statements.size() != 1 || p->statements[0].conditions.size() != 1) {
      ADD_FAILURE() << "expected exactly one condition: " << text;
      return false;
    }
    return p->statements[0].conditions[0].eval(env, nullptr);
  }
};

using RuntimeConditionDecision = RuntimeConditionTest;

// --- What is and is not a variable ----------------------------------

TEST_F(RuntimeConditionTest, LiteralDollarIsNotAVariable)
{
  // Only "${" opens a variable [V1]. A bare "$" is an ordinary
  // character and the value is an ordinary literal.
  for (const char* lit : {"$", "$5", "$x", "price$", "$$", "{aws:username}",
                          "$ {aws:username}"}) {
    auto text = policy_text("StringEquals", "s3:prefix", q(lit));
    ASSERT_TRUE(parses(text)) << text;
    EXPECT_TRUE(eval(text, {{"s3:prefix", lit}})) << lit;
    EXPECT_FALSE(eval(text, {{"s3:prefix", "alice"},
                             {"aws:username", "alice"}})) << lit;
  }
}

TEST_F(RuntimeConditionTest, EmbeddedVariablesParse)
{
  // [V8]: a variable may be part of a longer value.
  for (const char* v : {"${aws:username}/*",
                        "home/${aws:username}",
                        "home/${aws:username}/*",
                        "${aws:PrincipalTag/team}-${aws:PrincipalTag/env}",
                        "arn:aws:s3:::bucket/${aws:username}/*"}) {
    EXPECT_TRUE(parses(policy_text("StringLike", "s3:prefix", q(v)))) << v;
  }
}

TEST_F(RuntimeConditionTest, EmbeddedVariablesSubstitute)
{
  // The documented example [V8].
  auto team = policy_text("StringLike", "s3:prefix",
                          q("${aws:PrincipalTag/team}/*"));
  EXPECT_TRUE(eval(team, {{"s3:prefix", "marketing/q3.csv"},
                          {"aws:PrincipalTag/team", "marketing"}}));
  EXPECT_FALSE(eval(team, {{"s3:prefix", "finance/q3.csv"},
                           {"aws:PrincipalTag/team", "marketing"}}));
  // No team tag: the variable has no value and nothing is like it [V4].
  EXPECT_FALSE(eval(team, {{"s3:prefix", "marketing/q3.csv"}}));

  auto two = policy_text("StringEquals", "s3:prefix",
                         q("${aws:PrincipalTag/team}-${aws:PrincipalTag/env}"));
  EXPECT_TRUE(eval(two, {{"s3:prefix", "web-prod"},
                         {"aws:PrincipalTag/team", "web"},
                         {"aws:PrincipalTag/env", "prod"}}));
  EXPECT_FALSE(eval(two, {{"s3:prefix", "web-prod"},
                          {"aws:PrincipalTag/team", "web"}}));
  EXPECT_FALSE(eval(two, {{"s3:prefix", "web-"},
                          {"aws:PrincipalTag/team", "web"}}));
}

// --- Operators that take variables [V2] -----------------------------

TEST_F(RuntimeConditionTest, WholeValueVariableStringOperators)
{
  const Environment match{{"s3:prefix", "alice"}, {"aws:username", "alice"}};
  const Environment differ{{"s3:prefix", "bob"}, {"aws:username", "alice"}};
  const Environment caseonly{{"s3:prefix", "ALICE"}, {"aws:username", "alice"}};
  auto t = [](const char* op) {
    return policy_text(op, "s3:prefix", q("${aws:username}"));
  };

  EXPECT_TRUE(eval(t("StringEquals"), match));
  EXPECT_FALSE(eval(t("StringEquals"), differ));
  EXPECT_FALSE(eval(t("StringEquals"), caseonly));

  EXPECT_FALSE(eval(t("StringNotEquals"), match));
  EXPECT_TRUE(eval(t("StringNotEquals"), differ));
  EXPECT_TRUE(eval(t("StringNotEquals"), caseonly));

  EXPECT_TRUE(eval(t("StringEqualsIgnoreCase"), match));
  EXPECT_TRUE(eval(t("StringEqualsIgnoreCase"), caseonly));
  EXPECT_FALSE(eval(t("StringEqualsIgnoreCase"), differ));

  EXPECT_FALSE(eval(t("StringNotEqualsIgnoreCase"), match));
  EXPECT_FALSE(eval(t("StringNotEqualsIgnoreCase"), caseonly));
  EXPECT_TRUE(eval(t("StringNotEqualsIgnoreCase"), differ));

  EXPECT_TRUE(eval(t("StringLike"), match));
  EXPECT_FALSE(eval(t("StringLike"), differ));
  EXPECT_FALSE(eval(t("StringLike"), caseonly));

  EXPECT_FALSE(eval(t("StringNotLike"), match));
  EXPECT_TRUE(eval(t("StringNotLike"), differ));
  EXPECT_TRUE(eval(t("StringNotLike"), caseonly));
}

TEST_F(RuntimeConditionTest, SetOperatorsWithVariable)
{
  // Request key multi-valued, variable single-valued.
  auto all = policy_text("ForAllValues:StringEquals", "aws:TagKeys",
                         q("${aws:PrincipalTag/only}"));
  auto any = policy_text("ForAnyValue:StringEquals", "aws:TagKeys",
                         q("${aws:PrincipalTag/only}"));
  const Environment one{{"aws:TagKeys", "a"}, {"aws:PrincipalTag/only", "a"}};
  const Environment two{{"aws:TagKeys", "a"}, {"aws:TagKeys", "b"},
                        {"aws:PrincipalTag/only", "a"}};
  const Environment none{{"aws:TagKeys", "b"}, {"aws:PrincipalTag/only", "a"}};

  EXPECT_TRUE(eval(all, one));
  EXPECT_FALSE(eval(all, two));
  EXPECT_FALSE(eval(all, none));

  EXPECT_TRUE(eval(any, one));
  EXPECT_TRUE(eval(any, two));
  EXPECT_FALSE(eval(any, none));

  auto like = policy_text("ForAllValues:StringLike", "aws:TagKeys",
                          q("${aws:PrincipalTag/only}-*"));
  EXPECT_TRUE(eval(like, {{"aws:TagKeys", "a-1"}, {"aws:TagKeys", "a-2"},
                          {"aws:PrincipalTag/only", "a"}}));
  EXPECT_FALSE(eval(like, {{"aws:TagKeys", "a-1"}, {"aws:TagKeys", "b-2"},
                           {"aws:PrincipalTag/only", "a"}}));
}

TEST_F(RuntimeConditionTest, ArnOperatorsWithVariable)
{
  const std::string key = "aws:SourceArn";
  auto whole = policy_text("ArnEquals", key, q("${aws:PrincipalArn}"));
  EXPECT_TRUE(eval(whole, {{key, "arn:aws:iam::123:user/alice"},
                           {"aws:PrincipalArn", "arn:aws:iam::123:user/alice"}}));
  EXPECT_FALSE(eval(whole, {{key, "arn:aws:iam::123:user/bob"},
                            {"aws:PrincipalArn", "arn:aws:iam::123:user/alice"}}));

  auto embedded = policy_text("ArnLike", key, q("arn:aws:sns:*:*:${aws:username}-*"));
  EXPECT_TRUE(eval(embedded, {{key, "arn:aws:sns:us-east-1:123:alice-topic"},
                              {"aws:username", "alice"}}));
  EXPECT_FALSE(eval(embedded, {{key, "arn:aws:sns:us-east-1:123:bob-topic"},
                               {"aws:username", "alice"}}));

  auto notlike = policy_text("ArnNotLike", key, q("arn:aws:sns:*:*:${aws:username}-*"));
  EXPECT_FALSE(eval(notlike, {{key, "arn:aws:sns:us-east-1:123:alice-topic"},
                              {"aws:username", "alice"}}));
  EXPECT_TRUE(eval(notlike, {{key, "arn:aws:sns:us-east-1:123:bob-topic"},
                             {"aws:username", "alice"}}));
}

// --- Lists mixing literals and variables ----------------------------

TEST_F(RuntimeConditionTest, MixedLiteralAndVariableLists)
{
  const Environment viavar{{"s3:prefix", "alice"}, {"aws:username", "alice"}};
  const Environment vialit{{"s3:prefix", "shared"}, {"aws:username", "alice"}};
  const Environment neither{{"s3:prefix", "bob"}, {"aws:username", "alice"}};
  for (const char* vals : {R"(["${aws:username}", "shared"])",
                           R"(["shared", "${aws:username}"])",
                           R"(["other", "${aws:username}", "shared"])"}) {
    auto text = policy_text("StringEquals", "s3:prefix", vals);
    EXPECT_TRUE(eval(text, viavar)) << vals;
    EXPECT_TRUE(eval(text, vialit)) << vals;
    EXPECT_FALSE(eval(text, neither)) << vals;
  }

  // Two variables in one list.
  auto text = policy_text("StringEquals", "s3:prefix",
                          R"(["${aws:username}", "${aws:PrincipalTag/team}"])");
  EXPECT_TRUE(eval(text, {{"s3:prefix", "web"}, {"aws:username", "alice"},
                          {"aws:PrincipalTag/team", "web"}}));
  EXPECT_TRUE(eval(text, {{"s3:prefix", "alice"}, {"aws:username", "alice"},
                          {"aws:PrincipalTag/team", "web"}}));
  EXPECT_FALSE(eval(text, {{"s3:prefix", "bob"}, {"aws:username", "alice"},
                           {"aws:PrincipalTag/team", "web"}}));
}

TEST_F(RuntimeConditionTest, ShortLiteralAfterVariableDoesNotThrow)
{
  // Regression: brace-stripping applied to the LAST value regardless
  // of whether it was the variable. On a literal shorter than three
  // characters that threw std::out_of_range from Condition::eval, an
  // uncaught exception in the request path.
  const Environment env{{"s3:prefix", "alice"}, {"aws:username", "alice"}};
  for (const char* vals : {R"(["${aws:username}", "a"])",
                           R"(["${aws:username}", "ab"])",
                           R"(["${aws:username}", ""])"}) {
    auto text = policy_text("StringEquals", "s3:prefix", vals);
    bool r = false;
    EXPECT_NO_THROW(r = eval(text, env)) << vals;
    EXPECT_TRUE(r) << vals;
  }
}

// --- Variable names [V3] --------------------------------------------

TEST_F(RuntimeConditionTest, VariableNameIsCaseInsensitive)
{
  const Environment env{{"s3:prefix", "alice"}, {"aws:username", "alice"}};
  for (const char* v : {"${aws:username}", "${AWS:Username}",
                        "${aws:USERNAME}", "${Aws:UserName}"}) {
    EXPECT_TRUE(eval(policy_text("StringEquals", "s3:prefix", q(v)), env)) << v;
  }
}

// --- Variables with no value [V4] -----------------------------------

TEST_F(RuntimeConditionTest, UnresolvedVariablePositiveFalseNegativeTrue)
{
  // Condition key present, variable's key absent.
  const Environment env{{"s3:prefix", "alice"}};
  auto t = [](const char* op) {
    return policy_text(op, "s3:prefix", q("${aws:username}"));
  };
  EXPECT_FALSE(eval(t("StringEquals"), env));
  EXPECT_TRUE(eval(t("StringNotEquals"), env));
  EXPECT_FALSE(eval(t("StringEqualsIgnoreCase"), env));
  EXPECT_TRUE(eval(t("StringNotEqualsIgnoreCase"), env));
  EXPECT_FALSE(eval(t("StringLike"), env));
  EXPECT_TRUE(eval(t("StringNotLike"), env));
  EXPECT_FALSE(eval(t("ForAnyValue:StringEquals"), env));
  EXPECT_FALSE(eval(t("ForAllValues:StringEquals"), env));

  const Environment arn{{"aws:SourceArn", "arn:aws:s3:::bucket"}};
  EXPECT_FALSE(eval(policy_text("ArnEquals", "aws:SourceArn",
                                q("${aws:PrincipalArn}")), arn));
  EXPECT_TRUE(eval(policy_text("ArnNotEquals", "aws:SourceArn",
                               q("${aws:PrincipalArn}")), arn));
  EXPECT_FALSE(eval(policy_text("ArnLike", "aws:SourceArn",
                                q("${aws:PrincipalArn}")), arn));
  EXPECT_TRUE(eval(policy_text("ArnNotLike", "aws:SourceArn",
                               q("${aws:PrincipalArn}")), arn));

  // An unresolved variable embedded in a longer value is just as null.
  EXPECT_FALSE(eval(policy_text("StringLike", "s3:prefix",
                                q("${aws:username}/*")),
                    {{"s3:prefix", "/x"}}));
  EXPECT_FALSE(eval(policy_text("StringLike", "s3:prefix",
                                q("${aws:username}/*")),
                    {{"s3:prefix", "alice/x"}}));
}

TEST_F(RuntimeConditionTest, UnresolvedVariableDoesNotHideResolvedOnes)
{
  // One variable unresolved, another resolved, in the same list.
  auto text = policy_text("StringEquals", "s3:prefix",
                          R"(["${aws:username}", "${aws:PrincipalTag/team}"])");
  EXPECT_TRUE(eval(text, {{"s3:prefix", "web"}, {"aws:PrincipalTag/team", "web"}}));
  EXPECT_FALSE(eval(text, {{"s3:prefix", "alice"}, {"aws:PrincipalTag/team", "web"}}));
}

// --- Default values [V7] --------------------------------------------

TEST_F(RuntimeConditionTest, DefaultValue)
{
  auto text = policy_text("StringEquals", "s3:prefix",
                          q("${aws:PrincipalTag/team, 'company-wide'}"));
  EXPECT_TRUE(eval(text, {{"s3:prefix", "yellow"},
                          {"aws:PrincipalTag/team", "yellow"}}));
  EXPECT_FALSE(eval(text, {{"s3:prefix", "company-wide"},
                           {"aws:PrincipalTag/team", "yellow"}}));
  EXPECT_TRUE(eval(text, {{"s3:prefix", "company-wide"}}));
  EXPECT_FALSE(eval(text, {{"s3:prefix", "yellow"}}));

  auto embedded = policy_text("StringLike", "s3:prefix",
                              q("${aws:PrincipalTag/team, 'company-wide'}/*"));
  EXPECT_TRUE(eval(embedded, {{"s3:prefix", "company-wide/readme"}}));
  EXPECT_TRUE(eval(embedded, {{"s3:prefix", "yellow/readme"},
                              {"aws:PrincipalTag/team", "yellow"}}));
  EXPECT_FALSE(eval(embedded, {{"s3:prefix", "company-wide/readme"},
                               {"aws:PrincipalTag/team", "yellow"}}));

  // An empty default is a value, not an absence.
  auto empty = policy_text("StringEquals", "s3:prefix",
                           q("${aws:PrincipalTag/team, ''}"));
  EXPECT_TRUE(eval(empty, {{"s3:prefix", ""}}));
  EXPECT_FALSE(eval(policy_text("StringNotEquals", "s3:prefix",
                                q("${aws:PrincipalTag/team, ''}")),
                    {{"s3:prefix", ""}}));
}

// --- Special characters [V6] ----------------------------------------

TEST_F(RuntimeConditionTest, SpecialCharacterVariables)
{
  auto star = policy_text("StringLike", "s3:prefix", q("a${*}b"));
  EXPECT_TRUE(eval(star, {{"s3:prefix", "a*b"}}));
  EXPECT_FALSE(eval(star, {{"s3:prefix", "aXb"}}));
  EXPECT_FALSE(eval(star, {{"s3:prefix", "ab"}}));

  auto qmark = policy_text("StringLike", "s3:prefix", q("a${?}b"));
  EXPECT_TRUE(eval(qmark, {{"s3:prefix", "a?b"}}));
  EXPECT_FALSE(eval(qmark, {{"s3:prefix", "aXb"}}));

  auto dollar = policy_text("StringLike", "s3:prefix", q("${$}"));
  EXPECT_TRUE(eval(dollar, {{"s3:prefix", "$"}}));
  EXPECT_FALSE(eval(dollar, {{"s3:prefix", "x"}}));

  // ${$} escapes what would otherwise be a variable.
  auto escaped = policy_text("StringEquals", "s3:prefix",
                             q("${$}{aws:username}"));
  EXPECT_TRUE(eval(escaped, {{"s3:prefix", "${aws:username}"},
                             {"aws:username", "alice"}}));
  EXPECT_FALSE(eval(escaped, {{"s3:prefix", "alice"},
                              {"aws:username", "alice"}}));

  // Special variables mixed with a real one.
  auto mixed = policy_text("StringLike", "s3:prefix",
                           q("${aws:username}/${*}/*"));
  EXPECT_TRUE(eval(mixed, {{"s3:prefix", "alice/*/x"},
                           {"aws:username", "alice"}}));
  EXPECT_FALSE(eval(mixed, {{"s3:prefix", "alice/y/x"},
                            {"aws:username", "alice"}}));
}

// --- Our decisions where AWS is silent or compatibility is at stake --

TEST_F(RuntimeConditionDecision, MalformedVariableRejectedAtParse)
{
  // AWS does not document what an unterminated "${" or an empty name
  // means. We fail closed at parse time rather than guess.
  for (const char* bad : {"${", "${aws:username", "a${b", "${}", "x${}y"}) {
    EXPECT_FALSE(parses(policy_text("StringEquals", "s3:prefix", q(bad))))
      << bad;
  }
}

TEST_F(RuntimeConditionDecision, VariablesRejectedInNonStringOperators)
{
  // [V2] forbids variables outside the string and ARN operators. We
  // reject the policy rather than evaluate it; before this, a variable
  // under NumericNotEquals (unconvertible, so skipped) was an
  // unconditional match, and under Bool or Null it read as "true".
  struct { const char* op; const char* key; } cases[] = {
    {"NumericEquals", "s3:max-keys"},
    {"NumericNotEquals", "s3:max-keys"},
    {"NumericLessThan", "s3:max-keys"},
    {"NumericLessThanEquals", "s3:max-keys"},
    {"NumericGreaterThan", "s3:max-keys"},
    {"NumericGreaterThanEquals", "s3:max-keys"},
    {"DateEquals", "aws:CurrentTime"},
    {"DateNotEquals", "aws:CurrentTime"},
    {"DateLessThan", "aws:CurrentTime"},
    {"DateLessThanEquals", "aws:CurrentTime"},
    {"DateGreaterThan", "aws:CurrentTime"},
    {"DateGreaterThanEquals", "aws:CurrentTime"},
    {"Bool", "aws:SecureTransport"},
    {"BinaryEquals", "s3:prefix"},
    {"IpAddress", "aws:SourceIp"},
    {"NotIpAddress", "aws:SourceIp"},
    {"Null", "s3:prefix"},
  };
  for (const auto& c : cases) {
    auto text = policy_text(c.op, c.key, q("${aws:username}"));
    EXPECT_FALSE(parses(text)) << text;
    // Should it parse anyway, it must never be a match.
    if (auto p = parse(text)) {
      EXPECT_FALSE(p->statements[0].conditions[0].eval(
                     {{c.key, "10"}, {"aws:username", "alice"}}, nullptr))
        << text;
    }
  }
}

TEST_F(RuntimeConditionDecision, VariablesAreLiteralBefore2012)
{
  // [V5]. Compatibility note: policies written without a Version line
  // (our parser defaults to 2008-10-17) that relied on interpolation
  // stop interpolating.
  for (const char* version : {"2008-10-17", ""}) {
    auto text = policy_text("StringEquals", "s3:prefix",
                            q("${aws:username}"), version);
    ASSERT_TRUE(parses(text)) << text;
    EXPECT_TRUE(eval(text, {{"s3:prefix", "${aws:username}"},
                            {"aws:username", "alice"}})) << text;
    EXPECT_FALSE(eval(text, {{"s3:prefix", "alice"},
                             {"aws:username", "alice"}})) << text;
  }
}

TEST_F(RuntimeConditionDecision, MultiValuedVariableIsUnresolved)
{
  // [V3] forbids multivalued keys as variables but does not say what
  // happens if you do it. We treat the variable as having no value;
  // before this, every value was substituted, turning a scalar
  // comparison into a set comparison.
  const Environment env{{"s3:prefix", "a"},
                        {"aws:TagKeys", "a"}, {"aws:TagKeys", "b"}};
  EXPECT_FALSE(eval(policy_text("StringEquals", "s3:prefix",
                                q("${aws:TagKeys}")), env));
  EXPECT_TRUE(eval(policy_text("StringNotEquals", "s3:prefix",
                               q("${aws:TagKeys}")), env));
  EXPECT_FALSE(eval(policy_text("StringLike", "s3:prefix",
                                q("${aws:TagKeys}*")), env));
}
