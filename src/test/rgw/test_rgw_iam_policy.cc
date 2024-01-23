// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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

#include <string>

#include <boost/intrusive_ptr.hpp>
#include <boost/optional.hpp>

#include <gtest/gtest.h>

#include "include/stringify.h"
#include "common/code_environment.h"
#include "common/ceph_context.h"
#include "global/global_init.h"
#include "rgw/rgw_auth.h"
#include "rgw/rgw_iam_policy.h"
#include "rgw/rgw_op.h"
#include "rgw_sal_rados.h"


using std::string;
using std::vector;

using boost::container::flat_set;
using boost::intrusive_ptr;
using boost::make_optional;
using boost::none;

using rgw::auth::Identity;
using rgw::auth::Principal;

using rgw::ARN;
using rgw::IAM::Effect;
using rgw::IAM::Environment;
using rgw::Partition;
using rgw::IAM::Policy;
using rgw::IAM::s3All;
using rgw::IAM::s3Count;
using rgw::IAM::s3GetAccelerateConfiguration;
using rgw::IAM::s3GetBucketAcl;
using rgw::IAM::s3GetBucketCORS;
using rgw::IAM::s3GetBucketLocation;
using rgw::IAM::s3GetBucketLogging;
using rgw::IAM::s3GetBucketNotification;
using rgw::IAM::s3GetBucketPolicy;
using rgw::IAM::s3GetBucketPolicyStatus;
using rgw::IAM::s3GetBucketPublicAccessBlock;
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
using rgw::IAM::s3GetPublicAccessBlock;
using rgw::IAM::s3GetReplicationConfiguration;
using rgw::IAM::s3ListAllMyBuckets;
using rgw::IAM::s3ListBucket;
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
using rgw::Service;
using rgw::IAM::TokenID;
using rgw::IAM::Version;
using rgw::IAM::Action_t;
using rgw::IAM::NotAction_t;
using rgw::IAM::iamCreateRole;
using rgw::IAM::iamDeleteRole;
using rgw::IAM::iamAll;
using rgw::IAM::stsAll;
using rgw::IAM::allCount;

class FakeIdentity : public Identity {
  const Principal id;
public:

  explicit FakeIdentity(Principal&& id) : id(std::move(id)) {}
  uint32_t get_perms_from_aclspec(const DoutPrefixProvider* dpp, const aclspec_t& aclspec) const override {
    ceph_abort();
    return 0;
  };

  bool is_admin_of(const rgw_user& uid) const override {
    ceph_abort();
    return false;
  }

  bool is_owner_of(const rgw_user& uid) const override {
    ceph_abort();
    return false;
  }

  virtual uint32_t get_perm_mask() const override {
    ceph_abort();
    return 0;
  }

  string get_acct_name() const override {
    abort();
    return 0;
  }

  string get_subuser() const override {
    abort();
    return 0;
  }

  void to_str(std::ostream& out) const override {
    out << id;
  }

  bool is_identity(const flat_set<Principal>& ids) const override {
    if (id.is_wildcard() && (!ids.empty())) {
      return true;
    }
    return ids.find(id) != ids.end() || ids.find(Principal::wildcard()) != ids.end();
  }

  uint32_t get_identity_type() const override {
    return TYPE_RGW;
  }
};

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
    cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);
  }
};

TEST_F(PolicyTest, Parse1) {
  boost::optional<Policy> p;

  ASSERT_NO_THROW(p = Policy(cct.get(), arbitrary_tenant,
			     bufferlist::static_from_string(example1)));
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
  auto p  = Policy(cct.get(), arbitrary_tenant,
		   bufferlist::static_from_string(example1));
  Environment e;

  ARN arn1(Partition::aws, Service::s3,
		       "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(p.eval(e, none, s3ListBucket, arn1),
	    Effect::Allow);

  ARN arn2(Partition::aws, Service::s3,
		       "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(p.eval(e, none, s3PutBucketAcl, arn2),
	    Effect::Pass);

  ARN arn3(Partition::aws, Service::s3,
		       "", arbitrary_tenant, "erroneous_bucket");
  EXPECT_EQ(p.eval(e, none, s3ListBucket, arn3),
	    Effect::Pass);

}

TEST_F(PolicyTest, Parse2) {
  boost::optional<Policy> p;

  ASSERT_NO_THROW(p = Policy(cct.get(), arbitrary_tenant,
			     bufferlist::static_from_string(example2)));
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
	    Principal::tenant("ACCOUNT-ID-WITHOUT-HYPHENS"));
  EXPECT_TRUE(p->statements[0].noprinc.empty());
  EXPECT_EQ(p->statements[0].effect, Effect::Allow);
  Action_t act;
  for (auto i = 0ULL; i < s3Count; i++)
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
  auto p  = Policy(cct.get(), arbitrary_tenant,
		   bufferlist::static_from_string(example2));
  Environment e;

  auto trueacct = FakeIdentity(
    Principal::tenant("ACCOUNT-ID-WITHOUT-HYPHENS"));

  auto notacct = FakeIdentity(
    Principal::tenant("some-other-account"));
  for (auto i = 0ULL; i < s3Count; ++i) {
    ARN arn1(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "mybucket");
    EXPECT_EQ(p.eval(e, trueacct, i, arn1),
	      Effect::Allow);
    ARN arn2(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "mybucket/myobject");
    EXPECT_EQ(p.eval(e, trueacct, i, arn2),
	      Effect::Allow);
    ARN arn3(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "mybucket");
    EXPECT_EQ(p.eval(e, notacct, i, arn3),
	      Effect::Pass);
    ARN arn4(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "mybucket/myobject");
    EXPECT_EQ(p.eval(e, notacct, i, arn4),
	      Effect::Pass);
    ARN arn5(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "notyourbucket");
    EXPECT_EQ(p.eval(e, trueacct, i, arn5),
	      Effect::Pass);
    ARN arn6(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "notyourbucket/notyourobject");
    EXPECT_EQ(p.eval(e, trueacct, i, arn6),
	      Effect::Pass);

  }
}

TEST_F(PolicyTest, Parse3) {
  boost::optional<Policy> p;

  ASSERT_NO_THROW(p = Policy(cct.get(), arbitrary_tenant,
			     bufferlist::static_from_string(example3)));
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
  act2[s3GetAccelerateConfiguration] = 1;
  act2[s3GetBucketAcl] = 1;
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
  auto p  = Policy(cct.get(), arbitrary_tenant,
		   bufferlist::static_from_string(example3));
  Environment em;
  Environment tr = { { "aws:MultiFactorAuthPresent", "true" } };
  Environment fa = { { "aws:MultiFactorAuthPresent", "false" } };

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
  s3allow[s3GetObjectTorrent] = 1;
  s3allow[s3GetObjectVersionTorrent] = 1;
  s3allow[s3GetAccelerateConfiguration] = 1;
  s3allow[s3GetBucketAcl] = 1;
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

  ARN arn1(Partition::aws, Service::s3,
		       "", arbitrary_tenant, "mybucket");
  EXPECT_EQ(p.eval(em, none, s3PutBucketPolicy, arn1),
	    Effect::Allow);

  ARN arn2(Partition::aws, Service::s3,
		       "", arbitrary_tenant, "mybucket");
  EXPECT_EQ(p.eval(em, none, s3PutBucketPolicy, arn2),
	    Effect::Allow);


  for (auto op = 0ULL; op < s3Count; ++op) {
    if ((op == s3ListAllMyBuckets) || (op == s3PutBucketPolicy)) {
      continue;
    }
    ARN arn3(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "confidential-data");
    EXPECT_EQ(p.eval(em, none, op, arn3),
	      Effect::Pass);
    ARN arn4(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "confidential-data");
    EXPECT_EQ(p.eval(tr, none, op, arn4),
	      s3allow[op] ? Effect::Allow : Effect::Pass);
    ARN arn5(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "confidential-data");
    EXPECT_EQ(p.eval(fa, none, op, arn5),
	      Effect::Pass);
    ARN arn6(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "confidential-data/moo");
    EXPECT_EQ(p.eval(em, none, op, arn6),
	      Effect::Pass);
    ARN arn7(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "confidential-data/moo");
    EXPECT_EQ(p.eval(tr, none, op, arn7),
	      s3allow[op] ? Effect::Allow : Effect::Pass);
    ARN arn8(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "confidential-data/moo");
    EXPECT_EQ(p.eval(fa, none, op, arn8),
	      Effect::Pass);
    ARN arn9(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "really-confidential-data");
    EXPECT_EQ(p.eval(em, none, op, arn9),
	      Effect::Pass);
    ARN arn10(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "really-confidential-data");
    EXPECT_EQ(p.eval(tr, none, op, arn10),
	      Effect::Pass);
    ARN arn11(Partition::aws, Service::s3,
			 "", arbitrary_tenant, "really-confidential-data");
    EXPECT_EQ(p.eval(fa, none, op, arn11),
	      Effect::Pass);
    ARN arn12(Partition::aws, Service::s3,
			 "", arbitrary_tenant,
			 "really-confidential-data/moo");
    EXPECT_EQ(p.eval(em, none, op, arn12), Effect::Pass);
    ARN arn13(Partition::aws, Service::s3,
			 "", arbitrary_tenant,
			 "really-confidential-data/moo");
    EXPECT_EQ(p.eval(tr, none, op, arn13), Effect::Pass);
    ARN arn14(Partition::aws, Service::s3,
			 "", arbitrary_tenant,
			 "really-confidential-data/moo");
    EXPECT_EQ(p.eval(fa, none, op, arn14), Effect::Pass);

  }
}

TEST_F(PolicyTest, Parse4) {
  boost::optional<Policy> p;

  ASSERT_NO_THROW(p = Policy(cct.get(), arbitrary_tenant,
			     bufferlist::static_from_string(example4)));
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
  auto p  = Policy(cct.get(), arbitrary_tenant,
		   bufferlist::static_from_string(example4));
  Environment e;

  ARN arn1(Partition::aws, Service::iam,
		       "", arbitrary_tenant, "role/example_role");
  EXPECT_EQ(p.eval(e, none, iamCreateRole, arn1),
	    Effect::Allow);

  ARN arn2(Partition::aws, Service::iam,
		       "", arbitrary_tenant, "role/example_role");
  EXPECT_EQ(p.eval(e, none, iamDeleteRole, arn2),
	    Effect::Pass);
}

TEST_F(PolicyTest, Parse5) {
  boost::optional<Policy> p;

  ASSERT_NO_THROW(p = Policy(cct.get(), arbitrary_tenant,
			     bufferlist::static_from_string(example5)));
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
  for (auto i = s3All+1; i <= iamAll; i++)
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
  auto p  = Policy(cct.get(), arbitrary_tenant,
		   bufferlist::static_from_string(example5));
  Environment e;

  ARN arn1(Partition::aws, Service::iam,
		       "", arbitrary_tenant, "role/example_role");
  EXPECT_EQ(p.eval(e, none, iamCreateRole, arn1),
	    Effect::Allow);

  ARN arn2(Partition::aws, Service::iam,
		       "", arbitrary_tenant, "role/example_role");
  EXPECT_EQ(p.eval(e, none, s3ListBucket, arn2),
	    Effect::Pass);

  ARN arn3(Partition::aws, Service::iam,
		       "", "", "role/example_role");
  EXPECT_EQ(p.eval(e, none, iamCreateRole, arn3),
	    Effect::Pass);
}

TEST_F(PolicyTest, Parse6) {
  boost::optional<Policy> p;

  ASSERT_NO_THROW(p = Policy(cct.get(), arbitrary_tenant,
			     bufferlist::static_from_string(example6)));
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
  for (auto i = 0U; i <= stsAll; i++)
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
  auto p  = Policy(cct.get(), arbitrary_tenant,
		   bufferlist::static_from_string(example6));
  Environment e;

  ARN arn1(Partition::aws, Service::iam,
		       "", arbitrary_tenant, "user/A");
  EXPECT_EQ(p.eval(e, none, iamCreateRole, arn1),
	    Effect::Allow);

  ARN arn2(Partition::aws, Service::iam,
		       "", arbitrary_tenant, "user/A");
  EXPECT_EQ(p.eval(e, none, s3ListBucket, arn2),
	    Effect::Allow);
}

TEST_F(PolicyTest, Parse7) {
  boost::optional<Policy> p;

  ASSERT_NO_THROW(p = Policy(cct.get(), arbitrary_tenant,
			     bufferlist::static_from_string(example7)));
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
  EXPECT_EQ(p->statements[0].princ.begin()->get_tenant(), "");
  EXPECT_EQ(p->statements[0].princ.begin()->get_id(), "A:subA");
  EXPECT_TRUE(p->statements[0].notresource.empty());
  EXPECT_TRUE(p->statements[0].conditions.empty());
}

TEST_F(PolicyTest, Eval7) {
  auto p  = Policy(cct.get(), arbitrary_tenant,
		   bufferlist::static_from_string(example7));
  Environment e;

  auto subacct = FakeIdentity(
    Principal::user(std::move(""), "A:subA"));
  auto parentacct = FakeIdentity(
    Principal::user(std::move(""), "A"));
  auto sub2acct = FakeIdentity(
    Principal::user(std::move(""), "A:sub2A"));

  ARN arn1(Partition::aws, Service::s3,
		       "", arbitrary_tenant, "mybucket/*");
  EXPECT_EQ(p.eval(e, subacct, s3ListBucket, arn1),
	    Effect::Allow);
  
  ARN arn2(Partition::aws, Service::s3,
		       "", arbitrary_tenant, "mybucket/*");
  EXPECT_EQ(p.eval(e, parentacct, s3ListBucket, arn2),
	    Effect::Pass);

  ARN arn3(Partition::aws, Service::s3,
		       "", arbitrary_tenant, "mybucket/*");
  EXPECT_EQ(p.eval(e, sub2acct, s3ListBucket, arn3),
	    Effect::Pass);
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
    cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);
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

TEST_F(IPPolicyTest, IPEnvironment) {
  // Unfortunately RGWCivetWeb is too tightly tied to civetweb to test RGWCivetWeb::init_env.
  RGWEnv rgw_env;
  rgw::sal::RGWRadosStore store;
  std::unique_ptr<rgw::sal::RGWUser> user = store.get_user(rgw_user());
  rgw_env.set("REMOTE_ADDR", "192.168.1.1");
  rgw_env.set("HTTP_HOST", "1.2.3.4");
  req_state rgw_req_state(cct.get(), &rgw_env, 0);
  rgw_req_state.set_user(user);
  rgw_build_iam_environment(&store, &rgw_req_state);
  auto ip = rgw_req_state.env.find("aws:SourceIp");
  ASSERT_NE(ip, rgw_req_state.env.end());
  EXPECT_EQ(ip->second, "192.168.1.1");

  ASSERT_EQ(cct.get()->_conf.set_val("rgw_remote_addr_param", "SOME_VAR"), 0);
  EXPECT_EQ(cct.get()->_conf->rgw_remote_addr_param, "SOME_VAR");
  rgw_req_state.env.clear();
  rgw_build_iam_environment(&store, &rgw_req_state);
  ip = rgw_req_state.env.find("aws:SourceIp");
  EXPECT_EQ(ip, rgw_req_state.env.end());

  rgw_env.set("SOME_VAR", "192.168.1.2");
  rgw_req_state.env.clear();
  rgw_build_iam_environment(&store, &rgw_req_state);
  ip = rgw_req_state.env.find("aws:SourceIp");
  ASSERT_NE(ip, rgw_req_state.env.end());
  EXPECT_EQ(ip->second, "192.168.1.2");

  ASSERT_EQ(cct.get()->_conf.set_val("rgw_remote_addr_param", "HTTP_X_FORWARDED_FOR"), 0);
  rgw_env.set("HTTP_X_FORWARDED_FOR", "192.168.1.3");
  rgw_req_state.env.clear();
  rgw_build_iam_environment(&store, &rgw_req_state);
  ip = rgw_req_state.env.find("aws:SourceIp");
  ASSERT_NE(ip, rgw_req_state.env.end());
  EXPECT_EQ(ip->second, "192.168.1.3");

  rgw_env.set("HTTP_X_FORWARDED_FOR", "192.168.1.4, 4.3.2.1, 2001:db8:85a3:8d3:1319:8a2e:370:7348");
  rgw_req_state.env.clear();
  rgw_build_iam_environment(&store, &rgw_req_state);
  ip = rgw_req_state.env.find("aws:SourceIp");
  ASSERT_NE(ip, rgw_req_state.env.end());
  EXPECT_EQ(ip->second, "192.168.1.4");
}

TEST_F(IPPolicyTest, ParseIPAddress) {
  boost::optional<Policy> p;

  ASSERT_NO_THROW(p = Policy(cct.get(), arbitrary_tenant,
			     bufferlist::static_from_string(ip_address_full_example)));
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
  auto allowp  = Policy(cct.get(), arbitrary_tenant,
			bufferlist::static_from_string(ip_address_allow_example));
  auto denyp  = Policy(cct.get(), arbitrary_tenant,
		       bufferlist::static_from_string(ip_address_deny_example));
  auto fullp  = Policy(cct.get(), arbitrary_tenant,
		   bufferlist::static_from_string(ip_address_full_example));
  Environment e;
  Environment allowedIP, blocklistedIP, allowedIPv6, blocklistedIPv6;
  allowedIP.emplace("aws:SourceIp","192.168.1.2");
  allowedIPv6.emplace("aws:SourceIp", "::1");
  blocklistedIP.emplace("aws:SourceIp", "192.168.1.1");
  blocklistedIPv6.emplace("aws:SourceIp", "2001:0db8:85a3:0000:0000:8a2e:0370:7334");

  auto trueacct = FakeIdentity(
    Principal::tenant("ACCOUNT-ID-WITHOUT-HYPHENS"));
  // Without an IP address in the environment then evaluation will always pass
  ARN arn1(Partition::aws, Service::s3,
			    "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(allowp.eval(e, trueacct, s3ListBucket, arn1),
	    Effect::Pass);
  ARN arn2(Partition::aws, Service::s3,
      "", arbitrary_tenant, "example_bucket/myobject");
  EXPECT_EQ(fullp.eval(e, trueacct, s3ListBucket, arn2),
	    Effect::Pass);

  ARN arn3(Partition::aws, Service::s3,
			    "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(allowp.eval(allowedIP, trueacct, s3ListBucket, arn3),
	    Effect::Allow);
  ARN arn4(Partition::aws, Service::s3,
			    "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(allowp.eval(blocklistedIPv6, trueacct, s3ListBucket, arn4),
	    Effect::Pass);

  ARN arn5(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(denyp.eval(allowedIP, trueacct, s3ListBucket, arn5),
	    Effect::Deny);
  ARN arn6(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket/myobject");
  EXPECT_EQ(denyp.eval(allowedIP, trueacct, s3ListBucket, arn6),
	    Effect::Deny);

  ARN arn7(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(denyp.eval(blocklistedIP, trueacct, s3ListBucket, arn7),
	    Effect::Pass);
  ARN arn8(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket/myobject");
  EXPECT_EQ(denyp.eval(blocklistedIP, trueacct, s3ListBucket, arn8),
	    Effect::Pass);

  ARN arn9(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(denyp.eval(blocklistedIPv6, trueacct, s3ListBucket, arn9),
	    Effect::Pass);
  ARN arn10(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket/myobject");
  EXPECT_EQ(denyp.eval(blocklistedIPv6, trueacct, s3ListBucket, arn10),
	    Effect::Pass);
  ARN arn11(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(denyp.eval(allowedIPv6, trueacct, s3ListBucket, arn11),
	    Effect::Deny);
  ARN arn12(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket/myobject");
  EXPECT_EQ(denyp.eval(allowedIPv6, trueacct, s3ListBucket, arn12),
	    Effect::Deny);

  ARN arn13(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(fullp.eval(allowedIP, trueacct, s3ListBucket, arn13),
	    Effect::Allow);
  ARN arn14(Partition::aws, Service::s3,
      "", arbitrary_tenant, "example_bucket/myobject");
  EXPECT_EQ(fullp.eval(allowedIP, trueacct, s3ListBucket, arn14),
	    Effect::Allow);

  ARN arn15(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(fullp.eval(blocklistedIP, trueacct, s3ListBucket, arn15),
	    Effect::Pass);
  ARN arn16(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket/myobject");
  EXPECT_EQ(fullp.eval(blocklistedIP, trueacct, s3ListBucket, arn16),
	    Effect::Pass);

  ARN arn17(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(fullp.eval(allowedIPv6, trueacct, s3ListBucket, arn17),
	    Effect::Allow);
  ARN arn18(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket/myobject");
  EXPECT_EQ(fullp.eval(allowedIPv6, trueacct, s3ListBucket, arn18),
	    Effect::Allow);

  ARN arn19(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket");
  EXPECT_EQ(fullp.eval(blocklistedIPv6, trueacct, s3ListBucket, arn19),
	    Effect::Pass);
  ARN arn20(Partition::aws, Service::s3,
			   "", arbitrary_tenant, "example_bucket/myobject");
  EXPECT_EQ(fullp.eval(blocklistedIPv6, trueacct, s3ListBucket, arn20),
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
  // note: parsing of * is not greedy, so * does not match 'bc' here
  EXPECT_FALSE(match_wildcards("a*c", "abcc"));
  EXPECT_FALSE(match_wildcards("a*c", "abcc", MATCH_CASE_INSENSITIVE));
}

TEST(MatchPolicy, Action)
{
  constexpr auto flag = MATCH_POLICY_ACTION;
  EXPECT_TRUE(match_policy("a:b:c", "a:b:c", flag));
  EXPECT_TRUE(match_policy("a:b:c", "A:B:C", flag)); // case insensitive
  EXPECT_TRUE(match_policy("a:*:e", "a:bcd:e", flag));
  EXPECT_FALSE(match_policy("a:*", "a:b:c", flag)); // cannot span segments
}

TEST(MatchPolicy, Resource)
{
  constexpr auto flag = MATCH_POLICY_RESOURCE;
  EXPECT_TRUE(match_policy("a:b:c", "a:b:c", flag));
  EXPECT_FALSE(match_policy("a:b:c", "A:B:C", flag)); // case sensitive
  EXPECT_TRUE(match_policy("a:*:e", "a:bcd:e", flag));
  EXPECT_TRUE(match_policy("a:*", "a:b:c", flag)); // can span segments
}

TEST(MatchPolicy, ARN)
{
  constexpr auto flag = MATCH_POLICY_ARN;
  EXPECT_TRUE(match_policy("a:b:c", "a:b:c", flag));
  EXPECT_TRUE(match_policy("a:b:c", "A:B:C", flag)); // case insensitive
  EXPECT_TRUE(match_policy("a:*:e", "a:bcd:e", flag));
  EXPECT_FALSE(match_policy("a:*", "a:b:c", flag)); // cannot span segments
}

TEST(MatchPolicy, String)
{
  constexpr auto flag = MATCH_POLICY_STRING;
  EXPECT_TRUE(match_policy("a:b:c", "a:b:c", flag));
  EXPECT_FALSE(match_policy("a:b:c", "A:B:C", flag)); // case sensitive
  EXPECT_TRUE(match_policy("a:*:e", "a:bcd:e", flag));
  EXPECT_TRUE(match_policy("a:*", "a:b:c", flag)); // can span segments
}

Action_t set_range_bits(std::uint64_t start, std::uint64_t end)
{
  Action_t result;
  for (uint64_t i = start; i < end; i++) {
    result.set(i);
  }
  return result;
}

using rgw::IAM::s3AllValue;
using rgw::IAM::stsAllValue;
using rgw::IAM::allValue;
using rgw::IAM::iamAllValue;
TEST(set_cont_bits, iamconsts)
{
  EXPECT_EQ(s3AllValue, set_range_bits(0, s3All));
  EXPECT_EQ(iamAllValue, set_range_bits(s3All+1, iamAll));
  EXPECT_EQ(stsAllValue, set_range_bits(iamAll+1, stsAll));
  EXPECT_EQ(allValue , set_range_bits(0, allCount));
}
