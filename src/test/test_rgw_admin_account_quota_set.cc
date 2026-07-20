// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "common/Finisher.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include <gtest/gtest.h>
#include "test_rgw_admin_helper.h"


using namespace std;
using admin_helper::g_test;

int default_quota_max = -1;
string account_id = "RGW00000000000000001";
string account_name = CEPH_UID;
string uid = CEPH_UID;
string display_name = "CEPH";

TEST(TestRGWAdmin, account_quota_put_accounts_no_access){
  JSONParser parser;
  RGWUserInfo uinfo;
  RGWAccountInfo ainfo;
  string request_params = "id=" + account_id + "&max-size=999&max-objects=999&enabled=true";

  ASSERT_EQ(0, admin_helper::account_create(account_id, account_name));
  ASSERT_EQ(0, admin_helper::account_info(account_id, ainfo));

  ASSERT_EQ(0, admin_helper::user_create(uid, display_name, true, account_id, false));
  ASSERT_EQ(0, admin_helper::user_info(uid, display_name, uinfo));

  // assert default values
  EXPECT_EQ(default_quota_max, ainfo.quota.max_size);
  EXPECT_EQ(default_quota_max, ainfo.quota.max_objects);
  EXPECT_FALSE(ainfo.quota.enabled);

  // assert a user with no access gets 403 unauthorized
  g_test->send_request(string("PUT"), "/admin/account?quota&quota-type=account&" + request_params);
  EXPECT_EQ(403U, g_test->get_resp_code());

  ASSERT_EQ(0, admin_helper::user_rm(uid, display_name));
  ASSERT_EQ(0, admin_helper::account_rm(account_id));
}

TEST(TestRGWAdmin, account_quota_put){
  JSONParser parser;
  RGWUserInfo uinfo;
  RGWAccountInfo ainfo;

  string request_params = "id=" + account_id + "&max-size=999&max-objects=999&enabled=true";

  ASSERT_EQ(0, admin_helper::account_create(account_id, account_name));
  ASSERT_EQ(0, admin_helper::account_info(account_id, ainfo));

  ASSERT_EQ(0, admin_helper::user_create(uid, display_name, true, account_id, true));
  ASSERT_EQ(0, admin_helper::user_info(uid, display_name, uinfo));

  // assert default values
  EXPECT_EQ(default_quota_max, ainfo.quota.max_size);
  EXPECT_EQ(default_quota_max, ainfo.quota.max_objects);
  EXPECT_FALSE(ainfo.quota.enabled);

  g_test->send_request(string("PUT"), "/admin/account?quota&quota-type=account&" + request_params);
  EXPECT_EQ(200U, g_test->get_resp_code());
  ASSERT_EQ(0, admin_helper::account_info(account_id, ainfo));

  // assert quotas are set correctly at the account level
  EXPECT_EQ(999U, ainfo.quota.max_size);
  EXPECT_EQ(999U, ainfo.quota.max_objects);
  EXPECT_TRUE(ainfo.quota.enabled);

  // assert bucket quota remains the default
  EXPECT_EQ(default_quota_max, ainfo.bucket_quota.max_size);
  EXPECT_EQ(default_quota_max, ainfo.bucket_quota.max_objects);
  EXPECT_FALSE(ainfo.bucket_quota.enabled);

  g_test->send_request(string("PUT"), "/admin/account?quota&quota-type=bucket&" + request_params);
  EXPECT_EQ(200U, g_test->get_resp_code());
  ASSERT_EQ(0, admin_helper::account_info(account_id, ainfo));

  // assert quotas are set correctly at the bucket level
  EXPECT_EQ(999U, ainfo.bucket_quota.max_size);
  EXPECT_EQ(999U, ainfo.bucket_quota.max_objects);
  EXPECT_TRUE(ainfo.bucket_quota.enabled);

  ASSERT_EQ(0, admin_helper::user_rm(uid, display_name));
  ASSERT_EQ(0, admin_helper::account_rm(account_id));
}

TEST(TestRGWAdmin, account_quota_put_partial){
  JSONParser parser;
  RGWUserInfo uinfo;
  RGWAccountInfo ainfo;

  ASSERT_EQ(0, admin_helper::account_create(account_id, account_name));
  ASSERT_EQ(0, admin_helper::account_info(account_id, ainfo));

  ASSERT_EQ(0, admin_helper::user_create(uid, display_name, true, account_id, true));
  ASSERT_EQ(0, admin_helper::user_info(uid, display_name, uinfo));

  // assert default values
  EXPECT_EQ(default_quota_max, ainfo.quota.max_size);
  EXPECT_EQ(default_quota_max, ainfo.quota.max_objects);
  EXPECT_FALSE(ainfo.quota.enabled);

  // assert not having anything changed for account quota (max-objects, etc) maintains the default values
  g_test->send_request(string("PUT"), "/admin/account?quota&quota-type=account&id=" + account_id);
  EXPECT_EQ(200U, g_test->get_resp_code());
  ASSERT_EQ(0, admin_helper::account_info(account_id, ainfo));

  EXPECT_EQ(default_quota_max, ainfo.quota.max_size);
  EXPECT_EQ(default_quota_max, ainfo.quota.max_objects);
  EXPECT_FALSE(ainfo.quota.enabled);

  // assert having one field changed leaves the others unchanged
  g_test->send_request(string("PUT"), "/admin/account?quota&quota-type=account&id=" + account_id + "&max-size=100");
  EXPECT_EQ(200U, g_test->get_resp_code());
  ASSERT_EQ(0, admin_helper::account_info(account_id, ainfo));

  EXPECT_EQ(100U, ainfo.quota.max_size);
  EXPECT_EQ(default_quota_max, ainfo.quota.max_objects);
  EXPECT_FALSE(ainfo.quota.enabled);

  // assert not having anything changed for bucket quota (max-objects, etc) maintains the default values
  g_test->send_request(string("PUT"), "/admin/account?quota&quota-type=bucket&id=" + account_id);
  EXPECT_EQ(200U, g_test->get_resp_code());
  ASSERT_EQ(0, admin_helper::account_info(account_id, ainfo));

  EXPECT_EQ(default_quota_max, ainfo.bucket_quota.max_size);
  EXPECT_EQ(default_quota_max, ainfo.bucket_quota.max_objects);
  EXPECT_FALSE(ainfo.bucket_quota.enabled);

  // assert having one field changed leaves the others unchanged
  g_test->send_request(string("PUT"), "/admin/account?quota&quota-type=account&id=" + account_id + "&enabled=true");
  EXPECT_EQ(200U, g_test->get_resp_code());
  ASSERT_EQ(0, admin_helper::account_info(account_id, ainfo));

  EXPECT_EQ(default_quota_max, ainfo.bucket_quota.max_size);
  EXPECT_EQ(default_quota_max, ainfo.bucket_quota.max_objects);
  EXPECT_TRUE(ainfo.quota.enabled);

  ASSERT_EQ(0, admin_helper::user_rm(uid, display_name));
  ASSERT_EQ(0, admin_helper::account_rm(account_id));
}

TEST(TestRGWAdmin, account_quota_put_invalid_args){
  JSONParser parser;
  RGWUserInfo uinfo;
  RGWAccountInfo ainfo;

  ASSERT_EQ(0, admin_helper::account_create(account_id, account_name));
  ASSERT_EQ(0, admin_helper::account_info(account_id, ainfo));

  ASSERT_EQ(0, admin_helper::user_create(uid, display_name, true, account_id, true));
  ASSERT_EQ(0, admin_helper::user_info(uid, display_name, uinfo));

  // assert default values
  EXPECT_EQ(default_quota_max, ainfo.quota.max_size);
  EXPECT_EQ(default_quota_max, ainfo.quota.max_objects);
  EXPECT_FALSE(ainfo.quota.enabled);

  // assert trying to set quota with no quota-type returns a 400 error
  g_test->send_request(string("PUT"), "/admin/account?quota&id=" + account_id);
  EXPECT_EQ(400U, g_test->get_resp_code());

  // assert trying to set quota with invalid quota type returns a 400 error
  g_test->send_request(string("PUT"), "/admin/account?quota&quota-type=invalid&id=" + account_id);
  EXPECT_EQ(400U, g_test->get_resp_code());

  // assert trying to set quota with no account id returns a 400 error
  g_test->send_request(string("PUT"), "/admin/account?quota&quota-type=account");
  EXPECT_EQ(400U, g_test->get_resp_code());

  ASSERT_EQ(0, admin_helper::user_rm(uid, display_name));
  ASSERT_EQ(0, admin_helper::account_rm(account_id));
}

int main(int argc, char *argv[]){
  auto args = argv_to_vec(argc, argv);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_test = new admin_helper::test_helper();
  Finisher *finisher = new Finisher(g_ceph_context);
#ifdef GTEST
  ::testing::InitGoogleTest(&argc, argv);
#endif
  finisher->start();

  if(g_test->extract_input(argc, argv) < 0){
    admin_helper::print_usage(argv[0]);
    return -1;
  }
#ifdef GTEST
  int r = RUN_ALL_TESTS();
  if (r == 0) {
    cout << "There are no failures in the test case\n";
  } else {
    cout << "There are some failures\n";
  }
#endif
  finisher->stop();
  return 0;
}