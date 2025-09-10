// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

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

string uid = CEPH_UID;
string display_name = "CEPH";
string meta_caps = "metadata";

TEST(TestRGWAdmin, meta_list){
  JSONParser parser;
  bool found = false;
  const char *perm = "*";

  ASSERT_EQ(0, admin_helper::user_create(uid, display_name));
  ASSERT_EQ(0, admin_helper::caps_add(meta_caps, uid, perm));

  /*Check the sections*/
  g_test->send_request(string("GET"), string("/admin/metadata/"));
  EXPECT_EQ(200U, g_test->get_resp_code());

  ASSERT_TRUE(admin_helper::parse_json_resp(parser) == 0);
  EXPECT_TRUE(parser.is_array());
  
  vector<string> l;
  l = parser.get_array_elements();
  for(vector<string>::iterator it = l.begin();
      it != l.end(); ++it) {
    if((*it).compare("\"user\"") == 0) {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found);

  /*Check with a wrong section*/
  g_test->send_request(string("GET"), string("/admin/metadata/users"));
  EXPECT_EQ(404U, g_test->get_resp_code());

  /*Check the list of keys*/
  g_test->send_request(string("GET"), string("/admin/metadata/user"));
  EXPECT_EQ(200U, g_test->get_resp_code());

  ASSERT_TRUE(admin_helper::parse_json_resp(parser) == 0);
  EXPECT_TRUE(parser.is_array());
  
  l = parser.get_array_elements();
  // depending on the setup, the result may be different.
  // on an empty cluster, the size will be 1. with vstart, the size will be 9.
  ASSERT_TRUE(l.size() == 1U || l.size() == 9U);
  for(vector<string>::iterator it = l.begin();
      it != l.end(); ++it) {
    if((*it).compare(string("\"") + uid + string("\"")) == 0) {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found);

  /*Check with second user*/
  string uid2 = "ceph1", display_name2 = "CEPH1";
  ASSERT_EQ(0, admin_helper::user_create(uid2, display_name2, false));
  /*Check the list of keys*/
  g_test->send_request(string("GET"), string("/admin/metadata/user"));
  EXPECT_EQ(200U, g_test->get_resp_code());

  ASSERT_TRUE(admin_helper::parse_json_resp(parser) == 0);
  EXPECT_TRUE(parser.is_array());
  
  l = parser.get_array_elements();
  // depending on the setup, the result may be different.
  // on an empty cluster, the size will be 2. with vstart, the size will be 10.
  ASSERT_TRUE(l.size() == 2U || l.size() == 10U);
  bool found2 = false;
  for(vector<string>::iterator it = l.begin();
      it != l.end(); ++it) {
    if((*it).compare(string("\"") + uid + string("\"")) == 0) {
      found = true;
    }
    if((*it).compare(string("\"") + uid2 + string("\"")) == 0) {
      found2 = true;
    }
  }
  EXPECT_TRUE(found && found2);
  ASSERT_EQ(0, admin_helper::user_rm(uid2, display_name2));

  /*Remove the metadata caps*/
  int rv = admin_helper::caps_rm(meta_caps, uid, perm);
  EXPECT_EQ(0, rv);
  
  if(rv == 0) {
    g_test->send_request(string("GET"), string("/admin/metadata/"));
    EXPECT_EQ(403U, g_test->get_resp_code());

    g_test->send_request(string("GET"), string("/admin/metadata/user"));
    EXPECT_EQ(403U, g_test->get_resp_code());
  }
  ASSERT_EQ(0, admin_helper::user_rm(uid, display_name));
}

TEST(TestRGWAdmin, meta_get){
  JSONParser parser;
  const char *perm = "*";
  RGWUserInfo info;

  ASSERT_EQ(0, admin_helper::user_create(uid, display_name));
  ASSERT_EQ(0, admin_helper::caps_add(meta_caps, uid, perm));

  ASSERT_EQ(0, admin_helper::user_info(uid, display_name, info));
 
  // user with key = "test" exists in vstart
  g_test->send_request(string("GET"), string("/admin/metadata/user?key=test"));
  ASSERT_TRUE(g_test->get_resp_code() == 200U || g_test->get_resp_code() == 404U);

  g_test->send_request(string("GET"), string("/admin/metadata/user?key=doesnotexist"));
  ASSERT_EQ(404U, g_test->get_resp_code());

  g_test->send_request(string("GET"), (string("/admin/metadata/user?key=") + uid));
  EXPECT_EQ(200U, g_test->get_resp_code());

  ASSERT_TRUE(admin_helper::parse_json_resp(parser) == 0);
  RGWObjVersionTracker objv_tracker;
  string metadata_key;

  obj_version *objv = &objv_tracker.read_version;
     
  JSONDecoder::decode_json("key", metadata_key, &parser);
  JSONDecoder::decode_json("ver", *objv, &parser);
  JSONObj *jo = parser.find_obj("data");
  ASSERT_TRUE(jo);
  string exp_meta_key = "user:";
  exp_meta_key.append(uid);
  EXPECT_TRUE(metadata_key.compare(exp_meta_key) == 0);

  RGWUserInfo obt_info;
  decode_json_obj(obt_info, jo);

  EXPECT_TRUE(admin_helper::compare_user_info(info, obt_info, meta_caps) == 0);

  /*Make a modification and check if its reflected*/
  ASSERT_EQ(0, admin_helper::caps_rm(meta_caps, uid, perm));
  perm = "read";
  ASSERT_EQ(0, admin_helper::caps_add(meta_caps, uid, perm));
  
  JSONParser parser1;
  g_test->send_request(string("GET"), (string("/admin/metadata/user?key=") + uid));
  EXPECT_EQ(200U, g_test->get_resp_code());

  ASSERT_TRUE(admin_helper::parse_json_resp(parser1) == 0);
 
  RGWObjVersionTracker objv_tracker1;
  obj_version *objv1 = &objv_tracker1.read_version;

  JSONDecoder::decode_json("key", metadata_key, &parser1);
  JSONDecoder::decode_json("ver", *objv1, &parser1);
  jo = parser1.find_obj("data");
  ASSERT_TRUE(jo);

  decode_json_obj(obt_info, jo);
  uint32_t p1, p2;
  p1 = RGW_CAP_ALL;
  p2 = RGW_CAP_READ;
  EXPECT_TRUE (info.caps.check_cap(meta_caps, p1) == 0);
  EXPECT_TRUE (obt_info.caps.check_cap(meta_caps, p2) == 0);
  p2 = RGW_CAP_WRITE;
  EXPECT_TRUE (obt_info.caps.check_cap(meta_caps, p2) != 0);

  /*Version and tag information*/
  EXPECT_TRUE(objv1->ver > objv->ver);
  EXPECT_EQ(objv1->tag, objv->tag);
  
  int rv = admin_helper::caps_rm(meta_caps, uid, perm);
  EXPECT_EQ(0, rv);
  
  if(rv == 0) {
    g_test->send_request(string("GET"), (string("/admin/metadata/user?key=") + uid));
    EXPECT_EQ(403U, g_test->get_resp_code());
  }
  ASSERT_EQ(0, admin_helper::user_rm(uid, display_name));
}

TEST(TestRGWAdmin, meta_put){
  JSONParser parser;
  const char *perm = "*";
  RGWUserInfo info;

  ASSERT_EQ(0, admin_helper::user_create(uid, display_name));
  ASSERT_EQ(0, admin_helper::caps_add(meta_caps, uid, perm));
  
  g_test->send_request(string("GET"), (string("/admin/metadata/user?key=") + uid));
  EXPECT_EQ(200U, g_test->get_resp_code());

  ASSERT_TRUE(admin_helper::parse_json_resp(parser) == 0);
  RGWObjVersionTracker objv_tracker;
  string metadata_key;

  obj_version *objv = &objv_tracker.read_version;
     
  JSONDecoder::decode_json("key", metadata_key, &parser);
  JSONDecoder::decode_json("ver", *objv, &parser);
  JSONObj *jo = parser.find_obj("data");
  ASSERT_TRUE(jo);
  string exp_meta_key = "user:";
  exp_meta_key.append(uid);
  EXPECT_TRUE(metadata_key.compare(exp_meta_key) == 0);

  RGWUserInfo obt_info;
  decode_json_obj(obt_info, jo);

  /*Change the cap and PUT */
  RGWUserCaps caps;
  string new_cap;
  Formatter *f = new JSONFormatter();

  new_cap = meta_caps + string("=write");
  caps.add_from_string(new_cap);
  obt_info.caps = caps;
  f->open_object_section("metadata_info");
  ::encode_json("key", metadata_key, f);
  ::encode_json("ver", *objv, f);
  ::encode_json("data", obt_info, f);
  f->close_section();
  std::stringstream ss;
  f->flush(ss);

  g_test->send_request(string("PUT"), (string("/admin/metadata/user?key=") + uid), 
                       admin_helper::meta_read_json,
                       (void *)&ss, ss.str().length());
  EXPECT_EQ(204U, g_test->get_resp_code());

  ASSERT_EQ(0, admin_helper::user_info(uid, display_name, obt_info));
  uint32_t cp;
  cp = RGW_CAP_WRITE;
  EXPECT_TRUE (obt_info.caps.check_cap(meta_caps, cp) == 0);
  cp = RGW_CAP_READ;
  EXPECT_TRUE (obt_info.caps.check_cap(meta_caps, cp) != 0);
  
  int rv = admin_helper::caps_rm(meta_caps, uid, "write");
  EXPECT_EQ(0, rv);
  if(rv == 0) {
    g_test->send_request(string("PUT"), (string("/admin/metadata/user?key=") + uid));
    EXPECT_EQ(403U, g_test->get_resp_code());
  }
  ASSERT_EQ(0, admin_helper::user_rm(uid, display_name));
}

TEST(TestRGWAdmin, meta_delete){
  JSONParser parser;
  const char *perm = "*";
  RGWUserInfo info;

  ASSERT_EQ(0, admin_helper::user_create(uid, display_name));
  ASSERT_EQ(0, admin_helper::caps_add(meta_caps, uid, perm));

  g_test->send_request(string("DELETE"), (string("/admin/metadata/user?key=") + uid));
  EXPECT_EQ(200U, g_test->get_resp_code());

  ASSERT_TRUE(admin_helper::user_info(uid, display_name, info) != 0);

  ASSERT_EQ(0, admin_helper::user_create(uid, display_name));
  perm = "read";
  ASSERT_EQ(0, admin_helper::caps_add(meta_caps, uid, perm));
  
  g_test->send_request(string("DELETE"), (string("/admin/metadata/user?key=") + uid));
  EXPECT_EQ(403U, g_test->get_resp_code());
  ASSERT_EQ(0, admin_helper::user_rm(uid, display_name));
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
