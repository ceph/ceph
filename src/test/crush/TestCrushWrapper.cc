// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library Public License for more details.
 *
 */

#include <iostream>
#include <gtest/gtest.h>

#include "include/stringify.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "global/global_context.h"

#include "crush/CrushWrapper.h"

TEST(CrushWrapper, get_immediate_parent) {
  CrushWrapper *c = new CrushWrapper;
  
  const int ROOT_TYPE = 1;
  c->set_type_name(ROOT_TYPE, "root");
  const int OSD_TYPE = 0;
  c->set_type_name(OSD_TYPE, "osd");

  int rootno;
  c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
		ROOT_TYPE, 0, NULL, NULL, &rootno);
  c->set_item_name(rootno, "default");

  int item = 0;

  pair <string,string> loc;
  int ret;
  loc = c->get_immediate_parent(item, &ret);
  EXPECT_EQ(-ENOENT, ret);

  {
    map<string,string> loc;
    loc["root"] = "default";

    EXPECT_EQ(0, c->insert_item(g_ceph_context, item, 1.0,
				"osd.0", loc));
  }

  loc = c->get_immediate_parent(item, &ret);
  EXPECT_EQ(0, ret);
  EXPECT_EQ("root", loc.first);
  EXPECT_EQ("default", loc.second);
}

TEST(CrushWrapper, move_bucket) {
  CrushWrapper *c = new CrushWrapper;
  
  const int ROOT_TYPE = 2;
  c->set_type_name(ROOT_TYPE, "root");
  const int HOST_TYPE = 1;
  c->set_type_name(HOST_TYPE, "host");
  const int OSD_TYPE = 0;
  c->set_type_name(OSD_TYPE, "osd");

  int root0;
  EXPECT_EQ(0, c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
			     ROOT_TYPE, 0, NULL, NULL, &root0));
  EXPECT_EQ(0, c->set_item_name(root0, "root0"));

  int item = 0;
  {
    map<string,string> loc;
    loc["root"] = "root0";
    loc["host"] = "host0";

    EXPECT_EQ(0, c->insert_item(g_ceph_context, item, 1.0,
				"osd.0", loc));
  }
  int host0 = c->get_item_id("host0");

  int root1;
  EXPECT_EQ(0, c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
			     ROOT_TYPE, 0, NULL, NULL, &root1));
  EXPECT_EQ(0, c->set_item_name(root1, "root1"));

  map<string,string> loc;
  loc["root"] = "root1";

  // 0 is not a valid bucket number, must be negative
  EXPECT_EQ(-EINVAL, c->move_bucket(g_ceph_context, 0, loc));
  // -100 is not an existing bucket
  EXPECT_EQ(-ENOENT, c->move_bucket(g_ceph_context, -100, loc));
  // move host0 from root0 to root1
  {
    pair <string,string> loc;
    int ret;
    loc = c->get_immediate_parent(host0, &ret);
    EXPECT_EQ(0, ret);
    EXPECT_EQ("root", loc.first);
    EXPECT_EQ("root0", loc.second);
  }
  EXPECT_EQ(0, c->move_bucket(g_ceph_context, host0, loc));
  {
    pair <string,string> loc;
    int ret;
    loc = c->get_immediate_parent(host0, &ret);
    EXPECT_EQ(0, ret);
    EXPECT_EQ("root", loc.first);
    EXPECT_EQ("root1", loc.second);
  }
}

TEST(CrushWrapper, check_item_loc) {
  CrushWrapper *c = new CrushWrapper;
  int item = 0;
  float expected_weight = 1.0;

  // fail if loc is empty
  {
    float weight;
    map<string,string> loc;
    EXPECT_FALSE(c->check_item_loc(g_ceph_context, item, loc, &weight));
  }

  const int ROOT_TYPE = 2;
  c->set_type_name(ROOT_TYPE, "root");
  const int HOST_TYPE = 1;
  c->set_type_name(HOST_TYPE, "host");
  const int OSD_TYPE = 0;
  c->set_type_name(OSD_TYPE, "osd");

  int rootno;
  c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
		ROOT_TYPE, 0, NULL, NULL, &rootno);
  c->set_item_name(rootno, "default");

  // fail because the item is not found at the specified location
  {
    float weight;
    map<string,string> loc;
    loc["root"] = "default";
    EXPECT_FALSE(c->check_item_loc(g_ceph_context, item, loc, &weight));
  }
  // fail because the bucket name does not match an existing bucket
  {
    float weight;
    map<string,string> loc;
    loc["root"] = "default";
    const string HOST("host0");
    loc["host"] = HOST;
    EXPECT_FALSE(c->check_item_loc(g_ceph_context, item, loc, &weight));
  }
  const string OSD("osd.0");
  {
    map<string,string> loc;
    loc["root"] = "default";
    EXPECT_EQ(0, c->insert_item(g_ceph_context, item, expected_weight,
				OSD, loc));
  }
  // fail because osd.0 is not a bucket and must not be in loc, in
  // addition to being of the wrong type
  {
    float weight;
    map<string,string> loc;
    loc["root"] = "osd.0";
    EXPECT_FALSE(c->check_item_loc(g_ceph_context, item, loc, &weight));
  }
  // succeed and retrieves the expected weight
  {
    float weight;
    map<string,string> loc;
    loc["root"] = "default";
    EXPECT_TRUE(c->check_item_loc(g_ceph_context, item, loc, &weight));
    EXPECT_EQ(expected_weight, weight);
  }
}

TEST(CrushWrapper, update_item) {
  CrushWrapper *c = new CrushWrapper;

  const int ROOT_TYPE = 2;
  c->set_type_name(ROOT_TYPE, "root");
  const int HOST_TYPE = 1;
  c->set_type_name(HOST_TYPE, "host");
  const int OSD_TYPE = 0;
  c->set_type_name(OSD_TYPE, "osd");

  int rootno;
  c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
		ROOT_TYPE, 0, NULL, NULL, &rootno);
  c->set_item_name(rootno, "default");

  const string HOST0("host0");
  int host0;
  c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
		HOST_TYPE, 0, NULL, NULL, &host0);
  c->set_item_name(host0, HOST0);

  const string HOST1("host1");
  int host1;
  c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
		HOST_TYPE, 0, NULL, NULL, &host1);
  c->set_item_name(host1, HOST1);

  int item = 0;

  // fail if invalid names anywhere in loc
  {
    map<string,string> loc;
    loc["rack"] = "\001";
    EXPECT_EQ(-EINVAL, c->update_item(g_ceph_context, item, 1.0,
				      "osd." + stringify(item), loc));
  }
  // fail if invalid item name
  {
    map<string,string> loc;
    EXPECT_EQ(-EINVAL, c->update_item(g_ceph_context, item, 1.0,
				      "\005", loc));
  }
  const string OSD0("osd.0");
  const string OSD1("osd.1");
  float original_weight = 1.0;
  float modified_weight = 2.0;
  float weight;

  map<string,string> loc;
  loc["root"] = "default";
  loc["host"] = HOST0;
  EXPECT_GE(0.0, c->get_item_weightf(host0));
  EXPECT_EQ(0, c->insert_item(g_ceph_context, item, original_weight,
			      OSD0, loc));

  // updating nothing changes nothing
  EXPECT_EQ(OSD0, c->get_item_name(item));
  EXPECT_EQ(original_weight, c->get_item_weightf(item));
  EXPECT_TRUE(c->check_item_loc(g_ceph_context, item, loc, &weight));
  EXPECT_EQ(0, c->update_item(g_ceph_context, item, original_weight,
			      OSD0, loc));
  EXPECT_EQ(OSD0, c->get_item_name(item));
  EXPECT_EQ(original_weight, c->get_item_weightf(item));
  EXPECT_TRUE(c->check_item_loc(g_ceph_context, item, loc, &weight));

  // update the name and weight of the item but not the location
  EXPECT_EQ(OSD0, c->get_item_name(item));
  EXPECT_EQ(original_weight, c->get_item_weightf(item));
  EXPECT_TRUE(c->check_item_loc(g_ceph_context, item, loc, &weight));
  EXPECT_EQ(1, c->update_item(g_ceph_context, item, modified_weight,
			      OSD1, loc));
  EXPECT_EQ(OSD1, c->get_item_name(item));
  EXPECT_EQ(modified_weight, c->get_item_weightf(item));
  EXPECT_TRUE(c->check_item_loc(g_ceph_context, item, loc, &weight));
  c->set_item_name(item, OSD0);
  c->adjust_item_weightf(g_ceph_context, item, original_weight);

  // update the name and weight of the item and change its location
  map<string,string> other_loc;
  other_loc["root"] = "default";
  other_loc["host"] = HOST1;

  EXPECT_EQ(OSD0, c->get_item_name(item));
  EXPECT_EQ(original_weight, c->get_item_weightf(item));
  EXPECT_TRUE(c->check_item_loc(g_ceph_context, item, loc, &weight));
  EXPECT_FALSE(c->check_item_loc(g_ceph_context, item, other_loc, &weight));
  EXPECT_EQ(1, c->update_item(g_ceph_context, item, modified_weight,
			      OSD1, other_loc));
  EXPECT_EQ(OSD1, c->get_item_name(item));
  EXPECT_EQ(modified_weight, c->get_item_weightf(item));
  EXPECT_FALSE(c->check_item_loc(g_ceph_context, item, loc, &weight));
  EXPECT_TRUE(c->check_item_loc(g_ceph_context, item, other_loc, &weight));
}

TEST(CrushWrapper, insert_item) {
  CrushWrapper *c = new CrushWrapper;

  const int ROOT_TYPE = 2;
  c->set_type_name(ROOT_TYPE, "root");
  const int HOST_TYPE = 1;
  c->set_type_name(HOST_TYPE, "host");
  const int OSD_TYPE = 0;
  c->set_type_name(OSD_TYPE, "osd");

  int rootno;
  c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
		ROOT_TYPE, 0, NULL, NULL, &rootno);
  c->set_item_name(rootno, "default");

  int item = 0;

  // invalid names anywhere in loc trigger an error
  {
    map<string,string> loc;
    loc["host"] = "\001";
    EXPECT_EQ(-EINVAL, c->insert_item(g_ceph_context, item, 1.0,
				      "osd." + stringify(item), loc));
  }

  // insert an item in an existing bucket
  {
    map<string,string> loc;
    loc["root"] = "default";

    item++;
    EXPECT_EQ(0, c->insert_item(g_ceph_context, item, 1.0,
				"osd." + stringify(item), loc));
    int another_item = item + 1;
    EXPECT_EQ(-EEXIST, c->insert_item(g_ceph_context, another_item, 1.0,
				      "osd." + stringify(item), loc));
  }
  // implicit creation of a bucket 
  {
    string name = "NAME";
    map<string,string> loc;
    loc["root"] = "default";
    loc["host"] = name;

    item++;
    EXPECT_EQ(0, c->insert_item(g_ceph_context, item, 1.0,
				"osd." + stringify(item), loc));
  }
  // adding to an existing item name that is not associated with a bucket
  {
    string name = "ITEM_WITHOUT_BUCKET";
    map<string,string> loc;
    loc["root"] = "default";
    loc["host"] = name;
    item++;
    c->set_item_name(item, name);

    item++;
    EXPECT_EQ(-EINVAL, c->insert_item(g_ceph_context, item, 1.0,
				      "osd." + stringify(item), loc));
  }
  // 
  //   When there is:
  //
  //   default --> host0 --> item
  //
  //   Trying to insert the same item higher in the hirarchy will fail
  //   because it would create a loop.
  //
  //   default --> host0 --> item
  //           |
  //           +-> item 
  //
  {
    item++;
    {
      map<string,string> loc;
      loc["root"] = "default";
      loc["host"] = "host0";

      EXPECT_EQ(0, c->insert_item(g_ceph_context, item, 1.0,
				  "osd." + stringify(item), loc));
    }
    {
      map<string,string> loc;
      loc["root"] = "default";

      EXPECT_EQ(-EINVAL, c->insert_item(g_ceph_context, item, 1.0,
					"osd." + stringify(item), loc));
    }
  }
  // 
  //   When there is:
  //
  //   default --> host0
  //
  //   Trying to insert default under host0 must fail
  //   because it would create a loop.
  //
  //   default --> host0 --> default
  //
  {
    map<string,string> loc;
    loc["host"] = "host0";

    EXPECT_EQ(-ELOOP, c->insert_item(g_ceph_context, rootno, 1.0,
				     "default", loc));
  }
  // fail when mapping a bucket to the wrong type
  {
    // create an OSD bucket
    int osdno;
    c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
		  OSD_TYPE, 0, NULL, NULL, &osdno);
    c->set_item_name(osdno, "myosd");
    map<string,string> loc;
    loc["root"] = "default";
    // wrongfully pretend the osd is of type host
    loc["host"] = "myosd";

    item++;
    EXPECT_EQ(-EINVAL, c->insert_item(g_ceph_context, item, 1.0,
				      "osd." + stringify(item), loc));
  }
  // fail when no location 
  {
    map<string,string> loc;
    item++;
    EXPECT_EQ(-EINVAL, c->insert_item(g_ceph_context, item, 1.0,
				      "osd." + stringify(item), loc));
  }

  delete c;
}

TEST(CrushWrapper, item_bucket_names) {
  CrushWrapper *c = new CrushWrapper;
  int index = 123;
  string name = "NAME";
  EXPECT_EQ(-EINVAL, c->set_item_name(index, "\001"));
  EXPECT_EQ(0, c->set_item_name(index, name));
  EXPECT_TRUE(c->name_exists(name));
  EXPECT_TRUE(c->item_exists(index));
  EXPECT_EQ(index, c->get_item_id(name));
  EXPECT_EQ(name, c->get_item_name(index));
  delete c;
}

TEST(CrushWrapper, bucket_types) {
  CrushWrapper *c = new CrushWrapper;
  int index = 123;
  string name = "NAME";
  c->set_type_name(index, name);
  EXPECT_EQ(1, c->get_num_type_names());
  EXPECT_EQ(index, c->get_type_id(name));
  EXPECT_EQ(name, c->get_type_name(index));
  delete c;
}

TEST(CrushWrapper, is_valid_crush_name) {
  EXPECT_TRUE(CrushWrapper::is_valid_crush_name("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ012456789-_"));
  EXPECT_FALSE(CrushWrapper::is_valid_crush_name(""));
  EXPECT_FALSE(CrushWrapper::is_valid_crush_name("\001"));
}

TEST(CrushWrapper, is_valid_crush_loc) {
  map<string,string> loc;
  EXPECT_TRUE(CrushWrapper::is_valid_crush_loc(g_ceph_context, loc));
  loc["good"] = "better";
  EXPECT_TRUE(CrushWrapper::is_valid_crush_loc(g_ceph_context, loc));
  {
    map<string,string> loc;
    loc["\005"] = "default";
    EXPECT_FALSE(CrushWrapper::is_valid_crush_loc(g_ceph_context, loc));
  }
  {
    map<string,string> loc;
    loc["host"] = "\003";
    EXPECT_FALSE(CrushWrapper::is_valid_crush_loc(g_ceph_context, loc));
  }
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make unittest_crush_wrapper && 
 *    valgrind \
 *    --max-stackframe=20000000 --tool=memcheck \
 *    ./unittest_crush_wrapper --log-to-stderr=true --debug-crush=20 # --gtest_filter=CrushWrapper.insert_item"
 * End:
 */
