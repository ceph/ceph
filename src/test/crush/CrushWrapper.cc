// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
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
#include "include/Context.h"
#include "osd/osd_types.h"

#include "crush/CrushWrapper.h"

TEST(CrushWrapper, get_immediate_parent) {
  std::unique_ptr<CrushWrapper> c(new CrushWrapper);

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
  std::unique_ptr<CrushWrapper> c(new CrushWrapper);

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

  {
    map<string,string> loc;
    loc["root"] = "root0";
    loc["host"] = "host0";

    int item = 0;
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

TEST(CrushWrapper, swap_bucket) {
  std::unique_ptr<CrushWrapper> c(new CrushWrapper);

  const int ROOT_TYPE = 2;
  c->set_type_name(ROOT_TYPE, "root");
  const int HOST_TYPE = 1;
  c->set_type_name(HOST_TYPE, "host");
  const int OSD_TYPE = 0;
  c->set_type_name(OSD_TYPE, "osd");

  int root;
  EXPECT_EQ(0, c->add_bucket(0, CRUSH_BUCKET_STRAW2, CRUSH_HASH_RJENKINS1,
			     ROOT_TYPE, 0, NULL, NULL, &root));
  EXPECT_EQ(0, c->set_item_name(root, "root"));

  int a, b;
  EXPECT_EQ(0, c->add_bucket(0, CRUSH_BUCKET_STRAW2, CRUSH_HASH_RJENKINS1,
			     HOST_TYPE, 0, NULL, NULL, &a));
  EXPECT_EQ(0, c->set_item_name(a, "a"));
  EXPECT_EQ(0, c->add_bucket(0, CRUSH_BUCKET_STRAW2, CRUSH_HASH_RJENKINS1,
			     HOST_TYPE, 0, NULL, NULL, &b));
  EXPECT_EQ(0, c->set_item_name(b, "b"));

  {
    map<string,string> loc;
    loc["root"] = "root";
    EXPECT_EQ(0, c->move_bucket(g_ceph_context, a, loc));
  }
  {
    map<string,string> loc;
    loc["root"] = "root";
    loc["host"] = "a";
    EXPECT_EQ(0, c->insert_item(g_ceph_context, 0, 1.0, "osd.0", loc));
    EXPECT_EQ(0, c->insert_item(g_ceph_context, 1, 1.0, "osd.1", loc));
    EXPECT_EQ(0, c->insert_item(g_ceph_context, 2, 1.0, "osd.2", loc));
  }
  {
    map<string,string> loc;
    loc["host"] = "b";
    EXPECT_EQ(0, c->insert_item(g_ceph_context, 3, 1.0, "osd.3", loc));
  }
  ASSERT_EQ(0x30000, c->get_item_weight(a));
  ASSERT_EQ(string("a"), c->get_item_name(a));
  ASSERT_EQ(0x10000, c->get_item_weight(b));
  ASSERT_EQ(string("b"), c->get_item_name(b));
  ASSERT_EQ(a, c->get_bucket_item(root, 0));
  ASSERT_EQ(0, c->get_bucket_item(a, 0));
  ASSERT_EQ(1, c->get_bucket_item(a, 1));
  ASSERT_EQ(2, c->get_bucket_item(a, 2));
  ASSERT_EQ(3, c->get_bucket_item(b, 0));
	
  // check if it can swap parent with child
  ASSERT_EQ(-EINVAL, c->swap_bucket(g_ceph_context, root, a));

  c->swap_bucket(g_ceph_context, a, b);
  ASSERT_EQ(0x30000, c->get_item_weight(b));
  ASSERT_EQ(string("a"), c->get_item_name(b));
  ASSERT_EQ(0x10000, c->get_item_weight(a));
  ASSERT_EQ(string("b"), c->get_item_name(a));
  ASSERT_EQ(a, c->get_bucket_item(root, 0));
  ASSERT_EQ(0, c->get_bucket_item(b, 0));
  ASSERT_EQ(1, c->get_bucket_item(b, 1));
  ASSERT_EQ(2, c->get_bucket_item(b, 2));
  ASSERT_EQ(3, c->get_bucket_item(a, 0));
}

TEST(CrushWrapper, rename_bucket_or_item) {
  std::unique_ptr<CrushWrapper> c(new CrushWrapper);

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
  item++;
  {
    map<string,string> loc;
    loc["root"] = "root0";
    loc["host"] = "host1";

    EXPECT_EQ(0, c->insert_item(g_ceph_context, item, 1.0,
				"osd.1", loc));
  }

  stringstream ss;
  EXPECT_EQ(-EINVAL, c->can_rename_item("host0", "????", &ss));
  EXPECT_EQ(-EINVAL, c->rename_item("host0", "????", &ss));
  EXPECT_EQ(-EINVAL, c->can_rename_bucket("host0", "????", &ss));
  EXPECT_EQ(-EINVAL, c->rename_bucket("host0", "????", &ss));

  EXPECT_EQ(-EEXIST, c->can_rename_item("host0", "host1", &ss));
  EXPECT_EQ(-EEXIST, c->rename_item("host0", "host1", &ss));
  EXPECT_EQ(-EEXIST, c->can_rename_bucket("host0", "host1", &ss));
  EXPECT_EQ(-EEXIST, c->rename_bucket("host0", "host1", &ss));

  EXPECT_EQ(-EALREADY, c->can_rename_item("gone", "host1", &ss));
  EXPECT_EQ(-EALREADY, c->rename_item("gone", "host1", &ss));
  EXPECT_EQ(-EALREADY, c->can_rename_bucket("gone", "host1", &ss));
  EXPECT_EQ(-EALREADY, c->rename_bucket("gone", "host1", &ss));

  EXPECT_EQ(-ENOENT, c->can_rename_item("doesnotexist", "somethingelse", &ss));
  EXPECT_EQ(-ENOENT, c->rename_item("doesnotexist", "somethingelse", &ss));
  EXPECT_EQ(-ENOENT, c->can_rename_bucket("doesnotexist", "somethingelse", &ss));
  EXPECT_EQ(-ENOENT, c->rename_bucket("doesnotexist", "somethingelse", &ss));

  EXPECT_EQ(-ENOTDIR, c->can_rename_bucket("osd.1", "somethingelse", &ss));
  EXPECT_EQ(-ENOTDIR, c->rename_bucket("osd.1", "somethingelse", &ss));

  int host0id = c->get_item_id("host0");
  EXPECT_EQ(0, c->rename_bucket("host0", "host0renamed", &ss));
  EXPECT_EQ(host0id, c->get_item_id("host0renamed"));

  int osd0id = c->get_item_id("osd0");
  EXPECT_EQ(0, c->rename_item("osd.0", "osd0renamed", &ss));
  EXPECT_EQ(osd0id, c->get_item_id("osd0renamed"));
}

TEST(CrushWrapper, check_item_loc) {
  std::unique_ptr<CrushWrapper> c(new CrushWrapper);
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
  std::unique_ptr<CrushWrapper> c(new CrushWrapper);

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

TEST(CrushWrapper, adjust_item_weight) {
  std::unique_ptr<CrushWrapper> c(new CrushWrapper);

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

  const string FAKE("fake");
  int hostfake;
  c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
		HOST_TYPE, 0, NULL, NULL, &hostfake);
  c->set_item_name(hostfake, FAKE);

  int item = 0;

  // construct crush map

  {
    map<string,string> loc;
    loc["host"] = "host0";
    float host_weight = 2.0;
    int bucket_id = 0;

    item = 0;
    EXPECT_EQ(0, c->insert_item(g_ceph_context, item, 1.0,
				"osd." + stringify(item), loc));
    item = 1;
    EXPECT_EQ(0, c->insert_item(g_ceph_context, item, 1.0,
				"osd." + stringify(item), loc));

    bucket_id = c->get_item_id("host0");
    EXPECT_EQ(true, c->bucket_exists(bucket_id));
    EXPECT_EQ(host_weight, c->get_bucket_weightf(bucket_id));

    map<string,string> bloc;
    bloc["root"] = "default";
    EXPECT_EQ(0, c->insert_item(g_ceph_context, host0, host_weight,
				HOST0, bloc));
  }

  {
    map<string,string> loc;
    loc["host"] = "fake";
    float host_weight = 2.0;
    int bucket_id = 0;

    item = 0;
    EXPECT_EQ(0, c->insert_item(g_ceph_context, item, 1.0,
				"osd." + stringify(item), loc));
    item = 1;
    EXPECT_EQ(0, c->insert_item(g_ceph_context, item, 1.0,
				"osd." + stringify(item), loc));

    bucket_id = c->get_item_id("fake");
    EXPECT_EQ(true, c->bucket_exists(bucket_id));
    EXPECT_EQ(host_weight, c->get_bucket_weightf(bucket_id));

    map<string,string> bloc;
    bloc["root"] = "default";
    EXPECT_EQ(0, c->insert_item(g_ceph_context, hostfake, host_weight,
				FAKE, bloc));
  }

  //
  //   When there is:
  //
  //   default --> host0 --> osd.0 1.0
  //           |         |
  //           |         +-> osd.1 1.0
  //           |
  //           +-> fake  --> osd.0 1.0
  //                     |
  //                     +-> osd.1 1.0
  //
  //   Trying to adjust osd.0 weight to 2.0 in all buckets
  //   Trying to adjust osd.1 weight to 2.0 in host=fake
  //
  //   So the crush map will be:
  //
  //   default --> host0 --> osd.0 2.0
  //           |         |
  //           |         +-> osd.1 1.0
  //           |
  //           +-> fake  --> osd.0 2.0
  //                     |
  //                     +-> osd.1 2.0
  //

  float original_weight = 1.0;
  float modified_weight = 2.0;
  map<string,string> loc_one, loc_two;
  loc_one["host"] = "host0";
  loc_two["host"] = "fake";

  item = 0;
  EXPECT_EQ(2, c->adjust_item_weightf(g_ceph_context, item, modified_weight));
  EXPECT_EQ(modified_weight, c->get_item_weightf_in_loc(item, loc_one));
  EXPECT_EQ(modified_weight, c->get_item_weightf_in_loc(item, loc_two));

  item = 1;
  EXPECT_EQ(1, c->adjust_item_weightf_in_loc(g_ceph_context, item, modified_weight, loc_two));
  EXPECT_EQ(original_weight, c->get_item_weightf_in_loc(item, loc_one));
  EXPECT_EQ(modified_weight, c->get_item_weightf_in_loc(item, loc_two));
}

TEST(CrushWrapper, adjust_subtree_weight) {
  std::unique_ptr<CrushWrapper> c(new CrushWrapper);

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

  const string FAKE("fake");
  int hostfake;
  c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
		HOST_TYPE, 0, NULL, NULL, &hostfake);
  c->set_item_name(hostfake, FAKE);

  int item = 0;

  // construct crush map

  {
    map<string,string> loc;
    loc["host"] = "host0";
    float host_weight = 2.0;
    int bucket_id = 0;

    item = 0;
    EXPECT_EQ(0, c->insert_item(g_ceph_context, item, 1.0,
				"osd." + stringify(item), loc));
    item = 1;
    EXPECT_EQ(0, c->insert_item(g_ceph_context, item, 1.0,
				"osd." + stringify(item), loc));

    bucket_id = c->get_item_id("host0");
    EXPECT_EQ(true, c->bucket_exists(bucket_id));
    EXPECT_EQ(host_weight, c->get_bucket_weightf(bucket_id));

    map<string,string> bloc;
    bloc["root"] = "default";
    EXPECT_EQ(0, c->insert_item(g_ceph_context, host0, host_weight,
				HOST0, bloc));
  }

  {
    map<string,string> loc;
    loc["host"] = "fake";
    float host_weight = 2.0;
    int bucket_id = 0;

    item = 0;
    EXPECT_EQ(0, c->insert_item(g_ceph_context, item, 1.0,
				"osd." + stringify(item), loc));
    item = 1;
    EXPECT_EQ(0, c->insert_item(g_ceph_context, item, 1.0,
				"osd." + stringify(item), loc));

    bucket_id = c->get_item_id("fake");
    EXPECT_EQ(true, c->bucket_exists(bucket_id));
    EXPECT_EQ(host_weight, c->get_bucket_weightf(bucket_id));

    map<string,string> bloc;
    bloc["root"] = "default";
    EXPECT_EQ(0, c->insert_item(g_ceph_context, hostfake, host_weight,
				FAKE, bloc));
  }

  //cout << "--------before---------" << std::endl;
  //c->dump_tree(&cout, NULL);
  ASSERT_EQ(c->get_bucket_weight(host0), 131072);
  ASSERT_EQ(c->get_bucket_weight(rootno), 262144);

  int r = c->adjust_subtree_weightf(g_ceph_context, host0, 2.0);
  ASSERT_EQ(r, 2); // 2 items changed

  //cout << "--------after---------" << std::endl;
  //c->dump_tree(&cout, NULL);

  ASSERT_EQ(c->get_bucket_weight(host0), 262144);
  ASSERT_EQ(c->get_item_weight(host0), 262144);
  ASSERT_EQ(c->get_bucket_weight(rootno), 262144 + 131072);
}

TEST(CrushWrapper, insert_item) {
  std::unique_ptr<CrushWrapper> c(new CrushWrapper);

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
    int r = c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
			  10, 0, NULL, NULL, &osdno);
    ASSERT_EQ(0, r);
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
}

TEST(CrushWrapper, remove_item) {
  std::unique_ptr<CrushWrapper> c(new CrushWrapper);

  const int ROOT_TYPE = 2;
  c->set_type_name(ROOT_TYPE, "root");
  const int HOST_TYPE = 1;
  c->set_type_name(HOST_TYPE, "host");
  const int OSD_TYPE = 0;
  c->set_type_name(OSD_TYPE, "osd");

  {
    int root;
    ASSERT_EQ(0, c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
			       ROOT_TYPE, 0, NULL, NULL, &root));
    c->set_item_name(root, "root0");
  }

  {
    int host;
    c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
		  HOST_TYPE, 0, NULL, NULL, &host);
    c->set_item_name(host, "host0");
  }

  const int num_osd = 12;
  {
    map<string, string> loc = {{"root", "root0"},
			       {"host", "host0"}};
    string name{"osd."};
    for (int item = 0; item < num_osd; item++) {
      ASSERT_EQ(0, c->insert_item(g_ceph_context, item, 1.0,
				  name + to_string(item), loc));
    }
  }
  const int item_to_remove = num_osd / 2;
  map<string, string> loc;
  loc.insert(c->get_immediate_parent(item_to_remove));
  ASSERT_EQ(0, c->remove_item(g_ceph_context, item_to_remove, true));
  float weight;
  EXPECT_FALSE(c->check_item_loc(g_ceph_context, item_to_remove, loc, &weight));
}

TEST(CrushWrapper, item_bucket_names) {
  std::unique_ptr<CrushWrapper> c(new CrushWrapper);
  int index = 123;
  string name = "NAME";
  EXPECT_EQ(-EINVAL, c->set_item_name(index, "\001"));
  EXPECT_EQ(0, c->set_item_name(index, name));
  EXPECT_TRUE(c->name_exists(name));
  EXPECT_TRUE(c->item_exists(index));
  EXPECT_EQ(index, c->get_item_id(name));
  EXPECT_EQ(name, c->get_item_name(index));
}

TEST(CrushWrapper, bucket_types) {
  std::unique_ptr<CrushWrapper> c(new CrushWrapper);
  int index = 123;
  string name = "NAME";
  c->set_type_name(index, name);
  EXPECT_EQ(1, c->get_num_type_names());
  EXPECT_EQ(index, c->get_type_id(name));
  EXPECT_EQ(name, c->get_type_name(index));
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

TEST(CrushWrapper, dump_rules) {
  std::unique_ptr<CrushWrapper> c(new CrushWrapper);

  const int ROOT_TYPE = 1;
  c->set_type_name(ROOT_TYPE, "root");
  const int OSD_TYPE = 0;
  c->set_type_name(OSD_TYPE, "osd");

  string failure_domain_type("osd");
  string root_name("default");
  int rootno;
  c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
		ROOT_TYPE, 0, NULL, NULL, &rootno);
  c->set_item_name(rootno, root_name);

  int item = 0;

  pair <string,string> loc;
  int ret;
  loc = c->get_immediate_parent(item, &ret);
  EXPECT_EQ(-ENOENT, ret);

  {
    map<string,string> loc;
    loc["root"] = root_name;

    EXPECT_EQ(0, c->insert_item(g_ceph_context, item, 1.0,
				"osd.0", loc));
  }

  // no rule by default
  {
    Formatter *f = Formatter::create("json-pretty");
    f->open_array_section("rules");
    c->dump_rules(f);
    f->close_section();
    stringstream ss;
    f->flush(ss);
    delete f;
    EXPECT_EQ("[]\n", ss.str());
  }

  string name("NAME");
  int rule = c->add_simple_rule(name, root_name, failure_domain_type, "",
				   "firstn", pg_pool_t::TYPE_ERASURE);
  EXPECT_EQ(0, rule);

  {
    Formatter *f = Formatter::create("xml");
    c->dump_rules(f);
    stringstream ss;
    f->flush(ss);
    delete f;
    EXPECT_EQ((unsigned)0, ss.str().find("<rule><rule_id>0</rule_id><rule_name>NAME</rule_name>"));
  }

  {
    Formatter *f = Formatter::create("xml");
    c->dump_rule(rule, f);
    stringstream ss;
    f->flush(ss);
    delete f;
    EXPECT_EQ((unsigned)0, ss.str().find("<rule><rule_id>0</rule_id><rule_name>NAME</rule_name>"));
    EXPECT_NE(string::npos,
	      ss.str().find("<item_name>default</item_name></step>"));
  }

  map<int,float> wm;
  c->get_rule_weight_osd_map(0, &wm);
  ASSERT_TRUE(wm.size() == 1);
  ASSERT_TRUE(wm[0] == 1.0);
}

TEST(CrushWrapper, distance) {
  CrushWrapper c;
  c.create();
  c.set_type_name(1, "host");
  c.set_type_name(2, "rack");
  c.set_type_name(3, "root");
  int bno;
  int r = c.add_bucket(0, CRUSH_BUCKET_STRAW,
		       CRUSH_HASH_DEFAULT, 3, 0, NULL,
		       NULL, &bno);
  ASSERT_EQ(0, r);
  ASSERT_EQ(-1, bno);
  c.set_item_name(bno, "default");

  c.set_max_devices(10);

  //JSONFormatter jf(true);

  map<string,string> loc;
  loc["host"] = "a1";
  loc["rack"] = "a";
  loc["root"] = "default";
  c.insert_item(g_ceph_context, 0, 1, "osd.0", loc);

  loc.clear();
  loc["host"] = "a2";
  loc["rack"] = "a";
  loc["root"] = "default";
  c.insert_item(g_ceph_context, 1, 1, "osd.1", loc);

  loc.clear();
  loc["host"] = "b1";
  loc["rack"] = "b";
  loc["root"] = "default";
  c.insert_item(g_ceph_context, 2, 1, "osd.2", loc);

  loc.clear();
  loc["host"] = "b2";
  loc["rack"] = "b";
  loc["root"] = "default";
  c.insert_item(g_ceph_context, 3, 1, "osd.3", loc);

  vector<pair<string,string> > ol;
  c.get_full_location_ordered(3, ol);
  ASSERT_EQ(3u, ol.size());
  ASSERT_EQ(make_pair(string("host"),string("b2")), ol[0]);
  ASSERT_EQ(make_pair(string("rack"),string("b")), ol[1]);
  ASSERT_EQ(make_pair(string("root"),string("default")), ol[2]);

  //c.dump(&jf);
  //jf.flush(cout);

  multimap<string,string> p;
  p.insert(make_pair("host","b2"));
  p.insert(make_pair("rack","b"));
  p.insert(make_pair("root","default"));
  ASSERT_EQ(3, c.get_common_ancestor_distance(g_ceph_context, 0, p));
  ASSERT_EQ(3, c.get_common_ancestor_distance(g_ceph_context, 1, p));
  ASSERT_EQ(2, c.get_common_ancestor_distance(g_ceph_context, 2, p));
  ASSERT_EQ(1, c.get_common_ancestor_distance(g_ceph_context, 3, p));
  ASSERT_EQ(-ENOENT, c.get_common_ancestor_distance(g_ceph_context, 123, p));

  // make sure a "multipath" location will reflect a minimal
  // distance for both paths
  p.insert(make_pair("host","b1"));
  ASSERT_EQ(1, c.get_common_ancestor_distance(g_ceph_context, 2, p));
  ASSERT_EQ(1, c.get_common_ancestor_distance(g_ceph_context, 3, p));
}

TEST(CrushWrapper, choose_args_compat) {
  CrushWrapper c;
  c.create();
  c.set_type_name(1, "host");
  c.set_type_name(2, "rack");
  c.set_type_name(3, "root");

  int weight = 12;

  map<string,string> loc;
  loc["host"] = "b1";
  loc["rack"] = "r11";
  loc["root"] = "default";
  int item = 1;
  c.insert_item(g_ceph_context, item, weight, "osd.1", loc);

  loc["host"] = "b2";
  loc["rack"] = "r12";
  loc["root"] = "default";
  item = 2;
  c.insert_item(g_ceph_context, item, weight, "osd.2", loc);

  ceph_assert(c.add_simple_rule("rule1", "r11", "host", "",
			   "firstn", pg_pool_t::TYPE_ERASURE) >= 0);

  int id = c.get_item_id("b1");

  __u32 weights = 666 * 0x10000;
  crush_weight_set weight_set;
  weight_set.size = 1;
  weight_set.weights = &weights;
  int maxbuckets = c.get_max_buckets();
  ceph_assert(maxbuckets > 0);
  crush_choose_arg choose_args[maxbuckets];
  memset(choose_args, '\0', sizeof(crush_choose_arg) * maxbuckets);
  choose_args[-1-id].ids_size = 0;
  choose_args[-1-id].weight_set_positions = 1;
  choose_args[-1-id].weight_set = &weight_set;
  crush_choose_arg_map arg_map;
  arg_map.size = c.get_max_buckets();
  arg_map.args = choose_args;

  uint64_t features = CEPH_FEATURE_CRUSH_TUNABLES5|CEPH_FEATURE_INCARNATION_2;
  int64_t caid = CrushWrapper::DEFAULT_CHOOSE_ARGS;

  // if the client is capable, encode choose_args
  {
    c.choose_args[caid] = arg_map;
    bufferlist bl;
    c.encode(bl, features|CEPH_FEATURE_CRUSH_CHOOSE_ARGS);
    auto i = bl.cbegin();
    CrushWrapper c_new;
    c_new.decode(i);
    ASSERT_EQ(1u, c_new.choose_args.size());
    ASSERT_EQ(1u, c_new.choose_args[caid].args[-1-id].weight_set_positions);
    ASSERT_EQ(weights, c_new.choose_args[caid].args[-1-id].weight_set[0].weights[0]);
    ASSERT_EQ(weight, c_new.get_bucket_item_weightf(id, 0));
  }

  // if the client is not compatible, copy choose_arg in the weights
  {
    c.choose_args[caid] = arg_map;
    bufferlist bl;
    c.encode(bl, features);
    c.choose_args.clear();
    auto i = bl.cbegin();
    CrushWrapper c_new;
    c_new.decode(i);
    ASSERT_EQ(0u, c_new.choose_args.size());
    ASSERT_EQ((int)weights, c_new.get_bucket_item_weight(id, 0));
  }
}

TEST(CrushWrapper, remove_root) {
  CrushWrapper c;
  c.create();
  c.set_type_name(1, "host");
  c.set_type_name(2, "rack");
  c.set_type_name(3, "root");

  int weight = 1;

  map<string,string> loc;
  loc["host"] = "b1";
  loc["rack"] = "r11";
  loc["root"] = "default";
  int item = 1;
  c.insert_item(g_ceph_context, item, weight, "osd.1", loc);
  item = 2;
  loc["host"] = "b2";
  loc["rack"] = "r12";
  loc["root"] = "default";
  c.insert_item(g_ceph_context, item, weight, "osd.2", loc);

  ceph_assert(c.add_simple_rule("rule1", "r11", "host", "",
			   "firstn", pg_pool_t::TYPE_ERASURE) >= 0);
  ASSERT_TRUE(c.name_exists("default"));
  ASSERT_TRUE(c.name_exists("r11"));
  ASSERT_TRUE(c.name_exists("r12"));
  ASSERT_EQ(c.remove_root(g_ceph_context, c.get_item_id("default")), 0);
  ASSERT_FALSE(c.name_exists("default"));
  ASSERT_FALSE(c.name_exists("r11"));
  ASSERT_FALSE(c.name_exists("r12"));
}

TEST(CrushWrapper, trim_roots_with_class) {
  CrushWrapper c;
  c.create();
  c.set_type_name(1, "root");

  int weight = 1;
  map<string,string> loc;
  loc["root"] = "default";

  int item = 1;
  c.insert_item(g_ceph_context, item, weight, "osd.1", loc);
  int cl = c.get_or_create_class_id("ssd");
  c.class_map[item] = cl;


  int root_id = c.get_item_id("default");
  int clone_id;
  map<int32_t, map<int32_t, int32_t>> old_class_bucket;
  map<int,map<int,vector<int>>> cmap_item_weight; // cargs -> bno -> weights
  set<int32_t> used_ids;

  ASSERT_EQ(c.device_class_clone(root_id, cl, old_class_bucket, used_ids,
				 &clone_id, &cmap_item_weight), 0);

  ASSERT_TRUE(c.name_exists("default"));
  ASSERT_TRUE(c.name_exists("default~ssd"));
  c.trim_roots_with_class(g_ceph_context);
  ASSERT_TRUE(c.name_exists("default"));
  ASSERT_FALSE(c.name_exists("default~ssd"));
}

TEST(CrushWrapper, device_class_clone) {
  CrushWrapper c;
  c.create();
  c.set_type_name(1, "host");
  c.set_type_name(2, "root");

  map<string,string> loc;
  loc["host"] = "b1";
  loc["root"] = "default";
  int weight = 1;

  int item = 1;
  c.insert_item(g_ceph_context, item, weight, "osd.1", loc);
  int cl = c.get_or_create_class_id("ssd");
  c.class_map[item] = cl;

  int item_no_class = 2;
  c.insert_item(g_ceph_context, item_no_class, weight, "osd.2", loc);

  c.reweight(g_ceph_context);

  map<int32_t, map<int32_t, int32_t>> old_class_bucket;
  map<int,map<int,vector<int>>> cmap_item_weight; // cargs -> bno -> weights
  set<int32_t> used_ids;
  int root_id = c.get_item_id("default");
  int clone_id;
  ASSERT_EQ(c.device_class_clone(root_id, cl, old_class_bucket, used_ids,
				 &clone_id, &cmap_item_weight), 0);
  ASSERT_TRUE(c.name_exists("default~ssd"));
  ASSERT_EQ(clone_id, c.get_item_id("default~ssd"));
  ASSERT_TRUE(c.subtree_contains(clone_id, item));
  ASSERT_FALSE(c.subtree_contains(clone_id, item_no_class));
  ASSERT_TRUE(c.subtree_contains(root_id, item_no_class));
  ASSERT_EQ(c.get_item_weightf(root_id), 2);
  ASSERT_EQ(c.get_item_weightf(clone_id), 1);
  // cloning again does nothing and returns the existing one
  int other_clone_id;
  ASSERT_EQ(c.device_class_clone(root_id, cl, old_class_bucket, used_ids,
				 &other_clone_id, &cmap_item_weight), 0);
  ASSERT_EQ(clone_id, other_clone_id);
  // invalid arguments
  ASSERT_EQ(c.device_class_clone(12345, cl, old_class_bucket, used_ids,
				 &other_clone_id, &cmap_item_weight), -ECHILD);
  ASSERT_EQ(c.device_class_clone(root_id, 12345, old_class_bucket, used_ids,
				 &other_clone_id, &cmap_item_weight), -EBADF);
}

TEST(CrushWrapper, split_id_class) {
  CrushWrapper c;
  c.create();
  c.set_type_name(1, "root");

  int weight = 1;
  map<string,string> loc;
  loc["root"] = "default";

  int item = 1;
  c.insert_item(g_ceph_context, item, weight, "osd.1", loc);
  int class_id = c.get_or_create_class_id("ssd");
  c.class_map[item] = class_id;

  map<int32_t, map<int32_t, int32_t>> old_class_bucket;
  map<int,map<int,vector<int>>> cmap_item_weight; // cargs -> bno -> weights
  set<int32_t> used_ids;
  int item_id = c.get_item_id("default");
  int clone_id;
  ASSERT_EQ(c.device_class_clone(item_id, class_id, old_class_bucket, used_ids,
				 &clone_id, &cmap_item_weight), 0);
  int retrieved_item_id;
  int retrieved_class_id;
  ASSERT_EQ(c.split_id_class(clone_id, &retrieved_item_id, &retrieved_class_id), 0);
  ASSERT_EQ(item_id, retrieved_item_id);
  ASSERT_EQ(class_id, retrieved_class_id);

  ASSERT_EQ(c.split_id_class(item_id, &retrieved_item_id, &retrieved_class_id), 0);
  ASSERT_EQ(item_id, retrieved_item_id);
  ASSERT_EQ(-1, retrieved_class_id);
}

TEST(CrushWrapper, populate_classes) {
  CrushWrapper c;
  c.create();
  c.set_type_name(1, "root");

  int weight = 1;
  map<string,string> loc;
  loc["root"] = "default";

  int item = 1;
  c.insert_item(g_ceph_context, item, weight, "osd.1", loc);
  int class_id = c.get_or_create_class_id("ssd");
  c.class_map[item] = class_id;

  map<int32_t, map<int32_t, int32_t>> old_class_bucket;
  ASSERT_EQ(c.populate_classes(old_class_bucket), 0);

  ASSERT_TRUE(c.name_exists("default~ssd"));

  old_class_bucket = c.class_bucket;
  ASSERT_EQ(c.populate_classes(old_class_bucket), 0);
  ASSERT_EQ(old_class_bucket, c.class_bucket);
}

TEST(CrushWrapper, remove_class_name) {
  CrushWrapper c;
  c.create();

  ASSERT_EQ(-ENOENT, c.remove_class_name("ssd"));
  ASSERT_GE(0, c.get_or_create_class_id("ssd"));
  ASSERT_EQ(0, c.remove_class_name("ssd"));
  ASSERT_EQ(-ENOENT, c.remove_class_name("ssd"));
}

TEST(CrushWrapper, try_remap_rule) {
  // build a simple 2 level map
  CrushWrapper c;
  c.create();
  c.set_type_name(0, "osd");
  c.set_type_name(1, "host");
  c.set_type_name(2, "rack");
  c.set_type_name(3, "root");
  int bno;
  int r = c.add_bucket(0, CRUSH_BUCKET_STRAW2,
		       CRUSH_HASH_DEFAULT, 3, 0, NULL,
		       NULL, &bno);
  ASSERT_EQ(0, r);
  ASSERT_EQ(-1, bno);
  c.set_item_name(bno, "default");

  c.set_max_devices(20);

  //JSONFormatter jf(true);

  map<string,string> loc;
  loc["host"] = "foo";
  loc["rack"] = "a";
  loc["root"] = "default";
  c.insert_item(g_ceph_context, 0, 1, "osd.0", loc);
  c.insert_item(g_ceph_context, 1, 1, "osd.1", loc);
  c.insert_item(g_ceph_context, 2, 1, "osd.2", loc);

  loc.clear();
  loc["host"] = "bar";
  loc["rack"] = "a";
  loc["root"] = "default";
  c.insert_item(g_ceph_context, 3, 1, "osd.3", loc);
  c.insert_item(g_ceph_context, 4, 1, "osd.4", loc);
  c.insert_item(g_ceph_context, 5, 1, "osd.5", loc);

  loc.clear();
  loc["host"] = "baz";
  loc["rack"] = "b";
  loc["root"] = "default";
  c.insert_item(g_ceph_context, 6, 1, "osd.6", loc);
  c.insert_item(g_ceph_context, 7, 1, "osd.7", loc);
  c.insert_item(g_ceph_context, 8, 1, "osd.8", loc);

  loc.clear();
  loc["host"] = "qux";
  loc["rack"] = "b";
  loc["root"] = "default";
  c.insert_item(g_ceph_context, 9, 1, "osd.9", loc);
  c.insert_item(g_ceph_context, 10, 1, "osd.10", loc);
  c.insert_item(g_ceph_context, 11, 1, "osd.11", loc);
  c.finalize();

  loc.clear();
  loc["host"] = "bif";
  loc["rack"] = "c";
  loc["root"] = "default";
  c.insert_item(g_ceph_context, 12, 1, "osd.12", loc);
  c.insert_item(g_ceph_context, 13, 1, "osd.13", loc);
  c.insert_item(g_ceph_context, 14, 1, "osd.14", loc);
  c.finalize();

  loc.clear();
  loc["host"] = "pop";
  loc["rack"] = "c";
  loc["root"] = "default";
  c.insert_item(g_ceph_context, 15, 1, "osd.15", loc);
  c.insert_item(g_ceph_context, 16, 1, "osd.16", loc);
  c.insert_item(g_ceph_context, 17, 1, "osd.17", loc);
  c.finalize();

  //c.dump(&jf);
  //jf.flush(cout);

  // take + emit
  {
  }

  // take + choose device + emit
  {
    cout << "take + choose + emit" << std::endl;
    ostringstream err;
    int rule = c.add_simple_rule("one", "default", "osd", "",
				 "firstn", 0, &err);
    ASSERT_EQ(rule, 0);

    vector<int> orig = { 0, 3, 9 };
    set<int> overfull = { 3 };
    vector<int> underfull = { 0, 2, 5, 8, 11 };
    vector<int> out;
    int r = c.try_remap_rule(g_ceph_context, rule, 3,
			      overfull, underfull,
			      orig, &out);
    cout << orig << " -> r = " << (int)r << " out " << out << std::endl;
    ASSERT_EQ(r, 0);
    ASSERT_EQ(3u, out.size());
    ASSERT_EQ(0, out[0]);
    ASSERT_EQ(2, out[1]);
    ASSERT_EQ(9, out[2]);

    // make sure we cope with dups between underfull and future values in orig
    underfull = {9, 0, 2, 5};
    orig = {1, 3, 9};

    r = c.try_remap_rule(g_ceph_context, rule, 3,
			 overfull, underfull,
			 orig, &out);
    cout << orig << " -> r = " << (int)r << " out " << out << std::endl;
    ASSERT_EQ(r, 0);
    ASSERT_EQ(3u, out.size());
    ASSERT_EQ(1, out[0]);
    ASSERT_EQ(0, out[1]);
    ASSERT_EQ(9, out[2]);
  }

  // chooseleaf
  {
    cout << "take + chooseleaf + emit" << std::endl;
    ostringstream err;
    int rule = c.add_simple_rule("two", "default", "host", "",
				 "firstn", 0, &err);
    ASSERT_EQ(rule, 1);

    vector<int> orig = { 0, 3, 9 };
    set<int> overfull = { 3 };
    vector<int> underfull = { 0, 2, 5, 8, 11 };
    vector<int> out;
    int r = c.try_remap_rule(g_ceph_context, rule, 3,
			      overfull, underfull,
			      orig, &out);
    cout << orig << " -> r = " << (int)r << " out " << out << std::endl;
    ASSERT_EQ(r, 0);
    ASSERT_EQ(3u, out.size());
    ASSERT_EQ(0, out[0]);
    ASSERT_EQ(5, out[1]);
    ASSERT_EQ(9, out[2]);
  }

  // choose + choose
  {
    cout << "take + choose + choose + choose + emit" << std::endl;
    int rule = c.add_rule(2, 5, 0, 1, 10);
    ASSERT_EQ(2, rule);
    c.set_rule_step_take(rule, 0, bno);
    c.set_rule_step_choose_indep(rule, 1, 2, 2);
    c.set_rule_step_choose_indep(rule, 2, 2, 1);
    c.set_rule_step_choose_indep(rule, 3, 1, 0);
    c.set_rule_step_emit(rule, 4);

    vector<int> orig = { 0, 3, 16, 12 };
    set<int> overfull = { 3, 12 };
    vector<int> underfull = { 6, 7, 9, 3, 0, 1, 15, 16, 13, 2, 5, 8, 11 };
    vector<int> out;
    int r = c.try_remap_rule(g_ceph_context, rule, 3,
			      overfull, underfull,
			      orig, &out);
    cout << orig << " -> r = " << (int)r << " out " << out << std::endl;
    ASSERT_EQ(r, 0);
    ASSERT_EQ(4u, out.size());
    ASSERT_EQ(0, out[0]);
    ASSERT_EQ(5, out[1]);
    ASSERT_EQ(16, out[2]);
    ASSERT_EQ(13, out[3]);

    orig.pop_back();
    out.clear();
    r = c.try_remap_rule(g_ceph_context, rule, 3,
			 overfull, underfull,
			 orig, &out);
    cout << orig << " -> r = " << (int)r << " out " << out << std::endl;
    ASSERT_EQ(r, 0);
    ASSERT_EQ(3u, out.size());
    ASSERT_EQ(0, out[0]);
    ASSERT_EQ(5, out[1]);
    ASSERT_EQ(16, out[2]);
  }
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  map<string,string> defaults = {
    { "debug_crush", "0" }
  };
  auto cct = global_init(&defaults, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
// Local Variables:
// compile-command: "cd ../../../build ; make -j4 unittest_crush_wrapper && valgrind --tool=memcheck bin/unittest_crush_wrapper"
// End:
