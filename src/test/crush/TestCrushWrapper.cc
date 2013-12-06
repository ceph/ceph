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

TEST(CrushWrapper, update_item) {
  CrushWrapper *c = new CrushWrapper;
  c->create();

#define ROOT_TYPE 3
  c->set_type_name(ROOT_TYPE, "root");
#define RACK_TYPE 2
  c->set_type_name(RACK_TYPE, "rack");
#define HOST_TYPE 1
  c->set_type_name(HOST_TYPE, "host");
#define OSD_TYPE 0
  c->set_type_name(OSD_TYPE, "osd");

  int rootno;
  c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
		ROOT_TYPE, 0, NULL, NULL, &rootno);
  c->set_item_name(rootno, "default");

  int item = 0;

  // invalid names anywhere in loc trigger an error
  {
    map<string,string> loc;
    loc["rack"] = "\001";
    EXPECT_EQ(-EINVAL, c->update_item(g_ceph_context, item, 1.0,
				      "osd." + stringify(item), loc));
  }
}

TEST(CrushWrapper, insert_item) {
  CrushWrapper *c = new CrushWrapper;
  c->create();

#define ROOT_TYPE 3
  c->set_type_name(ROOT_TYPE, "root");
#define RACK_TYPE 2
  c->set_type_name(RACK_TYPE, "rack");
#define HOST_TYPE 1
  c->set_type_name(HOST_TYPE, "host");
#define OSD_TYPE 0
  c->set_type_name(OSD_TYPE, "osd");

  int rootno;
  c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
		ROOT_TYPE, 0, NULL, NULL, &rootno);
  c->set_item_name(rootno, "default");

  int item = 0;

  // invalid names anywhere in loc trigger an error
  {
    map<string,string> loc;
    loc["rack"] = "\001";
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
    std::string name = "NAME";
    map<string,string> loc;
    loc["root"] = "default";
    loc["rack"] = name;

    item++;
    EXPECT_EQ(0, c->insert_item(g_ceph_context, item, 1.0,
				"osd." + stringify(item), loc));
  }
  // adding to an existing item name that is not associated with a bucket
  {
    std::string name = "ITEM_WITHOUT_BUCKET";
    map<string,string> loc;
    loc["root"] = "default";
    loc["rack"] = name;
    item++;
    c->set_item_name(item, name);

    item++;
    EXPECT_EQ(-EINVAL, c->insert_item(g_ceph_context, item, 1.0,
				      "osd." + stringify(item), loc));
  }
  // 
  //   When there is:
  //
  //   default --> rack0 --> item
  //
  //   Trying to insert the same item higher in the hirarchy will fail
  //   because it would create a loop.
  //
  //   default --> rack0 --> item
  //           |
  //           +-> item 
  //
  {
    item++;
    {
      map<string,string> loc;
      loc["root"] = "default";
      loc["rack"] = "rack0";

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
  //   default --> rack0
  //
  //   Trying to insert default under rack0 must fail
  //   because it would create a loop.
  //
  //   default --> rack0 --> default
  //
  {
    map<string,string> loc;
    loc["rack"] = "rack0";

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
    // wrongfully pretend the osd is of type rack
    loc["rack"] = "myosd";

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
  c->create();
  int index = 123;
  std::string name = "NAME";
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
  c->create();
  int index = 123;
  std::string name = "NAME";
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
    loc["rack"] = "\003";
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
