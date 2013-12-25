// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank <info@inktank.com>
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

#include "crush/CrushWrapper.h"
#include "gtest/gtest.h"
#include "global/global_init.h"
#include "global/global_context.h"
#include "common/ceph_argparse.h"
#include "common/Formatter.h"

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


int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
