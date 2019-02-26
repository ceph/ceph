// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <condition_variable>
#include <map>
#include <sstream>
#include <string>

#include "gtest/gtest.h"

#include "include/rados/librados.hpp"
#include "include/stringify.h"
#include "test/librados/test_cxx.h"

using namespace librados;
using std::map;
using std::ostringstream;
using std::string;

TEST(LibRadosCmd, MonDescribePP) {
  Rados cluster;
  ASSERT_EQ("", connect_cluster_pp(cluster));
  bufferlist inbl, outbl;
  string outs;
  ASSERT_EQ(0, cluster.mon_command("{\"prefix\": \"get_command_descriptions\"}",
				   inbl, &outbl, &outs));
  ASSERT_LT(0u, outbl.length());
  ASSERT_LE(0u, outs.length());
  cluster.shutdown();
}

TEST(LibRadosCmd, OSDCmdPP) {
  Rados cluster;
  ASSERT_EQ("", connect_cluster_pp(cluster));
  int r;
  bufferlist inbl, outbl;
  string outs;
  string cmd;

  // note: tolerate NXIO here in case the cluster is thrashing out underneath us.
  cmd = "asdfasdf";
  r = cluster.osd_command(0, cmd, inbl, &outbl, &outs);
  ASSERT_TRUE(r == -22 || r == -ENXIO);
  cmd = "version";
  r = cluster.osd_command(0, cmd, inbl, &outbl, &outs);
  ASSERT_TRUE(r == -22 || r == -ENXIO);
  cmd = "{\"prefix\":\"version\"}";
  r = cluster.osd_command(0, cmd, inbl, &outbl, &outs);
  ASSERT_TRUE((r == 0 && outbl.length() > 0) || (r == -ENXIO && outbl.length() == 0));
  cluster.shutdown();
}

TEST(LibRadosCmd, PGCmdPP) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));

  int r;
  bufferlist inbl, outbl;
  string outs;
  string cmd;

  int64_t poolid = cluster.pool_lookup(pool_name.c_str());
  ASSERT_LT(0, poolid);

  string pgid = stringify(poolid) + ".0";

  cmd = "asdfasdf";
  // note: tolerate NXIO here in case the cluster is thrashing out underneath us.
  r = cluster.pg_command(pgid.c_str(), cmd, inbl, &outbl, &outs);
  ASSERT_TRUE(r == -22 || r == -ENXIO);

  // make sure the pg exists on the osd before we query it
  IoCtx io;
  cluster.ioctx_create(pool_name.c_str(), io);
  for (int i=0; i<100; i++) {
    string oid = "obj" + stringify(i);
    ASSERT_EQ(-ENOENT, io.stat(oid, NULL, NULL));
  }
  io.close();

  cmd = "{\"prefix\":\"pg\", \"cmd\":\"query\", \"pgid\":\"" +  pgid + "\"}";
  // note: tolerate ENOENT/ENXIO here if hte osd is thrashing out underneath us
  r = cluster.pg_command(pgid.c_str(), cmd, inbl, &outbl, &outs);
  ASSERT_TRUE(r == 0 || r == -ENOENT || r == -ENXIO);

  ASSERT_LT(0u, outbl.length());

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

