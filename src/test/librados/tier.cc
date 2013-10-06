// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "gtest/gtest.h"

#include "mds/mdstypes.h"
#include "include/buffer.h"
#include "include/rbd_types.h"
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/stringify.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "test/librados/test.h"

#include <errno.h>
#include <map>
#include <sstream>
#include <string>

using namespace librados;
using ceph::buffer;
using std::map;
using std::ostringstream;
using std::string;

TEST(LibRadosMisc, Dirty) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));

  {
    ObjectWriteOperation op;
    op.create(true);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bool dirty = false;
    int r = -1;
    ObjectReadOperation op;
    op.is_dirty(&dirty, &r);
    ASSERT_EQ(0, ioctx.operate("foo", &op, NULL));
    ASSERT_TRUE(dirty);
    ASSERT_EQ(0, r);
  }
  {
    ObjectWriteOperation op;
    op.undirty();
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bool dirty = false;
    int r = -1;
    ObjectReadOperation op;
    op.is_dirty(&dirty, &r);
    ASSERT_EQ(0, ioctx.operate("foo", &op, NULL));
    ASSERT_FALSE(dirty);
    ASSERT_EQ(0, r);
  }
  {
    ObjectWriteOperation op;
    op.truncate(0);  // still a write even tho it is a no-op
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bool dirty = false;
    int r = -1;
    ObjectReadOperation op;
    op.is_dirty(&dirty, &r);
    ASSERT_EQ(0, ioctx.operate("foo", &op, NULL));
    ASSERT_TRUE(dirty);
    ASSERT_EQ(0, r);
  }

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(LibRadosMisc, HitSet) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));

  {
    ObjectReadOperation op;
    list< pair<time_t,time_t> > ls;
    int rval;
    op.hit_set_ls(123, &ls, &rval);
    ASSERT_EQ(0, ioctx.operate("foo", &op, NULL));
  }

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}


int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);

  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  return RUN_ALL_TESTS();
}
