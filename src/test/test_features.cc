// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <stdio.h>

#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "gtest/gtest.h"
#include "include/ceph_features.h"
#include "include/rados.h"


TEST(features, release_features)
{
  for (int r = 1; r < CEPH_RELEASE_MAX; ++r) {
    const char *name = ceph_release_name(r);
    ASSERT_NE(string("unknown"), name);
    ASSERT_EQ(r, ceph_release_from_name(name));
    uint64_t features = ceph_release_features(r);
    int rr = ceph_release_from_features(features);
    cout << r << " " << name << " features 0x" << std::hex << features
	 << std::dec << " looks like " << ceph_release_name(rr) << std::endl;
    EXPECT_LE(rr, r);
  }
}

TEST(features, release_from_features) {
  ASSERT_EQ(CEPH_RELEASE_JEWEL, ceph_release_from_features(575862587619852283));
  ASSERT_EQ(CEPH_RELEASE_LUMINOUS,
	    ceph_release_from_features(1152323339925389307));
}

int main(int argc, char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
