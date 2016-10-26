// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include <errno.h>
#include <stdlib.h>
#include "global/global_init.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "common/config.h"
#include "gtest/gtest.h"

TEST(ErasureCodePlugin, factory)
{
  PluginRegistry *instance = g_ceph_context->get_plugin_registry();
  ErasureCodePlugin* ecp = dynamic_cast<ErasureCodePlugin*>(instance->get_with_load("erasure-code", "jerasure"));
  EXPECT_TRUE(ecp);
  ErasureCodeProfile profile;
  {
    ErasureCodeInterfaceRef erasure_code;
    EXPECT_FALSE(erasure_code);
    EXPECT_EQ(-ENOENT, ecp->factory(profile, &erasure_code, &cerr));
  }
  const char *techniques[] = {
    "reed_sol_van",
    "reed_sol_r6_op",
    "cauchy_orig",
    "cauchy_good",
    "liberation",
    "blaum_roth",
    "liber8tion",
    0
  };
  for(const char **technique = techniques; *technique; technique++) {
    ErasureCodeInterfaceRef erasure_code;
    ErasureCodeProfile profile;
    profile["technique"] = *technique;
    EXPECT_FALSE(erasure_code);
    ErasureCodePlugin* ecp = dynamic_cast<ErasureCodePlugin*>(instance->get_with_load("erasure-code", "jerasure"));
    EXPECT_TRUE(ecp);
    EXPECT_EQ(0, ecp->factory(profile, &erasure_code, &cerr));
    EXPECT_TRUE(erasure_code.get());
  }
}

int main(int argc, char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  const char* env = getenv("CEPH_LIB");
  string directory(env ? env : "lib");

  g_conf->set_val("plugin_dir", directory, false, false);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 &&
 *   make unittest_erasure_code_plugin_jerasure &&
 *   valgrind --tool=memcheck ./unittest_erasure_code_plugin_jerasure \
 *      --gtest_filter=*.* --log-to-stderr=true --debug-osd=20"
 * End:
 */
