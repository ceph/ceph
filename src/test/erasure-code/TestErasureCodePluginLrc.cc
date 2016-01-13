// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
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
#include "arch/probe.h"
#include "arch/intel.h"
#include "global/global_init.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "common/config.h"
#include "gtest/gtest.h"

TEST(ErasureCodePlugin, factory)
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  ErasureCodeProfile profile;
  profile["mapping"] = "DD_";
  profile["layers"] = "[ [ \"DDc\", \"\" ] ]";
  ErasureCodeInterfaceRef erasure_code;
  EXPECT_FALSE(erasure_code);
  EXPECT_EQ(0, instance.factory("lrc",
				g_conf->erasure_code_dir,
				profile, &erasure_code, &cerr));
  EXPECT_TRUE(erasure_code.get());
}

int main(int argc, char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  string directory = getenv("CEPH_LIB");
  g_conf->set_val("erasure_code_dir", directory, false, false);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 &&
 *   make unittest_erasure_code_plugin_lrc &&
 *   valgrind --tool=memcheck ./unittest_erasure_code_plugin_lrc \
 *      --gtest_filter=*.* --log-to-stderr=true --debug-osd=20"
 * End:
 */
