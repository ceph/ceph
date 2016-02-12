// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2015 Mirantis, Inc.
 *
 * Author: Alyona Kiseleva <akiselyova@mirantis.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <gtest/gtest.h>
#include "global/global_init.h"
#include "compressor/Compressor.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "common/config.h"

TEST(CompressionPluginSnappy, factory)
{
  CompressorRef compressor = Compressor::create(g_ceph_context, "snappy");
  cout << compressor;
  EXPECT_TRUE(compressor.get());
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  const char* env = getenv("CEPH_LIB");
  std::string directory(env ? env : "lib");
  string mkdir_compressor = "mkdir -p " + directory + "/compressor";
  int r = system(mkdir_compressor.c_str());
  (void)r;

  string cp_libceph_snappy = "cp " + directory + "/libceph_snappy.so* " + directory + "/compressor/";
  r = system(cp_libceph_snappy.c_str());
  (void)r;

  g_conf->set_val("plugin_dir", directory, false, false);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 && 
 *   make unittest_compression_plugin_snappy && 
 *   valgrind --tool=memcheck \
 *      ./unittest_compression_plugin_snappy \
 *      --gtest_filter=*.* --log-to-stderr=true --debug-osd=20"
 * End:
 */
