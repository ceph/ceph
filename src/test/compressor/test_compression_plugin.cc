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
#include <string.h>
#include <gtest/gtest.h>
#include "global/global_init.h"
#include "compressor/CompressionPlugin.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "common/config.h"

TEST(CompressionPlugin, all)
{
  const char* env = getenv("CEPH_LIB");
  std::string directory(env ? env : "lib");
  CompressorRef compressor;
  PluginRegistry *reg = g_ceph_context->get_plugin_registry();
  EXPECT_TRUE(reg);
  CompressionPlugin *factory = dynamic_cast<CompressionPlugin*>(reg->get_with_load("compressor", "invalid"));
  EXPECT_FALSE(factory);
  factory = dynamic_cast<CompressionPlugin*>(reg->get_with_load("compressor", "example"));
  EXPECT_TRUE(factory);
  stringstream ss;
  EXPECT_EQ(0, factory->factory(&compressor, &ss));
  EXPECT_TRUE(compressor.get());
  {
    Mutex::Locker l(reg->lock);
    EXPECT_EQ(-ENOENT, reg->remove("compressor", "does not exist"));
    EXPECT_EQ(0, reg->remove("compressor", "example"));
    EXPECT_EQ(0, reg->load("compressor", "example"));
  }
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  const char* env = getenv("CEPH_LIB");
  string directory(env ? env : "lib");
  string mkdir_compressor = "mkdir -p " + directory + "/compressor";
  int r = system(mkdir_compressor.c_str());
  (void)r;

  string cp_libceph_example = "cp " + directory + "/libceph_example.so* " + directory + "/compressor/";
  r = system(cp_libceph_example.c_str());
  (void)r;

  g_conf->set_val("plugin_dir", directory, false, false);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 && 
 *   make unittest_compression_plugin && 
 *   valgrind --tool=memcheck \
 *      ./unittest_compression_plugin \
 *      --gtest_filter=*.* --log-to-stderr=true --debug-osd=20"
 * End:
 */
