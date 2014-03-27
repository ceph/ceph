// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
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
#include "arch/probe.h"
#include "arch/intel.h"
#include "global/global_init.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "gtest/gtest.h"

TEST(ErasureCodePlugin, factory)
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  map<std::string,std::string> parameters;
  parameters["directory"] = ".libs";
  {
    ErasureCodeInterfaceRef erasure_code;
    EXPECT_FALSE(erasure_code);
    EXPECT_EQ(-ENOENT, instance.factory("jerasure", parameters,
                                        &erasure_code, cerr));
    EXPECT_FALSE(erasure_code);
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
    parameters["technique"] = *technique;
    EXPECT_FALSE(erasure_code);
    EXPECT_EQ(0, instance.factory("jerasure", parameters,
                                  &erasure_code, cerr));
    EXPECT_TRUE(erasure_code);
  }
}

TEST(ErasureCodePlugin, select)
{
  ceph_arch_probe();
  // save probe results
  int arch_intel_pclmul = ceph_arch_intel_pclmul;
  int arch_intel_sse42  = ceph_arch_intel_sse42;
  int arch_intel_sse41  = ceph_arch_intel_sse41;
  int arch_intel_ssse3  = ceph_arch_intel_ssse3;
  int arch_intel_sse3   = ceph_arch_intel_sse3;
  int arch_intel_sse2   = ceph_arch_intel_sse2;

  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  map<std::string,std::string> parameters;
  // load test plugins instead of actual plugins to assert the desired side effect
  // happens
  parameters["jerasure-name"] = "test_jerasure";
  parameters["directory"] = ".libs";
  parameters["technique"] = "reed_sol_van";

  // all features are available, load the SSE4 plugin
  {
    ceph_arch_intel_pclmul = 1;
    ceph_arch_intel_sse42  = 1;
    ceph_arch_intel_sse41  = 1;
    ceph_arch_intel_ssse3  = 1;
    ceph_arch_intel_sse3   = 1;
    ceph_arch_intel_sse2   = 1;

    ErasureCodeInterfaceRef erasure_code;
    int sse4_side_effect = -444;
    EXPECT_EQ(sse4_side_effect, instance.factory("jerasure", parameters,
                                                 &erasure_code, cerr));
  }
  // pclmul is missing, load the SSE3 plugin
  {
    ceph_arch_intel_pclmul = 0;
    ceph_arch_intel_sse42  = 1;
    ceph_arch_intel_sse41  = 1;
    ceph_arch_intel_ssse3  = 1;
    ceph_arch_intel_sse3   = 1;
    ceph_arch_intel_sse2   = 1;

    ErasureCodeInterfaceRef erasure_code;
    int sse3_side_effect = -333;
    EXPECT_EQ(sse3_side_effect, instance.factory("jerasure", parameters,
                                                 &erasure_code, cerr));
  }
  // pclmul and sse3 are missing, load the generic plugin
  {
    ceph_arch_intel_pclmul = 0;
    ceph_arch_intel_sse42  = 1;
    ceph_arch_intel_sse41  = 1;
    ceph_arch_intel_ssse3  = 1;
    ceph_arch_intel_sse3   = 0;
    ceph_arch_intel_sse2   = 1;

    ErasureCodeInterfaceRef erasure_code;
    int generic_side_effect = -111;
    EXPECT_EQ(generic_side_effect, instance.factory("jerasure", parameters,
                                                 &erasure_code, cerr));
  }


  // restore probe results
  ceph_arch_intel_pclmul = arch_intel_pclmul;
  ceph_arch_intel_sse42  = arch_intel_sse42;
  ceph_arch_intel_sse41  = arch_intel_sse41;
  ceph_arch_intel_ssse3  = arch_intel_ssse3;
  ceph_arch_intel_sse3   = arch_intel_sse3;
  ceph_arch_intel_sse2   = arch_intel_sse2;
}

TEST(ErasureCodePlugin, sse)
{
  ceph_arch_probe();
  bool sse4 = ceph_arch_intel_pclmul &&
    ceph_arch_intel_sse42 && ceph_arch_intel_sse41 &&
    ceph_arch_intel_ssse3 && ceph_arch_intel_sse3 &&
    ceph_arch_intel_sse2;
  bool sse3 = ceph_arch_intel_ssse3 && ceph_arch_intel_sse3 &&
    ceph_arch_intel_sse2;
  vector<string> sse_variants;
  sse_variants.push_back("generic");
  if (!sse3)
    cerr << "SKIP sse3 plugin testing because CPU does not support it\n";
  else
    sse_variants.push_back("sse3");
  if (!sse4)
    cerr << "SKIP sse4 plugin testing because CPU does not support it\n";
  else
    sse_variants.push_back("sse4");

#define LARGE_ENOUGH 2048
  bufferptr in_ptr(buffer::create_page_aligned(LARGE_ENOUGH));
  in_ptr.zero();
  in_ptr.set_length(0);
  const char *payload =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  in_ptr.append(payload, strlen(payload));
  bufferlist in;
  in.push_front(in_ptr);

  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  map<std::string,std::string> parameters;
  parameters["directory"] = ".libs";
  parameters["technique"] = "reed_sol_van";
  parameters["k"] = "2";
  parameters["m"] = "1";
  for (vector<string>::iterator sse_variant = sse_variants.begin();
       sse_variant != sse_variants.end();
       sse_variant++) {
    //
    // load the plugin variant
    //
    ErasureCodeInterfaceRef erasure_code;
    EXPECT_FALSE(erasure_code);
    EXPECT_EQ(0, instance.factory("jerasure_" + *sse_variant, parameters,
                                  &erasure_code, cerr));
    EXPECT_TRUE(erasure_code);

    //
    // encode
    //
    int want_to_encode[] = { 0, 1, 2 };
    map<int, bufferlist> encoded;
    EXPECT_EQ(0, erasure_code->encode(set<int>(want_to_encode, want_to_encode+3),
                                      in,
                                      &encoded));
    EXPECT_EQ(3u, encoded.size());
    unsigned length =  encoded[0].length();
    EXPECT_EQ(0, strncmp(encoded[0].c_str(), in.c_str(), length));
    EXPECT_EQ(0, strncmp(encoded[1].c_str(), in.c_str() + length,
                         in.length() - length));

    //
    // decode with reconstruction
    //
    map<int, bufferlist> degraded = encoded;
    degraded.erase(1);
    EXPECT_EQ(2u, degraded.size());
    int want_to_decode[] = { 0, 1 };
    map<int, bufferlist> decoded;
    EXPECT_EQ(0, erasure_code->decode(set<int>(want_to_decode, want_to_decode+2),
                                      degraded,
                                      &decoded));
    EXPECT_EQ(3u, decoded.size());
    EXPECT_EQ(length, decoded[0].length());
    EXPECT_EQ(0, strncmp(decoded[0].c_str(), in.c_str(), length));
    EXPECT_EQ(0, strncmp(decoded[1].c_str(), in.c_str() + length,
                         in.length() - length));

  }
}

int main(int argc, char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

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
