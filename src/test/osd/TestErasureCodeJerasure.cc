// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include "global/global_init.h"
#include "osd/ErasureCodePluginJerasure/ErasureCodeJerasure.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "gtest/gtest.h"

template <typename T>
class ErasureCodeTest : public ::testing::Test {
 public:
};

typedef ::testing::Types<
  ErasureCodeJerasureReedSolomonVandermonde,
  ErasureCodeJerasureReedSolomonRAID6,
  ErasureCodeJerasureCauchyOrig,
  ErasureCodeJerasureCauchyGood,
  ErasureCodeJerasureLiberation,
  ErasureCodeJerasureBlaumRoth,
  ErasureCodeJerasureLiber8tion
> JerasureTypes;
TYPED_TEST_CASE(ErasureCodeTest, JerasureTypes);

TYPED_TEST(ErasureCodeTest, encode_decode) {
  TypeParam jerasure;
  map<std::string,std::string> parameters;
  parameters["erasure-code-k"] = "2";
  parameters["erasure-code-m"] = "2";
  parameters["erasure-code-w"] = "7";
  parameters["erasure-code-packetsize"] = "8";
  jerasure.init(parameters);

  bufferlist in;
  for (int i = 0; i < 5; i++)
    in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789");
  int want_to_encode[] = { 0, 1, 2, 3 };
  map<int, bufferlist> encoded;
  EXPECT_EQ(0, jerasure.encode(set<int>(want_to_encode, want_to_encode+4),
                              in,
                              &encoded));
  EXPECT_EQ(4u, encoded.size());
  unsigned length =  encoded[0].length();
  EXPECT_EQ(0, strncmp(encoded[0].c_str(), in.c_str(), length));
  EXPECT_EQ(0, strncmp(encoded[1].c_str(), in.c_str() + length, in.length() - length));


  // all chunks are available
  {
    int want_to_decode[] = { 0, 1 };
    map<int, bufferlist> decoded;
    EXPECT_EQ(0, jerasure.decode(set<int>(want_to_decode, want_to_decode+2),
                                encoded,
                                &decoded));
    // always decode all, regardless of want_to_decode
    EXPECT_EQ(4u, decoded.size()); 
    EXPECT_EQ(length, decoded[0].length());
    EXPECT_EQ(0, strncmp(decoded[0].c_str(), in.c_str(), length));
    EXPECT_EQ(0, strncmp(decoded[1].c_str(), in.c_str() + length, in.length() - length));
  }

  // two chunks are missing 
  {
    map<int, bufferlist> degraded = encoded;
    degraded.erase(0);
    degraded.erase(1);
    EXPECT_EQ(2u, degraded.size());
    int want_to_decode[] = { 0, 1 };
    map<int, bufferlist> decoded;
    EXPECT_EQ(0, jerasure.decode(set<int>(want_to_decode, want_to_decode+2),
                                degraded,
                                &decoded));
    // always decode all, regardless of want_to_decode
    EXPECT_EQ(4u, decoded.size()); 
    EXPECT_EQ(length, decoded[0].length());
    EXPECT_EQ(0, strncmp(decoded[0].c_str(), in.c_str(), length));
    EXPECT_EQ(0, strncmp(decoded[1].c_str(), in.c_str() + length, in.length() - length));
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

// Local Variables:
// compile-command: "cd ../.. ; make -j4 && make unittest_erasure_code_jerasure && valgrind --tool=memcheck ./unittest_erasure_code_jerasure --gtest_filter=*.* --log-to-stderr=true --debug-osd=20"
// End:
