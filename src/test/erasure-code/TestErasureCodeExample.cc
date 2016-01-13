// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
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
#include <stdlib.h>

#include "include/stringify.h"
#include "global/global_init.h"
#include "ErasureCodeExample.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "gtest/gtest.h"

TEST(ErasureCodeExample, chunk_size)
{
  ErasureCodeExample example;
  EXPECT_EQ(3u, example.get_chunk_count());
  EXPECT_EQ(11u, example.get_chunk_size(20));
}

TEST(ErasureCodeExample, minimum_to_decode)
{
  ErasureCodeExample example;
  set<int> available_chunks;
  set<int> want_to_read;
  want_to_read.insert(1);
  {
    set<int> minimum;
    EXPECT_EQ(-EIO, example.minimum_to_decode(want_to_read,
                                              available_chunks,
                                              &minimum));
  }
  available_chunks.insert(0);
  available_chunks.insert(2);
  {
    set<int> minimum;
    EXPECT_EQ(0, example.minimum_to_decode(want_to_read,
                                           available_chunks,
                                           &minimum));
    EXPECT_EQ(available_chunks, minimum);
    EXPECT_EQ(2u, minimum.size());
    EXPECT_EQ(1u, minimum.count(0));
    EXPECT_EQ(1u, minimum.count(2));
  }
  {
    set<int> minimum;
    available_chunks.insert(1);
    EXPECT_EQ(0, example.minimum_to_decode(want_to_read,
                                           available_chunks,
                                           &minimum));
    EXPECT_EQ(1u, minimum.size());
    EXPECT_EQ(1u, minimum.count(1));
  }
}

TEST(ErasureCodeExample, minimum_to_decode_with_cost)
{
  ErasureCodeExample example;
  map<int,int> available;
  set<int> want_to_read;
  want_to_read.insert(1);
  {
    set<int> minimum;
    EXPECT_EQ(-EIO, example.minimum_to_decode_with_cost(want_to_read,
							available,
							&minimum));
  }
  available[0] = 1;
  available[2] = 1;
  {
    set<int> minimum;
    EXPECT_EQ(0, example.minimum_to_decode_with_cost(want_to_read,
						     available,
						     &minimum));
    EXPECT_EQ(2u, minimum.size());
    EXPECT_EQ(1u, minimum.count(0));
    EXPECT_EQ(1u, minimum.count(2));
  }
  {
    set<int> minimum;
    available[1] = 1;
    EXPECT_EQ(0, example.minimum_to_decode_with_cost(want_to_read,
						     available,
						     &minimum));
    EXPECT_EQ(1u, minimum.size());
    EXPECT_EQ(1u, minimum.count(1));
  }
  {
    set<int> minimum;
    available[1] = 2;
    EXPECT_EQ(0, example.minimum_to_decode_with_cost(want_to_read,
						     available,
						     &minimum));
    EXPECT_EQ(2u, minimum.size());
    EXPECT_EQ(1u, minimum.count(0));
    EXPECT_EQ(1u, minimum.count(2));
  }
}

TEST(ErasureCodeExample, encode_decode)
{
  ErasureCodeExample example;

  bufferlist in;
  in.append("ABCDE");
  set<int> want_to_encode;
  for(unsigned int i = 0; i < example.get_chunk_count(); i++)
    want_to_encode.insert(i);
  map<int, bufferlist> encoded;
  EXPECT_EQ(0, example.encode(want_to_encode, in, &encoded));
  EXPECT_EQ(example.get_chunk_count(), encoded.size());
  EXPECT_EQ(example.get_chunk_size(in.length()), encoded[0].length());
  EXPECT_EQ('A', encoded[0][0]);
  EXPECT_EQ('B', encoded[0][1]);
  EXPECT_EQ('C', encoded[0][2]);
  EXPECT_EQ('D', encoded[1][0]);
  EXPECT_EQ('E', encoded[1][1]);
  EXPECT_EQ('A'^'D', encoded[2][0]);
  EXPECT_EQ('B'^'E', encoded[2][1]);
  EXPECT_EQ('C'^0, encoded[2][2]);

  // all chunks are available
  {
    int want_to_decode[] = { 0, 1 };
    map<int, bufferlist> decoded;
    EXPECT_EQ(0, example.decode(set<int>(want_to_decode, want_to_decode+2),
                                encoded,
                                &decoded));
    EXPECT_EQ(2u, decoded.size());
    EXPECT_EQ(3u, decoded[0].length());
    EXPECT_EQ('A', decoded[0][0]);
    EXPECT_EQ('B', decoded[0][1]);
    EXPECT_EQ('C', decoded[0][2]);
    EXPECT_EQ('D', decoded[1][0]);
    EXPECT_EQ('E', decoded[1][1]);
  }

  // one chunk is missing 
  {
    map<int, bufferlist> degraded = encoded;
    degraded.erase(0);
    EXPECT_EQ(2u, degraded.size());
    int want_to_decode[] = { 0, 1 };
    map<int, bufferlist> decoded;
    EXPECT_EQ(0, example.decode(set<int>(want_to_decode, want_to_decode+2),
                                degraded,
                                &decoded));
    EXPECT_EQ(2u, decoded.size());
    EXPECT_EQ(3u, decoded[0].length());
    EXPECT_EQ('A', decoded[0][0]);
    EXPECT_EQ('B', decoded[0][1]);
    EXPECT_EQ('C', decoded[0][2]);
    EXPECT_EQ('D', decoded[1][0]);
    EXPECT_EQ('E', decoded[1][1]);
  }
}

TEST(ErasureCodeExample, decode)
{
  ErasureCodeExample example;

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
  int want_to_encode[] = { 0, 1, 2 };
  map<int, bufferlist> encoded;
  EXPECT_EQ(0, example.encode(set<int>(want_to_encode, want_to_encode+3),
                              in,
                              &encoded));
  EXPECT_EQ(3u, encoded.size());

  // successfull decode
  bufferlist out;
  EXPECT_EQ(0, example.decode_concat(encoded, &out));
  bufferlist usable;
  usable.substr_of(out, 0, in.length());
  EXPECT_TRUE(usable == in);

  // cannot recover
  map<int, bufferlist> degraded;  
  degraded[0] = encoded[0];
  EXPECT_EQ(-ERANGE, example.decode_concat(degraded, &out));
}

TEST(ErasureCodeExample, create_ruleset)
{
  CrushWrapper *c = new CrushWrapper;
  c->create();
  c->set_type_name(2, "root");
  c->set_type_name(1, "host");
  c->set_type_name(0, "osd");

  int rootno;
  c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
		5, 0, NULL, NULL, &rootno);
  c->set_item_name(rootno, "default");

  map<string,string> loc;
  loc["root"] = "default";

  int num_host = 2;
  int num_osd = 5;
  int osd = 0;
  for (int h=0; h<num_host; ++h) {
    loc["host"] = string("host-") + stringify(h);
    for (int o=0; o<num_osd; ++o, ++osd) {
      c->insert_item(g_ceph_context, osd, 1.0, string("osd.") + stringify(osd), loc);
    }
  }

  stringstream ss;
  ErasureCodeExample example;
  EXPECT_EQ(0, example.create_ruleset("myrule", *c, &ss));
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  char *CEPH_LIB = getenv("CEPH_LIB");
  g_conf->set_val("erasure_code_dir", CEPH_LIB, false, false);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; 
 *   make -j4 && 
 *   make unittest_erasure_code_example && 
 *   valgrind  --leak-check=full --tool=memcheck \
 *      ./unittest_erasure_code_example --gtest_filter=*.* \
 *      --log-to-stderr=true --debug-osd=20
 * "
 * End:
 */

