// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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
#include "ErasureCodeExample.h"
#include "global/global_context.h"
#include "gtest/gtest.h"

IGNORE_DEPRECATED

using namespace std;

TEST(ErasureCodeExample, chunk_size)
{
  ErasureCodeExample example;
  EXPECT_EQ(3u, example.get_chunk_count());
  EXPECT_EQ(11u, example.get_chunk_size(20));
}

TEST(ErasureCodeExample, minimum_to_decode)
{
  ErasureCodeExample example;
  shard_id_set  available_chunks;
  shard_id_set  want_to_read;
  want_to_read.insert(shard_id_t(1));
  {
    shard_id_set  minimum;
    EXPECT_EQ(-EIO, example._minimum_to_decode(want_to_read,
                                              available_chunks,
                                              &minimum));
  }
  available_chunks.insert(shard_id_t(0));
  available_chunks.insert(shard_id_t(2));
  {
    shard_id_set  minimum;
    EXPECT_EQ(0, example._minimum_to_decode(want_to_read,
					    available_chunks,
					    &minimum));
    EXPECT_EQ(available_chunks, minimum);
    EXPECT_EQ(2u, minimum.size());
    EXPECT_EQ(1u, minimum.count(shard_id_t(0)));
    EXPECT_EQ(1u, minimum.count(shard_id_t(2)));
  }
  {
    shard_id_set  minimum;
    available_chunks.insert(shard_id_t(1));
    EXPECT_EQ(0, example._minimum_to_decode(want_to_read,
					    available_chunks,
					    &minimum));
    EXPECT_EQ(1u, minimum.size());
    EXPECT_EQ(1u, minimum.count(shard_id_t(1)));
  }
}

TEST(ErasureCodeExample, minimum_to_decode_with_cost)
{
  ErasureCodeExample example;
  shard_id_map<int> available(example.get_chunk_count());
  shard_id_set  want_to_read;
  want_to_read.insert(shard_id_t(1));
  {
    shard_id_set  minimum;
    EXPECT_EQ(-EIO, example.minimum_to_decode_with_cost(want_to_read,
							available,
							&minimum));
  }
  available[shard_id_t(0)] = 1;
  available[shard_id_t(2)] = 1;
  {
    shard_id_set  minimum;
    EXPECT_EQ(0, example.minimum_to_decode_with_cost(want_to_read,
						     available,
						     &minimum));
    EXPECT_EQ(2u, minimum.size());
    EXPECT_EQ(1u, minimum.count(shard_id_t(0)));
    EXPECT_EQ(1u, minimum.count(shard_id_t(2)));
  }
  {
    shard_id_set  minimum;
    available[shard_id_t(1)] = 1;
    EXPECT_EQ(0, example.minimum_to_decode_with_cost(want_to_read,
						     available,
						     &minimum));
    EXPECT_EQ(1u, minimum.size());
    EXPECT_EQ(1u, minimum.count(shard_id_t(1)));
  }
  {
    shard_id_set  minimum;
    available[shard_id_t(1)] = 2;
    EXPECT_EQ(0, example.minimum_to_decode_with_cost(want_to_read,
						     available,
						     &minimum));
    EXPECT_EQ(2u, minimum.size());
    EXPECT_EQ(1u, minimum.count(shard_id_t(0)));
    EXPECT_EQ(1u, minimum.count(shard_id_t(2)));
  }
}

TEST(ErasureCodeExample, encode_decode)
{
  ErasureCodeExample example;

  bufferlist in;
  in.append("ABCDE");
  shard_id_set  want_to_encode;
  for(unsigned int i = 0; i < example.get_chunk_count(); i++)
    want_to_encode.insert(shard_id_t(i));
  shard_id_map<bufferlist> encoded(example.get_chunk_count());
  EXPECT_EQ(0, example.encode(want_to_encode, in, &encoded));
  EXPECT_EQ(example.get_chunk_count(), encoded.size());
  EXPECT_EQ(example.get_chunk_size(in.length()), encoded[shard_id_t(0)].length());
  EXPECT_EQ('A', encoded[shard_id_t(0)][0]);
  EXPECT_EQ('B', encoded[shard_id_t(0)][1]);
  EXPECT_EQ('C', encoded[shard_id_t(0)][2]);
  EXPECT_EQ('D', encoded[shard_id_t(1)][0]);
  EXPECT_EQ('E', encoded[shard_id_t(1)][1]);
  EXPECT_EQ('A'^'D', encoded[shard_id_t(2)][0]);
  EXPECT_EQ('B'^'E', encoded[shard_id_t(2)][1]);
  EXPECT_EQ('C'^0, encoded[shard_id_t(2)][2]);

  // all chunks are available
  {
    int want_to_decode[] = { 0, 1 };
    shard_id_map<bufferlist> decoded(example.get_chunk_count());
    EXPECT_EQ(0, example._decode(shard_id_set (want_to_decode, want_to_decode+2),
				 encoded,
				 &decoded));
    EXPECT_EQ(2u, decoded.size());
    EXPECT_EQ(3u, decoded[shard_id_t(0)].length());
    EXPECT_EQ('A', decoded[shard_id_t(0)][0]);
    EXPECT_EQ('B', decoded[shard_id_t(0)][1]);
    EXPECT_EQ('C', decoded[shard_id_t(0)][2]);
    EXPECT_EQ('D', decoded[shard_id_t(1)][0]);
    EXPECT_EQ('E', decoded[shard_id_t(1)][1]);
  }

  // one chunk is missing
  {
    shard_id_map<bufferlist> degraded = encoded;
    degraded.erase(shard_id_t(0));
    EXPECT_EQ(2u, degraded.size());
    int want_to_decode[] = { 0, 1 };
    shard_id_map<bufferlist> decoded(example.get_chunk_count());
    EXPECT_EQ(0, example._decode(shard_id_set (want_to_decode, want_to_decode+2),
				 degraded,
				 &decoded));
    EXPECT_EQ(2u, decoded.size());
    EXPECT_EQ(3u, decoded[shard_id_t(0)].length());
    EXPECT_EQ('A', decoded[shard_id_t(0)][0]);
    EXPECT_EQ('B', decoded[shard_id_t(0)][1]);
    EXPECT_EQ('C', decoded[shard_id_t(0)][2]);
    EXPECT_EQ('D', decoded[shard_id_t(1)][0]);
    EXPECT_EQ('E', decoded[shard_id_t(1)][1]);
  }
}

IGNORE_DEPRECATED
TEST(ErasureCodeExample, decode_legacy)
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
  in.push_back(in_ptr);
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
  EXPECT_EQ(2u*encoded[0].length(), out.length());
  usable.substr_of(out, 0, in.length());
  EXPECT_TRUE(usable == in);

  // partial chunk decode
  map<int, bufferlist> partial_decode = encoded;
  set<int> partial_want_to_read{want_to_encode, want_to_encode+1};
  EXPECT_EQ(1u, partial_want_to_read.size());
  out.clear();
  EXPECT_EQ(0, example.decode_concat(partial_want_to_read,
				     partial_decode,
				     &out));
  EXPECT_EQ(out.length(), encoded[0].length());

  // partial degraded chunk decode
  partial_decode = encoded;
  partial_decode.erase(0);
  EXPECT_EQ(1, partial_want_to_read.size());
  out.clear();
  EXPECT_EQ(0, example.decode_concat(partial_want_to_read,
				     partial_decode,
				     &out));
  EXPECT_EQ(out.length(), encoded[0].length());

  // cannot recover
  map<int, bufferlist> degraded;
  degraded[2] = encoded[2];
  EXPECT_EQ(-ERANGE, example.decode_concat(degraded, &out));
}
END_IGNORE_DEPRECATED

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
  in.push_back(in_ptr);
  int want_to_encode[] = { 0, 1, 2 };
  shard_id_map<bufferlist> encoded(example.get_chunk_count());
  EXPECT_EQ(0, example.encode(shard_id_set(want_to_encode, want_to_encode+3),
                              in,
                              &encoded));
  EXPECT_EQ(3u, encoded.size());

  // successful decode
  bufferlist out;
  shard_id_t shard0(0);
  shard_id_t shard1(1);
  encoded.erase(shard0);
  shard_id_map<bufferlist> decoded(example.get_chunk_count());
  EXPECT_EQ(0, example.decode(shard_id_set{shard0},
              encoded, &decoded, 0));
  bufferlist usable;
  EXPECT_EQ(decoded[shard0].length(), encoded[shard1].length());
}

TEST(ErasureCodeExample, create_rule)
{
  std::unique_ptr<CrushWrapper> c = std::make_unique<CrushWrapper>();
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
  EXPECT_EQ(0, example.create_rule("myrule", *c, &ss));
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

END_IGNORE_DEPRECATED
