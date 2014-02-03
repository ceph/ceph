// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
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
#include "global/global_init.h"
#include "osd/ErasureCodePluginPyramid/ErasureCodePyramid.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "gtest/gtest.h"

TEST(ErasureCodeTest, encode_decode)
{
  ErasureCodePyramid pyramid;
  map<std::string,std::string> parameters;
  const char *description =
    "["
    "    { \"erasure-code-plugin\": \"jerasure\","
    "      \"erasure-code-technique\": \"cauchy_good\","
    "      \"erasure-code-k\": \"12\","
    "      \"erasure-code-m\": \"6\","
    "      \"mapping\": \"0000^^-0000^^-0000^^-\","
    "    },"
    "    { \"erasure-code-plugin\": \"jerasure\","
    "      \"erasure-code-technique\": \"cauchy_good\","
    "      \"erasure-code-k\": \"6\","
    "      \"erasure-code-m\": \"1\","
    "      \"type\": \"datacenter\","
    "      \"size\": 3,"
    "      \"mapping\": \"000000^111111^222222^\","
    "    },"
    "]";

  parameters["erasure-code-directory"] = ".libs";
  parameters["erasure-code-pyramid"] = description;
  EXPECT_EQ(0, pyramid.init(parameters, &cerr));
  unsigned int data_chunk_count = pyramid.get_data_chunk_count();
  unsigned int chunk_size = pyramid.get_chunk_size(1);
  EXPECT_EQ(chunk_size, pyramid.get_chunk_size(data_chunk_count * chunk_size));
  string payload;
  for (unsigned int chunk = 0; chunk < data_chunk_count; chunk++) {
    string str(chunk_size, 'A' + (char)chunk);
    payload += str;
  }
  bufferptr in_ptr(buffer::create_page_aligned(data_chunk_count * chunk_size));
  in_ptr.zero();
  in_ptr.set_length(0);
  in_ptr.append(payload.c_str(), payload.size());
  bufferlist in;
  in.push_front(in_ptr);
  set<int> want_to_encode;
  unsigned int chunk_count = pyramid.get_chunk_count();
  for (unsigned int i = 0; i < chunk_count; ++i)
    want_to_encode.insert(i);
  map<int, bufferlist> encoded;
  EXPECT_EQ(0, pyramid.encode(want_to_encode, in, &encoded));
  EXPECT_EQ(21u, encoded.size());

  EXPECT_EQ('A', encoded[0][0]);
  EXPECT_EQ('B', encoded[1][0]);
  EXPECT_EQ('C', encoded[2][0]);
  EXPECT_EQ('D', encoded[3][0]);

  EXPECT_EQ('E', encoded[7][0]);
  EXPECT_EQ('F', encoded[8][0]);
  EXPECT_EQ('G', encoded[9][0]);
  EXPECT_EQ('H', encoded[10][0]);

  EXPECT_EQ('I', encoded[14][0]);
  EXPECT_EQ('J', encoded[15][0]);
  EXPECT_EQ('K', encoded[16][0]);
  EXPECT_EQ('L', encoded[17][0]);

  unsigned length =  encoded[0].length();
  EXPECT_EQ(chunk_size, length);
  EXPECT_EQ(0, strncmp(encoded[0].c_str(), in.c_str(), length));
  EXPECT_EQ(0, strncmp(encoded[1].c_str(), in.c_str() + length,
		       in.length() - length));

  // all chunks are available
  {
    int want_to_decode[] = { 0, 1, 7, 14 };
    map<int, bufferlist> decoded;
    EXPECT_EQ(0, pyramid.decode(set<int>(want_to_decode, want_to_decode+4),
                                encoded,
                                &decoded));
    // always decode all, regardless of want_to_decode
    EXPECT_EQ(4u, decoded.size()); 

    EXPECT_EQ('A', decoded[0][0]);
    EXPECT_EQ('B', decoded[1][0]);

    EXPECT_EQ('E', decoded[7][0]);

    EXPECT_EQ('I', decoded[14][0]);

    EXPECT_EQ(length, decoded[0].length());
    EXPECT_EQ(0, strncmp(decoded[0].c_str(), in.c_str(), length));
    EXPECT_EQ(0, strncmp(decoded[1].c_str(), in.c_str() + length,
			 in.length() - length));
  }

  // two chunks are missing 
  {
    map<int, bufferlist> degraded = encoded;
    unsigned lost0 = 0;
    degraded.erase(lost0);
    unsigned lost7 = 7;
    degraded.erase(lost7);
    EXPECT_EQ(21u - 2u, degraded.size());
    unsigned int want_to_decode[] = { lost0, lost7 };
    map<int, bufferlist> decoded;
    EXPECT_EQ(0, pyramid.decode(set<int>(want_to_decode, want_to_decode+2),
                                degraded,
                                &decoded));
    EXPECT_EQ(2u, decoded.size()); 
    EXPECT_EQ(chunk_size, decoded[lost0].length());
    EXPECT_EQ(0, strncmp(decoded[lost0].c_str(), in.c_str(), chunk_size));
    EXPECT_EQ('E', decoded[7][0]);
    EXPECT_EQ(0, strncmp(decoded[lost7].c_str(), in.c_str() + 4*chunk_size, chunk_size));
  }
}

TEST(ErasureCodeTest, pyramid_decode)
{
  ErasureCodePyramid pyramid;
  map<std::string,std::string> parameters;
  const char *description =
    "["
    "    { \"erasure-code-plugin\": \"jerasure\","
    "      \"erasure-code-technique\": \"cauchy_good\","
    "      \"erasure-code-k\": \"4\","
    "      \"erasure-code-m\": \"2\","
    "      \"mapping\": \"00^-00^-\","
    "    },"
    "    { \"erasure-code-plugin\": \"jerasure\","
    "      \"erasure-code-technique\": \"cauchy_good\","
    "      \"erasure-code-k\": \"3\","
    "      \"erasure-code-m\": \"1\","
    "      \"type\": \"datacenter\","
    "      \"size\": 2,"
    "      \"mapping\": \"000^000^\","
    "    },"
    "]";

  parameters["erasure-code-directory"] = ".libs";
  parameters["erasure-code-pyramid"] = description;
  EXPECT_EQ(0, pyramid.init(parameters, &cerr));
  unsigned int data_chunk_count = pyramid.get_data_chunk_count();
  unsigned int chunk_size = pyramid.get_chunk_size(1);
  EXPECT_EQ(chunk_size, pyramid.get_chunk_size(data_chunk_count * chunk_size));
  string payload;
  for (unsigned int chunk = 0; chunk < data_chunk_count; chunk++) {
    string str(chunk_size, 'A' + (char)chunk);
    payload += str;
  }
  bufferptr in_ptr(buffer::create_page_aligned(data_chunk_count * chunk_size));
  in_ptr.zero();
  in_ptr.set_length(0);
  in_ptr.append(payload.c_str(), payload.size());
  bufferlist in;
  in.push_front(in_ptr);
  set<int> want_to_encode;
  unsigned int chunk_count = pyramid.get_chunk_count();
  for (unsigned int i = 0; i < chunk_count; ++i)
    want_to_encode.insert(i);
  map<int, bufferlist> encoded;
  EXPECT_EQ(0, pyramid.encode(want_to_encode, in, &encoded));
  EXPECT_EQ(8u, encoded.size());

  EXPECT_EQ('A', encoded[0][0]);
  EXPECT_EQ('B', encoded[1][0]);

  EXPECT_EQ('C', encoded[4][0]);
  EXPECT_EQ('D', encoded[5][0]);

  unsigned length =  encoded[0].length();
  EXPECT_EQ(chunk_size, length);
  EXPECT_EQ(0, strncmp(encoded[0].c_str(), in.c_str(), length));
  EXPECT_EQ(0, strncmp(encoded[1].c_str(), in.c_str() + length,
		       in.length() - length));

  // three chunks are missing and cannot be recovered
  {
    map<int, bufferlist> degraded;
    set<int> want_to_decode;
    want_to_decode.insert(0);
    map<int, bufferlist> decoded;
    EXPECT_EQ(-EIO, pyramid.decode(want_to_decode,
				   degraded,
				   &decoded));
  }
  // three chunks are missing 
  {
    map<int, bufferlist> degraded;
    degraded[0] = encoded[0];
    // 1 erased
    degraded[2] = encoded[2];
    degraded[3] = encoded[3];
    // 4 & 5 erased
    degraded[6] = encoded[6];
    degraded[7] = encoded[7];
    set<int> want_to_decode;
    want_to_decode.insert(1); // can be recovered locally
    want_to_decode.insert(4); // need global recovery
    want_to_decode.insert(5); // need global recovery
    map<int, bufferlist> decoded;
    EXPECT_EQ(0, pyramid.decode(want_to_decode, degraded, &decoded));
    EXPECT_EQ(3u, decoded.size()); 
  }
}

TEST(ErasureCodeTest, sanity_checks)
{
  map<std::string,std::string> parameters;

  parameters["erasure-code-directory"] = ".libs";
  {
    ErasureCodePyramid pyramid;
    stringstream ss;
    EXPECT_EQ(ERROR_PYRAMID_DESCRIPTION, pyramid.init(parameters, &ss));
    EXPECT_NE(string::npos, ss.str().find("erasure-code-pyramid"));
  }
  parameters["erasure-code-pyramid"] = "{";
  {
    ErasureCodePyramid pyramid;
    stringstream ss;
    EXPECT_EQ(ERROR_PYRAMID_PARSE_JSON, pyramid.init(parameters, &ss));
    EXPECT_NE(string::npos, ss.str().find("failed to parse"));
  }
  parameters["erasure-code-pyramid"] = "0";
  {
    ErasureCodePyramid pyramid;
    stringstream ss;
    EXPECT_EQ(ERROR_PYRAMID_ARRAY, pyramid.init(parameters, &ss));
    EXPECT_NE(string::npos, ss.str().find("JSON array"));
  }
  parameters["erasure-code-pyramid"] = "[0]";
  {
    ErasureCodePyramid pyramid;
    stringstream ss;
    EXPECT_EQ(ERROR_PYRAMID_OBJECT, pyramid.init(parameters, &ss));
    EXPECT_NE(string::npos, ss.str().find("JSON object"));
  }
  parameters["erasure-code-pyramid"] = "[{\"size\":\"XXX\"}]";
  {
    ErasureCodePyramid pyramid;
    stringstream ss;
    EXPECT_EQ(ERROR_PYRAMID_INT, pyramid.init(parameters, &ss));
    EXPECT_NE(string::npos, ss.str().find("instead of int"));
  }
  parameters["erasure-code-pyramid"] = "[{\"field\":0}]";
  {
    ErasureCodePyramid pyramid;
    stringstream ss;
    EXPECT_EQ(ERROR_PYRAMID_STR, pyramid.init(parameters, &ss));
    EXPECT_NE(string::npos, ss.str().find("instead of string"));
  }
  parameters["erasure-code-pyramid"] = "[{}]";
  {
    ErasureCodePyramid pyramid;
    stringstream ss;
    EXPECT_EQ(ERROR_PYRAMID_FIRST_MAPPING, pyramid.init(parameters, &ss));
    EXPECT_NE(string::npos, ss.str().find("mapping is missing"));
  }
  parameters["erasure-code-pyramid"] = "[{\"mapping\": \"A\"}]";
  {
    ErasureCodePyramid pyramid;
    stringstream ss;
    EXPECT_EQ(ERROR_PYRAMID_PLUGIN, pyramid.init(parameters, &ss));
    EXPECT_NE(string::npos, ss.str().find("erasure-code-plugin"));
  }
  parameters["erasure-code-pyramid"] = 
    "[{\"mapping\": \"A\",\"erasure-code-plugin\":\"p\"},{}]";
  {
    ErasureCodePyramid pyramid;
    stringstream ss;
    EXPECT_EQ(ERROR_PYRAMID_MAPPING, pyramid.init(parameters, &ss));
    EXPECT_NE(string::npos, ss.str().find("mapping is missing"));
  }
  parameters["erasure-code-pyramid"] = 
    "["
    "{\"mapping\": \"A\",\"erasure-code-plugin\":\"p\"},"
    "{\"mapping\": \"ABCD\",\"erasure-code-plugin\":\"p\"},"
    "]";
  {
    ErasureCodePyramid pyramid;
    stringstream ss;
    EXPECT_EQ(ERROR_PYRAMID_MAPPING_SIZE, pyramid.init(parameters, &ss));
    EXPECT_NE(string::npos, ss.str().find("same size"));
  }
  parameters["erasure-code-pyramid"] = 
    "["
    "    { \"erasure-code-plugin\": \"jerasure\","
    "      \"erasure-code-technique\": \"cauchy_good\","
    "      \"erasure-code-k\": \"4\","
    "      \"erasure-code-m\": \"2\","
    "      \"mapping\": \"00^-00^-\","
    "    },"
    "    { \"erasure-code-plugin\": \"jerasure\","
    "      \"erasure-code-technique\": \"cauchy_good\","
    "      \"erasure-code-k\": \"3\","
    "      \"erasure-code-m\": \"1\","
    "      \"type\": \"datacenter\","
    "      \"size\": 2,"
    "      \"mapping\": \"0--^000^\"," // wrong
    "    },"
    "]";
  {
    ErasureCodePyramid pyramid;
    stringstream ss;
    EXPECT_EQ(ERROR_PYRAMID_COUNT_CONSTRAINT, pyramid.init(parameters, &ss));
    EXPECT_NE(string::npos, ss.str().find("count is different"));
  }
}

TEST(ErasureCodeTest, minimum_to_decode)
{
  ErasureCodePyramid pyramid;
  map<std::string,std::string> parameters;
  const char *description =
    "["
    "    { \"erasure-code-plugin\": \"jerasure\","
    "      \"erasure-code-technique\": \"cauchy_good\","
    "      \"erasure-code-k\": \"4\","
    "      \"erasure-code-m\": \"2\","
    "      \"mapping\": \"00^-00^-\","
    "    },"
    "    { \"erasure-code-plugin\": \"jerasure\","
    "      \"erasure-code-technique\": \"cauchy_good\","
    "      \"erasure-code-k\": \"3\","
    "      \"erasure-code-m\": \"1\","
    "      \"type\": \"datacenter\","
    "      \"size\": 2,"
    "      \"mapping\": \"000^000^\","
    "    },"
    "]";

  parameters["erasure-code-directory"] = ".libs";
  parameters["erasure-code-pyramid"] = description;
  EXPECT_EQ(0, pyramid.init(parameters, &cerr));
  {
    set<int> want_to_read;
    want_to_read.insert(0);
    set<int> available;
    available.insert(0);
    set<int> minimum;
    EXPECT_EQ(0, pyramid.minimum_to_decode(want_to_read,
					   available,
					   &minimum));
    EXPECT_EQ(1U, minimum.size());
  }
  {
    set<int> want_to_read;
    want_to_read.insert(1);
    set<int> available;
    available.insert(0);
    // 1 erased
    available.insert(2);
    available.insert(3);
    available.insert(4);
    available.insert(5);
    available.insert(6);
    available.insert(7);
    set<int> minimum;
    EXPECT_EQ(0, pyramid.minimum_to_decode(want_to_read,
					   available,
					   &minimum));
    EXPECT_EQ(3U, minimum.size());
    EXPECT_TRUE(minimum.find(0) != minimum.end());
    EXPECT_TRUE(minimum.find(2) != minimum.end());
    EXPECT_TRUE(minimum.find(3) != minimum.end());
  }
  {
    set<int> want_to_read;
    want_to_read.insert(1);
    set<int> available;
    available.insert(0);
    // 1 erased
    // 2 erased
    available.insert(3);
    available.insert(4);
    available.insert(5);
    available.insert(6);
    available.insert(7);
    set<int> minimum;
    EXPECT_EQ(0, pyramid.minimum_to_decode(want_to_read,
					   available,
					   &minimum));
    EXPECT_EQ(4U, minimum.size());
    EXPECT_TRUE(minimum.find(0) != minimum.end());
    EXPECT_TRUE(minimum.find(4) != minimum.end());
    EXPECT_TRUE(minimum.find(5) != minimum.end());
    EXPECT_TRUE(minimum.find(6) != minimum.end());
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
 * compile-command: "cd ../../.. ; make -j4 && 
 *   make unittest_erasure_code_pyramid && 
 *   valgrind --tool=memcheck --leak-check=full \
 *      ./unittest_erasure_code_pyramid \
 *      --gtest_filter=*.* --log-to-stderr=true --debug-osd=20"
 * End:
 */
