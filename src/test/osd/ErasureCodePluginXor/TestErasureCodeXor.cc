// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2014 CERN/Switzerland
 *
 * Author: Andreas-Joachim Peters <andreas.joachim.peters@gmail.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

// -----------------------------------------------------------------------------

#include "global/global_init.h"
#include "osd/ErasureCodePluginXor/ErasureCodeXor.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "gtest/gtest.h"

// -----------------------------------------------------------------------------

#include <errno.h>

// -----------------------------------------------------------------------------

TEST (ErasureCodeXorTest, encode_decode)
{
  ErasureCodeXor XOR;
  map<std::string, std::string> parameters;

  parameters["erasure-code-directory"] = ".libs";
  parameters["erasure-code-k"] = "4";
  XOR.init(parameters);
  unsigned int data_chunk_count = XOR.get_data_chunk_count();
  unsigned int chunk_size = XOR.get_chunk_size(1);
  EXPECT_EQ(chunk_size, XOR.get_chunk_size(data_chunk_count * chunk_size));
  string payload;
  for (unsigned int chunk = 0; chunk < data_chunk_count; chunk++) {
    string str(chunk_size, 'A' + (char) chunk);
    payload += str;
  }
  bufferptr in_ptr(buffer::create_page_aligned(data_chunk_count * chunk_size));
  in_ptr.zero();
  in_ptr.set_length(0);
  in_ptr.append(payload.c_str(), payload.size());
  bufferlist in;
  in.push_front(in_ptr);
  set<int> want_to_encode;
  unsigned int chunk_count = XOR.get_chunk_count();
  for (unsigned int i = 0; i < chunk_count; ++i)
    want_to_encode.insert(i);
  map<int, bufferlist> encoded;
  EXPECT_EQ(0, XOR.encode(want_to_encode, in, &encoded));
  EXPECT_EQ(5u, encoded.size());
  EXPECT_EQ('A', encoded[0][0]);
  EXPECT_EQ('B', encoded[1][0]);
  EXPECT_EQ('C', encoded[2][0]);
  EXPECT_EQ('D', encoded[3][0]);

  unsigned length = encoded[0].length();
  EXPECT_EQ(chunk_size, length);
  EXPECT_EQ(0, strncmp(encoded[0].c_str(), in.c_str(), length));
  EXPECT_EQ(0, strncmp(encoded[1].c_str(), in.c_str() + length,
                       in.length() - length));

  //
  // If trying to read nothing, the minimum is empty.
  //
  {
    set<int> want_to_read;
    set<int> available_chunks;
    set<int> minimum;

    EXPECT_EQ(0, XOR.minimum_to_decode(want_to_read,
                                       available_chunks,
                                       &minimum));
    EXPECT_TRUE(minimum.empty());
  }
  //
  // There is no way to read a chunk if none are available.
  //
  {
    set<int> want_to_read;
    set<int> available_chunks;
    set<int> minimum;

    want_to_read.insert(0);

    EXPECT_EQ(-EIO, XOR.minimum_to_decode(want_to_read,
                                          available_chunks,
                                          &minimum));
  }
  //
  // Reading a subset of the available chunks is always possible.
  //
  {
    set<int> want_to_read;
    set<int> available_chunks;
    set<int> minimum;

    want_to_read.insert(0);
    available_chunks.insert(0);

    EXPECT_EQ(0, XOR.minimum_to_decode(want_to_read,
                                       available_chunks,
                                       &minimum));
    EXPECT_EQ(want_to_read, minimum);
  }
  //
  // There is no way to read a missing chunk if there is less than k
  // chunks available.
  //
  {
    set<int> want_to_read;
    set<int> available_chunks;
    set<int> minimum;

    want_to_read.insert(0);
    want_to_read.insert(1);
    available_chunks.insert(0);

    EXPECT_EQ(-EIO, XOR.minimum_to_decode(want_to_read,
                                          available_chunks,
                                          &minimum));
  }
  //
  // When a chunk is not available, we have to add the parity chunk
  //
  {
    set<int> want_to_read;
    set<int> available_chunks;
    set<int> minimum;

    want_to_read.insert(1);
    want_to_read.insert(3);
    available_chunks.insert(0);
    available_chunks.insert(2);
    available_chunks.insert(3);
    available_chunks.insert(4);

    EXPECT_EQ(0, XOR.minimum_to_decode(want_to_read,
                                       available_chunks,
                                       &minimum));
    EXPECT_EQ(4u, minimum.size());
    EXPECT_EQ(1u, minimum.count(4));
  }

  //
  // all chunks are available
  //
  {
    int want_to_decode[] = {0, 1};
    map<int, bufferlist> decoded;
    EXPECT_EQ(5u, encoded.size());
    EXPECT_EQ(0, XOR.decode(set<int>(want_to_decode, want_to_decode + 2),
                            encoded,
                            &decoded));
    EXPECT_EQ(2u, decoded.size());

    EXPECT_EQ('A', decoded[0][0]);
    EXPECT_EQ('B', decoded[1][0]);

    EXPECT_EQ(length, decoded[0].length());
    EXPECT_EQ(0, strncmp(decoded[0].c_str(), in.c_str(), length));
    EXPECT_EQ(0, strncmp(decoded[1].c_str(), in.c_str() + length,
                         in.length() - length));
    // check that decoding does not copy buffers
    EXPECT_EQ(true, (decoded[0].c_str() == in.c_str()));
  }

  //
  // one chunk is missing 
  //
  {
    map<int, bufferlist> degraded = encoded;
    unsigned lost0 = 0;
    degraded.erase(lost0);
    EXPECT_EQ(5u - 1u, degraded.size());
    unsigned int want_to_decode[] = {lost0};
    map<int, bufferlist> decoded;

    EXPECT_EQ(0, XOR.decode(set<int>(want_to_decode, want_to_decode + 1),
                            degraded,
                            &decoded));
    EXPECT_EQ(1u, decoded.size());
    EXPECT_EQ(chunk_size, decoded[lost0].length());
    EXPECT_EQ('A', decoded[0][0]);
    EXPECT_EQ(0, strncmp(decoded[lost0].c_str(), in.c_str(), chunk_size));
    // check that decoding recreated the lost buffer
    EXPECT_EQ(true, (decoded[lost0].c_str() != in.c_str()));
  }

  //
  // two chunks are missing 
  //
  {
    map<int, bufferlist> degraded = encoded;
    unsigned lost0 = 0;
    degraded.erase(lost0);
    unsigned lost2 = 2;
    degraded.erase(lost2);
    EXPECT_EQ(5u - 2u, degraded.size());
    unsigned int want_to_decode[] = {lost0};
    map<int, bufferlist> decoded;
    EXPECT_EQ(-EIO, XOR.decode(set<int>(want_to_decode, want_to_decode + 2),
                               degraded,
                               &decoded));
    // although the decoding fails there are pointers to buffers 
    // in the decoded map!!
    EXPECT_EQ(5u, decoded.size());
  }
}

// -----------------------------------------------------------------------------

int
main (int argc, char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **) argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

// -----------------------------------------------------------------------------

/* 
 * Local Variables:
 * compile-command: "cd ../../.. ; make -j4 && 
 *   make unittest_erasure_code_xor && 
 *   valgrind --tool=memcheck --leak-check=full \
 *      ./unittest_erasure_code_xor \
 *      --gtest_filter=*.* --log-to-stderr=true --debug-osd=20"
 * End:
 */
