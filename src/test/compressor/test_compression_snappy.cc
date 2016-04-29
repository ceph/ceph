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
#include <string.h>
#include <gtest/gtest.h>
#include "global/global_init.h"
#include "compressor/snappy/SnappyCompressor.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "common/config.h"

TEST(SnappyCompressor, compress_decompress)
{
  SnappyCompressor sp;
  EXPECT_EQ(sp.get_method_name(), "snappy");
  const char* test = "This is test text";
  int len = strlen(test);
  bufferlist in, out;
  in.append(test, len);
  int res = sp.compress(in, out);
  EXPECT_EQ(res, 0);
  bufferlist after;
  res = sp.decompress(out, after);
  EXPECT_EQ(res, 0);
}

TEST(SnappyCompressor, sharded_input_decompress)
{
  const size_t small_prefix_size=3;

  SnappyCompressor sp;
  EXPECT_EQ(sp.get_method_name(), "snappy");
  string test(128*1024,0);
  int len = test.size();
  bufferlist in, out;
  in.append(test.c_str(), len);
  int res = sp.compress(in, out);
  EXPECT_EQ(res, 0);
  EXPECT_GT(out.length(), small_prefix_size);
  
  bufferlist out2, tmp;
  tmp.substr_of(out, 0, small_prefix_size );
  out2.append( tmp );
  size_t left = out.length()-small_prefix_size;
  size_t offs = small_prefix_size;
  while( left > 0 ){
    size_t shard_size = MIN( 2048, left ); 
    tmp.substr_of(out, offs, shard_size );
    out2.append( tmp );
    left -= shard_size;
    offs += shard_size;
  }

  bufferlist after;
  res = sp.decompress(out2, after);
  EXPECT_EQ(res, 0);
}

int main(int argc, char **argv) {
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
 *   make unittest_compression_snappy && 
 *   valgrind --tool=memcheck \
 *      ./unittest_compression_snappy \
 *      --gtest_filter=*.* --log-to-stderr=true --debug-osd=20"
 * End:
 */
