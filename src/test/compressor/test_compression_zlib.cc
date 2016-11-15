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
#include <stdlib.h>
#include <string.h>
#include "compressor/zlib/CompressionZlib.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "common/config.h"
#include "test/unit.h"

TEST(CompressionZlib, compress_decompress)
{
  CompressionZlib sp;
  EXPECT_STREQ(sp.get_method_name(), "zlib");
  const char* test = "This is test text";
  int len = strlen(test);
  bufferlist in, out;
  in.append(test, len);
  int res = sp.compress(in, out);
  EXPECT_EQ(res, 0);
  bufferlist after;
  res = sp.decompress(out, after);
  EXPECT_EQ(res, 0);
  bufferlist exp;
  exp.append(test);
  EXPECT_TRUE(exp.contents_equal(after));
}

TEST(CompressionZlib, compress_decompress_chunk)
{
  CompressionZlib sp;
  EXPECT_STREQ(sp.get_method_name(), "zlib");
  const char* test = "This is test text";
  buffer::ptr test2 ("1234567890", 10);
  int len = strlen(test);
  bufferlist in, out;
  in.append(test, len);
  in.append(test2);
  int res = sp.compress(in, out);
  EXPECT_EQ(res, 0);
  bufferlist after;
  res = sp.decompress(out, after);
  EXPECT_EQ(res, 0);
  bufferlist exp;
  exp.append("This is test text1234567890");
  EXPECT_TRUE(exp.contents_equal(after));
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 && 
 *   make unittest_compression_zlib && 
 *   valgrind --tool=memcheck \
 *      ./unittest_compression_zlib \
 *      --gtest_filter=*.* --log-to-stderr=true --debug-osd=20"
 * End:
 */
