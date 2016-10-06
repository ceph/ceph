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

class CompressionTest : public ::testing::Test,
			public ::testing::WithParamInterface<const char*> {
public:
  std::string plugin;
  CompressorRef compressor;

  CompressionTest() {
    plugin = GetParam();
    size_t pos = plugin.find('/');
    if (pos != std::string::npos) {
      string isal = plugin.substr(pos + 1);
      plugin = plugin.substr(0, pos);
      if (isal == "isal") {
	g_conf->set_val("compressor_zlib_isal", "true");
	g_ceph_context->_conf->apply_changes(NULL);
      } else if (isal == "noisal") {
	g_conf->set_val("compressor_zlib_isal", "false");
	g_ceph_context->_conf->apply_changes(NULL);
      } else {
	assert(0 == "bad option");
      }
    }
    cout << "[plugin " << plugin << " (" << GetParam() << ")]" << std::endl;
  }

  void SetUp() {
    compressor = Compressor::create(g_ceph_context, plugin);
    ASSERT_TRUE(compressor);
  }
  void TearDown() {
    compressor.reset();
  }
};

TEST_P(CompressionTest, load_plugin)
{
}

TEST_P(CompressionTest, small_round_trip)
{
  bufferlist orig;
  orig.append("This is a short string.  There are many strings like it but this one is mine.");
  bufferlist compressed;
  int r = compressor->compress(orig, compressed);
  ASSERT_EQ(0, r);
  bufferlist decompressed;
  r = compressor->decompress(compressed, decompressed);
  ASSERT_EQ(0, r);
  ASSERT_EQ(decompressed.length(), orig.length());
  ASSERT_TRUE(decompressed.contents_equal(orig));
  cout << "orig " << orig.length() << " compressed " << compressed.length()
       << " with " << GetParam() << std::endl;
}

TEST_P(CompressionTest, big_round_trip_repeated)
{
  unsigned len = 1048576 * 4;
  bufferlist orig;
  while (orig.length() < len) {
    orig.append("This is a short string.  There are many strings like it but this one is mine.");
  }
  bufferlist compressed;
  int r = compressor->compress(orig, compressed);
  ASSERT_EQ(0, r);
  bufferlist decompressed;
  r = compressor->decompress(compressed, decompressed);
  ASSERT_EQ(0, r);
  ASSERT_EQ(decompressed.length(), orig.length());
  ASSERT_TRUE(decompressed.contents_equal(orig));
  cout << "orig " << orig.length() << " compressed " << compressed.length()
       << " with " << GetParam() << std::endl;
}

TEST_P(CompressionTest, big_round_trip_randomish)
{
  unsigned len = 1048576 * 100;//269;
  bufferlist orig;
  const char *alphabet = "abcdefghijklmnopqrstuvwxyz";
  if (false) {
    while (orig.length() < len) {
      orig.append(alphabet[rand() % 10]);
    }
  } else {
    bufferptr bp(len);
    char *p = bp.c_str();
    for (unsigned i=0; i<len; ++i) {
      p[i] = alphabet[rand() % 10];
    }
    orig.append(bp);
  }
  bufferlist compressed;
  int r = compressor->compress(orig, compressed);
  ASSERT_EQ(0, r);
  bufferlist decompressed;
  r = compressor->decompress(compressed, decompressed);
  ASSERT_EQ(0, r);
  ASSERT_EQ(decompressed.length(), orig.length());
  ASSERT_TRUE(decompressed.contents_equal(orig));
  cout << "orig " << orig.length() << " compressed " << compressed.length()
       << " with " << GetParam() << std::endl;
}

#if 0
TEST_P(CompressionTest, big_round_trip_file)
{
  bufferlist orig;
  int fd = ::open("bin/ceph-osd", O_RDONLY);
  struct stat st;
  ::fstat(fd, &st);
  orig.read_fd(fd, st.st_size);

  bufferlist compressed;
  int r = compressor->compress(orig, compressed);
  ASSERT_EQ(0, r);
  bufferlist decompressed;
  r = compressor->decompress(compressed, decompressed);
  ASSERT_EQ(0, r);
  ASSERT_EQ(decompressed.length(), orig.length());
  ASSERT_TRUE(decompressed.contents_equal(orig));
  cout << "orig " << orig.length() << " compressed " << compressed.length()
       << " with " << GetParam() << std::endl;
}
#endif


INSTANTIATE_TEST_CASE_P(
  Compression,
  CompressionTest,
  ::testing::Values(
//    "zlib/isal",
    "zlib/noisal",
    "snappy"));

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  const char* env = getenv("CEPH_LIB");
  if (env)
    g_conf->set_val("plugin_dir", env, false, false);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
