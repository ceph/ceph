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

#include <iostream> // for std::cout

#include "gtest/gtest.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "compressor/Compressor.h"
#include "compressor/CompressionPlugin.h"
#include "global/global_context.h"
#include "osd/OSDMap.h"

using namespace std;

class CompressorTest : public ::testing::Test,
			public ::testing::WithParamInterface<const char*> {
public:
  std::string plugin;
  CompressorRef compressor;
  bool old_zlib_isal;

  CompressorTest() {
    // note for later
    old_zlib_isal = g_conf()->compressor_zlib_isal;

    plugin = GetParam();
    size_t pos = plugin.find('/');
    if (pos != std::string::npos) {
      string isal = plugin.substr(pos + 1);
      plugin = plugin.substr(0, pos);
      if (isal == "isal") {
	g_conf().set_val("compressor_zlib_isal", "true");
	g_ceph_context->_conf.apply_changes(nullptr);
      } else if (isal == "noisal") {
	g_conf().set_val("compressor_zlib_isal", "false");
	g_ceph_context->_conf.apply_changes(nullptr);
      } else {
	ceph_abort_msg("bad option");
      }
    }
    cout << "[plugin " << plugin << " (" << GetParam() << ")]" << std::endl;
  }
  ~CompressorTest() override {
    g_conf().set_val("compressor_zlib_isal", old_zlib_isal ? "true" : "false");
    g_ceph_context->_conf.apply_changes(nullptr);
  }

  void SetUp() override {
    compressor = Compressor::create(g_ceph_context, plugin);
    ASSERT_TRUE(compressor);
  }
  void TearDown() override {
    compressor.reset();
  }
};

TEST_P(CompressorTest, load_plugin)
{
}

TEST_P(CompressorTest, small_round_trip)
{
  bufferlist orig;
  orig.append("This is a short string.  There are many strings like it but this one is mine.");
  bufferlist compressed;
  std::optional<int32_t> compressor_message;
  int r = compressor->compress(orig, compressed, compressor_message);
  ASSERT_EQ(0, r);
  bufferlist decompressed;
  r = compressor->decompress(compressed, decompressed, compressor_message);
  ASSERT_EQ(0, r);
  ASSERT_EQ(decompressed.length(), orig.length());
  ASSERT_TRUE(decompressed.contents_equal(orig));
  cout << "orig " << orig.length() << " compressed " << compressed.length()
       << " with " << GetParam() << std::endl;
}

TEST_P(CompressorTest, big_round_trip_repeated)
{
  unsigned len = 1048576 * 4;
  bufferlist orig;
  while (orig.length() < len) {
    orig.append("This is a short string.  There are many strings like it but this one is mine.");
  }
  bufferlist compressed;
  std::optional<int32_t> compressor_message;
  int r = compressor->compress(orig, compressed, compressor_message);
  ASSERT_EQ(0, r);
  bufferlist decompressed;
  r = compressor->decompress(compressed, decompressed, compressor_message);
  ASSERT_EQ(0, r);
  ASSERT_EQ(decompressed.length(), orig.length());
  ASSERT_TRUE(decompressed.contents_equal(orig));
  cout << "orig " << orig.length() << " compressed " << compressed.length()
       << " with " << GetParam() << std::endl;
}

TEST_P(CompressorTest, big_round_trip_randomish)
{
  unsigned len = 1048576 * 10;//269;
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
  std::optional<int32_t> compressor_message;
  int r = compressor->compress(orig, compressed, compressor_message);
  ASSERT_EQ(0, r);
  bufferlist decompressed;
  r = compressor->decompress(compressed, decompressed, compressor_message);
  ASSERT_EQ(0, r);
  ASSERT_EQ(decompressed.length(), orig.length());
  ASSERT_TRUE(decompressed.contents_equal(orig));
  cout << "orig " << orig.length() << " compressed " << compressed.length()
       << " with " << GetParam() << std::endl;
}

#if 0
TEST_P(CompressorTest, big_round_trip_file)
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


TEST_P(CompressorTest, round_trip_osdmap)
{
#include "osdmaps/osdmap.2982809.h"

  auto compressor = Compressor::create(g_ceph_context, plugin);
  bufferlist orig;
  orig.append((char*)osdmap_a, sizeof(osdmap_a));
  cout << "orig length " << orig.length() << std::endl;
  uint32_t size = 128*1024;
  OSDMap *o = new OSDMap;
  o->decode(orig);
  bufferlist fbl;
  o->encode(fbl, o->get_encoding_features() | CEPH_FEATURE_RESERVED);
  ASSERT_TRUE(fbl.contents_equal(orig));
  for (int j = 0; j < 3; j++) {
    bufferlist chunk;
    uint32_t l = std::min(size, fbl.length() - j*size);
    chunk.substr_of(fbl, j*size, l);
    //fbl.rebuild();
    bufferlist compressed;
    std::optional<int32_t> compressor_message;
    int r = compressor->compress(chunk, compressed, compressor_message);
    ASSERT_EQ(0, r);
    bufferlist decompressed;
    r = compressor->decompress(compressed, decompressed, compressor_message);
    ASSERT_EQ(0, r);
    ASSERT_EQ(decompressed.length(), chunk.length());
    if (!decompressed.contents_equal(chunk)) {
      cout << "FAILED, orig bl was\n" << fbl << std::endl;
      ASSERT_TRUE(decompressed.contents_equal(chunk));
    }
    cout << "chunk " << chunk.length()
	 << " compressed " << compressed.length()
	 << " decompressed " << decompressed.length()
	 << " with " << plugin << std::endl;
  }
  delete o;
}

TEST_P(CompressorTest, compress_decompress)
{
  const char* test = "This is test text";
  int res;
  int len = strlen(test);
  bufferlist in, out;
  bufferlist after;
  bufferlist exp;
  in.append(test, len);
  std::optional<int32_t> compressor_message;
  res = compressor->compress(in, out, compressor_message);
  EXPECT_EQ(res, 0);
  res = compressor->decompress(out, after, compressor_message);
  EXPECT_EQ(res, 0);
  exp.append(test);
  EXPECT_TRUE(exp.contents_equal(after));
  after.clear();
  size_t compressed_len = out.length();
  out.append_zero(12);
  auto it = out.cbegin();
  res = compressor->decompress(it, compressed_len, after, compressor_message);
  EXPECT_EQ(res, 0);
  EXPECT_TRUE(exp.contents_equal(after));

  //large block and non-begin iterator for continuous block
  std::string data;
  data.resize(0x10000 * 1);
  for(size_t i = 0; i < data.size(); i++)
    data[i] = i / 256;
  in.clear();
  out.clear();
  in.append(data);
  exp = in;
  res = compressor->compress(in, out, compressor_message);
  EXPECT_EQ(res, 0);
  compressed_len = out.length();
  out.append_zero(0x10000 - out.length());
  after.clear();
  out.c_str();
  bufferlist prefix;
  prefix.append(string("some prefix"));
  size_t prefix_len = prefix.length();
  prefix.claim_append(out);
  out.swap(prefix);
  it = out.cbegin();
  it += prefix_len;
  res = compressor->decompress(it, compressed_len, after, compressor_message);
  EXPECT_EQ(res, 0);
  EXPECT_TRUE(exp.contents_equal(after));
}

TEST_P(CompressorTest, sharded_input_decompress)
{
  const size_t small_prefix_size=3;

  string test(128*1024,0);
  int len = test.size();
  bufferlist in, out;
  in.append(test.c_str(), len);
  std::optional<int32_t> compressor_message;
  int res = compressor->compress(in, out, compressor_message);
  EXPECT_EQ(res, 0);
  EXPECT_GT(out.length(), small_prefix_size);

  bufferlist out2, tmp;
  tmp.substr_of(out, 0, small_prefix_size );
  out2.append( tmp );
  size_t left = out.length()-small_prefix_size;
  size_t offs = small_prefix_size;
  while( left > 0 ){
    size_t shard_size = std::min<size_t>(2048, left);
    tmp.substr_of(out, offs, shard_size );
    out2.append( tmp );
    left -= shard_size;
    offs += shard_size;
  }

  bufferlist after;
  res = compressor->decompress(out2, after, compressor_message);
  EXPECT_EQ(res, 0);
}

void test_compress(CompressorRef compressor, size_t size)
{
  char* data = (char*) malloc(size);
  for (size_t t = 0; t < size; t++) {
    data[t] = (t & 0xff) | (t >> 8);
  }
  bufferlist in;
  in.append(data, size);
  for (size_t t = 0; t < 10000; t++) {
    bufferlist out;
    std::optional<int32_t> compressor_message;
    int res = compressor->compress(in, out, compressor_message);
    EXPECT_EQ(res, 0);
  }
  free(data);
}

void test_decompress(CompressorRef compressor, size_t size)
{
  char* data = (char*) malloc(size);
  for (size_t t = 0; t < size; t++) {
    data[t] = (t & 0xff) | (t >> 8);
  }
  bufferlist in, out;
  in.append(data, size);
  std::optional<int32_t> compressor_message;
  int res = compressor->compress(in, out, compressor_message);
  EXPECT_EQ(res, 0);
  for (size_t t = 0; t < 10000; t++) {
    bufferlist out_dec;
    int res = compressor->decompress(out, out_dec, compressor_message);
    EXPECT_EQ(res, 0);
  }
  free(data);
}

TEST_P(CompressorTest, compress_1024)
{
  test_compress(compressor, 1024);
}

TEST_P(CompressorTest, compress_2048)
{
  test_compress(compressor, 2048);
}

TEST_P(CompressorTest, compress_4096)
{
  test_compress(compressor, 4096);
}

TEST_P(CompressorTest, compress_8192)
{
  test_compress(compressor, 8192);
}

TEST_P(CompressorTest, compress_16384)
{
  test_compress(compressor, 16384);
}

TEST_P(CompressorTest, decompress_1024)
{
  test_decompress(compressor, 1024);
}

TEST_P(CompressorTest, decompress_2048)
{
  test_decompress(compressor, 2048);
}

TEST_P(CompressorTest, decompress_4096)
{
  test_decompress(compressor, 4096);
}

TEST_P(CompressorTest, decompress_8192)
{
  test_decompress(compressor, 8192);
}

TEST_P(CompressorTest, decompress_16384)
{
  test_decompress(compressor, 16384);
}


INSTANTIATE_TEST_SUITE_P(
  Compressor,
  CompressorTest,
  ::testing::Values(
#ifdef HAVE_LZ4
    "lz4",
#endif
#if defined(__x86_64__) || defined(__aarch64__)
    "zlib/isal",
#endif
    "zlib/noisal",
    "snappy",
#ifdef HAVE_BROTLI
    "brotli",
#endif
    "zstd"));

#if defined(__x86_64__) || defined(__aarch64__)

TEST(ZlibCompressor, zlib_isal_compatibility)
{
  g_conf().set_val("compressor_zlib_isal", "true");
  g_ceph_context->_conf.apply_changes(nullptr);
  CompressorRef isal = Compressor::create(g_ceph_context, "zlib");
  if (!isal) {
    // skip the test if the plugin is not ready
    return;
  }
  g_conf().set_val("compressor_zlib_isal", "false");
  g_ceph_context->_conf.apply_changes(nullptr);
  CompressorRef zlib = Compressor::create(g_ceph_context, "zlib");
  char test[101];
  srand(time(0));
  for (int i=0; i<100; ++i)
    test[i] = 'a' + rand()%26;
  test[100] = '\0';
  int len = strlen(test);
  bufferlist in, out;
  in.append(test, len);
  // isal -> zlib
  std::optional<int32_t> compressor_message;
  int res = isal->compress(in, out, compressor_message);
  EXPECT_EQ(res, 0);
  bufferlist after;
  res = zlib->decompress(out, after, compressor_message);
  EXPECT_EQ(res, 0);
  bufferlist exp;
  exp.append(static_cast<char*>(test));
  EXPECT_TRUE(exp.contents_equal(after));
  after.clear();
  out.clear();
  exp.clear();
  // zlib -> isal
  res = zlib->compress(in, out, compressor_message);
  EXPECT_EQ(res, 0);
  res = isal->decompress(out, after, compressor_message);
  EXPECT_EQ(res, 0);
  exp.append(static_cast<char*>(test));
  EXPECT_TRUE(exp.contents_equal(after));
}
#endif

TEST(CompressionPlugin, all)
{
  CompressorRef compressor;
  PluginRegistry *reg = g_ceph_context->get_plugin_registry();
  EXPECT_TRUE(reg);
  CompressionPlugin *factory = dynamic_cast<CompressionPlugin*>(reg->get_with_load("compressor", "invalid"));
  EXPECT_FALSE(factory);
  factory = dynamic_cast<CompressionPlugin*>(reg->get_with_load("compressor", "example"));
  ASSERT_TRUE(factory);
  stringstream ss;
  EXPECT_EQ(0, factory->factory(&compressor, &ss));
  EXPECT_TRUE(compressor.get());
  {
    std::lock_guard l(reg->lock);
    EXPECT_EQ(-ENOENT, reg->remove("compressor", "does not exist"));
    EXPECT_EQ(0, reg->remove("compressor", "example"));
    EXPECT_EQ(0, reg->load("compressor", "example"));
  }
}

#if defined(__x86_64__) || defined(__aarch64__)

TEST(ZlibCompressor, isal_compress_zlib_decompress_random)
{
  g_conf().set_val("compressor_zlib_isal", "true");
  g_ceph_context->_conf.apply_changes(nullptr);
  CompressorRef isal = Compressor::create(g_ceph_context, "zlib");
  if (!isal) {
    // skip the test if the plugin is not ready
    return;
  }
  g_conf().set_val("compressor_zlib_isal", "false");
  g_ceph_context->_conf.apply_changes(nullptr);
  CompressorRef zlib = Compressor::create(g_ceph_context, "zlib");

  for (int cnt=0; cnt<100; cnt++)
  {
    srand(cnt + 1000);
    int log2 = (rand()%18) + 1;
    int size = (rand() % (1 << log2)) + 1;

    char test[size];
    for (int i=0; i<size; ++i)
      test[i] = rand()%256;
    bufferlist in, out;
    in.append(test, size);

    std::optional<int32_t> compressor_message;
    int res = isal->compress(in, out, compressor_message);
    EXPECT_EQ(res, 0);
    bufferlist after;
    res = zlib->decompress(out, after, compressor_message);
    EXPECT_EQ(res, 0);
    bufferlist exp;
    exp.append(test, size);
    EXPECT_TRUE(exp.contents_equal(after));
  }
}

TEST(ZlibCompressor, isal_compress_zlib_decompress_walk)
{
  g_conf().set_val("compressor_zlib_isal", "true");
  g_ceph_context->_conf.apply_changes(nullptr);
  CompressorRef isal = Compressor::create(g_ceph_context, "zlib");
  if (!isal) {
    // skip the test if the plugin is not ready
    return;
  }
  g_conf().set_val("compressor_zlib_isal", "false");
  g_ceph_context->_conf.apply_changes(nullptr);
  CompressorRef zlib = Compressor::create(g_ceph_context, "zlib");

  for (int cnt=0; cnt<100; cnt++)
  {
    srand(cnt + 1000);
    int log2 = (rand()%18) + 1;
    int size = (rand() % (1 << log2)) + 1;

    int range = 1;

    char test[size];
    test[0] = rand()%256;
    for (int i=1; i<size; ++i)
      test[i] = test[i-1] + rand()%(range*2+1) - range;
    bufferlist in, out;
    in.append(test, size);

    std::optional<int32_t> compressor_message;
    int res = isal->compress(in, out, compressor_message);
    EXPECT_EQ(res, 0);
    bufferlist after;
    res = zlib->decompress(out, after, compressor_message);
    EXPECT_EQ(res, 0);
    bufferlist exp;
    exp.append(test, size);
    EXPECT_TRUE(exp.contents_equal(after));
  }
}

#endif	// __x86_64__

#ifdef HAVE_QATZIP
TEST(QAT, enc_qat_dec_noqat) {
#ifdef HAVE_LZ4
  const char* alg_collection[] = {"zlib", "lz4", "snappy"}; 
#else
  const char* alg_collection[] = {"zlib", "snappy"}; 
#endif
  for (auto alg : alg_collection) {
    g_conf().set_val("qat_compressor_enabled", "true");
    CompressorRef q = Compressor::create(g_ceph_context, alg);
    g_conf().set_val("qat_compressor_enabled", "false");
    CompressorRef noq = Compressor::create(g_ceph_context, alg);

    // generate random buffer
    for (int cnt=0; cnt<100; cnt++) {
      srand(cnt + 1000);
      int log2 = (rand()%18) + 1;
      int size = (rand() % (1 << log2)) + 1;
  
      char test[size];
      for (int i=0; i<size; ++i)
        test[i] = rand()%256;
      bufferlist in, out;
      in.append(test, size);
  
      std::optional<int32_t> compressor_message;
      int res = q->compress(in, out, compressor_message);
      EXPECT_EQ(res, 0);
      bufferlist after;
      res = noq->decompress(out, after, compressor_message);
      EXPECT_EQ(res, 0);
      bufferlist exp;
      exp.append(test, size);
      EXPECT_TRUE(exp.contents_equal(after));
    }
  }
}

TEST(QAT, enc_noqat_dec_qat) {
#ifdef HAVE_LZ4
  const char* alg_collection[] = {"zlib", "lz4", "snappy"}; 
#else
  const char* alg_collection[] = {"zlib", "snappy"}; 
#endif
  for (auto alg : alg_collection) {
    g_conf().set_val("qat_compressor_enabled", "true");
    CompressorRef q = Compressor::create(g_ceph_context, alg);
    g_conf().set_val("qat_compressor_enabled", "false");
    CompressorRef noq = Compressor::create(g_ceph_context, alg);

    // generate random buffer
    for (int cnt=0; cnt<100; cnt++) {
      srand(cnt + 1000);
      int log2 = (rand()%18) + 1;
      int size = (rand() % (1 << log2)) + 1;
  
      char test[size];
      for (int i=0; i<size; ++i)
        test[i] = rand()%256;
      bufferlist in, out;
      in.append(test, size);
  
      std::optional<int32_t> compressor_message;
      int res = noq->compress(in, out, compressor_message);
      EXPECT_EQ(res, 0);
      bufferlist after;
      res = q->decompress(out, after, compressor_message);
      EXPECT_EQ(res, 0);
      bufferlist exp;
      exp.append(test, size);
      EXPECT_TRUE(exp.contents_equal(after));
    }
  }
}

#endif	// HAVE_QATZIP

#ifdef HAVE_UADK
TEST(UADK, enc_uadk_dec_nouadk) {
  //reserve for more algs in the future
  const char* alg_collection[] = {"zlib"};

  for (auto alg : alg_collection) {
    g_conf().set_val("uadk_compressor_enabled", "true");
    g_conf().set_val("compressor_zlib_winsize", "15");
    g_ceph_context->_conf.apply_changes(nullptr);
    CompressorRef hw = Compressor::create(g_ceph_context, alg);
    if (hw == NULL) 
      return;

    g_conf().set_val("uadk_compressor_enabled", "false");
    g_conf().set_val("compressor_zlib_winsize", "15");
    g_ceph_context->_conf.apply_changes(nullptr);
    CompressorRef sw = Compressor::create(g_ceph_context, alg);

    //generate random buffer
    for (int cnt = 0; cnt < 100; cnt++) {
      srand(cnt + 1000);
      int log2 = (rand()%18) + 1;
      int size = (rand() % (1 << log2)) + 1;

      char test[size];
      for (int i = 0; i < size; ++i)
	        test[i] = rand()%256;
      bufferlist in, out;
      in.append(test, size);

      std::optional<int32_t> compressor_message;
      int res = hw->compress(in, out, compressor_message);
      EXPECT_EQ(res, 0);
      bufferlist after;
      res = sw->decompress(out, after, compressor_message);
      EXPECT_EQ(res, 0);
      bufferlist exp;
      exp.append(test, size);
      EXPECT_TRUE(exp.contents_equal(after));
    }
  }
}

TEST(UADK, enc_nouadk_dec_uadk) {
  const char* alg_collection[] = {"zlib"};

  for (auto alg : alg_collection) {
    g_conf().set_val("uadk_compressor_enabled", "true");
    g_conf().set_val("compressor_zlib_winsize", "15");
    g_ceph_context->_conf.apply_changes(nullptr);
    CompressorRef hw = Compressor::create(g_ceph_context, alg);
    if (hw == NULL) 
      return;
    g_conf().set_val("uadk_compressor_enabled", "false");
    g_conf().set_val("compressor_zlib_winsize", "15");
    g_ceph_context->_conf.apply_changes(nullptr);
    CompressorRef sw = Compressor::create(g_ceph_context, alg);

    //generate random buffer
    for (int cnt = 0; cnt < 100; cnt++) {
      srand(cnt + 1000);
      int log2 = (rand()%18) +1;
      int size = (rand() % (1 << log2)) + 1;

      char test[size];
      for (int i = 0; i < size; ++i)
        test[i] = rand()%256;
      bufferlist in, out;
      in.append(test, size);

      std::optional<int32_t> compressor_message;
      int res = sw->compress(in, out, compressor_message);
      EXPECT_EQ(res, 0);
      bufferlist after;
      res = hw->decompress(out, after, compressor_message);
      EXPECT_EQ(res, 0);
      bufferlist exp;
      exp.append(test, size);
      EXPECT_TRUE(exp.contents_equal(after));
    }
  }
}

#endif //HAVE_UADK
