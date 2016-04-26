// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include <time.h>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/binomial_distribution.hpp>
#include <gtest/gtest.h>
#include "common/ceph_argparse.h"
#include "compressor/AsyncCompressor.h"
#include "global/global_init.h"

typedef boost::mt11213b gen_type;

class AsyncCompressorTest : public ::testing::Test {
 public:
  AsyncCompressor *async_compressor;
  virtual void SetUp() {
    cerr << __func__ << " start set up " << std::endl;
    async_compressor = new AsyncCompressor(g_ceph_context);
    async_compressor->init();
  }
  virtual void TearDown() {
    async_compressor->terminate();
    delete async_compressor;
  }

  void generate_random_data(bufferlist &bl, uint64_t len = 0) {
    static const char *base= "znvm,x12399zasdfjkl1209zxcvjlkasjdfljwqelrjzx,cvn,m123#*(@)";
    if (!len) {
      boost::uniform_int<> kb(16, 4096);
      gen_type rng(time(NULL));
      len = kb(rng) * 1024;
    }

    while (bl.length() < len)
      bl.append(base, sizeof(base)-1);
  }
};

TEST_F(AsyncCompressorTest, SimpleTest) {
  bufferlist compress_data, decompress_data, rawdata;
  generate_random_data(rawdata, 1<<22);
  bool finished;
  uint64_t id = async_compressor->async_compress(rawdata);
  ASSERT_EQ(0, async_compressor->get_compress_data(id, compress_data, true, &finished));
  ASSERT_TRUE(finished == true);
  id = async_compressor->async_decompress(compress_data);
  do {
    ASSERT_EQ(0, async_compressor->get_decompress_data(id, decompress_data, false, &finished));
  } while (!finished);
  ASSERT_TRUE(finished == true);
  ASSERT_TRUE(rawdata.contents_equal(decompress_data));
  ASSERT_EQ(-ENOENT, async_compressor->get_decompress_data(id, decompress_data, true, &finished));
}

TEST_F(AsyncCompressorTest, GrubWaitTest) {
  async_compressor->terminate();
  bufferlist compress_data, decompress_data, rawdata;
  generate_random_data(rawdata, 1<<22);
  bool finished;
  uint64_t id = async_compressor->async_compress(rawdata);
  ASSERT_EQ(0, async_compressor->get_compress_data(id, compress_data, true, &finished));
  ASSERT_TRUE(finished == true);
  id = async_compressor->async_decompress(compress_data);
  ASSERT_EQ(0, async_compressor->get_decompress_data(id, decompress_data, true, &finished));
  ASSERT_TRUE(finished == true);
  ASSERT_TRUE(rawdata.contents_equal(decompress_data));
  async_compressor->init();
}

TEST_F(AsyncCompressorTest, DecompressInjectTest) {
  bufferlist compress_data, decompress_data, rawdata;
  generate_random_data(rawdata, 1<<22);
  bool finished;
  uint64_t id = async_compressor->async_compress(rawdata);
  ASSERT_EQ(0, async_compressor->get_compress_data(id, compress_data, true, &finished));
  ASSERT_TRUE(finished == true);
  char error[] = "asjdfkwejrljqwaelrj";
  memcpy(compress_data.c_str()+1024, error, sizeof(error)-1);
  id = async_compressor->async_decompress(compress_data);
  ASSERT_EQ(-EIO, async_compressor->get_decompress_data(id, decompress_data, true, &finished));
}

class SyntheticWorkload {
  set<pair<uint64_t, uint64_t> > compress_jobs, decompress_jobs;
  AsyncCompressor *async_compressor;
  vector<bufferlist> rand_data, compress_data;
  gen_type rng;
  static const uint64_t MAX_INFLIGHT = 128;

 public:
  explicit SyntheticWorkload(AsyncCompressor *ac): async_compressor(ac), rng(time(NULL)) {
    for (int i = 0; i < 100; i++) {
      bufferlist bl;
      boost::uniform_int<> u(4096, 1<<24);
      uint64_t value_len = u(rng);
      bufferptr bp(value_len);
      bp.zero();
      for (uint64_t j = 0; j < value_len-sizeof(i); ) {
        memcpy(bp.c_str()+j, &i, sizeof(i));
        j += 4096;
      }

      bl.append(bp);
      rand_data.push_back(bl);
      compress_jobs.insert(make_pair(async_compressor->async_compress(rand_data[i]), i));
      if (!(i % 10)) cerr << "seeding compress data " << i << std::endl;
    }
    compress_data.resize(100);
    reap(true);
  }
  void do_compress() {
    boost::uniform_int<> u(0, rand_data.size()-1);
    uint64_t index = u(rng);
    compress_jobs.insert(make_pair(async_compressor->async_compress(rand_data[index]), index));
  }
  void do_decompress() {
    boost::uniform_int<> u(0, compress_data.size()-1);
    uint64_t index = u(rng);
    if (compress_data[index].length())
      decompress_jobs.insert(make_pair(async_compressor->async_decompress(compress_data[index]), index));
  }
  void reap(bool blocking) {
    bufferlist data;
    bool finished;
    set<pair<uint64_t, uint64_t> >::iterator prev;
    uint64_t c_reap = 0, d_reap = 0;
    do {
      for (set<pair<uint64_t, uint64_t> >::iterator it = compress_jobs.begin();
           it != compress_jobs.end();) {
        prev = it;
        ++it;
        ASSERT_EQ(0, async_compressor->get_compress_data(prev->first, data, blocking, &finished));
        if (finished) {
          c_reap++;
          if (compress_data[prev->second].length())
            ASSERT_TRUE(compress_data[prev->second].contents_equal(data));
          else
            compress_data[prev->second].swap(data);
          compress_jobs.erase(prev);
        }
      }

      for (set<pair<uint64_t, uint64_t> >::iterator it = decompress_jobs.begin();
           it != decompress_jobs.end();) {
        prev = it;
        ++it;
        ASSERT_EQ(0, async_compressor->get_decompress_data(prev->first, data, blocking, &finished));
        if (finished) {
          d_reap++;
          ASSERT_TRUE(rand_data[prev->second].contents_equal(data));
          decompress_jobs.erase(prev);
        }
      }
      usleep(1000 * 500);
    } while (compress_jobs.size() + decompress_jobs.size() > MAX_INFLIGHT);
    cerr << " reap compress jobs " << c_reap << " decompress jobs " << d_reap << std::endl;
  }
  void print_internal_state() {
    cerr << "inlfight compress jobs: " << compress_jobs.size()
         << " inflight decompress jobs: " << decompress_jobs.size() << std::endl;
  }
  bool empty() const { return compress_jobs.empty() && decompress_jobs.empty(); }
};

TEST_F(AsyncCompressorTest, SyntheticTest) {
  SyntheticWorkload test_ac(async_compressor);
  gen_type rng(time(NULL));
  boost::uniform_int<> true_false(0, 99);
  int val;
  for (int i = 0; i < 3000; ++i) {
    if (!(i % 10)) {
      cerr << "Op " << i << ": ";
      test_ac.print_internal_state();
    }
    val = true_false(rng);
    if (val < 45) {
      test_ac.do_compress();
    } else if (val < 95) {
      test_ac.do_decompress();
    } else {
      test_ac.reap(false);
    }
  }
  while (!test_ac.empty()) {
    test_ac.reap(false);
    test_ac.print_internal_state();
    usleep(1000*500);
  }
}


int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  const char* env = getenv("CEPH_LIB");
  string directory(env ? env : ".libs");

  // copy libceph_snappy.so into $plugin_dir/compressor for PluginRegistry
  // TODO: just build the compressor plugins in this subdir
  string mkdir_compressor = "mkdir -p " + directory + "/compressor";
  int r = system(mkdir_compressor.c_str());
  (void)r;

  string cp_libceph_snappy = "cp " + directory + "/libceph_snappy.so* " + directory + "/compressor/";
  r = system(cp_libceph_snappy.c_str());
  (void)r;

  g_conf->set_val("plugin_dir", directory, false, false);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 unittest_async_compressor && valgrind --tool=memcheck ./unittest_async_compressor"
 * End:
 */
