// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <vector>
#include <cstring>

#include "include/types.h"
#include "include/buffer.h"

#include "common/rabin.h"
#include "gtest/gtest.h"

TEST(Rabin, rabin_hash_zero) {
  uint64_t expected;
  uint64_t result;
  //char data[] = "q";
  //expected = 0;
  //result = gen_rabin_hash(data, 0);
  //EXPECT_EQ(expected, result);

  char zero_data[1024];
  memset(zero_data, 0, 1024);
  expected = 0;
  result = gen_rabin_hash(zero_data, 0);
  ASSERT_EQ(expected, result);
  ASSERT_EQ(true, end_of_chunk(result));
}

TEST(Rabin, rabin_hash_simple) {
  uint64_t expected = 680425538102669423;
  uint64_t result;

  unsigned int window_size = 48;
  char data[window_size + 1];
  memset(data, 0, window_size + 1);
  for (unsigned int i = 0; i < window_size; ++i) {
    data[i] = i;
  }
  result = gen_rabin_hash(data, 0);
  ASSERT_EQ(expected, result);
}

TEST(Rabin, chunk_file_less_than_min) {
  // just put a small file
  const char *fname = "rabin_chunk_testfile";
  ::unlink(fname);
  int fd = ::open(fname, O_RDWR|O_CREAT|O_TRUNC, 0600);
  ASSERT_NE(fd, -1);
  const char buf[] = "0123456789";
  for (int i = 0; i < 1; i++) {
    ASSERT_EQ((ssize_t)sizeof(buf), write(fd, buf, sizeof(buf)));
  }
  ::close(fd);

  std::string error;
  bufferlist bl;
  int err = bl.read_file(fname, &error);
  ASSERT_GE(err, 0);

  std::vector<bufferlist> out;
  size_t min_chunk = 2000;
  size_t max_chunk = 8000;
  get_rabin_chunks(min_chunk, max_chunk, bl, &out);
  for (size_t i = 0; i < out.size(); ++i) {
    // test if min <= chunk <= max
    uint64_t chunk_size = out[i].length();
    ASSERT_GE(chunk_size , min_chunk);
    ASSERT_LE(chunk_size , max_chunk);
  }

  ::unlink(fname);
}

TEST(Rabin, chunk_binbash) {
  const char *fname = "/bin/bash";
  std::string error;
  bufferlist bl;

  int err = bl.read_file(fname, &error);
  ASSERT_GE(err, 0);

  std::vector<bufferlist> out;
  size_t min_chunk = 2000;
  size_t max_chunk = 8000;
  int hist_size = 5;
  int hist [hist_size] = {0}; 
  size_t range = (max_chunk - min_chunk) / hist_size;
  get_rabin_chunks(min_chunk, max_chunk, bl, &out, 5);
  for (size_t i = 0; i < out.size(); ++i) {
    // test if min <= chunk <= max
    uint64_t chunk_size = out[i].length();
    printf(" chunk has size %zu\n", chunk_size);
    ASSERT_GE(chunk_size , min_chunk);
    ASSERT_LE(chunk_size , max_chunk);
    int bucket = (chunk_size - min_chunk) / range;
    hist[bucket] += 1;
  }
  printf("min chunk %zu, max chunk %zu", min_chunk, max_chunk);
  printf("hist size %d, range %zu\n", hist_size, range);
  for (int i = 0; i < hist_size; ++i) {
    printf("  hist %d contains %d chunks\n", i, hist[i]);
  }
}

