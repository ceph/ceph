// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <vector>
#include <cstring>

#include "include/types.h"
#include "include/buffer.h"

#include "common/rabin.h"
#include "gtest/gtest.h"

TEST(Rabin, rabin_hash_simple) {
  uint64_t expected = 680425538102669423;
  uint64_t result;

  unsigned int window_size = 48;
  char data[window_size + 1];
  RabinChunk rabin;
  memset(data, 0, window_size + 1);
  for (unsigned int i = 0; i < window_size; ++i) {
    data[i] = i;
  }
  result = rabin.gen_rabin_hash(data, 0);
  ASSERT_EQ(expected, result);
}

TEST(Rabin, chunk_check_min_max) {
  const char buf[] = "0123456789";

  bufferlist bl;
  RabinChunk rabin;
  for (int i = 0; i < 250; i++) {
    bl.append(buf);
  }

  vector<pair<uint64_t, uint64_t>> chunks;
  size_t min_chunk = 2000;
  size_t max_chunk = 8000;

  rabin.do_rabin_chunks(bl, chunks, min_chunk, max_chunk);
  uint64_t chunk_size = chunks[0].second;
  ASSERT_GE(chunk_size , min_chunk);
  ASSERT_LE(chunk_size , max_chunk);
}

TEST(Rabin, test_cdc) {
  const char *base_str = "123456789012345678901234567890123456789012345678";
  bufferlist bl, cmp_bl;
  for (int i = 0; i < 100; i++) {
    bl.append(base_str);
  }
  cmp_bl.append('a');
  for (int i = 0; i < 100; i++) {
    cmp_bl.append(base_str);
  }

  RabinChunk rabin;
  vector<pair<uint64_t, uint64_t>> chunks;
  vector<pair<uint64_t, uint64_t>> cmp_chunks;
  size_t min_chunk = 200;
  size_t max_chunk = 800;
  rabin.do_rabin_chunks(bl, chunks, min_chunk, max_chunk);
  rabin.do_rabin_chunks(cmp_bl, cmp_chunks, min_chunk, max_chunk);
  // offset, len will be the same, except in the case of first offset
  ASSERT_EQ(chunks[4].first + 1, cmp_chunks[4].first);
  ASSERT_EQ(chunks[4].second, cmp_chunks[4].second);
}

