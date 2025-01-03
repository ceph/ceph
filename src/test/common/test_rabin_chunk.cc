// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <vector>
#include <cstring>
#include <random>

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

void generate_buffer(int size, bufferlist *outbl)
{
  outbl->clear();
  outbl->append_zero(size);
  char *b = outbl->c_str();
  std::mt19937_64 engine;
  for (size_t i = 0; i < size / sizeof(uint64_t); ++i) {
    ((uint64_t*)b)[i] = engine();
  }
}

#if 0
this fails!
TEST(Rabin, shifts)
{
  RabinChunk rabin;
  rabin.set_target_bits(18, 2);

  for (int frontlen = 1; frontlen < 5; frontlen++) {
    bufferlist bl1, bl2;
    generate_buffer(4*1024*1024, &bl1);
    generate_buffer(frontlen, &bl2);
    bl2.append(bl1);
    bl2.rebuild();

    vector<pair<uint64_t, uint64_t>> chunks1, chunks2;
    rabin.do_rabin_chunks(bl1, chunks1);
    rabin.do_rabin_chunks(bl2, chunks2);
    cout << "1: " << chunks1 << std::endl;
    cout << "2: " << chunks2 << std::endl;

    ASSERT_GE(chunks2.size(), chunks1.size());
    int match = 0;
    for (unsigned i = 0; i < chunks1.size(); ++i) {
      unsigned j = i + (chunks2.size() - chunks1.size());
      if (chunks1[i].first + frontlen == chunks2[j].first &&
	  chunks1[i].second == chunks2[j].second) {
	match++;
      }
    }
    ASSERT_GE(match, chunks1.size() - 1);
  }
}
#endif

void do_size_histogram(RabinChunk& rabin, bufferlist& bl,
		       map<int,int> *h)
{
  vector<pair<uint64_t, uint64_t>> chunks;
  rabin.do_rabin_chunks(bl, chunks);
  for (auto& i : chunks) {
    unsigned b = i.second & 0xfffff000;
    //unsigned b = 1 << cbits(i.second);
    (*h)[b]++;
  }
}

void print_histogram(map<int,int>& h)
{
  cout << "size\tcount" << std::endl;
  for (auto i : h) {
    cout << i.first << "\t" << i.second << std::endl;
  }
}

TEST(Rabin, chunk_random)
{
  RabinChunk rabin;
  rabin.set_target_bits(18, 2);

  map<int,int> h;
  for (int i = 0; i < 8; ++i) {
    cout << ".";
    cout.flush();
    bufferlist r;
    generate_buffer(16*1024*1024, &r);
    do_size_histogram(rabin, r, &h);
  }
  cout << std::endl;
  print_histogram(h);
}
