// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <vector>
#include <cstring>
#include <random>

#include "include/types.h"
#include "include/buffer.h"

#include "common/CDC.h"
#include "gtest/gtest.h"

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

class CDCTest : public ::testing::Test,
		public ::testing::WithParamInterface<const char*> {
public:
  std::unique_ptr<CDC> cdc;

  CDCTest() {
    auto plugin = GetParam();
    cdc = std::move(CDC::create(plugin, 18));
  }
};

TEST_P(CDCTest, insert_front)
{
  for (int frontlen = 1; frontlen < 163840; frontlen *= 3) {
    bufferlist bl1, bl2;
    generate_buffer(4*1024*1024, &bl1);
    generate_buffer(frontlen, &bl2);
    bl2.append(bl1);
    bl2.rebuild();

    vector<pair<uint64_t, uint64_t>> chunks1, chunks2;
    cdc->calc_chunks(bl1, &chunks1);
    cdc->calc_chunks(bl2, &chunks2);
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

TEST_P(CDCTest, insert_middle)
{
  for (int frontlen = 1; frontlen < 163840; frontlen *= 3) {
    bufferlist bl1, bl2;
    generate_buffer(4*1024*1024, &bl1);
    bufferlist f, m, e;
    generate_buffer(frontlen, &m);
    f.substr_of(bl1, 0, bl1.length() / 2);
    e.substr_of(bl1, bl1.length() / 2, bl1.length() / 2);
    bl2 = f;
    bl2.append(m);
    bl2.append(e);
    bl2.rebuild();

    vector<pair<uint64_t, uint64_t>> chunks1, chunks2;
    cdc->calc_chunks(bl1, &chunks1);
    cdc->calc_chunks(bl2, &chunks2);
    cout << "1: " << chunks1 << std::endl;
    cout << "2: " << chunks2 << std::endl;

    ASSERT_GE(chunks2.size(), chunks1.size());
    int match = 0;
    unsigned i;
    for (i = 0; i < chunks1.size()/2; ++i) {
      unsigned j = i;
      if (chunks1[i].first == chunks2[j].first &&
	  chunks1[i].second == chunks2[j].second) {
	match++;
      }
    }
    for (; i < chunks1.size(); ++i) {
      unsigned j = i + (chunks2.size() - chunks1.size());
      if (chunks1[i].first + frontlen == chunks2[j].first &&
	  chunks1[i].second == chunks2[j].second) {
	match++;
      }
    }
    ASSERT_GE(match, chunks1.size() - 2);
  }
}

void do_size_histogram(CDC& cdc, bufferlist& bl,
		       map<int,int> *h)
{
  vector<pair<uint64_t, uint64_t>> chunks;
  cdc.calc_chunks(bl, &chunks);
  for (auto& i : chunks) {
    //unsigned b = i.second & 0xfffff000;
    unsigned b = 1 << cbits(i.second);
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

TEST_P(CDCTest, chunk_random)
{
  map<int,int> h;
  for (int i = 0; i < 32; ++i) {
    cout << ".";
    cout.flush();
    bufferlist r;
    generate_buffer(16*1024*1024, &r);
    do_size_histogram(*cdc, r, &h);
  }
  cout << std::endl;
  print_histogram(h);
}


INSTANTIATE_TEST_SUITE_P(
  CDC,
  CDCTest,
  ::testing::Values(
    "fastcdc"
    ));
