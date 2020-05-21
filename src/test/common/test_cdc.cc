// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <vector>
#include <cstring>
#include <random>

#include "include/types.h"
#include "include/buffer.h"

#include "common/CDC.h"
#include "gtest/gtest.h"

void generate_buffer(int size, bufferlist *outbl, int seed = 0)
{
  std::mt19937_64 engine, engine2;
  engine.seed(seed);
  engine2.seed(seed);

  // assemble from randomly-sized segments!
  outbl->clear();
  auto left = size;
  while (left) {
    size_t l = std::min<size_t>((engine2() & 0xffff0) + 16, left);
    left -= l;
    bufferptr p(l);
    p.set_length(l);
    char *b = p.c_str();
    for (size_t i = 0; i < l / sizeof(uint64_t); ++i) {
      ((uint64_t*)b)[i] = engine();
    }
    outbl->append(p);
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
  if (GetParam() == "fixed"s) return;
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
  if (GetParam() == "fixed"s) return;
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

TEST_P(CDCTest, specific_result)
{
  map<string,vector<pair<uint64_t,uint64_t>>> expected = {
    {"fixed", { {0, 262144}, {262144, 262144}, {524288, 262144}, {786432, 262144}, {1048576, 262144}, {1310720, 262144}, {1572864, 262144}, {1835008, 262144}, {2097152, 262144}, {2359296, 262144}, {2621440, 262144}, {2883584, 262144}, {3145728, 262144}, {3407872, 262144}, {3670016, 262144}, {3932160, 262144} }},
    {"fastcdc", { {0, 151460}, {151460, 441676}, {593136, 407491}, {1000627, 425767}, {1426394, 602875}, {2029269, 327307}, {2356576, 155515}, {2512091, 159392}, {2671483, 829416}, {3500899, 539667}, {4040566, 153738}}},
  };

  bufferlist bl;
  generate_buffer(4*1024*1024, &bl);
  vector<pair<uint64_t,uint64_t>> chunks;
  cdc->calc_chunks(bl, &chunks);
  ASSERT_EQ(chunks, expected[GetParam()]);
}


void do_size_histogram(CDC& cdc, bufferlist& bl,
		       map<int,int> *h)
{
  vector<pair<uint64_t, uint64_t>> chunks;
  cdc.calc_chunks(bl, &chunks);
  uint64_t total = 0;
  uint64_t num = 0;
  for (auto& i : chunks) {
    //unsigned b = i.second & 0xfffff000;
    unsigned b = 1 << (cbits(i.second - 1));
    (*h)[b]++;
    ++num;
    total += i.second;
  }
  (*h)[0] = total / num;
}

void print_histogram(map<int,int>& h)
{
  cout << "size\tcount" << std::endl;
  for (auto i : h) {
    if (i.first) {
      cout << i.first << "\t" << i.second << std::endl;
    } else {
      cout << "avg\t" << i.second << std::endl;
    }
  }
}

TEST_P(CDCTest, chunk_random)
{
  map<int,int> h;
  for (int i = 0; i < 32; ++i) {
    cout << ".";
    cout.flush();
    bufferlist r;
    generate_buffer(16*1024*1024, &r, i);
    do_size_histogram(*cdc, r, &h);
  }
  cout << std::endl;
  print_histogram(h);
}


INSTANTIATE_TEST_SUITE_P(
  CDC,
  CDCTest,
  ::testing::Values(
    "fixed",   // note: we skip most tests bc this is not content-based
    "fastcdc"
    ));
