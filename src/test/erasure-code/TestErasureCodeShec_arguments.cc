// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 FUJITSU LIMITED
 *
 * Author: Shotaro Kawaguchi <kawaguchi.s@jp.fujitsu.com>
 * Author: Takanori Nakao <nakao.takanori@jp.fujitsu.com>
 * Author: Takeshi Miyamae <miyamae.takeshi@jp.fujitsu.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

// SUMMARY: shec's gtest for each argument of minimum_to_decode()/decode()

#include <algorithm>
#include <bit>
#include <cerrno>
#include <cstdlib>

#include "crush/CrushWrapper.h"
#include "osd/osd_types.h"
#include "include/stringify.h"
#include "global/global_init.h"
#include "erasure-code/shec/ErasureCodeShec.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "gtest/gtest.h"

using namespace std;

unsigned int count_num = 0;
unsigned int unexpected_count = 0;
unsigned int value_count = 0;

map<shard_id_set,set<shard_id_set>> shec_table;

constexpr int getint(std::initializer_list<int> is) {
  int a = 0;
  for (const auto i : is) {
    a |= 1 << i;
  }
  return a;
}

void create_table_shec432() {
  shard_id_set table_key, vec_avails;
  set<shard_id_set> table_value;

  for (int want_count = 0; want_count < 7; ++want_count) {
    for (unsigned want = 1; want < (1<<7); ++want) {
      table_key.clear();
      table_value.clear();
      if (std::popcount(want) != want_count) {
        continue;
      }
      {
        for (int i = 0; i < 7; ++i) {
          if (want & (1 << i)) {
            table_key.insert(shard_id_t(i));
          }
        }
      }
      vector<int> vec;
      for (unsigned avails = 0; avails < (1<<7); ++avails) {
        if (want & avails) {
          continue;
        }
        if (std::popcount(avails) == 2 &&
            std::popcount(want) == 1) {
	  if (std::cmp_equal(want | avails, getint({0,1,5})) ||
	      std::cmp_equal(want | avails, getint({2,3,6}))) {
            vec.push_back(avails);
          }
        }
      }

      for (unsigned avails = 0; avails < (1<<7); ++avails) {
        if (want & avails) {
          continue;
        }
        if (std::popcount(avails) == 4) {
	  auto a = to_array<std::initializer_list<int>>({
	      {0,1,2,3}, {0,1,2,4}, {0,1,2,6}, {0,1,3,4}, {0,1,3,6}, {0,1,4,6},
	      {0,2,3,4}, {0,2,3,5}, {0,2,4,5}, {0,2,4,6}, {0,2,5,6}, {0,3,4,5},
	      {0,3,4,6}, {0,3,5,6}, {0,4,5,6}, {1,2,3,4}, {1,2,3,5}, {1,2,4,5},
	      {1,2,4,6}, {1,2,5,6}, {1,3,4,5}, {1,3,4,6}, {1,3,5,6}, {1,4,5,6},
	      {2,3,4,5}, {2,4,5,6}, {3,4,5,6}});
          if (ranges::any_of(a, std::bind_front(cmp_equal<uint, int>, avails),
			     getint)) {
	    vec.push_back(avails);
	  }
	}
      }
      for (int i = 0; i < (int)vec.size(); ++i) {
        for (int j = i + 1; j < (int)vec.size(); ++j) {
          if ((vec[i] & vec[j]) == vec[i]) {
            vec.erase(vec.begin() + j);
            --j;
          }
        }
      }
      for (int i = 0; i < (int)vec.size(); ++i) {
        vec_avails.clear();
        for (int j = 0; j < 7; ++j) {
          if (vec[i] & (1 << j)) {
            vec_avails.insert(shard_id_t(j));
          }
        }
        table_value.insert(vec_avails);
      }
      shec_table.insert(std::make_pair(table_key,table_value));
    }
  }
}

bool search_table_shec432(shard_id_set want_to_read, shard_id_set available_chunks) {
  set<shard_id_set > tmp;
  shard_id_set settmp;
  bool found;

  tmp = shec_table.find(want_to_read)->second;
  for (set<shard_id_set >::iterator itr = tmp.begin();itr != tmp.end(); ++itr) {
    found = true;
    value_count = 0;
    settmp = *itr;
    for (shard_id_set::const_iterator setitr = settmp.begin();setitr != settmp.end(); ++setitr) {
      if (!available_chunks.count(*setitr)) {
        found = false;
      }
      ++value_count;
    }
    if (found) {
      return true;
    }
  }
  return false;
}

IGNORE_DEPRECATED
TEST(ParameterTest, combination_all)
{
  const unsigned int kObjectSize = 128;

  //get profile
  char* k = (char*)"4";
  char* m = (char*)"3";
  char* c = (char*)"2";
  int i_k = atoi(k);
  int i_m = atoi(m);
  int i_c = atoi(c);
  const unsigned alignment = i_k * 8 * sizeof(int);
  const unsigned tail = kObjectSize % alignment;
  const unsigned padded_length = kObjectSize + (tail ? (alignment - tail) : 0);
  const unsigned c_size = padded_length / i_k;

  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *profile = new map<std::string,
							 std::string>();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["crush-failure-domain"] = "osd";
  (*profile)["k"] = k;
  (*profile)["m"] = m;
  (*profile)["c"] = c;

  int result = shec->init(*profile, &cerr);

  //check profile
  EXPECT_EQ(i_k, shec->k);
  EXPECT_EQ(i_m, shec->m);
  EXPECT_EQ(i_c, shec->c);
  EXPECT_EQ(8, shec->w);
  EXPECT_EQ(ErasureCodeShec::MULTIPLE, shec->technique);
  EXPECT_STREQ("default", shec->rule_root.c_str());
  EXPECT_STREQ("osd", shec->rule_failure_domain.c_str());
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, result);

  //encode
  bufferlist in;
  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "0123"//128
  );
  set<int> want_to_encode;
  for (unsigned int i = 0; i < shec->get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }

  map<int, bufferlist> encoded;
  result = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, result);
  EXPECT_EQ(i_k+i_m, (int)encoded.size());
  EXPECT_EQ(c_size, encoded[0].length());
  bufferlist out1;
  //out1 is "encoded"
  for (unsigned int i = 0; i < encoded.size(); ++i) {
    out1.append(encoded[i]);
  }
  EXPECT_FALSE(out1 == in);

  for (unsigned int w1 = 0; w1 <= shec->get_chunk_count(); ++w1) {
    // combination(k+m,w1)
    int array_want_to_read[shec->get_chunk_count()];
    for (unsigned int i = 0; i < shec->get_chunk_count(); ++i) {
      array_want_to_read[i] = i < w1 ? 1 : 0;
    }

    for (unsigned w2 = 0; w2 <= shec->get_chunk_count(); ++w2) {
      // combination(k+m,w2)
      int array_available_chunks[shec->get_chunk_count()];
      for (unsigned int i = 0; i < shec->get_chunk_count(); ++i ) {
        array_available_chunks[i] = i < w2 ? 1 : 0;
      }

      do {
        do {
	  set<int> want_to_read, available_chunks;
	  map<int, bufferlist> inchunks;
          for (unsigned int i = 0; i < shec->get_chunk_count(); ++i) {
	    if (array_want_to_read[i]) {
	      want_to_read.insert(i);
	    }
            if (array_available_chunks[i]) {
              available_chunks.insert(i);
              inchunks.insert(make_pair(i,encoded[i]));
            }
          }

	  map<int, vector<pair<int,int>>> minimum_chunks;
	  map<int, bufferlist> decoded;
          result = shec->minimum_to_decode(want_to_read, available_chunks,
				           &minimum_chunks);
          int dresult = shec->decode(want_to_read, inchunks, &decoded,
				     shec->get_chunk_size(kObjectSize));
          ++count_num;
	  unsigned int minimum_count = 0;

          if (want_to_read.size() == 0) {
            EXPECT_EQ(0, result);
	    EXPECT_EQ(0u, minimum_chunks.size());
            EXPECT_EQ(0, dresult);
            EXPECT_EQ(0u, decoded.size());
            EXPECT_EQ(0u, decoded[0].length());
            if (result != 0 || dresult != 0) {
              ++unexpected_count;
            }
          } else {
            // want - avail
	    set<int> want_to_read_without_avails;
            for (auto chunk : want_to_read) {
              if (!available_chunks.count(chunk)) {
                want_to_read_without_avails.insert(chunk);
              } else {
                ++minimum_count;
              }
            }
            
            if (want_to_read_without_avails.size() == 0) {
              EXPECT_EQ(0, result);
	      EXPECT_LT(0u, minimum_chunks.size());
	      EXPECT_GE(minimum_count, minimum_chunks.size());
              EXPECT_EQ(0, dresult);
              EXPECT_NE(0u, decoded.size());
              for (unsigned int i = 0; i < shec->get_data_chunk_count(); ++i) {
                if (array_want_to_read[i]) {
		  bufferlist usable;
                  usable.substr_of(in, c_size * i, c_size);
                  int cmp = memcmp(decoded[i].c_str(), usable.c_str(), c_size);
                  EXPECT_EQ(c_size, decoded[i].length());
                  EXPECT_EQ(0, cmp);
                  if (cmp != 0) {
                    ++unexpected_count;
                  }
                }
              }
              if (result != 0 || dresult != 0) {
                ++unexpected_count;
              }
            } else if (want_to_read_without_avails.size() > 3) {
              EXPECT_EQ(-EIO, result);
	      EXPECT_EQ(0u, minimum_chunks.size());
              EXPECT_EQ(-1, dresult);
              if (result != -EIO || dresult != -1) {
                ++unexpected_count;
              }
            } else {
              // search
              if (search_table_shec432(want_to_read_without_avails,available_chunks)) {
                EXPECT_EQ(0, result);
	        EXPECT_LT(0u, minimum_chunks.size());
	        EXPECT_GE(value_count + minimum_count, minimum_chunks.size());
                EXPECT_EQ(0, dresult);
                EXPECT_NE(0u, decoded.size());
                for (unsigned int i = 0; i < shec->get_data_chunk_count(); ++i) {
                  if (array_want_to_read[i]) {
		    bufferlist usable;
                    usable.substr_of(in, c_size * i, c_size);
                    int cmp = memcmp(decoded[i].c_str(), usable.c_str(), c_size);
                    EXPECT_EQ(c_size, decoded[i].length());
                    EXPECT_EQ(0, cmp);
                    if (cmp != 0) {
                      ++unexpected_count;
                      std::cout << "decoded[" << i << "] = " << decoded[i].c_str() << std::endl;
                      std::cout << "usable = " << usable.c_str() << std::endl;
                      std::cout << "want_to_read    :" << want_to_read << std::endl;
                      std::cout << "available_chunks:" << available_chunks << std::endl;
                      std::cout << "minimum_chunks  :" << minimum_chunks << std::endl;
                    }
                  }
                }
                if (result != 0 || dresult != 0) {
                  ++unexpected_count;
                }
              } else {
                EXPECT_EQ(-EIO, result);
	        EXPECT_EQ(0u, minimum_chunks.size());
                EXPECT_EQ(-1, dresult);
                if (result != -EIO || dresult != -1) {
                  ++unexpected_count;
                }
              }
            }
          }
        } while (std::prev_permutation(
		   array_want_to_read,
		   array_want_to_read + shec->get_chunk_count()));

      } while (std::prev_permutation(
                 array_available_chunks,
                 array_available_chunks + shec->get_chunk_count()));
    }
  }

  delete shec;
  delete profile;
}
END_IGNORE_DEPRECATED

int main(int argc, char **argv)
{
  auto args = argv_to_vec(argc, argv);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_MON_CONFIG);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);

  create_table_shec432();

  int r = RUN_ALL_TESTS();

  std::cout << "minimum_to_decode:total_num = " << count_num
      << std::endl;
  std::cout << "minimum_to_decode:unexpected_num = " << unexpected_count
      << std::endl;

  return r;
}
