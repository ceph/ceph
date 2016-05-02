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

#include <errno.h>
#include <stdlib.h>

#include "crush/CrushWrapper.h"
#include "osd/osd_types.h"
#include "include/stringify.h"
#include "global/global_init.h"
#include "erasure-code/shec/ErasureCodeShec.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "gtest/gtest.h"

unsigned int count_num = 0;
unsigned int unexpected_count = 0;
unsigned int value_count = 0;

map<set<int>,set<set<int> > > shec_table;

int getint(int a, int b) {
  return ((1 << a) | (1 << b));
}

int getint(int a, int b, int c) {
  return ((1 << a) | (1 << b) | (1 << c));
}

int getint(int a, int b, int c, int d) {
  return ((1 << a) | (1 << b) | (1 << c) | (1 << d));
}

void create_table_shec432() {
  set<int> table_key,vec_avails;
  set<set<int> > table_value;

  for (int want_count = 0; want_count < 7; ++want_count) {
    for (int want = 1; want < (1<<7); ++want) {
      table_key.clear();
      table_value.clear();
      if (__builtin_popcount(want) != want_count) {
        continue;
      }
      {
        for (int i = 0; i < 7; ++i) {
          if (want & (1 << i)) {
            table_key.insert(i);
          }
        }
      }
      vector<int> vec;
      for (int avails = 0; avails < (1<<7); ++avails) {
        if (want & avails) {
          continue;
        }
        if (__builtin_popcount(avails) == 2 &&
            __builtin_popcount(want) == 1) {
          if ((want | avails) == getint(0,1,5) ||
              (want | avails) == getint(2,3,6)) {
            vec.push_back(avails);
          }
        }
      }
      
      for (int avails = 0; avails < (1<<7); ++avails) {
        if (want & avails) {
          continue;
        }
        if (__builtin_popcount(avails) == 4) {
          if ((avails) == getint(0,1,2,3) ||
              (avails) == getint(0,1,2,4) ||
              (avails) == getint(0,1,2,6) ||
              (avails) == getint(0,1,3,4) ||
              (avails) == getint(0,1,3,6) ||
              (avails) == getint(0,1,4,6) ||
              (avails) == getint(0,2,3,4) ||
              (avails) == getint(0,2,3,5) ||
              (avails) == getint(0,2,4,5) ||
              (avails) == getint(0,2,4,6) ||
              (avails) == getint(0,2,5,6) ||
              (avails) == getint(0,3,4,5) ||
              (avails) == getint(0,3,4,6) ||
              (avails) == getint(0,3,5,6) ||
              (avails) == getint(0,4,5,6) ||
              (avails) == getint(1,2,3,4) ||
              (avails) == getint(1,2,3,5) ||
              (avails) == getint(1,2,4,5) ||
              (avails) == getint(1,2,4,6) ||
              (avails) == getint(1,2,5,6) ||
              (avails) == getint(1,3,4,5) ||
              (avails) == getint(1,3,4,6) ||
              (avails) == getint(1,3,5,6) ||
              (avails) == getint(1,4,5,6) ||
              (avails) == getint(2,3,4,5) ||
              (avails) == getint(2,4,5,6) ||
              (avails) == getint(3,4,5,6)) {
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
            vec_avails.insert(j);
          }
        }
        table_value.insert(vec_avails);
      }
      shec_table.insert(std::make_pair(table_key,table_value));
    }
  }
}

bool search_table_shec432(set<int> want_to_read, set<int> available_chunks) {
  set<set<int> > tmp;
  set<int> settmp;
  bool found;

  tmp = shec_table.find(want_to_read)->second;
  for (set<set<int> >::iterator itr = tmp.begin();itr != tmp.end(); ++itr) {
    found = true;
    value_count = 0;
    settmp = *itr;
    for (set<int>::iterator setitr = settmp.begin();setitr != settmp.end(); ++setitr) {
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

TEST(ParameterTest, combination_all)
{
  int result;
  unsigned alignment, tail, padded_length;
  const unsigned int kObjectSize = 128;

  //get profile
  char* k = (char*)"4";
  char* m = (char*)"3";
  char* c = (char*)"2";
  int i_k = atoi(k);
  int i_m = atoi(m);
  int i_c = atoi(c);
  alignment = i_k * 8 * sizeof(int);
  tail = kObjectSize % alignment;
  padded_length = kObjectSize + (tail ? (alignment - tail) : 0);
  unsigned c_size = padded_length / i_k;

  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *profile = new map<std::string,
							 std::string>();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = k;
  (*profile)["m"] = m;
  (*profile)["c"] = c;

  result = shec->init(*profile, &cerr);

  //check profile
  EXPECT_EQ(i_k, shec->k);
  EXPECT_EQ(i_m, shec->m);
  EXPECT_EQ(i_c, shec->c);
  EXPECT_EQ(8, shec->w);
  EXPECT_EQ(ErasureCodeShec::MULTIPLE, shec->technique);
  EXPECT_STREQ("default", shec->ruleset_root.c_str());
  EXPECT_STREQ("osd", shec->ruleset_failure_domain.c_str());
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, result);

  //encode
  bufferlist in,out1;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "0123"//128
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }

  result = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, result);
  EXPECT_EQ(i_k+i_m, (int)encoded.size());
  EXPECT_EQ(c_size, encoded[0].length());
  //out1 is "encoded"
  for (unsigned int i = 0; i < encoded.size(); ++i) {
    out1.append(encoded[i]);
  }
  EXPECT_FALSE(out1 == in);

  set<int> want_to_read, available_chunks, minimum_chunks, want_to_read_without_avails;
  set<int>::iterator itr;
  int array_want_to_read[shec->get_chunk_count()];
  int array_available_chunks[shec->get_chunk_count()];
  int dresult,cmp;
  map<int, bufferlist> inchunks,decoded;
  bufferlist usable;
  unsigned int minimum_count;

  for (unsigned int w1 = 0; w1 <= shec->get_chunk_count(); ++w1) {
    const unsigned int r1 = w1;		// combination(k+m,r1)

    for (unsigned int i = 0; i < r1; ++i) {
      array_want_to_read[i] = 1;
    }
    for (unsigned int i = r1; i < shec->get_chunk_count(); ++i) {
      array_want_to_read[i] = 0;
    }

    for (unsigned w2 = 0; w2 <= shec->get_chunk_count(); ++w2) {
      const unsigned int r2 = w2;	// combination(k+m,r2)

      for (unsigned int i = 0; i < r2; ++i ) {
        array_available_chunks[i] = 1;
      }
      for (unsigned int i = r2; i < shec->get_chunk_count(); ++i ) {
        array_available_chunks[i] = 0;
      }

      do {
        do {
          for (unsigned int i = 0; i < shec->get_chunk_count(); ++i) {
	    if (array_want_to_read[i]) {
	      want_to_read.insert(i);
	    }
            if (array_available_chunks[i]) {
              available_chunks.insert(i);
              inchunks.insert(make_pair(i,encoded[i]));
            }
          }

          result = shec->minimum_to_decode(want_to_read, available_chunks,
				           &minimum_chunks);
          dresult = shec->decode(want_to_read, inchunks, &decoded);
          ++count_num;
          minimum_count = 0;

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
            for (itr = want_to_read.begin();itr != want_to_read.end(); ++itr) {
              if (!available_chunks.count(*itr)) {
                want_to_read_without_avails.insert(*itr);
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
                  usable.clear();
                  usable.substr_of(in, c_size * i, c_size);
                  cmp = memcmp(decoded[i].c_str(), usable.c_str(), c_size);
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
              EXPECT_EQ(shec->get_chunk_count(), decoded.size());
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
                    usable.clear();
                    usable.substr_of(in, c_size * i, c_size);
                    cmp = memcmp(decoded[i].c_str(), usable.c_str(), c_size);
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
                EXPECT_EQ(shec->get_chunk_count(), decoded.size());
                if (result != -EIO || dresult != -1) {
                  ++unexpected_count;
                }
              }
            }
          }

          want_to_read.clear();
          want_to_read_without_avails.clear();
          available_chunks.clear();
          minimum_chunks.clear();
          inchunks.clear();
          decoded.clear();
          usable.clear();
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

int main(int argc, char **argv)
{
  int r;

  vector<const char*> args;
  argv_to_vec(argc, (const char **) argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  const char* env = getenv("CEPH_LIB");
  std::string directory(env ? env : ".libs");
  g_conf->set_val("erasure_code_dir", directory, false, false);

  ::testing::InitGoogleTest(&argc, argv);

  create_table_shec432();

  r = RUN_ALL_TESTS();

  std::cout << "minimum_to_decode:total_num = " << count_num
      << std::endl;
  std::cout << "minimum_to_decode:unexpected_num = " << unexpected_count
      << std::endl;

  return r;
}
