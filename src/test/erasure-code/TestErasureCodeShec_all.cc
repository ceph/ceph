// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014,2015 FUJITSU LIMITED
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

// SUMMARY: TestErasureCodeShec combination of k,m,c by 301 patterns

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

struct Param_d {
  char* k;
  char* m;
  char* c;
  int ch_size;
  char sk[16];
  char sm[16];
  char sc[16];
};
struct Param_d param[301];

unsigned int g_recover = 0;
unsigned int g_cannot_recover = 0;
struct Recover_d {
  int k;
  int m;
  int c;
  set<int> want;
  set<int> avail;
};
struct std::vector<Recover_d> cannot_recover;

class ParameterTest : public ::testing::TestWithParam<struct Param_d> {

};

TEST_P(ParameterTest, parameter_all)
{
  int result;
  //get parameters
  char* k = GetParam().k;
  char* m = GetParam().m;
  char* c = GetParam().c;
  unsigned c_size = GetParam().ch_size;
  int i_k = atoi(k);
  int i_m = atoi(m);
  int i_c = atoi(c);

  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
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

  //minimum_to_decode
  //want_to_decode will be a combination that chooses 1~c from k+m
  set<int> want_to_decode, available_chunks, minimum_chunks;
  int array_want_to_decode[shec->get_chunk_count()];
  struct Recover_d comb;

  for (int w = 1; w <= i_c; w++) {
    const unsigned int r = w;		// combination(k+m,r)

    for (unsigned int i = 0; i < r; ++i) {
      array_want_to_decode[i] = 1;
    }
    for (unsigned int i = r; i < shec->get_chunk_count(); ++i) {
      array_want_to_decode[i] = 0;
    }

    do {
      for (unsigned int i = 0; i < shec->get_chunk_count(); i++) {
	available_chunks.insert(i);
      }
      for (unsigned int i = 0; i < shec->get_chunk_count(); i++) {
	if (array_want_to_decode[i]) {
	  want_to_decode.insert(i);
	  available_chunks.erase(i);
	}
      }

      result = shec->minimum_to_decode(want_to_decode, available_chunks,
				       &minimum_chunks);

      if (result == 0){
	EXPECT_EQ(0, result);
	EXPECT_TRUE(minimum_chunks.size());
	g_recover++;
      } else {
	EXPECT_EQ(-EIO, result);
	EXPECT_EQ(0u, minimum_chunks.size());
	g_cannot_recover++;
	comb.k = shec->k;
	comb.m = shec->m;
	comb.c = shec->c;
	comb.want = want_to_decode;
	comb.avail = available_chunks;
	cannot_recover.push_back(comb);
      }

      want_to_decode.clear();
      available_chunks.clear();
      minimum_chunks.clear();
    } while (std::prev_permutation(
		 array_want_to_decode,
		 array_want_to_decode + shec->get_chunk_count()));
  }

  //minimum_to_decode_with_cost
  set<int> want_to_decode_with_cost, minimum_chunks_with_cost;
  map<int, int> available_chunks_with_cost;

  for (unsigned int i = 0; i < 1; i++) {
    want_to_decode_with_cost.insert(i);
  }
  for (unsigned int i = 0; i < shec->get_chunk_count(); i++) {
    available_chunks_with_cost[i] = i;
  }

  result = shec->minimum_to_decode_with_cost(
      want_to_decode_with_cost,
      available_chunks_with_cost,
      &minimum_chunks_with_cost);
  EXPECT_EQ(0, result);
  EXPECT_TRUE(minimum_chunks_with_cost.size());

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
	    "012345"//192
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); i++) {
    want_to_encode.insert(i);
  }

  result = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, result);
  EXPECT_EQ(i_k+i_m, (int)encoded.size());
  EXPECT_EQ(c_size, encoded[0].length());

  //decode
  int want_to_decode2[i_k + i_m];
  map<int, bufferlist> decoded;

  for (unsigned int i = 0; i < shec->get_chunk_count(); i++) {
    want_to_decode2[i] = i;
  }

  result = shec->decode(set<int>(want_to_decode2, want_to_decode2 + 2),
			encoded, &decoded);
  EXPECT_EQ(0, result);
  EXPECT_EQ(2u, decoded.size());
  EXPECT_EQ(c_size, decoded[0].length());

  //check encoded,decoded
  bufferlist out1, out2, usable;

  //out1 is "encoded"
  for (unsigned int i = 0; i < encoded.size(); i++) {
    out1.append(encoded[i]);
  }

  //out2 is "decoded"
  shec->decode_concat(encoded, &out2);
  usable.substr_of(out2, 0, in.length());

  EXPECT_FALSE(out1 == in);
  EXPECT_TRUE(usable == in);

  //create_ruleset
  stringstream ss;
  CrushWrapper *crush = new CrushWrapper;
  crush->create();
  crush->set_type_name(2, "root");
  crush->set_type_name(1, "host");
  crush->set_type_name(0, "osd");

  int rootno;
  crush->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1, 2, 0, NULL,
		    NULL, &rootno);
  crush->set_item_name(rootno, "default");

  map < string, string > loc;
  loc["root"] = "default";

  int num_host = 2;
  int num_osd = 5;
  int osd = 0;
  for (int h = 0; h < num_host; ++h) {
    loc["host"] = string("host-") + stringify(h);
    for (int o = 0; o < num_osd; ++o, ++osd) {
      crush->insert_item(g_ceph_context, osd, 1.0,
			 string("osd.") + stringify(osd), loc);
    }
  }

  result = shec->create_ruleset("myrule", *crush, &ss);
  EXPECT_EQ(0, result);
  EXPECT_STREQ("myrule", crush->rule_name_map[0].c_str());

  //get_chunk_count
  EXPECT_EQ(i_k+i_m, (int)shec->get_chunk_count());

  //get_data_chunk_count
  EXPECT_EQ(i_k, (int)shec->get_data_chunk_count());

  //get_chunk_size
  EXPECT_EQ(c_size, shec->get_chunk_size(192));

  delete shec;
  delete profile;
  delete crush;
}

INSTANTIATE_TEST_CASE_P(Test, ParameterTest, ::testing::ValuesIn(param));

int main(int argc, char **argv)
{
  int i = 0;
  int r;
  const int kObjectSize = 192;
  unsigned alignment, tail, padded_length;
  float recovery_percentage;

  //make_kmc
  for (unsigned int k = 1; k <= 12; k++) {
    for (unsigned int m = 1; (m <= k) && (k + m <= 20); m++) {
      for (unsigned int c = 1; c <= m; c++) {
	sprintf(param[i].sk, "%u", k);
	sprintf(param[i].sm, "%u", m);
	sprintf(param[i].sc, "%u", c);

	param[i].k = param[i].sk;
	param[i].m = param[i].sm;
	param[i].c = param[i].sc;

	alignment = k * 8 * sizeof(int);
	tail = kObjectSize % alignment;
	padded_length = kObjectSize + (tail ? (alignment - tail) : 0);
	param[i].ch_size = padded_length / k;
	i++;
      }
    }
  }

  vector<const char*> args;
  argv_to_vec(argc, (const char **) argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  const char* env = getenv("CEPH_LIB");
  string directory(env ? env : ".libs");
  g_conf->set_val("erasure_code_dir", directory, false, false);

  ::testing::InitGoogleTest(&argc, argv);

  r = RUN_ALL_TESTS();

  std::cout << "minimum_to_decode:recover_num = " << g_recover << std::endl;
  std::cout << "minimum_to_decode:cannot_recover_num = " << g_cannot_recover
      << std::endl;
  recovery_percentage = 100.0
      - (float) (100.0 * g_cannot_recover / (g_recover + g_cannot_recover));
  printf("recovery_percentage:%f\n",recovery_percentage);
  if (recovery_percentage > 99.0) {
    std::cout << "[       OK ] Recovery percentage is more than 99.0%"
	<< std::endl;
  } else {
    std::cout << "[       NG ] Recovery percentage is less than 99.0%"
	<< std::endl;
  }
  std::cout << "cannot recovery patterns:" << std::endl;
  for (std::vector<Recover_d>::const_iterator i = cannot_recover.begin();
       i != cannot_recover.end(); ++i) {
    std::cout << "---" << std::endl;
    std::cout << "k = " << i->k << ", m = " << i->m << ", c = " << i->c
	<< std::endl;
    std::cout << "want_to_decode  :" << i->want << std::endl;
    std::cout << "available_chunks:" << i->avail << std::endl;
  }
  std::cout << "---" << std::endl;

  return r;
}
