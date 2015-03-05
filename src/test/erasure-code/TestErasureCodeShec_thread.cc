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

// SUMMARY: TestErasureCodeShec executes some threads at the same time

#include <errno.h>
#include <pthread.h>

#include "crush/CrushWrapper.h"
#include "osd/osd_types.h"
#include "include/stringify.h"
#include "global/global_init.h"
#include "erasure-code/shec/ErasureCodeShec.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "gtest/gtest.h"

void* thread1(void* pParam);

class TestParam {
public:
  string k, m, c, w;
};

TEST(ErasureCodeShec, thread)
{
  TestParam param1, param2, param3, param4, param5;
  param1.k = "6";
  param1.m = "4";
  param1.c = "3";
  param1.w = "8";

  param2.k = "4";
  param2.m = "3";
  param2.c = "2";
  param2.w = "16";

  param3.k = "10";
  param3.m = "8";
  param3.c = "4";
  param3.w = "32";

  param4.k = "5";
  param4.m = "5";
  param4.c = "5";
  param4.w = "8";

  param5.k = "9";
  param5.m = "9";
  param5.c = "6";
  param5.w = "16";

  pthread_t tid1, tid2, tid3, tid4, tid5;
  pthread_create(&tid1, NULL, thread1, (void*) &param1);
  std::cout << "thread1 start " << std::endl;
  pthread_create(&tid2, NULL, thread1, (void*) &param2);
  std::cout << "thread2 start " << std::endl;
  pthread_create(&tid3, NULL, thread1, (void*) &param3);
  std::cout << "thread3 start " << std::endl;
  pthread_create(&tid4, NULL, thread1, (void*) &param4);
  std::cout << "thread4 start " << std::endl;
  pthread_create(&tid5, NULL, thread1, (void*) &param5);
  std::cout << "thread5 start " << std::endl;

  pthread_join(tid1, NULL);
  pthread_join(tid2, NULL);
  pthread_join(tid3, NULL);
  pthread_join(tid4, NULL);
  pthread_join(tid5, NULL);
}

int main(int argc, char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **) argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

void* thread1(void* pParam)
{
  TestParam* param = static_cast<TestParam*>(pParam);

  time_t start, end;

  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();

  instance.disable_dlclose = true;
  {
    Mutex::Locker l(instance.lock);
    __erasure_code_init((char*) "shec", (char*) "");
  }
  std::cout << "__erasure_code_init finish " << std::endl;

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789" //length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
	    "012345"//192
  );

  //decode
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
  map<int, bufferlist> decoded;
  bufferlist out1, out2, usable;

  time(&start);
  time(&end);
  const int kTestSec = 60;
  ErasureCodeShecTableCache tcache;

  while (kTestSec >= (end - start)) {
    //init
    int r;
    ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				    tcache,
				    ErasureCodeShec::MULTIPLE);
    map < std::string, std::string > *parameters = new map<std::string,
							   std::string>();
    (*parameters)["plugin"] = "shec";
    (*parameters)["technique"] = "multiple";
    (*parameters)["ruleset-failure-domain"] = "osd";
    (*parameters)["k"] = param->k;
    (*parameters)["m"] = param->m;
    (*parameters)["c"] = param->c;
    (*parameters)["w"] = param->w;
    r = shec->init(*parameters);

    int i_k = std::atoi(param->k.c_str());
    int i_m = std::atoi(param->m.c_str());
    int i_c = std::atoi(param->c.c_str());
    int i_w = std::atoi(param->w.c_str());

    EXPECT_EQ(0, r);
    EXPECT_EQ(i_k, shec->k);
    EXPECT_EQ(i_m, shec->m);
    EXPECT_EQ(i_c, shec->c);
    EXPECT_EQ(i_w, shec->w);
    EXPECT_EQ(ErasureCodeShec::MULTIPLE, shec->technique);
    EXPECT_STREQ("default", shec->ruleset_root.c_str());
    EXPECT_STREQ("osd", shec->ruleset_failure_domain.c_str());
    EXPECT_TRUE(shec->matrix != NULL);
    if ((shec->matrix == NULL)) {
      std::cout << "matrix is null" << std::endl;
      // error
      break;
    }

    //encode
    for (unsigned int i = 0; i < shec->get_chunk_count(); i++) {
      want_to_encode.insert(i);
    }
    r = shec->encode(want_to_encode, in, &encoded);

    EXPECT_EQ(0, r);
    EXPECT_EQ(shec->get_chunk_count(), encoded.size());
    EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

    if (r != 0) {
      std::cout << "error in encode" << std::endl;
      //error
      break;
    }

    //decode
    r = shec->decode(set<int>(want_to_decode, want_to_decode + 2),
		     encoded,
		     &decoded);

    EXPECT_EQ(0, r);
    EXPECT_EQ(2u, decoded.size());
    EXPECT_EQ(shec->get_chunk_size(in.length()), decoded[0].length());

    if (r != 0) {
      std::cout << "error in decode" << std::endl;
      //error
      break;
    }

    //out1 is "encoded"
    for (unsigned int i = 0; i < encoded.size(); i++) {
      out1.append(encoded[i]);
    }
    //out2 is "decoded"
    shec->decode_concat(encoded, &out2);
    usable.substr_of(out2, 0, in.length());
    EXPECT_FALSE(out1 == in);
    EXPECT_TRUE(usable == in);
    if (out1 == in || !(usable == in)) {
      std::cout << "encode(decode) result is not correct" << std::endl;
      break;
    }

    delete shec;
    delete parameters;
    want_to_encode.clear();
    encoded.clear();
    decoded.clear();
    out1.clear();
    out2.clear();
    usable.clear();

    time(&end);
  }

  return NULL;
}
