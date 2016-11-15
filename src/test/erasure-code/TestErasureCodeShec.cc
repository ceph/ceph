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

//SUMMARY: TestErasureCodeShec

#include <errno.h>
#include <pthread.h>
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
#include "test/unit.h"

void* thread1(void* pParam);
void* thread2(void* pParam);
void* thread3(void* pParam);
void* thread4(void* pParam);
void* thread5(void* pParam);

static int g_flag = 0;

TEST(ErasureCodeShec, init_1)
{
  //all parameters are normal values
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";

  int r = shec->init(*profile, &cerr);

  //check profile
  EXPECT_EQ(4, shec->k);
  EXPECT_EQ(3, shec->m);
  EXPECT_EQ(2, shec->c);
  EXPECT_EQ(8, shec->w);
  EXPECT_EQ(ErasureCodeShec::MULTIPLE, shec->technique);
  EXPECT_STREQ("default", shec->ruleset_root.c_str());
  EXPECT_STREQ("osd", shec->ruleset_failure_domain.c_str());
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_2)
{
  //all parameters are normal values
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-root"] = "test";
  (*profile)["ruleset-failure-domain"] = "host";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  (*profile)["w"] = "8";

  int r = shec->init(*profile, &cerr);

  //check profile
  EXPECT_EQ(4, shec->k);
  EXPECT_EQ(3, shec->m);
  EXPECT_EQ(2, shec->c);
  EXPECT_EQ(8, shec->w);
  EXPECT_EQ(ErasureCodeShec::MULTIPLE, shec->technique);
  EXPECT_STREQ("test", shec->ruleset_root.c_str());
  EXPECT_STREQ("host", shec->ruleset_failure_domain.c_str());
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_3)
{
  //all parameters are normal values
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  (*profile)["w"] = "16";

  int r = shec->init(*profile, &cerr);

  //check profile
  EXPECT_EQ(4, shec->k);
  EXPECT_EQ(3, shec->m);
  EXPECT_EQ(2, shec->c);
  EXPECT_EQ(16, shec->w);
  EXPECT_EQ(ErasureCodeShec::MULTIPLE, shec->technique);
  EXPECT_STREQ("default", shec->ruleset_root.c_str());
  EXPECT_STREQ("osd", shec->ruleset_failure_domain.c_str());
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_4)
{
  //all parameters are normal values
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  (*profile)["w"] = "32";

  int r = shec->init(*profile, &cerr);

  //check profile
  EXPECT_EQ(4, shec->k);
  EXPECT_EQ(3, shec->m);
  EXPECT_EQ(2, shec->c);
  EXPECT_EQ(32, shec->w);
  EXPECT_EQ(ErasureCodeShec::MULTIPLE, shec->technique);
  EXPECT_STREQ("default", shec->ruleset_root.c_str());
  EXPECT_STREQ("osd", shec->ruleset_failure_domain.c_str());
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_5)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  //plugin is not specified
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";

  int r = shec->init(*profile, &cerr);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_6)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "jerasure";	//unexpected value
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";

  int r = shec->init(*profile, &cerr);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_7)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "abc";	//unexpected value
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";

  int r = shec->init(*profile, &cerr);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_8)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";

  int r = shec->init(*profile, &cerr);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_9)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-root"] = "abc";	//unexpected value
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";

  int r = shec->init(*profile, &cerr);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_10)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "abc";	//unexpected value
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";

  int r = shec->init(*profile, &cerr);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_11)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "abc";		//unexpected value
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";

  int r = shec->init(*profile, &cerr);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_12)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "-1";	//unexpected value
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";

  int r = shec->init(*profile, &cerr);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_13)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "abc";
  (*profile)["k"] = "0.1";	//unexpected value
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";

  int r = shec->init(*profile, &cerr);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_14)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "a";		//unexpected value
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";

  int r = shec->init(*profile, &cerr);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_15)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  //k is not specified
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";

  int r = shec->init(*profile, &cerr);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_16)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "-1";		//unexpected value
  (*profile)["c"] = "2";

  int r = shec->init(*profile, &cerr);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_17)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "0.1";		//unexpected value
  (*profile)["c"] = "2";

  int r = shec->init(*profile, &cerr);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_18)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "a";		//unexpected value
  (*profile)["c"] = "2";

  int r = shec->init(*profile, &cerr);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_19)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  //m is not specified
  (*profile)["c"] = "2";

  int r = shec->init(*profile, &cerr);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_20)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "-1";		//unexpected value

  int r = shec->init(*profile, &cerr);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_21)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "0.1";		//unexpected value

  int r = shec->init(*profile, &cerr);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_22)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "a";		//unexpected value

  int r = shec->init(*profile, &cerr);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_23)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  //c is not specified

  int r = shec->init(*profile, &cerr);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_24)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  (*profile)["w"] = "1";		//unexpected value

  int r = shec->init(*profile, &cerr);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(4, shec->k);
  EXPECT_EQ(3, shec->m);
  EXPECT_EQ(2, shec->c);
  EXPECT_EQ(8, shec->w);
  //w is default value

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_25)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  (*profile)["w"] = "-1";		//unexpected value

  int r = shec->init(*profile, &cerr);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(4, shec->k);
  EXPECT_EQ(3, shec->m);
  EXPECT_EQ(2, shec->c);
  EXPECT_EQ(8, shec->w);
  //w is default value

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_26)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  (*profile)["w"] = "0.1";		//unexpected value

  int r = shec->init(*profile, &cerr);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(4, shec->k);
  EXPECT_EQ(3, shec->m);
  EXPECT_EQ(2, shec->c);
  EXPECT_EQ(8, shec->w);
  //w is default value

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_27)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  (*profile)["w"] = "a";		//unexpected value

  int r = shec->init(*profile, &cerr);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(4, shec->k);
  EXPECT_EQ(3, shec->m);
  EXPECT_EQ(2, shec->c);
  EXPECT_EQ(8, shec->w);
  //w is default value

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_28)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "10";	//c > m

  int r = shec->init(*profile, &cerr);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_29)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  //k is not specified
  //m is not specified
  //c is not specified

  int r = shec->init(*profile, &cerr);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  //k,m,c are default values
  EXPECT_EQ(4, shec->k);
  EXPECT_EQ(3, shec->m);
  EXPECT_EQ(2, shec->c);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_30)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "12";
  (*profile)["m"] = "8";
  (*profile)["c"] = "8";

  int r = shec->init(*profile, &cerr);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(12, shec->k);
  EXPECT_EQ(8, shec->m);
  EXPECT_EQ(8, shec->c);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_31)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "13";
  (*profile)["m"] = "7";
  (*profile)["c"] = "7";

  int r = shec->init(*profile, &cerr);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_32)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "7";
  (*profile)["m"] = "13";
  (*profile)["c"] = "13";

  int r = shec->init(*profile, &cerr);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_33)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "12";
  (*profile)["m"] = "9";
  (*profile)["c"] = "8";

  int r = shec->init(*profile, &cerr);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init_34)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "8";
  (*profile)["m"] = "12";
  (*profile)["c"] = "12";

  int r = shec->init(*profile, &cerr);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init2_4)
{
  //all parameters are normal values
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);
  int r = shec->init(*profile, &cerr);	//init executed twice

  //check profile
  EXPECT_EQ(4, shec->k);
  EXPECT_EQ(3, shec->m);
  EXPECT_EQ(2, shec->c);
  EXPECT_EQ(8, shec->w);
  EXPECT_EQ(ErasureCodeShec::MULTIPLE, shec->technique);
  EXPECT_STREQ("default", shec->ruleset_root.c_str());
  EXPECT_STREQ("osd", shec->ruleset_failure_domain.c_str());
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, init2_5)
{
  //all parameters are normal values
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  ErasureCodeProfile *profile2 = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "host";
  (*profile)["k"] = "10";
  (*profile)["m"] = "6";
  (*profile)["c"] = "5";
  (*profile)["w"] = "16";

  int r = shec->init(*profile, &cerr);

  //reexecute init
  (*profile2)["plugin"] = "shec";
  (*profile2)["technique"] = "";
  (*profile2)["ruleset-failure-domain"] = "osd";
  (*profile2)["k"] = "4";
  (*profile2)["m"] = "3";
  (*profile2)["c"] = "2";
  shec->init(*profile2, &cerr);

  EXPECT_EQ(4, shec->k);
  EXPECT_EQ(3, shec->m);
  EXPECT_EQ(2, shec->c);
  EXPECT_EQ(8, shec->w);
  EXPECT_EQ(ErasureCodeShec::MULTIPLE, shec->technique);
  EXPECT_STREQ("default", shec->ruleset_root.c_str());
  EXPECT_STREQ("osd", shec->ruleset_failure_domain.c_str());
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, minimum_to_decode_8)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //minimum_to_decode
  set<int> want_to_decode;
  set<int> available_chunks;
  set<int> minimum_chunks;

  for (int i = 0; i < 8; ++i) {
    want_to_decode.insert(i);
  }
  for (int i = 0; i < 5; ++i) {
    available_chunks.insert(i);
  }

  int r = shec->minimum_to_decode(want_to_decode, available_chunks,
				  &minimum_chunks);
  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, minimum_to_decode_9)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //minimum_to_decode
  set<int> want_to_decode;
  set<int> available_chunks;
  set<int> minimum_chunks;

  for (int i = 0; i < 4; ++i) {
    want_to_decode.insert(i);
  }
  for (int i = 0; i < 8; ++i) {
    available_chunks.insert(i);
  }

  int r = shec->minimum_to_decode(want_to_decode, available_chunks,
				  &minimum_chunks);
  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, minimum_to_decode_10)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //minimum_to_decode
  set<int> want_to_decode;
  set<int> available_chunks;
  set<int> minimum_chunks;

  for (int i = 0; i < 7; ++i) {
    want_to_decode.insert(i);
  }
  for (int i = 4; i < 7; ++i) {
    available_chunks.insert(i);
  }

  int r = shec->minimum_to_decode(want_to_decode, available_chunks,
				  &minimum_chunks);
  EXPECT_EQ(-EIO, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, minimum_to_decode_11)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //minimum_to_decode
  set<int> want_to_decode;
  set<int> available_chunks;
  set<int> minimum_chunks;

  for (int i = 0; i < 5; ++i) {
    want_to_decode.insert(i);
  }
  for (int i = 4; i < 7; ++i) {
    available_chunks.insert(i);
  }

  int r = shec->minimum_to_decode(want_to_decode, available_chunks,
				  &minimum_chunks);
  EXPECT_EQ(-EIO, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, minimum_to_decode_12)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //minimum_to_decode
  set<int> want_to_decode;
  set<int> available_chunks;
  //minimum_chunks is NULL

  for (int i = 0; i < 7; ++i) {
    want_to_decode.insert(i);
    available_chunks.insert(i);
  }

  int r = shec->minimum_to_decode(want_to_decode, available_chunks, NULL);
  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, minimum_to_decode_13)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //minimum_to_decode
  set<int> want_to_decode;
  set<int> available_chunks;
  set<int> minimum_chunks, minimum;

  for (int i = 0; i < 7; ++i) {
    want_to_decode.insert(i);
    available_chunks.insert(i);
  }
  shec->minimum_to_decode(want_to_decode, available_chunks, &minimum_chunks);
  minimum = minimum_chunks;		//normal value
  for (int i = 100; i < 120; ++i) {
    minimum_chunks.insert(i);	//insert extra data
  }

  int r = shec->minimum_to_decode(want_to_decode, available_chunks,
				  &minimum_chunks);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(minimum, minimum_chunks);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, minimum_to_decode2_1)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //minimum_to_decode
  set<int> want_to_decode;
  set<int> available_chunks;
  set<int> minimum_chunks;

  want_to_decode.insert(0);
  available_chunks.insert(0);
  available_chunks.insert(1);
  available_chunks.insert(2);

  int r = shec->minimum_to_decode(want_to_decode, available_chunks,
				  &minimum_chunks);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_TRUE(minimum_chunks.size());

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, minimum_to_decode2_3)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //minimum_to_decode
  set<int> want_to_decode;
  set<int> available_chunks;
  set<int> minimum_chunks;

  want_to_decode.insert(0);
  want_to_decode.insert(2);
  available_chunks.insert(0);
  available_chunks.insert(1);
  available_chunks.insert(2);
  available_chunks.insert(3);

  pthread_t tid;
  g_flag = 0;
  pthread_create(&tid, NULL, thread1, shec);
  while (g_flag == 0) {
    usleep(1);
  }
  sleep(1);
  printf("*** test start ***\n");
  int r = shec->minimum_to_decode(want_to_decode, available_chunks,
				  &minimum_chunks);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(want_to_decode, minimum_chunks);
  printf("*** test end ***\n");
  g_flag = 0;
  pthread_join(tid, NULL);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, minimum_to_decode_with_cost_1)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //minimum_to_decode_with_cost
  set<int> want_to_decode;
  map<int, int> available_chunks;
  set<int> minimum_chunks;

  for (int i = 0; i < 7; ++i) {
    want_to_decode.insert(i);
    available_chunks.insert(make_pair(i, i));
  }

  int r = shec->minimum_to_decode_with_cost(want_to_decode, available_chunks,
					    &minimum_chunks);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_TRUE(minimum_chunks.size());

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, minimum_to_decode_with_cost_2_3)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //minimum_to_decode_with_cost
  set<int> want_to_decode;
  map<int, int> available_chunks;
  set<int> minimum_chunks;

  want_to_decode.insert(0);
  want_to_decode.insert(2);
  available_chunks[0] = 0;
  available_chunks[1] = 1;
  available_chunks[2] = 2;
  available_chunks[3] = 3;

  pthread_t tid;
  g_flag = 0;
  pthread_create(&tid, NULL, thread2, shec);
  while (g_flag == 0) {
    usleep(1);
  }
  sleep(1);
  printf("*** test start ***\n");
  int r = shec->minimum_to_decode_with_cost(want_to_decode, available_chunks,
					    &minimum_chunks);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(want_to_decode, minimum_chunks);
  printf("*** test end ***\n");
  g_flag = 0;
  pthread_join(tid, NULL);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, encode_1)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "0123"//128
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }

  int r = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, r);
  EXPECT_EQ(shec->get_chunk_count(), encoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

  //decode
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6 };
  map<int, bufferlist> decoded;
  decoded.clear();
  r = shec->decode(set<int>(want_to_decode, want_to_decode + 2),
		   encoded,
		   &decoded);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(2u, decoded.size());
  EXPECT_EQ(32u, decoded[0].length());

  bufferlist out1, out2, usable;
  //out1 is "encoded"
  for (unsigned int i = 0; i < encoded.size(); ++i) {
    out1.append(encoded[i]);
  }
  //out2 is "decoded"
  r = shec->decode_concat(encoded, &out2);
  usable.substr_of(out2, 0, in.length());
  EXPECT_FALSE(out1 == in);
  EXPECT_TRUE(usable == in);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, encode_2)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }

  int r = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, r);
  EXPECT_EQ(shec->get_chunk_count(), encoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

  //decode
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6 };
  map<int, bufferlist> decoded;
  r = shec->decode(set<int>(want_to_decode, want_to_decode + 2), encoded,
		   &decoded);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(2u, decoded.size());
  EXPECT_EQ(32u, decoded[0].length());

  bufferlist out1, out2, usable;
  //out1 is "encoded"
  for (unsigned int i = 0; i < encoded.size(); ++i)
    out1.append(encoded[i]);
  //out2 is "decoded"
  shec->decode_concat(encoded, &out2);
  usable.substr_of(out2, 0, in.length());
  EXPECT_FALSE(out1 == in);
  EXPECT_TRUE(usable == in);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, encode_3)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  bufferlist in;
  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
  );
  set<int> want_to_encode;
  for (unsigned int i = 0; i < shec->get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }
  want_to_encode.insert(10);
  want_to_encode.insert(11);
  map<int, bufferlist> encoded;
  int r = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, r);
  EXPECT_EQ(shec->get_chunk_count(), encoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

  //decode
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6 };
  map<int, bufferlist> decoded;
  r = shec->decode(set<int>(want_to_decode, want_to_decode + 2), encoded,
		   &decoded);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(2u, decoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), decoded[0].length());

  bufferlist out1, out2, usable;
  //out1 is "encoded"
  for (unsigned int i = 0; i < encoded.size(); ++i) {
    out1.append(encoded[i]);
  }
  //out2 is "decoded"
  shec->decode_concat(encoded, &out2);
  usable.substr_of(out2, 0, in.length());
  EXPECT_FALSE(out1 == in);
  EXPECT_TRUE(usable == in);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, encode_4)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
  );
  for (unsigned int i = 0; i < shec->get_chunk_count() - 1; ++i) {
    want_to_encode.insert(i);
  }
  want_to_encode.insert(100);

  int r = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, r);
  EXPECT_EQ(shec->get_chunk_count()-1, encoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

  //decode
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6 };
  map<int, bufferlist> decoded;
  r = shec->decode(set<int>(want_to_decode, want_to_decode + 2), encoded,
		   &decoded);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(2u, decoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), decoded[0].length());

  bufferlist out1, out2, usable;
  //out1 is "encoded"
  for (unsigned int i = 0; i < encoded.size(); ++i) {
    out1.append(encoded[i]);
  }
  //out2 is "decoded"
  shec->decode_concat(encoded, &out2);
  usable.substr_of(out2, 0, in.length());
  EXPECT_FALSE(out1 == in);
  EXPECT_TRUE(usable == in);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, encode_8)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //encode
  bufferlist in;
  set<int> want_to_encode;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }

  int r = shec->encode(want_to_encode, in, NULL);	//encoded = NULL
  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, encode_9)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }
  for (int i = 0; i < 100; ++i) {
    encoded[i].append("ABCDEFGHIJKLMNOPQRSTUVWXYZ");
  }

  int r = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, encode2_1)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "0123"//128
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }

  int r = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, r);
  EXPECT_EQ(shec->get_chunk_count(), encoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

  //decode
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6 };
  map<int, bufferlist> decoded;
  r = shec->decode(set<int>(want_to_decode, want_to_decode + 2), encoded,
		   &decoded);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(2u, decoded.size());
  EXPECT_EQ(32u, decoded[0].length());

  bufferlist out1, out2, usable;
  //out1 is "encoded"
  for (unsigned int i = 0; i < encoded.size(); ++i) {
    out1.append(encoded[i]);
  }
  //out2 is "decoded"
  shec->decode_concat(encoded, &out2);
  usable.substr_of(out2, 0, in.length());
  EXPECT_FALSE(out1 == in);
  EXPECT_TRUE(usable == in);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, encode2_3)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "0123"//128
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }

  pthread_t tid;
  g_flag = 0;
  pthread_create(&tid, NULL, thread4, shec);
  while (g_flag == 0) {
    usleep(1);
  }
  sleep(1);
  printf("*** test start ***\n");
  int r = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, r);
  EXPECT_EQ(shec->get_chunk_count(), encoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());
  printf("*** test end ***\n");
  g_flag = 0;
  pthread_join(tid, NULL);

  //decode
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6 };
  map<int, bufferlist> decoded;

  r = shec->decode(set<int>(want_to_decode, want_to_decode + 2), encoded,
		   &decoded);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(2u, decoded.size());
  EXPECT_EQ(32u, decoded[0].length());

  bufferlist out1, out2, usable;
  //out1 is "encoded"
  for (unsigned int i = 0; i < encoded.size(); ++i) {
    out1.append(encoded[i]);
  }
  //out2 is "decoded"
  shec->decode_concat(encoded, &out2);
  usable.substr_of(out2, 0, in.length());
  EXPECT_FALSE(out1 == in);
  EXPECT_TRUE(usable == in);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, decode_1)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//248
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }

  int r = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, r);
  EXPECT_EQ(shec->get_chunk_count(), encoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

  // all chunks are available
  //decode
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6 };
  map<int, bufferlist> decoded;

  r = shec->decode(set<int>(want_to_decode, want_to_decode + 7), encoded,
		   &decoded);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(7u, decoded.size());

  bufferlist usable;
  int cmp;
  unsigned int c_size = shec->get_chunk_size(in.length());
  for (unsigned int i = 0; i < shec->get_data_chunk_count(); ++i) {
    usable.clear();
    EXPECT_EQ(c_size, decoded[i].length());
    if ( c_size * (i+1) <= in.length() ) {
      usable.substr_of(in, c_size * i, c_size);
      cmp = memcmp(decoded[i].c_str(), usable.c_str(), c_size);
    } else {
      usable.substr_of(in, c_size * i, in.length() % c_size);
      cmp = memcmp(decoded[i].c_str(), usable.c_str(), in.length() % c_size);
    }
    EXPECT_EQ(0, cmp);
  }

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, decode_8)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//248
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }

  int r = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, r);
  EXPECT_EQ(shec->get_chunk_count(), encoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

  // all chunks are available
  //decode
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7 }; //more than k+m
  map<int, bufferlist> decoded;

  r = shec->decode(set<int>(want_to_decode, want_to_decode + 8), encoded,
		   &decoded);
  EXPECT_EQ(0, r);
  EXPECT_EQ(7u, decoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

  bufferlist usable;
  int cmp;
  unsigned int c_size = shec->get_chunk_size(in.length());
  for (unsigned int i = 0; i < shec->get_data_chunk_count(); ++i) {
    usable.clear();
    EXPECT_EQ(c_size, decoded[i].length());
    if ( c_size * (i+1) <= in.length() ) {
      usable.substr_of(in, c_size * i, c_size);
      cmp = memcmp(decoded[i].c_str(), usable.c_str(), c_size);
    } else {
      usable.substr_of(in, c_size * i, in.length() % c_size);
      cmp = memcmp(decoded[i].c_str(), usable.c_str(), in.length() % c_size);
    }
    EXPECT_EQ(0, cmp);
  }

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, decode_9)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//248
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }

  int r = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, r);
  EXPECT_EQ(shec->get_chunk_count(), encoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

  // all chunks are available
  //decode
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
  map<int, bufferlist> decoded;

  //extra data
  bufferlist buf;
  buf.append("abc");
  encoded[100] = buf;

  r = shec->decode(set<int>(want_to_decode, want_to_decode + 10), encoded,
		   &decoded);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(7u, decoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), decoded[0].length());

  bufferlist out1, usable;
  //out1 is "encoded"
  for (unsigned int i = 0; i < encoded.size(); ++i) {
    out1.append(encoded[i]);
  }
  EXPECT_FALSE(out1 == in);
  //usable is "decoded"
  int cmp;
  unsigned int c_size = shec->get_chunk_size(in.length());
  for (unsigned int i = 0; i < shec->get_data_chunk_count(); ++i) {
    usable.clear();
    EXPECT_EQ(c_size, decoded[i].length());
    if ( c_size * (i+1) <= in.length() ) {
      usable.substr_of(in, c_size * i, c_size);
      cmp = memcmp(decoded[i].c_str(), usable.c_str(), c_size);
    } else {
      usable.substr_of(in, c_size * i, in.length() % c_size);
      cmp = memcmp(decoded[i].c_str(), usable.c_str(), in.length() % c_size);
    }
    EXPECT_EQ(0, cmp);
  }

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, decode_10)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//248
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }

  int r = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, r);
  EXPECT_EQ(shec->get_chunk_count(), encoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

  //decode
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6 }; //more than k+m
  map<int, bufferlist> decoded, inchunks;

  for ( unsigned int i = 0; i < 3; ++i) {
    inchunks.insert(make_pair(i, encoded[i]));
  }

  r = shec->decode(set<int>(want_to_decode, want_to_decode + 7), inchunks,
		   &decoded);
  EXPECT_EQ(-1, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, decode_11)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCD"//128
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }

  int r = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, r);
  EXPECT_EQ(shec->get_chunk_count(), encoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

  //decode
  int want_to_decode[] = { 0, 1, 2, 3, 4 };
  map<int, bufferlist> decoded, inchunks;

  for ( unsigned int i = 4; i < 7; ++i) {
    inchunks.insert(make_pair(i, encoded[i]));
  }

  r = shec->decode(set<int>(want_to_decode, want_to_decode + 5), inchunks,
		   &decoded);
  EXPECT_EQ(-1, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, decode_12)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//248
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }

  int r = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, r);
  EXPECT_EQ(shec->get_chunk_count(), encoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

  // all chunks are available
  //decode
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6 };

  //decoded = NULL
  r = shec->decode(set<int>(want_to_decode, want_to_decode + 7), encoded,
		   NULL);
  EXPECT_NE(0, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, decode_13)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//248
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }

  int r = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, r);
  EXPECT_EQ(shec->get_chunk_count(), encoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

  // all chunks are available
  //decode
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6 };
  map<int, bufferlist> decoded;

  //extra data
  bufferlist buf;
  buf.append("a");
  for (int i = 0; i < 100; ++i) {
    decoded[i] = buf;
  }

  r = shec->decode(set<int>(want_to_decode, want_to_decode + 7), encoded,
		   &decoded);
  EXPECT_NE(0, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, decode2_1)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//248
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }

  int r = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, r);
  EXPECT_EQ(shec->get_chunk_count(), encoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

  // all chunks are available
  //decode
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
  map<int, bufferlist> decoded;

  r = shec->decode(set<int>(want_to_decode, want_to_decode + 2), encoded,
		   &decoded);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(2u, decoded.size());

  bufferlist out;
  shec->decode_concat(encoded, &out);
  bufferlist usable;
  usable.substr_of(out, 0, in.length());
  EXPECT_TRUE(usable == in);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, decode2_3)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//248
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }

  int r = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, r);
  EXPECT_EQ(shec->get_chunk_count(), encoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

  // all chunks are available
  //decode
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
  map<int, bufferlist> decoded;

  pthread_t tid;
  g_flag = 0;
  pthread_create(&tid, NULL, thread4, shec);
  while (g_flag == 0) {
    usleep(1);
  }
  sleep(1);
  printf("*** test start ***\n");
  r = shec->decode(set<int>(want_to_decode, want_to_decode + 2), encoded,
		   &decoded);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(2u, decoded.size());
  printf("*** test end ***\n");
  g_flag = 0;
  pthread_join(tid, NULL);

  bufferlist out;
  shec->decode_concat(encoded, &out);
  bufferlist usable;
  usable.substr_of(out, 0, in.length());
  EXPECT_TRUE(usable == in);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, decode2_4)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//248
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }

  int r = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, r);
  EXPECT_EQ(shec->get_chunk_count(), encoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

  //decode
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
  map<int, bufferlist> decoded;

  // cannot recover
  bufferlist out;
  map<int, bufferlist> degraded;
  degraded[0] = encoded[0];

  r = shec->decode(set<int>(want_to_decode, want_to_decode + 2), degraded,
		   &decoded);
  EXPECT_EQ(-1, r);

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, create_ruleset_1_2)
{
  //create ruleset
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

  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //create_ruleset
  stringstream ss;

  int r = shec->create_ruleset("myrule", *crush, &ss);
  EXPECT_EQ(0, r);
  EXPECT_STREQ("myrule", crush->rule_name_map[0].c_str());

  //reexecute create_ruleset
  r = shec->create_ruleset("myrule", *crush, &ss);
  EXPECT_EQ(-EEXIST, r);

  delete shec;
  delete profile;
  delete crush;
}

TEST(ErasureCodeShec, create_ruleset_4)
{
  //create ruleset
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

  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //create_ruleset
  int r = shec->create_ruleset("myrule", *crush, NULL);	//ss = NULL
  EXPECT_EQ(0, r);

  delete shec;
  delete profile;
  delete crush;
}

TEST(ErasureCodeShec, create_ruleset2_1)
{
  //create ruleset
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

  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //create_ruleset
  stringstream ss;

  int r = shec->create_ruleset("myrule", *crush, &ss);
  EXPECT_EQ(0, r);
  EXPECT_STREQ("myrule", crush->rule_name_map[0].c_str());

  delete shec;
  delete profile;
  delete crush;
}

struct CreateRuleset2_3_Param_d {
  ErasureCodeShec *shec;
  CrushWrapper *crush;
};

TEST(ErasureCodeShec, create_ruleset2_3)
{
  //create ruleset
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

  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //create_ruleset
  stringstream ss;

  pthread_t tid;
  g_flag = 0;
  pthread_create(&tid, NULL, thread3, shec);
  while (g_flag == 0) {
    usleep(1);
  }
  sleep(1);
  printf("*** test start ***\n");
  int r = (shec->create_ruleset("myrule", *crush, &ss));
  EXPECT_TRUE(r >= 0);
  printf("*** test end ***\n");
  g_flag = 0;
  pthread_join(tid, NULL);

  delete shec;
  delete profile;
  delete crush;
}

TEST(ErasureCodeShec, get_chunk_count_1)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //get_chunk_count
  EXPECT_EQ(7u, shec->get_chunk_count());

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, get_data_chunk_count_1)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  shec->init(*profile, &cerr);

  //get_data_chunk_count
  EXPECT_EQ(4u, shec->get_data_chunk_count());

  delete shec;
  delete profile;
}

TEST(ErasureCodeShec, get_chunk_size_1_2)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  ErasureCodeProfile *profile = new ErasureCodeProfile();
  (*profile)["plugin"] = "shec";
  (*profile)["technique"] = "";
  (*profile)["ruleset-failure-domain"] = "osd";
  (*profile)["k"] = "4";
  (*profile)["m"] = "3";
  (*profile)["c"] = "2";
  (*profile)["w"] = "8";
  shec->init(*profile, &cerr);

  //when there is no padding(128=k*w*4)
  EXPECT_EQ(32u, shec->get_chunk_size(128));
  //when there is padding(126=k*w*4-2)
  EXPECT_EQ(32u, shec->get_chunk_size(126));

  delete shec;
  delete profile;
}

void* thread1(void* pParam)
{
  ErasureCodeShec* shec = (ErasureCodeShec*) pParam;
  set<int> want_to_decode;
  set<int> available_chunks;
  set<int> minimum_chunks;

  want_to_decode.insert(0);
  want_to_decode.insert(1);
  available_chunks.insert(0);
  available_chunks.insert(1);
  available_chunks.insert(2);

  printf("*** thread loop start ***\n");
  g_flag = 1;
  while (g_flag == 1) {
    shec->minimum_to_decode(want_to_decode, available_chunks, &minimum_chunks);
  }
  printf("*** thread loop end ***\n");

  return NULL;
}

void* thread2(void* pParam)
{
  ErasureCodeShec* shec = (ErasureCodeShec*) pParam;
  set<int> want_to_decode;
  map<int, int> available_chunks;
  set<int> minimum_chunks;

  want_to_decode.insert(0);
  want_to_decode.insert(1);
  available_chunks[0] = 0;
  available_chunks[1] = 1;
  available_chunks[2] = 2;

  printf("*** thread loop start ***\n");
  g_flag = 1;
  while (g_flag == 1) {
    shec->minimum_to_decode_with_cost(want_to_decode, available_chunks,
				      &minimum_chunks);
    minimum_chunks.clear();
  }
  printf("*** thread loop end ***\n");

  return NULL;
}

void* thread3(void* pParam)
{
  ErasureCodeShec* shec = (ErasureCodeShec*) pParam;

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

  stringstream ss;
  int i = 0;
  char name[30];

  printf("*** thread loop start ***\n");
  g_flag = 1;
  while (g_flag == 1) {
    sprintf(name, "myrule%d", i);
    shec->create_ruleset(name, *crush, &ss);
    ++i;
  }
  printf("*** thread loop end ***\n");

  return NULL;
}

void* thread4(void* pParam)
{
  ErasureCodeShec* shec = (ErasureCodeShec*) pParam;

  bufferlist in;
  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//248
  );
  set<int> want_to_encode;
  for (unsigned int i = 0; i < shec->get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }

  map<int, bufferlist> encoded;

  printf("*** thread loop start ***\n");
  g_flag = 1;
  while (g_flag == 1) {
    shec->encode(want_to_encode, in, &encoded);
    encoded.clear();
  }
  printf("*** thread loop end ***\n");

  return NULL;
}

void* thread5(void* pParam)
{
  ErasureCodeShec* shec = (ErasureCodeShec*) pParam;

  bufferlist in;
  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
	  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//248
	  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//310
  );
  set<int> want_to_encode;
  for (unsigned int i = 0; i < shec->get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }
  map<int, bufferlist> encoded;
  shec->encode(want_to_encode, in, &encoded);

  int want_to_decode[] = { 0, 1, 2, 3, 4, 5 };
  map<int, bufferlist> decoded;

  printf("*** thread loop start ***\n");
  g_flag = 1;
  while (g_flag == 1) {
    shec->decode(set<int>(want_to_decode, want_to_decode + 2), encoded,
		 &decoded);
    decoded.clear();
  }
  printf("*** thread loop end ***\n");

  return NULL;
}
