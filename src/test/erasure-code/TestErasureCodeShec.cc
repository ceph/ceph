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
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["directory"] = "/usr/lib64/ceph/erasure-code";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";

  int r = shec->init(*parameters);

  //check parameters
  EXPECT_EQ(6u, shec->k);
  EXPECT_EQ(4u, shec->m);
  EXPECT_EQ(3u, shec->c);
  EXPECT_EQ(8u, shec->w);
  EXPECT_EQ(ErasureCodeShec::MULTIPLE, shec->technique);
  EXPECT_STREQ("default", shec->ruleset_root.c_str());
  EXPECT_STREQ("osd", shec->ruleset_failure_domain.c_str());
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_2)
{
  //all parameters are normal values
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-root"] = "test";
  (*parameters)["ruleset-failure-domain"] = "host";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  (*parameters)["w"] = "8";

  int r = shec->init(*parameters);

  //check parameters
  EXPECT_EQ(6u, shec->k);
  EXPECT_EQ(4u, shec->m);
  EXPECT_EQ(3u, shec->c);
  EXPECT_EQ(8u, shec->w);
  EXPECT_EQ(ErasureCodeShec::MULTIPLE, shec->technique);
  EXPECT_STREQ("test", shec->ruleset_root.c_str());
  EXPECT_STREQ("host", shec->ruleset_failure_domain.c_str());
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_3)
{
  //all parameters are normal values
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  (*parameters)["w"] = "16";

  int r = shec->init(*parameters);

  //check parameters
  EXPECT_EQ(6u, shec->k);
  EXPECT_EQ(4u, shec->m);
  EXPECT_EQ(3u, shec->c);
  EXPECT_EQ(16u, shec->w);
  EXPECT_EQ(ErasureCodeShec::MULTIPLE, shec->technique);
  EXPECT_STREQ("default", shec->ruleset_root.c_str());
  EXPECT_STREQ("osd", shec->ruleset_failure_domain.c_str());
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_4)
{
  //all parameters are normal values
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  (*parameters)["w"] = "32";

  int r = shec->init(*parameters);

  //check parameters
  EXPECT_EQ(6u, shec->k);
  EXPECT_EQ(4u, shec->m);
  EXPECT_EQ(3u, shec->c);
  EXPECT_EQ(32u, shec->w);
  EXPECT_EQ(ErasureCodeShec::MULTIPLE, shec->technique);
  EXPECT_STREQ("default", shec->ruleset_root.c_str());
  EXPECT_STREQ("osd", shec->ruleset_failure_domain.c_str());
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_5)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  //plugin is not specified
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";

  int r = shec->init(*parameters);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_6)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "jerasure";	//unexpected value
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";

  int r = shec->init(*parameters);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_7)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "abc";	//unexpected value
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";

  int r = shec->init(*parameters);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_8)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["directory"] = "/usr/lib64/";	//unexpected value
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";

  int r = shec->init(*parameters);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_9)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-root"] = "abc";	//unexpected value
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";

  int r = shec->init(*parameters);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_10)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "abc";	//unexpected value
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";

  int r = shec->init(*parameters);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_11)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "abc";		//unexpected value
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";

  int r = shec->init(*parameters);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_12)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "-1";	//unexpected value
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";

  int r = shec->init(*parameters);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_13)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "abc";
  (*parameters)["k"] = "0.1";	//unexpected value
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";

  int r = shec->init(*parameters);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_14)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "a";		//unexpected value
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";

  int r = shec->init(*parameters);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_15)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  //k is not specified
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";

  int r = shec->init(*parameters);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_16)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "-1";		//unexpected value
  (*parameters)["c"] = "3";

  int r = shec->init(*parameters);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_17)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "0.1";		//unexpected value
  (*parameters)["c"] = "3";

  int r = shec->init(*parameters);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_18)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "a";		//unexpected value
  (*parameters)["c"] = "3";

  int r = shec->init(*parameters);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_19)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  //m is not specified
  (*parameters)["c"] = "3";

  int r = shec->init(*parameters);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_20)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "-1";		//unexpected value

  int r = shec->init(*parameters);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_21)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "0.1";		//unexpected value

  int r = shec->init(*parameters);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_22)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "a";		//unexpected value

  int r = shec->init(*parameters);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_23)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  //c is not specified

  int r = shec->init(*parameters);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_24)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  (*parameters)["w"] = "1";		//unexpected value

  int r = shec->init(*parameters);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(6u, shec->k);
  EXPECT_EQ(4u, shec->m);
  EXPECT_EQ(3u, shec->c);
  EXPECT_EQ(8u, shec->w);
  //w is default value

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_25)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  (*parameters)["w"] = "-1";		//unexpected value

  int r = shec->init(*parameters);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(6u, shec->k);
  EXPECT_EQ(4u, shec->m);
  EXPECT_EQ(3u, shec->c);
  EXPECT_EQ(8u, shec->w);
  //w is default value

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_26)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  (*parameters)["w"] = "0.1";		//unexpected value

  int r = shec->init(*parameters);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(6u, shec->k);
  EXPECT_EQ(4u, shec->m);
  EXPECT_EQ(3u, shec->c);
  EXPECT_EQ(8u, shec->w);
  //w is default value

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_27)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  (*parameters)["w"] = "a";		//unexpected value

  int r = shec->init(*parameters);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(6u, shec->k);
  EXPECT_EQ(4u, shec->m);
  EXPECT_EQ(3u, shec->c);
  EXPECT_EQ(8u, shec->w);
  //w is default value

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_28)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "10";	//c > m

  int r = shec->init(*parameters);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_29)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  //k is not specified
  //m is not specified
  //c is not specified

  int r = shec->init(*parameters);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  //k,m,c are default values
  EXPECT_EQ(4u, shec->k);
  EXPECT_EQ(3u, shec->m);
  EXPECT_EQ(2u, shec->c);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_30)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "12";
  (*parameters)["m"] = "8";
  (*parameters)["c"] = "8";

  int r = shec->init(*parameters);

  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(12u, shec->k);
  EXPECT_EQ(8u, shec->m);
  EXPECT_EQ(8u, shec->c);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_31)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "13";
  (*parameters)["m"] = "7";
  (*parameters)["c"] = "7";

  int r = shec->init(*parameters);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_32)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "7";
  (*parameters)["m"] = "13";
  (*parameters)["c"] = "13";

  int r = shec->init(*parameters);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_33)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "12";
  (*parameters)["m"] = "9";
  (*parameters)["c"] = "8";

  int r = shec->init(*parameters);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init_34)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "8";
  (*parameters)["m"] = "12";
  (*parameters)["c"] = "12";

  int r = shec->init(*parameters);

  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init2_4)
{
  //all parameters are normal values
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);
  int r = shec->init(*parameters);	//init executed twice

  //check parameters
  EXPECT_EQ(6u, shec->k);
  EXPECT_EQ(4u, shec->m);
  EXPECT_EQ(3u, shec->c);
  EXPECT_EQ(8u, shec->w);
  EXPECT_EQ(ErasureCodeShec::MULTIPLE, shec->technique);
  EXPECT_STREQ("default", shec->ruleset_root.c_str());
  EXPECT_STREQ("osd", shec->ruleset_failure_domain.c_str());
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, init2_5)
{
  //all parameters are normal values
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  map < std::string, std::string > *parameters2 = new map<std::string,
      std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "host";
  (*parameters)["k"] = "10";
  (*parameters)["m"] = "6";
  (*parameters)["c"] = "5";
  (*parameters)["w"] = "16";

  int r = shec->init(*parameters);

  //reexecute init
  (*parameters2)["plugin"] = "shec";
  (*parameters2)["technique"] = "";
  (*parameters2)["ruleset-failure-domain"] = "osd";
  (*parameters2)["k"] = "6";
  (*parameters2)["m"] = "4";
  (*parameters2)["c"] = "3";
  shec->init(*parameters2);

  EXPECT_EQ(6u, shec->k);
  EXPECT_EQ(4u, shec->m);
  EXPECT_EQ(3u, shec->c);
  EXPECT_EQ(8u, shec->w);
  EXPECT_EQ(ErasureCodeShec::MULTIPLE, shec->technique);
  EXPECT_STREQ("default", shec->ruleset_root.c_str());
  EXPECT_STREQ("osd", shec->ruleset_failure_domain.c_str());
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, minimum_to_decode_1)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

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
  delete parameters;
}

TEST(ErasureCodeShec, minimum_to_decode_2)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //minimum_to_decode
  set<int> want_to_decode;
  set<int> available_chunks;
  set<int> minimum_chunks;

  for (int i = 0; i < 10; i++) {
    want_to_decode.insert(i);
    available_chunks.insert(i);
  }

  int r = shec->minimum_to_decode(want_to_decode, available_chunks,
				  &minimum_chunks);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_TRUE(minimum_chunks.size());

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, minimum_to_decode_3)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //minimum_to_decode
  set<int> want_to_decode;
  set<int> available_chunks;
  set<int> minimum_chunks;

  for (int i = 0; i < 32; i++) {		//want_to_decode.size() > k+m
    want_to_decode.insert(i);
    available_chunks.insert(i);
  }

  int r = shec->minimum_to_decode(want_to_decode, available_chunks,
				  &minimum_chunks);
  EXPECT_EQ(-EINVAL, r);
  EXPECT_EQ(0, minimum_chunks.size());

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, minimum_to_decode_4)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //minimum_to_decode
  set<int> want_to_decode;
  set<int> available_chunks;
  set<int> minimum_chunks;

  for (int i = 0; i < 9; i++) {
    want_to_decode.insert(i);
    available_chunks.insert(i);
  }
  want_to_decode.insert(100);
  available_chunks.insert(100);

  int r = shec->minimum_to_decode(want_to_decode, available_chunks,
				  &minimum_chunks);
  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, minimum_to_decode_5)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //minimum_to_decode
  set<int> want_to_decode;
  set<int> available_chunks;
  set<int> minimum_chunks;

  for (int i = 0; i < 10; i++) {
    want_to_decode.insert(i);
  }
  for (int i = 0; i < 32; i++) {		//available_chunks.size() > k+m
    available_chunks.insert(i);
  }

  int r = shec->minimum_to_decode(want_to_decode, available_chunks,
				  &minimum_chunks);
  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, minimum_to_decode_6)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //minimum_to_decode
  set<int> want_to_decode;
  set<int> available_chunks;
  set<int> minimum_chunks;

  for (int i = 0; i < 9; i++) {
    want_to_decode.insert(i);
    available_chunks.insert(i);
  }
  available_chunks.insert(100);

  int r = shec->minimum_to_decode(want_to_decode, available_chunks,
				  &minimum_chunks);
  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, minimum_to_decode_7)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //minimum_to_decode
  set<int> want_to_decode;
  set<int> available_chunks;
  set<int> minimum_chunks;

  want_to_decode.insert(1);
  want_to_decode.insert(3);
  want_to_decode.insert(5);
  available_chunks.insert(1);
  available_chunks.insert(3);
  available_chunks.insert(6);

  int r = shec->minimum_to_decode(want_to_decode, available_chunks,
				  &minimum_chunks);
  EXPECT_EQ(-EIO, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, minimum_to_decode_8)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //minimum_to_decode
  set<int> want_to_decode;
  set<int> available_chunks;
  //minimum_chunks is NULL

  for (int i = 0; i < 10; i++) {
    want_to_decode.insert(i);
    available_chunks.insert(i);
  }

  int r = shec->minimum_to_decode(want_to_decode, available_chunks, NULL);
  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, minimum_to_decode_9)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //minimum_to_decode
  set<int> want_to_decode;
  set<int> available_chunks;
  set<int> minimum_chunks, minimum;

  for (int i = 0; i < 10; i++) {
    want_to_decode.insert(i);
    available_chunks.insert(i);
  }
  shec->minimum_to_decode(want_to_decode, available_chunks, &minimum_chunks);
  minimum = minimum_chunks;		//normal value
  for (int i = 100; i < 120; i++) {
    minimum_chunks.insert(i);	//insert extra data
  }

  int r = shec->minimum_to_decode(want_to_decode, available_chunks,
				  &minimum_chunks);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(minimum, minimum_chunks);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, minimum_to_decode2_1)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

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
  delete parameters;
}

TEST(ErasureCodeShec, minimum_to_decode2_3)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

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
  delete parameters;
}

TEST(ErasureCodeShec, minimum_to_decode_with_cost_1)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //minimum_to_decode_with_cost
  set<int> want_to_decode;
  map<int, int> available_chunks;
  set<int> minimum_chunks;

  want_to_decode.insert(0);
  available_chunks[0] = 0;
  available_chunks[1] = 1;
  available_chunks[2] = 2;

  int r = shec->minimum_to_decode_with_cost(want_to_decode, available_chunks,
					    &minimum_chunks);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_TRUE(minimum_chunks.size());

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, minimum_to_decode_with_cost_2_3)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

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
  delete parameters;
}

TEST(ErasureCodeShec, encode_1)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

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

  int r = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, r);
  EXPECT_EQ(shec->get_chunk_count(), encoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

  //decode
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
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
  for (unsigned int i = 0; i < encoded.size(); i++) {
    out1.append(encoded[i]);
  }
  //out2 is "decoded"
  r = shec->decode_concat(encoded, &out2);
  usable.substr_of(out2, 0, in.length());
  EXPECT_FALSE(out1 == in);
  EXPECT_TRUE(usable == in);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, encode_2)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); i++) {
    want_to_encode.insert(i);
  }

  int r = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, r);
  EXPECT_EQ(shec->get_chunk_count(), encoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

  //decode
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
  map<int, bufferlist> decoded;
  r = shec->decode(set<int>(want_to_decode, want_to_decode + 2), encoded,
		   &decoded);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(2u, decoded.size());
  EXPECT_EQ(32u, decoded[0].length());

  bufferlist out1, out2, usable;
  //out1 is "encoded"
  for (unsigned int i = 0; i < encoded.size(); i++)
    out1.append(encoded[i]);
  //out2 is "decoded"
  shec->decode_concat(encoded, &out2);
  usable.substr_of(out2, 0, in.length());
  EXPECT_FALSE(out1 == in);
  EXPECT_TRUE(usable == in);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, encode_3)
{
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  bufferlist in;
  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
  );
  set<int> want_to_encode;
  for (unsigned int i = 0; i < shec->get_chunk_count(); i++) {
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
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
  map<int, bufferlist> decoded;
  r = shec->decode(set<int>(want_to_decode, want_to_decode + 2), encoded,
		   &decoded);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(2u, decoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), decoded[0].length());

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

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, encode_4)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
  );
  for (unsigned int i = 0; i < shec->get_chunk_count() - 1; i++) {
    want_to_encode.insert(i);
  }
  want_to_encode.insert(100);

  int r = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, r);
  EXPECT_EQ(shec->get_chunk_count()-1, encoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

  //decode
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
  map<int, bufferlist> decoded;
  r = shec->decode(set<int>(want_to_decode, want_to_decode + 2), encoded,
		   &decoded);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(2u, decoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), decoded[0].length());

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

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, encode_8)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //encode
  bufferlist in;
  set<int> want_to_encode;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); i++) {
    want_to_encode.insert(i);
  }

  int r = shec->encode(want_to_encode, in, NULL);	//encoded = NULL
  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, encode_9)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); i++) {
    want_to_encode.insert(i);
  }
  for (int i = 0; i < 100; i++) {
    encoded[i].append("ABCDEFGHIJKLMNOPQRSTUVWXYZ");
  }

  int r = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(-EINVAL, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, encode2_1)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

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

  int r = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, r);
  EXPECT_EQ(shec->get_chunk_count(), encoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

  //decode
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
  map<int, bufferlist> decoded;
  r = shec->decode(set<int>(want_to_decode, want_to_decode + 2), encoded,
		   &decoded);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(2u, decoded.size());
  EXPECT_EQ(32u, decoded[0].length());

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

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, encode2_3)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

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
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
  map<int, bufferlist> decoded;

  r = shec->decode(set<int>(want_to_decode, want_to_decode + 2), encoded,
		   &decoded);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(2u, decoded.size());
  EXPECT_EQ(32u, decoded[0].length());

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

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, decode_1)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//248
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); i++) {
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

  bufferlist out, usable;
  shec->decode_concat(encoded, &out);
  usable.substr_of(out, 0, in.length());
  EXPECT_TRUE(usable == in);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, decode_2)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

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

  bufferlist out, usable;
  shec->decode_concat(encoded, &out);
  usable.substr_of(out, 0, in.length());
  EXPECT_TRUE(usable == in);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, decode_3)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//248
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); i++) {
    want_to_encode.insert(i);
  }

  int r = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, r);
  EXPECT_EQ(shec->get_chunk_count(), encoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

  // all chunks are available
  //decode
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 }; //more than k+m
  map<int, bufferlist> decoded;

  r = shec->decode(set<int>(want_to_decode, want_to_decode + 11), encoded,
		   &decoded);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(10u, decoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), decoded[0].length());

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

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, decode_4)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//248
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); i++) {
    want_to_encode.insert(i);
  }

  int r = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, r);
  EXPECT_EQ(shec->get_chunk_count(), encoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

  // all chunks are available
  //decode
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 100 };
  map<int, bufferlist> decoded;

  r = shec->decode(set<int>(want_to_decode, want_to_decode + 9), encoded,
		   &decoded);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(10u, decoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), decoded[0].length());

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

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, decode_7)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//248
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); i++) {
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

  r = shec->decode(set<int>(want_to_decode, want_to_decode + 2), encoded,
		   &decoded);
  EXPECT_TRUE(shec->matrix != NULL);
  EXPECT_EQ(0, r);
  EXPECT_EQ(2u, decoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), decoded[0].length());

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

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, decode_8)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//248
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); i++) {
    want_to_encode.insert(i);
  }

  int r = shec->encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, r);
  EXPECT_EQ(shec->get_chunk_count(), encoded.size());
  EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

  // all chunks are available
  //decode
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

  //decoded = NULL
  r = shec->decode(set<int>(want_to_decode, want_to_decode + 2), encoded,
		   NULL);
  EXPECT_NE(0, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, decode_9)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//248
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); i++) {
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
  buf.append("a");
  for (int i = 0; i < 100; i++) {
    decoded[i] = buf;
  }

  r = shec->decode(set<int>(want_to_decode, want_to_decode + 2), encoded,
		   &decoded);
  EXPECT_NE(0, r);

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, decode2_1)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//248
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); i++) {
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
  delete parameters;
}

TEST(ErasureCodeShec, decode2_3)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//248
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); i++) {
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
  delete parameters;
}

TEST(ErasureCodeShec, decode2_4)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//248
  );
  for (unsigned int i = 0; i < shec->get_chunk_count(); i++) {
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
  delete parameters;
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
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //create_ruleset
  stringstream ss;

  int r = shec->create_ruleset("myrule", *crush, &ss);
  EXPECT_EQ(0, r);
  EXPECT_STREQ("myrule", crush->rule_name_map[0].c_str());

  //reexecute create_ruleset
  r = shec->create_ruleset("myrule", *crush, &ss);
  EXPECT_EQ(-EEXIST, r);

  delete shec;
  delete parameters;
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
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //create_ruleset
  int r = shec->create_ruleset("myrule", *crush, NULL);	//ss = NULL
  EXPECT_EQ(0, r);

  delete shec;
  delete parameters;
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
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //create_ruleset
  stringstream ss;

  int r = shec->create_ruleset("myrule", *crush, &ss);
  EXPECT_EQ(0, r);
  EXPECT_STREQ("myrule", crush->rule_name_map[0].c_str());

  delete shec;
  delete parameters;
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
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

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
  delete parameters;
  delete crush;
}

TEST(ErasureCodeShec, get_chunk_count_1)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //get_chunk_count
  EXPECT_EQ(10u, shec->get_chunk_count());

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, get_data_chunk_count_1)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  shec->init(*parameters);

  //get_data_chunk_count
  EXPECT_EQ(6u, shec->get_data_chunk_count());

  delete shec;
  delete parameters;
}

TEST(ErasureCodeShec, get_chunk_size_1_2)
{
  //init
  ErasureCodeShecTableCache tcache;
  ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
				  tcache,
				  ErasureCodeShec::MULTIPLE);
  map < std::string, std::string > *parameters = new map<std::string,
							 std::string>();
  (*parameters)["plugin"] = "shec";
  (*parameters)["technique"] = "";
  (*parameters)["ruleset-failure-domain"] = "osd";
  (*parameters)["k"] = "6";
  (*parameters)["m"] = "4";
  (*parameters)["c"] = "3";
  (*parameters)["w"] = "8";
  shec->init(*parameters);

  //when there is no padding(192=k*w*4)
  EXPECT_EQ(32u, shec->get_chunk_size(192));
  //when there is padding(190=k*w*4-2)
  EXPECT_EQ(32u, shec->get_chunk_size(190));

  delete shec;
  delete parameters;
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
    i++;
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
  for (unsigned int i = 0; i < shec->get_chunk_count(); i++) {
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
  for (unsigned int i = 0; i < shec->get_chunk_count(); i++) {
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
