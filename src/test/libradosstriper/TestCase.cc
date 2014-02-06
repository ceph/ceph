// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include "test/librados/test.h"
#include "test/libradosstriper/TestCase.h"

using namespace libradosstriper;

std::string StriperTest::pool_name;
rados_t StriperTest::s_cluster = NULL;

void StriperTest::SetUpTestCase()
{
  pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &s_cluster));
}

void StriperTest::TearDownTestCase()
{
  ASSERT_EQ(0, destroy_one_pool(pool_name, &s_cluster));
}

void StriperTest::SetUp()
{
  cluster = StriperTest::s_cluster;
  ASSERT_EQ(0, rados_ioctx_create(cluster, pool_name.c_str(), &ioctx));
  ASSERT_EQ(0, rados_striper_create(ioctx, &striper));
}

void StriperTest::TearDown()
{
  rados_striper_destroy(striper);
  rados_ioctx_destroy(ioctx);
}

std::string StriperTestPP::pool_name;
librados::Rados StriperTestPP::s_cluster;

void StriperTestPP::SetUpTestCase()
{
  pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, s_cluster));
}

void StriperTestPP::TearDownTestCase()
{
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, s_cluster));
}

void StriperTestPP::SetUp()
{
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));
  ASSERT_EQ(0, RadosStriper::striper_create(ioctx, &striper));
}

// this is pure copy and paste from previous class
// but for the inheritance from TestWithParam
// with gtest >= 1.6, we couldd avoid this by using
// inheritance from WithParamInterface
std::string StriperTestParam::pool_name;
librados::Rados StriperTestParam::s_cluster;

void StriperTestParam::SetUpTestCase()
{
  pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, s_cluster));
}

void StriperTestParam::TearDownTestCase()
{
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, s_cluster));
}

void StriperTestParam::SetUp()
{
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));
  ASSERT_EQ(0, RadosStriper::striper_create(ioctx, &striper));
}
