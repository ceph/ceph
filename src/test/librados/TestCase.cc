// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librados/test.h"
#include "test/librados/TestCase.h"

using namespace librados;

std::string RadosTest::pool_name;
rados_t RadosTest::s_cluster = NULL;

void RadosTest::SetUpTestCase()
{
  pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &s_cluster));
}

void RadosTest::TearDownTestCase()
{
  ASSERT_EQ(0, destroy_one_pool(pool_name, &s_cluster));
}

void RadosTest::SetUp()
{
  cluster = RadosTest::s_cluster;
  ASSERT_EQ(0, rados_ioctx_create(cluster, pool_name.c_str(), &ioctx));
  std::string nspace = get_temp_pool_name();
  rados_ioctx_set_namespace(ioctx, nspace.c_str());
}

void RadosTest::TearDown()
{
  rados_ioctx_destroy(ioctx);
}

std::string RadosTestPP::pool_name;
Rados RadosTestPP::s_cluster;

void RadosTestPP::SetUpTestCase()
{
  pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, s_cluster));
}

void RadosTestPP::TearDownTestCase()
{
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, s_cluster));
}

void RadosTestPP::SetUp()
{
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));
  ns = get_temp_pool_name();
  ioctx.set_namespace(ns);
}

void RadosTestPP::TearDown()
{
  ioctx.close();
}
