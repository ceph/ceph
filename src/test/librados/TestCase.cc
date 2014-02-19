// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
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
  cleanup_default_namespace(ioctx);
  rados_ioctx_destroy(ioctx);
}

void RadosTest::cleanup_default_namespace(rados_ioctx_t ioctx)
{
  // remove all objects from the default namespace to avoid polluting
  // other tests
  rados_ioctx_set_namespace(ioctx, "");
  rados_list_ctx_t list_ctx;
  ASSERT_EQ(0, rados_objects_list_open(ioctx, &list_ctx));
  int r;
  const char *entry = NULL;
  const char *key = NULL;
  while ((r = rados_objects_list_next(list_ctx, &entry, &key)) != -ENOENT) {
    ASSERT_EQ(0, r);
    rados_ioctx_locator_set_key(ioctx, key);
    ASSERT_EQ(0, rados_remove(ioctx, entry));
  }
  rados_objects_list_close(list_ctx);
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
  cleanup_default_namespace(ioctx);
  ioctx.close();
}

void RadosTestPP::cleanup_default_namespace(librados::IoCtx ioctx)
{
  // remove all objects from the default namespace to avoid polluting
  // other tests
  ioctx.set_namespace("");
  for (ObjectIterator it = ioctx.objects_begin();
       it != ioctx.objects_end(); ++it) {
    ioctx.locator_set_key(it->second);
    ASSERT_EQ(0, ioctx.remove(it->first));
  }
}
