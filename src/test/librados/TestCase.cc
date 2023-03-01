// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <fmt/format.h>
#include "test/librados/test.h"
#include "test/librados/TestCase.h"
#include "include/scope_guard.h"
#include "crimson_utils.h"

std::string RadosTestNS::pool_name;
rados_t RadosTestNS::s_cluster = NULL;


void RadosTestNS::SetUpTestCase()
{
  auto pool_prefix = fmt::format("{}_", ::testing::UnitTest::GetInstance()->current_test_case()->name());
  pool_name = get_temp_pool_name(pool_prefix);
  ASSERT_EQ("", create_one_pool(pool_name, &s_cluster));
}

void RadosTestNS::TearDownTestCase()
{
  ASSERT_EQ(0, destroy_one_pool(pool_name, &s_cluster));
}

void RadosTestNS::SetUp()
{
  cluster = RadosTestNS::s_cluster;
  ASSERT_EQ(0, rados_ioctx_create(cluster, pool_name.c_str(), &ioctx));
  int req;
  ASSERT_EQ(0, rados_ioctx_pool_requires_alignment2(ioctx, &req));
  ASSERT_FALSE(req);
}

void RadosTestNS::TearDown()
{
  if (cleanup)
    cleanup_all_objects(ioctx);
  rados_ioctx_destroy(ioctx);
}

void RadosTestNS::cleanup_all_objects(rados_ioctx_t ioctx)
{
  // remove all objects to avoid polluting other tests
  rados_ioctx_snap_set_read(ioctx, LIBRADOS_SNAP_HEAD);
  rados_ioctx_set_namespace(ioctx, LIBRADOS_ALL_NSPACES);
  rados_list_ctx_t list_ctx;

  ASSERT_EQ(0, rados_nobjects_list_open(ioctx, &list_ctx));
  auto sg = make_scope_guard([&] { rados_nobjects_list_close(list_ctx); });

  int r;
  const char *entry = NULL;
  const char *key = NULL;
  const char *nspace = NULL;
  while ((r = rados_nobjects_list_next(list_ctx, &entry, &key, &nspace)) != -ENOENT) {
    ASSERT_EQ(0, r);
    rados_ioctx_locator_set_key(ioctx, key);
    rados_ioctx_set_namespace(ioctx, nspace);
    ASSERT_EQ(0, rados_remove(ioctx, entry));
  }
}

std::string RadosTestECNS::pool_name;
rados_t RadosTestECNS::s_cluster = NULL;

void RadosTestECNS::SetUpTestCase()
{
  SKIP_IF_CRIMSON();
  auto pool_prefix = fmt::format("{}_", ::testing::UnitTest::GetInstance()->current_test_case()->name());
  pool_name = get_temp_pool_name(pool_prefix);
  ASSERT_EQ("", create_one_ec_pool(pool_name, &s_cluster));
}

void RadosTestECNS::TearDownTestCase()
{
  SKIP_IF_CRIMSON();
  ASSERT_EQ(0, destroy_one_ec_pool(pool_name, &s_cluster));
}

void RadosTestECNS::SetUp()
{
  SKIP_IF_CRIMSON();
  cluster = RadosTestECNS::s_cluster;
  ASSERT_EQ(0, rados_ioctx_create(cluster, pool_name.c_str(), &ioctx));
  int req;
  ASSERT_EQ(0, rados_ioctx_pool_requires_alignment2(ioctx, &req));
  ASSERT_TRUE(req);
  ASSERT_EQ(0, rados_ioctx_pool_required_alignment2(ioctx, &alignment));
  ASSERT_NE(0U, alignment);
}

void RadosTestECNS::TearDown()
{
  SKIP_IF_CRIMSON();
  if (cleanup)
    cleanup_all_objects(ioctx);
  rados_ioctx_destroy(ioctx);
}

std::string RadosTest::pool_name;
rados_t RadosTest::s_cluster = NULL;

void RadosTest::SetUpTestCase()
{
  auto pool_prefix = fmt::format("{}_", ::testing::UnitTest::GetInstance()->current_test_case()->name());
  pool_name = get_temp_pool_name(pool_prefix);
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
  nspace = get_temp_pool_name();
  rados_ioctx_set_namespace(ioctx, nspace.c_str());
  int req;
  ASSERT_EQ(0, rados_ioctx_pool_requires_alignment2(ioctx, &req));
  ASSERT_FALSE(req);
}

void RadosTest::TearDown()
{
  if (cleanup) {
    cleanup_default_namespace(ioctx);
    cleanup_namespace(ioctx, nspace);
  }
  rados_ioctx_destroy(ioctx);
}

void RadosTest::cleanup_default_namespace(rados_ioctx_t ioctx)
{
  // remove all objects from the default namespace to avoid polluting
  // other tests
  cleanup_namespace(ioctx, "");
}

void RadosTest::cleanup_namespace(rados_ioctx_t ioctx, std::string ns)
{
  rados_ioctx_snap_set_read(ioctx, LIBRADOS_SNAP_HEAD);
  rados_ioctx_set_namespace(ioctx, ns.c_str());
  rados_list_ctx_t list_ctx;

  ASSERT_EQ(0, rados_nobjects_list_open(ioctx, &list_ctx));
  auto sg = make_scope_guard([&] { rados_nobjects_list_close(list_ctx); });

  int r;
  const char *entry = NULL;
  const char *key = NULL;
  while ((r = rados_nobjects_list_next(list_ctx, &entry, &key, NULL)) != -ENOENT) {
    ASSERT_EQ(0, r);
    rados_ioctx_locator_set_key(ioctx, key);
    ASSERT_EQ(0, rados_remove(ioctx, entry));
  }
}

std::string RadosTestEC::pool_name;
rados_t RadosTestEC::s_cluster = NULL;

void RadosTestEC::SetUpTestCase()
{
  SKIP_IF_CRIMSON();
  auto pool_prefix = fmt::format("{}_", ::testing::UnitTest::GetInstance()->current_test_case()->name()); 
  pool_name = get_temp_pool_name(pool_prefix);
  ASSERT_EQ("", create_one_ec_pool(pool_name, &s_cluster));
}

void RadosTestEC::TearDownTestCase()
{
  SKIP_IF_CRIMSON();
  ASSERT_EQ(0, destroy_one_ec_pool(pool_name, &s_cluster));
}

void RadosTestEC::SetUp()
{
  SKIP_IF_CRIMSON();
  cluster = RadosTestEC::s_cluster;
  ASSERT_EQ(0, rados_ioctx_create(cluster, pool_name.c_str(), &ioctx));
  nspace = get_temp_pool_name();
  rados_ioctx_set_namespace(ioctx, nspace.c_str());
  int req;
  ASSERT_EQ(0, rados_ioctx_pool_requires_alignment2(ioctx, &req));
  ASSERT_TRUE(req);
  ASSERT_EQ(0, rados_ioctx_pool_required_alignment2(ioctx, &alignment));
  ASSERT_NE(0U, alignment);
}

void RadosTestEC::TearDown()
{
  SKIP_IF_CRIMSON();
  if (cleanup) {
    cleanup_default_namespace(ioctx);
    cleanup_namespace(ioctx, nspace);
  }
  rados_ioctx_destroy(ioctx);
}

