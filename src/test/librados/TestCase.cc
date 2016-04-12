// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include "test/librados/test.h"
#include "test/librados/TestCase.h"

using namespace librados;

std::string RadosTestNS::pool_name;
rados_t RadosTestNS::s_cluster = NULL;

void RadosTestNS::SetUpTestCase()
{
  pool_name = get_temp_pool_name();
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
  int requires;
  ASSERT_EQ(0, rados_ioctx_pool_requires_alignment2(ioctx, &requires));
  ASSERT_FALSE(requires);
}

void RadosTestNS::TearDown()
{
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
  rados_nobjects_list_close(list_ctx);
}

std::string RadosTestPPNS::pool_name;
Rados RadosTestPPNS::s_cluster;

void RadosTestPPNS::SetUpTestCase()
{
  pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, s_cluster));
}

void RadosTestPPNS::TearDownTestCase()
{
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, s_cluster));
}

void RadosTestPPNS::SetUp()
{
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));
  bool requires;
  ASSERT_EQ(0, ioctx.pool_requires_alignment2(&requires));
  ASSERT_FALSE(requires);
}

void RadosTestPPNS::TearDown()
{
  cleanup_all_objects(ioctx);
  ioctx.close();
}

void RadosTestPPNS::cleanup_all_objects(librados::IoCtx ioctx)
{
  // remove all objects to avoid polluting other tests
  ioctx.snap_set_read(librados::SNAP_HEAD);
  ioctx.set_namespace(all_nspaces);
  for (NObjectIterator it = ioctx.nobjects_begin();
       it != ioctx.nobjects_end(); ++it) {
    ioctx.locator_set_key(it->get_locator());
    ioctx.set_namespace(it->get_nspace());
    ASSERT_EQ(0, ioctx.remove(it->get_oid()));
  }
}

std::string RadosTestParamPPNS::pool_name;
std::string RadosTestParamPPNS::cache_pool_name;
Rados RadosTestParamPPNS::s_cluster;

void RadosTestParamPPNS::SetUpTestCase()
{
  pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, s_cluster));
}

void RadosTestParamPPNS::TearDownTestCase()
{
  if (cache_pool_name.length()) {
    // tear down tiers
    bufferlist inbl;
    ASSERT_EQ(0, s_cluster.mon_command(
      "{\"prefix\": \"osd tier remove-overlay\", \"pool\": \"" + pool_name +
      "\"}",
      inbl, NULL, NULL));
    ASSERT_EQ(0, s_cluster.mon_command(
      "{\"prefix\": \"osd tier remove\", \"pool\": \"" + pool_name +
      "\", \"tierpool\": \"" + cache_pool_name + "\"}",
      inbl, NULL, NULL));
    ASSERT_EQ(0, s_cluster.mon_command(
      "{\"prefix\": \"osd pool delete\", \"pool\": \"" + cache_pool_name +
      "\", \"pool2\": \"" + cache_pool_name + "\", \"sure\": \"--yes-i-really-really-mean-it\"}",
      inbl, NULL, NULL));
    cache_pool_name = "";
  }
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, s_cluster));
}

void RadosTestParamPPNS::SetUp()
{
  if (strcmp(GetParam(), "cache") == 0 && cache_pool_name.empty()) {
    cache_pool_name = get_temp_pool_name();
    bufferlist inbl;
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd pool create\", \"pool\": \"" + cache_pool_name +
      "\", \"pg_num\": 4}",
      inbl, NULL, NULL));
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
      "\", \"tierpool\": \"" + cache_pool_name +
      "\", \"force_nonempty\": \"--force-nonempty\" }",
      inbl, NULL, NULL));
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
      "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
      inbl, NULL, NULL));
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
      "\", \"mode\": \"writeback\"}",
      inbl, NULL, NULL));
    cluster.wait_for_latest_osdmap();
  }

  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));
  bool requires;
  ASSERT_EQ(0, ioctx.pool_requires_alignment2(&requires));
  ASSERT_FALSE(requires);
}

void RadosTestParamPPNS::TearDown()
{
  cleanup_all_objects(ioctx);
  ioctx.close();
}

void RadosTestParamPPNS::cleanup_all_objects(librados::IoCtx ioctx)
{
  // remove all objects to avoid polluting other tests
  ioctx.snap_set_read(librados::SNAP_HEAD);
  ioctx.set_namespace(all_nspaces);
  for (NObjectIterator it = ioctx.nobjects_begin();
       it != ioctx.nobjects_end(); ++it) {
    ioctx.locator_set_key(it->get_locator());
    ioctx.set_namespace(it->get_nspace());
    ASSERT_EQ(0, ioctx.remove(it->get_oid()));
  }
}

std::string RadosTestECNS::pool_name;
rados_t RadosTestECNS::s_cluster = NULL;

void RadosTestECNS::SetUpTestCase()
{
  pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_ec_pool(pool_name, &s_cluster));
}

void RadosTestECNS::TearDownTestCase()
{
  ASSERT_EQ(0, destroy_one_ec_pool(pool_name, &s_cluster));
}

void RadosTestECNS::SetUp()
{
  cluster = RadosTestECNS::s_cluster;
  ASSERT_EQ(0, rados_ioctx_create(cluster, pool_name.c_str(), &ioctx));
  int requires;
  ASSERT_EQ(0, rados_ioctx_pool_requires_alignment2(ioctx, &requires));
  ASSERT_TRUE(requires);
  ASSERT_EQ(0, rados_ioctx_pool_required_alignment2(ioctx, &alignment));
  ASSERT_NE(0U, alignment);
}

void RadosTestECNS::TearDown()
{
  cleanup_all_objects(ioctx);
  rados_ioctx_destroy(ioctx);
}

std::string RadosTestECPPNS::pool_name;
Rados RadosTestECPPNS::s_cluster;

void RadosTestECPPNS::SetUpTestCase()
{
  pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_ec_pool_pp(pool_name, s_cluster));
}

void RadosTestECPPNS::TearDownTestCase()
{
  ASSERT_EQ(0, destroy_one_ec_pool_pp(pool_name, s_cluster));
}

void RadosTestECPPNS::SetUp()
{
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));
  bool requires;
  ASSERT_EQ(0, ioctx.pool_requires_alignment2(&requires));
  ASSERT_TRUE(requires);
  ASSERT_EQ(0, ioctx.pool_required_alignment2(&alignment));
  ASSERT_NE(0U, alignment);
}

void RadosTestECPPNS::TearDown()
{
  cleanup_all_objects(ioctx);
  ioctx.close();
}

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
  nspace = get_temp_pool_name();
  rados_ioctx_set_namespace(ioctx, nspace.c_str());
  int requires;
  ASSERT_EQ(0, rados_ioctx_pool_requires_alignment2(ioctx, &requires));
  ASSERT_FALSE(requires);
}

void RadosTest::TearDown()
{
  cleanup_default_namespace(ioctx);
  cleanup_namespace(ioctx, nspace);
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
  int r;
  const char *entry = NULL;
  const char *key = NULL;
  while ((r = rados_nobjects_list_next(list_ctx, &entry, &key, NULL)) != -ENOENT) {
    ASSERT_EQ(0, r);
    rados_ioctx_locator_set_key(ioctx, key);
    ASSERT_EQ(0, rados_remove(ioctx, entry));
  }
  rados_nobjects_list_close(list_ctx);
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
  nspace = get_temp_pool_name();
  ioctx.set_namespace(nspace);
  bool requires;
  ASSERT_EQ(0, ioctx.pool_requires_alignment2(&requires));
  ASSERT_FALSE(requires);
}

void RadosTestPP::TearDown()
{
  cleanup_default_namespace(ioctx);
  cleanup_namespace(ioctx, nspace);
  ioctx.close();
}

void RadosTestPP::cleanup_default_namespace(librados::IoCtx ioctx)
{
  // remove all objects from the default namespace to avoid polluting
  // other tests
  cleanup_namespace(ioctx, "");
}

void RadosTestPP::cleanup_namespace(librados::IoCtx ioctx, std::string ns)
{
  ioctx.snap_set_read(librados::SNAP_HEAD);
  ioctx.set_namespace(ns);
  for (NObjectIterator it = ioctx.nobjects_begin();
       it != ioctx.nobjects_end(); ++it) {
    ioctx.locator_set_key(it->get_locator());
    ObjectWriteOperation op;
    op.remove();
    librados::AioCompletion *completion = s_cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(it->get_oid(), completion, &op,
				   librados::OPERATION_IGNORE_CACHE));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
}

std::string RadosTestParamPP::pool_name;
std::string RadosTestParamPP::cache_pool_name;
Rados RadosTestParamPP::s_cluster;

void RadosTestParamPP::SetUpTestCase()
{
  pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, s_cluster));
}

void RadosTestParamPP::TearDownTestCase()
{
  if (cache_pool_name.length()) {
    // tear down tiers
    bufferlist inbl;
    ASSERT_EQ(0, s_cluster.mon_command(
      "{\"prefix\": \"osd tier remove-overlay\", \"pool\": \"" + pool_name +
      "\"}",
      inbl, NULL, NULL));
    ASSERT_EQ(0, s_cluster.mon_command(
      "{\"prefix\": \"osd tier remove\", \"pool\": \"" + pool_name +
      "\", \"tierpool\": \"" + cache_pool_name + "\"}",
      inbl, NULL, NULL));
    ASSERT_EQ(0, s_cluster.mon_command(
      "{\"prefix\": \"osd pool delete\", \"pool\": \"" + cache_pool_name +
      "\", \"pool2\": \"" + cache_pool_name + "\", \"sure\": \"--yes-i-really-really-mean-it\"}",
      inbl, NULL, NULL));
    cache_pool_name = "";
  }
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, s_cluster));
}

void RadosTestParamPP::SetUp()
{
  if (strcmp(GetParam(), "cache") == 0 && cache_pool_name.empty()) {
    cache_pool_name = get_temp_pool_name();
    bufferlist inbl;
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd pool create\", \"pool\": \"" + cache_pool_name +
      "\", \"pg_num\": 4}",
      inbl, NULL, NULL));
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
      "\", \"tierpool\": \"" + cache_pool_name +
      "\", \"force_nonempty\": \"--force-nonempty\" }",
      inbl, NULL, NULL));
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
      "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
      inbl, NULL, NULL));
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
      "\", \"mode\": \"writeback\"}",
      inbl, NULL, NULL));
    cluster.wait_for_latest_osdmap();
  }

  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));
  nspace = get_temp_pool_name();
  ioctx.set_namespace(nspace);
  bool requires;
  ASSERT_EQ(0, ioctx.pool_requires_alignment2(&requires));
  ASSERT_FALSE(requires);
}

void RadosTestParamPP::TearDown()
{
  cleanup_default_namespace(ioctx);
  cleanup_namespace(ioctx, nspace);
  ioctx.close();
}

void RadosTestParamPP::cleanup_default_namespace(librados::IoCtx ioctx)
{
  // remove all objects from the default namespace to avoid polluting
  // other tests
  cleanup_namespace(ioctx, "");
}

void RadosTestParamPP::cleanup_namespace(librados::IoCtx ioctx, std::string ns)
{
  ioctx.snap_set_read(librados::SNAP_HEAD);
  ioctx.set_namespace(ns);
  for (NObjectIterator it = ioctx.nobjects_begin();
       it != ioctx.nobjects_end(); ++it) {
    ioctx.locator_set_key(it->get_locator());
    ASSERT_EQ(0, ioctx.remove(it->get_oid()));
  }
}

std::string RadosTestEC::pool_name;
rados_t RadosTestEC::s_cluster = NULL;

void RadosTestEC::SetUpTestCase()
{
  pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_ec_pool(pool_name, &s_cluster));
}

void RadosTestEC::TearDownTestCase()
{
  ASSERT_EQ(0, destroy_one_ec_pool(pool_name, &s_cluster));
}

void RadosTestEC::SetUp()
{
  cluster = RadosTestEC::s_cluster;
  ASSERT_EQ(0, rados_ioctx_create(cluster, pool_name.c_str(), &ioctx));
  nspace = get_temp_pool_name();
  rados_ioctx_set_namespace(ioctx, nspace.c_str());
  int requires;
  ASSERT_EQ(0, rados_ioctx_pool_requires_alignment2(ioctx, &requires));
  ASSERT_TRUE(requires);
  ASSERT_EQ(0, rados_ioctx_pool_required_alignment2(ioctx, &alignment));
  ASSERT_NE(0U, alignment);
}

void RadosTestEC::TearDown()
{
  cleanup_default_namespace(ioctx);
  cleanup_namespace(ioctx, nspace);
  rados_ioctx_destroy(ioctx);
}

std::string RadosTestECPP::pool_name;
Rados RadosTestECPP::s_cluster;

void RadosTestECPP::SetUpTestCase()
{
  pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_ec_pool_pp(pool_name, s_cluster));
}

void RadosTestECPP::TearDownTestCase()
{
  ASSERT_EQ(0, destroy_one_ec_pool_pp(pool_name, s_cluster));
}

void RadosTestECPP::SetUp()
{
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));
  nspace = get_temp_pool_name();
  ioctx.set_namespace(nspace);
  bool requires;
  ASSERT_EQ(0, ioctx.pool_requires_alignment2(&requires));
  ASSERT_TRUE(requires);
  ASSERT_EQ(0, ioctx.pool_required_alignment2(&alignment));
  ASSERT_NE(0U, alignment);
}

void RadosTestECPP::TearDown()
{
  cleanup_default_namespace(ioctx);
  cleanup_namespace(ioctx, nspace);
  ioctx.close();
}

