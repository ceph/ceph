// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "testcase_cxx.h"

#include <errno.h>
#include <fmt/format.h>
#include "test_cxx.h"
#include "test_shared.h"
#include "crimson_utils.h"
#include "include/scope_guard.h"

using namespace librados;

namespace {

void init_rand() {
  static bool seeded = false;
  if (!seeded) {
    seeded = true;
    int seed = getpid();
    std::cout << "seed " << seed << std::endl;
    srand(seed);
  }
}

} // anonymous namespace

std::string RadosTestPPNS::pool_name;
Rados RadosTestPPNS::s_cluster;

void RadosTestPPNS::SetUpTestCase()
{
  auto pool_prefix = fmt::format("{}_", ::testing::UnitTest::GetInstance()->current_test_case()->name());
  pool_name = get_temp_pool_name(pool_prefix);
  ASSERT_EQ("", create_one_pool_pp(pool_name, s_cluster));
}

void RadosTestPPNS::TearDownTestCase()
{
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, s_cluster));
}

void RadosTestPPNS::SetUp()
{
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));
  bool req;
  ASSERT_EQ(0, ioctx.pool_requires_alignment2(&req));
  ASSERT_FALSE(req);
}

void RadosTestPPNS::TearDown()
{
  if (cleanup)
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
  auto pool_prefix = fmt::format("{}_", ::testing::UnitTest::GetInstance()->current_test_case()->name());
  pool_name = get_temp_pool_name(pool_prefix);
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
      "\", \"pool2\": \"" + cache_pool_name + "\", \"yes_i_really_really_mean_it\": true}",
      inbl, NULL, NULL));
    cache_pool_name = "";
  }
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, s_cluster));
}

void RadosTestParamPPNS::SetUp()
{
  if (!is_crimson_cluster() && strcmp(GetParam(), "cache") == 0 &&
      cache_pool_name.empty()) {
    auto pool_prefix = fmt::format("{}_", ::testing::UnitTest::GetInstance()->current_test_case()->name());
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
  bool req;
  ASSERT_EQ(0, ioctx.pool_requires_alignment2(&req));
  ASSERT_FALSE(req);
}

void RadosTestParamPPNS::TearDown()
{
  if (cleanup)
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

std::string RadosTestECPPNS::pool_name;
Rados RadosTestECPPNS::s_cluster;

void RadosTestECPPNS::SetUpTestCase()
{
  auto pool_prefix = fmt::format("{}_", ::testing::UnitTest::GetInstance()->current_test_case()->name());
  pool_name = get_temp_pool_name(pool_prefix);
  ASSERT_EQ("", create_one_ec_pool_pp(pool_name, s_cluster));
}

void RadosTestECPPNS::TearDownTestCase()
{
  ASSERT_EQ(0, destroy_one_ec_pool_pp(pool_name, s_cluster));
}

void RadosTestECPPNS::SetUp()
{
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));
  bool req;
  ASSERT_EQ(0, ioctx.pool_requires_alignment2(&req));
  ASSERT_TRUE(req);
  ASSERT_EQ(0, ioctx.pool_required_alignment2(&alignment));
  ASSERT_NE(0U, alignment);
}

void RadosTestECPPNS::TearDown()
{
  if (cleanup)
    cleanup_all_objects(ioctx);
  ioctx.close();
}

std::string RadosTestPP::pool_name;
Rados RadosTestPP::s_cluster;

void RadosTestPP::SetUpTestCase()
{
  init_rand();

  auto pool_prefix = fmt::format("{}_", ::testing::UnitTest::GetInstance()->current_test_case()->name());
  pool_name = get_temp_pool_name(pool_prefix);
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
  bool req;
  ASSERT_EQ(0, ioctx.pool_requires_alignment2(&req));
  ASSERT_FALSE(req);
}

void RadosTestPP::TearDown()
{
  if (cleanup) {
    cleanup_default_namespace(ioctx);
    cleanup_namespace(ioctx, nspace);
  }
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
  int tries = 20;
  while (--tries) {
    int got_enoent = 0;
    for (NObjectIterator it = ioctx.nobjects_begin();
	 it != ioctx.nobjects_end(); ++it) {
      ioctx.locator_set_key(it->get_locator());
      ObjectWriteOperation op;
      op.remove();
      librados::AioCompletion *completion = s_cluster.aio_create_completion();
      auto sg = make_scope_guard([&] { completion->release(); });
      ASSERT_EQ(0, ioctx.aio_operate(it->get_oid(), completion, &op,
				     librados::OPERATION_IGNORE_CACHE));
      completion->wait_for_complete();
      if (completion->get_return_value() == -ENOENT) {
	++got_enoent;
	std::cout << " got ENOENT removing " << it->get_oid()
		  << " in ns " << ns << std::endl;
      } else {
	ASSERT_EQ(0, completion->get_return_value());
      }
    }
    if (!got_enoent) {
      break;
    }
    std::cout << " got ENOENT on " << got_enoent
	      << " objects, waiting a bit for snap"
	      << " trimming before retrying " << tries << " more times..."
	      << std::endl;
    sleep(1);
  }
  if (tries == 0) {
    std::cout << "failed to clean up; probably need to scrub purged_snaps."
	      << std::endl;
  }
}

std::string RadosTestParamPP::pool_name;
std::string RadosTestParamPP::cache_pool_name;
Rados RadosTestParamPP::s_cluster;

void RadosTestParamPP::SetUpTestCase()
{
  auto pool_prefix = fmt::format("{}_", ::testing::UnitTest::GetInstance()->current_test_case()->name());
  pool_name = get_temp_pool_name(pool_prefix);
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
      "\", \"pool2\": \"" + cache_pool_name + "\", \"yes_i_really_really_mean_it\": true}",
      inbl, NULL, NULL));
    cache_pool_name = "";
  }
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, s_cluster));
}

void RadosTestParamPP::SetUp()
{
  if (!is_crimson_cluster() && strcmp(GetParam(), "cache") == 0 &&
      cache_pool_name.empty()) {
    auto pool_prefix = fmt::format("{}_", ::testing::UnitTest::GetInstance()->current_test_case()->name());
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
  bool req;
  ASSERT_EQ(0, ioctx.pool_requires_alignment2(&req));
  ASSERT_FALSE(req);
}

void RadosTestParamPP::TearDown()
{
  if (cleanup) {
    cleanup_default_namespace(ioctx);
    cleanup_namespace(ioctx, nspace);
  }
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

std::string RadosTestECPP::pool_name;
Rados RadosTestECPP::s_cluster;

void RadosTestECPP::SetUpTestCase()
{
  SKIP_IF_CRIMSON();
  auto pool_prefix = fmt::format("{}_", ::testing::UnitTest::GetInstance()->current_test_case()->name());
  pool_name = get_temp_pool_name(pool_prefix);
  ASSERT_EQ("", create_one_ec_pool_pp(pool_name, s_cluster));
}

void RadosTestECPP::TearDownTestCase()
{
  SKIP_IF_CRIMSON();
  ASSERT_EQ(0, destroy_one_ec_pool_pp(pool_name, s_cluster));
}

void RadosTestECPP::SetUp()
{
  SKIP_IF_CRIMSON();
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));
  nspace = get_temp_pool_name();
  ioctx.set_namespace(nspace);
  bool req;
  ASSERT_EQ(0, ioctx.pool_requires_alignment2(&req));
  ASSERT_TRUE(req);
  ASSERT_EQ(0, ioctx.pool_required_alignment2(&alignment));
  ASSERT_NE(0U, alignment);
}

void RadosTestECPP::TearDown()
{
  SKIP_IF_CRIMSON();
  if (cleanup) {
    cleanup_default_namespace(ioctx);
    cleanup_namespace(ioctx, nspace);
  }
  ioctx.close();
}

