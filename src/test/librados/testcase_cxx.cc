// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "testcase_cxx.h"

#include <chrono>
#include <thread>

#include <errno.h>
#include <fmt/format.h>
#include "test_cxx.h"
#include "test_shared.h"
#include "crimson_utils.h"
#include "include/scope_guard.h"

#include "common/json/OSDStructures.h"
#include "common/ceph_context.h"
#include "common/perf_counters_collection.h"

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

int RadosTestECPP::freeze_omap_journal() {
  bufferlist inbl, outbl;
  std::ostringstream oss;
  oss << "{\"prefix\": \"tell\", \"target\": \"osd.*\", \"args\": [\"freezeomapjournal\"]}";
  int rc = rados.mon_command(oss.str(), std::move(inbl), outbl, nullptr);
  return rc;
}

int RadosTestECPP::unfreeze_omap_journal() {
  bufferlist inbl, outbl;
  std::ostringstream oss;
  oss << "{\"prefix\": \"tell\", \"target\": \"osd.*\", \"args\": [\"freezeomapjournal\"]}";
  int rc = rados.mon_command(oss.str(), std::move(inbl), outbl, nullptr);
  return rc;
}

void RadosTestECPP::write_omap_keys(std::string oid, int min_index, int max_index) {
  const std::string omap_value = "val";
  bufferlist omap_val_bl;
  encode(omap_value, omap_val_bl);
  std::map<std::string, bufferlist> omap_map;

  int width = std::to_string(max_index).length();
  if (width < 3) {
    width = 3;
  }

  for (int i = min_index; i <= max_index; i++) {
    std::stringstream key_ss;
    key_ss << "key_" << std:setw(width) << std::setfill('0') << i;
    omap_map[key_ss.str()] = omap_val_bl;
  }
  
  ObjectWriteOperation write;
  write.omap_set(omap_map);

  std::cout << "Writing OMap keys from " << min_index << " to " << max_index << "..." << std::endl;
  int ret = ioctx.operate(oid, &write);
  EXPECT_EQ(ret, 0);
}

void RadosTestECPP::check_returned_keys(
    std::list<std::pair<std::string, std::string>> expected_ranges,
    std::set<std::string> returned_keys)
{
  for (auto &range : expected_ranges) {
    // Need to do!
  }
}

void RadosTestECPP::remove_omap_range(
    std::string oid, 
    std::string start, 
    std::string end)
{
  // How can we do this?
}

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
    ASSERT_EQ(0, s_cluster.mon_command(
      "{\"prefix\": \"osd tier remove-overlay\", \"pool\": \"" + pool_name +
      "\"}",
      {}, NULL, NULL));
    ASSERT_EQ(0, s_cluster.mon_command(
      "{\"prefix\": \"osd tier remove\", \"pool\": \"" + pool_name +
      "\", \"tierpool\": \"" + cache_pool_name + "\"}",
      {}, NULL, NULL));
    ASSERT_EQ(0, s_cluster.mon_command(
      "{\"prefix\": \"osd pool delete\", \"pool\": \"" + cache_pool_name +
      "\", \"pool2\": \"" + cache_pool_name + "\", \"yes_i_really_really_mean_it\": true}",
      {}, NULL, NULL));
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
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd pool create\", \"pool\": \"" + cache_pool_name +
      "\", \"pg_num\": 4}",
      {}, NULL, NULL));
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
      "\", \"tierpool\": \"" + cache_pool_name +
      "\", \"force_nonempty\": \"--force-nonempty\" }",
      {}, NULL, NULL));
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
      "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
      {}, NULL, NULL));
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
      "\", \"mode\": \"writeback\"}",
      {}, NULL, NULL));
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
Rados RadosTestPPBase::s_cluster;

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

void RadosTestPPBase::cleanup_default_namespace(librados::IoCtx ioctx)
{
  // remove all objects from the default namespace to avoid polluting
  // other tests
  cleanup_namespace(ioctx, "");
}

void RadosTestPPBase::cleanup_namespace(librados::IoCtx ioctx, std::string ns)
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
    ASSERT_EQ(0, s_cluster.mon_command(
      "{\"prefix\": \"osd tier remove-overlay\", \"pool\": \"" + pool_name +
      "\"}",
      {}, NULL, NULL));
    ASSERT_EQ(0, s_cluster.mon_command(
      "{\"prefix\": \"osd tier remove\", \"pool\": \"" + pool_name +
      "\", \"tierpool\": \"" + cache_pool_name + "\"}",
      {}, NULL, NULL));
    ASSERT_EQ(0, s_cluster.mon_command(
      "{\"prefix\": \"osd pool delete\", \"pool\": \"" + cache_pool_name +
      "\", \"pool2\": \"" + cache_pool_name + "\", \"yes_i_really_really_mean_it\": true}",
      {}, NULL, NULL));
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
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd pool create\", \"pool\": \"" + cache_pool_name +
      "\", \"pg_num\": 4}",
      {}, NULL, NULL));
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
      "\", \"tierpool\": \"" + cache_pool_name +
      "\", \"force_nonempty\": \"--force-nonempty\" }",
      {}, NULL, NULL));
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
      "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
      {}, NULL, NULL));
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
      "\", \"mode\": \"writeback\"}",
      {}, NULL, NULL));
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

std::string RadosTestECPP::pool_name_default;
std::string RadosTestECPP::pool_name_fast;
Rados RadosTestECPP::s_cluster;

void RadosTestECPP::SetUpTestCase()
{
  SKIP_IF_CRIMSON();
  auto pool_prefix = fmt::format("{}_", ::testing::UnitTest::GetInstance()->current_test_case()->name());
  pool_name_default = get_temp_pool_name(pool_prefix);
  pool_name_fast = get_temp_pool_name(pool_prefix);
  ASSERT_EQ("", connect_cluster_pp(s_cluster));
  ASSERT_EQ("", create_ec_pool_pp(pool_name_default, s_cluster, false));
  ASSERT_EQ("", create_ec_pool_pp(pool_name_fast, s_cluster, true));
}

void RadosTestECPP::TearDownTestCase()
{
  SKIP_IF_CRIMSON();
  ASSERT_EQ(0, destroy_ec_pool_pp(pool_name_default, s_cluster));
  ASSERT_EQ(0, destroy_ec_pool_pp(pool_name_fast, s_cluster));
  s_cluster.shutdown();
}

void RadosTestECPP::SetUp()
{
  SKIP_IF_CRIMSON();
  const bool& fast_ec = GetParam();
  if (fast_ec) {
    pool_name = pool_name_fast;
  } else {
    GTEST_SKIP() << "Skipping Legacy EC test for now";
    pool_name = pool_name_default;
  }
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
  if (ec_overwrites_set) {
    ASSERT_EQ(0, destroy_one_ec_pool_pp(pool_name, s_cluster));
    ASSERT_EQ("", create_one_ec_pool_pp(pool_name, s_cluster));
    ec_overwrites_set = false;
  }
  ioctx.close();
}

void RadosTestECPP::set_allow_ec_overwrites()
{
  ec_overwrites_set = true;
  ASSERT_EQ("", set_allow_ec_overwrites_pp(pool_name, cluster, true));

  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));

  const std::string objname = "RadosTestECPP::set_allow_ec_overwrites:test_obj";
  ASSERT_EQ(0, ioctx.write(objname, bl, sizeof(buf), 0));
  const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(120);
  while (true) {
    if (0 == ioctx.write(objname, bl, sizeof(buf), 0)) {
      break;
    }
    ASSERT_LT(std::chrono::steady_clock::now(), end);
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }
}

int RadosTestECPP::request_osd_map(
    std::string pool_name, 
    std::string oid, 
    std::string nspace, 
    ceph::messaging::osd::OSDMapReply* reply) {
  bufferlist inbl, outbl;
  auto formatter = std::make_unique<JSONFormatter>(false);
  ceph::messaging::osd::OSDMapRequest osdMapRequest{pool_name, oid, nspace};
  encode_json("OSDMapRequest", osdMapRequest, formatter.get());

  std::ostringstream oss;
  formatter.get()->flush(oss);
  int rc = cluster.mon_command(oss.str(), std::move(inbl), &outbl, nullptr);
  if (rc != 0) {
    return rc;
  }

  JSONParser p;
  bool success = p.parse(outbl.c_str(), outbl.length());
  if (!success) {
    return -1;
  }

  reply->decode_json(&p);
  return 0;
}

int RadosTestECPP::set_osd_upmap(
    std::string pgid,
    std::vector<int> up_osds) {
  bufferlist inbl, outbl;
  std::ostringstream oss;
  oss << "{\"prefix\": \"osd pg-upmap\", \"pgid\": \"" << pgid << "\", \"id\": [";
  for (size_t i = 0; i < up_osds.size(); i++) {
    oss << up_osds[i];
    if (i != up_osds.size() - 1) {
      oss << ", ";
    }
  }
  oss << "]}";
  int rc = cluster.mon_command(oss.str(), std::move(inbl), &outbl, nullptr);
  return rc;
}

int RadosTestECPP::wait_for_upmap(
    std::string pool_name,
    std::string oid,
    std::string nspace,
    int desired_primary,
    std::chrono::seconds timeout) {
  bool upmap_in_effect = false;
  auto start_time = std::chrono::steady_clock::now();
  while (!upmap_in_effect && (std::chrono::steady_clock::now() - start_time < timeout)) {
    ceph::messaging::osd::OSDMapReply reply;
    int res = request_osd_map(pool_name, "error_inject_oid", nspace, &reply);
    EXPECT_TRUE(res == 0);
    std::vector<int> acting_osds = reply.acting;
    if (!acting_osds.empty() && acting_osds[0] == desired_primary) {
      print_osd_map("New upmap in effect, acting set: ", acting_osds);
      upmap_in_effect = true;
    } else {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
  return upmap_in_effect ? 0 : -ETIMEDOUT;
}

void RadosTestECPP::read_xattrs(
    std::string oid,
    std::string xattr_key,
    std::string xattr_value,
    int expected_size,
    int expected_ret,
    int expected_err) {
  ObjectReadOperation read;
  bufferlist xattr_read_bl, xattr_val_bl;
  encode(xattr_value, xattr_val_bl);
  int err = 0;
  read.getxattr(xattr_key.c_str(), &xattr_read_bl, &err);
  std::map<std::string, bufferlist> xattrs_read{ {"_", {}}, {xattr_key, {}}};
  read.getxattrs(&xattrs_read, &err);
  read.cmpxattr(xattr_key.c_str(), CEPH_OSD_CMPXATTR_OP_EQ, xattr_val_bl);
  int ret = ioctx.operate(oid, &read, nullptr);
  EXPECT_EQ(ret, expected_ret);
  EXPECT_EQ(err, expected_err);
  EXPECT_EQ(xattrs_read.size(), expected_size);
}

void RadosTestECPP::read_omap(
    std::string oid,
    std::string omap_key,
    std::string omap_value,
    int expected_size,
    int expected_err) {
  ObjectReadOperation read;
  int err = 0;
  std::map<std::string,bufferlist> vals_read{ {"_", {}} };
  read.omap_get_vals2("", LONG_MAX, &vals_read, nullptr, &err);
  int ret = ioctx.operate(oid, &read, nullptr);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(err, expected_err);
  EXPECT_EQ(vals_read.size(), expected_size);
  EXPECT_NE(vals_read.find(omap_key), vals_read.end());
  bufferlist val_read_bl = vals_read[omap_key];
  std::string val_read;
  decode(val_read, val_read_bl);
  EXPECT_EQ(omap_value, val_read);
}

void RadosTestECPP::print_osd_map(std::string message, std::vector<int> osd_vec) {
  std::stringstream out_vec;
  std::copy(osd_vec.begin(), osd_vec.end(), std::ostream_iterator<int>(out_vec, " "));
  std::cout << message << out_vec.str().c_str() << std::endl;
}
