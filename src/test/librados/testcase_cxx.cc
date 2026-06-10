// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "testcase_cxx.h"

#include <chrono>
#include <thread>
#include <string_view>

#include <errno.h>
#include <fmt/format.h>
#include "test_cxx.h"
#include "test_shared.h"
#include "crimson_utils.h"
#include "include/scope_guard.h"

#include "common/ceph_context.h"
#include "common/perf_counters_collection.h"
#include "common/ceph_json.h"

#include "erasure-code/consistency/RadosCommands.h"
#include "common/json/OSDStructures.h"
#include "crush/crush.h"

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

std::string RadosTestPP::pool_name_default;
Rados RadosTestPPBase::s_cluster;

void RadosTestPP::SetUpTestCase()
{
  init_rand();
  auto pool_prefix = fmt::format("{}_", ::testing::UnitTest::GetInstance()->current_test_case()->name());
  pool_name_default = get_temp_pool_name(pool_prefix);
  std::map<std::string, std::string> config = {{"rados_replica_read_policy", "default"}};
  ASSERT_EQ("", connect_cluster_pp(s_cluster, config));
  ASSERT_EQ("", create_pool_pp(pool_name_default, s_cluster));
  s_cluster.wait_for_latest_osdmap();
}

void RadosTestPP::TearDownTestCase()
{
  ASSERT_EQ(0, destroy_pool_pp(pool_name_default, s_cluster));
}

void RadosTestPP::SetUp()
{
  bool s = GetParam();
  split_ops = s;
  balanced_read_flags = split_ops ? librados::OPERATION_BALANCE_READS : 0;

  if (split_ops) {
    ASSERT_EQ(0, cluster.conf_set("osd_min_split_replica_read_size", "262144"));
  }

  pool_name = pool_name_default;
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

void RadosTestECPP::SetUpTestCase()
{
  SKIP_IF_CRIMSON();
  auto pool_prefix = fmt::format("{}_", ::testing::UnitTest::GetInstance()->current_test_case()->name());
  pool_name_default = get_temp_pool_name(pool_prefix);
  pool_name_fast = get_temp_pool_name(pool_prefix);
  std::map<std::string, std::string> config = {{"rados_replica_read_policy", "default"}};
  ASSERT_EQ("", connect_cluster_pp(s_cluster, config));
  ASSERT_EQ("", create_ec_pool_pp(pool_name_default, s_cluster, false));
  ASSERT_EQ("", create_ec_pool_pp(pool_name_fast, s_cluster, true));
  s_cluster.wait_for_latest_osdmap();
}

void RadosTestECPP::TearDownTestCase()
{
  SKIP_IF_CRIMSON();
  ASSERT_EQ(0, destroy_pool_pp(pool_name_default, s_cluster));
  ASSERT_EQ(0, destroy_pool_pp(pool_name_fast, s_cluster));
  s_cluster.shutdown();
}

void RadosTestECPP::SetUp()
{
  SKIP_IF_CRIMSON();
  const auto& params = GetParam();
  fast_ec = std::get<0>(params);
  split_ops = std::get<1>(params);
  balanced_read_flags = split_ops ? librados::OPERATION_BALANCE_READS : 0;

  if (split_ops) {
    ASSERT_EQ(0, cluster.conf_set("osd_min_split_replica_read_size", "262144"));
  }

  if (fast_ec) {
    pool_name = pool_name_fast;
  } else if (!split_ops) {
    pool_name = pool_name_default;
  } else {
    GTEST_SKIP() << "Legacy EC does not support split ops";
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
    ASSERT_EQ(0, destroy_pool_pp(pool_name, s_cluster));
    ASSERT_EQ("", create_ec_pool_pp(pool_name, s_cluster, fast_ec));

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

void RadosTestECPP::wait_for_stable_acting_set(const std::string &objname) {
  ceph::consistency::RadosCommands ec_commands(s_cluster);

  // Get EC profile to determine expected number of shards
  ceph::ErasureCodeProfile profile = ec_commands.get_ec_profile_for_pool(pool_name);
  int k = std::stoi(profile["k"]);
  int m = std::stoi(profile["m"]);
  int num_shards = k + m;

  const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(120);
  while (std::chrono::steady_clock::now() < end) {
    // Get OSD map information
    ceph::messaging::osd::OSDMapRequest osd_map_request{pool_name, objname, nspace};
    JSONFormatter f;
    encode_json("OSDMapRequest", osd_map_request, &f);

    std::ostringstream oss;
    f.flush(oss);

    ceph::bufferlist outbl;
    int rc = s_cluster.mon_command(oss.str(), {}, &outbl, nullptr);
    ASSERT_EQ(0, rc);

    JSONParser p;
    bool success = p.parse(outbl.c_str(), outbl.length());
    ASSERT_TRUE(success);

    ceph::messaging::osd::OSDMapReply reply;
    reply.decode_json(&p);

    // Check if acting set is stable:
    // 1. Acting set size matches expected
    // 2. All OSDs in acting set are valid (not CRUSH_ITEM_NONE)
    // 3. Acting set equals up set
    // 4. Primary OSD is shard 0
    bool stable = true;

    if (reply.acting.size() != (size_t)num_shards) {
      stable = false;
    } else {
      for (int osd : reply.acting) {
        if (osd == CRUSH_ITEM_NONE) {
          std::cout << "OSD " << osd << " not valid " << std::endl;
          stable = false;
          break;
        }
      }

      if (stable && reply.acting != reply.up) {
        std::cout << "acting != up" << std::endl;
        stable = false;
      }

      if (stable && reply.acting_primary != reply.acting[0]) {
        std::cout << "acting_primary != acting[0]" << std::endl;
        stable = false;
      }
      if (stable && reply.acting_primary != reply.up_primary) {
        std::cout << "acting_primary != up_primary" << std::endl;
        stable = false;
      }
    }

    if (stable) {
      std::cout << "Acting set is stable for " << objname << std::endl;
      return;
    } else {
      std::cout << "Unstable for " << objname << " reply=" << p << std::endl;

    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  FAIL() << "Timeout waiting for stable acting set for " << objname;
}

void RadosTestECPP::inject_ec_write_error(const std::string &objname, int error_type, shard_id_t shard) {
  ceph::consistency::RadosCommands ec_commands(s_cluster);
  int primary_osd = ec_commands.get_primary_osd(pool_name, objname, nspace);

  ceph::messaging::osd::InjectECErrorRequest<
    io_exerciser::InjectOpType::WriteFailAndRollback>
  injectErrorRequest(pool_name, "*", shard.id, error_type, 0, std::numeric_limits<int64_t>::max());

  JSONFormatter f;
  encode_json("WriteFailAndRollbackInject", injectErrorRequest, &f);

  std::ostringstream oss;
  f.flush(oss);
  std::cout << " Inject primary OSD "<< primary_osd << " for shard " << shard.id << " with: \"" << oss.str() << "\"" << std::endl;
  int rc = s_cluster.osd_command(primary_osd, oss.str(), {}, {}, nullptr);
  ASSERT_EQ(0, rc);
}

void RadosTestECPP::inject_ec_write_error_all_osds(const std::string &objname, int error_type) {
  ceph::consistency::RadosCommands ec_commands(s_cluster);
  ceph::ErasureCodeProfile profile = ec_commands.get_ec_profile_for_pool(pool_name);
  int k = std::stoi(profile["k"]);
  int m = std::stoi(profile["m"]);
  int num_shards = k + m;

  // Inject on all OSDs, each affecting its local shard
  for (int i = 0; i < num_shards; i++) {
    shard_id_t shard(i);
    int osd = ec_commands.get_shard_osd(pool_name, objname, shard, nspace);

    ceph::messaging::osd::InjectECErrorRequest<
      io_exerciser::InjectOpType::WriteFailAndRollback>
    injectErrorRequest(pool_name, "*", shard.id, error_type, 0, std::numeric_limits<int64_t>::max());

    JSONFormatter f;
    encode_json("WriteFailAndRollbackInject", injectErrorRequest, &f);

    std::ostringstream oss;
    f.flush(oss);
    std::cout << " Inject OSD "<< osd << " with: \"" << oss.str() << "\"" << std::endl;
    int rc = s_cluster.osd_command(osd, oss.str(), {}, {}, nullptr);
    ASSERT_EQ(0, rc);
  }
}

void RadosTestECPP::clear_ec_write_error(const std::string &objname, int error_type, shard_id_t shard) {
  ceph::consistency::RadosCommands ec_commands(s_cluster);
  int primary_osd = ec_commands.get_primary_osd(pool_name, objname, nspace);

  ceph::messaging::osd::InjectECClearErrorRequest<
      io_exerciser::InjectOpType::WriteFailAndRollback>
      clearErrorInject{pool_name, "*", shard.id, error_type};

  JSONFormatter f;
  encode_json("WriteFailAndRollbackInject", clearErrorInject, &f);

  std::ostringstream oss;
  f.flush(oss);
  std::cout << " Clear error on primary OSD " << primary_osd << " for shard " << shard.id << ": " << oss.str() << std::endl;
  int rc = s_cluster.osd_command(primary_osd, oss.str(), {}, {}, nullptr);
  ASSERT_EQ(0, rc);
}

void RadosTestECPP::clear_ec_write_error_all_osds(const std::string &objname, int error_type) {
  ceph::consistency::RadosCommands ec_commands(s_cluster);
  ceph::ErasureCodeProfile profile = ec_commands.get_ec_profile_for_pool(pool_name);
  int k = std::stoi(profile["k"]);
  int m = std::stoi(profile["m"]);
  int num_shards = k + m;

  // Clear on all OSDs, each affecting its local shard
  for (int i = 0; i < num_shards; i++) {
    shard_id_t shard(i);
    int osd = ec_commands.get_shard_osd(pool_name, objname, shard, nspace);

    ceph::messaging::osd::InjectECClearErrorRequest<
        io_exerciser::InjectOpType::WriteFailAndRollback>
        clearErrorInject{pool_name, "*", shard.id, error_type};

    JSONFormatter f;
    encode_json("WriteFailAndRollbackInject", clearErrorInject, &f);

    std::ostringstream oss;
    f.flush(oss);
    std::cout << " Inject " << oss.str() << std::endl;
    int rc = s_cluster.osd_command(osd, oss.str(), {}, {}, nullptr);
    ASSERT_EQ(0, rc);
  }
}

uint64_t RadosTestPPBase::get_perf_counter_by_path(std::string_view path) {
  CephContext* cct = (CephContext*)cluster.cct();
  PerfCountersCollection *coll = cct->get_perfcounters_collection();

  std::stringstream ss;
  bool found = false;

  uint64_t value = 0;
  coll->with_counters([&](const auto& counter_map) {
    auto it = counter_map.find(std::string(path));
    if (it != counter_map.end()) {
      value = it->second.data->u64.load();
      found = true;
    }
  });

  if (!found) {
    ss << "Performance counter not found: '" << path << "'.\n";
    ss << "Available counters are:\n";
    coll->with_counters([&](const auto& counter_map) {
      if (counter_map.empty()) {
        ss << "  <none> (The collection is empty, check initialization timing)";
      } else {
        for (const auto& pair : counter_map) {
          ss << "  - " << pair.first << "\n";
        }
      }
    });

    throw(std::range_error(ss.str()));
  }

  return value;
}
std::string RadosTestECOptimisedPP::pool_name;
Rados RadosTestECOptimisedPP::s_cluster;

void RadosTestECOptimisedPP::SetUpTestCase()
{
  SKIP_IF_CRIMSON();
  auto pool_prefix = fmt::format("{}_", ::testing::UnitTest::GetInstance()->current_test_case()->name());
  pool_name = get_temp_pool_name(pool_prefix);
  ASSERT_EQ("", create_one_ec_pool_pp(pool_name, s_cluster, true));
}

void RadosTestECOptimisedPP::TearDownTestCase()
{
  SKIP_IF_CRIMSON();
  ASSERT_EQ(0, destroy_one_ec_pool_pp(pool_name, s_cluster));
}

void RadosTestECOptimisedPP::SetUp()
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

void RadosTestECOptimisedPP::TearDown()
{
  SKIP_IF_CRIMSON();
  if (cleanup) {
    cleanup_default_namespace(ioctx);
    cleanup_namespace(ioctx, nspace);
  }
  ioctx.close();
}

void RadosTestECOptimisedPP::turn_balancing_off()
{
  int rc;
  std::ostringstream oss;
  bufferlist outbl;

  oss.str("");
  bufferlist inbl_autoscaler;
  oss << "{\"prefix\": \"osd set\", \"key\": \"noautoscale\"}";
  rc = cluster.mon_command(oss.str(), std::move(inbl_autoscaler), &outbl, nullptr);
  EXPECT_EQ(rc, 0);

  oss.str("");
  oss << "{\"prefix\": \"balancer off\"}";
  bufferlist inbl_balancer;
  rc = cluster.mon_command(oss.str(), std::move(inbl_balancer), &outbl, nullptr);
  EXPECT_EQ(rc, 0);
}

void RadosTestECOptimisedPP::turn_balancing_on()
{
  int rc;
  std::ostringstream oss;
  bufferlist outbl;

  oss.str("");
  bufferlist inbl_autoscaler;
  oss << "{\"prefix\": \"osd unset\", \"key\": \"noautoscale\"}";
  rc = cluster.mon_command(oss.str(), std::move(inbl_autoscaler), &outbl, nullptr);
  EXPECT_EQ(rc, 0);

  oss.str("");
  oss << "{\"prefix\": \"balancer on\"}";
  bufferlist inbl_balancer;
  rc = cluster.mon_command(oss.str(), std::move(inbl_balancer), &outbl, nullptr);
  EXPECT_EQ(rc, 0);
}

void RadosTestECOptimisedPP::enable_omap()
{
  bufferlist inbl, outbl;
  std::ostringstream oss;
  oss << "{\"prefix\": \"osd pool set\", \"pool\": \"" << pool_name
      << "\", \"var\": \"supports_omap\", \"val\": \"true\"}";
  int ret = cluster.mon_command(oss.str(), std::move(inbl), &outbl, nullptr);
  EXPECT_EQ(ret, 0);
}

int RadosTestECOptimisedPP::request_osd_map(
    std::string oid,
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

int RadosTestECOptimisedPP::set_osd_upmap(
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

int RadosTestECOptimisedPP::wait_for_upmap(
    std::string oid,
    int desired_primary,
    std::chrono::seconds timeout) {
  bool upmap_in_effect = false;
  auto start_time = std::chrono::steady_clock::now();
  while (!upmap_in_effect && (std::chrono::steady_clock::now() - start_time < timeout)) {
    ceph::messaging::osd::OSDMapReply reply;
    int res = request_osd_map(oid, &reply);
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

void RadosTestECOptimisedPP::print_osd_map(std::string message, std::vector<int> osd_vec) {
  std::stringstream out_vec;
  std::copy(osd_vec.begin(), osd_vec.end(), std::ostream_iterator<int>(out_vec, " "));
  std::cout << message << out_vec.str().c_str() << std::endl;
}

void RadosTestECOptimisedPP::check_omap_read(
    std::string oid,
    std::string first_omap_key,
    std::string first_omap_value,
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
  if (vals_read.find(first_omap_key) == vals_read.end()) {
    ADD_FAILURE() << "Missing key " << first_omap_key;
  } else {
    bufferlist val_read_bl = vals_read[first_omap_key];
    std::string val_read;
    decode(val_read, val_read_bl);
    EXPECT_EQ(first_omap_value, val_read);
  }
}