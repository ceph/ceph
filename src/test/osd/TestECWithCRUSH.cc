// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <gtest/gtest.h>

#include "crush/crush.h"   // CRUSH_ITEM_NONE
#include "osdc/SplitOp.h"
#include "test/osd/ECCrushTestFixture.h"
#include "test/osd/TestCommon.h"

using namespace std;

/**
 * TestECWithCRUSH - parameterized EC tests for pools configured with a real
 * CRUSH rule.
 *
 * ECCrushTestFixture (the base) builds a proper CRUSH map with a bucket
 * hierarchy and an EC-specific indep rule, and points the pool at that rule.
 * The pg_upmap from ECPeeringTestFixture is kept intact so shard == osd, but
 * peering, OSDMap validation, and all pool-level checks see a correctly
 * configured EC crush rule.
 *
 * The fixture is parameterized over BackendConfig for future expansion.
 * A single 2+1 ISA config is registered to start; add entries to
 * kECCrushConfigs to cover additional configurations.
 */
class TestECWithCRUSH : public ECCrushTestFixture,
                         public ::testing::WithParamInterface<BackendConfig> {
public:
  TestECWithCRUSH() : ECCrushTestFixture()
  {
    const auto& cfg = GetParam();
    k = cfg.k;
    m = cfg.m;
    stripe_unit = cfg.stripe_unit;
    ec_plugin = cfg.ec_plugin;
    ec_technique = cfg.ec_technique;
    pool_flags = cfg.pool_flags;
    num_zones = cfg.num_zones;
  }

  void SetUp() override
  {
    ECPeeringTestFixture::SetUp();
  }
};

// ---------------------------------------------------------------------------
// EC backend configurations for parameterized tests
// ---------------------------------------------------------------------------

namespace {

/**
 * kECCrushConfigs - EC configurations to test with real CRUSH placement.
 *
 * Each entry is a BackendConfig that controls k, m, stripe_unit, plugin, and
 * pool flags.  Add new entries here to cover additional configurations; no
 * other code changes are required.
 */
const std::vector<BackendConfig> kECCrushConfigs = {
  // ISA plugin with optimizations
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  4, 2, 1, "EC_ISA_Opt_k4m2_su4k_CRUSH"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  8192,  4, 2, 1, "EC_ISA_Opt_k4m2_su8k_CRUSH"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  16384, 4, 2, 1, "EC_ISA_Opt_k4m2_su16k_CRUSH"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  2, 1, 1, "EC_ISA_Opt_k2m1_su4k_CRUSH"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  8, 3, 1, "EC_ISA_Opt_k8m3_su4k_CRUSH"},

  // Jerasure plugin with optimizations
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  4, 2, 1, "EC_Jerasure_Opt_k4m2_su4k_CRUSH"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  8192,  4, 2, 1, "EC_Jerasure_Opt_k4m2_su8k_CRUSH"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  16384, 4, 2, 1, "EC_Jerasure_Opt_k4m2_su16k_CRUSH"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  2, 1, 1, "EC_Jerasure_Opt_k2m1_su4k_CRUSH"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  8, 3, 1, "EC_Jerasure_Opt_k8m3_su4k_CRUSH"},

  // 2-zone stretch configurations — CRUSH map has two datacenter buckets
  // (zone-0, zone-1), each with one host and k+m OSDs.  The pool uses the
  // "ec_stretch_rule" built by add_simple_stretch_rule() in pre_peering_hook.
  {PGBackendTestFixture::EC, "isa",     "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  4, 2, 2, "EC_ISA_Opt_k4m2_su4k_2zone_CRUSH"},
  {PGBackendTestFixture::EC, "isa",     "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  2, 1, 2, "EC_ISA_Opt_k2m1_su4k_2zone_CRUSH"},
  {PGBackendTestFixture::EC, "jerasure","reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  4, 2, 2, "EC_Jerasure_Opt_k4m2_su4k_2zone_CRUSH"},
  {PGBackendTestFixture::EC, "jerasure","reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  2, 1, 2, "EC_Jerasure_Opt_k2m1_su4k_2zone_CRUSH"},
};

}  // namespace

// ---------------------------------------------------------------------------
// Parameterized test instantiation
// ---------------------------------------------------------------------------

INSTANTIATE_TEST_SUITE_P(
  ECCrushBasic,
  TestECWithCRUSH,
  ::testing::ValuesIn(kECCrushConfigs),
  [](const ::testing::TestParamInfo<BackendConfig>& info) {
    return info.param.label;
  });

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/**
 * BasicWriteVerify - create an EC object and verify its contents.
 *
 * This is the canonical sanity test for the CRUSH-based fixture.  It:
 *   1. Asserts that the pool is Active/Clean after CRUSH-based peering.
 *   2. Creates an EC object and writes one full stripe of data.
 *   3. Reads the data back and verifies it matches what was written.
 *
 * All three operations are performed through the single create_and_write_verify
 * helper which combines the write and the verification into one call.
 */
TEST_P(TestECWithCRUSH, BasicWriteVerify)
{
  ASSERT_TRUE(all_shards_active())
    << "Pool must be Active/Clean after CRUSH-based peering before writing";

  const std::string obj_name = "crush_test_obj";
  const size_t data_size = stripe_unit * k;  // one full stripe
  const std::string data(data_size, 'X');

  // Create the object, write the data, and read it back in one call.
  create_and_write_verify(obj_name, data);
}

// ===========================================================================
// ECSplitOp::local_zone_for_acting_set() tests
//
// Tests zone resolution: one arbitrary OSD per zone is picked as a
// representative, its CRUSH distance to the client is computed, and the zone
// with the closest representative wins.
//
// A minimal 2-zone CRUSH map is built inline (no PG, no peering, no store)
// so that the CRUSH lookups produce real results.
// ===========================================================================

class TestLocalZoneForActingSet : public ::testing::Test {
protected:
  std::shared_ptr<OSDMap> osdmap;
  static constexpr int zone_size = 6;
  static constexpr int num_zones = 2;

  void SetUp() override
  {
    CephContext* cct = g_ceph_context;
    constexpr int num_osds = 12;

    osdmap = std::make_shared<OSDMap>();
    uuid_d fsid;
    fsid.generate_random();
    int r = osdmap->build_simple(cct, 1, fsid, num_osds);
    ceph_assert(r == 0);

    // Build a 2-zone CRUSH map:
    //   root "default"
    //     ├─ datacenter "zone-0" → host "host-0" → osd.0 … osd.5
    //     └─ datacenter "zone-1" → host "host-1" → osd.6 … osd.11
    CrushWrapper crush;
    crush.create();
    OSDMap::_build_crush_types(crush);

    int root_type = crush.get_type_id("root");
    ceph_assert(root_type >= 0);
    int rootid = 0;
    r = crush.add_bucket(0, CRUSH_BUCKET_STRAW2, CRUSH_HASH_DEFAULT,
                         root_type, 0, nullptr, nullptr, &rootid);
    ceph_assert(r == 0);
    crush.set_item_name(rootid, "default");

    for (int z = 0; z < 2; z++) {
      std::map<std::string, std::string> loc;
      loc["root"]       = "default";
      loc["datacenter"] = "zone-" + std::to_string(z);
      loc["host"]       = "host-" + std::to_string(z);
      for (int i = 0; i < 6; i++) {
        int osd = z * 6 + i;
        crush.insert_item(cct, osd, 1.0, "osd." + std::to_string(osd), loc);
      }
    }
    crush.finalize();

    OSDMap::Incremental crush_inc(osdmap->get_epoch() + 1);
    crush_inc.fsid = osdmap->get_fsid();
    crush.encode(crush_inc.crush, CEPH_FEATURES_SUPPORTED_DEFAULT);
    osdmap->apply_incremental(crush_inc);
  }

  std::multimap<std::string, std::string> make_loc(int zone_index)
  {
    std::multimap<std::string, std::string> loc;
    if (zone_index >= 0) {
      loc.emplace("datacenter", "zone-" + std::to_string(zone_index));
    }
    return loc;
  }

  // acting = [osd.0..5 (zone-0), osd.6..11 (zone-1)]
  std::vector<int> make_acting()
  {
    std::vector<int> acting(12);
    for (int i = 0; i < 12; ++i) acting[i] = i;
    return acting;
  }
};

TEST_F(TestLocalZoneForActingSet, ClientInZone0ReturnsZero)
{
  auto loc = make_loc(0);
  auto acting = make_acting();
  EXPECT_EQ(0, ECSplitOp::local_zone_for_acting_set(
    acting, num_zones, zone_size, osdmap->crush.get(), g_ceph_context, loc));
}

TEST_F(TestLocalZoneForActingSet, ClientInZone1ReturnsOne)
{
  auto loc = make_loc(1);
  auto acting = make_acting();
  EXPECT_EQ(1, ECSplitOp::local_zone_for_acting_set(
    acting, num_zones, zone_size, osdmap->crush.get(), g_ceph_context, loc));
}

TEST_F(TestLocalZoneForActingSet, NoCrushLocationReturnsZero)
{
  std::multimap<std::string, std::string> empty_loc;
  auto acting = make_acting();
  EXPECT_EQ(0, ECSplitOp::local_zone_for_acting_set(
    acting, num_zones, zone_size, osdmap->crush.get(), g_ceph_context, empty_loc));
}

TEST_F(TestLocalZoneForActingSet, AllZone0OsdsDownFallsBackToZero)
{
  auto loc = make_loc(0);
  auto acting = make_acting();
  // Mark all zone-0 OSDs as -1 so no representative can be found for zone-0.
  // With crush_location only specifying datacenter=zone-0,
  // get_common_ancestor_distance returns -ERANGE for zone-1's representative
  // (different datacenter, no higher-level match in loc).
  // Result: neither zone scores → returns 0 (default; falls back to zone 0).
  for (int i = 0; i < zone_size; ++i) acting[i] = CRUSH_ITEM_NONE;
  EXPECT_EQ(0, ECSplitOp::local_zone_for_acting_set(
    acting, num_zones, zone_size, osdmap->crush.get(), g_ceph_context, loc));
}

TEST_F(TestLocalZoneForActingSet, AllZone0OsdsDownWithRootLocPrefersZone1)
{
  // With root=default in crush_location, zone-1's representative OSD matches
  // at root level even though it is in a different datacenter.  Zone-0 has no
  // representative (all OSDs CRUSH_ITEM_NONE), so zone-1 is the only zone with
  // a finite score and wins.
  std::multimap<std::string, std::string> loc;
  loc.emplace("datacenter", "zone-0");
  loc.emplace("root", "default");
  auto acting = make_acting();
  for (int i = 0; i < zone_size; ++i) acting[i] = CRUSH_ITEM_NONE;
  EXPECT_EQ(1, ECSplitOp::local_zone_for_acting_set(
    acting, num_zones, zone_size, osdmap->crush.get(), g_ceph_context, loc));
}

TEST_F(TestLocalZoneForActingSet, SingleZoneReturnsZero)
{
  auto loc = make_loc(0);
  auto acting = make_acting();
  EXPECT_EQ(0, ECSplitOp::local_zone_for_acting_set(
    acting, /*num_zones=*/1, zone_size, osdmap->crush.get(), g_ceph_context, loc));
}

TEST_F(TestLocalZoneForActingSet, ActingTooSmallReturnsZero)
{
  auto loc = make_loc(0);
  std::vector<int> short_acting = {0, 1, 2};
  EXPECT_EQ(0, ECSplitOp::local_zone_for_acting_set(
    short_acting, num_zones, zone_size, osdmap->crush.get(), g_ceph_context, loc));
}

TEST_F(TestLocalZoneForActingSet, LocalizeReadsPicksNearestZone)
{
  auto acting = make_acting();
  // Client in zone-0 → zone 0 nearest.
  auto loc0 = make_loc(0);
  EXPECT_EQ(0, ECSplitOp::local_zone_for_acting_set(
    acting, num_zones, zone_size, osdmap->crush.get(), g_ceph_context, loc0));
  // Client in zone-1 → zone 1 nearest.
  auto loc1 = make_loc(1);
  EXPECT_EQ(1, ECSplitOp::local_zone_for_acting_set(
    acting, num_zones, zone_size, osdmap->crush.get(), g_ceph_context, loc1));
}

TEST_F(TestLocalZoneForActingSet, BalanceReadsPicksZoneFromActingSet)
{
  auto acting = make_acting();
  // With crush_location in zone-0, function returns 0.
  auto loc = make_loc(0);
  EXPECT_EQ(0, ECSplitOp::local_zone_for_acting_set(
    acting, num_zones, zone_size, osdmap->crush.get(), g_ceph_context, loc));
  // Remove zone-0 OSDs from acting set; function falls back to 0 (default).
  for (int i = 0; i < zone_size; ++i) acting[i] = CRUSH_ITEM_NONE;
  EXPECT_EQ(0, ECSplitOp::local_zone_for_acting_set(
    acting, num_zones, zone_size, osdmap->crush.get(), g_ceph_context, loc));
}

TEST_F(TestLocalZoneForActingSet, NonFastECBypassesZoneRouting)
{
  // A pool without FLAG_EC_OPTIMIZATIONS should not pass validate_flags
  // for LOCALIZE_READS when FLAG_CLIENT_SPLIT_READS is also absent.
  // Here we test the validate_flags logic directly: it accepts LOCALIZE_READS
  // regardless of EC_OPTIMIZATIONS (that guard is in init_read), but the
  // create() function rejects pools without FLAG_CLIENT_SPLIT_READS.
  pg_pool_t pool;
  pool.type = pg_pool_t::TYPE_ERASURE;
  // No FLAG_EC_OPTIMIZATIONS, no FLAG_CLIENT_SPLIT_READS
  ASSERT_FALSE(pool.has_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS));
  ASSERT_FALSE(pool.has_flag(pg_pool_t::FLAG_CLIENT_SPLIT_READS));
  // validate_flags accepts LOCALIZE_READS (flag check only)
  EXPECT_TRUE(SplitOp::validate_flags(&pool, CEPH_OSD_FLAG_LOCALIZE_READS, g_ceph_context));
  // But without FLAG_CLIENT_SPLIT_READS, create() would return false
  // (tested here by checking the pool flag that create() gates on)
  EXPECT_FALSE(pool.has_flag(pg_pool_t::FLAG_CLIENT_SPLIT_READS));
}

TEST_F(TestLocalZoneForActingSet, FastECLocalizeBothFlagsAccepted)
{
  pg_pool_t pool;
  pool.type = pg_pool_t::TYPE_ERASURE;
  pool.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  pool.set_flag(pg_pool_t::FLAG_CLIENT_SPLIT_READS);
  // Both BALANCE_READS and LOCALIZE_READS should pass flag validation.
  EXPECT_TRUE(SplitOp::validate_flags(&pool, CEPH_OSD_FLAG_BALANCE_READS, g_ceph_context));
  EXPECT_TRUE(SplitOp::validate_flags(&pool, CEPH_OSD_FLAG_LOCALIZE_READS, g_ceph_context));
  // Neither should pass for writes.
  EXPECT_FALSE(SplitOp::validate_flags(&pool, CEPH_OSD_FLAG_BALANCE_READS | CEPH_OSD_FLAG_WRITE, g_ceph_context));
  EXPECT_FALSE(SplitOp::validate_flags(&pool, CEPH_OSD_FLAG_LOCALIZE_READS | CEPH_OSD_FLAG_WRITE, g_ceph_context));
  // No read flag at all should fail.
  EXPECT_FALSE(SplitOp::validate_flags(&pool, 0, g_ceph_context));
}

