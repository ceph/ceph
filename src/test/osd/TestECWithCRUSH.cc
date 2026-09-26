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
 * Unlike ECPeeringTestFixture, the pg_upmap is disabled (use_upmap() returns
 * false), so placement is driven entirely by CRUSH rule evaluation, and
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
  // (zone-0, zone-1), each with k+m single-OSD hosts.  The pool uses the
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
// SplitOp::local_zone_for_acting_set() tests
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
  EXPECT_EQ(0, SplitOp::local_zone_for_acting_set(
    acting, num_zones, zone_size, osdmap->crush.get(), g_ceph_context, loc));
}

TEST_F(TestLocalZoneForActingSet, ClientInZone1ReturnsOne)
{
  auto loc = make_loc(1);
  auto acting = make_acting();
  EXPECT_EQ(1, SplitOp::local_zone_for_acting_set(
    acting, num_zones, zone_size, osdmap->crush.get(), g_ceph_context, loc));
}

TEST_F(TestLocalZoneForActingSet, NoCrushLocationReturnsZero)
{
  std::multimap<std::string, std::string> empty_loc;
  auto acting = make_acting();
  EXPECT_EQ(0, SplitOp::local_zone_for_acting_set(
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
  EXPECT_EQ(0, SplitOp::local_zone_for_acting_set(
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
  EXPECT_EQ(1, SplitOp::local_zone_for_acting_set(
    acting, num_zones, zone_size, osdmap->crush.get(), g_ceph_context, loc));
}

// NOTE: num_zones<2 and acting-too-small guard cases are intentionally not
// duplicated here - they are already covered, without needing a real CRUSH
// map, by TestLocalZoneGuards.SingleZoneReturnsZero and
// TestLocalZoneGuards.ActingTooSmallReturnsZero in TestSplitOpsUT.cc (see
// the comment on that fixture: guard-path cases belong there, CRUSH-
// dependent cases belong here).

TEST_F(TestLocalZoneForActingSet, LocalizeReadsPicksNearestZone)
{
  // Exercise the actual localize/non-localize branch that init_read() uses
  // (via the choose_local_zone_index() extraction), not just the
  // localize-agnostic local_zone_for_acting_set() helper by itself - the
  // previous version of this test called only the latter, so it could not
  // tell whether the `localize` flag did anything at all.
  auto acting = make_acting();
  auto loc0 = make_loc(0);
  auto loc1 = make_loc(1);

  // localize=true: zone selection tracks the client's CRUSH location, same
  // as calling local_zone_for_acting_set() directly.
  EXPECT_EQ(0, ECSplitOp::choose_local_zone_index(
    /*localize=*/true, acting, num_zones, zone_size,
    osdmap->crush.get(), g_ceph_context, loc0));
  EXPECT_EQ(1, ECSplitOp::choose_local_zone_index(
    /*localize=*/true, acting, num_zones, zone_size,
    osdmap->crush.get(), g_ceph_context, loc1));

  // localize=false: BALANCE_READS semantics - the result must NOT depend on
  // the client's crush_location at all.  Reset rand()'s seed before each
  // call so both calls draw the same "random" value; if the location were
  // consulted (e.g. a regression that inverted the localize check), loc0
  // and loc1 would disagree the same way the localize=true calls above do.
  srand(1);
  int non_localized_zone0_loc = ECSplitOp::choose_local_zone_index(
    /*localize=*/false, acting, num_zones, zone_size,
    osdmap->crush.get(), g_ceph_context, loc0);
  srand(1);
  int non_localized_zone1_loc = ECSplitOp::choose_local_zone_index(
    /*localize=*/false, acting, num_zones, zone_size,
    osdmap->crush.get(), g_ceph_context, loc1);
  EXPECT_EQ(non_localized_zone0_loc, non_localized_zone1_loc);
}

TEST_F(TestLocalZoneForActingSet, BalanceReadsPicksZoneFromActingSet)
{
  auto acting = make_acting();
  // With crush_location in zone-0, function returns 0.
  auto loc = make_loc(0);
  EXPECT_EQ(0, SplitOp::local_zone_for_acting_set(
    acting, num_zones, zone_size, osdmap->crush.get(), g_ceph_context, loc));
  // Remove zone-0 OSDs from acting set; function falls back to 0 (default).
  for (int i = 0; i < zone_size; ++i) acting[i] = CRUSH_ITEM_NONE;
  EXPECT_EQ(0, SplitOp::local_zone_for_acting_set(
    acting, num_zones, zone_size, osdmap->crush.get(), g_ceph_context, loc));
}

TEST_F(TestLocalZoneForActingSet, ValidateFlagsAcceptsLocalizeReadsRegardlessOfECOptimizations)
{
  // The previous version of this test ("NonFastECBypassesZoneRouting")
  // claimed to check that non-fast-EC pools bypass zone routing, but its
  // only assertions were ASSERT_FALSE on the flags of a just-constructed
  // pg_pool_t - true for any default pool, and true regardless of what
  // validate_flags() or zone routing actually do.  Its comment also
  // asserted a "guard...in init_read" for FLAG_EC_OPTIMIZATIONS that does
  // not exist: grep over SplitOp.cc finds no reference to
  // FLAG_EC_OPTIMIZATIONS at all, and get_num_zone() (osd_types.h), which
  // drives zone routing in ECSplitOp::init_read(), is unconditional on that
  // flag. So "non-fast-EC pools bypass zone routing" is not a property this
  // module implements today, and cannot honestly be tested here.
  //
  // What IS real and testable is that SplitOp::validate_flags() accepts
  // LOCALIZE_READS independently of FLAG_EC_OPTIMIZATIONS - assert that
  // directly, with the flag both absent and present, rather than asserting
  // facts about a pool that was never passed to validate_flags().
  pg_pool_t pool;
  pool.type = pg_pool_t::TYPE_ERASURE;

  EXPECT_TRUE(SplitOp::validate_flags(&pool, CEPH_OSD_FLAG_LOCALIZE_READS, g_ceph_context));

  pool.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  EXPECT_TRUE(SplitOp::validate_flags(&pool, CEPH_OSD_FLAG_LOCALIZE_READS, g_ceph_context));
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
