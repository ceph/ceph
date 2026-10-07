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

#include <optional>
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
  int num_zones = 2;

  void SetUp() override
  {
    CephContext* cct = g_ceph_context;
    const int num_osds = zone_size * num_zones;

    osdmap = std::make_shared<OSDMap>();
    uuid_d fsid;
    fsid.generate_random();
    int r = osdmap->build_simple(cct, 1, fsid, num_osds);
    ceph_assert(r == 0);

    // One datacenter per zone:
    //   root "default"
    //     ├─ datacenter "zone-0" → host "host-0" → osd.0 … osd.5
    //     └─ datacenter "zone-1" → host "host-1" → osd.6 … osd.11 (…)
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

    for (int z = 0; z < num_zones; z++) {
      std::map<std::string, std::string> loc;
      loc["root"]       = "default";
      loc["datacenter"] = "zone-" + std::to_string(z);
      loc["host"]       = "host-" + std::to_string(z);
      for (int i = 0; i < zone_size; i++) {
        int osd = z * zone_size + i;
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

  // acting = [osd.0..5 (zone-0), osd.6..11 (zone-1), …]
  std::vector<int> make_acting()
  {
    std::vector<int> acting(zone_size * num_zones);
    for (int i = 0; i < (int)acting.size(); ++i) acting[i] = i;
    return acting;
  }
};

class TestLocalZoneForActingSet3Zone : public TestLocalZoneForActingSet {
protected:
  TestLocalZoneForActingSet3Zone() { num_zones = 3; }
};

TEST_F(TestLocalZoneForActingSet3Zone, ClientInZone2ReturnsTwo)
{
  auto loc = make_loc(2);
  auto acting = make_acting();
  EXPECT_EQ(2, SplitOp::local_zone_for_acting_set(
    acting, num_zones, zone_size, osdmap->crush.get(), g_ceph_context, loc));
}

TEST_F(TestLocalZoneForActingSet3Zone, ClientZoneDownTieGoesToLowestZone)
{
  // zone-0 and zone-1 match only at root level, so neither is closer
  std::multimap<std::string, std::string> loc;
  loc.emplace("datacenter", "zone-2");
  loc.emplace("root", "default");
  auto acting = make_acting();
  for (int i = 2 * zone_size; i < 3 * zone_size; ++i) acting[i] = CRUSH_ITEM_NONE;
  EXPECT_EQ(0, SplitOp::local_zone_for_acting_set(
    acting, num_zones, zone_size, osdmap->crush.get(), g_ceph_context, loc));
}

TEST_F(TestLocalZoneForActingSet3Zone, MiddleZoneDownClientThereFallsBack)
{
  std::multimap<std::string, std::string> loc;
  loc.emplace("datacenter", "zone-1");
  loc.emplace("root", "default");
  auto acting = make_acting();
  for (int i = zone_size; i < 2 * zone_size; ++i) acting[i] = CRUSH_ITEM_NONE;
  EXPECT_EQ(0, SplitOp::local_zone_for_acting_set(
    acting, num_zones, zone_size, osdmap->crush.get(), g_ceph_context, loc));
}

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
  // LOCALIZE_READS on a multi-zone pool independently of
  // FLAG_EC_OPTIMIZATIONS - assert that directly, with the flag both absent
  // and present, rather than asserting facts about a pool that was never
  // passed to validate_flags().
  pg_pool_t pool;
  pool.type = pg_pool_t::TYPE_ERASURE;
  pool.num_zones = 2;

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
  pool.num_zones = 2;
  // Both BALANCE_READS and LOCALIZE_READS should pass flag validation.
  EXPECT_TRUE(SplitOp::validate_flags(&pool, CEPH_OSD_FLAG_BALANCE_READS, g_ceph_context));
  EXPECT_TRUE(SplitOp::validate_flags(&pool, CEPH_OSD_FLAG_LOCALIZE_READS, g_ceph_context));
  // Neither should pass for writes.
  EXPECT_FALSE(SplitOp::validate_flags(&pool, CEPH_OSD_FLAG_BALANCE_READS | CEPH_OSD_FLAG_WRITE, g_ceph_context));
  EXPECT_FALSE(SplitOp::validate_flags(&pool, CEPH_OSD_FLAG_LOCALIZE_READS | CEPH_OSD_FLAG_WRITE, g_ceph_context));
  // No read flag at all should fail.
  EXPECT_FALSE(SplitOp::validate_flags(&pool, 0, g_ceph_context));
}

// ===========================================================================
// Peering on a real stretch EC pool (2 zones, 2+1, per-zone min_size k+m).
// ===========================================================================

class TestECStretchPeering : public ECCrushTestFixture {
public:
  TestECStretchPeering() : ECCrushTestFixture() {
    k = 2;
    m = 1;
    num_zones = 2;
    ec_plugin = "isa";
  }

protected:
  void pre_peering_hook() override {
    ECCrushTestFixture::pre_peering_hook();
    pg_pool_t updated = *osdmap->get_pg_pool(pool_id);
    updated.min_size = k + 1;
    updated.peering_crush_bucket_barrier =
      osdmap->crush->get_type_id("datacenter");
    updated.peering_crush_bucket_target = num_zones;
    updated.peering_crush_bucket_count = num_zones;
    updated.peering_crush_mandatory_member = CRUSH_ITEM_NONE;
    OSDMap::Incremental inc(osdmap->get_epoch() + 1);
    inc.fsid = osdmap->get_fsid();
    inc.new_pools[pool_id] = updated;
    osdmap->apply_incremental(inc);
  }

  // Trims the log, so that the up OSDs of a later remap need backfill.
  // Deleting the object leaves that backfill nothing to copy.
  void trim_log(bool delete_obj = false) {
    trim_min.emplace("osd_pg_log_trim_min", "1");
    trim_max.emplace("osd_pg_log_trim_max", "1000");
    osdmap->set_flag(CEPH_OSDMAP_PGLOG_HARDLIMIT);
    const std::string data(stripe_unit * k, 'A');
    create_and_write_verify("obj", data);
    enable_log_trimming = true;
    set_target_pg_log_entries(1);
    for (int i = 0; i < 5; ++i) {
      write_verify("obj", 0, data, data.size());
    }
    if (delete_obj) {
      ASSERT_EQ(0, delete_object("obj"));
    }
    enable_log_trimming = false;
    ASSERT_GT(get_primary_test_pg()->get_peering_state()->get_info().log_tail,
              eversion_t());
  }

  // An empty upmap or pg_temp removes it.
  void remap(const vector<int> &upmap, const vector<int> &pg_temp,
             const vector<int> &osds_up = {}) {
    auto next = std::make_shared<OSDMap>();
    next->deepish_copy_from(*osdmap);
    OSDMap::Incremental inc(next->get_epoch() + 1);
    inc.fsid = next->get_fsid();
    if (upmap.empty()) {
      inc.old_pg_upmap.insert(pgid);
    } else {
      inc.new_pg_upmap[pgid] =
        mempool::osdmap::vector<int32_t>(upmap.begin(), upmap.end());
    }
    vector<int> temp;
    if (!pg_temp.empty()) {
      temp = next->pgtemp_primaryfirst(*next->get_pg_pool(pool_id), pg_temp);
    }
    inc.new_pg_temp[pgid] =
      mempool::osdmap::vector<int32_t>(temp.begin(), temp.end());
    next->apply_incremental(inc);
    for (int osd : osds_up) {
      OSDMapTestHelpers::mark_osd_up(next, osd);
    }
    update_osdmap_with_peering(next);
    event_loop->run_until_idle();
  }

  void set_backfill_progress(const pg_shard_t &t, const hobject_t &progress) {
    get_primary_test_pg()->get_peering_state()->update_peer_last_backfill(
      t, progress);
    ObjectStore::Transaction tx;
    get_test_pg(t)->get_peering_state()->update_backfill_progress(
      progress, pg_stat_t(), false, tx);
  }

  void complete_backfill(const pg_shard_t &t) {
    set_backfill_progress(t, hobject_t::get_max());
    get_test_pg(t)->get_peering_state()->handle_event(
      std::make_shared<PGPeeringEvent>(
        osdmap->get_epoch(), osdmap->get_epoch(), RecoveryDone()),
      get_test_pg(t)->get_peering_ctx());
  }

  void finish_backfill() {
    get_primary_test_pg()->get_peering_state()->handle_event(
      std::make_shared<PGPeeringEvent>(
        osdmap->get_epoch(), osdmap->get_epoch(), PeeringState::Backfilled()),
      get_primary_test_pg()->get_peering_ctx());
    new_epoch_loop();
  }

  std::optional<ScopedConfig> trim_min;
  std::optional<ScopedConfig> trim_max;
};

// Losing a whole zone leaves the PG peered until degraded stretch mode names the surviving zone as mandatory.
TEST_F(TestECStretchPeering, ZoneLoss_PeeredUntilDegradedStretchMode)
{
  ASSERT_TRUE(all_shards_active());
  vector<int> acting;
  int acting_primary;
  osdmap->pg_to_acting_osds(pgid, &acting, &acting_primary);
  const int zone_size = k + m;
  const int primary_zone =
    osdmap->crush->get_parent_of_type(acting_primary, 8,
                                      osdmap->get_pg_pool(pool_id)->crush_rule);
  vector<int> other_zone_osds;
  for (int i = 0; i < num_zones * zone_size; ++i) {
    if (osdmap->crush->get_parent_of_type(
          i, 8, osdmap->get_pg_pool(pool_id)->crush_rule) != primary_zone) {
      other_zone_osds.push_back(i);
    }
  }
  ASSERT_EQ(other_zone_osds.size(), (size_t)zone_size);

  mark_osds_down(other_zone_osds);
  PeeringState *ps = get_primary_test_pg()->get_peering_state();
  EXPECT_TRUE(ps->is_peered()) << get_state_name(0);
  EXPECT_FALSE(ps->is_active()) << get_state_name(0);

  auto new_osdmap = std::make_shared<OSDMap>();
  new_osdmap->deepish_copy_from(*osdmap);
  pg_pool_t degraded = *new_osdmap->get_pg_pool(pool_id);
  degraded.peering_crush_bucket_count = 1;
  degraded.peering_crush_bucket_target = 1;
  degraded.peering_crush_mandatory_member = primary_zone;
  OSDMap::Incremental inc(new_osdmap->get_epoch() + 1);
  inc.fsid = new_osdmap->get_fsid();
  inc.new_pools[pool_id] = degraded;
  new_osdmap->apply_incremental(inc);
  update_osdmap_with_peering(new_osdmap);

  ps = get_primary_test_pg()->get_peering_state();
  EXPECT_TRUE(ps->is_active()) << get_state_name(0);
}

// A pg-upmap swapping the two zone blocks while a pg_temp still pins the old
// acting set, with a trimmed log so the up OSDs need backfill for their new
// shards.  The old acting set keeps serving and the up OSDs are backfilled.
TEST_F(TestECStretchPeering, ZoneBlockSwapWithPgTemp_NoChooseActingAbort)
{
  ASSERT_TRUE(osdmap->get_pg_pool(pool_id)->is_stretch_pool());
  ASSERT_TRUE(all_shards_active());
  trim_log();

  vector<int> up, acting;
  int up_primary, acting_primary;
  osdmap->pg_to_up_acting_osds(pgid, &up, &up_primary, &acting, &acting_primary);
  const int zone_size = k + m;
  vector<int> swapped(acting.begin() + zone_size, acting.end());
  swapped.insert(swapped.end(), acting.begin(), acting.begin() + zone_size);
  remap(swapped, acting);

  PeeringState *ps = get_primary_test_pg()->get_peering_state();
  EXPECT_TRUE(ps->is_active()) << get_state_name(0);
  EXPECT_EQ(ps->get_acting(), acting);
  set<pg_shard_t> expected_backfill;
  for (unsigned i = 0; i < swapped.size(); ++i) {
    expected_backfill.insert(pg_shard_t(swapped[i], shard_id_t(i)));
  }
  EXPECT_EQ(ps->get_backfill_targets(), expected_backfill);
}

// Zone 1 loses two OSDs, then a pg-upmap moves zone block 1 onto the zone 0
// OSDs serving block 0 while a pg_temp keeps block 0 there with a lone
// shard 3 in zone 1.  Once the up OSDs are backfilled, Recovered must ask to
// drop the pg_temp rather than abort in choose_acting.
TEST_F(TestECStretchPeering, BackfilledZoneBlock_RecoveredDropsPgTemp)
{
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  ASSERT_TRUE(all_shards_active());
  trim_log();

  vector<int> a;
  int acting_primary;
  osdmap->pg_to_acting_osds(pgid, &a, &acting_primary);
  mark_osds_down({a[4], a[5]});

  const int N = CRUSH_ITEM_NONE;
  remap({a[4], a[5], N, a[1], a[0], a[2]}, {a[0], a[1], a[2], a[3], N, N});

  TestPG *primary = get_primary_test_pg();
  PeeringState *ps = primary->get_peering_state();
  const vector<int> up = {N, N, N, a[1], a[0], a[2]};
  ASSERT_EQ(ps->get_up(), up);
  ASSERT_EQ(ps->get_acting(), (vector<int>{a[0], a[1], a[2], a[3], N, N}));
  ASSERT_STREQ(ps->get_current_state(), "Started/Primary/Active/Backfilling");
  const set<pg_shard_t> targets = {pg_shard_t(a[1], shard_id_t(3)),
                                   pg_shard_t(a[0], shard_id_t(4)),
                                   pg_shard_t(a[2], shard_id_t(5))};
  ASSERT_EQ(ps->get_backfill_targets(), targets);
  for (const auto &t : targets) {
    complete_backfill(t);
  }

  ASSERT_FALSE(primary->get_peering_listener()->pg_temp_wanted);
  EXPECT_EXIT({
      finish_backfill();
      _exit(get_primary_test_pg()->get_peering_state()->get_acting() == up ?
            0 : 1);
    }, ::testing::ExitedWithCode(0), "");
}
