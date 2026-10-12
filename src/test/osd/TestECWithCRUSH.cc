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
};

// Losing a whole zone leaves the PG peered until degraded stretch mode names the surviving zone as mandatory.

// A pg-upmap swapping the two zone blocks while a pg_temp still pins the old
// acting set, with a trimmed log so the up OSDs need backfill for their new
// shards.  The old acting set keeps serving and the up OSDs are backfilled.

// Zone 1 loses two OSDs, then a pg-upmap moves zone block 1 onto the zone 0
// OSDs serving block 0 while a pg_temp keeps block 0 there with a lone
// shard 3 in zone 1.  Once the up OSDs are backfilled, Recovered must ask to
// drop the pg_temp rather than abort in choose_acting.
