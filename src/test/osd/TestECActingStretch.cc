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
#include "test/osd/ECPeeringTestFixture.h"
#include "osd/PeeringState.h"
#include "common/ceph_context.h"
#include "crush/CrushWrapper.h"
#include "crush/crush.h"

using namespace std;

/**
 * TestECActingStretch - Unit tests for stretch mode EC acting set selection
 *
 * This test suite validates the zone isolation and bucket_max enforcement
 * in calc_ec_acting_stretch and choose_async_recovery_ec for stretched EC pools.
 *
 * Test Configuration:
 * - 2 zones (datacenters), 4 hosts per zone, 1 OSD per host = 8 OSDs total
 * - Stretched 2+1 EC: Each zone has complete stripe [shard.0, shard.1, shard.2]
 * - Acting set: 6 OSDs (3 per zone) with PRIMARY coordinating I/O
 * - Extra OSDs (6, 7) available as strays for failure scenarios
 * - CRUSH rule: choose firstn 2 type datacenter, chooseleaf indep 3 type host
 */
class TestECActingStretch : public ECPeeringTestFixture {
protected:
  void SetUp() override {
    ECPeeringTestFixture::SetUp();
    
    // Create stretched 2+1 EC pool with 2 zones
    // Zone 1: OSDs 0,1,2,6 (hosts host0, host1, host2, host6 in datacenter dc0)
    // Zone 2: OSDs 3,4,5,7 (hosts host3, host4, host5, host7 in datacenter dc1)
    // Acting set uses OSDs 0-5, OSDs 6-7 available as strays
    setup_stretched_ec_pool();
  }
  
  void setup_stretched_ec_pool() {
    // Create OSDMap with 2 datacenters, 4 hosts each
    auto new_osdmap = std::make_shared<OSDMap>();
    new_osdmap->set_max_osd(8);
    new_osdmap->set_state(0, CEPH_OSD_EXISTS | CEPH_OSD_UP);
    new_osdmap->set_state(1, CEPH_OSD_EXISTS | CEPH_OSD_UP);
    new_osdmap->set_state(2, CEPH_OSD_EXISTS | CEPH_OSD_UP);
    new_osdmap->set_state(3, CEPH_OSD_EXISTS | CEPH_OSD_UP);
    new_osdmap->set_state(4, CEPH_OSD_EXISTS | CEPH_OSD_UP);
    new_osdmap->set_state(5, CEPH_OSD_EXISTS | CEPH_OSD_UP);
    new_osdmap->set_state(6, CEPH_OSD_EXISTS | CEPH_OSD_UP);
    new_osdmap->set_state(7, CEPH_OSD_EXISTS | CEPH_OSD_UP);
    new_osdmap->set_epoch(1);
    
    // Build CRUSH map with 2 datacenters
    CrushWrapper crush;
    crush.create();
    
    // Set type names
    crush.set_type_name(10, "root");
    crush.set_type_name(9, "datacenter");
    crush.set_type_name(1, "host");
    crush.set_type_name(0, "osd");
    
    // Create root bucket
    int root_id;
    crush.add_bucket(0, CRUSH_BUCKET_STRAW2, CRUSH_HASH_RJENKINS1,
                     10 /*type*/, 0, NULL, NULL, &root_id);
    crush.set_item_name(root_id, "default");
    
    // Insert OSDs with location hierarchy
    // dc0: OSDs 0,1,2,6 in hosts host0, host1, host2, host6
    // dc1: OSDs 3,4,5,7 in hosts host3, host4, host5, host7
    for (int dc = 0; dc < 2; dc++) {
      std::string dc_name = (dc == 0) ? "dc0" : "dc1";
      
      for (int h = 0; h < 4; h++) {
        int osd_id = (dc == 0) ? (h < 3 ? h : 6) : (h < 3 ? h + 3 : 7);
        std::string host_name = "host" + std::to_string(osd_id);
        
        std::map<std::string, std::string> loc;
        loc["root"] = "default";
        loc["datacenter"] = dc_name;
        loc["host"] = host_name;
        
        crush.insert_item(g_ceph_context, osd_id, 1.0,
                          "osd." + std::to_string(osd_id), loc);
      }
    }
    
    // Create CRUSH rule for mirrored EC
    // choose 2 type datacenter, chooseleaf indep 3 type host
    int rule_id = 0;
    root_id = crush.get_item_id("default");
    int steps = 6;
    crush_rule *rule = crush_make_rule(steps, pg_pool_t::TYPE_ERASURE);
    int step = 0;
    crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSELEAF_TRIES, 5, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSE_TRIES, 100, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_TAKE, root_id, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_CHOOSE_INDEP, 2, 9 /* datacenter */);
    crush_rule_set_step(rule, step++, CRUSH_RULE_CHOOSELEAF_INDEP, 3, 1 /* host */);
    crush_rule_set_step(rule, step++, CRUSH_RULE_EMIT, 0, 0);
    ASSERT_EQ(step, steps);
    int r = crush_add_rule(crush.get_crush_map(), rule, rule_id);
    ASSERT_GE(r, 0);
    crush.set_rule_name(rule_id, "mirrored_ec_rule");
    
    // Apply CRUSH map via incremental
    OSDMap::Incremental inc(2);
    inc.fsid = new_osdmap->get_fsid();
    crush.encode(inc.crush, CEPH_FEATURES_SUPPORTED_DEFAULT);
    new_osdmap->apply_incremental(inc);
    
    // Create mirrored EC pool
    pool_id = 1;  // Use member variable from base class
    pg_pool_t pool_info;
    pool_info.type = pg_pool_t::TYPE_ERASURE;
    pool_info.size = 6; // 2 zones * 3 shards
    pool_info.min_size = 5; // Can tolerate 1 OSD failure
    pool_info.crush_rule = rule_id;
    pool_info.set_pg_num(8);
    pool_info.set_pgp_num(8);
    
    // EC profile for 2+1
    std::map<std::string, std::string> erasure_code_profile = {
      {"k", "2"},
      {"m", "1"},
      {"plugin", "jerasure"},
      {"technique", "reed_sol_van"}
    };
    new_osdmap->set_erasure_code_profile("default", erasure_code_profile);
    pool_info.erasure_code_profile = "default";
    
    // Stretch mode settings
    pool_info.peering_crush_bucket_barrier = 9; // datacenter type
    pool_info.peering_crush_bucket_target = 2;  // 2 datacenters
    pool_info.peering_crush_bucket_count = 2;  // 2 datacenters
    pool_info.peering_crush_mandatory_member = CRUSH_ITEM_NONE;
    
    // EC pool configuration
    pool_info.set_flag(pg_pool_t::FLAG_EC_OVERWRITES);
    
    OSDMapTestHelpers::add_pool(new_osdmap, pool_id, pool_info, "test_ec_pool");
    
    // Update osdmap
    osdmap = new_osdmap;
  }
};


// calc_ec_acting_stretch Tests 
/**
 * Test: Zone isolation - up set respects zone boundaries
 *
 * Scenario: All OSDs up, verify acting set contains shards from both zones
 * Expected: want = [0,1,2,3,4,5] with proper zone distribution [0-2 from dc0, 3-5 from dc1]
 */
TEST_F(TestECActingStretch, ZoneIsolation_AllUp) {
  const pg_pool_t* pool = osdmap->get_pg_pool(pool_id);
  ASSERT_NE(pool, nullptr);
  PGPool pgpool(osdmap, pool_id, *pool, "test_ec_pool");
  
  // Simulate CRUSH mapping: OSDs 0,1,2 from dc0, OSDs 3,4,5 from dc1
  vector<int> up = {0, 1, 2, 3, 4, 5};
  vector<int> acting = {0, 1, 2, 3, 4, 5};
  
  // Build all_info map with pg_info for each shard
  map<pg_shard_t, pg_info_t> all_info;
  pg_history_t history;
  history.epoch_created = 1;
  history.same_interval_since = 1;
  
  for (unsigned i = 0; i < 6; i++) {
    pg_shard_t shard(i, shard_id_t(i));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(i)));
    info.history = history;
    info.last_update = eversion_t(1, i);
    all_info[shard] = info;
  }
  
  // Auth log shard is primary (OSD 0, shard 0)
  auto auth_log_shard = all_info.find(pg_shard_t(0, shard_id_t(0)));
  ASSERT_NE(auth_log_shard, all_info.end());
  
  // Call calc_ec_acting_stretch
  vector<int> want;
  set<pg_shard_t> backfill;
  set<pg_shard_t> acting_backfill;
  ostringstream ss;
  
  PeeringState::calc_ec_acting_stretch(
    auth_log_shard,
    pool->size,
    acting,
    up,
    all_info,
    false, // restrict_to_up_acting
    &want,
    &backfill,
    &acting_backfill,
    osdmap,
    pgpool,
    ss);
  
  // Verify want contains all 6 OSDs
  EXPECT_EQ(want.size(), 6);
  EXPECT_EQ(want[0], 0);
  EXPECT_EQ(want[1], 1);
  EXPECT_EQ(want[2], 2);
  EXPECT_EQ(want[3], 3);
  EXPECT_EQ(want[4], 4);
  EXPECT_EQ(want[5], 5);
  
  // Verify no backfill needed
  EXPECT_TRUE(backfill.empty()) << "No backfill needed when all OSDs up";
  
  // Verify zone distribution: OSDs 0-2 in dc0, OSDs 3-5 in dc1
  for (int i = 0; i < 3; i++) {
    int dc0 = osdmap->crush->get_parent_of_type(i, 9, pool->crush_rule);
    int dc1 = osdmap->crush->get_parent_of_type(i + 3, 9, pool->crush_rule);
    EXPECT_NE(dc0, dc1) << "dc0 and dc1 should be different buckets";
  }
}

/**
 * Test: Zone isolation - single zone OSD down
 *
 * Scenario: OSD 1 (dc0) down, verify strays only selected from dc0
 * Expected: want should prefer OSD from same zone as replacement
 */
TEST_F(TestECActingStretch, ZoneIsolation_SingleOSDDown) {
  const pg_pool_t* pool = osdmap->get_pg_pool(pool_id);
  ASSERT_NE(pool, nullptr);
  PGPool pgpool(osdmap, pool_id, *pool, "test_ec_pool");
  
  // Mark OSD 1 down
  mark_osd_down(1);

  // OSD 1 is down - shard position 1 in dc0
  // CRUSH remaps position 1 to OSD 6 (also in dc0)
  vector<int> up = {0, 6, 2, 3, 4, 5};     // Current CRUSH mapping (1 → 6)
  vector<int> acting = {0, 1, 2, 3, 4, 5}; // Old acting set (still has down OSD 1)

  // Build all_info map
  map<pg_shard_t, pg_info_t> all_info;
  pg_history_t history;
  history.epoch_created = 1;
  history.same_interval_since = 1;
  
  // OSD 1 is down, so don't include it in all_info (we can't get pg_info from a down OSD)
  for (unsigned i = 0; i < 6; i++) {
    if (i == 1) continue; // Skip OSD 1 (down)
    pg_shard_t shard(i, shard_id_t(i));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(i)));
    info.history = history;
    info.last_update = eversion_t(1, i);
    all_info[shard] = info;
  }
  
  // Add OSD 6 (dc0) with pg_info for shard 1
  // OSD 6 is now in the up set (CRUSH remapped 1→6)
  pg_shard_t shard_6(6, shard_id_t(1));
  pg_info_t info_6(spg_t(pg_t(1, pool_id), shard_id_t(1)));
  info_6.history = history;
  info_6.last_update = eversion_t(1, 1); // Same update as OSD 1 would have had
  all_info[shard_6] = info_6;
  
  auto auth_log_shard = all_info.find(pg_shard_t(0, shard_id_t(0)));
  ASSERT_NE(auth_log_shard, all_info.end());
  
  // Call calc_ec_acting_stretch
  vector<int> want;
  set<pg_shard_t> backfill;
  set<pg_shard_t> acting_backfill;
  ostringstream ss;
  
  PeeringState::calc_ec_acting_stretch(
    auth_log_shard,
    pool->size,
    acting,
    up,
    all_info,
    false,
    &want,
    &backfill,
    &acting_backfill,
    osdmap,
    pgpool,
    ss);
  
  // Verify want vector
  ASSERT_EQ(want.size(), 6);
  
  // Position 1 should be filled by OSD 6 from up[1]
  // CRUSH remapped 1→6 when OSD 1 went down
  EXPECT_EQ(want[1], 6) << "Position 1 should select up[1] = OSD 6 from dc0";
  
  // Verify OSD 6 is in dc0 (same zone as OSD 1 was)
  int zone_want1 = osdmap->crush->get_parent_of_type(want[1], 9, pool->crush_rule);
  int zone_osd0 = osdmap->crush->get_parent_of_type(0, 9, pool->crush_rule);
  EXPECT_EQ(zone_want1, zone_osd0) << "OSD 6 must be from dc0, same zone as failed OSD 1";
  
  // Verify other positions unchanged
  EXPECT_EQ(want[0], 0);
  EXPECT_EQ(want[2], 2);
  EXPECT_EQ(want[3], 3);
  EXPECT_EQ(want[4], 4);
  EXPECT_EQ(want[5], 5);
}
