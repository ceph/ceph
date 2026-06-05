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
    pool_info.opts.set(pool_opts_t::NUM_ZONES, static_cast<int64_t>(2));
    
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


/**
 * Test: CRUSH rehash - osds change shard zone (location in vector)
 *
 * Scenario: up set OSDs are flip zones from acting - expect OSDs to not mix zones
 * Expected: want set should be the same as up (OSDs do not mix zones)
 */
TEST_F(TestECActingStretch, CRUSH_rehash) {
  const pg_pool_t* pool = osdmap->get_pg_pool(pool_id);
  ASSERT_NE(pool, nullptr);
  PGPool pgpool(osdmap, pool_id, *pool, "test_ec_pool");

  // OSD 1 is down - shard position 1 in dc0
  // CRUSH remaps position 1 to OSD 6 (also in dc0)
  vector<int> up = {3, 4, 5, 0, 1, 2};     // Current CRUSH mapping 
  vector<int> acting = {0, 1, 2, 3, 4, 5}; // Old acting set

  // Build all_info map
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
  
  std::cerr << ss.str();
  // Verify want vector
  ASSERT_EQ(want.size(), 6);
    // Verify zone distribution: OSDs 0-2 in dc0, OSDs 3-5 in dc1
  for (int i = 0; i < 3; i++) {
    int dc0 = osdmap->crush->get_parent_of_type(i, 9, pool->crush_rule);
    int dc1 = osdmap->crush->get_parent_of_type(i + 3, 9, pool->crush_rule);
    EXPECT_NE(dc0, dc1) << "dc0 and dc1 should be different buckets";
  }
    // Verify want contains all 6 OSDs
  EXPECT_EQ(want.size(), 6);
  EXPECT_EQ(want[0], 3);
  EXPECT_EQ(want[1], 4);
  EXPECT_EQ(want[2], 5);
  EXPECT_EQ(want[3], 0);
  EXPECT_EQ(want[4], 1);
  EXPECT_EQ(want[5], 2);
  
  // Verify no backfill needed
  EXPECT_FALSE(backfill.empty());
}

/**
 * Test: Stray selected from correct zone via all_info_by_rel_shard
 *
 * Scenario: up[1] is CRUSH_ITEM_NONE (OSD 1 down, no CRUSH remap available),
 * acting[1] = 1 (also absent from all_info). A stray OSD 6 (dc0) holds
 * shard 1 and should be selected. A stray OSD 7 (dc1) also holds a shard
 * with the same relative position but must NOT be selected (wrong zone).
 *
 * Expected: Want should only use strays from same zone.
 */
TEST_F(TestECActingStretch, StraySearch_CorrectZoneOnly) {
  const pg_pool_t* pool = osdmap->get_pg_pool(pool_id);
  ASSERT_NE(pool, nullptr);
  PGPool pgpool(osdmap, pool_id, *pool, "test_ec_pool");

  // OSD 1 is down, CRUSH has no replacement — position 1 is CRUSH_ITEM_NONE in up
  vector<int> up    = {0, CRUSH_ITEM_NONE, 2, 3, 4, 5};
  vector<int> acting = {0, 1, 2, 3, 4, 5};

  map<pg_shard_t, pg_info_t> all_info;
  pg_history_t history;
  history.epoch_created = 1;
  history.same_interval_since = 1;

  // All current acting OSDs except OSD 1 (down)
  for (int i = 0; i < 6; i++) {
    if (i == 1) continue;
    pg_shard_t shard(i, shard_id_t(i));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(i)));
    info.history = history;
    info.last_update = eversion_t(1, 1);
    all_info[shard] = info;
  }

  // OSD 6 (dc0) holds shard 1 — correct zone, should be selected as stray
  {
    pg_shard_t shard(6, shard_id_t(1));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(1)));
    info.history = history;
    info.last_update = eversion_t(1, 1);
    all_info[shard] = info;
  }

  // OSD 7 (dc1) holds shard 4 (relative shard 1 in dc1) — wrong zone for position 1
  {
    pg_shard_t shard(7, shard_id_t(4));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(4)));
    info.history = history;
    info.last_update = eversion_t(1, 1);
    all_info[shard] = info;
  }

  auto auth_log_shard = all_info.find(pg_shard_t(0, shard_id_t(0)));
  ASSERT_NE(auth_log_shard, all_info.end());

  vector<int> want;
  set<pg_shard_t> backfill;
  set<pg_shard_t> acting_backfill;
  ostringstream ss;

  PeeringState::calc_ec_acting_stretch(
    auth_log_shard, pool->size, acting, up, all_info,
    false, &want, &backfill, &acting_backfill, osdmap, pgpool, ss);

  ASSERT_EQ((int)want.size(), 6);
  // Position 1 must be filled by OSD 6 (dc0 stray), not OSD 7 (dc1)
  EXPECT_EQ(want[1], 6) << "Stray OSD 6 from dc0 should fill position 1\n" << ss.str();
  // OSD 7 must not appear anywhere in want — wrong zone
  for (int i = 0; i < 6; i++) {
    EXPECT_NE(want[i], 7) << "OSD 7 (dc1) must not appear in want\n" << ss.str();
  }
  // All other positions unchanged
  EXPECT_EQ(want[0], 0);
  EXPECT_EQ(want[2], 2);
  EXPECT_EQ(want[3], 3);
  EXPECT_EQ(want[4], 4);
  EXPECT_EQ(want[5], 5);
}

/**
 * Test: up[i] behind log tail → acting fallback wins for same position
 *
 * Scenario: up[1] = OSD 6 (dc0, shard 1) exists in all_info but is behind
 * the auth log tail — it is added to backfill. acting[1] = OSD 1 (dc0,
 * shard 1) is current (last_update >= log_tail). The acting fallback loop
 * then runs for the same position and selects OSD 1.
 *
 * This exercises all three decision points for a single position:
 *   up[i] exists → backfilled (behind log) → acting fallback → selected
 * and confirms the stray path is never needed.
 */
TEST_F(TestECActingStretch, UpBehindLog_ActingFallbackWins) {
  const pg_pool_t* pool = osdmap->get_pg_pool(pool_id);
  ASSERT_NE(pool, nullptr);
  PGPool pgpool(osdmap, pool_id, *pool, "test_ec_pool");

  eversion_t log_tail(1, 5);
  eversion_t current(1, 5);
  eversion_t behind(1, 2);

  // CRUSH remapped position 1 to OSD 6; OSD 1 remains in acting
  vector<int> up     = {0, 6, 2, 3, 4, 5};
  vector<int> acting = {0, 1, 2, 3, 4, 5};

  map<pg_shard_t, pg_info_t> all_info;
  pg_history_t history;
  history.epoch_created = 1;
  history.same_interval_since = 1;

  // Positions 0,2,3,4,5 are current
  for (int i : {0, 2, 3, 4, 5}) {
    pg_shard_t s(i, shard_id_t(i));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(i)));
    info.history = history;
    info.last_update = current;
    all_info[s] = info;
  }
  // OSD 6 (up[1]) is behind the log tail — will be backfilled
  {
    pg_shard_t s(6, shard_id_t(1));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(1)));
    info.history = history;
    info.last_update = behind;
    all_info[s] = info;
  }
  // OSD 1 (acting[1]) is current — should win via acting fallback
  {
    pg_shard_t s(1, shard_id_t(1));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(1)));
    info.history = history;
    info.last_update = current;
    all_info[s] = info;
  }

  auto auth_log_shard = all_info.find(pg_shard_t(0, shard_id_t(0)));
  ASSERT_NE(auth_log_shard, all_info.end());
  auth_log_shard->second.log_tail = log_tail;

  vector<int> want;
  set<pg_shard_t> backfill;
  set<pg_shard_t> acting_backfill;
  ostringstream ss;

  PeeringState::calc_ec_acting_stretch(
    auth_log_shard, pool->size, acting, up, all_info,
    false, &want, &backfill, &acting_backfill, osdmap, pgpool, ss);

  ASSERT_EQ((int)want.size(), 6) << ss.str();
  // Acting fallback must win position 1 with OSD 1
  EXPECT_EQ(want[1], 1)
    << "Acting fallback OSD 1 should win position 1 (up[1]=OSD6 was behind)\n" << ss.str();
  // OSD 6 must be in backfill (behind log), not in want
  EXPECT_TRUE(backfill.count(pg_shard_t(6, shard_id_t(1))))
    << "OSD 6 should be in backfill\n" << ss.str();
  EXPECT_NE(want[1], 6) << "OSD 6 must not be in want\n" << ss.str();
  // Other positions unchanged
  EXPECT_EQ(want[0], 0);
  EXPECT_EQ(want[2], 2);
  EXPECT_EQ(want[3], 3);
  EXPECT_EQ(want[4], 4);
  EXPECT_EQ(want[5], 5);
}

/**
 * Test: up[i] behind log tail, acting[i] absent → stray wins for same position
 *
 * Scenario: up[1] = OSD 6 (dc0, shard 1) is behind the auth log tail —
 * added to backfill. acting[1] = OSD 1 is fully absent from all_info
 * (completely gone). The stray search then runs for the same position.
 * OSD 6 is also present in all_info_by_rel_shard as a stray with the same
 * shard but is behind — so it too fails the log_tail check.
 * OSD 7 (dc1, shard 4 = rel-shard 1) is current but in the wrong zone.
 * A second stray, OSD 6 at shard 1, cannot fill want (it's behind).
 * Result: position 1 must be CRUSH_ITEM_NONE.
 *
 * This exercises all three decision points for a single position:
 *   up[i] → backfill (behind log) → acting fallback → no info → stray search → none viable
 */
TEST_F(TestECActingStretch, UpBehindLog_ActingAbsent_StrayWrongZone_NoFill) {
  const pg_pool_t* pool = osdmap->get_pg_pool(pool_id);
  ASSERT_NE(pool, nullptr);
  PGPool pgpool(osdmap, pool_id, *pool, "test_ec_pool");

  eversion_t log_tail(1, 5);
  eversion_t current(1, 5);
  eversion_t behind(1, 2);

  vector<int> up     = {0, 6, 2, 3, 4, 5};
  vector<int> acting = {0, 1, 2, 3, 4, 5};  // OSD 1 still listed but has no pg_info

  map<pg_shard_t, pg_info_t> all_info;
  pg_history_t history;
  history.epoch_created = 1;
  history.same_interval_since = 1;

  // Positions 0,2,3,4,5 are current
  for (int i : {0, 2, 3, 4, 5}) {
    pg_shard_t s(i, shard_id_t(i));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(i)));
    info.history = history;
    info.last_update = current;
    all_info[s] = info;
  }
  // OSD 6 (up[1], dc0, shard 1) is behind — backfilled; also appears as stray candidate
  {
    pg_shard_t s(6, shard_id_t(1));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(1)));
    info.history = history;
    info.last_update = behind;
    all_info[s] = info;
  }
  // OSD 1 (acting[1]) NOT in all_info — completely gone
  // OSD 7 (dc1, shard 4 = rel-shard 1) is current but wrong zone
  {
    pg_shard_t s(7, shard_id_t(4));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(4)));
    info.history = history;
    info.last_update = current;
    all_info[s] = info;
  }

  auto auth_log_shard = all_info.find(pg_shard_t(0, shard_id_t(0)));
  ASSERT_NE(auth_log_shard, all_info.end());
  auth_log_shard->second.log_tail = log_tail;

  vector<int> want;
  set<pg_shard_t> backfill;
  set<pg_shard_t> acting_backfill;
  ostringstream ss;

  PeeringState::calc_ec_acting_stretch(
    auth_log_shard, pool->size, acting, up, all_info,
    false, &want, &backfill, &acting_backfill, osdmap, pgpool, ss);

  ASSERT_EQ((int)want.size(), 6) << ss.str();
  // All three paths exhausted — position 1 must be CRUSH_ITEM_NONE
  EXPECT_EQ(want[1], CRUSH_ITEM_NONE)
    << "Position 1 should be NONE: up behind, acting gone, stray wrong zone\n" << ss.str();
  // OSD 6 must be in backfill (it is up but behind the log)
  EXPECT_TRUE(backfill.count(pg_shard_t(6, shard_id_t(1))))
    << "OSD 6 (behind) should be in backfill\n" << ss.str();
  // OSD 7 (wrong zone) must never appear in want
  for (int i = 0; i < 6; i++)
    EXPECT_NE(want[i], 7) << "OSD 7 (dc1) must not appear in want\n" << ss.str();
  // Other positions unchanged
  EXPECT_EQ(want[0], 0);
  EXPECT_EQ(want[2], 2);
  EXPECT_EQ(want[3], 3);
  EXPECT_EQ(want[4], 4);
  EXPECT_EQ(want[5], 5);
}

/**
 * Test: up[i] behind log tail, acting[i] absent → correct-zone stray fills position
 *
 * Scenario: Same setup as UpBehindLog_ActingAbsent_StrayWrongZone_NoFill, but
 * now a second stray OSD — OSD 6 at shard 1 is still behind, but there is a
 * *different* stray OSD available: a fresh OSD 6 holding shard 1 that doesn't
 * appear in backfill path (it was never in up). To model this cleanly we use
 * two distinct strays: OSD 6 (shard 1, behind, in up/backfill) and a
 * hypothetical fresh dc0 stray. Since the fixture only has one spare OSD per
 * zone (OSD 6 for dc0), we instead test with the OSD 6 stray *not* in up so
 * it is not backfilled, making it eligible for stray selection.
 *
 * Concretely: up[1] = CRUSH_ITEM_NONE (no remap), acting[1] = OSD 1 absent,
 * stray OSD 6 (dc0, shard 1) is current → stray wins.
 * OSD 7 (dc1, shard 4) is also current but wrong zone → rejected.
 *
 * This is the full three-path walk with a successful stray resolution:
 *   up[i]=NONE (skipped) → acting fallback → no info → stray search → OSD 6 wins
 */
TEST_F(TestECActingStretch, UpNone_ActingAbsent_StrayFills) {
  const pg_pool_t* pool = osdmap->get_pg_pool(pool_id);
  ASSERT_NE(pool, nullptr);
  PGPool pgpool(osdmap, pool_id, *pool, "test_ec_pool");

  // up[1] has no CRUSH remap; OSD 1 absent from all_info
  vector<int> up     = {0, CRUSH_ITEM_NONE, 2, 3, 4, 5};
  vector<int> acting = {0, 1, 2, 3, 4, 5};

  map<pg_shard_t, pg_info_t> all_info;
  pg_history_t history;
  history.epoch_created = 1;
  history.same_interval_since = 1;

  // All acting OSDs except OSD 1 (missing)
  for (int i : {0, 2, 3, 4, 5}) {
    pg_shard_t s(i, shard_id_t(i));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(i)));
    info.history = history;
    info.last_update = eversion_t(1, 1);
    all_info[s] = info;
  }
  // OSD 6 (dc0, shard 1) is a current stray — correct zone for position 1
  {
    pg_shard_t s(6, shard_id_t(1));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(1)));
    info.history = history;
    info.last_update = eversion_t(1, 1);
    all_info[s] = info;
  }
  // OSD 7 (dc1, shard 4 = rel-shard 1) is current — wrong zone
  {
    pg_shard_t s(7, shard_id_t(4));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(4)));
    info.history = history;
    info.last_update = eversion_t(1, 1);
    all_info[s] = info;
  }

  auto auth_log_shard = all_info.find(pg_shard_t(0, shard_id_t(0)));
  ASSERT_NE(auth_log_shard, all_info.end());

  vector<int> want;
  set<pg_shard_t> backfill;
  set<pg_shard_t> acting_backfill;
  ostringstream ss;

  PeeringState::calc_ec_acting_stretch(
    auth_log_shard, pool->size, acting, up, all_info,
    false, &want, &backfill, &acting_backfill, osdmap, pgpool, ss);

  ASSERT_EQ((int)want.size(), 6) << ss.str();
  // Stray OSD 6 (dc0) must fill position 1
  EXPECT_EQ(want[1], 6)
    << "Stray OSD 6 (dc0) should fill position 1: up=NONE, acting absent\n" << ss.str();
  // OSD 7 (dc1 wrong zone) must not appear
  for (int i = 0; i < 6; i++)
    EXPECT_NE(want[i], 7) << "OSD 7 (dc1) must not appear in want\n" << ss.str();
  // No backfill — OSD 6 was not in up
  EXPECT_FALSE(backfill.count(pg_shard_t(6, shard_id_t(1))))
    << "OSD 6 should not be in backfill (was not in up)\n" << ss.str();
  EXPECT_EQ(want[0], 0);
  EXPECT_EQ(want[2], 2);
  EXPECT_EQ(want[3], 3);
  EXPECT_EQ(want[4], 4);
  EXPECT_EQ(want[5], 5);
}

/**
 * Test: CRUSH_ITEM_NONE in up — expected_zone derived from zone block, not up[i]
 *
 * Scenario: up[0] is CRUSH_ITEM_NONE but up[1] and up[2] are valid dc0 OSDs.
 * The expected_zone for position 0 must still be dc0 (derived from
 * positions in the same zone block). A stray OSD (dc0) holding shard 0
 * should be selected; a stray from dc1 must not be.
 *
 * Expected: Scans the zone block for the first non-NONE OSD rather than relying on up[i] directly.
 */
TEST_F(TestECActingStretch, ZoneFromBlock_UpPositionNone) {
  const pg_pool_t* pool = osdmap->get_pg_pool(pool_id);
  ASSERT_NE(pool, nullptr);
  PGPool pgpool(osdmap, pool_id, *pool, "test_ec_pool");

  // OSD 0 is down; CRUSH has no replacement for position 0.
  // Positions 1 and 2 are still in dc0 — zone block 0 is dc0.
  vector<int> up    = {CRUSH_ITEM_NONE, 1, 2, 3, 4, 5};
  vector<int> acting = {0, 1, 2, 3, 4, 5};

  map<pg_shard_t, pg_info_t> all_info;
  pg_history_t history;
  history.epoch_created = 1;
  history.same_interval_since = 1;

  // All acting OSDs except OSD 0 (down)
  for (int i = 1; i < 6; i++) {
    pg_shard_t shard(i, shard_id_t(i));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(i)));
    info.history = history;
    info.last_update = eversion_t(1, 1);
    all_info[shard] = info;
  }

  // OSD 6 (dc0) holds shard 0 — correct zone, should be selected as stray
  {
    pg_shard_t shard(6, shard_id_t(0));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(0)));
    info.history = history;
    info.last_update = eversion_t(1, 1);
    all_info[shard] = info;
  }

  // OSD 7 (dc1) holds shard 3 (relative shard 0 in dc1) — wrong zone for position 0
  {
    pg_shard_t shard(7, shard_id_t(3));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(3)));
    info.history = history;
    info.last_update = eversion_t(1, 1);
    all_info[shard] = info;
  }

  auto auth_log_shard = all_info.find(pg_shard_t(1, shard_id_t(1)));
  ASSERT_NE(auth_log_shard, all_info.end());

  vector<int> want;
  set<pg_shard_t> backfill;
  set<pg_shard_t> acting_backfill;
  ostringstream ss;

  PeeringState::calc_ec_acting_stretch(
    auth_log_shard, pool->size, acting, up, all_info,
    false, &want, &backfill, &acting_backfill, osdmap, pgpool, ss);

  ASSERT_EQ((int)want.size(), 6);
  // Position 0 must be filled by OSD 6 (dc0 stray), not OSD 7 (dc1)
  EXPECT_EQ(want[0], 6) << "Stray OSD 6 from dc0 should fill position 0\n" << ss.str();
  for (int i = 0; i < 6; i++) {
    EXPECT_NE(want[i], 7) << "OSD 7 (dc1) must not appear in want\n" << ss.str();
  }
}

/**
 * Test: Acting set size less than up set (num_zones transition)
 *
 * Scenario: Pool is transitioning from 1 zone (size 3) to 2 zones (size 6).
 * acting.size() == 3 (old epoch), up.size() == 6 (new epoch).
 * The acting fallback loop must not mis-stride through the old acting set.
 * All 6 positions should be filled from up or stray; acting OSDs at old
 * positions must not corrupt zone accounting.
 *
 * Expected: acting is handled safely when sizes differ.
 */
TEST_F(TestECActingStretch, ActingSizeLess_NumZonesTransition) {
  const pg_pool_t* pool = osdmap->get_pg_pool(pool_id);
  ASSERT_NE(pool, nullptr);
  PGPool pgpool(osdmap, pool_id, *pool, "test_ec_pool");

  // New pool: size 6 (2 zones x 3 shards). up is full size.
  // Old acting: size 3 (1 zone x 3 shards) — from previous epoch.
  vector<int> up    = {0, 1, 2, 3, 4, 5};  // new layout, both zones
  vector<int> acting = {0, 1, 2};           // old layout, one zone only

  map<pg_shard_t, pg_info_t> all_info;
  pg_history_t history;
  history.epoch_created = 1;
  history.same_interval_since = 1;

  // All 6 up OSDs have valid info for their new shard positions
  for (int i = 0; i < 6; i++) {
    pg_shard_t shard(i, shard_id_t(i));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(i)));
    info.history = history;
    info.last_update = eversion_t(1, 1);
    all_info[shard] = info;
  }

  auto auth_log_shard = all_info.find(pg_shard_t(0, shard_id_t(0)));
  ASSERT_NE(auth_log_shard, all_info.end());

  vector<int> want;
  set<pg_shard_t> backfill;
  set<pg_shard_t> acting_backfill;
  ostringstream ss;

  PeeringState::calc_ec_acting_stretch(
    auth_log_shard, pool->size, acting, up, all_info,
    false, &want, &backfill, &acting_backfill, osdmap, pgpool, ss);

  ASSERT_EQ((int)want.size(), 6);
  // All positions should be filled from the up set
  for (int i = 0; i < 6; i++) {
    EXPECT_EQ(want[i], i) << "Position " << i << " should be filled from up\n" << ss.str();
  }
  // No backfill needed — all up OSDs have valid info
  EXPECT_TRUE(backfill.empty()) << "No backfill needed\n" << ss.str();
  // Zone distribution: positions 0-2 in dc0, positions 3-5 in dc1
  int zone0 = osdmap->crush->get_parent_of_type(want[0], 9, pool->crush_rule);
  int zone1 = osdmap->crush->get_parent_of_type(want[3], 9, pool->crush_rule);
  EXPECT_NE(zone0, zone1) << "Positions 0-2 and 3-5 must be in different zones\n" << ss.str();
}

/**
 * Test: Acting fallback used when up[i] needs backfill
 *
 * Scenario: up[2] (OSD 2, dc0) is behind the auth log tail — needs backfill.
 * acting[2] (OSD 2) is also behind. A different acting OSD at the same
 * relative shard position in dc0 (via CRUSH rehash) is current and should
 * be selected instead.
 *
 */
TEST_F(TestECActingStretch, ActingFallback_RelativeShardStride) {
  const pg_pool_t* pool = osdmap->get_pg_pool(pool_id);
  ASSERT_NE(pool, nullptr);
  PGPool pgpool(osdmap, pool_id, *pool, "test_ec_pool");

  // OSD 6 (dc0) has been rehashed by CRUSH to position 2
  vector<int> up    = {0, 1, 6, 3, 4, 5};
  vector<int> acting = {0, 1, 2, 3, 4, 5};

  map<pg_shard_t, pg_info_t> all_info;
  pg_history_t history;
  history.epoch_created = 1;
  history.same_interval_since = 1;
  eversion_t current(2, 10);  // auth log version

  // OSDs 0, 1, 3, 4, 5 are current
  for (int i : {0, 1, 3, 4, 5}) {
    pg_shard_t shard(i, shard_id_t(i));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(i)));
    info.history = history;
    info.last_update = current;
    all_info[shard] = info;
  }

  // OSD 6 at position 2 (up[2]) is behind — needs backfill
  {
    pg_shard_t shard(6, shard_id_t(2));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(2)));
    info.history = history;
    info.last_update = eversion_t(1, 1);  // behind auth log tail
    info.log_tail = eversion_t(1, 1);
    all_info[shard] = info;
  }

  // OSD 2 (acting[2]) is current — should be selected via acting fallback
  {
    pg_shard_t shard(2, shard_id_t(2));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(2)));
    info.history = history;
    info.last_update = current;
    all_info[shard] = info;
  }

  auto auth_log_shard = all_info.find(pg_shard_t(0, shard_id_t(0)));
  ASSERT_NE(auth_log_shard, all_info.end());
  // Set auth log tail so OSD 6 is behind it
  auth_log_shard->second.log_tail = eversion_t(1, 5);

  vector<int> want;
  set<pg_shard_t> backfill;
  set<pg_shard_t> acting_backfill;
  ostringstream ss;

  PeeringState::calc_ec_acting_stretch(
    auth_log_shard, pool->size, acting, up, all_info,
    false, &want, &backfill, &acting_backfill, osdmap, pgpool, ss);

  ASSERT_EQ((int)want.size(), 6);
  // Position 2: up[2]=OSD6 is behind, acting[2]=OSD2 is current → select OSD 2
  EXPECT_EQ(want[2], 2) << "Acting fallback should select OSD 2 for position 2\n" << ss.str();
  // OSD 6 should be in backfill (it's in up but behind)
  EXPECT_TRUE(backfill.count(pg_shard_t(6, shard_id_t(2))))
    << "OSD 6 should be marked for backfill\n" << ss.str();
  // Other positions unchanged
  EXPECT_EQ(want[0], 0);
  EXPECT_EQ(want[1], 1);
  EXPECT_EQ(want[3], 3);
  EXPECT_EQ(want[4], 4);
  EXPECT_EQ(want[5], 5);
}

/**
 * Test: bucket_max enforced — no zone gets more than zone_size shards
 *
 * Scenario: CRUSH maps positions 0-3 all to dc0 OSDs (e.g. due to dc1 being
 * entirely down). bucket_max = 3. Positions 0, 1, 2 should be filled from
 * dc0; position 3 must be CRUSH_ITEM_NONE (dc0 is at max, dc1 has no OSDs).
 *
 * This tests that zone_at_max() correctly caps selection per zone.
 */
TEST_F(TestECActingStretch, BucketMax_ZoneCapEnforced) {
  const pg_pool_t* pool = osdmap->get_pg_pool(pool_id);
  ASSERT_NE(pool, nullptr);
  PGPool pgpool(osdmap, pool_id, *pool, "test_ec_pool");

  // dc1 is entirely down — CRUSH maps positions 3-5 to CRUSH_ITEM_NONE
  vector<int> up    = {0, 1, 2, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE};
  vector<int> acting = {0, 1, 2, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE};

  map<pg_shard_t, pg_info_t> all_info;
  pg_history_t history;
  history.epoch_created = 1;
  history.same_interval_since = 1;

  // Only dc0 OSDs available
  for (int i = 0; i < 3; i++) {
    pg_shard_t shard(i, shard_id_t(i));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(i)));
    info.history = history;
    info.last_update = eversion_t(1, 1);
    all_info[shard] = info;
  }

  // OSD 6 (dc0) holds shard 0 — stray, but dc0 is already at bucket_max after
  // positions 0,1,2 are filled. Should NOT be selected for positions 3-5.
  {
    pg_shard_t shard(6, shard_id_t(0));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(0)));
    info.history = history;
    info.last_update = eversion_t(1, 1);
    all_info[shard] = info;
  }

  auto auth_log_shard = all_info.find(pg_shard_t(0, shard_id_t(0)));
  ASSERT_NE(auth_log_shard, all_info.end());

  vector<int> want;
  set<pg_shard_t> backfill;
  set<pg_shard_t> acting_backfill;
  ostringstream ss;

  PeeringState::calc_ec_acting_stretch(
    auth_log_shard, pool->size, acting, up, all_info,
    false, &want, &backfill, &acting_backfill, osdmap, pgpool, ss);

  ASSERT_EQ((int)want.size(), 6);
  // dc0 fills its 3 positions
  EXPECT_EQ(want[0], 0);
  EXPECT_EQ(want[1], 1);
  EXPECT_EQ(want[2], 2);
  // dc1 positions cannot be filled — dc0 is at bucket_max, dc1 has no OSDs
  EXPECT_EQ(want[3], CRUSH_ITEM_NONE) << "dc1 position 3 must be NONE — dc0 at max\n" << ss.str();
  EXPECT_EQ(want[4], CRUSH_ITEM_NONE) << "dc1 position 4 must be NONE\n" << ss.str();
  EXPECT_EQ(want[5], CRUSH_ITEM_NONE) << "dc1 position 5 must be NONE\n" << ss.str();
}

/**
 * Test: Rehash + all three resolution paths in one call
 *
 * The pool previously had acting={0,1,2,3,4,5}. A CRUSH rehash moved the
 * zones: dc1 now maps to positions 0-2 and dc0 to 3-5.
 *
 *   acting = [0, 1, 2, 3, 4, 5]   previous epoch
 *   up = {3, 4, 5, NONE, NONE, 2}
 */
TEST_F(TestECActingStretch, Rehash_AllThreePaths) {
  const pg_pool_t* pool = osdmap->get_pg_pool(pool_id);
  ASSERT_NE(pool, nullptr);
  PGPool pgpool(osdmap, pool_id, *pool, "test_ec_pool");

  eversion_t log_tail(1, 5);
  eversion_t current(1, 5);
  eversion_t behind(1, 2);

  // Zones are swapped vs acting. Positions 3 and 4 have no CRUSH remap.
  // Block 1's zone is inferred from up[5]=OSD2 (dc0), the only non-NONE in block 1.
  vector<int> up     = {3, 4, 5,  CRUSH_ITEM_NONE, CRUSH_ITEM_NONE, 2};
  vector<int> acting = {0, 1, 2,  3, 4, 5};

  map<pg_shard_t, pg_info_t> all_info;
  pg_history_t history;
  history.epoch_created = 1;
  history.same_interval_since = 1;

  for (int i = 0; i < 6; i++) {
    if (i == 0 || i == 1) //down OSDs
      continue;
    pg_shard_t shard(i, shard_id_t(i));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(i)));
    info.history = history;
    info.last_update = current;  // must be >= log_tail for acting fallback to accept
    all_info[shard] = info;
  }

  // dc0 OSD 6 at shard 0 (acting[0]) — current.
  {
    pg_shard_t s(6, shard_id_t(0));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(0)));
    info.history = history;
    info.last_update = current;
    all_info[s] = info;
  }

  // Auth shard: OSD 3 is now at pos 0 in the rehashed layout.
  auto auth_log_shard = all_info.find(pg_shard_t(3, shard_id_t(3)));
  ASSERT_NE(auth_log_shard, all_info.end());
  auth_log_shard->second.log_tail = log_tail;

  vector<int> want;
  set<pg_shard_t> backfill;
  set<pg_shard_t> acting_backfill;
  ostringstream ss;

  PeeringState::calc_ec_acting_stretch(
    auth_log_shard, pool->size, acting, up, all_info,
    false, &want, &backfill, &acting_backfill, osdmap, pgpool, ss);

  ASSERT_EQ((int)want.size(), 6) << ss.str();

  // pos 0-2: UP wins (rehashed dc1 block)
  EXPECT_EQ(want[0], 3) << "pos 0: up OSD3 (dc1) wins\n"              << ss.str();
  EXPECT_EQ(want[1], 4) << "pos 1: up OSD4 (dc1) wins\n"              << ss.str();
  EXPECT_EQ(want[2], 5) << "pos 2: up OSD5 (dc1) wins\n"              << ss.str();

  // pos 3: up=NONE, stray fallback picks OSD0 (dc6, rel=0, j=0)
  EXPECT_EQ(want[3], 6) << "pos 3: acting fallback OSD0 (dc6) wins\n" << ss.str();

  EXPECT_EQ(want[4], CRUSH_ITEM_NONE) << "dc1 position 4 must be NONE\n" << ss.str();

  // pos 5: UP wins (dc0 OSD2 at shard 5)
  EXPECT_EQ(want[5], 2) << "pos 5: up OSD2 (dc0) wins\n"              << ss.str();

  // No backfill — no OSD was in up and behind the log
  EXPECT_FALSE(backfill.empty()) << "Backfill expected\n"            << ss.str();

  // Zone isolation: block 0 (pos 0-2) all dc1; block 1 (pos 3-5) all dc0
  int z_dc1 = osdmap->crush->get_parent_of_type(3, 9, pool->crush_rule);
  int z_dc0 = osdmap->crush->get_parent_of_type(0, 9, pool->crush_rule);
  EXPECT_NE(z_dc0, z_dc1);
}

/**
 * TestECActingStretch3Zone - Unit tests for stretch mode EC acting set selection
 *                            with 3 zones.
 *
 * Test Configuration:
 * - 3 zones (datacenters), 4 hosts per zone, 1 OSD per host = 12 OSDs total
 *   dc0: OSDs 0,1,2,9    (hosts host0..host2, host9)
 *   dc1: OSDs 3,4,5,10   (hosts host3..host5, host10)
 *   dc2: OSDs 6,7,8,11   (hosts host6..host8, host11)
 * - Stretched 2+1 EC: pool.size = 9  (3 zones × 3 shards each)
 *   zone_size = 3,  bucket_max = 3
 *   Shard layout: shards 0-2 → dc0 | shards 3-5 → dc1 | shards 6-8 → dc2
 * - Normal acting set: OSDs 0-8
 * - Extra stray OSDs: 9 (dc0), 10 (dc1), 11 (dc2)
 * - CRUSH rule: choose firstn 3 type datacenter, chooseleaf indep 3 type host
 */
class TestECActingStretch3Zone : public ECPeeringTestFixture {
protected:
  void SetUp() override {
    ECPeeringTestFixture::SetUp();
    setup_3zone_ec_pool();
  }

  void setup_3zone_ec_pool() {
    auto new_osdmap = std::make_shared<OSDMap>();
    new_osdmap->set_max_osd(12);
    for (int i = 0; i < 12; ++i)
      new_osdmap->set_state(i, CEPH_OSD_EXISTS | CEPH_OSD_UP);
    new_osdmap->set_epoch(1);

    CrushWrapper crush;
    crush.create();
    crush.set_type_name(10, "root");
    crush.set_type_name(9,  "datacenter");
    crush.set_type_name(1,  "host");
    crush.set_type_name(0,  "osd");

    int root_id;
    crush.add_bucket(0, CRUSH_BUCKET_STRAW2, CRUSH_HASH_RJENKINS1,
                     10 /*type*/, 0, nullptr, nullptr, &root_id);
    crush.set_item_name(root_id, "default");

    // dc0: OSDs 0,1,2,9  | dc1: OSDs 3,4,5,10 | dc2: OSDs 6,7,8,11
    static const int dc_osds[3][4] = {
      {0, 1, 2, 9},
      {3, 4, 5, 10},
      {6, 7, 8, 11},
    };
    for (int dc = 0; dc < 3; ++dc) {
      std::string dc_name = "dc" + std::to_string(dc);
      for (int h = 0; h < 4; ++h) {
        int osd_id = dc_osds[dc][h];
        std::map<std::string, std::string> loc;
        loc["root"]       = "default";
        loc["datacenter"] = dc_name;
        loc["host"]       = "host" + std::to_string(osd_id);
        crush.insert_item(g_ceph_context, osd_id, 1.0,
                          "osd." + std::to_string(osd_id), loc);
      }
    }

    // CRUSH rule: choose 3 datacenters, chooseleaf indep 3 hosts per dc
    int rule_id = 0;
    root_id = crush.get_item_id("default");
    int steps = 6;
    crush_rule *rule = crush_make_rule(steps, pg_pool_t::TYPE_ERASURE);
    int step = 0;
    crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSELEAF_TRIES, 5,   0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSE_TRIES,     100, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_TAKE, root_id, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_CHOOSE_INDEP,     3, 9 /* datacenter */);
    crush_rule_set_step(rule, step++, CRUSH_RULE_CHOOSELEAF_INDEP, 3, 1 /* host */);
    crush_rule_set_step(rule, step++, CRUSH_RULE_EMIT, 0, 0);
    ASSERT_EQ(step, steps);
    int r = crush_add_rule(crush.get_crush_map(), rule, rule_id);
    ASSERT_GE(r, 0);
    crush.set_rule_name(rule_id, "mirrored_ec_3zone_rule");

    OSDMap::Incremental inc(2);
    inc.fsid = new_osdmap->get_fsid();
    crush.encode(inc.crush, CEPH_FEATURES_SUPPORTED_DEFAULT);
    new_osdmap->apply_incremental(inc);

    pool_id = 1;
    pg_pool_t pool_info;
    pool_info.type          = pg_pool_t::TYPE_ERASURE;
    pool_info.size          = 9;  // 3 zones * 3 shards
    pool_info.min_size      = 7;  // tolerate 2 OSD failures
    pool_info.crush_rule    = rule_id;
    pool_info.set_pg_num(8);
    pool_info.set_pgp_num(8);
    pool_info.opts.set(pool_opts_t::NUM_ZONES, static_cast<int64_t>(3));

    std::map<std::string, std::string> ec_profile = {
      {"k", "2"}, {"m", "1"}, {"plugin", "jerasure"}, {"technique", "reed_sol_van"}
    };
    new_osdmap->set_erasure_code_profile("default", ec_profile);
    pool_info.erasure_code_profile = "default";

    pool_info.peering_crush_bucket_barrier = 9; // datacenter type
    pool_info.peering_crush_bucket_target  = 3; // 3 datacenters
    pool_info.peering_crush_bucket_count   = 3;
    pool_info.peering_crush_mandatory_member = CRUSH_ITEM_NONE;
    pool_info.set_flag(pg_pool_t::FLAG_EC_OVERWRITES);

    OSDMapTestHelpers::add_pool(new_osdmap, pool_id, pool_info, "test_ec_pool_3z");
    osdmap = new_osdmap;
  }
};

// ── TestECActingStretch3Zone tests ───────────────────────────────────────────

/**
 * Test: All OSDs up across 3 zones — happy path
 *
 * Scenario: Perfect state, all 9 OSDs healthy, CRUSH maps shards 0-2→dc0,
 *           3-5→dc1, 6-8→dc2.
 * Expected: want == up == [0,1,2,3,4,5,6,7,8], no backfill.
 */
TEST_F(TestECActingStretch3Zone, AllUp_ThreeZones) {
  const pg_pool_t* pool = osdmap->get_pg_pool(pool_id);
  ASSERT_NE(pool, nullptr);
  PGPool pgpool(osdmap, pool_id, *pool, "test_ec_pool_3z");

  vector<int> up     = {0,1,2,3,4,5,6,7,8};
  vector<int> acting = {0,1,2,3,4,5,6,7,8};

  map<pg_shard_t, pg_info_t> all_info;
  pg_history_t history;
  history.epoch_created = 1;
  history.same_interval_since = 1;
  
  for (unsigned i = 0; i < 9; i++) {
    pg_shard_t shard(i, shard_id_t(i));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(i)));
    info.history = history;
    info.last_update = eversion_t(1, i);
    all_info[shard] = info;
  }
  
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
  
  std::cerr << ss.str();
    // Verify want contains all 9 OSDs
  EXPECT_EQ(want.size(), 9);
  EXPECT_EQ(want[0], 0);
  EXPECT_EQ(want[1], 1);
  EXPECT_EQ(want[2], 2);
  EXPECT_EQ(want[3], 3);
  EXPECT_EQ(want[4], 4);
  EXPECT_EQ(want[5], 5);
  EXPECT_EQ(want[6], 6);
  EXPECT_EQ(want[7], 7);  
  EXPECT_EQ(want[8], 8);
  
  // Verify no backfill needed
  EXPECT_TRUE(backfill.empty());

  int z0 = osdmap->crush->get_parent_of_type(want[0], 9, pool->crush_rule);
  int z1 = osdmap->crush->get_parent_of_type(want[3], 9, pool->crush_rule);
  int z2 = osdmap->crush->get_parent_of_type(want[6], 9, pool->crush_rule);
  EXPECT_NE(z0, z1) << "dc0 and dc1 must differ\n" << ss.str();
  EXPECT_NE(z1, z2) << "dc1 and dc2 must differ\n" << ss.str();
  EXPECT_NE(z0, z2) << "dc0 and dc2 must differ\n" << ss.str();
}

TEST_F(TestECActingStretch3Zone, CRUSH_rehash_ThreeZones) {
  const pg_pool_t* pool = osdmap->get_pg_pool(pool_id);
  ASSERT_NE(pool, nullptr);
  PGPool pgpool(osdmap, pool_id, *pool, "test_ec_pool_3z");

  vector<int> up     = {3,4,5, 6,7,8, 0,1,2};
  vector<int> acting = {0,1,2, 3,4,5, 6,7,8};

  map<pg_shard_t, pg_info_t> all_info;
  pg_history_t history;
  history.epoch_created = 1;
  history.same_interval_since = 1;
  
  for (unsigned i = 0; i < 9; i++) {
    pg_shard_t shard(i, shard_id_t(i));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(i)));
    info.history = history;
    info.last_update = eversion_t(1, i);
    all_info[shard] = info;
  }
  
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
  
  std::cerr << ss.str();
    // Verify want contains all 9 OSDs
  EXPECT_EQ(want.size(), 9);
  EXPECT_EQ(want[0], 3);
  EXPECT_EQ(want[1], 4);
  EXPECT_EQ(want[2], 5);
  EXPECT_EQ(want[3], 6);
  EXPECT_EQ(want[4], 7);
  EXPECT_EQ(want[5], 8);
  EXPECT_EQ(want[6], 0);
  EXPECT_EQ(want[7], 1);  
  EXPECT_EQ(want[8], 2);
  
  // Verify backfill needed
  EXPECT_TRUE(!backfill.empty());

  int z0 = osdmap->crush->get_parent_of_type(want[0], 9, pool->crush_rule);
  int z1 = osdmap->crush->get_parent_of_type(want[3], 9, pool->crush_rule);
  int z2 = osdmap->crush->get_parent_of_type(want[6], 9, pool->crush_rule);
  EXPECT_NE(z0, z1) << "dc0 and dc1 must differ\n" << ss.str();
  EXPECT_NE(z1, z2) << "dc1 and dc2 must differ\n" << ss.str();
  EXPECT_NE(z0, z2) << "dc0 and dc2 must differ\n" << ss.str();
}

/**
 * Test: Transition from 3-zone pool to 2-zone pool
 *
 * Simulates a pool reconfiguration where dc2 is decommissioned.
 * The CRUSH map still has all 12 OSDs, but the new pool config targets only
 * 2 datacenters (size=6, peering_crush_bucket_target=2).
 *
 * Previous epoch (3-zone): acting = [0,1,2, 3,4,5, 6,7,8]
 *   all_info has {osd=i, shard=i} for i in 0..8 (all responded during peering).
 *   OSDs 6,7,8 (dc2) are the stale/retiring shards — they responded but the
 *   new pool config no longer allocates a zone for them.
 *
 * New epoch (2-zone):  pool.size=6, up = [3,4,5, 0,1,2]
 *   CRUSH now only maps 2 datacenters: dc1 (shards 0-2) and dc0 (shards 3-5).
 *
 * Expected: all 6 positions filled via the up-path (OSDs 0-5 are healthy and
 *           have matching all_info entries). No backfill needed.
 *           dc2 OSDs (6,7,8) must not appear in want.
 */
TEST_F(TestECActingStretch3Zone, ZoneTransition_ThreeToTwo) {
const pg_pool_t* pool = osdmap->get_pg_pool(pool_id);
  ASSERT_NE(pool, nullptr);
  PGPool pgpool(osdmap, pool_id, *pool, "test_ec_pool_3z");

  vector<int> up     = {3,4,5, 0,1,2};
  vector<int> acting = {0,1,2, 3,4,5, 6,7,8};

  map<pg_shard_t, pg_info_t> all_info;
  pg_history_t history;
  history.epoch_created = 1;
  history.same_interval_since = 1;
  
  for (unsigned i = 0; i < 9; i++) {
    pg_shard_t shard(i, shard_id_t(i));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(i)));
    info.history = history;
    info.last_update = eversion_t(1, i);
    all_info[shard] = info;
  }
  
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
  
  std::cerr << ss.str();
    // Verify want contains all 9 OSDs
  EXPECT_EQ(want.size(), 9);
  EXPECT_EQ(want[0], 3);
  EXPECT_EQ(want[1], 4);
  EXPECT_EQ(want[2], 5);
  EXPECT_EQ(want[3], 0);
  EXPECT_EQ(want[4], 1);  
  EXPECT_EQ(want[5], 2);
  
  // Verify backfill needed
  EXPECT_TRUE(!backfill.empty());

  int z0 = osdmap->crush->get_parent_of_type(want[0], 9, pool->crush_rule);
  int z1 = osdmap->crush->get_parent_of_type(want[3], 9, pool->crush_rule);
  EXPECT_NE(z0, z1) << "dc0 and dc1 must differ\n" << ss.str();
}

