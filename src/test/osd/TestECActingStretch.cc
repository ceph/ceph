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
    // Build the new map on top of the current epoch so same_interval_since
    // is always strictly less than the epoch we hand to advance_map().
    auto new_osdmap = std::make_shared<OSDMap>();
    new_osdmap->deepish_copy_from(*osdmap);

    // Expand to 8 OSDs and bring OSDs 0-7 up with full features.
    {
      OSDMap::Incremental inc(new_osdmap->get_epoch() + 1);
      inc.fsid = new_osdmap->get_fsid();
      new_osdmap->set_max_osd(8);
      for (int i = 0; i < 8; ++i) {
        if (!new_osdmap->is_up(i)) {
          // The in weight below makes a new OSD exist
          inc.new_state[i] = CEPH_OSD_UP;
        }
        inc.new_weight[i] = CEPH_OSD_IN;
        inc.new_up_thru[i] = 100;
        osd_xinfo_t xinfo;
        xinfo.features = CEPH_FEATUREMASK_SERVER_NAUTILUS |
                         CEPH_FEATUREMASK_SERVER_OCTOPUS  |
                         CEPH_FEATUREMASK_SERVER_QUINCY;
        inc.new_xinfo[i] = xinfo;
      }
      new_osdmap->apply_incremental(inc);
    }

    // Install the stretch CRUSH map.
    // dc0: OSDs 0,1,2,6 | dc1: OSDs 3,4,5,7
    {
      CrushWrapper crush;
      crush.create();
      crush.set_type_name(10, "root");
      crush.set_type_name(9,  "datacenter");
      crush.set_type_name(1,  "host");
      crush.set_type_name(0,  "osd");

      int root_id;
      crush.add_bucket(0, CRUSH_BUCKET_STRAW2, CRUSH_HASH_RJENKINS1,
                       10, 0, NULL, NULL, &root_id);
      crush.set_item_name(root_id, "default");

      for (int dc = 0; dc < 2; ++dc) {
        std::string dc_name = (dc == 0) ? "dc0" : "dc1";
        for (int h = 0; h < 4; ++h) {
          int osd_id = (dc == 0) ? (h < 3 ? h : 6) : (h < 3 ? h + 3 : 7);
          std::map<std::string, std::string> loc;
          loc["root"]       = "default";
          loc["datacenter"] = dc_name;
          loc["host"]       = "host" + std::to_string(osd_id);
          crush.insert_item(g_ceph_context, osd_id, 1.0,
                            "osd." + std::to_string(osd_id), loc);
        }
      }

      int rule_id = 0;
      root_id = crush.get_item_id("default");
      int steps = 6;
      crush_rule *rule = crush_make_rule(steps, pg_pool_t::TYPE_ERASURE);
      int step = 0;
      crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSELEAF_TRIES, 5, 0);
      crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSE_TRIES, 100, 0);
      crush_rule_set_step(rule, step++, CRUSH_RULE_TAKE, root_id, 0);
      crush_rule_set_step(rule, step++, CRUSH_RULE_CHOOSE_INDEP, 2, 9);
      crush_rule_set_step(rule, step++, CRUSH_RULE_CHOOSELEAF_INDEP, 3, 1);
      crush_rule_set_step(rule, step++, CRUSH_RULE_EMIT, 0, 0);
      ASSERT_EQ(step, steps);
      int r = crush_add_rule(crush.get_crush_map(), rule, rule_id);
      ASSERT_GE(r, 0);
      crush.set_rule_name(rule_id, "mirrored_ec_rule");

      OSDMap::Incremental inc(new_osdmap->get_epoch() + 1);
      inc.fsid = new_osdmap->get_fsid();
      crush.encode(inc.crush, CEPH_FEATURES_SUPPORTED_DEFAULT);
      new_osdmap->apply_incremental(inc);
    }

    // Add the stretch EC pool.
    pool_id = 1;
    pg_pool_t pool_info;
    pool_info.type     = pg_pool_t::TYPE_ERASURE;
    pool_info.size     = 6; // 2 zones * 3 shards
    pool_info.min_size = 5;
    pool_info.crush_rule = 0;
    pool_info.set_pg_num(8);
    pool_info.set_pgp_num(8);
    pool_info.num_zones = 2;

    std::map<std::string, std::string> erasure_code_profile = {
      {"k", "2"}, {"m", "1"}, {"plugin", "jerasure"}, {"technique", "reed_sol_van"}
    };
    new_osdmap->set_erasure_code_profile("default", erasure_code_profile);
    pool_info.erasure_code_profile = "default";
    pool_info.ec_data_shard_count = 2;
    pool_info.ec_coding_shard_count = 1;

    pool_info.peering_crush_bucket_barrier = 9;
    pool_info.peering_crush_bucket_target  = 2;
    pool_info.peering_crush_bucket_count   = 2;
    pool_info.peering_crush_mandatory_member = CRUSH_ITEM_NONE;
    pool_info.set_flag(pg_pool_t::FLAG_EC_OVERWRITES);

    OSDMapTestHelpers::add_pool(new_osdmap, pool_id, pool_info, "test_ec_pool");

    update_osdmap_with_peering(new_osdmap);
  }

  void add_info(map<pg_shard_t, pg_info_t> &all_info, int osd, int shard,
                eversion_t last_update, eversion_t log_tail = eversion_t()) {
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(shard)));
    info.history.epoch_created = 1;
    info.history.same_interval_since = 1;
    info.last_update = last_update;
    info.last_complete = last_update;
    info.log_tail = log_tail;
    all_info[pg_shard_t(osd, shard_id_t(shard))] = info;
  }

  void calc(const vector<int> &up, const vector<int> &acting,
            const map<pg_shard_t, pg_info_t> &all_info, pg_shard_t auth,
            bool restrict_to_up_acting, vector<int> *want,
            set<pg_shard_t> *backfill, set<pg_shard_t> *acting_backfill,
            ostringstream &ss, const pg_pool_t *pool_info = nullptr) {
    const pg_pool_t *pool =
      pool_info ? pool_info : osdmap->get_pg_pool(pool_id);
    ASSERT_NE(pool, nullptr);
    PGPool pgpool(osdmap, pool_id, *pool, "test_ec_pool");
    auto auth_it = all_info.find(auth);
    ASSERT_NE(auth_it, all_info.end());
    PeeringState::calc_ec_acting_stretch(
      auth_it, pool->size, acting, up, all_info, restrict_to_up_acting,
      want, backfill, acting_backfill, osdmap, pgpool, ss);
    ASSERT_EQ(want->size(), pool->size);
  }

  int dc_of(int osd) {
    return osdmap->crush->get_parent_of_type(
      osd, 9, osdmap->get_pg_pool(pool_id)->crush_rule);
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
 * Scenario: up set OSDs are flip zones from acting; they only hold the shards
 * of their old positions.
 * Expected: acting is kept and every up OSD is backfilled for its new shard.
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
  EXPECT_EQ(want, acting);
  EXPECT_EQ(backfill, (set<pg_shard_t>{
    pg_shard_t(3, shard_id_t(0)), pg_shard_t(4, shard_id_t(1)),
    pg_shard_t(5, shard_id_t(2)), pg_shard_t(0, shard_id_t(3)),
    pg_shard_t(1, shard_id_t(4)), pg_shard_t(2, shard_id_t(5))}));
}


/**
 * Test: CRUSH rehash degraded - osds change shard zone (location in vector)
 *
 * Scenario: the surviving zone's OSDs move from zone block 0 in acting to
 * zone block 1 in up, holding only their block 0 shards.
 * Expected: they keep serving block 0 and are backfilled for block 1.
 */
TEST_F(TestECActingStretch, CRUSH_rehash_degraded) {
  const pg_pool_t* pool = osdmap->get_pg_pool(pool_id);
  ASSERT_NE(pool, nullptr);
  PGPool pgpool(osdmap, pool_id, *pool, "test_ec_pool");

  // Zone 0 is down
  vector<int> acting = {5, 4, 3, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE};     // Old acting set
  vector<int> up = {CRUSH_ITEM_NONE, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE, 5, 4, 3}; // Current CRUSH mapping 

  // Build all_info map
  map<pg_shard_t, pg_info_t> all_info;
  pg_history_t history;
  history.epoch_created = 1;
  history.same_interval_since = 1;
  
  for (unsigned i = 0; i < 3; i++) {
    pg_shard_t shard(acting[i], shard_id_t(i));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(i)));
    info.history = history;
    info.last_update = eversion_t(1, i);
    all_info[shard] = info;
  }
  
  auto auth_log_shard = all_info.find(pg_shard_t(acting[0], shard_id_t(0)));
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
  EXPECT_EQ(want, acting);
  EXPECT_EQ(backfill, (set<pg_shard_t>{
    pg_shard_t(5, shard_id_t(3)), pg_shard_t(4, shard_id_t(4)),
    pg_shard_t(3, shard_id_t(5))}));
}
/**
 * Test: Stray selected only from the zone serving its block
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
 * OSD 6 is also a stray for the same shard but is behind — so it too fails
 * the log_tail check.
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
 * Test: CRUSH_ITEM_NONE in up — the zone comes from the whole block, not up[i]
 *
 * Scenario: up[0] is CRUSH_ITEM_NONE but up[1] and up[2] are valid dc0 OSDs.
 * Zone block 0 is served from dc0, which holds most of its shards, so a
 * stray OSD (dc0) holding shard 0 should be selected; a stray from dc1 must
 * not be.
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
 * This tests that bucket_max caps selection per zone.
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
 *
 * The up OSDs hold none of their new shards, so each zone block stays in the
 * zone holding its data: block 0 in dc0 (acting OSD 2 and stray OSD 6) and
 * block 1 in dc1 (acting OSDs 3-5).  Backfilling up would not change that,
 * as block 1 would have only OSD 2 in dc0, so nothing is backfilled.
 */
TEST_F(TestECActingStretch, Rehash_AllThreePaths) {
  const pg_pool_t* pool = osdmap->get_pg_pool(pool_id);
  ASSERT_NE(pool, nullptr);
  PGPool pgpool(osdmap, pool_id, *pool, "test_ec_pool");

  eversion_t log_tail(1, 5);
  eversion_t current(1, 5);
  eversion_t behind(1, 2);

  // Zones are swapped vs acting. Positions 3 and 4 have no CRUSH remap.
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

  // pos 0: stray OSD 6 (dc0) holds shard 0; nobody holds shard 1.
  EXPECT_EQ(want, (vector<int>{6, CRUSH_ITEM_NONE, 2, 3, 4, 5})) << ss.str();
  EXPECT_TRUE(backfill.empty()) << ss.str();
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
    new_osdmap->deepish_copy_from(*osdmap);

    // Expand to 12 OSDs and bring OSDs 0-11 up with full features.
    {
      OSDMap::Incremental inc(new_osdmap->get_epoch() + 1);
      inc.fsid = new_osdmap->get_fsid();
      new_osdmap->set_max_osd(12);
      for (int i = 0; i < 12; ++i) {
        if (!new_osdmap->is_up(i)) {
          // The in weight below makes a new OSD exist
          inc.new_state[i] = CEPH_OSD_UP;
        }
        inc.new_weight[i]  = CEPH_OSD_IN;
        inc.new_up_thru[i] = 100;
        osd_xinfo_t xinfo;
        xinfo.features = CEPH_FEATUREMASK_SERVER_NAUTILUS |
                         CEPH_FEATUREMASK_SERVER_OCTOPUS  |
                         CEPH_FEATUREMASK_SERVER_QUINCY;
        inc.new_xinfo[i] = xinfo;
      }
      new_osdmap->apply_incremental(inc);
    }

    // Install the 3-zone stretch CRUSH map.
    // dc0: OSDs 0,1,2,9 | dc1: OSDs 3,4,5,10 | dc2: OSDs 6,7,8,11
    {
      CrushWrapper crush;
      crush.create();
      crush.set_type_name(10, "root");
      crush.set_type_name(9,  "datacenter");
      crush.set_type_name(1,  "host");
      crush.set_type_name(0,  "osd");

      int root_id;
      crush.add_bucket(0, CRUSH_BUCKET_STRAW2, CRUSH_HASH_RJENKINS1,
                       10, 0, nullptr, nullptr, &root_id);
      crush.set_item_name(root_id, "default");

      static const int dc_osds[3][4] = {
        {0, 1, 2, 9}, {3, 4, 5, 10}, {6, 7, 8, 11},
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

      int rule_id = 0;
      root_id = crush.get_item_id("default");
      int steps = 6;
      crush_rule *rule = crush_make_rule(steps, pg_pool_t::TYPE_ERASURE);
      int step = 0;
      crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSELEAF_TRIES, 5,   0);
      crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSE_TRIES,     100, 0);
      crush_rule_set_step(rule, step++, CRUSH_RULE_TAKE, root_id, 0);
      crush_rule_set_step(rule, step++, CRUSH_RULE_CHOOSE_INDEP,     3, 9);
      crush_rule_set_step(rule, step++, CRUSH_RULE_CHOOSELEAF_INDEP, 3, 1);
      crush_rule_set_step(rule, step++, CRUSH_RULE_EMIT, 0, 0);
      ASSERT_EQ(step, steps);
      int r = crush_add_rule(crush.get_crush_map(), rule, rule_id);
      ASSERT_GE(r, 0);
      crush.set_rule_name(rule_id, "mirrored_ec_3zone_rule");

      OSDMap::Incremental inc(new_osdmap->get_epoch() + 1);
      inc.fsid = new_osdmap->get_fsid();
      crush.encode(inc.crush, CEPH_FEATURES_SUPPORTED_DEFAULT);
      new_osdmap->apply_incremental(inc);
    }

    pool_id = 1;
    pg_pool_t pool_info;
    pool_info.type          = pg_pool_t::TYPE_ERASURE;
    pool_info.size          = 9;  // 3 zones * 3 shards
    pool_info.min_size      = 7;  // tolerate 2 OSD failures
    pool_info.crush_rule    = 0;
    pool_info.set_pg_num(8);
    pool_info.set_pgp_num(8);
    pool_info.num_zones = 3;

    std::map<std::string, std::string> ec_profile = {
      {"k", "2"}, {"m", "1"}, {"plugin", "jerasure"}, {"technique", "reed_sol_van"}
    };
    new_osdmap->set_erasure_code_profile("default", ec_profile);
    pool_info.erasure_code_profile = "default";
    pool_info.ec_data_shard_count = 2;
    pool_info.ec_coding_shard_count = 1;

    pool_info.peering_crush_bucket_barrier = 9; // datacenter type
    pool_info.peering_crush_bucket_target  = 3; // 3 datacenters
    pool_info.peering_crush_bucket_count   = 3;
    pool_info.peering_crush_mandatory_member = CRUSH_ITEM_NONE;
    pool_info.set_flag(pg_pool_t::FLAG_EC_OVERWRITES);

    OSDMapTestHelpers::add_pool(new_osdmap, pool_id, pool_info, "test_ec_pool_3z");
    update_osdmap_with_peering(new_osdmap);
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
  // The up OSDs only hold their old shards: keep acting, backfill up.
  EXPECT_EQ(want, acting);
  set<pg_shard_t> expected_backfill;
  for (unsigned i = 0; i < up.size(); ++i) {
    expected_backfill.insert(pg_shard_t(up[i], shard_id_t(i)));
  }
  EXPECT_EQ(backfill, expected_backfill);

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
 * Expected: OSDs 0-5 keep serving the shards they hold (their old
 *           positions) and are backfilled for their new ones.
 *           dc2 OSDs (6,7,8) must not appear in want.
 */
TEST_F(TestECActingStretch3Zone, ZoneTransition_ThreeToTwo) {
  pg_pool_t two_zone = *osdmap->get_pg_pool(pool_id);
  two_zone.size = 6;
  two_zone.num_zones = 2;
  two_zone.peering_crush_bucket_target = 2;
  two_zone.peering_crush_bucket_count = 2;
  const pg_pool_t* pool = &two_zone;
  PGPool pgpool(osdmap, pool_id, two_zone, "test_ec_pool_3z");

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
  EXPECT_EQ(want, (vector<int>{0, 1, 2, 3, 4, 5}));
  EXPECT_EQ(backfill, (set<pg_shard_t>{
    pg_shard_t(3, shard_id_t(0)), pg_shard_t(4, shard_id_t(1)),
    pg_shard_t(5, shard_id_t(2)), pg_shard_t(0, shard_id_t(3)),
    pg_shard_t(1, shard_id_t(4)), pg_shard_t(2, shard_id_t(5))}));

  int z0 = osdmap->crush->get_parent_of_type(want[0], 9, pool->crush_rule);
  int z1 = osdmap->crush->get_parent_of_type(want[3], 9, pool->crush_rule);
  EXPECT_NE(z0, z1) << "dc0 and dc1 must differ\n" << ss.str();
}


// Growing a 2-zone pool to 3 zones: the old acting set has 6 entries, the
// new zone's OSDs have empty infos and the shards' logs start at log_tail.
static void calc_two_to_three(const OSDMapRef& osdmap, int64_t pool_id,
                              const vector<int>& up, eversion_t log_tail,
                              vector<int>* want, set<pg_shard_t>* backfill,
                              ostringstream& ss)
{
  const pg_pool_t* pool = osdmap->get_pg_pool(pool_id);
  PGPool pgpool(osdmap, pool_id, *pool, "test_ec_pool_3z");
  vector<int> acting = {0,1,2, 3,4,5};
  map<pg_shard_t, pg_info_t> all_info;
  pg_history_t history;
  history.epoch_created = 1;
  history.same_interval_since = 1;
  for (unsigned i = 0; i < 6; i++) {
    pg_shard_t shard(i, shard_id_t(i));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(i)));
    info.history = history;
    info.last_update = eversion_t(1, 10);
    info.log_tail = log_tail;
    all_info[shard] = info;
  }
  // the new zone's OSDs answer the primary's query with an empty info
  for (unsigned i = 6; i < 9; i++) {
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(i)));
    info.history = history;
    all_info[pg_shard_t(up[i], shard_id_t(i))] = info;
  }
  auto auth_log_shard = all_info.find(pg_shard_t(0, shard_id_t(0)));
  set<pg_shard_t> acting_backfill;
  PeeringState::calc_ec_acting_stretch(
    auth_log_shard, pool->size, acting, up, all_info, false,
    want, backfill, &acting_backfill, osdmap, pgpool, ss);
}

TEST_F(TestECActingStretch3Zone, ZoneTransition_TwoToThree) {
  vector<int> want;
  set<pg_shard_t> backfill;
  ostringstream ss;
  const vector<int> up = {0,1,2, 3,4,5, 6,7,8};

  // the log goes back far enough to recover the new zone from it
  calc_two_to_three(osdmap, pool_id, up, eversion_t(), &want, &backfill, ss);
  EXPECT_EQ(want, up) << ss.str();
  EXPECT_TRUE(backfill.empty()) << backfill << "\n" << ss.str();

  // with a trimmed log the new zone is backfilled, and serves nothing yet
  want.clear();
  backfill.clear();
  calc_two_to_three(osdmap, pool_id, up, eversion_t(1, 5), &want, &backfill, ss);
  EXPECT_EQ(want, (vector<int>{0,1,2, 3,4,5, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE,
                               CRUSH_ITEM_NONE})) << ss.str();
  EXPECT_EQ(backfill, (set<pg_shard_t>{
    pg_shard_t(6, shard_id_t(6)), pg_shard_t(7, shard_id_t(7)),
    pg_shard_t(8, shard_id_t(8))})) << ss.str();
}

// CRUSH can put the new zone first; want must still keep each zone block in
// one datacenter, never use an OSD twice and keep the OSDs that hold data.
TEST_F(TestECActingStretch3Zone, ZoneTransition_TwoToThree_NewZoneFirst) {
  vector<int> want;
  set<pg_shard_t> backfill;
  ostringstream ss;
  calc_two_to_three(osdmap, pool_id, {6,7,8, 0,1,2, 3,4,5}, eversion_t(1, 5),
                    &want, &backfill, ss);
  ASSERT_EQ(9u, want.size()) << ss.str();
  set<int> seen;
  for (int osd : want) {
    if (osd != CRUSH_ITEM_NONE) {
      EXPECT_TRUE(seen.insert(osd).second) << "osd." << osd << " twice\n" << ss.str();
    }
  }
  for (int zone = 0; zone < 3; ++zone) {
    set<int> dcs;
    for (int i = zone * 3; i < zone * 3 + 3; ++i) {
      if (want[i] != CRUSH_ITEM_NONE) {
        dcs.insert(osdmap->crush->get_parent_of_type(want[i], 9, 0));
      }
    }
    EXPECT_LE(dcs.size(), 1u) << "zone block " << zone << "\n" << ss.str();
  }
  for (int osd = 0; osd < 6; ++osd) {
    EXPECT_TRUE(seen.contains(osd)) << "osd." << osd << " holds data\n" << ss.str();
  }
}

/*
 * The two tests below cover a zone whose whole block of up positions is
 * CRUSH_ITEM_NONE, so its zone cannot be taken from up.
 */

/**
 * Test: a stray from the wrong zone must not fill a position in a zone whose
 * up entries are all missing.
 *
 * Pool is 2 zones x 3 shards, dc0 = OSDs 0,1,2,6 and dc1 = OSDs 3,4,5,7, so
 * positions 0-2 belong to dc0 and positions 3-5 to dc1.  Take the whole of
 * dc1 out of the up set and offer OSD 6 - which lives in dc0 - as a stray
 * holding shard 3.  Shard 3 belongs to zone 1, so placing it on a dc0 OSD
 * would put two copies of relative shard 0 in one datacenter.
 *
 * dc0 already serves zone block 0, so zone block 1 cannot be served from it
 * and position 3 stays empty.
 */
TEST_F(TestECActingStretch, StrayFromWrongZoneRejectedWhenZoneBlockDown) {
  const pg_pool_t* pool = osdmap->get_pg_pool(pool_id);
  ASSERT_NE(pool, nullptr);
  PGPool pgpool(osdmap, pool_id, *pool, "test_ec_pool");

  // All of dc1 is gone from the up set.
  vector<int> up = {0, 1, 2, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE};
  vector<int> acting = up;

  map<pg_shard_t, pg_info_t> all_info;
  pg_history_t history;
  history.epoch_created = 1;
  history.same_interval_since = 1;

  auto add_info = [&](int osd, int shard) {
    pg_shard_t s(osd, shard_id_t(shard));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(shard)));
    info.history = history;
    info.last_update = eversion_t(1, shard);
    all_info[s] = info;
  };

  // The surviving dc0 shards, plus a stray copy of shard 3 that also sits in
  // dc0 - the only candidate offered for position 3.
  add_info(0, 0);
  add_info(1, 1);
  add_info(2, 2);
  add_info(6, 3);

  auto auth_log_shard = all_info.find(pg_shard_t(0, shard_id_t(0)));
  ASSERT_NE(auth_log_shard, all_info.end());

  vector<int> want;
  set<pg_shard_t> backfill;
  set<pg_shard_t> acting_backfill;
  ostringstream ss;

  PeeringState::calc_ec_acting_stretch(
    auth_log_shard, pool->size, acting, up, all_info,
    false /* restrict_to_up_acting */,
    &want, &backfill, &acting_backfill, osdmap, pgpool, ss);

  ASSERT_EQ(want.size(), 6u);

  const int dc_of_position_0 =
    osdmap->crush->get_parent_of_type(want[0], 9, pool->crush_rule);

  // Position 3 carries a zone 1 shard, so it must not be served from the same
  // datacenter as position 0, and must not be served by OSD 6 at all.
  if (want[3] != CRUSH_ITEM_NONE) {
    const int dc_of_position_3 =
      osdmap->crush->get_parent_of_type(want[3], 9, pool->crush_rule);
    EXPECT_NE(dc_of_position_3, dc_of_position_0)
      << "position 3 holds a zone 1 shard but was filled from the same "
      << "datacenter as position 0 (osd." << want[3] << ")\n" << ss.str();
  }
  EXPECT_NE(want[3], 6)
    << "osd.6 lives in dc0 and cannot hold a zone 1 shard\n" << ss.str();
}

/**
 * Test: no OSD may appear twice in the want vector.
 *
 * Positions 3, 4 and 5 have no up entry.  OSD 0 holds relative shard 0, but
 * only as absolute shard 0, so it must not also be placed at position 3.
 */
TEST_F(TestECActingStretch, NoOsdAppearsTwiceInWant) {
  const pg_pool_t* pool = osdmap->get_pg_pool(pool_id);
  ASSERT_NE(pool, nullptr);
  PGPool pgpool(osdmap, pool_id, *pool, "test_ec_pool");

  // Two dc0 shards up, everything else missing.  Position 2 is deliberately
  // left without any candidate so dc0 stays below bucket_max.
  vector<int> up = {0, 1, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE,
                    CRUSH_ITEM_NONE};
  vector<int> acting = up;

  map<pg_shard_t, pg_info_t> all_info;
  pg_history_t history;
  history.epoch_created = 1;
  history.same_interval_since = 1;

  for (int shard : {0, 1}) {
    pg_shard_t s(shard, shard_id_t(shard));
    pg_info_t info(spg_t(pg_t(1, pool_id), shard_id_t(shard)));
    info.history = history;
    info.last_update = eversion_t(1, shard);
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
    false /* restrict_to_up_acting */,
    &want, &backfill, &acting_backfill, osdmap, pgpool, ss);

  ASSERT_EQ(want.size(), 6u);

  std::set<int> seen;
  for (unsigned i = 0; i < want.size(); ++i) {
    if (want[i] == CRUSH_ITEM_NONE) {
      continue;
    }
    EXPECT_TRUE(seen.insert(want[i]).second)
      << "osd." << want[i] << " appears more than once in want, at position "
      << i << "\n" << ss.str();
  }
}

// restrict_to_up_acting keeps a current same-zone stray out of want.
TEST_F(TestECActingStretch, RestrictToUpActing_NoStrays) {
  const int N = CRUSH_ITEM_NONE;
  vector<int> up = {0, N, 2, 3, 4, 5};
  vector<int> acting = up;
  map<pg_shard_t, pg_info_t> all_info;
  for (int i : {0, 2, 3, 4, 5}) {
    add_info(all_info, i, i, eversion_t(1, 10));
  }
  add_info(all_info, 6, 1, eversion_t(1, 10));

  vector<int> want;
  set<pg_shard_t> backfill, acting_backfill;
  ostringstream ss;
  calc(up, acting, all_info, pg_shard_t(0, shard_id_t(0)), true,
       &want, &backfill, &acting_backfill, ss);
  EXPECT_EQ(want, (vector<int>{0, N, 2, 3, 4, 5})) << ss.str();
  EXPECT_TRUE(backfill.empty()) << ss.str();
  EXPECT_FALSE(acting_backfill.count(pg_shard_t(6, shard_id_t(1)))) << ss.str();

  want.clear();
  backfill.clear();
  acting_backfill.clear();
  ostringstream ss2;
  calc(up, acting, all_info, pg_shard_t(0, shard_id_t(0)), false,
       &want, &backfill, &acting_backfill, ss2);
  EXPECT_EQ(want, (vector<int>{0, 6, 2, 3, 4, 5})) << ss2.str();
}

// restrict_to_up_acting still falls back to acting[i] when up[i] is behind.
TEST_F(TestECActingStretch, RestrictToUpActing_ActingFallbackStillUsed) {
  vector<int> up = {0, 6, 2, 3, 4, 5};
  vector<int> acting = {0, 1, 2, 3, 4, 5};
  map<pg_shard_t, pg_info_t> all_info;
  for (int i = 0; i < 6; ++i) {
    add_info(all_info, i, i, eversion_t(1, 10), eversion_t(1, 5));
  }
  add_info(all_info, 6, 1, eversion_t(1, 2));

  vector<int> want;
  set<pg_shard_t> backfill, acting_backfill;
  ostringstream ss;
  calc(up, acting, all_info, pg_shard_t(0, shard_id_t(0)), true,
       &want, &backfill, &acting_backfill, ss);
  EXPECT_EQ(want, (vector<int>{0, 1, 2, 3, 4, 5})) << ss.str();
  EXPECT_EQ(backfill, (set<pg_shard_t>{pg_shard_t(6, shard_id_t(1))})) << ss.str();
  EXPECT_TRUE(acting_backfill.count(pg_shard_t(6, shard_id_t(1)))) << ss.str();
}

// Auth log in zone 1 with all of zone 0 behind its tail: zone 0 is refilled
// only from a same-zone stray holding the same absolute shard.
TEST_F(TestECActingStretch, Zone1Auth_Zone0BehindLogTail) {
  const int N = CRUSH_ITEM_NONE;
  vector<int> up = {0, 1, 2, 3, 4, 5};
  vector<int> acting = up;
  map<pg_shard_t, pg_info_t> all_info;
  for (int i : {0, 1, 2}) {
    add_info(all_info, i, i, eversion_t(1, 5));
  }
  for (int i : {3, 4, 5}) {
    add_info(all_info, i, i, eversion_t(1, 12), eversion_t(1, 10));
  }
  add_info(all_info, 6, 0, eversion_t(1, 12));
  add_info(all_info, 7, 3, eversion_t(1, 12));

  vector<int> want;
  set<pg_shard_t> backfill, acting_backfill;
  ostringstream ss;
  calc(up, acting, all_info, pg_shard_t(4, shard_id_t(4)), false,
       &want, &backfill, &acting_backfill, ss);
  EXPECT_EQ(want, (vector<int>{6, N, N, 3, 4, 5})) << ss.str();
  EXPECT_EQ(backfill, (set<pg_shard_t>{pg_shard_t(0, shard_id_t(0)),
                                       pg_shard_t(1, shard_id_t(1)),
                                       pg_shard_t(2, shard_id_t(2))}))
    << ss.str();
  set<pg_shard_t> expected_ab = backfill;
  for (unsigned i = 0; i < want.size(); ++i) {
    if (want[i] != N) {
      expected_ab.insert(pg_shard_t(want[i], shard_id_t(i)));
    }
  }
  EXPECT_EQ(acting_backfill, expected_ab) << ss.str();
}

// Zone blocks swapped in up with a trimmed log: the up OSDs only have the
// empty info returned for a shard they do not hold.  choose_acting asserts
// that want == up implies no backfill.
TEST_F(TestECActingStretch, ZoneBlockSwap_WantEqualsUpImpliesNoBackfill) {
  vector<int> up = {3, 4, 5, 0, 1, 2};
  vector<int> acting = {0, 1, 2, 3, 4, 5};
  map<pg_shard_t, pg_info_t> all_info;
  for (int i = 0; i < 6; ++i) {
    add_info(all_info, i, i, eversion_t(1, 10), eversion_t(1, 5));
    add_info(all_info, up[i], i, eversion_t());
  }

  vector<int> want;
  set<pg_shard_t> backfill, acting_backfill;
  ostringstream ss;
  calc(up, acting, all_info, pg_shard_t(0, shard_id_t(0)), false,
       &want, &backfill, &acting_backfill, ss);
  EXPECT_FALSE(want == up && !backfill.empty())
    << "want " << want << " == up with backfill " << backfill << "\n"
    << ss.str();
}

// All of dc1 marked out but still up: CRUSH leaves the zone 1 block of up
// empty, yet acting[3..5] are up with current data and should be kept.
TEST_F(TestECActingStretch, UpZoneBlockAllNone_CurrentActingZoneKept) {
  const int N = CRUSH_ITEM_NONE;
  vector<int> up = {0, 1, 2, N, N, N};
  vector<int> acting = {0, 1, 2, 3, 4, 5};
  map<pg_shard_t, pg_info_t> all_info;
  for (int i = 0; i < 6; ++i) {
    add_info(all_info, i, i, eversion_t(1, 10), eversion_t(1, 5));
  }

  vector<int> want;
  set<pg_shard_t> backfill, acting_backfill;
  ostringstream ss;
  calc(up, acting, all_info, pg_shard_t(0, shard_id_t(0)), false,
       &want, &backfill, &acting_backfill, ss);
  EXPECT_EQ(want, (vector<int>{0, 1, 2, 3, 4, 5})) << ss.str();
}

// An OSD picked for position i on the strength of its info for another
// absolute shard j (same relative shard) holds no data for shard i, so it
// must either have a usable info for shard i or be backfilled.
TEST_F(TestECActingStretch, OtherAbsoluteShardPick_IsBackfilled) {
  const int N = CRUSH_ITEM_NONE;
  auto check = [&](const vector<int> &up, const vector<int> &acting,
                   const map<pg_shard_t, pg_info_t> &all_info,
                   bool restrict_to_up_acting) {
    vector<int> want;
    set<pg_shard_t> backfill, acting_backfill;
    ostringstream ss;
    calc(up, acting, all_info, pg_shard_t(0, shard_id_t(0)),
         restrict_to_up_acting, &want, &backfill, &acting_backfill, ss);
    const eversion_t log_tail =
      all_info.at(pg_shard_t(0, shard_id_t(0))).log_tail;
    for (unsigned i = 0; i < want.size(); ++i) {
      if (want[i] == N) {
        continue;
      }
      pg_shard_t s(want[i], shard_id_t(i));
      auto it = all_info.find(s);
      bool usable = it != all_info.end() && !it->second.is_incomplete() &&
                    it->second.last_update >= log_tail;
      EXPECT_TRUE(usable || backfill.count(s))
        << "want[" << i << "]=osd." << want[i] << " has no info for shard "
        << i << " and is not backfilled\n" << ss.str();
    }
  };

  // Stray path: osd.7 (dc1) has current relative shard 1 as absolute shard 1.
  {
    vector<int> up = {0, 1, 2, 3, N, 5};
    map<pg_shard_t, pg_info_t> all_info;
    for (int i : {0, 1, 2, 3, 5}) {
      add_info(all_info, i, i, eversion_t(1, 10), eversion_t(1, 5));
    }
    add_info(all_info, 7, 1, eversion_t(1, 10));
    check(up, up, all_info, false);
  }

  // Acting path: osd.7 (dc1) sits at acting[1] and is taken for position 4.
  {
    vector<int> up = {0, 1, 2, 3, N, 5};
    vector<int> acting = {0, 7, 2, 3, N, 5};
    map<pg_shard_t, pg_info_t> all_info;
    for (int i : {0, 1, 2, 3, 5}) {
      add_info(all_info, i, i, eversion_t(1, 10), eversion_t(1, 5));
    }
    add_info(all_info, 7, 1, eversion_t(1, 10));
    check(up, acting, all_info, true);
  }
}

// Strays picked for zone 0 must count toward dc0's bucket_max, so a
// cross-zone up entry (legal pg-upmap-items 3->6) cannot push dc0 past it.
TEST_F(TestECActingStretch, StrayPick_CountsTowardZoneBucketMax) {
  const int N = CRUSH_ITEM_NONE;
  vector<int> up = {0, N, N, 6, N, N};
  vector<int> acting = up;
  map<pg_shard_t, pg_info_t> all_info;
  add_info(all_info, 0, 0, eversion_t(1, 10));
  add_info(all_info, 1, 1, eversion_t(1, 10));
  add_info(all_info, 2, 2, eversion_t(1, 10));
  add_info(all_info, 6, 3, eversion_t(1, 10));

  vector<int> want;
  set<pg_shard_t> backfill, acting_backfill;
  ostringstream ss;
  calc(up, acting, all_info, pg_shard_t(0, shard_id_t(0)), false,
       &want, &backfill, &acting_backfill, ss);
  const unsigned bucket_max = 3;
  map<int, unsigned> per_dc;
  for (int osd : want) {
    if (osd != N) {
      ++per_dc[dc_of(osd)];
    }
  }
  for (auto &[dc, count] : per_dc) {
    EXPECT_LE(count, bucket_max)
      << "dc " << dc << " supplies " << count << " OSDs, want " << want
      << "\n" << ss.str();
  }
}

// A single cross-zone up entry (pg-upmap-items 3->6) must not cost the
// rest of the healthy zone 1 block.  osd.6 is not backfilled: block 1 stays
// in dc1, so once complete it would be skipped and Recovered's
// choose_acting would assert that the backfill targets are unchanged.
TEST_F(TestECActingStretch, SingleCrossZoneUpEntry_KeepsRestOfZoneBlock) {
  vector<int> up = {0, 1, 2, 6, 4, 5};
  vector<int> acting = {0, 1, 2, 3, 4, 5};
  map<pg_shard_t, pg_info_t> all_info;
  for (int i = 0; i < 6; ++i) {
    add_info(all_info, i, i, eversion_t(1, 10), eversion_t(1, 5));
  }
  add_info(all_info, 6, 3, eversion_t());

  vector<int> want;
  set<pg_shard_t> backfill, acting_backfill;
  ostringstream ss;
  calc(up, acting, all_info, pg_shard_t(0, shard_id_t(0)), false,
       &want, &backfill, &acting_backfill, ss);
  EXPECT_EQ(want[3], 3) << ss.str();
  EXPECT_EQ(want[4], 4) << ss.str();
  EXPECT_EQ(want[5], 5) << ss.str();
  EXPECT_TRUE(backfill.empty()) << ss.str();
}

// up has moved zone block 0 to dc1, which holds it, but block 1 has not been
// backfilled into dc0.  Only the acting layout serves every position.  The
// dc1 holders of block 0 are backfilled with the rest of up: otherwise they
// get no writes, and if the log is trimmed past osd.3 before Recovered,
// block 0 stays in dc0 and choose_acting asserts that the backfill targets
// are unchanged.
TEST_F(TestECActingStretch, PartialZoneBlockSwap_KeepsCompleteActing) {
  vector<int> up = {3, 4, 5, 0, 1, 2};
  vector<int> acting = {0, 1, 2, 3, 4, 5};
  map<pg_shard_t, pg_info_t> all_info;
  for (int i = 0; i < 6; ++i) {
    add_info(all_info, i, i, eversion_t(1, 10), eversion_t(1, 5));
  }
  for (int i = 0; i < 3; ++i) {
    add_info(all_info, up[i], i, eversion_t(1, 10), eversion_t(1, 5));
  }

  vector<int> want;
  set<pg_shard_t> backfill, acting_backfill;
  ostringstream ss;
  calc(up, acting, all_info, pg_shard_t(0, shard_id_t(0)), false,
       &want, &backfill, &acting_backfill, ss);
  EXPECT_EQ(want, acting) << ss.str();
  EXPECT_EQ(backfill, (set<pg_shard_t>{
    pg_shard_t(3, shard_id_t(0)), pg_shard_t(4, shard_id_t(1)),
    pg_shard_t(5, shard_id_t(2)), pg_shard_t(0, shard_id_t(3)),
    pg_shard_t(1, shard_id_t(4)), pg_shard_t(2, shard_id_t(5))})) << ss.str();

  add_info(all_info, 3, 0, eversion_t(1, 4));
  for (const auto &t : backfill) {
    add_info(all_info, t.osd, t.shard.id, eversion_t(1, 10), eversion_t(1, 5));
  }
  want.clear();
  backfill.clear();
  acting_backfill.clear();
  ostringstream ss2;
  calc(up, acting, all_info, pg_shard_t(0, shard_id_t(0)), true,
       &want, &backfill, &acting_backfill, ss2);
  EXPECT_EQ(want, up) << ss2.str();
  EXPECT_TRUE(backfill.empty()) << ss2.str();
}

// Both zones hold every shard and up is empty: keep serving from acting
// rather than swapping the zone blocks onto strays.
TEST_F(TestECActingStretch, EqualHoldersNoUp_PrefersActing) {
  const int N = CRUSH_ITEM_NONE;
  vector<int> up(6, N);
  vector<int> acting = {0, 1, 2, 3, 4, 5};
  vector<int> swapped = {3, 4, 5, 0, 1, 2};
  map<pg_shard_t, pg_info_t> all_info;
  for (int i = 0; i < 6; ++i) {
    add_info(all_info, acting[i], i, eversion_t(1, 10), eversion_t(1, 5));
    add_info(all_info, swapped[i], i, eversion_t(1, 10), eversion_t(1, 5));
  }

  vector<int> want;
  set<pg_shard_t> backfill, acting_backfill;
  ostringstream ss;
  calc(up, acting, all_info, pg_shard_t(0, shard_id_t(0)), false,
       &want, &backfill, &acting_backfill, ss);
  EXPECT_EQ(want, acting) << ss.str();
}

// No zone holds k shards of either block.  acting keeps one shard of each
// block, which is recoverable; serving block 0 from up[0]'s zone would
// leave only one shard.
TEST_F(TestECActingStretch, NoZoneWithKShards_KeepsMostShards) {
  const int N = CRUSH_ITEM_NONE;
  vector<int> up = {3, N, N, N, N, N};
  vector<int> acting = {0, N, N, N, 4, N};
  map<pg_shard_t, pg_info_t> all_info;
  add_info(all_info, 0, 0, eversion_t(1, 10), eversion_t(1, 5));
  add_info(all_info, 4, 4, eversion_t(1, 10), eversion_t(1, 5));
  add_info(all_info, 3, 0, eversion_t(1, 10), eversion_t(1, 5));

  vector<int> want;
  set<pg_shard_t> backfill, acting_backfill;
  ostringstream ss;
  calc(up, acting, all_info, pg_shard_t(0, shard_id_t(0)), false,
       &want, &backfill, &acting_backfill, ss);
  EXPECT_EQ(want, acting) << ss.str();
}

// No zone holds k shards of either block, and each zone holds one shard of
// each.  acting keeps relative shards 2 and 0, which is recoverable; up
// points at relative shard 1 in both zones, which is not.
TEST_F(TestECActingStretch, NoZoneWithKShards_UpDoesNotCostRecoverability) {
  const int N = CRUSH_ITEM_NONE;
  vector<int> up = {N, 3, N, N, 0, N};
  vector<int> acting = {N, N, 0, 3, N, N};
  map<pg_shard_t, pg_info_t> all_info;
  add_info(all_info, 0, 2, eversion_t(1, 10), eversion_t(1, 5));
  add_info(all_info, 0, 4, eversion_t(1, 10), eversion_t(1, 5));
  add_info(all_info, 3, 1, eversion_t(1, 10), eversion_t(1, 5));
  add_info(all_info, 3, 3, eversion_t(1, 10), eversion_t(1, 5));

  vector<int> want;
  set<pg_shard_t> backfill, acting_backfill;
  ostringstream ss;
  calc(up, acting, all_info, pg_shard_t(0, shard_id_t(2)), false,
       &want, &backfill, &acting_backfill, ss);
  EXPECT_EQ(want, acting) << ss.str();
}

// A single-zone stretch EC pool whose rule spreads the shards over both
// datacenters (bucket_max 3) keeps every up OSD.
TEST_F(TestECActingStretch, SingleZoneStretchPool_KeepsUpAcrossDatacenters) {
  pg_pool_t single_zone = *osdmap->get_pg_pool(pool_id);
  single_zone.num_zones = 1;
  single_zone.peering_crush_bucket_count = 1;
  vector<int> up = {0, 1, 2, 3, 4, 5};
  map<pg_shard_t, pg_info_t> all_info;
  for (int i = 0; i < 6; ++i) {
    add_info(all_info, i, i, eversion_t(1, 10), eversion_t(1, 5));
  }

  vector<int> want;
  set<pg_shard_t> backfill, acting_backfill;
  ostringstream ss;
  calc(up, up, all_info, pg_shard_t(0, shard_id_t(0)), false,
       &want, &backfill, &acting_backfill, ss, &single_zone);
  EXPECT_EQ(want, up) << ss.str();
  EXPECT_TRUE(backfill.empty()) << ss.str();
}

// up moves zone block 1 onto the dc0 OSDs serving block 0, and acting keeps
// block 0 there with a lone shard 3 in dc1.  Once the up OSDs are
// backfilled, Recovered's choose_acting must move to up: with want ==
// acting it asserts that the backfill targets are unchanged.
TEST_F(TestECActingStretch, BackfilledUpZoneBlock_JoinsWant) {
  const int N = CRUSH_ITEM_NONE;
  vector<int> up = {N, N, N, 1, 0, 2};
  vector<int> acting = {0, 1, 2, 3, N, N};
  map<pg_shard_t, pg_info_t> all_info;
  for (int i = 0; i < 4; ++i) {
    add_info(all_info, acting[i], i, eversion_t(1, 10), eversion_t(1, 5));
  }
  for (int i = 3; i < 6; ++i) {
    add_info(all_info, up[i], i, eversion_t());
  }

  vector<int> want;
  set<pg_shard_t> backfill, acting_backfill;
  ostringstream ss;
  calc(up, acting, all_info, pg_shard_t(0, shard_id_t(0)), false,
       &want, &backfill, &acting_backfill, ss);
  EXPECT_EQ(want, acting) << ss.str();
  const set<pg_shard_t> targets = {pg_shard_t(1, shard_id_t(3)),
                                   pg_shard_t(0, shard_id_t(4)),
                                   pg_shard_t(2, shard_id_t(5))};
  ASSERT_EQ(backfill, targets) << ss.str();

  for (const auto &t : targets) {
    add_info(all_info, t.osd, t.shard.id, eversion_t(1, 10), eversion_t(1, 5));
  }
  want.clear();
  backfill.clear();
  acting_backfill.clear();
  ostringstream ss2;
  calc(up, acting, all_info, pg_shard_t(0, shard_id_t(0)), true,
       &want, &backfill, &acting_backfill, ss2);
  EXPECT_EQ(want, up) << ss2.str();
  EXPECT_TRUE(backfill.empty()) << ss2.str();
}
