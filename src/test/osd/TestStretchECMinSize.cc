// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library Public License for more details.
 *
 */

/**
 * Unit tests for two per-zone min_size helpers on OSDMap:
 *
 *   at_least_one_zone_has_min_size(pool, acting)
 *     Returns true if ANY zone in the CRUSH topology has >= pool.min_size
 *     acting OSDs.
 *
 *   stretch_ec_num_acting_below_min_size(pool, acting)
 *     Returns the total per-zone deficit: sum over all zones of
 *     max(0, min_size - zone_acting_count).  Returns 0 for non-stretch or
 *     non-erasure pools.
 *
 * Topology used by all tests
 * ─────────────────────────
 *   root "default"
 *     datacenter "dc0"  (type 9): OSDs 0, 1, 2, 6
 *     datacenter "dc1"  (type 9): OSDs 3, 4, 5, 7
 *
 *   pool: erasure, size=8, min_size=2
 */

#include <gtest/gtest.h>
#include "test/osd/OSDMapTestHelpers.h"
#include "osd/OSDMap.h"
#include "osd/osd_types.h"
#include "crush/CrushWrapper.h"
#include "crush/crush.h"

using namespace std;

static std::shared_ptr<OSDMap> make_stretch_ec_osdmap()
{
  auto osdmap = std::make_shared<OSDMap>();
  osdmap->set_max_osd(8);
  for (int i = 0; i < 8; ++i) {    
    osdmap->set_state(i, CEPH_OSD_EXISTS | CEPH_OSD_UP);
  }
  osdmap->set_epoch(1);

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

    // Insert OSDs with location hierarchy
    // dc0: OSDs 0,1,2,6 in hosts host0, host1, host2, host6
    // dc1: OSDs 3,4,5,7 in hosts host3, host4, host5, host7
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

  // CRUSH rule: choose 2 datacenters, chooseleaf indep 3 hosts
  int rule_id = 0;
  int steps = 6;
  crush_rule *rule = crush_make_rule(steps, pg_pool_t::TYPE_ERASURE);
  int step = 0;
  crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSELEAF_TRIES, 5, 0);
  crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSE_TRIES, 100, 0);
  crush_rule_set_step(rule, step++, CRUSH_RULE_TAKE, root_id, 0);
  crush_rule_set_step(rule, step++, CRUSH_RULE_CHOOSE_INDEP, 2, 9 /* datacenter */);
  crush_rule_set_step(rule, step++, CRUSH_RULE_CHOOSELEAF_INDEP, 3, 1 /* host */);
  crush_rule_set_step(rule, step++, CRUSH_RULE_EMIT, 0, 0);
  ceph_assert(step == steps);
  int r = crush_add_rule(crush.get_crush_map(), rule, rule_id);
  ceph_assert(r >= 0);
  crush.set_rule_name(rule_id, "stretch_ec_rule");

  OSDMap::Incremental inc(2);
  inc.fsid = osdmap->get_fsid();
  crush.encode(inc.crush, CEPH_FEATURES_SUPPORTED_DEFAULT);
  osdmap->apply_incremental(inc);

  int64_t pool_id = 1;
  pg_pool_t pool;
  pool.type     = pg_pool_t::TYPE_ERASURE;
  pool.size     = 6;
  pool.min_size = 2;
  pool.crush_rule = rule_id;
  pool.set_pg_num(8);
  pool.set_pgp_num(8);
  pool.set_flag(pg_pool_t::FLAG_EC_OVERWRITES);
  pool.peering_crush_bucket_barrier = 9; // datacenter
  pool.peering_crush_bucket_target  = 2;
  pool.peering_crush_bucket_count   = 2;
  pool.peering_crush_mandatory_member = CRUSH_ITEM_NONE;

  OSDMapTestHelpers::add_pool(osdmap, pool_id, pool, "test_ec_pool");
  return osdmap;
}

class StretchECMinSizeTest : public ::testing::Test {
protected:
  std::shared_ptr<OSDMap> osdmap;
  const pg_pool_t *pool = nullptr;

  void SetUp() override {
    osdmap = make_stretch_ec_osdmap();
    pool = osdmap->get_pg_pool(1);
    ASSERT_NE(pool, nullptr);
  }
};

  // ===========================================================================
  // at_least_one_zone_has_min_size
  // ===========================================================================

// Both zones fully populated - both exceed min_size
TEST_F(StretchECMinSizeTest, ZoneHasMinSize_BothZonesHealthy)
{
  vector<int> acting = {0, 1, 2, 3, 4, 5}; // dc0: 3 OSDs, dc1: 3 OSDs
  EXPECT_TRUE(osdmap->at_least_one_zone_has_min_size(*pool, acting));
}

// dc0 full, dc1 completely absent - at least one zone qualifies
TEST_F(StretchECMinSizeTest, ZoneHasMinSize_OnlyOneDCPresent)
{
  vector<int> acting = {0, 1, 2,
                        CRUSH_ITEM_NONE, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE};
  EXPECT_TRUE(osdmap->at_least_one_zone_has_min_size(*pool, acting));
}

// dc0 exactly at min_size (2), dc1 has only 1 - dc0 still qualifies
TEST_F(StretchECMinSizeTest, ZoneHasMinSize_OneZoneExactlyAtMinSize)
{
  vector<int> acting = {0, 1, CRUSH_ITEM_NONE,   // dc0: 2 = min_size
                        3, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE}; // dc1: 1 < min_size
  EXPECT_TRUE(osdmap->at_least_one_zone_has_min_size(*pool, acting));
}

// Both zones have only 1 OSD each (< min_size=2)
TEST_F(StretchECMinSizeTest, ZoneHasMinSize_BothZonesBelowMinSize)
{
  vector<int> acting = {0, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE,
                        3, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE};
  EXPECT_FALSE(osdmap->at_least_one_zone_has_min_size(*pool, acting));
}

// Completely empty acting set - no zone qualifies
TEST_F(StretchECMinSizeTest, ZoneHasMinSize_EmptyActingSet)
{
  vector<int> acting(6, CRUSH_ITEM_NONE);
  EXPECT_FALSE(osdmap->at_least_one_zone_has_min_size(*pool, acting));
}

// ===========================================================================
// stretch_ec_num_acting_below_min_size
// ===========================================================================

// All zones fully populated
TEST_F(StretchECMinSizeTest, NumActingBelowMinSize_AllZonesHealthy)
{
  vector<int> acting = {0, 1, 2, 3, 4, 5};
  EXPECT_EQ(0u, osdmap->stretch_ec_num_acting_below_min_size(*pool, acting));
}

// One zone exactly at min_size
TEST_F(StretchECMinSizeTest, NumActingBelowMinSize_OneZoneAtMinSize)
{
  vector<int> acting = {0, 1, CRUSH_ITEM_NONE,   // dc0: 2 = min_size
                        3, 4, 5};                 // dc1: 3
  EXPECT_EQ(0u, osdmap->stretch_ec_num_acting_below_min_size(*pool, acting));
}

// dc0 has 1 OSD (< min_size=2) - deficit 1; dc1 OK
TEST_F(StretchECMinSizeTest, NumActingBelowMinSize_OneZoneBelowMinSize)
{
  vector<int> acting = {0, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE,
                        3, 4, 5};
  EXPECT_EQ(1u, osdmap->stretch_ec_num_acting_below_min_size(*pool, acting));
}

// Both zones have 1 OSD each - deficit 1+1 = 2
TEST_F(StretchECMinSizeTest, NumActingBelowMinSize_BothZonesBelowMinSize)
{
  vector<int> acting = {0, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE,
                        3, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE};
  EXPECT_EQ(2u, osdmap->stretch_ec_num_acting_below_min_size(*pool, acting));
}

// dc1 completely empty - deficit = min_size = 2
TEST_F(StretchECMinSizeTest, NumActingBelowMinSize_OneZoneCompletelyEmpty)
{
  vector<int> acting = {0, 1, 2,
                        CRUSH_ITEM_NONE, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE};
  EXPECT_EQ(2u, osdmap->stretch_ec_num_acting_below_min_size(*pool, acting));
}

// Both zones empty - deficit = 2 * min_size = 4
TEST_F(StretchECMinSizeTest, NumActingBelowMinSize_BothZonesEmpty)
{
  vector<int> acting(6, CRUSH_ITEM_NONE);
  EXPECT_EQ(4u, osdmap->stretch_ec_num_acting_below_min_size(*pool, acting));
}

// Non-stretch pool - always 0
TEST_F(StretchECMinSizeTest, NumActingBelowMinSize_NonStretchPool)
{
  pg_pool_t non_stretch = *pool;
  non_stretch.peering_crush_bucket_count = 0;
  vector<int> acting = {0, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE,
                        3, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE};
  EXPECT_EQ(0u, osdmap->stretch_ec_num_acting_below_min_size(non_stretch, acting));
}

// Non-erasure pool - always 0
TEST_F(StretchECMinSizeTest, NumActingBelowMinSize_ReplicatedPool)
{
  pg_pool_t rep = *pool;
  rep.type = pg_pool_t::TYPE_REPLICATED;
  vector<int> acting = {0, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE,
                        3, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE};
  EXPECT_EQ(0u, osdmap->stretch_ec_num_acting_below_min_size(rep, acting));
}
