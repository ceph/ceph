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
 * Unit tests for the per-zone min_size helper on OSDMap, and the past
 * intervals that use it:
 *
 *   stretch_num_acting_below_min_size(pool, acting)
 *     Returns the total per-zone deficit: sum over all zones of
 *     max(0, min_size - zone_acting_count), except for 3AZ pools where
 *     min_size applies to the whole cluster.  Returns 0 for non-stretch
 *
 * Topology used by all tests
 * ─────────────────────────
 *   root "default"
 *     datacenter "dc0"  (type 9): OSDs 0, 1, 2, 6
 *     datacenter "dc1"  (type 9): OSDs 3, 4, 5, 7
 *
 *   pool: erasure, two zones, size=6, min_size=2 per zone
 */

#include <gtest/gtest.h>
#include "test/osd/OSDMapTestHelpers.h"
#include "osd/OSDMap.h"
#include "osd/osd_types.h"
#include "crush/CrushWrapper.h"
#include "crush/crush.h"
#include <functional>

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
  pool.num_zones = 2;
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

  void modify_crush(const std::function<void(CrushWrapper&)> &f) {
    CrushWrapper crush;
    bufferlist bl;
    osdmap->crush->encode(bl, CEPH_FEATURES_SUPPORTED_DEFAULT);
    auto p = bl.cbegin();
    crush.decode(p);
    f(crush);
    OSDMap::Incremental inc(osdmap->get_epoch() + 1);
    inc.fsid = osdmap->get_fsid();
    crush.encode(inc.crush, CEPH_FEATURES_SUPPORTED_DEFAULT);
    osdmap->apply_incremental(inc);
    pool = osdmap->get_pg_pool(1);
  }

  // Gives every OSD device class hdd and returns a stretch rule taking the
  // shadow root default~hdd, as a profile with crush-device-class does.
  int add_hdd_stretch_rule() {
    int rule_id = -1;
    modify_crush([&](CrushWrapper &crush) {
      for (int osd = 0; osd < 8; ++osd) {
        std::ostringstream ss;
        int r = crush.update_device_class(osd, "hdd", "osd." + std::to_string(osd), &ss);
        ceph_assert(r >= 0);
      }
      std::ostringstream ss;
      rule_id = crush.add_simple_stretch_rule(
        "hdd_stretch_rule", "default", "datacenter", "host", 2, 3, "hdd",
        "indep", pg_pool_t::TYPE_ERASURE, false, &ss);
      ceph_assert(rule_id >= 0);
    });
    return rule_id;
  }

  // Whether PastIntervals::check_new_interval() marks the interval that
  // ends with old_acting as maybe_went_rw, with interval_pool as the pool
  // of that interval's map and the primary up through the interval.
  bool interval_maybe_went_rw(const pg_pool_t &interval_pool,
                              const vector<int> &old_acting) {
    int primary = -1;
    for (int osd : old_acting) {
      if (osd != CRUSH_ITEM_NONE) {
        primary = osd;
        break;
      }
    }
    auto lastmap = std::make_shared<OSDMap>();
    lastmap->deepish_copy_from(*osdmap);
    OSDMap::Incremental inc(lastmap->get_epoch() + 1);
    inc.fsid = lastmap->get_fsid();
    inc.new_pools[1] = interval_pool;
    if (primary >= 0) {
      inc.new_up_thru[primary] = inc.epoch;
    }
    lastmap->apply_incremental(inc);
    auto newmap = std::make_shared<OSDMap>();
    newmap->deepish_copy_from(*lastmap);
    OSDMap::Incremental next(newmap->get_epoch() + 1);
    next.fsid = newmap->get_fsid();
    newmap->apply_incremental(next);

    struct : IsPGRecoverablePredicate {
      bool operator()(const set<pg_shard_t> &) const override { return true; }
    } recoverable;
    // a new interval: osd.7 alone, which no old_acting here is
    vector<int> new_acting(old_acting.size(), CRUSH_ITEM_NONE);
    new_acting[0] = 7;
    PastIntervals past;
    EXPECT_TRUE(PastIntervals::check_new_interval(
      primary, 7, old_acting, new_acting, primary, 7, old_acting, new_acting,
      lastmap->get_epoch(), 0, newmap.get(), lastmap.get(), pg_t(0, 1),
      recoverable, &past));
    return past.size() == 1;
  }

  void add_empty_datacenter(const std::string &name) {
    modify_crush([&](CrushWrapper &crush) {
      int dc = 0;
      int r = crush.add_bucket(0, CRUSH_BUCKET_STRAW2, CRUSH_HASH_RJENKINS1,
                               9, 0, nullptr, nullptr, &dc);
      ceph_assert(r == 0);
      crush.set_item_name(dc, name);
      r = crush.insert_item(g_ceph_context, dc, 0.0, name,
                            {{"root", "default"}});
      ceph_assert(r == 0);
    });
  }
};

// ===========================================================================
// stretch_num_acting_below_min_size
// ===========================================================================

// All zones fully populated
TEST_F(StretchECMinSizeTest, NumActingBelowMinSize_AllZonesHealthy)
{
  vector<int> acting = {0, 1, 2, 3, 4, 5};
  EXPECT_EQ(0u, osdmap->stretch_num_acting_below_min_size(*pool, acting));
}

// One zone exactly at min_size
TEST_F(StretchECMinSizeTest, NumActingBelowMinSize_OneZoneAtMinSize)
{
  vector<int> acting = {0, 1, CRUSH_ITEM_NONE,   // dc0: 2 = min_size
                        3, 4, 5};                 // dc1: 3
  EXPECT_EQ(0u, osdmap->stretch_num_acting_below_min_size(*pool, acting));
}

// dc0 has 1 OSD (< min_size=2) - deficit 1; dc1 OK
TEST_F(StretchECMinSizeTest, NumActingBelowMinSize_OneZoneBelowMinSize)
{
  vector<int> acting = {0, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE,
                        3, 4, 5};
  EXPECT_EQ(1u, osdmap->stretch_num_acting_below_min_size(*pool, acting));
}

// Both zones have 1 OSD each - deficit 1+1 = 2
TEST_F(StretchECMinSizeTest, NumActingBelowMinSize_BothZonesBelowMinSize)
{
  vector<int> acting = {0, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE,
                        3, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE};
  EXPECT_EQ(2u, osdmap->stretch_num_acting_below_min_size(*pool, acting));
}

// dc1 completely empty - deficit = min_size = 2
TEST_F(StretchECMinSizeTest, NumActingBelowMinSize_OneZoneCompletelyEmpty)
{
  vector<int> acting = {0, 1, 2,
                        CRUSH_ITEM_NONE, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE};
  EXPECT_EQ(2u, osdmap->stretch_num_acting_below_min_size(*pool, acting));
}

// Both zones empty - deficit = 2 * min_size = 4
TEST_F(StretchECMinSizeTest, NumActingBelowMinSize_BothZonesEmpty)
{
  vector<int> acting(6, CRUSH_ITEM_NONE);
  EXPECT_EQ(4u, osdmap->stretch_num_acting_below_min_size(*pool, acting));
}

TEST_F(StretchECMinSizeTest, NumActingBelowMinSize_ThreeAZCluster)
{
  pg_pool_t three_az_pool = *pool;
  three_az_pool.num_zones = 3;
  three_az_pool.peering_crush_bucket_count = 2;

  vector<int> acting = {0, 1, 2, 3, 4, 5};
  EXPECT_EQ(0u, osdmap->stretch_num_acting_below_min_size(three_az_pool, acting));

  acting = {0, 1, 2, 3, 4, CRUSH_ITEM_NONE};
  EXPECT_EQ(1u, osdmap->stretch_num_acting_below_min_size(three_az_pool, acting));
}

// An EC pool that an older release's stretch set gave stretch values has one
// zone, so min_size counts the whole acting set, not each datacenter
TEST_F(StretchECMinSizeTest, OneZonePoolWithStretchValues_WholeActingSetCounts)
{
  pg_pool_t one_zone = *pool;
  one_zone.num_zones = 1;
  one_zone.size = 3;
  vector<int> acting = {0, 1, 3}; // dc0: 2, dc1: 1
  EXPECT_EQ(0u, osdmap->stretch_num_acting_below_min_size(one_zone, acting));
  acting = {0, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE};
  EXPECT_EQ(1u, osdmap->stretch_num_acting_below_min_size(one_zone, acting));
  acting = {0, CRUSH_ITEM_NONE, 3};
  EXPECT_EQ(0u, osdmap->stretch_num_acting_below_min_size(one_zone, acting));
}

// Non-stretch pool - always 0
TEST_F(StretchECMinSizeTest, NumActingBelowMinSize_NonStretchPool)
{
  pg_pool_t non_stretch = *pool;
  non_stretch.peering_crush_bucket_count = 0;
  vector<int> acting = {0, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE,
                        3, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE};
  EXPECT_EQ(0u, osdmap->stretch_num_acting_below_min_size(non_stretch, acting));
}

// A replicated stretch pool is counted per zone, as an EC one is
TEST_F(StretchECMinSizeTest, NumActingBelowMinSize_ReplicatedPool)
{
  pg_pool_t rep = *pool;
  rep.type = pg_pool_t::TYPE_REPLICATED;
  vector<int> acting = {0, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE,
                        3, CRUSH_ITEM_NONE, CRUSH_ITEM_NONE};
  EXPECT_EQ(2u, osdmap->stretch_num_acting_below_min_size(rep, acting));
}

// Async recovery of a stretch pool must not take a zone below its min_size,
// however many OSDs the other zone has; other pools keep min_size in all.
TEST_F(StretchECMinSizeTest, AsyncRecoveryKeepsEachZoneAtMinSize)
{
  pg_pool_t rep = *pool;
  rep.type = pg_pool_t::TYPE_REPLICATED;
  rep.min_size = 2;
  // zone dc0 is OSDs 0-2, dc1 OSDs 3-5
  EXPECT_TRUE(osdmap->async_recovery_can_shrink(rep, {0, 1, 3, 4, 5}));
  EXPECT_FALSE(osdmap->async_recovery_can_shrink(rep, {0, 3, 4, 5}));
  EXPECT_TRUE(osdmap->async_recovery_can_shrink(rep, {0, 1, 3, 4}));

  pg_pool_t ec = *pool;
  ec.min_size = 2;
  const int none = CRUSH_ITEM_NONE;
  EXPECT_TRUE(osdmap->async_recovery_can_shrink(ec, {0, 1, none, 3, 4, 5}));
  EXPECT_FALSE(osdmap->async_recovery_can_shrink(ec, {0, none, none, 3, 4, 5}));

  for (auto p : {&rep, &ec}) {
    p->peering_crush_bucket_count = 0;
    p->peering_crush_bucket_target = 0;
    p->peering_crush_bucket_barrier = 0;
    p->num_zones = 1;
    p->size = 3;
  }
  EXPECT_TRUE(osdmap->async_recovery_can_shrink(rep, {0, 1}));
  EXPECT_FALSE(osdmap->async_recovery_can_shrink(rep, {0}));
  EXPECT_TRUE(osdmap->async_recovery_can_shrink(ec, {0, none, 2}));
  EXPECT_FALSE(osdmap->async_recovery_can_shrink(ec, {0, none, none}));
}

// Degraded stretch mode: only the mandatory zone contributes to the deficit.
TEST_F(StretchECMinSizeTest, NumActingBelowMinSize_MandatoryMemberOnlyCountsThatZone)
{
  pg_pool_t degraded = *pool;
  degraded.peering_crush_bucket_count = 1;
  degraded.peering_crush_mandatory_member = osdmap->crush->get_item_id("dc0");
  const int N = CRUSH_ITEM_NONE;
  EXPECT_EQ(0u, osdmap->stretch_num_acting_below_min_size(
    degraded, {0, 1, 2, N, N, N}));
  EXPECT_EQ(1u, osdmap->stretch_num_acting_below_min_size(
    degraded, {0, N, N, 3, 4, 5}));
  EXPECT_EQ(2u, osdmap->stretch_num_acting_below_min_size(
    degraded, {N, N, N, 3, 4, 5}));
}

// ===========================================================================
// Past intervals: an interval could have taken writes only if the PG could
// write in it, as PeeringState::acting_set_writeable() decides: min_size in
// every zone or, in degraded stretch mode, in the zone that was then the
// mandatory member, which the interval's own map gives.
// ===========================================================================

TEST_F(StretchECMinSizeTest, PastInterval_BothZonesHealthy)
{
  EXPECT_TRUE(interval_maybe_went_rw(*pool, {0, 1, 2, 3, 4, 5}));
}

// Healthy stretch mode needs min_size in both zones: with dc1 absent or below
// min_size, the PG was only peered.
TEST_F(StretchECMinSizeTest, PastInterval_OneZoneBelowMinSize)
{
  const int N = CRUSH_ITEM_NONE;
  EXPECT_FALSE(interval_maybe_went_rw(*pool, {0, 1, 2, N, N, N}));
  EXPECT_FALSE(interval_maybe_went_rw(*pool, {0, 1, N, 3, N, N}));
}

TEST_F(StretchECMinSizeTest, PastInterval_BothZonesBelowMinSize)
{
  const int N = CRUSH_ITEM_NONE;
  EXPECT_FALSE(interval_maybe_went_rw(*pool, {0, N, N, 3, N, N}));
}

TEST_F(StretchECMinSizeTest, PastInterval_EmptyActingSet)
{
  const int N = CRUSH_ITEM_NONE;
  EXPECT_FALSE(interval_maybe_went_rw(*pool, {N, N, N, N, N, N}));
}

// Degraded stretch mode counts only the mandatory member: min_size elsewhere
// does not let the PG write.
TEST_F(StretchECMinSizeTest, PastInterval_DegradedCountsOnlyMandatoryMember)
{
  const int N = CRUSH_ITEM_NONE;
  pg_pool_t degraded = *pool;
  degraded.peering_crush_bucket_count = 1;
  degraded.peering_crush_mandatory_member = osdmap->crush->get_item_id("dc1");
  EXPECT_TRUE(interval_maybe_went_rw(degraded, {N, N, N, 3, 4, N}));
  EXPECT_FALSE(interval_maybe_went_rw(degraded, {0, 1, 2, 3, N, N}));
  EXPECT_FALSE(interval_maybe_went_rw(degraded, {0, 1, 2, N, N, N}));
  EXPECT_FALSE(interval_maybe_went_rw(degraded, {0, 1, N, 3, N, N}));

  degraded.peering_crush_mandatory_member = osdmap->crush->get_item_id("dc0");
  EXPECT_TRUE(interval_maybe_went_rw(degraded, {0, 1, 2, N, N, N}));
  EXPECT_TRUE(interval_maybe_went_rw(degraded, {0, 1, N, 3, N, N}));
}

// An individual stretch pool counts min_size over all its copies.
TEST_F(StretchECMinSizeTest, PastInterval_OneZonePool)
{
  const int N = CRUSH_ITEM_NONE;
  pg_pool_t one_zone = *pool;
  one_zone.num_zones = 1;
  one_zone.size = 3;
  EXPECT_TRUE(interval_maybe_went_rw(one_zone, {0, N, 3}));
  EXPECT_FALSE(interval_maybe_went_rw(one_zone, {0, N, N}));
}

// A rule with one TAKE per datacenter finds the same zones as a root TAKE.
TEST_F(StretchECMinSizeTest, PerDatacenterTakeRule_FindsBothZones)
{
  int rule_id = -1;
  modify_crush([&](CrushWrapper &crush) {
    const int dc0 = crush.get_item_id("dc0");
    const int dc1 = crush.get_item_id("dc1");
    crush_rule *rule = crush_make_rule(6, pg_pool_t::TYPE_ERASURE);
    int step = 0;
    crush_rule_set_step(rule, step++, CRUSH_RULE_TAKE, dc0, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_CHOOSELEAF_INDEP, 3, 1);
    crush_rule_set_step(rule, step++, CRUSH_RULE_EMIT, 0, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_TAKE, dc1, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_CHOOSELEAF_INDEP, 3, 1);
    crush_rule_set_step(rule, step++, CRUSH_RULE_EMIT, 0, 0);
    rule_id = crush_add_rule(crush.get_crush_map(), rule, -1);
    ceph_assert(rule_id >= 0);
    crush.set_rule_name(rule_id, "per_dc_rule");
  });
  pg_pool_t per_dc = *pool;
  per_dc.crush_rule = rule_id;
  const int N = CRUSH_ITEM_NONE;
  EXPECT_EQ(0u, osdmap->stretch_num_acting_below_min_size(
    per_dc, {0, 1, 2, 3, 4, 5}));
  EXPECT_EQ(1u, osdmap->stretch_num_acting_below_min_size(
    per_dc, {0, 1, 2, 3, N, N}));
  EXPECT_EQ(2u, osdmap->stretch_num_acting_below_min_size(
    per_dc, {N, N, N, 3, 4, 5}));
}

// stretch_set_can_peer skips NONE entries and honors the mandatory member.
TEST_F(StretchECMinSizeTest, StretchSetCanPeer_NoneEntriesAndMandatoryMember)
{
  const int N = CRUSH_ITEM_NONE;
  EXPECT_FALSE(pool->stretch_set_can_peer(vector<int>{0, 1, 2, N, N, N}, *osdmap, nullptr));
  EXPECT_TRUE(pool->stretch_set_can_peer(vector<int>{0, N, N, 3, N, N}, *osdmap, nullptr));
  EXPECT_FALSE(pool->stretch_set_can_peer(vector<int>(6, N), *osdmap, nullptr));

  pg_pool_t degraded = *pool;
  degraded.peering_crush_bucket_count = 1;
  degraded.peering_crush_mandatory_member = osdmap->crush->get_item_id("dc1");
  EXPECT_FALSE(degraded.stretch_set_can_peer(vector<int>{0, 1, 2, N, N, N}, *osdmap, nullptr));
  EXPECT_TRUE(degraded.stretch_set_can_peer(vector<int>{N, N, N, 3, 4, 5}, *osdmap, nullptr));
  EXPECT_TRUE(degraded.stretch_set_can_peer(vector<int>{N, N, N, 3, N, N}, *osdmap, nullptr));
}

// A profile with crush-device-class makes the stretch rule TAKE the shadow
// root default~hdd; its datacenters are shadow buckets and must still count
// as zones.
TEST_F(StretchECMinSizeTest, DeviceClassRule_ShadowZonesCounted)
{
  pg_pool_t hdd = *pool;
  hdd.crush_rule = add_hdd_stretch_rule();
  const int N = CRUSH_ITEM_NONE;
  EXPECT_EQ(2u, osdmap->stretch_num_acting_below_min_size(
    hdd, {0, 1, 2, N, N, N}));

  // The mon stores the degraded-mode mandatory member as the normal bucket id
  hdd.peering_crush_bucket_count = 1;
  hdd.peering_crush_mandatory_member = osdmap->crush->get_item_id("dc0");
  EXPECT_EQ(0u, osdmap->stretch_num_acting_below_min_size(
    hdd, {0, 1, 2, N, N, N}));
  EXPECT_EQ(2u, osdmap->stretch_num_acting_below_min_size(
    hdd, {N, N, N, 3, 4, 5}));
}

// The degraded mode mandatory member is the normal bucket id of the
// surviving zone, whichever tree the rule takes.
TEST_F(StretchECMinSizeTest, DeviceClassRule_StretchSetCanPeerMandatoryMember)
{
  pg_pool_t hdd = *pool;
  hdd.crush_rule = add_hdd_stretch_rule();
  hdd.peering_crush_bucket_count = 1;
  hdd.peering_crush_mandatory_member = osdmap->crush->get_item_id("dc0");
  const int N = CRUSH_ITEM_NONE;
  EXPECT_TRUE(hdd.stretch_set_can_peer(vector<int>{0, 1, 2, N, N, N}, *osdmap, nullptr));
  EXPECT_FALSE(hdd.stretch_set_can_peer(vector<int>{N, N, N, 3, 4, 5}, *osdmap, nullptr));
}

// A datacenter with no OSDs, e.g. a site still being built, cannot hold any
// of the pool and must not add a deficit.
TEST_F(StretchECMinSizeTest, EmptyDatacenterUnderRoot_NoDeficit)
{
  add_empty_datacenter("dc2");
  const int N = CRUSH_ITEM_NONE;
  EXPECT_EQ(0u, osdmap->stretch_num_acting_below_min_size(
    *pool, {0, 1, 2, 3, 4, 5}));
  EXPECT_EQ(2u, osdmap->stretch_num_acting_below_min_size(
    *pool, {0, 1, 2, N, N, N}));
}

// A datacenter whose only OSD has another device class cannot hold any of a
// device-class pool either: its shadow bucket dc2~hdd is empty.
TEST_F(StretchECMinSizeTest, DeviceClassRule_DatacenterWithoutClassOsdsNoDeficit)
{
  pg_pool_t hdd = *pool;
  hdd.crush_rule = add_hdd_stretch_rule();
  modify_crush([&](CrushWrapper &crush) {
    int r = crush.insert_item(g_ceph_context, 8, 1.0, "osd.8",
                              {{"root", "default"}, {"datacenter", "dc2"},
                               {"host", "host8"}});
    ceph_assert(r == 0);
    std::ostringstream ss;
    r = crush.update_device_class(8, "ssd", "osd.8", &ss);
    ceph_assert(r >= 0);
  });
  ASSERT_TRUE(osdmap->crush->name_exists("dc2~hdd"));
  const int N = CRUSH_ITEM_NONE;
  EXPECT_EQ(0u, osdmap->stretch_num_acting_below_min_size(
    hdd, {0, 1, 2, 3, 4, 5}));
  EXPECT_EQ(2u, osdmap->stretch_num_acting_below_min_size(
    hdd, {0, 1, 2, N, N, N}));
}
