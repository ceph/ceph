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
#include "test/osd/ReplicaPeeringTestFixture.h"

using namespace std;

// Replicated pool, size 3, on osd.0-5.  SetUp() places the PG on osd.0-2.
class TestReplicaPeering : public ReplicaPeeringTestFixture {
public:
  TestReplicaPeering() {
    spare_osds = 3;
  }

  const std::string data = std::string(4096, 'A');
  const hobject_t obj = make_test_object("obj");

  PeeringState* primary_ps() {
    return get_primary_test_pg()->get_peering_state();
  }

  std::set<pg_shard_t> obj_locations() {
    return primary_ps()->get_missing_loc().get_locations(obj);
  }

  // Apply the pg_temp change that the primary requested, failing if it did
  // not request one.
  void apply_requested_pg_temp() {
    ASSERT_TRUE(get_primary_test_pg()->get_peering_listener()->pg_temp_wanted);
    ASSERT_TRUE(new_epoch(true));
    new_epoch_loop();
  }

  // Leave osd.0 and osd.1 as strays holding the only copies of "obj", with
  // the PG log trimmed past their last_update so that they would need
  // backfill to rejoin.
  void make_strays_sole_sources() {
    ASSERT_TRUE(all_shards_active());
    osdmap->set_flag(CEPH_OSDMAP_PGLOG_HARDLIMIT);

    mark_osd_down(2);
    create_and_write_verify("obj", data);
    mark_osd_up(2);

    // osd.3 and osd.4 are new to the PG, but the log still starts at 0'0 so
    // they peer, like osd.2, as log-recovery targets that are missing "obj".
    set_pg_upmap({3, 4, 2});
    ASSERT_EQ(3, get_primary_shard_from_osdmap());
    for (int osd : {2, 3, 4}) {
      ASSERT_TRUE(
        get_peering_state_on_osd(osd)->get_pg_log().get_missing().is_missing(obj));
    }
    ASSERT_EQ(std::set<pg_shard_t>({pg_shard_t(0), pg_shard_t(1)}),
              obj_locations());

    enable_log_trimming = true;
    set_target_pg_log_entries(1);
    create_and_write_verify("filler", data);
    for (const char c : {'B', 'C', 'D', 'E'}) {
      write_verify("filler", 0, std::string(data.size(), c), data.size());
    }
    ASSERT_LT(get_peering_state_on_osd(0)->get_info().last_update,
              primary_ps()->get_pg_log().get_tail());
    ASSERT_LT(get_peering_state_on_osd(1)->get_info().last_update,
              primary_ps()->get_pg_log().get_tail());
  }
};

// https://tracker.ceph.com/issues/82013: an OSD that has fallen off the log
// and rejoins the up set is backfilled, and resetting it for backfill
// forgets which objects it holds.  When it holds the last copies of objects
// that the acting set is missing, those objects become unfound for good.
TEST_F(TestReplicaPeering, RecoverySourcesRejoiningUpStayFound) {
  ScopedConfig cfg_trim_min("osd_pg_log_trim_min", "1");
  ScopedConfig cfg_async_recovery("osd_async_recovery_min_cost", "100");
  ASSERT_NO_FATAL_FAILURE(make_strays_sole_sources());
  const std::set<pg_shard_t> holders = {pg_shard_t(0), pg_shard_t(1)};

  // One holder rejoins the up set.
  set_pg_upmap({3, 4, 0});
  EXPECT_EQ(holders, obj_locations());
  EXPECT_EQ(0u, primary_ps()->get_num_unfound());

  // In a later epoch the other holder rejoins too.
  set_pg_upmap({3, 1, 0});
  EXPECT_EQ(holders, obj_locations());
  EXPECT_EQ(0u, primary_ps()->get_num_unfound());

  // A holder that restarts is still a source when it comes back.
  mark_osd_down(0);
  EXPECT_EQ(std::set<pg_shard_t>({pg_shard_t(1)}), obj_locations());
  mark_osd_up(0);
  EXPECT_EQ(holders, obj_locations());

  // So it is when another OSD becomes primary.
  mark_osd_down(3);
  EXPECT_NE(3, get_primary_shard_from_osdmap());
  EXPECT_EQ(holders, obj_locations());
  mark_osd_up(3);
  EXPECT_EQ(holders, obj_locations());

  // The holders stay sources when they leave the up set again.  Had they
  // been reset for backfill, no later epoch would make them sources again.
  set_pg_upmap({3, 4, 2});
  EXPECT_EQ(holders, obj_locations());
  EXPECT_EQ(0u, primary_ps()->get_num_unfound());
}

// Holders left out of backfill are backfilled once the objects they held have
// been recovered.
TEST_F(TestReplicaPeering, DeferredBackfillStartsAfterRecovery) {
  ScopedConfig cfg_trim_min("osd_pg_log_trim_min", "1");
  ScopedConfig cfg_async_recovery("osd_async_recovery_min_cost", "100");
  ASSERT_NO_FATAL_FAILURE(make_strays_sole_sources());
  const std::set<pg_shard_t> holders = {pg_shard_t(0), pg_shard_t(1)};

  set_pg_upmap({3, 1, 0});
  ASSERT_EQ(holders, obj_locations());
  ASSERT_TRUE(primary_ps()->get_backfill_targets().empty());

  run_recovery("obj");
  verify_object("obj");
  ASSERT_NO_FATAL_FAILURE(apply_requested_pg_temp());

  EXPECT_EQ(3, get_primary_shard_from_osdmap());
  EXPECT_EQ(holders, primary_ps()->get_backfill_targets());
  EXPECT_FALSE(primary_ps()->needs_recovery());
}

// When recovery completes and another OSD is waiting for backfill, the holders
// join that backfill in a new interval instead of waiting for it to finish.
TEST_F(TestReplicaPeering, DeferredBackfillJoinsOtherBackfill) {
  ScopedConfig cfg_trim_min("osd_pg_log_trim_min", "1");
  ScopedConfig cfg_async_recovery("osd_async_recovery_min_cost", "100");
  ASSERT_NO_FATAL_FAILURE(make_strays_sole_sources());

  set_pg_upmap({3, 1, 5});
  ASSERT_EQ(std::set<pg_shard_t>({pg_shard_t(0), pg_shard_t(1)}),
            obj_locations());
  ASSERT_EQ(std::set<pg_shard_t>({pg_shard_t(5)}),
            primary_ps()->get_backfill_targets());

  run_recovery("obj");
  ASSERT_NO_FATAL_FAILURE(apply_requested_pg_temp());

  EXPECT_EQ(std::set<pg_shard_t>({pg_shard_t(1), pg_shard_t(5)}),
            primary_ps()->get_backfill_targets());
}
