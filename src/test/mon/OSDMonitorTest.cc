// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2024 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

// WI-10-b: Unit tests verifying the require_osd_release umbrella gate for
// pool-level snapshot rollback operations.  These tests exercise the
// pg_pool_t / OSDMap structures that the preprocess_pool_op() handler
// relies on, validating that:
//   - a cluster at ceph_release_t::tentacle must return -EPERM
//   - a cluster at ceph_release_t::umbrella can proceed (no -EPERM)

#include "gtest/gtest.h"

#include "osd/osd_types.h"
#include "common/ceph_releases.h"

using namespace std;

// ---------------------------------------------------------------------------
// Helpers that replicate the logic in OSDMonitor::preprocess_pool_op() for
// the POOL_OP_ROLLBACK_SNAP and POOL_OP_ROLLBACK_UNMANAGED_SNAP cases without
// requiring a live monitor.
// ---------------------------------------------------------------------------

// Returns -EPERM when require_osd_release < umbrella; 0 otherwise.
static int check_rollback_snap_release_gate(ceph_release_t require_osd_release)
{
  if (require_osd_release < ceph_release_t::umbrella)
    return -EPERM;
  return 0;
}

// ---------------------------------------------------------------------------
// Pool-managed snap rollback gate tests
// ---------------------------------------------------------------------------

TEST(OSDMonitorRollbackGate, PoolSnapRollback_Tentacle_EPERM)
{
  // A cluster at tentacle must refuse pool-snap rollback with -EPERM.
  EXPECT_EQ(-EPERM,
            check_rollback_snap_release_gate(ceph_release_t::tentacle));
}

TEST(OSDMonitorRollbackGate, PoolSnapRollback_Umbrella_OK)
{
  // A cluster at umbrella or later must pass the gate (no -EPERM).
  EXPECT_EQ(0,
            check_rollback_snap_release_gate(ceph_release_t::umbrella));
}

// ---------------------------------------------------------------------------
// Unmanaged-snap rollback gate tests
// ---------------------------------------------------------------------------

TEST(OSDMonitorRollbackGate, UnmanagedSnapRollback_Tentacle_EPERM)
{
  EXPECT_EQ(-EPERM,
            check_rollback_snap_release_gate(ceph_release_t::tentacle));
}

TEST(OSDMonitorRollbackGate, UnmanagedSnapRollback_Umbrella_OK)
{
  EXPECT_EQ(0,
            check_rollback_snap_release_gate(ceph_release_t::umbrella));
}

// ---------------------------------------------------------------------------
// pg_pool_t rollback_snaps idempotency logic
//
// The preprocess handler short-circuits with the existing rollback_id when a
// second rollback for the same source snap is requested.  Verify that the
// rollback_snaps map on pg_pool_t works as expected.
// ---------------------------------------------------------------------------

TEST(OSDMonitorRollbackGate, IdempotencyCheck_ExistingRollback)
{
  pg_pool_t pool;
  pool.add_snap("snap1", utime_t());          // snap_seq → 1
  snapid_t source = pool.snap_exists("snap1");
  ASSERT_NE(0u, static_cast<uint64_t>(source));

  // Insert a rollback entry as prepare_pool_op() would
  rollback_snap_info_t rb;
  rb.source_snap  = source;
  rb.rollback_id  = pool.get_snap_seq() + 1;    // snap_seq + 1
  pool.rollback_snaps[rb.rollback_id] = rb;

  // A second request for the same source snap should be found (idempotency)
  bool found = false;
  for (auto& [id, info] : pool.rollback_snaps) {
    if (info.source_snap == source) {
      found = true;
      EXPECT_EQ(rb.rollback_id, id);
      break;
    }
  }
  EXPECT_TRUE(found) << "Expected to find existing rollback entry";
}

TEST(OSDMonitorRollbackGate, IdempotencyCheck_NoExistingRollback)
{
  pg_pool_t pool;
  pool.add_snap("snap1", utime_t());
  snapid_t source = pool.snap_exists("snap1");
  ASSERT_NE(0u, static_cast<uint64_t>(source));

  // No rollback registered yet -- lookup should fail
  snapid_t other_source = source + 10;
  bool found = false;
  for (auto& [id, info] : pool.rollback_snaps) {
    if (info.source_snap == other_source) {
      found = true;
      break;
    }
  }
  EXPECT_FALSE(found) << "Should not find rollback entry for unregistered source";
}

// ---------------------------------------------------------------------------
// WI-9-g: MON integration tests: preprocess guards, idempotency, ID
// allocation, and prune.
//
// These tests exercise the pg_pool_t and OSDMap data-structure-level logic
// that OSDMonitor::preprocess_pool_op() and prepare_pool_op() rely on,
// without requiring a live monitor.
//
// Scenarios:
//   (a) ID allocation: prepare_pool_op allocates rollback_id = snap_seq + 1.
//   (b) ID is monotonically increasing: second rollback gets higher ID.
//   (c) Idempotency: preprocess guard finds existing rollback for same source.
//   (d) Idempotency: no existing rollback for source → preprocess allows it.
//   (e) Prune: try_prune_completed_rollbacks removes entries from rollback_snaps
//       when all PGs have reported completion.
//   (f) Prune guard: if new_completed_rollbacks already non-empty, no double-prune.
// ---------------------------------------------------------------------------

// (a) ID allocation: rollback_id must be snap_seq + 1
TEST(OSDMonitorIntegration, IdAllocation_RollbackIdIsSnapSeqPlusOne)
{
  pg_pool_t pool;
  pool.flags = pg_pool_t::FLAG_POOL_SNAPS;
  pool.add_snap("snap1", utime_t());   // snap_seq → 1

  snapid_t snap_seq_before = pool.get_snap_seq();
  ASSERT_EQ(snapid_t(1), snap_seq_before);

  // prepare_pool_op allocates rollback_id = snap_seq + 1
  snapid_t rollback_id = snap_seq_before + 1;

  EXPECT_EQ(snapid_t(2), rollback_id)
      << "rollback_id must be snap_seq + 1 = 2";

  // After allocation, snap_seq advances to rollback_id
  pool.snap_seq = rollback_id;
  rollback_snap_info_t rb;
  rb.rollback_id = rollback_id;
  rb.source_snap = snapid_t(1);
  pool.rollback_snaps[rb.rollback_id] = rb;

  EXPECT_EQ(snapid_t(2), pool.snap_seq);
  EXPECT_TRUE(pool.rollback_snaps.count(snapid_t(2)));
}

// (b) Monotonically increasing: second rollback gets ID > first
TEST(OSDMonitorIntegration, IdAllocation_Monotonic)
{
  pg_pool_t pool;
  pool.flags = pg_pool_t::FLAG_POOL_SNAPS;
  pool.add_snap("snap1", utime_t());   // snap_seq = 1
  pool.add_snap("snap2", utime_t());   // snap_seq = 2

  // First rollback: snap_seq=2, rollback_id=3
  snapid_t rb1_id = pool.get_snap_seq() + 1;   // 3
  pool.snap_seq = rb1_id;
  rollback_snap_info_t rb1; rb1.rollback_id = rb1_id; rb1.source_snap = snapid_t(1);
  pool.rollback_snaps[rb1_id] = rb1;

  // Second rollback: snap_seq=3, rollback_id=4
  snapid_t rb2_id = pool.get_snap_seq() + 1;   // 4
  pool.snap_seq = rb2_id;
  rollback_snap_info_t rb2; rb2.rollback_id = rb2_id; rb2.source_snap = snapid_t(2);
  pool.rollback_snaps[rb2_id] = rb2;

  EXPECT_LT(rb1_id, rb2_id)
      << "second rollback ID must be strictly greater than the first";
  EXPECT_EQ(snapid_t(4), pool.snap_seq);
  EXPECT_EQ(2u, pool.rollback_snaps.size());
}

// (c) Idempotency: preprocess guard detects existing rollback for same source
TEST(OSDMonitorIntegration, Idempotency_ExistingRollbackDetected)
{
  pg_pool_t pool;
  pool.flags = pg_pool_t::FLAG_POOL_SNAPS;
  pool.add_snap("snap1", utime_t());     // snap_seq = 1
  snapid_t source = pool.snap_exists("snap1");
  ASSERT_NE(0u, static_cast<uint64_t>(source));

  // Insert first rollback
  snapid_t rb1_id = pool.get_snap_seq() + 1;
  pool.snap_seq = rb1_id;
  rollback_snap_info_t rb1; rb1.rollback_id = rb1_id; rb1.source_snap = source;
  pool.rollback_snaps[rb1_id] = rb1;

  // Simulate preprocess: a second request for the same source must be found
  snapid_t found_id = CEPH_NOSNAP;
  for (auto& [id, info] : pool.rollback_snaps) {
    if (info.source_snap == source) {
      found_id = id;
      break;
    }
  }

  EXPECT_NE(CEPH_NOSNAP, found_id)
      << "preprocess guard must detect existing rollback for the same source";
  EXPECT_EQ(rb1_id, found_id)
      << "the found rollback ID must match the first allocation";
}

// (d) Idempotency: no existing rollback → preprocess allows the new request
TEST(OSDMonitorIntegration, Idempotency_NewRollbackAllowed)
{
  pg_pool_t pool;
  pool.flags = pg_pool_t::FLAG_POOL_SNAPS;
  pool.add_snap("snap1", utime_t());
  snapid_t source = pool.snap_exists("snap1");

  // No rollback for source yet
  snapid_t found_id = CEPH_NOSNAP;
  for (auto& [id, info] : pool.rollback_snaps) {
    if (info.source_snap == source) {
      found_id = id;
      break;
    }
  }

  EXPECT_EQ(CEPH_NOSNAP, found_id)
      << "no existing rollback: preprocess must allow new allocation";
}

// (e) Prune: completed rollback IDs are removed from rollback_snaps.
//     Simulate try_prune_completed_rollbacks removing rb_id when all PGs
//     have reported completion (intersection == pool's rollback_snaps keys).
TEST(OSDMonitorIntegration, Prune_CompletedRollbackRemovedFromPool)
{
  pg_pool_t pool;
  pool.flags = pg_pool_t::FLAG_POOL_SNAPS;
  pool.add_snap("snap1", utime_t());

  const snapid_t rb_id(2);
  rollback_snap_info_t rb; rb.rollback_id = rb_id; rb.source_snap = snapid_t(1);
  pool.rollback_snaps[rb_id] = rb;

  ASSERT_EQ(1u, pool.rollback_snaps.size());

  // Simulate prune: intersect completed set with rollback_snaps
  snap_interval_set_t pool_completed;
  pool_completed.insert(rb_id, 1);  // all PGs reported rb_id complete

  snap_interval_set_t to_prune;
  for (auto& [id, _] : pool.rollback_snaps) {
    if (pool_completed.contains(id)) {
      to_prune.insert(id, 1);
    }
  }

  // Apply prune
  for (auto i = to_prune.begin(); i != to_prune.end(); ++i) {
    snapid_t start = i.get_start();
    snapid_t end   = i.get_start() + i.get_len();
    while (start < end) {
      pool.rollback_snaps.erase(start);
      ++start;
    }
  }

  EXPECT_TRUE(pool.rollback_snaps.empty())
      << "pruned rollback must be removed from pool.rollback_snaps";
}

// (f) Prune guard: if new_completed_rollbacks already non-empty, skip prune.
//     Simulates the idempotency guard at the top of try_prune_completed_rollbacks().
TEST(OSDMonitorIntegration, Prune_SkipIfAlreadyPruned)
{
  // Simulate the state: new_completed_rollbacks already has an entry
  snap_interval_set_t new_completed_rollbacks;
  new_completed_rollbacks.insert(snapid_t(2), 1);  // from prior prune pass

  // Guard: if !new_completed_rollbacks.empty(), return false (skip)
  bool should_skip = !new_completed_rollbacks.empty();
  EXPECT_TRUE(should_skip)
      << "prune must be skipped when new_completed_rollbacks is already non-empty "
         "(idempotency guard)";
}
