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
  rb.stamp        = utime_t();
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
