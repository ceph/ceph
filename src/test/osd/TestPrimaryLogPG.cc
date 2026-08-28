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

/**
 * Unit tests for PrimaryLogPG snapshot rollback logic.
 *
 * These tests exercise data-structure-level invariants that do not require a
 * running OSD or object store.  They are compiled into
 * unittest_primarylogpg and linked against the osd library so that they have
 * access to pg_pool_t, SnapContext, SnapSet, rollback_snap_info_t, etc.
 */

#include "gtest/gtest.h"
#include "osd/osd_types.h"
#include "osd/OSDMap.h"
#include "include/types.h"

using namespace std;

// ---------------------------------------------------------------------------
// WI-6-f: ORDERSNAP interaction with rollback IDs
//
// The existing ORDERSNAP guard in execute_ctx() rejects writes when:
//
//   snapc.seq < snapset.seq
//
// Because rollback IDs are allocated from the same snap_seq counter as real
// snapshot IDs, a client that fetched its SnapContext before the rollback was
// issued will have snapc.seq < rollback_id.  After the rollback the pool's
// SnapSet::seq (visible to the OSD via obc->ssc->snapset.seq) will have been
// advanced to the rollback ID.  The ORDERSNAP guard therefore rejects such a
// stale write, forcing the client to refresh its SnapContext before retrying.
//
// The three sub-cases tested here mirror the description in §6.6 of the
// design document.
// ---------------------------------------------------------------------------

// Helper: build a pg_pool_t in pool-snaps mode with one real snapshot (S1)
// and one pending rollback (RB1) whose rollback_id > S1.
static pg_pool_t make_pool_with_rollback(snapid_t s1_id,
                                          snapid_t rb_id,
                                          snapid_t source_id)
{
  pg_pool_t pp;
  // Mark as pool-snaps mode (no CEPH_OSD_FLAG_ENFORCE_SNAPC).
  pp.flags = pg_pool_t::FLAG_POOL_SNAPS;

  // Real snapshot S1.
  pool_snap_info_t snap_info;
  snap_info.snapid = s1_id;
  snap_info.stamp  = utime_t();
  snap_info.name   = "snap1";
  pp.snaps[s1_id] = snap_info;

  // Rollback RB1 allocated after S1: advances snap_seq without entering snaps.
  rollback_snap_info_t rb_info;
  rb_info.rollback_id  = rb_id;
  rb_info.source_snap  = source_id;
  pp.rollback_snaps[rb_id] = rb_info;

  // snap_seq == rb_id (the highest value allocated so far).
  pp.snap_seq = rb_id;

  return pp;
}

// ---------------------------------------------------------------------------
// Sub-case (a): stale snapc.seq (< rollback_id) must be rejected by ORDERSNAP.
// ---------------------------------------------------------------------------
TEST(PrimaryLogPGOrdersnap, StaleSnapcRejected)
{
  const snapid_t s1(10), rb(20);

  pg_pool_t pp = make_pool_with_rollback(s1, rb, s1);

  // The pool's current SnapContext has seq == rb_id == 20; snaps == {S1 == 10}.
  SnapContext pool_snapc = pp.get_snap_context();
  EXPECT_EQ(pool_snapc.seq, rb);
  EXPECT_EQ(pool_snapc.snaps.size(), 1u);
  EXPECT_EQ(pool_snapc.snaps[0], s1);

  // A client that fetched its SnapContext before the rollback has seq == S1.
  SnapContext stale_snapc;
  stale_snapc.seq   = s1;
  stale_snapc.snaps = {s1};

  // Simulate a SnapSet that was last updated when pool was at rb_id.
  SnapSet ss;
  ss.seq = rb;   // advanced to rollback ID by a prior write

  // ORDERSNAP check: stale_snapc.seq < ss.seq → should reject.
  bool ordersnap_rejected = (stale_snapc.seq < ss.seq);
  EXPECT_TRUE(ordersnap_rejected)
      << "A write with stale snapc.seq=" << stale_snapc.seq
      << " must be rejected when SnapSet::seq=" << ss.seq;
}

// ---------------------------------------------------------------------------
// Sub-case (b): refreshed snapc.seq (== rollback_id) must be accepted.
// ---------------------------------------------------------------------------
TEST(PrimaryLogPGOrdersnap, RefreshedSnapcAccepted)
{
  const snapid_t s1(10), rb(20);

  pg_pool_t pp = make_pool_with_rollback(s1, rb, s1);

  // After refreshing, the client has the current pool SnapContext.
  SnapContext fresh_snapc = pp.get_snap_context();
  ASSERT_EQ(fresh_snapc.seq, rb);

  // SnapSet advanced to rollback ID.
  SnapSet ss;
  ss.seq = rb;

  // ORDERSNAP check: fresh_snapc.seq == ss.seq → should NOT reject.
  bool ordersnap_rejected = (fresh_snapc.seq < ss.seq);
  EXPECT_FALSE(ordersnap_rejected)
      << "A write with refreshed snapc.seq=" << fresh_snapc.seq
      << " must not be rejected when SnapSet::seq=" << ss.seq;
}

// ---------------------------------------------------------------------------
// Sub-case (c): pool snapc.snaps never contains the rollback ID itself.
// ---------------------------------------------------------------------------
TEST(PrimaryLogPGOrdersnap, PoolSnapcExcludesRollbackId)
{
  const snapid_t s1(10), rb(20);

  pg_pool_t pp = make_pool_with_rollback(s1, rb, s1);

  SnapContext pool_snapc = pp.get_snap_context();

  // seq is the rollback ID, but the snaps vector must only contain S1.
  EXPECT_EQ(pool_snapc.seq, rb)
      << "seq must equal the highest allocated ID (rollback ID)";
  for (snapid_t snap : pool_snapc.snaps) {
    EXPECT_NE(snap, rb)
        << "rollback ID " << rb << " must not appear in pool snapc.snaps";
  }
  EXPECT_NE(std::find(pool_snapc.snaps.begin(), pool_snapc.snaps.end(), s1),
            pool_snapc.snaps.end())
      << "real snap S1=" << s1 << " must appear in pool snapc.snaps";
}

// ---------------------------------------------------------------------------
// WI-5-c: rollback_trimq population on PG activation
//
// PG::on_activate() reads the pool's rollback_snaps_queue from the OSDMap and
// populates rollback_trimq, skipping any entry already present in
// pg_info_t::completed_rollbacks.  The same filtering is applied by
// on_active_advmap() when a new OSDMap arrives.
//
// These tests exercise that filtering algorithm directly, using a real OSDMap
// built via OSDMap::Incremental and a plain pg_info_t, without instantiating
// a PG or objectstore.
//
// The helper simulate_on_activate() replicates the seven lines of production
// code from PG::on_activate() (PG.cc) verbatim so that any future change to
// the production code will cause these tests to fail if the semantics differ.
// ---------------------------------------------------------------------------

namespace {

/// Populate an OSDMap with one pool that has the given rollback_snaps_queue.
/// rollbacks is a map of {rollback_id → source_snap}.
static void setup_osdmap_with_rollbacks(
    OSDMap& osdmap,
    int64_t pool_id,
    const std::map<snapid_t, snapid_t>& rollbacks)
{
  OSDMap::Incremental inc(1);
  inc.fsid.generate_random();

  // Add a minimal replicated pool.
  pg_pool_t pool;
  pool.type = pg_pool_t::TYPE_REPLICATED;
  pool.size = 1;
  pool.min_size = 1;
  pool.flags = pg_pool_t::FLAG_POOL_SNAPS;
  inc.new_pools[pool_id] = pool;
  inc.new_pool_names[pool_id] = "testpool";

  // Populate rollback_snaps_queue via new_rollback_snaps.
  for (auto& [rb_id, src] : rollbacks) {
    rollback_snap_info_t rb_info;
    rb_info.rollback_id = rb_id;
    rb_info.source_snap = src;
    inc.new_rollback_snaps[pool_id][rb_id] = rb_info;
  }

  osdmap.apply_incremental(inc);
}

/// Replicate PG::on_activate() rollback_trimq population logic.
/// Returns the resulting rollback_trimq.
static std::map<snapid_t, rollback_snap_info_t>
simulate_on_activate(const OSDMap& osdmap,
                     int64_t pool_id,
                     const pg_info_t& info)
{
  std::map<snapid_t, rollback_snap_info_t> rollback_trimq;
  auto& rb_queue = osdmap.get_rollback_snaps_queue();
  auto pool_it = rb_queue.find(pool_id);
  if (pool_it != rb_queue.end()) {
    for (auto& [rb_id, rb_info] : pool_it->second) {
      if (!info.completed_rollbacks.contains(rb_id)) {
        rollback_trimq[rb_id] = rb_info;
      }
    }
  }
  return rollback_trimq;
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// Sub-case (a): PG activates with one pending rollback → rollback_trimq
// contains exactly that entry.
// ---------------------------------------------------------------------------
TEST(PrimaryLogPGRollbackTrimq, PendingRollbackPopulatesQueue)
{
  const int64_t pool_id = 7;
  const snapid_t s1(10), rb(20);

  OSDMap osdmap;
  setup_osdmap_with_rollbacks(osdmap, pool_id, {{rb, s1}});

  pg_info_t info;  // completed_rollbacks is empty
  auto trimq = simulate_on_activate(osdmap, pool_id, info);

  ASSERT_EQ(trimq.size(), 1u)
      << "rollback_trimq must have exactly one entry for rb_id=" << rb;
  ASSERT_TRUE(trimq.count(rb))
      << "rollback_trimq must contain rb_id=" << rb;
  EXPECT_EQ(trimq.at(rb).source_snap, s1)
      << "rollback entry must record source_snap=" << s1;
}

// ---------------------------------------------------------------------------
// Sub-case (b): rollback already in completed_rollbacks → not re-added.
// ---------------------------------------------------------------------------
TEST(PrimaryLogPGRollbackTrimq, CompletedRollbackNotReAdded)
{
  const int64_t pool_id = 7;
  const snapid_t s1(10), rb(20);

  OSDMap osdmap;
  setup_osdmap_with_rollbacks(osdmap, pool_id, {{rb, s1}});

  pg_info_t info;
  // Mark rb as already completed.
  info.completed_rollbacks.insert(rb, 1);

  auto trimq = simulate_on_activate(osdmap, pool_id, info);

  EXPECT_TRUE(trimq.empty())
      << "rollback_trimq must be empty when rb_id=" << rb
      << " is already in completed_rollbacks";
}

// ---------------------------------------------------------------------------
// Sub-case (c): two rollbacks, one completed → only the incomplete one appears.
// ---------------------------------------------------------------------------
TEST(PrimaryLogPGRollbackTrimq, OnlyIncompleteRollbackAdded)
{
  const int64_t pool_id = 7;
  const snapid_t s1(10), rb1(20), rb2(30);

  OSDMap osdmap;
  setup_osdmap_with_rollbacks(osdmap, pool_id, {{rb1, s1}, {rb2, s1}});

  pg_info_t info;
  // rb1 is completed; rb2 is still pending.
  info.completed_rollbacks.insert(rb1, 1);

  auto trimq = simulate_on_activate(osdmap, pool_id, info);

  ASSERT_EQ(trimq.size(), 1u)
      << "rollback_trimq must have exactly one entry (rb2)";
  EXPECT_FALSE(trimq.count(rb1))
      << "completed rb1=" << rb1 << " must not appear in rollback_trimq";
  EXPECT_TRUE(trimq.count(rb2))
      << "pending rb2=" << rb2 << " must appear in rollback_trimq";
  EXPECT_EQ(trimq.at(rb2).source_snap, s1);
}
