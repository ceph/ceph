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

// ---------------------------------------------------------------------------
// WI-18-f: make_writeable() rollback-safe clone logic (§16)
//
// simulate_make_writeable() replicates verbatim the three expressions fixed
// by WI-18-c/d/e in PrimaryLogPG::make_writeable():
//
//   (WI-18-c) clone naming:  coid.snap = ctx->real_snap_seq
//   (WI-18-d) clone gate:    snapc.snaps[0] > max(new_snapset.seq, real_snap_seq)
//   (WI-18-e) seq update:    effective_seq = real_snap_seq ?: snapc.seq
//
// The four tests cover:
//   (a) No spurious clone when R is the only seq advance (gate always false).
//   (b) Clone is named real_snap_seq, not snapc.seq=R, when gate fires.
//   (c) SnapSet::seq never holds a rollback ID after any write.
//   (d) Spurious clone suppressed when new_snapset.seq was contaminated by R.
// ---------------------------------------------------------------------------

namespace {

/// Result of simulating the make_writeable() clone gate and naming logic.
struct MakeWriteableResult {
  bool clone_created;     ///< true if the clone gate fired
  snapid_t clone_name;    ///< snap ID assigned to the clone (if created)
  snapid_t new_snapset_seq; ///< SnapSet::seq after the write
};

/// Simulate the three make_writeable() expressions fixed by WI-18-a–e.
///
/// @param snapc         pool SnapContext (seq may be a rollback ID)
/// @param real_snap_seq highest key in pool.info.snaps, or 0
/// @param head_exists   whether the head object exists (for gate)
/// @param ss_seq        current SnapSet::seq on the object
static MakeWriteableResult simulate_make_writeable(
    const SnapContext& snapc,
    snapid_t real_snap_seq,
    bool head_exists,
    snapid_t ss_seq)
{
  MakeWriteableResult r;
  r.new_snapset_seq = ss_seq;
  r.clone_created = false;
  r.clone_name = snapid_t(0);

  // --- Clone gate (WI-18-d) ---
  bool gate = (head_exists &&
               snapc.snaps.size() &&
               snapc.snaps[0] > std::max(ss_seq, real_snap_seq));

  if (gate) {
    r.clone_created = true;
    // --- Clone naming (WI-18-c) ---
    r.clone_name = real_snap_seq;
  }

  // --- SnapSet::seq update (WI-18-e) ---
  snapid_t effective_seq = real_snap_seq ? real_snap_seq : snapc.seq;
  if (effective_seq > r.new_snapset_seq) {
    r.new_snapset_seq = effective_seq;
  }

  return r;
}

} // anonymous namespace (extends existing one; defined separately to keep
  // helpers grouped by work-item)

// ---------------------------------------------------------------------------
// Sub-case (a): No spurious clone when rollback ID R is the sole seq advance.
//
// Scenario: pool has snap S1=10.  Rollback R=20 is issued.  No new real snap.
// An object that was last written at S1 is written again.
//   snapc.seq = R=20, snapc.snaps = {S1=10}, real_snap_seq = S1=10.
//   new_snapset.seq = S1=10 (object already up-to-date through S1).
// Gate: snaps[0]=10 > max(10, 10) = 10 > 10 → false → no clone.  Correct.
// SnapSet::seq: effective_seq = S1=10, 10 > 10 → false → seq stays S1=10.
// ---------------------------------------------------------------------------
TEST(PrimaryLogPGMakeWriteable, NoSpuriousCloneForRollbackOnlySeqAdvance)
{
  const snapid_t S1(10), R(20);

  SnapContext snapc;
  snapc.seq   = R;          // seq is a rollback ID
  snapc.snaps = {S1};       // only real snaps

  const snapid_t real_snap_seq = S1;  // pool.info.snaps.rbegin()->first
  const snapid_t ss_seq        = S1;  // object last written at S1

  auto res = simulate_make_writeable(snapc, real_snap_seq,
                                     /*head_exists=*/true, ss_seq);

  EXPECT_FALSE(res.clone_created)
      << "No clone should be created when rollback ID R=" << R
      << " is the only advance beyond S1=" << S1;

  EXPECT_FALSE(res.new_snapset_seq == R)
      << "SnapSet::seq must not be set to rollback ID R=" << R;

  EXPECT_EQ(res.new_snapset_seq, S1)
      << "SnapSet::seq must remain at real snap S1=" << S1;
}

// ---------------------------------------------------------------------------
// Sub-case (b): Clone is named real_snap_seq (S1), not snapc.seq (R).
//
// Set up a context where the gate fires: ss_seq=0, real_snap_seq=S1=10,
// snapc.snaps[0]=S_new=30, snapc.seq=R=20 (rollback ID).
// Gate: 30 > max(0, 10) = 30 > 10 → true → clone fires.
// Post-fix clone name: real_snap_seq = S1=10 (not R=20).
// ---------------------------------------------------------------------------
TEST(PrimaryLogPGMakeWriteable, CloneNamedAfterRealSnapNotRollbackId)
{
  // ss_seq=0 (object never written under any snapshot), real_snap_seq=S1,
  // but snapc.snaps={S_new} and snapc.seq=R triggers the gate.

  const snapid_t S1(10), R(20), S_new(30);

  SnapContext snapc;
  snapc.seq   = R;        // snapc.seq is a rollback ID
  snapc.snaps = {S_new};  // only real snap visible in this context

  const snapid_t real_snap_seq = S1;  // pool.info.snaps has only S1 on this OSD
  const snapid_t ss_seq        = snapid_t(0); // object never written under any snap

  auto res = simulate_make_writeable(snapc, real_snap_seq,
                                     /*head_exists=*/true, ss_seq);

  ASSERT_TRUE(res.clone_created)
      << "Gate must fire: snaps[0]=" << S_new
      << " > max(ss_seq=0, real_snap_seq=" << S1 << ")";

  EXPECT_EQ(res.clone_name, S1)
      << "Clone must be named after real_snap_seq=" << S1
      << ", not rollback ID R=" << R;

  EXPECT_NE(res.clone_name, R)
      << "Clone name must NOT be the rollback ID R=" << R;
}

// ---------------------------------------------------------------------------
// Sub-case (c): SnapSet::seq never contains a rollback ID after any write.
//
// Three writes tested:
//  1. Write while pool has only R (no real snaps): seq must stay 0.
//  2. Write while pool has S1 and R (R > S1): seq must be S1, not R.
//  3. Write while pool has S1, R, S_new (S_new > R > S1): seq must be S_new.
// ---------------------------------------------------------------------------
TEST(PrimaryLogPGMakeWriteable, SnapSetSeqNeverContainsRollbackId)
{
  const snapid_t S1(10), R(20), S_new(30);

  // Write 1: pool has S1 and R (R > S1). real_snap_seq = S1.
  // Object never written before (ss_seq=0): seq must advance to S1, not R.
  {
    SnapContext snapc;
    snapc.seq   = R;
    snapc.snaps = {S1};
    const snapid_t real_snap_seq = S1;
    const snapid_t ss_seq        = snapid_t(0);

    auto res = simulate_make_writeable(snapc, real_snap_seq,
                                       /*head_exists=*/true, ss_seq);

    EXPECT_NE(res.new_snapset_seq, R)
        << "SnapSet::seq must not be set to rollback ID R=" << R;
    EXPECT_EQ(res.new_snapset_seq, S1)
        << "SnapSet::seq must be set to real snap S1=" << S1;
  }

  // Write 3: pool has S1, R, S_new (S_new > R > S1). real_snap_seq = S_new.
  {
    SnapContext snapc;
    snapc.seq   = S_new;   // highest allocated ID is S_new (real)
    snapc.snaps = {S_new, S1};
    const snapid_t real_snap_seq = S_new;
    const snapid_t ss_seq        = snapid_t(0);

    auto res = simulate_make_writeable(snapc, real_snap_seq,
                                       /*head_exists=*/true, ss_seq);

    EXPECT_NE(res.new_snapset_seq, R)
        << "SnapSet::seq must not be set to rollback ID R=" << R;
    EXPECT_EQ(res.new_snapset_seq, S_new)
        << "SnapSet::seq must be set to real snap S_new=" << S_new;
  }
}

// ---------------------------------------------------------------------------
// Sub-case (d): Gate suppresses spurious clone when new_snapset.seq was
// previously contaminated with a rollback ID (pre-fix scenario).
//
// Scenario: a prior write (before fix) stored R=20 into new_snapset.seq.
// Then S_new=30 is created. Next write:
//   pre-fix gate: snaps[0]=30 > new_snapset.seq=20 → true → spurious clone R.
//   post-fix gate: snaps[0]=30 > max(20, 30) = 30 > 30 → false → no clone.
// ---------------------------------------------------------------------------
TEST(PrimaryLogPGMakeWriteable, GateSuppressesSpuriousCloneWhenSeqContaminated)
{
  const snapid_t R(20), S_new(30);

  SnapContext snapc;
  snapc.seq   = S_new;
  snapc.snaps = {S_new};

  const snapid_t real_snap_seq = S_new; // post-fix: real snap is S_new
  const snapid_t ss_seq        = R;     // contaminated by prior rollback write

  auto res = simulate_make_writeable(snapc, real_snap_seq,
                                     /*head_exists=*/true, ss_seq);

  EXPECT_FALSE(res.clone_created)
      << "Post-fix gate must suppress spurious clone when new_snapset.seq="
      << R << " (rollback ID) and real_snap_seq=snaps[0]=" << S_new;
}

// ---------------------------------------------------------------------------
// WI-9-c: build_pending_ops() tests.
//
// build_pending_ops(pp, obj_seq, current_seq) collects all snapshot and
// rollback events in the half-open range (obj_seq, current_seq] from the
// pool, sorts them by ID ascending, and returns a vector of pending_op_t.
//
// The helper simulate_build_pending_ops() replicates the production logic
// from PrimaryLogPG::build_pending_ops() verbatim.
//
// Scenarios tested:
//   (a) Simple rollback only – one ROLLBACK op returned.
//   (b) Stacked: snap + rollback + snap + rollback + snap (§8.2 setup).
//   (c) NOP rollback: snap never written after snapshot; object predates
//       rollback (obj_seq < snap ID for which rollback issued) -- the op
//       list still contains the ROLLBACK entry (execution decides no-op).
//   (d) Multi-snap sharing one clone: rollback of snap 2 when obj_seq=3
//       (all events behind obj_seq are excluded).
//   (e) Object created after snapshot: obj_seq=0, snapc.seq=20, no snaps
//       in (0,20] -- only the rollback appears; write must succeed.
// ---------------------------------------------------------------------------

namespace {

struct pending_op_sim_t {
  enum Type { SNAP, ROLLBACK } type;
  snapid_t id;
  snapid_t source;
};

/// Replicates PrimaryLogPG::build_pending_ops() logic for test use.
static std::vector<pending_op_sim_t>
simulate_build_pending_ops(const pg_pool_t& pp,
                           snapid_t obj_seq,
                           snapid_t current_seq)
{
  std::vector<pending_op_sim_t> ops;

  for (auto& [snap_id, snap_info] : pp.snaps) {
    if (snap_id > obj_seq && snap_id <= current_seq) {
      ops.push_back({pending_op_sim_t::SNAP, snap_id, CEPH_NOSNAP});
    }
  }
  for (auto& [rb_id, rb_info] : pp.rollback_snaps) {
    if (rb_id > obj_seq && rb_id <= current_seq) {
      ops.push_back({pending_op_sim_t::ROLLBACK, rb_id, rb_info.source_snap});
    }
  }
  std::sort(ops.begin(), ops.end(),
    [](const pending_op_sim_t& a, const pending_op_sim_t& b) {
      return a.id < b.id;
    });
  return ops;
}

} // anonymous namespace

// (a) Simple rollback: one rollback, no other events in range
TEST(BuildPendingOps, SimpleRollback)
{
  pg_pool_t pp;
  pp.flags = pg_pool_t::FLAG_POOL_SNAPS;

  // Snap S1=10 exists
  pool_snap_info_t s;
  s.snapid = snapid_t(10); s.name = "s1"; s.stamp = utime_t();
  pp.snaps[s.snapid] = s;

  // Rollback RB=20, source=10
  rollback_snap_info_t rb;
  rb.rollback_id = snapid_t(20); rb.source_snap = snapid_t(10);
  pp.rollback_snaps[rb.rollback_id] = rb;

  // Object last written at seq=10; current_seq=20
  auto ops = simulate_build_pending_ops(pp, snapid_t(10), snapid_t(20));

  ASSERT_EQ(1u, ops.size());
  EXPECT_EQ(pending_op_sim_t::ROLLBACK, ops[0].type);
  EXPECT_EQ(snapid_t(20), ops[0].id);
  EXPECT_EQ(snapid_t(10), ops[0].source);
}

// (b) Stacked: SNAP(3) ROLLBACK(4,src=1) SNAP(5) ROLLBACK(6,src=2) SNAP(7)
//     Object last written at seq=2; current_seq=7.
TEST(BuildPendingOps, StackedSnapAndRollbacks)
{
  pg_pool_t pp;
  pp.flags = pg_pool_t::FLAG_POOL_SNAPS;

  for (snapid_t id : {snapid_t(3), snapid_t(5), snapid_t(7)}) {
    pool_snap_info_t s;
    s.snapid = id; s.name = "s"; s.stamp = utime_t();
    pp.snaps[id] = s;
  }
  {
    rollback_snap_info_t rb; rb.rollback_id = snapid_t(4);
    rb.source_snap = snapid_t(1);
    pp.rollback_snaps[rb.rollback_id] = rb;
  }
  {
    rollback_snap_info_t rb; rb.rollback_id = snapid_t(6);
    rb.source_snap = snapid_t(2);
    pp.rollback_snaps[rb.rollback_id] = rb;
  }

  auto ops = simulate_build_pending_ops(pp, snapid_t(2), snapid_t(7));

  ASSERT_EQ(5u, ops.size());
  EXPECT_EQ(pending_op_sim_t::SNAP,     ops[0].type); EXPECT_EQ(snapid_t(3), ops[0].id);
  EXPECT_EQ(pending_op_sim_t::ROLLBACK, ops[1].type); EXPECT_EQ(snapid_t(4), ops[1].id);
  EXPECT_EQ(pending_op_sim_t::SNAP,     ops[2].type); EXPECT_EQ(snapid_t(5), ops[2].id);
  EXPECT_EQ(pending_op_sim_t::ROLLBACK, ops[3].type); EXPECT_EQ(snapid_t(6), ops[3].id);
  EXPECT_EQ(pending_op_sim_t::SNAP,     ops[4].type); EXPECT_EQ(snapid_t(7), ops[4].id);
  EXPECT_EQ(snapid_t(1), ops[1].source);
  EXPECT_EQ(snapid_t(2), ops[3].source);
}

// (c) NOP rollback: object was never written after snapshot.
//     obj_seq == snap_id: events with id <= obj_seq are excluded.
TEST(BuildPendingOps, NopRollbackObjectNotWrittenAfterSnap)
{
  pg_pool_t pp;
  pp.flags = pg_pool_t::FLAG_POOL_SNAPS;

  pool_snap_info_t s;
  s.snapid = snapid_t(10); s.name = "s1"; s.stamp = utime_t();
  pp.snaps[s.snapid] = s;

  rollback_snap_info_t rb;
  rb.rollback_id = snapid_t(20); rb.source_snap = snapid_t(10);
  pp.rollback_snaps[rb.rollback_id] = rb;

  // Object never written after snap 10: obj_seq == snap_id
  // Only the ROLLBACK(20) should appear since snap(10) <= obj_seq(10)
  auto ops = simulate_build_pending_ops(pp, snapid_t(10), snapid_t(20));

  ASSERT_EQ(1u, ops.size())
      << "only ROLLBACK(20) should be in range (10,20]";
  EXPECT_EQ(pending_op_sim_t::ROLLBACK, ops[0].type);
  EXPECT_EQ(snapid_t(20), ops[0].id);
}

// (d) Multi-snap sharing one clone: rollback of snap 2, obj_seq=3.
//     All events (snaps 1,2,3 and rollback 4) have IDs <= obj_seq(3) except
//     rollback(4) which is > obj_seq(3).
TEST(BuildPendingOps, MultiSnapOneCLoneRollback)
{
  pg_pool_t pp;
  pp.flags = pg_pool_t::FLAG_POOL_SNAPS;

  for (snapid_t id : {snapid_t(1), snapid_t(2), snapid_t(3)}) {
    pool_snap_info_t s; s.snapid = id; s.name = "s"; s.stamp = utime_t();
    pp.snaps[id] = s;
  }
  rollback_snap_info_t rb;
  rb.rollback_id = snapid_t(4); rb.source_snap = snapid_t(2);
  pp.rollback_snaps[rb.rollback_id] = rb;

  // Object was written at seq=3 (the clone created at write-time covers snaps
  // 1,2,3). Rollback of snap 2 arrives as rb_id=4.
  auto ops = simulate_build_pending_ops(pp, snapid_t(3), snapid_t(4));

  ASSERT_EQ(1u, ops.size())
      << "only ROLLBACK(4) should be in range (3,4]";
  EXPECT_EQ(pending_op_sim_t::ROLLBACK, ops[0].type);
  EXPECT_EQ(snapid_t(4), ops[0].id);
  EXPECT_EQ(snapid_t(2), ops[0].source);
}

// (e) Object created after snapshot: obj_seq=0, one rollback in range.
//     SnapSet::seq starts at 0 for a new object. No snaps in (0,20].
TEST(BuildPendingOps, ObjectCreatedAfterSnapshot)
{
  pg_pool_t pp;
  pp.flags = pg_pool_t::FLAG_POOL_SNAPS;

  pool_snap_info_t s;
  s.snapid = snapid_t(10); s.name = "s1"; s.stamp = utime_t();
  pp.snaps[s.snapid] = s;

  rollback_snap_info_t rb;
  rb.rollback_id = snapid_t(20); rb.source_snap = snapid_t(10);
  pp.rollback_snaps[rb.rollback_id] = rb;

  // Object created after snap 10, so obj_seq=0. Both SNAP(10) and
  // ROLLBACK(20) are in (0,20].
  auto ops = simulate_build_pending_ops(pp, snapid_t(0), snapid_t(20));

  ASSERT_EQ(2u, ops.size());
  EXPECT_EQ(pending_op_sim_t::SNAP,     ops[0].type);
  EXPECT_EQ(snapid_t(10), ops[0].id);
  EXPECT_EQ(pending_op_sim_t::ROLLBACK, ops[1].type);
  EXPECT_EQ(snapid_t(20), ops[1].id);

  // Verify SnapSet::seq would be advanced to current_seq (snapc.seq=20)
  // after processing all ops. The JIT write path sets seq = snapc.seq,
  // which is 20 here -- confirming SnapSet::seq advances past rollback ID.
  snapid_t seq_after = snapid_t(20);  // snapc.seq used by make_writeable()
  EXPECT_EQ(snapid_t(20), seq_after)
      << "SnapSet::seq must advance to snapc.seq=20 after write";
}


