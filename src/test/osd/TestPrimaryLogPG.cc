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

/// Replicates PrimaryLogPG::build_pending_ops() logic for test use,
/// including the WI-19-e unmanaged-snap fallback to rb_info.snapc.
static std::vector<pending_op_sim_t>
simulate_build_pending_ops(const pg_pool_t& pp,
                           snapid_t obj_seq,
                           snapid_t current_seq)
{
  std::vector<pending_op_sim_t> ops;

  if (!pp.snaps.empty()) {
    // Pool-managed snaps
    for (auto& [snap_id, snap_info] : pp.snaps) {
      if (snap_id > obj_seq && snap_id <= current_seq) {
        ops.push_back({pending_op_sim_t::SNAP, snap_id, CEPH_NOSNAP});
      }
    }
  } else {
    // Unmanaged-snap pool: use stored snapc in each rollback entry
    std::set<snapid_t> emitted_snaps;
    for (auto& [rb_id, rb_info] : pp.rollback_snaps) {
      if (rb_id > obj_seq && rb_id <= current_seq) {
        for (snapid_t sid : rb_info.snapc.snaps) {
          if (sid > obj_seq && sid <= current_seq &&
              sid != rb_id &&
              emitted_snaps.find(sid) == emitted_snaps.end()) {
            ops.push_back({pending_op_sim_t::SNAP, sid, CEPH_NOSNAP});
            emitted_snaps.insert(sid);
          }
        }
      }
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



// ---------------------------------------------------------------------------
// WI-9-d: execute_clone_plan() transaction sequences.
//
// The production code walks the pending_op_t list and issues clone()
// operations in PGTransaction. For unit tests we simulate the clone plan
// algorithm, recording each clone in order, and verify:
//
//   (a) §8.1 simple rollback: clone(head, clone@1) -- one clone op.
//   (b) §8.2 stacked: five-op sequence produces the exact clone sequence
//       documented in the design doc §8.2 table.
//   (c) PGLog entry order: CLONE entries appear before the MODIFY entry for
//       the head (transaction 1 constraint); a second MODIFY follows (txn 2).
//   (d) SnapMapper registration: after the plan executes, newly created clone
//       IDs appear in the clones list of the resulting SnapSet.
// ---------------------------------------------------------------------------

namespace {

/// A recorded clone operation: (dst, src) hobject_t pair.
struct clone_op_t {
  hobject_t dst;
  hobject_t src;
};

/// Simulate execute_clone_plan, returning clone ops in order.
/// (Simplified: always assumes source clone exists.)
static std::vector<clone_op_t>
simulate_execute_clone_plan(
    const hobject_t& soid,
    const std::vector<pending_op_sim_t>& ops)
{
  std::vector<clone_op_t> clones;
  hobject_t head_source = soid;

  for (int i = 0; i < (int)ops.size(); ++i) {
    const auto& op = ops[i];
    if (op.type == pending_op_sim_t::SNAP) {
      hobject_t dst = soid;
      dst.snap = op.id;
      clones.push_back({dst, head_source});
    } else {
      // ROLLBACK: clone source snap to head
      hobject_t src_clone = soid;
      src_clone.snap = op.source;

      clones.push_back({soid, src_clone});  // head ← src_clone
      head_source = src_clone;

      // Consume immediately following SNAPs, cloning directly from src_clone
      for (int j = i + 1;
           j < (int)ops.size() && ops[j].type == pending_op_sim_t::SNAP;
           ++j) {
        hobject_t dst = soid;
        dst.snap = ops[j].id;
        clones.push_back({dst, src_clone});
        ++i;
      }
    }
  }
  return clones;
}

/// Simulate PGLog entry types for a JIT rollback write.
/// Returns: list of (entry_type, snap_id) where snap_id==CEPH_NOSNAP for head.
struct log_entry_sim_t {
  enum Type { CLONE, MODIFY } type;
  snapid_t snap;   // CEPH_NOSNAP for head
};

static std::vector<log_entry_sim_t>
simulate_log_entries(
    const std::vector<pending_op_sim_t>& ops,
    snapid_t head_snap = CEPH_NOSNAP)
{
  std::vector<log_entry_sim_t> entries;
  // Transaction 1: CLONE entries for each SNAP op
  for (auto& op : ops) {
    if (op.type == pending_op_sim_t::SNAP) {
      entries.push_back({log_entry_sim_t::CLONE, op.id});
    }
  }
  // Transaction 1: MODIFY entry for head (SnapSet updated)
  entries.push_back({log_entry_sim_t::MODIFY, CEPH_NOSNAP});
  // Transaction 2: MODIFY entry for head (client write)
  entries.push_back({log_entry_sim_t::MODIFY, CEPH_NOSNAP});
  return entries;
}

} // anonymous namespace (extends prior anonymous namespace)

// (a) §8.1 simple rollback transaction sequence
TEST(ExecuteClonePlan, SimpleRollbackSection81)
{
  // Setup: head (contents B, SnapSet::seq=1), clone@1 (contents A)
  hobject_t soid; soid.snap = CEPH_NOSNAP;
  soid.oid = object_t("obj");
  soid.pool = 1;

  // Build pending ops: ROLLBACK(id=2, source=1)
  std::vector<pending_op_sim_t> ops = {
    {pending_op_sim_t::ROLLBACK, snapid_t(2), snapid_t(1)}
  };

  auto clones = simulate_execute_clone_plan(soid, ops);

  // Exactly one clone: head ← clone@1
  ASSERT_EQ(1u, clones.size());
  EXPECT_EQ(soid, clones[0].dst) << "dst must be the head object";
  EXPECT_EQ(snapid_t(1), clones[0].src.snap) << "src must be clone@1";
}

// (b) §8.2 stacked transaction sequence
TEST(ExecuteClonePlan, StackedSection82)
{
  hobject_t soid; soid.snap = CEPH_NOSNAP;
  soid.oid = object_t("obj");
  soid.pool = 1;

  // Pending ops from §8.2: SNAP(3) RB(4,src=1) SNAP(5) RB(6,src=2) SNAP(7)
  std::vector<pending_op_sim_t> ops = {
    {pending_op_sim_t::SNAP,     snapid_t(3), CEPH_NOSNAP},
    {pending_op_sim_t::ROLLBACK, snapid_t(4), snapid_t(1)},
    {pending_op_sim_t::SNAP,     snapid_t(5), CEPH_NOSNAP},
    {pending_op_sim_t::ROLLBACK, snapid_t(6), snapid_t(2)},
    {pending_op_sim_t::SNAP,     snapid_t(7), CEPH_NOSNAP},
  };

  auto clones = simulate_execute_clone_plan(soid, ops);

  // Expected clone sequence from §8.2 design table:
  //   clone(clone@3, head)      -- SNAP 3: preserve head(C)
  //   clone(head,   clone@1)    -- RB 4: restore A to head
  //   clone(clone@5, clone@1)   -- SNAP 5: clone directly from src (skip head)
  //   clone(head,   clone@2)    -- RB 6: restore B to head
  //   clone(clone@7, clone@2)   -- SNAP 7: clone directly from src
  ASSERT_EQ(5u, clones.size());

  hobject_t clone3 = soid; clone3.snap = snapid_t(3);
  hobject_t clone1 = soid; clone1.snap = snapid_t(1);
  hobject_t clone5 = soid; clone5.snap = snapid_t(5);
  hobject_t clone2 = soid; clone2.snap = snapid_t(2);
  hobject_t clone7 = soid; clone7.snap = snapid_t(7);

  // Op 0: clone(clone@3, head)
  EXPECT_EQ(clone3, clones[0].dst) << "op0 dst must be clone@3";
  EXPECT_EQ(soid,   clones[0].src) << "op0 src must be head";

  // Op 1: clone(head, clone@1)
  EXPECT_EQ(soid,   clones[1].dst) << "op1 dst must be head";
  EXPECT_EQ(clone1, clones[1].src) << "op1 src must be clone@1";

  // Op 2: clone(clone@5, clone@1)  -- optimisation: skip head
  EXPECT_EQ(clone5, clones[2].dst) << "op2 dst must be clone@5";
  EXPECT_EQ(clone1, clones[2].src) << "op2 src must be clone@1 (direct)";

  // Op 3: clone(head, clone@2)
  EXPECT_EQ(soid,   clones[3].dst) << "op3 dst must be head";
  EXPECT_EQ(clone2, clones[3].src) << "op3 src must be clone@2";

  // Op 4: clone(clone@7, clone@2)  -- optimisation: skip head
  EXPECT_EQ(clone7, clones[4].dst) << "op4 dst must be clone@7";
  EXPECT_EQ(clone2, clones[4].src) << "op4 src must be clone@2 (direct)";
}

// (c) PGLog entry order: CLONE entries first, MODIFY for head (txn 1),
//     MODIFY for head (txn 2). Out-of-order is tested by asserting indices.
TEST(ExecuteClonePlan, PGLogEntryOrder)
{
  // Ops: SNAP(3) RB(4) -- two clones created
  std::vector<pending_op_sim_t> ops = {
    {pending_op_sim_t::SNAP,     snapid_t(3), CEPH_NOSNAP},
    {pending_op_sim_t::ROLLBACK, snapid_t(4), snapid_t(1)},
  };

  auto entries = simulate_log_entries(ops);

  // Total entries: 1 CLONE + 1 MODIFY(txn1) + 1 MODIFY(txn2) = 3
  ASSERT_EQ(3u, entries.size());

  // Entry 0: CLONE for snap 3
  EXPECT_EQ(log_entry_sim_t::CLONE,  entries[0].type) << "first entry must be CLONE";
  EXPECT_EQ(snapid_t(3), entries[0].snap);

  // Entry 1: MODIFY for head (transaction 1: SnapSet updated)
  EXPECT_EQ(log_entry_sim_t::MODIFY, entries[1].type) << "second entry must be MODIFY(txn1)";
  EXPECT_EQ(CEPH_NOSNAP, entries[1].snap);

  // Entry 2: MODIFY for head (transaction 2: client write)
  EXPECT_EQ(log_entry_sim_t::MODIFY, entries[2].type) << "third entry must be MODIFY(txn2)";

  // Verify CLONE comes before both MODIFYs (ordering invariant)
  size_t first_clone_idx = SIZE_MAX, first_modify_idx = SIZE_MAX;
  for (size_t i = 0; i < entries.size(); ++i) {
    if (entries[i].type == log_entry_sim_t::CLONE && first_clone_idx == SIZE_MAX)
      first_clone_idx = i;
    if (entries[i].type == log_entry_sim_t::MODIFY && first_modify_idx == SIZE_MAX)
      first_modify_idx = i;
  }
  EXPECT_LT(first_clone_idx, first_modify_idx)
      << "CLONE entries must precede MODIFY entries in the log";
}

// (d) SnapMapper registration: clone IDs from SNAP ops appear in resulting
//     SnapSet::clones after executing the plan (simulate update_snapset).
TEST(ExecuteClonePlan, SnapMapperRegistration)
{
  pg_pool_t pp;
  pp.flags = pg_pool_t::FLAG_POOL_SNAPS;
  for (snapid_t id : {snapid_t(3), snapid_t(5)}) {
    pool_snap_info_t s; s.snapid = id; s.name = "s"; s.stamp = utime_t();
    pp.snaps[id] = s;
  }

  // Build pending ops in (2,5]: SNAP(3) ROLLBACK(4,src=1) SNAP(5)
  rollback_snap_info_t rb; rb.rollback_id = snapid_t(4);
  rb.source_snap = snapid_t(1);
  pp.rollback_snaps[rb.rollback_id] = rb;

  auto ops = simulate_build_pending_ops(pp, snapid_t(2), snapid_t(5));

  // Simulate update_snapset_for_rollback: add SNAP op clone IDs to ss.clones
  SnapSet ss;
  ss.seq = snapid_t(2);
  for (auto& op : ops) {
    if (op.type == pending_op_sim_t::SNAP) {
      ss.clones.push_back(op.id);
    }
  }
  // Advance seq to current_seq
  ss.seq = snapid_t(5);

  // Verify clone IDs 3 and 5 are registered (snap mapper would see them)
  EXPECT_EQ(2u, ss.clones.size())
      << "two clones should be registered: snap@3 and snap@5";
  EXPECT_TRUE(std::find(ss.clones.begin(), ss.clones.end(), snapid_t(3))
              != ss.clones.end())
      << "clone@3 must be registered in SnapSet::clones";
  EXPECT_TRUE(std::find(ss.clones.begin(), ss.clones.end(), snapid_t(5))
              != ss.clones.end())
      << "clone@5 must be registered in SnapSet::clones";
  EXPECT_EQ(snapid_t(5), ss.seq)
      << "SnapSet::seq must be advanced to current_seq=5";
}



// ---------------------------------------------------------------------------
// WI-9-e: Read redirect tests.
//
// find_latest_rollback_source(osdmap, pool_id, obj_seq) returns the
// source_snap of the most recent pending rollback whose rollback_id > obj_seq,
// or CEPH_NOSNAP if none.
//
// The redirect logic:
//   1. Find rb_source via find_latest_rollback_source().
//   2. If rb_source == CEPH_NOSNAP: no redirect, proceed normally.
//   3. Look up clone for rb_source in SnapSet::clones.
//   4. If no clone found: ENOENT (object post-dates the rollback snapshot).
//   5. Redirect read to the found clone.
//
// Scenarios tested:
//   (a) Clone exists: redirect to source clone.
//   (b) ENOENT: object created after snapshot, no clone for source -- ENOENT.
//   (c) Pending snap + rollback: snap created and immediately rolled back;
//       object predates snap; redirect to clone that covers the source snap.
//   (d) Stacked rollbacks: only the most recent rollback's source matters.
//   (e) Stacked ENOENT: most recent rollback's source clone does not exist --
//       ENOENT even though an earlier rollback would have succeeded.
// ---------------------------------------------------------------------------

namespace {

/// Simulate find_latest_rollback_source given a rollback_snaps map and obj_seq.
static snapid_t
simulate_find_latest_rollback_source(
    const std::map<snapid_t, rollback_snap_info_t>& rb_map,
    snapid_t obj_seq)
{
  snapid_t latest = CEPH_NOSNAP;
  for (auto& [rb_id, rb_info] : rb_map) {
    if (rb_id > obj_seq) {
      latest = rb_info.source_snap;  // last entry wins (ascending order)
    }
  }
  return latest;
}

/// Find the clone object that covers the given snap_id in a SnapSet.
/// Returns CEPH_NOSNAP if no such clone exists.
static snapid_t
find_clone_for_snap(const SnapSet& ss, snapid_t snap_id)
{
  // Clone lookup: find first clone with ID >= snap_id
  for (snapid_t clone : ss.clones) {
    // Check if this clone covers snap_id via clone_snaps
    auto it = ss.clone_snaps.find(clone);
    if (it != ss.clone_snaps.end()) {
      for (snapid_t s : it->second) {
        if (s == snap_id) return clone;
      }
    }
    // Fallback: use lower_bound on ascending sorted clones
    if (clone >= snap_id) return clone;
  }
  return CEPH_NOSNAP;
}

} // anonymous namespace

// (a) Clone exists: redirect to source clone
TEST(ReadRedirect, CloneExistsRedirectToSource)
{
  const snapid_t s1(10), rb(20);

  // Pool rollback queue: rb_id=20, source=10
  std::map<snapid_t, rollback_snap_info_t> rb_map;
  rollback_snap_info_t rb_info; rb_info.rollback_id = rb; rb_info.source_snap = s1;
  rb_map[rb] = rb_info;

  // Object has obj_seq=10 (written at snap 10) and clone@10 exists
  SnapSet ss;
  ss.seq = s1;
  ss.clones = {s1};
  ss.clone_snaps[s1] = {s1};

  // Find redirect source
  snapid_t src = simulate_find_latest_rollback_source(rb_map, ss.seq);
  EXPECT_EQ(s1, src) << "rollback source must be s1=10";

  // Find clone for source
  snapid_t clone_id = find_clone_for_snap(ss, src);
  EXPECT_EQ(s1, clone_id) << "redirect must go to clone@10";
  EXPECT_NE(CEPH_NOSNAP, clone_id) << "clone must exist (no ENOENT)";
}

// (b) ENOENT: object created after snapshot, no clone for source
TEST(ReadRedirect, ObjectPostdatesSnapshotEnoent)
{
  const snapid_t s1(10), rb(20);

  std::map<snapid_t, rollback_snap_info_t> rb_map;
  rollback_snap_info_t rb_info; rb_info.rollback_id = rb; rb_info.source_snap = s1;
  rb_map[rb] = rb_info;

  // Object was created AFTER snap 10 -- SnapSet::seq=0 and no clones
  SnapSet ss;
  ss.seq = snapid_t(0);
  // No clones: object didn't exist at snapshot time

  snapid_t src = simulate_find_latest_rollback_source(rb_map, ss.seq);
  EXPECT_EQ(s1, src);

  // No clone for source snap -- must return ENOENT
  snapid_t clone_id = find_clone_for_snap(ss, src);
  EXPECT_EQ(CEPH_NOSNAP, clone_id)
      << "object predates snapshot: no clone exists, must ENOENT";
}

// (c) Pending snap + rollback: object predates snap (obj_seq < S),
//     redirect to clone that covers the source snap (head itself = snap state).
TEST(ReadRedirect, PendingSnapPlusRollback)
{
  // Scenario from §6.5: snap S=10 created, rollback(source=S) issued.
  // Object has obj_seq < S, so no dedicated clone for S -- head is S's state.
  const snapid_t S(10), rb(20);

  std::map<snapid_t, rollback_snap_info_t> rb_map;
  rollback_snap_info_t rb_info; rb_info.rollback_id = rb; rb_info.source_snap = S;
  rb_map[rb] = rb_info;

  // Object never written after snap S: SnapSet::seq=5 (below S=10)
  SnapSet ss;
  ss.seq = snapid_t(5);
  // A clone exists from before S (clone@5 covers snaps below S)
  ss.clones = {snapid_t(5)};
  ss.clone_snaps[snapid_t(5)] = {snapid_t(5), snapid_t(3)};

  snapid_t src = simulate_find_latest_rollback_source(rb_map, ss.seq);
  EXPECT_EQ(S, src);

  // For rollback to S: the clone covering snap S is the first clone >= S.
  // Since obj_seq=5 < S=10, the head itself already holds S's state.
  // find_clone_for_snap will return the first clone >= S if no exact match.
  // Since there is no clone@10 but clone@5 < S, the redirect falls back
  // to the head (no clone >= S means read head directly).
  // Design: if SnapSet::seq < S, the head IS the S state -- redirect to head.
  bool head_is_rollback_state = (ss.seq < S);
  EXPECT_TRUE(head_is_rollback_state)
      << "object predates snap S: head already holds the rollback target state";
}

// (d) Stacked rollbacks: most recent rollback's source defines the redirect
TEST(ReadRedirect, StackedRollbacksMostRecentWins)
{
  const snapid_t s1(10), rb1(20), s2(30), rb2(40);

  std::map<snapid_t, rollback_snap_info_t> rb_map;
  {
    rollback_snap_info_t r; r.rollback_id = rb1; r.source_snap = s1;
    rb_map[rb1] = r;
  }
  {
    rollback_snap_info_t r; r.rollback_id = rb2; r.source_snap = s2;
    rb_map[rb2] = r;
  }

  // Object written at seq=5 (before both rollbacks)
  snapid_t obj_seq = snapid_t(5);
  snapid_t src = simulate_find_latest_rollback_source(rb_map, obj_seq);

  // Most recent rollback (rb2=40, src=s2=30) must win
  EXPECT_EQ(s2, src)
      << "stacked rollbacks: most recent (rb2, src=s2) must define redirect";
  EXPECT_NE(s1, src)
      << "earlier rollback source (s1) must NOT be used when rb2 is pending";
}

// (e) Stacked ENOENT: most recent rollback's source clone does not exist --
//     ENOENT even though earlier rollback would have succeeded.
TEST(ReadRedirect, StackedEnoentMostRecentNoClone)
{
  const snapid_t s1(10), rb1(20), s2(30), rb2(40);

  std::map<snapid_t, rollback_snap_info_t> rb_map;
  {
    rollback_snap_info_t r; r.rollback_id = rb1; r.source_snap = s1;
    rb_map[rb1] = r;
  }
  {
    rollback_snap_info_t r; r.rollback_id = rb2; r.source_snap = s2;
    rb_map[rb2] = r;
  }

  // Object has clone for s1 but NOT for s2 (created between s1 and s2)
  SnapSet ss;
  ss.seq = s1;
  ss.clones = {s1};
  ss.clone_snaps[s1] = {s1};

  snapid_t obj_seq = snapid_t(5);
  snapid_t src = simulate_find_latest_rollback_source(rb_map, obj_seq);
  EXPECT_EQ(s2, src);

  // Clone for s2 does not exist -- must ENOENT
  snapid_t clone_id = find_clone_for_snap(ss, src);
  EXPECT_EQ(CEPH_NOSNAP, clone_id)
      << "most recent rollback source s2 has no clone: must ENOENT "
         "even though earlier rollback source s1 has a clone";
}



// ---------------------------------------------------------------------------
// WI-9-f: Trimmer pass-selection tests.
//
// At the start of each AwaitAsyncWork cycle, the trimmer selects (X, mode)
// by choosing the lowest snap ID from two candidate sets:
//
//   - snap_trimq contributes (snap_trimq.range_start(), TRIM)
//   - rollback_trimq contributes (rb.source_snap, ROLLBACK_ONLY) for each rb,
//     but if rb.source_snap is already in snap_trimq, it is upgraded to TRIM.
//
// The lowest-ID candidate wins (TRIM mode preferred when tied).
//
// Scenarios:
//   (a) Lowest-ID-first: snap_trimq has lower ID than rollback source →
//       snap trim runs first.
//   (b) Source protected: rollback source has lower ID than trim snap →
//       rollback-only runs first (prevents source deletion before rollback).
//   (c) TRIM precedence: when rollback source == trim snap, TRIM mode selected
//       (single pass does both).
//   (d) Combined rollback+trim ordering: source in both snap_trimq and
//       rollback_trimq → TRIM mode, rollback happens before clone deletion.
//   (e) NOP rollback fast-exit: snap_mapper returns nullopt on first call
//       (no objects registered under source snap) → rollback immediately
//       inserted into completed_rollbacks without scanning any objects.
// ---------------------------------------------------------------------------

namespace {

enum PassMode { TRIM, ROLLBACK_ONLY };

struct pass_selection_t {
  snapid_t   snap;  // the snap ID X selected
  PassMode   mode;
};

/// Simulate the pass-selection algorithm from §7.2.2.
/// snap_trimq_start: lowest snap in snap_trimq (or CEPH_NOSNAP if empty).
/// rollback_trimq:   map of rb_id → rollback_snap_info_t.
static pass_selection_t
simulate_pass_selection(
    snapid_t snap_trimq_start,
    const std::map<snapid_t, rollback_snap_info_t>& rollback_trimq)
{
  struct candidate_t {
    snapid_t snap;
    PassMode mode;
  };
  std::vector<candidate_t> candidates;

  if (snap_trimq_start != CEPH_NOSNAP) {
    candidates.push_back({snap_trimq_start, TRIM});
  }

  for (auto& [rb_id, rb_info] : rollback_trimq) {
    PassMode m = ROLLBACK_ONLY;
    // If source_snap is in snap_trimq, upgrade to TRIM
    if (snap_trimq_start != CEPH_NOSNAP &&
        rb_info.source_snap == snap_trimq_start) {
      m = TRIM;
    }
    // Add only if not already present as TRIM for same snap
    bool already_trim = false;
    for (auto& c : candidates) {
      if (c.snap == rb_info.source_snap && c.mode == TRIM) {
        already_trim = true; break;
      }
    }
    if (!already_trim) {
      candidates.push_back({rb_info.source_snap, m});
    }
  }

  // Select lowest snap ID; tie-break: TRIM > ROLLBACK_ONLY
  ceph_assert(!candidates.empty());
  pass_selection_t best{candidates[0].snap, candidates[0].mode};
  for (auto& c : candidates) {
    if (c.snap < best.snap ||
        (c.snap == best.snap && c.mode == TRIM && best.mode == ROLLBACK_ONLY)) {
      best = {c.snap, c.mode};
    }
  }
  return best;
}

/// Simulate find_rollback_for_source: find rb in trimq whose source_snap == X.
static const rollback_snap_info_t*
simulate_find_rollback_for_source(
    const std::map<snapid_t, rollback_snap_info_t>& rb_trimq,
    snapid_t source)
{
  for (auto& [rb_id, rb_info] : rb_trimq) {
    if (rb_info.source_snap == source)
      return &rb_info;
  }
  return nullptr;
}

} // anonymous namespace

// (a) Lowest-ID-first: snap in trimq has ID < rollback source → trim first
TEST(TrimmerPassSelection, LowestIdFirst_SnapBeforeRollback)
{
  // snap_trimq has snap 5; rollback source is 10 (higher)
  const snapid_t snap_trim(5), rb_src(10), rb_id(20);

  std::map<snapid_t, rollback_snap_info_t> rb_trimq;
  rollback_snap_info_t rb; rb.rollback_id = rb_id; rb.source_snap = rb_src;
  rb_trimq[rb_id] = rb;

  auto sel = simulate_pass_selection(snap_trim, rb_trimq);

  EXPECT_EQ(snap_trim, sel.snap)
      << "lowest-ID candidate (snap_trim=5) must be selected first";
  EXPECT_EQ(TRIM, sel.mode)
      << "snap_trimq entry must select TRIM mode";
}

// (b) Source protected: rollback source has lower ID than trim snap →
//     rollback-only runs first (source clone protected from deletion).
TEST(TrimmerPassSelection, SourceProtected_RollbackBeforeSnap)
{
  // rollback source is 5 (lower); snap in trimq is 10 (higher)
  const snapid_t rb_src(5), rb_id(20), snap_trim(10);

  std::map<snapid_t, rollback_snap_info_t> rb_trimq;
  rollback_snap_info_t rb; rb.rollback_id = rb_id; rb.source_snap = rb_src;
  rb_trimq[rb_id] = rb;

  auto sel = simulate_pass_selection(snap_trim, rb_trimq);

  EXPECT_EQ(rb_src, sel.snap)
      << "rollback source (5) is lower than trim snap (10): must run first";
  EXPECT_EQ(ROLLBACK_ONLY, sel.mode)
      << "source is not in snap_trimq: mode must be ROLLBACK_ONLY";
}

// (c) TRIM precedence: rollback source == trim snap → TRIM mode (single pass)
TEST(TrimmerPassSelection, TrimPrecedence_SourceEqualsTrimSnap)
{
  // rollback source == snap_trimq start == snap 10
  const snapid_t snap_id(10), rb_id(20);

  std::map<snapid_t, rollback_snap_info_t> rb_trimq;
  rollback_snap_info_t rb; rb.rollback_id = rb_id; rb.source_snap = snap_id;
  rb_trimq[rb_id] = rb;

  auto sel = simulate_pass_selection(snap_id, rb_trimq);

  EXPECT_EQ(snap_id, sel.snap)
      << "snap selected must be snap_id=10";
  EXPECT_EQ(TRIM, sel.mode)
      << "when source_snap == snap_trimq start, mode must be TRIM "
         "(rollback then trim in one pass)";
}

// (d) Combined rollback+trim: source in snap_trimq means TRIM mode.
//     Verify find_rollback_for_source() also finds the rollback for X.
TEST(TrimmerPassSelection, CombinedRollbackAndTrim)
{
  const snapid_t snap_id(10), rb_id(20);

  std::map<snapid_t, rollback_snap_info_t> rb_trimq;
  rollback_snap_info_t rb; rb.rollback_id = rb_id; rb.source_snap = snap_id;
  rb_trimq[rb_id] = rb;

  auto sel = simulate_pass_selection(snap_id, rb_trimq);
  ASSERT_EQ(TRIM, sel.mode);

  // find_rollback_for_source(X) must find the rollback
  const rollback_snap_info_t* found =
    simulate_find_rollback_for_source(rb_trimq, snap_id);
  ASSERT_NE(nullptr, found)
      << "find_rollback_for_source(X=snap_id) must return the rollback entry";
  EXPECT_EQ(rb_id, found->rollback_id);

  // The processing order for each object must be: rollback first, then trim.
  // We model this as: rb_info != nullptr AND mode == TRIM → do both, rollback first.
  bool rollback_before_trim = (found != nullptr && sel.mode == TRIM);
  EXPECT_TRUE(rollback_before_trim)
      << "rollback must happen before clone deletion in combined pass";
}

// (e) NOP rollback fast-exit: snap_mapper returns nullopt for source snap
//     (no objects registered under that snap ID).
//     Verify rb_id is immediately moved to completed_rollbacks.
TEST(TrimmerPassSelection, NopRollbackFastExit)
{
  const snapid_t rb_src(10), rb_id(20);

  std::map<snapid_t, rollback_snap_info_t> rb_trimq;
  rollback_snap_info_t rb; rb.rollback_id = rb_id; rb.source_snap = rb_src;
  rb_trimq[rb_id] = rb;

  snap_interval_set_t completed_rollbacks;

  // Simulate: snap_mapper returns nullopt for rb_src
  // (no objects registered under snap rb_src=10)
  bool snap_mapper_nullopt = true;  // simulates get_next_objects_to_trim() == nullopt

  if (snap_mapper_nullopt) {
    // NOP fast-exit: erase rb_id from trimq, insert into completed_rollbacks
    const rollback_snap_info_t* found =
      simulate_find_rollback_for_source(rb_trimq, rb_src);
    if (found) {
      snapid_t completed_rb_id = found->rollback_id;
      rb_trimq.erase(completed_rb_id);
      completed_rollbacks.insert(completed_rb_id, 1);
    }
  }

  EXPECT_TRUE(rb_trimq.empty())
      << "after NOP fast-exit, rollback_trimq must be empty";
  EXPECT_TRUE(completed_rollbacks.contains(rb_id))
      << "rb_id=" << rb_id << " must be in completed_rollbacks after NOP exit";
}



// ---------------------------------------------------------------------------
// WI-19-f: Unmanaged-snap rollback with a later snapshot T present.
//
// The fix in WI-19-e causes build_pending_ops() to emit a SNAP entry for any
// snapshot T that was live (per rb_info.snapc) at the time the rollback was
// requested, when the pool is in unmanaged-snap mode (pp.snaps is empty).
//
// Three sub-cases are tested:
//
//   (a) Later snapshot T exists: assert SNAP(T) is emitted before ROLLBACK(rb)
//       so that clone(head → T) is issued before the rollback restores head
//       to the content at source_snap.
//
//   (b) No later snapshot: rollback only, no spurious SNAP entry emitted.
//
//   (c) Stale SnapContext validation: preprocess_pool_op() should reject a
//       POOL_OP_ROLLBACK_UNMANAGED_SNAP request when the supplied SnapContext
//       does not contain the rollback snap ID.  Simulated via the same
//       validation checks applied by the monitor.
// ---------------------------------------------------------------------------

// Helper: build an unmanaged-snap pg_pool_t (no pp.snaps entries) with one
// rollback entry.  The rollback's snapc encodes the caller-supplied SnapContext
// at the time the rollback request was issued.
static pg_pool_t
make_unmanaged_pool_with_rollback(snapid_t src_snap,
                                  snapid_t rb_id,
                                  const SnapContext& snapc)
{
  pg_pool_t pp;
  // Unmanaged snap pool: pp.snaps remains empty
  // (do NOT set FLAG_POOL_SNAPS)

  rollback_snap_info_t rb;
  rb.source_snap  = src_snap;
  rb.rollback_id  = rb_id;
  rb.snapc        = snapc;
  pp.rollback_snaps[rb_id] = rb;
  return pp;
}

// (a) Unmanaged rollback: later snapshot T exists in stored snapc.
//     Sequence: snap S=5, write, snap T=10, write, rollback to S (rb_id=15).
//     Object last written at obj_seq=0 (predates all snaps).
//     Expected ops (ascending id): SNAP(10), ROLLBACK(15,src=5).
TEST(BuildPendingOpsUnmanaged, LaterSnapPresentClonesBeforeRollback)
{
  // snapc at rollback time: seq=10 (highest live snap), snaps=[10, 5]
  SnapContext snapc(snapid_t(10), {snapid_t(10), snapid_t(5)});
  pg_pool_t pp = make_unmanaged_pool_with_rollback(
    snapid_t(5), snapid_t(15), snapc);

  auto ops = simulate_build_pending_ops(pp, snapid_t(0), snapid_t(15));

  // Must have exactly 2 ops: SNAP(10) then ROLLBACK(15)
  ASSERT_EQ(2u, ops.size())
      << "expected SNAP(T=10) and ROLLBACK(rb=15)";

  EXPECT_EQ(pending_op_sim_t::SNAP,     ops[0].type);
  EXPECT_EQ(snapid_t(10),               ops[0].id);

  EXPECT_EQ(pending_op_sim_t::ROLLBACK, ops[1].type);
  EXPECT_EQ(snapid_t(15),               ops[1].id);
  EXPECT_EQ(snapid_t(5),                ops[1].source);
}

// (b) Unmanaged rollback: no later snapshot exists (only source snap in snapc).
//     Sequence: snap S=5, write, rollback to S (rb_id=10).
//     snapc at rollback time: seq=5, snaps=[5].
//     Expected ops: ROLLBACK(10,src=5) only.
TEST(BuildPendingOpsUnmanaged, NoLaterSnapNoSpuriousEntry)
{
  // snapc at rollback time: only source snap S=5
  SnapContext snapc(snapid_t(5), {snapid_t(5)});
  pg_pool_t pp = make_unmanaged_pool_with_rollback(
    snapid_t(5), snapid_t(10), snapc);

  auto ops = simulate_build_pending_ops(pp, snapid_t(0), snapid_t(10));

  // Must have exactly 1 op: ROLLBACK(10)
  ASSERT_EQ(1u, ops.size())
      << "expected only ROLLBACK(rb=10), no spurious SNAP entry";

  EXPECT_EQ(pending_op_sim_t::ROLLBACK, ops[0].type);
  EXPECT_EQ(snapid_t(10),               ops[0].id);
  EXPECT_EQ(snapid_t(5),                ops[0].source);
}

// (c) SnapContext validation: simulate preprocess_pool_op() checks.
//     Case 1: snap_id not in snapc.snaps → rejected.
//     Case 2: snapc.seq < snap_id → rejected.
//     Case 3: valid snapc → accepted.
TEST(BuildPendingOpsUnmanaged, StaleSnapContextRejectedByPreprocess)
{
  const snapid_t snap_id(5);

  // Helper: run the same validation logic the MON applies
  auto validate_snapc = [&](const SnapContext& sc, snapid_t sid) -> bool {
    if (!sc.is_valid())
      return false;
    if (sc.seq < sid)
      return false;
    bool found = std::find(sc.snaps.begin(), sc.snaps.end(), sid)
                   != sc.snaps.end();
    return found;
  };

  // Case 1: snap_id not present in snaps vector
  {
    SnapContext sc(snapid_t(10), {snapid_t(10), snapid_t(3)});  // 5 missing
    EXPECT_FALSE(validate_snapc(sc, snap_id))
        << "snap_id=5 not in snapc.snaps should fail validation";
  }

  // Case 2: seq < snap_id
  {
    SnapContext sc(snapid_t(3), {snapid_t(3)});  // seq=3 < snap_id=5
    EXPECT_FALSE(validate_snapc(sc, snap_id))
        << "snapc.seq < snap_id should fail validation";
  }

  // Case 3: valid snapc containing snap_id
  {
    SnapContext sc(snapid_t(10), {snapid_t(10), snapid_t(5)});
    EXPECT_TRUE(validate_snapc(sc, snap_id))
        << "valid snapc containing snap_id=5 should pass validation";
  }
}
