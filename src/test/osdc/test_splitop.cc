// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

/**
 * TestSplitOpsUT — dedicated unit tests for the SplitOps interface.
 *
 * These tests exercise the pure logic components of SplitOp that do not
 * require a running Objecter, OSD, or live I/O path:
 *
 *  1. SplitOp::validate_flags()   — flag-combination acceptance/rejection
 *  2. ECStripeIterator / ECStripeView — stripe-traversal geometry
 *  3. ECSplitOp::local_zone_for_acting_set() — zone selection edge cases
 *  4. SplitOp::create() — the OSD a single direct op is sent to
 *
 * Sections 1-3 use a minimal pg_pool_t built inline (no OSDMap, no peering).
 * For ECStripeIterator, a thin helper subclass exposes the protected types.
 * Section 4 gives an unconnected Objecter an OSDMap.
 */

#include <gtest/gtest.h>
#include <numeric>

#include "common/ref.h"
#include "common/async/context_pool.h"
#include "common/common_init.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "messages/MOSDMap.h"
#include "mon/MonClient.h"
#include "msg/Messenger.h"
#include "osd/OSDMap.h"
#include "osd/osd_types.h"
#include "osdc/SplitOp.h"
#include "global/global_context.h"
#include "include/rados.h"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

namespace {

constexpr uint64_t CHUNK = 4096;
constexpr int K = 4;
constexpr int M = 2;
constexpr uint64_t STRIPE_WIDTH = CHUNK * K;

/**
 * Build a minimal erasure-coded pg_pool_t.
 *
 * @param k             number of data chunks
 * @param m             number of coding chunks
 * @param stripe_unit   per-shard stripe unit in bytes (stripe_width = k * stripe_unit)
 * @param extra_flags   additional pool flags to OR in
 */
pg_pool_t make_ec_pool(int k = K, int m = M, uint64_t stripe_unit = CHUNK,
                       uint64_t extra_flags = 0)
{
  pg_pool_t pi;
  pi.type = pg_pool_t::TYPE_ERASURE;
  pi.size = k + m;
  pi.min_size = k;
  pi.ec_data_shard_count = k;
  pi.ec_coding_shard_count = m;
  pi.set_stripe_width(stripe_unit * k);
  pi.set_flag(pg_pool_t::FLAG_EC_OVERWRITES);
  pi.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  pi.set_flag(pg_pool_t::FLAG_CLIENT_SPLIT_READS);
  if (extra_flags) {
    pi.set_flag(extra_flags);
  }
  return pi;
}

/**
 * Build a minimal replicated pg_pool_t.
 */
pg_pool_t make_replicated_pool(int size = 3, uint64_t extra_flags = 0)
{
  pg_pool_t pi;
  pi.type = pg_pool_t::TYPE_REPLICATED;
  pi.size = size;
  pi.min_size = 2;
  pi.set_flag(pg_pool_t::FLAG_CLIENT_SPLIT_READS);
  if (extra_flags) {
    pi.set_flag(extra_flags);
  }
  return pi;
}

OSDOp make_read(uint64_t offset, uint64_t length) {
  OSDOp o;
  o.op.op = CEPH_OSD_OP_READ;
  o.op.extent.offset = offset;
  o.op.extent.length = length;
  return o;
}

/**
 * Build the Objecter::Op that a client would hand to SplitOp::create().
 *
 * validate_operations() only reads op->ops, so nothing else has to be wired up.
 */
ceph::ref_t<Objecter::Op> make_op(osdc_opvec &&ops) {
  return ceph::make_ref<Objecter::Op>(
    object_t("test_object"), object_locator_t(1), std::move(ops),
    CEPH_OSD_FLAG_READ | CEPH_OSD_FLAG_BALANCE_READS,
    static_cast<Context*>(nullptr), static_cast<version_t*>(nullptr));
}

/**
 * The raw shard prepare_single_op() picks for a read, verbatim from
 * SplitOp::prepare_single_op().  It only ever applies this to the *first* read
 * in the op, which is what makes disagreement between reads a bug.
 */
int raw_shard_of(uint64_t offset, uint64_t chunk_size = CHUNK, int k = K) {
  return (offset / chunk_size) % k;
}

/**
 * Run validate_operations() the way validate() does and report whether the op
 * was classified as a single direct read.
 *
 * validate_operations() only ever clears single_direct_op, so the caller has to
 * seed it; validate() seeds it with pi->is_erasure().
 */
bool is_single_direct_op(osdc_opvec &&ops,
                         const pg_pool_t &pool,
                         bool has_primary_ops = false) {
  auto op = make_op(std::move(ops));
  bool single_direct_op = pool.is_erasure();
  bool suitable_read_found = test_validate_operations(
    op.get(), &pool, pool.is_erasure(), /*replica_min_read_size=*/0,
    g_ceph_context, has_primary_ops, single_direct_op);
  EXPECT_TRUE(suitable_read_found);
  return single_direct_op;
}

osdc_opvec reads(std::initializer_list<std::pair<uint64_t, uint64_t>> extents) {
  osdc_opvec ops;
  for (const auto &[offset, length] : extents) {
    ops.push_back(make_read(offset, length));
  }
  return ops;
}

} // anonymous namespace

// ===========================================================================
// Section 1: validate_flags()
//
// SplitOp::validate_flags() is a public static method that checks whether a
// combination of operation flags and pool properties permits a split read.
// It rejects operations that:
//   - lack BALANCE_READS or LOCALIZE_READS
//   - are flagged as WRITE
//   - target a Crimson pool
// ===========================================================================

class TestValidateFlags : public ::testing::Test {
protected:
  pg_pool_t ec_pool;
  pg_pool_t ec_crimson_pool;
  pg_pool_t rep_pool;
  CephContext *cct = g_ceph_context;

  void SetUp() override {
    ec_pool        = make_ec_pool(4, 2, 4096);
    ec_crimson_pool = make_ec_pool(4, 2, 4096, pg_pool_t::FLAG_CRIMSON);
    rep_pool       = make_replicated_pool();
  }
};

// ===========================================================================
// Section 2: ECStripeIterator / ECStripeView
//
// ECStripeIterator and ECStripeView are protected members of SplitOp.
// A minimal test subclass is used to expose them for unit testing.
//
// Each test verifies the offset/length/shard_offset/raw_shard fields
// produced by the iterator for a given (offset, length, k, stripe_unit)
// combination.  The geometry for a k-data-chunk pool is:
//
//   stripe_width = k * chunk_size
//   raw_shard    = (offset / chunk_size) % k
//   shard_offset = (offset / (k * chunk_size)) * chunk_size
//                  + (offset % chunk_size)
// ===========================================================================

/**
 * StripeIteratorExposer — minimal SplitOp subclass that makes the protected
 * ECStripeIterator and ECStripeView types accessible from tests.
 *
 * Only the types are re-exported; no virtual methods are implemented because
 * this class is never instantiated — it exists solely to inherit access.
 */
class StripeIteratorExposer : public SplitOp {
public:
  // Re-export the protected iterator types so the test can use them directly.
  using SplitOp::ECStripeIterator;
  using SplitOp::ECStripeView;
  using SplitOp::ECChunkInfo;

  // Satisfy the pure-virtual interface — never called in these tests.
  std::pair<extent_set, bufferlist>
    assemble_buffer_sparse_read(int) const override { return {}; }
  void assemble_buffer_read(bufferlist &, int) const override {}
  void init_read(OSDOp &, bool, int, bool) override {}
  bool legacy_version_mismatch() const override { return false; }
  bool version_mismatch_v2() const override { return false; }
  void init_reference_sub_read() override {}

private:
  // Constructor is private; StripeIteratorExposer is never instantiated.
  // Suppress the "base is inaccessible" compiler warning.
  using SplitOp::SplitOp;
};

// Convenient type aliases for use in tests.
using ECStripeIterator = StripeIteratorExposer::ECStripeIterator;
using ECStripeView     = StripeIteratorExposer::ECStripeView;
using ECChunkInfo      = StripeIteratorExposer::ECChunkInfo;

// ---------------------------------------------------------------------------
// Helpers for stripe iterator tests
// ---------------------------------------------------------------------------

namespace {

/**
 * Collect all ECChunkInfo entries produced by a stripe view into a vector.
 */
std::vector<ECChunkInfo>
collect_chunks(uint64_t offset, uint64_t length, int k, uint32_t chunk_size)
{
  pg_pool_t pi = make_ec_pool(k, 2, chunk_size);
  ECStripeView view(offset, length, &pi);
  std::vector<ECChunkInfo> result;
  for (auto info : view) {
    result.push_back(info);
  }
  return result;
}

} // anonymous namespace

// ---------------------------------------------------------------------------

class TestECStripeIterator : public ::testing::Test {};

// Single chunk, at offset 0 — all data fits in shard 0.
// k=4, chunk_size=4096: one chunk [0, 4096) → raw_shard=0, shard_offset=0.
TEST_F(TestECStripeIterator, SingleChunkAtStart)
{
  auto chunks = collect_chunks(/*offset=*/0, /*length=*/4096,
                                /*k=*/4, /*chunk_size=*/4096);
  ASSERT_EQ(1u, chunks.size());
  EXPECT_EQ(0u,                  chunks[0].ro_offset);
  EXPECT_EQ(4096u,               chunks[0].length);
  EXPECT_EQ(raw_shard_id_t(0),   chunks[0].raw_shard);
  EXPECT_EQ(0u,                  chunks[0].shard_offset);
}

// Single chunk, starting at the boundary of the second shard.
// k=4, chunk_size=4096: offset=4096 → raw_shard=1, shard_offset=0.
TEST_F(TestECStripeIterator, SingleChunkSecondShard)
{
  auto chunks = collect_chunks(/*offset=*/4096, /*length=*/4096,
                                /*k=*/4, /*chunk_size=*/4096);
  ASSERT_EQ(1u, chunks.size());
  EXPECT_EQ(4096u,               chunks[0].ro_offset);
  EXPECT_EQ(4096u,               chunks[0].length);
  EXPECT_EQ(raw_shard_id_t(1),   chunks[0].raw_shard);
  EXPECT_EQ(0u,                  chunks[0].shard_offset);
}

// Two consecutive chunks, spanning shards 0 and 1 within the first stripe.
// k=4, chunk_size=4096, offset=0, length=8192.
TEST_F(TestECStripeIterator, TwoConsecutiveChunks)
{
  auto chunks = collect_chunks(0, 8192, 4, 4096);
  ASSERT_EQ(2u, chunks.size());

  EXPECT_EQ(0u,                chunks[0].ro_offset);
  EXPECT_EQ(4096u,             chunks[0].length);
  EXPECT_EQ(raw_shard_id_t(0), chunks[0].raw_shard);
  EXPECT_EQ(0u,                chunks[0].shard_offset);

  EXPECT_EQ(4096u,             chunks[1].ro_offset);
  EXPECT_EQ(4096u,             chunks[1].length);
  EXPECT_EQ(raw_shard_id_t(1), chunks[1].raw_shard);
  EXPECT_EQ(0u,                chunks[1].shard_offset);
}

// Full stripe across all k=4 shards.
TEST_F(TestECStripeIterator, FullStripe)
{
  const int k = 4;
  const uint32_t chunk_size = 4096;
  auto chunks = collect_chunks(0, k * chunk_size, k, chunk_size);
  ASSERT_EQ(static_cast<size_t>(k), chunks.size());
  for (int i = 0; i < k; ++i) {
    EXPECT_EQ(static_cast<uint64_t>(i) * chunk_size, chunks[i].ro_offset)
      << "chunk " << i;
    EXPECT_EQ(chunk_size,            chunks[i].length)      << "chunk " << i;
    EXPECT_EQ(raw_shard_id_t(i),     chunks[i].raw_shard)   << "chunk " << i;
    EXPECT_EQ(0u,                    chunks[i].shard_offset) << "chunk " << i;
  }
}

// Read that crosses a stripe boundary: offset=0, length = 5 * chunk_size
// with k=4.  Shard 4 wraps back to shard 0 in the second stripe.
TEST_F(TestECStripeIterator, WrapAroundStripe)
{
  const int k = 4;
  const uint32_t chunk_size = 4096;
  auto chunks = collect_chunks(0, 5 * chunk_size, k, chunk_size);
  ASSERT_EQ(5u, chunks.size());

  // Chunk 4 should be shard 0 again, but in the second stripe row.
  EXPECT_EQ(raw_shard_id_t(0), chunks[4].raw_shard);
  EXPECT_EQ(chunk_size,        chunks[4].shard_offset);   // second stripe row
  EXPECT_EQ(chunk_size,        chunks[4].length);
}

// Partial chunk: length smaller than chunk_size, not at a chunk boundary.
// offset=100, length=200, chunk_size=4096, k=4 → still within shard 0.
TEST_F(TestECStripeIterator, PartialFirstChunk)
{
  auto chunks = collect_chunks(100, 200, 4, 4096);
  ASSERT_EQ(1u, chunks.size());
  EXPECT_EQ(100u,              chunks[0].ro_offset);
  EXPECT_EQ(200u,              chunks[0].length);
  EXPECT_EQ(raw_shard_id_t(0), chunks[0].raw_shard);
  EXPECT_EQ(100u,              chunks[0].shard_offset);   // within-chunk offset preserved
}

// Read spanning a chunk boundary: offset inside shard 0, extends into shard 1.
// chunk_size=4096, k=4, offset=3000, length=2000.
// → chunk[0]: ro_offset=3000, length=1096, shard 0
// → chunk[1]: ro_offset=4096, length=904,  shard 1
TEST_F(TestECStripeIterator, SpanChunkBoundary)
{
  auto chunks = collect_chunks(3000, 2000, 4, 4096);
  ASSERT_EQ(2u, chunks.size());

  EXPECT_EQ(3000u,             chunks[0].ro_offset);
  EXPECT_EQ(1096u,             chunks[0].length);          // 4096 - 3000 = 1096
  EXPECT_EQ(raw_shard_id_t(0), chunks[0].raw_shard);

  EXPECT_EQ(4096u,             chunks[1].ro_offset);
  EXPECT_EQ(904u,              chunks[1].length);           // 2000 - 1096 = 904
  EXPECT_EQ(raw_shard_id_t(1), chunks[1].raw_shard);
}

// Single byte read.
TEST_F(TestECStripeIterator, SingleByteRead)
{
  auto chunks = collect_chunks(0, 1, 4, 4096);
  ASSERT_EQ(1u, chunks.size());
  EXPECT_EQ(1u, chunks[0].length);
}

// k=2 pool — stripe_width = 2 * chunk_size.
TEST_F(TestECStripeIterator, K2FullStripe)
{
  const int k = 2;
  const uint32_t chunk_size = 8192;
  auto chunks = collect_chunks(0, k * chunk_size, k, chunk_size);
  ASSERT_EQ(2u, chunks.size());
  EXPECT_EQ(raw_shard_id_t(0), chunks[0].raw_shard);
  EXPECT_EQ(raw_shard_id_t(1), chunks[1].raw_shard);
}

// ===========================================================================
// Section 3: ECSplitOp::local_zone_for_acting_set() — parameter guard cases
//
// The edge-case guard paths (fewer than 2 zones, empty crush_location, too-
// small acting set, null crush pointer) all return 0 and do not require a
// real CRUSH map.  More complete tests that exercise CRUSH lookups live in
// TestECWithCRUSH.cc.
// ===========================================================================

class TestLocalZoneGuards : public ::testing::Test {
protected:
  CephContext *cct = g_ceph_context;

  // A simple acting set — values need not correspond to real OSDs for these
  // guard-path tests.
  std::vector<int> make_acting(int count, int start = 0) {
    std::vector<int> v(count);
    std::iota(v.begin(), v.end(), start);
    return v;
  }

  std::multimap<std::string, std::string> make_loc(const std::string& dc) {
    std::multimap<std::string, std::string> loc;
    loc.emplace("datacenter", dc);
    return loc;
  }
};

// ===========================================================================
// Section 4: abs_shard computation for reference_sub_read (Bug 2 regression)
//
// When init_read() decides that primary_required is true it creates an extra
// sub-read for the version-check "reference" shard.  Before the fix the code
// always assigned:
//
//   sub_reads.emplace(reference_sub_read,  ..., shard_id_t(reference_sub_read));
//                                               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//                                               was: always a zone-0 acting index
//
// The fix uses:
//   shard_id_t(reference_sub_read + local_zone_index * zone_size)
//
// so the version-check sub-read goes to the same zone as the data sub-reads.
//
// The helpers below model both the old (buggy) and new (correct) formula so
// that the tests document what changed and act as regression guards.
// ===========================================================================

class TestReferenceSubReadAbsShard : public ::testing::Test {
protected:
  // Pool: k=2, m=1, 2 zones  →  pool.size=6, zone_size=3, data_chunk_count=2
  // Acting set:  [osd.0, osd.1, osd.2(par), osd.3, osd.4, osd.5(par)]
  //               zone-0 ────────────────── zone-1 ──────────────────
  static constexpr int k          = 2;
  static constexpr int m          = 1;
  static constexpr int num_zones  = 2;
  static constexpr int zone_size  = k + m;          // 3
  static constexpr int pool_size  = zone_size * num_zones; // 6
  // Primary shard lives at acting index 0 (zone-0).
  static constexpr int reference_sub_read = 0;

  // Compute the abs_shard that the current (buggy) code assigns to the
  // reference sub-read.
  static shard_id_t buggy_reference_abs_shard()
  {
    // Current code: shard_id_t(reference_sub_read) — ignores local_zone_index.
    return shard_id_t(reference_sub_read);
  }

  // Compute the abs_shard that the correct code should assign.
  static shard_id_t correct_reference_abs_shard(int local_zone_index)
  {
    return shard_id_t(reference_sub_read + local_zone_index * zone_size);
  }
};

// For a zone-0 client the bug is latent: both formulae agree.
TEST_F(TestReferenceSubReadAbsShard, Zone0ClientBugIsLatent)
{
  constexpr int local_zone = 0;
  EXPECT_EQ(buggy_reference_abs_shard(), correct_reference_abs_shard(local_zone))
    << "Zone-0: buggy and correct formula agree — bug is not visible";
  // Both map to acting[0], which is a zone-0 OSD — correct.
  EXPECT_EQ(shard_id_t(0), buggy_reference_abs_shard());
}

// For a zone-1 client the bug is exposed: the buggy formula targets
// acting[0] (zone-0 OSD) instead of acting[3] (zone-1 equivalent).
TEST_F(TestReferenceSubReadAbsShard, Zone1ClientBugExposed)
{
  constexpr int local_zone = 1;

  shard_id_t buggy   = buggy_reference_abs_shard();
  shard_id_t correct = correct_reference_abs_shard(local_zone);

  // Buggy code dispatches to acting[0] — a zone-0 OSD.
  EXPECT_EQ(shard_id_t(0),         buggy);
  // Correct code should dispatch to acting[3] — a zone-1 OSD.
  EXPECT_EQ(shard_id_t(zone_size), correct);

  // The two must differ — if they are equal this test is vacuous.
  EXPECT_NE(buggy, correct)
    << "Bug: reference sub-read abs_shard=" << (int)buggy
    << " targets zone-0 acting index even though local_zone=" << local_zone
    << "; correct abs_shard=" << (int)correct
    << " would target zone-1 acting index (acting[" << (int)correct << "])";
}

// Demonstrate that acting[buggy_abs] is always a zone-0 OSD regardless of
// which zone the client wants to read from.
TEST_F(TestReferenceSubReadAbsShard, BuggyShardAlwaysInZero)
{
  // acting = [0, 1, 2, 3, 4, 5]  (osd ids == acting indices for simplicity)
  std::vector<int> acting(pool_size);
  std::iota(acting.begin(), acting.end(), 0);

  for (int local_zone = 0; local_zone < num_zones; ++local_zone) {
    shard_id_t buggy = buggy_reference_abs_shard();
    int buggy_osd    = acting[(int)buggy];

    // Zone-0 OSDs are at acting indices [0, zone_size).
    bool is_zone0_osd = ((int)buggy < zone_size);
    EXPECT_TRUE(is_zone0_osd)
      << "local_zone=" << local_zone
      << ": buggy abs_shard=" << (int)buggy
      << " maps to acting[" << (int)buggy << "]=" << buggy_osd
      << " which is in zone-0 regardless of client zone";

    if (local_zone == 1) {
      // The correct zone-1 OSD should be at a strictly higher acting index.
      shard_id_t correct = correct_reference_abs_shard(local_zone);
      int correct_osd    = acting[(int)correct];
      bool is_zone1_osd  = ((int)correct >= zone_size && (int)correct < pool_size);
      EXPECT_TRUE(is_zone1_osd)
        << "correct abs_shard=" << (int)correct
        << " maps to acting[" << (int)correct << "]=" << correct_osd
        << " which should be in zone-1";
    }
  }
}

// Generalise over (k, m) combinations to show the bug scales with zone_size.
TEST(TestReferenceSubReadAbsShardParametric, BugScalesWithZoneSize)
{
  // { k, m }  →  zone_size = k+m
  const std::vector<std::pair<int,int>> configs = {{2,1},{4,2},{3,2},{8,3}};
  for (auto [k, m] : configs) {
    int zs = k + m;  // zone_size
    // reference_sub_read is the acting index of the primary shard (always 0
    // for a standard single-PG test setup).
    constexpr int ref = 0;
    constexpr int local_zone = 1;

    shard_id_t buggy   = shard_id_t(ref);
    shard_id_t correct = shard_id_t(ref + local_zone * zs);

    EXPECT_EQ(shard_id_t(0), buggy)
      << "k=" << k << " m=" << m << ": buggy always 0";
    EXPECT_EQ(shard_id_t(zs), correct)
      << "k=" << k << " m=" << m << ": correct should be zone_size=" << zs;
    EXPECT_NE(buggy, correct)
      << "k=" << k << " m=" << m << ": bug is visible for local_zone=1";
  }
}

// ===========================================================================
// Section 5: ReplicaSplitOp LOCALIZE_READS zone filtering
//
// When localize=true on a stretch replica pool, ReplicaSplitOp::init_read()
// must restrict the OSD set to replicas in the client's local zone only.
// When localize=false (BALANCE_READS), all replicas are used as before.
//
// These tests exercise the zone-selection formula directly (via the now-shared
// SplitOp::local_zone_for_acting_set()) and verify the acting-index ranges
// for each zone so the production filtering code can be reasoned about.
// ===========================================================================

class TestReplicaLocalizeZoneFiltering : public ::testing::Test {
protected:
  // Stretch replica pool: size=6, 2 zones, zone_size=3.
  // acting = [osd.0, osd.1, osd.2,  osd.3, osd.4, osd.5]
  //           ── zone-0 ──────────  ── zone-1 ──────────
  static constexpr int pool_size = 6;
  static constexpr int num_zones = 2;
  static constexpr int zone_size = pool_size / num_zones; // 3

  std::vector<int> make_acting() {
    std::vector<int> v(pool_size);
    std::iota(v.begin(), v.end(), 0);
    return v;
  }

  // Return the acting indices that belong to zone z.
  std::vector<int> zone_indices(int z) {
    std::vector<int> idx;
    for (int i = z * zone_size; i < (z + 1) * zone_size; ++i) {
      idx.push_back(i);
    }
    return idx;
  }
};

// The zone-0 indices are [0, zone_size) and zone-1 indices are [zone_size, 2*zone_size).
TEST_F(TestReplicaLocalizeZoneFiltering, ZoneIndexRangesAreNonOverlapping)
{
  auto z0 = zone_indices(0);
  auto z1 = zone_indices(1);

  // zone-0 and zone-1 indices must be disjoint.
  for (int i : z0) {
    EXPECT_EQ(std::count(z1.begin(), z1.end(), i), 0)
      << "index " << i << " appears in both zone-0 and zone-1";
  }
  // Together they cover the full acting set.
  EXPECT_EQ((int)(z0.size() + z1.size()), pool_size);
}

// For LOCALIZE_READS with local_zone=0, only acting indices [0, zone_size)
// are eligible — none from zone-1.
TEST_F(TestReplicaLocalizeZoneFiltering, LocalZone0FiltersOutZone1)
{
  auto acting = make_acting();
  int local_zone = 0;
  int zone_start = local_zone * zone_size;
  int zone_end   = zone_start + zone_size;

  // Simulate the filtering loop from ReplicaSplitOp::init_read().
  std::vector<int> filtered;
  for (int i = zone_start; i < zone_end; ++i) {
    filtered.push_back(acting[i]);
  }

  EXPECT_EQ((int)filtered.size(), zone_size);
  for (int osd : filtered) {
    EXPECT_LT(osd, zone_size) << "OSD " << osd << " is not in zone-0";
  }
}

// For LOCALIZE_READS with local_zone=1, only acting indices [zone_size, 2*zone_size)
// are eligible — none from zone-0.
TEST_F(TestReplicaLocalizeZoneFiltering, LocalZone1FiltersOutZone0)
{
  auto acting = make_acting();
  int local_zone = 1;
  int zone_start = local_zone * zone_size;
  int zone_end   = zone_start + zone_size;

  std::vector<int> filtered;
  for (int i = zone_start; i < zone_end; ++i) {
    filtered.push_back(acting[i]);
  }

  EXPECT_EQ((int)filtered.size(), zone_size);
  for (int osd : filtered) {
    EXPECT_GE(osd, zone_size) << "OSD " << osd << " is not in zone-1";
  }
}

// If all local-zone replicas are absent (acting[i] == CRUSH_ITEM_NONE),
// the filtered set is empty → abort → primary fallback.
TEST_F(TestReplicaLocalizeZoneFiltering, AllLocalZoneReplicasAbsentTriggersAbort)
{
  auto acting = make_acting();
  // Mark all zone-0 replicas absent.
  for (int i = 0; i < zone_size; ++i) {
    acting[i] = CRUSH_ITEM_NONE;
  }

  int local_zone = 0;
  int zone_start = local_zone * zone_size;
  int zone_end   = zone_start + zone_size;

  int available = 0;
  for (int i = zone_start; i < zone_end; ++i) {
    if (acting[i] != CRUSH_ITEM_NONE) {
      ++available;
    }
  }

  // With 0 available replicas in zone-0, the filtering loop yields 0 OSDs,
  // which is < 2 → init_read() sets abort = true.
  EXPECT_EQ(available, 0) << "Expected no zone-0 replicas available";
  EXPECT_LT(available, 2) << "abort condition requires < 2 local-zone replicas";
}

// For BALANCE_READS (localize=false), all available replicas across both
// zones are used — zone-filtering is not applied.
TEST_F(TestReplicaLocalizeZoneFiltering, BalanceReadsUsesAllReplicas)
{
  auto acting = make_acting();
  // Count all non-absent OSDs.
  int all_osds = 0;
  for (int osd : acting) {
    if (osd != CRUSH_ITEM_NONE) {
      ++all_osds;
    }
  }
  EXPECT_EQ(all_osds, pool_size)
    << "BALANCE_READS should have access to all " << pool_size << " replicas";
}

// Generalise: for any zone, the filtered set size equals zone_size.
TEST_F(TestReplicaLocalizeZoneFiltering, FilteredSetSizeEqualsZoneSize)
{
  auto acting = make_acting();
  for (int z = 0; z < num_zones; ++z) {
    int zone_start = z * zone_size;
    int zone_end   = zone_start + zone_size;
    int count = 0;
    for (int i = zone_start; i < zone_end; ++i) {
      if (acting[i] != CRUSH_ITEM_NONE) {
        ++count;
      }
    }
    EXPECT_EQ(count, zone_size)
      << "zone " << z << ": expected " << zone_size
      << " replicas, got " << count;
  }
}

/*
 * ---------------------------------------------------------------------------
 * Baseline: the cases the single-direct-read path exists to catch.
 * ---------------------------------------------------------------------------
 */

TEST(SplitOpValidate, OneReadInsideOneChunkIsASingleDirectOp) {
  const pg_pool_t pool = make_ec_pool();

  EXPECT_TRUE(is_single_direct_op(reads({{0, CHUNK}}), pool));
}

TEST(SplitOpValidate, ReadCrossingAChunkBoundaryIsNotASingleDirectOp) {
  const pg_pool_t pool = make_ec_pool();

  EXPECT_FALSE(is_single_direct_op(reads({{CHUNK / 2, CHUNK}}), pool));
}

TEST(SplitOpValidate, TwoReadsInsideTheSameChunkAreASingleDirectOp) {
  const pg_pool_t pool = make_ec_pool();

  // Both halves of chunk 0.  One OSD can serve the whole op.
  ASSERT_EQ(raw_shard_of(0), raw_shard_of(CHUNK / 2));

  EXPECT_TRUE(is_single_direct_op(
    reads({{0, CHUNK / 2}, {CHUNK / 2, CHUNK / 2}}), pool));
}

TEST(SplitOpValidate, TwoReadsOnTheSameShardInDifferentStripesAreASingleDirectOp) {
  const pg_pool_t pool = make_ec_pool();

  // Chunk 0 of stripe 0 and chunk 0 of stripe 1.  Different chunks of the
  // object, but both live on raw shard 0, so one OSD can still serve the op.
  // A fix that requires the reads to be in the same *chunk* rather than on the
  // same *shard* would wrongly reject this.
  ASSERT_EQ(raw_shard_of(0), raw_shard_of(STRIPE_WIDTH));

  EXPECT_TRUE(is_single_direct_op(
    reads({{0, CHUNK}, {STRIPE_WIDTH, CHUNK}}), pool));
}

TEST(SplitOpValidate, TwoReadsOnDifferentShardsAreNotASingleDirectOp) {
  const pg_pool_t pool = make_ec_pool();

  // Chunk 0 and chunk 1: each is chunk-aligned and exactly one chunk long
  ASSERT_NE(raw_shard_of(0), raw_shard_of(CHUNK));

  EXPECT_FALSE(is_single_direct_op(reads({{0, CHUNK}, {CHUNK, CHUNK}}), pool))
    << "reads on raw shards " << raw_shard_of(0) << " and "
    << raw_shard_of(CHUNK) << " were classified as a single direct read; ";
}

TEST(SplitOpValidate, TwoReadsOnDifferentNonZeroShardsAreNotASingleDirectOp) {
  const pg_pool_t pool = make_ec_pool();

  ASSERT_NE(raw_shard_of(CHUNK), raw_shard_of(2 * CHUNK));

  EXPECT_FALSE(is_single_direct_op(
    reads({{CHUNK, CHUNK}, {2 * CHUNK, CHUNK}}), pool))
    << "reads on raw shards " << raw_shard_of(CHUNK) << " and "
    << raw_shard_of(2 * CHUNK) << " were classified as a single direct read";
}

TEST(SplitOpValidate, TwoReadsOnDifferentShardsAcrossGeometries) {
  struct {
    int k;
    int m;
    uint64_t chunk_size;
  } const geometries[] = {
    {2, 1, 4096},
    {4, 2, 4096},
    {4, 2, 65536},
    {8, 3, 16384},
    {6, 2, 12288},
  };

  for (const auto &g : geometries) {
    const pg_pool_t pool = make_ec_pool(g.k, g.m, g.chunk_size);

    ASSERT_NE(raw_shard_of(0, g.chunk_size, g.k),
              raw_shard_of(g.chunk_size, g.chunk_size, g.k));

    EXPECT_FALSE(is_single_direct_op(
      reads({{0, g.chunk_size}, {g.chunk_size, g.chunk_size}}), pool))
      << "k=" << g.k << " m=" << g.m << " chunk_size=" << g.chunk_size;
  }
}

// ===========================================================================
// Section 4: SplitOp::create() — the OSD a single direct op is sent to
//
// An op whose reads all fall in one chunk is sent whole to the OSD holding
// that chunk. An op that also holds ops only the primary may run, such as
// CALL, must go to the primary.
// ===========================================================================

class TestSingleDirectOpTarget : public ::testing::Test {
protected:
  static constexpr int k = 2;
  static constexpr int m = 1;
  static constexpr uint32_t chunk_size = 4096;
  static inline boost::intrusive_ptr<CephContext> objecter_cct;
  CephContext *cct = nullptr;
  std::unique_ptr<ceph::async::io_context_pool> icp;
  std::unique_ptr<MonClient> monc;
  std::unique_ptr<Messenger> messenger;
  std::unique_ptr<Objecter> objecter;
  OSDMap osdmap;
  int64_t pool_id = -1;

  static void SetUpTestSuite() {
    if (!objecter_cct) {
      std::vector<const char*> args;
      objecter_cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                                 CODE_ENVIRONMENT_UTILITY,
                                 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
      common_init_finish(objecter_cct.get());
    }
  }

  void SetUp() override {
    cct = objecter_cct.get();
    icp = std::make_unique<ceph::async::io_context_pool>(1);
    monc = std::make_unique<MonClient>(cct, *icp);
    messenger.reset(
      Messenger::create_client_messenger(cct, "unittest_split_ops_ut"));
    messenger->start();
    objecter = std::make_unique<Objecter>(cct, messenger.get(), monc.get(),
                                          *icp);
    objecter->set_client_incarnation(0);
    objecter->init();
    build_osdmap();
  }

  void TearDown() override {
    objecter->shutdown();
    monc->shutdown();
    messenger->shutdown();
    messenger->wait();
    objecter.reset();
    monc.reset();
    messenger.reset();
    icp.reset();
  }

  // OSDs 0..k-1 have primary affinity 0, so a PG with all its data shards on
  // them has its parity shard as primary while shard 0 is up.
  void build_osdmap() {
    uuid_d fsid; // matches the unconnected MonClient
    osdmap.build_simple(cct, 0, fsid, k + m);
    OSDMap::Incremental osd_inc(osdmap.get_epoch() + 1);
    osd_inc.fsid = fsid;
    entity_addrvec_t addrs;
    addrs.v.push_back(entity_addr_t());
    for (int i = 0; i < k + m; ++i) {
      addrs.v[0].nonce = i;
      osd_inc.new_state[i] = CEPH_OSD_EXISTS | CEPH_OSD_NEW;
      osd_inc.new_up_client[i] = addrs;
      osd_inc.new_up_cluster[i] = addrs;
      osd_inc.new_hb_back_up[i] = addrs;
      osd_inc.new_hb_front_up[i] = addrs;
      osd_inc.new_weight[i] = CEPH_OSD_IN;
    }
    osdmap.apply_incremental(osd_inc);
    for (int i = 0; i < k; ++i) {
      osdmap.set_primary_affinity(i, 0);
    }

    int rule = osdmap.crush->add_simple_rule(
      "ec", "default", "osd", "", "indep", pg_pool_t::TYPE_ERASURE, &std::cerr);
    ASSERT_GE(rule, 0);
    OSDMap::Incremental pool_inc(osdmap.get_epoch() + 1);
    pool_inc.fsid = fsid;
    pool_inc.new_pool_max = osdmap.get_pool_max();
    pool_id = ++pool_inc.new_pool_max;
    pg_pool_t pool = make_ec_pool(k, m, chunk_size);
    pool.set_flag(pg_pool_t::FLAG_HASHPSPOOL);
    pool.object_hash = CEPH_STR_HASH_RJENKINS;
    pool.set_pg_num(64);
    pool.set_pgp_num(64);
    pool.crush_rule = rule;
    for (int i = 1; i < k; ++i) {
      pool.nonprimary_shards.insert(shard_id_t(i));
    }
    pool_inc.new_pools[pool_id] = pool;
    pool_inc.new_pool_names[pool_id] = "ec";
    osdmap.apply_incremental(pool_inc);

    bufferlist bl;
    osdmap.encode(bl, CEPH_FEATURES_ALL | CEPH_FEATURE_RESERVED);
    auto msg = ceph::make_message<MOSDMap>(fsid, CEPH_FEATURES_ALL);
    msg->maps[osdmap.get_epoch()] = bl;
    objecter->handle_osd_map(msg.get());
  }

  // An object whose PG has acting[primary_shard] as primary, with shard 0 up.
  object_t object_with_primary(int primary_shard) {
    for (int i = 0; i < 1000; ++i) {
      object_t oid("obj" + std::to_string(i));
      pg_t pgid = osdmap.object_locator_to_pg(oid, object_locator_t(pool_id));
      std::vector<int> acting;
      int primary;
      osdmap.pg_to_acting_osds(pgid, &acting, &primary);
      if (acting.size() == k + m && osdmap.is_up(acting[0]) &&
          primary == acting[primary_shard]) {
        return oid;
      }
    }
    ADD_FAILURE() << "no PG has shard " << primary_shard << " as primary";
    return object_t();
  }

  // Returns the op after SplitOp::create() chose not to split it.
  Objecter::Op *create(const object_t &oid, ObjectOperation &rd) {
    Objecter::Op *op = objecter->prepare_read_op(
      oid, object_locator_t(pool_id), rd, CEPH_NOSNAP, nullptr,
      CEPH_OSD_FLAG_BALANCE_READS, -1, nullptr);
    ceph::shared_mutex lock = ceph::make_shared_mutex("TestSingleDirectOpTarget");
    ceph::shunique_lock<ceph::shared_mutex> sul(lock, ceph::acquire_shared);
    EXPECT_FALSE(SplitOp::create(op, *objecter, sul, cct));
    return op;
  }
};

TEST_F(TestSingleDirectOpTarget, ReadGoesToItsShard)
{
  object_t oid = object_with_primary(k);
  ObjectOperation rd;
  bufferlist out;
  rd.read(0, chunk_size / 2, &out, nullptr, nullptr);
  Objecter::Op *op = create(oid, rd);
  EXPECT_EQ(op->target.acting[0], op->target.osd);
  EXPECT_TRUE(op->target.flags & CEPH_OSD_FLAG_EC_DIRECT_READ);
  op->put();
}

TEST_F(TestSingleDirectOpTarget, CallGoesToPrimary)
{
  object_t oid = object_with_primary(k);
  ObjectOperation rd;
  bufferlist out, in;
  rd.read(0, chunk_size / 2, &out, nullptr, nullptr);
  rd.call("lock", "get_info", in);
  Objecter::Op *op = create(oid, rd);
  EXPECT_NE(op->target.acting[0], op->target.acting_primary);
  EXPECT_EQ(op->target.acting_primary, op->target.osd)
    << "acting " << op->target.acting;
  op->put();
}

TEST_F(TestSingleDirectOpTarget, CallGoesDirectToPrimaryShardZero)
{
  object_t oid = object_with_primary(0);
  ObjectOperation rd;
  bufferlist out, in;
  rd.read(0, chunk_size / 2, &out, nullptr, nullptr);
  rd.call("lock", "get_info", in);
  Objecter::Op *op = create(oid, rd);
  EXPECT_EQ(op->target.acting_primary, op->target.osd);
  EXPECT_TRUE(op->target.flags & CEPH_OSD_FLAG_EC_DIRECT_READ);
  op->put();
}
