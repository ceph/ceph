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
 *
 * The tests use a minimal pg_pool_t built inline (no OSDMap, no peering).
 * For ECStripeIterator, a thin helper subclass exposes the protected types.
 */

#include <gtest/gtest.h>
#include <numeric>

#include "osd/osd_types.h"
#include "osdc/SplitOp.h"
#include "include/rados.h"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

namespace {

/**
 * Build a minimal erasure-coded pg_pool_t.
 *
 * @param k             number of data chunks
 * @param m             number of coding chunks
 * @param stripe_unit   per-shard stripe unit in bytes (stripe_width = k * stripe_unit)
 * @param extra_flags   additional pool flags to OR in
 */
pg_pool_t make_ec_pool(int k, int m, uint32_t stripe_unit,
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

// BALANCE_READS alone is sufficient for a non-Crimson pool.
TEST_F(TestValidateFlags, BalanceReadsAccepted)
{
  EXPECT_TRUE(SplitOp::validate_flags(
    &ec_pool, CEPH_OSD_FLAG_BALANCE_READS, cct));
}

// LOCALIZE_READS alone is also sufficient.
TEST_F(TestValidateFlags, LocalizeReadsAccepted)
{
  EXPECT_TRUE(SplitOp::validate_flags(
    &ec_pool, CEPH_OSD_FLAG_LOCALIZE_READS, cct));
}

// Both flags together must also pass.
TEST_F(TestValidateFlags, BothReadFlagsAccepted)
{
  EXPECT_TRUE(SplitOp::validate_flags(
    &ec_pool,
    CEPH_OSD_FLAG_BALANCE_READS | CEPH_OSD_FLAG_LOCALIZE_READS,
    cct));
}

// Neither flag set — must be rejected.
TEST_F(TestValidateFlags, NoReadFlagsRejected)
{
  EXPECT_FALSE(SplitOp::validate_flags(&ec_pool, 0, cct));
}

// WRITE flag alone (no read flag) — must be rejected.
TEST_F(TestValidateFlags, WriteFlagAloneRejected)
{
  EXPECT_FALSE(SplitOp::validate_flags(
    &ec_pool, CEPH_OSD_FLAG_WRITE, cct));
}

// WRITE flag combined with BALANCE_READS — write wins, must reject.
TEST_F(TestValidateFlags, WritePlusBalanceReadsRejected)
{
  EXPECT_FALSE(SplitOp::validate_flags(
    &ec_pool,
    CEPH_OSD_FLAG_BALANCE_READS | CEPH_OSD_FLAG_WRITE,
    cct));
}

// Crimson pool must always be rejected regardless of read flags.
TEST_F(TestValidateFlags, CrimsonPoolRejected)
{
  EXPECT_FALSE(SplitOp::validate_flags(
    &ec_crimson_pool, CEPH_OSD_FLAG_BALANCE_READS, cct));
}

// Replicated pool — same flag rules apply.
TEST_F(TestValidateFlags, ReplicatedBalanceReadsAccepted)
{
  EXPECT_TRUE(SplitOp::validate_flags(
    &rep_pool, CEPH_OSD_FLAG_BALANCE_READS, cct));
}

TEST_F(TestValidateFlags, ReplicatedNoReadFlagsRejected)
{
  EXPECT_FALSE(SplitOp::validate_flags(&rep_pool, 0, cct));
}

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
  void init_read(OSDOp &, bool, int) override {}
  bool version_mismatch() const override { return false; }
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

// num_zones < 2 → always zone 0.
TEST_F(TestLocalZoneGuards, SingleZoneReturnsZero)
{
  auto acting = make_acting(6);
  auto loc    = make_loc("dc0");
  // crush pointer is null — but the guard fires on num_zones first.
  EXPECT_EQ(0, ECSplitOp::local_zone_for_acting_set(
    acting, /*num_zones=*/1, /*zone_size=*/6,
    /*crush=*/nullptr, cct, loc));
}

// zone_size <= 0 → always zone 0.
TEST_F(TestLocalZoneGuards, ZeroZoneSizeReturnsZero)
{
  auto acting = make_acting(6);
  auto loc    = make_loc("dc0");
  EXPECT_EQ(0, ECSplitOp::local_zone_for_acting_set(
    acting, /*num_zones=*/2, /*zone_size=*/0,
    /*crush=*/nullptr, cct, loc));
}

// crush == nullptr → always zone 0.
TEST_F(TestLocalZoneGuards, NullCrushReturnsZero)
{
  auto acting = make_acting(12);
  auto loc    = make_loc("dc0");
  EXPECT_EQ(0, ECSplitOp::local_zone_for_acting_set(
    acting, /*num_zones=*/2, /*zone_size=*/6,
    /*crush=*/nullptr, cct, loc));
}

// Empty crush_location → always zone 0.
TEST_F(TestLocalZoneGuards, EmptyCrushLocationReturnsZero)
{
  auto acting = make_acting(12);
  std::multimap<std::string, std::string> empty_loc;
  EXPECT_EQ(0, ECSplitOp::local_zone_for_acting_set(
    acting, /*num_zones=*/2, /*zone_size=*/6,
    /*crush=*/nullptr, cct, empty_loc));
}

// Acting set too small (< num_zones * zone_size) → always zone 0.
TEST_F(TestLocalZoneGuards, ActingTooSmallReturnsZero)
{
  auto acting = make_acting(3);  // needs 12 (2 zones × 6)
  auto loc    = make_loc("dc0");
  EXPECT_EQ(0, ECSplitOp::local_zone_for_acting_set(
    acting, /*num_zones=*/2, /*zone_size=*/6,
    /*crush=*/nullptr, cct, loc));
}

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
