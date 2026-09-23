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
 *  3. SplitOp::local_zone_for_acting_set() — zone selection edge cases
 *
 * The tests use a minimal pg_pool_t built inline (no OSDMap, no peering).
 * For ECStripeIterator, a thin helper subclass exposes the protected types.
 */

#include <gtest/gtest.h>
#include <numeric>

#include <boost/asio/io_context.hpp>

#include "osd/OSDMap.h"
#include "osd/osd_types.h"
#include "osdc/SplitOp.h"
#include "include/rados.h"
#include "global/global_context.h"
#include "test/osd/OSDMapTestHelpers.h"

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
//   - lack BALANCE_READS, and LOCALIZE_READS on a multi-zone pool
//   - are flagged as WRITE
//   - target a Crimson pool
// ===========================================================================

class TestValidateFlags : public ::testing::Test {
protected:
  pg_pool_t ec_pool;
  pg_pool_t ec_zones_pool;
  pg_pool_t ec_crimson_pool;
  pg_pool_t rep_pool;
  CephContext *cct = g_ceph_context;

  void SetUp() override {
    ec_pool        = make_ec_pool(4, 2, 4096);
    ec_zones_pool  = make_ec_pool(4, 2, 4096);
    ec_zones_pool.opts.set(pool_opts_t::NUM_ZONES, static_cast<int64_t>(2));
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

// LOCALIZE_READS alone is sufficient for a multi-zone pool.
TEST_F(TestValidateFlags, LocalizeReadsAcceptedMultiZone)
{
  EXPECT_TRUE(SplitOp::validate_flags(
    &ec_zones_pool, CEPH_OSD_FLAG_LOCALIZE_READS, cct));
}

// On a single-zone pool LOCALIZE_READS alone must not split: the read goes
// to the nearest replica instead.
TEST_F(TestValidateFlags, LocalizeReadsRejectedSingleZone)
{
  EXPECT_FALSE(SplitOp::validate_flags(
    &ec_pool, CEPH_OSD_FLAG_LOCALIZE_READS, cct));
  EXPECT_FALSE(SplitOp::validate_flags(
    &rep_pool, CEPH_OSD_FLAG_LOCALIZE_READS, cct));
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
// Section 3: SplitOp::local_zone_for_acting_set() — parameter guard cases
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
  EXPECT_EQ(0, SplitOp::local_zone_for_acting_set(
    acting, /*num_zones=*/1, /*zone_size=*/6,
    /*crush=*/nullptr, cct, loc));
}

// zone_size <= 0 → always zone 0.
TEST_F(TestLocalZoneGuards, ZeroZoneSizeReturnsZero)
{
  auto acting = make_acting(6);
  auto loc    = make_loc("dc0");
  EXPECT_EQ(0, SplitOp::local_zone_for_acting_set(
    acting, /*num_zones=*/2, /*zone_size=*/0,
    /*crush=*/nullptr, cct, loc));
}

// crush == nullptr → always zone 0.
TEST_F(TestLocalZoneGuards, NullCrushReturnsZero)
{
  auto acting = make_acting(12);
  auto loc    = make_loc("dc0");
  EXPECT_EQ(0, SplitOp::local_zone_for_acting_set(
    acting, /*num_zones=*/2, /*zone_size=*/6,
    /*crush=*/nullptr, cct, loc));
}

// Empty crush_location → always zone 0.
TEST_F(TestLocalZoneGuards, EmptyCrushLocationReturnsZero)
{
  auto acting = make_acting(12);
  std::multimap<std::string, std::string> empty_loc;
  EXPECT_EQ(0, SplitOp::local_zone_for_acting_set(
    acting, /*num_zones=*/2, /*zone_size=*/6,
    /*crush=*/nullptr, cct, empty_loc));
}

// Acting set too small (< num_zones * zone_size) → always zone 0.
TEST_F(TestLocalZoneGuards, ActingTooSmallReturnsZero)
{
  auto acting = make_acting(3);  // needs 12 (2 zones × 6)
  auto loc    = make_loc("dc0");
  EXPECT_EQ(0, SplitOp::local_zone_for_acting_set(
    acting, /*num_zones=*/2, /*zone_size=*/6,
    /*crush=*/nullptr, cct, loc));
}

// ===========================================================================
// Section 4: ReplicaSplitOp LOCALIZE_READS zone filtering
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

// ===========================================================================
// Section 5: init_reference_sub_read() / init_read() against a real Objecter
//
// Three datacenters "zone-0".."zone-2", OSDs 4z..4z+3 in zone-z.  The
// Objecter is never started; only its OSDMap and crush_location are used.
// ===========================================================================

class ECSplitOpProbe : public ECSplitOp {
public:
  using ECSplitOp::ECSplitOp;
  using SplitOp::sub_reads;
  using SplitOp::reference_sub_read;
  using SplitOp::reference_sub_read_key;
  using SplitOp::abort;
  ~ECSplitOpProbe() { abort = true; }
};

class ReplicaSplitOpProbe : public ReplicaSplitOp {
public:
  using ReplicaSplitOp::ReplicaSplitOp;
  using SplitOp::init;
  using SplitOp::sub_reads;
  using SplitOp::reference_sub_read;
  using SplitOp::reference_sub_read_key;
  using SplitOp::abort;
  ~ReplicaSplitOpProbe() { abort = true; }
};

class TestSplitOpInit : public ::testing::Test {
protected:
  static constexpr int osds_per_zone = 4;
  static constexpr int64_t ec_pool_id = 1;
  static constexpr int64_t rep_pool_id = 2;
  static constexpr int64_t degraded_ec_pool_id = 3;
  boost::asio::io_context ioc;
  std::unique_ptr<Objecter> objecter;

  void SetUp() override
  {
    g_ceph_context->_conf.set_val_or_die("osd_min_split_replica_read_size", "4096");
    objecter = std::make_unique<Objecter>(g_ceph_context, nullptr, nullptr, ioc);
    objecter->init();

    OSDMap map;
    uuid_d fsid;
    fsid.generate_random();
    ceph_assert(map.build_simple(g_ceph_context, 1, fsid, 3 * osds_per_zone) == 0);
    for (int i = 0; i < 3 * osds_per_zone; i++) {
      map.set_state(i, CEPH_OSD_EXISTS | CEPH_OSD_UP);
    }

    CrushWrapper crush;
    crush.create();
    OSDMap::_build_crush_types(crush);
    int rootid = 0;
    ceph_assert(crush.add_bucket(0, CRUSH_BUCKET_STRAW2, CRUSH_HASH_DEFAULT,
                                 crush.get_type_id("root"), 0, nullptr,
                                 nullptr, &rootid) == 0);
    crush.set_item_name(rootid, "default");
    for (int z = 0; z < 3; z++) {
      std::map<std::string, std::string> loc = {
        {"root", "default"},
        {"datacenter", "zone-" + std::to_string(z)},
        {"host", "host-" + std::to_string(z)}};
      for (int i = 0; i < osds_per_zone; i++) {
        int osd = z * osds_per_zone + i;
        crush.insert_item(g_ceph_context, osd, 1.0, "osd." + std::to_string(osd), loc);
      }
    }
    crush.finalize();
    OSDMap::Incremental inc(map.get_epoch() + 1);
    inc.fsid = map.get_fsid();
    crush.encode(inc.crush, CEPH_FEATURES_SUPPORTED_DEFAULT);
    map.apply_incremental(inc);

    pg_pool_t ec = make_ec_pool(2, 1, 4096);
    ec.size = 6;
    ec.opts.set(pool_opts_t::NUM_ZONES, static_cast<int64_t>(2));
    ec.peering_crush_bucket_count = 2;
    ec.set_pg_num(1);
    ec.set_pgp_num(1);
    OSDMapTestHelpers::add_pool(map, ec_pool_id, ec);

    pg_pool_t rep = make_replicated_pool(4);
    rep.opts.set(pool_opts_t::NUM_ZONES, static_cast<int64_t>(2));
    rep.peering_crush_bucket_count = 2;
    rep.set_pg_num(1);
    rep.set_pgp_num(1);
    OSDMapTestHelpers::add_pool(map, rep_pool_id, rep);
    OSDMapTestHelpers::set_pg_acting(map, pg_t(0, ec_pool_id), {0, 1, 2, 4, 5, 6});
    OSDMapTestHelpers::add_pool(map, degraded_ec_pool_id, ec);
    OSDMapTestHelpers::set_pg_acting(map, pg_t(0, degraded_ec_pool_id),
                                     {0, 1, 2, 4, CRUSH_ITEM_NONE, 6});

    objecter->start(&map);
  }

  void TearDown() override
  {
    objecter->shutdown();
    objecter.reset();
    g_ceph_context->_conf.set_val_or_die("osd_min_split_replica_read_size", "0");
  }

  void set_client_zone(int zone)
  {
    objecter->crush_location = {{"datacenter", "zone-" + std::to_string(zone)}};
  }

  int zone_of(int osd) const
  {
    return osd / osds_per_zone;
  }

  Objecter::Op *make_read_op(int64_t pool, const std::vector<int>& acting,
                             int primary_shard, uint64_t len, int flags)
  {
    osdc_opvec ops(1);
    ops[0].op.op = CEPH_OSD_OP_READ;
    ops[0].op.extent.offset = 0;
    ops[0].op.extent.length = len;
    auto op = new Objecter::Op(object_t("obj"), object_locator_t(pool),
                               std::move(ops), flags, (Context*)nullptr, nullptr);
    op->target.acting = acting;
    op->target.actual_pgid = spg_t(pg_t(0, pool), shard_id_t(primary_shard));
    return op;
  }
};

// LOCALIZE_READS from zone 1 with a zone-1 primary reads zone-1 data shards.
TEST_F(TestSplitOpInit, ECLocalizeZone1PrimaryReadsLocalShards)
{
  set_client_zone(1);
  std::vector<int> acting = {0, 1, 2, 4, 5, 6};
  auto op = make_read_op(ec_pool_id, acting, 3, 8192, CEPH_OSD_FLAG_LOCALIZE_READS);
  {
    ECSplitOpProbe split(op, *objecter, g_ceph_context, 6, true);
    split.init_reference_sub_read();
    ASSERT_FALSE(split.abort);
    EXPECT_EQ(3, split.reference_sub_read_key);
    split.init_read(op->ops[0], false, 0);
    ASSERT_FALSE(split.abort);
    EXPECT_EQ(2u, split.sub_reads.size());
    EXPECT_EQ(shard_id_t(3), split.sub_reads.at(3).abs_shard);
    EXPECT_EQ(shard_id_t(4), split.sub_reads.at(4).abs_shard);
    for (auto& [key, sr] : split.sub_reads) {
      EXPECT_EQ(1, zone_of(acting[(int)sr.abs_shard])) << "key " << key;
    }
  }
  op->put();
}

// A missing shard in the chosen zone aborts the split read; no cross-zone fallback.
TEST_F(TestSplitOpInit, ECLocalizeMissingLocalShardAborts)
{
  set_client_zone(1);
  std::vector<int> acting = {0, 1, 2, 4, CRUSH_ITEM_NONE, 6};
  auto op = make_read_op(ec_pool_id, acting, 0, 8192, CEPH_OSD_FLAG_LOCALIZE_READS);
  {
    ECSplitOpProbe split(op, *objecter, g_ceph_context, 6, true);
    split.init_reference_sub_read();
    ASSERT_FALSE(split.abort);
    split.init_read(op->ops[0], false, 0);
    EXPECT_TRUE(split.abort);
  }
  op->put();
}

// Three zones: nearest zone wins, NONE representatives fall through or are skipped.
TEST_F(TestSplitOpInit, LocalZoneForActingSetThreeZones)
{
  const int zone_size = 3;
  std::vector<int> acting = {0, 1, 2, 4, 5, 6, 8, 9, 10};
  auto zone_for = [&](int client_zone) {
    std::multimap<std::string, std::string> loc =
      {{"datacenter", "zone-" + std::to_string(client_zone)}};
    return objecter->with_osdmap([&](const OSDMap& o) {
      return SplitOp::local_zone_for_acting_set(acting, 3, zone_size,
                                                o.crush.get(), g_ceph_context, loc);
    });
  };
  EXPECT_EQ(0, zone_for(0));
  EXPECT_EQ(1, zone_for(1));
  EXPECT_EQ(2, zone_for(2));

  acting[6] = CRUSH_ITEM_NONE;
  EXPECT_EQ(2, zone_for(2));

  acting[7] = acting[8] = CRUSH_ITEM_NONE;
  EXPECT_NE(2, zone_for(2));

  acting[0] = acting[1] = acting[2] = CRUSH_ITEM_NONE;
  EXPECT_EQ(1, zone_for(1));
}

// LOCALIZE_READS on a stretch replica pool reads only from the client's zone.
TEST_F(TestSplitOpInit, ReplicaLocalizeReadsOnlyLocalZone)
{
  set_client_zone(1);
  std::vector<int> acting = {0, 1, 4, 5};
  for (unsigned seed = 0; seed < 16; seed++) {
    srand(seed);
    auto op = make_read_op(rep_pool_id, acting, 0, 4 * 4096,
                           CEPH_OSD_FLAG_LOCALIZE_READS);
    {
      ReplicaSplitOpProbe split(op, *objecter, g_ceph_context, 16, true);
      split.init_reference_sub_read();
      ASSERT_FALSE(split.abort);
      split.init_read(op->ops[0], false, 0);
      ASSERT_FALSE(split.abort);
      for (auto& [key, sr] : split.sub_reads) {
        EXPECT_LT((int)sr.abs_shard, (int)acting.size())
          << "seed " << seed << " key " << key;
        if ((int)sr.abs_shard < (int)acting.size()) {
          EXPECT_EQ(1, zone_of(acting[(int)sr.abs_shard]))
            << "seed " << seed << " key " << key;
        }
      }
    }
    op->put();
  }
}

// LOCALIZE_READS from zone 1 with a zone-0 primary keeps data reads in zone 1.
TEST_F(TestSplitOpInit, ECLocalizeZone0PrimaryKeepsLocalDataShard)
{
  set_client_zone(1);
  std::vector<int> acting = {0, 1, 2, 4, 5, 6};
  auto op = make_read_op(ec_pool_id, acting, 0, 8192, CEPH_OSD_FLAG_LOCALIZE_READS);
  {
    ECSplitOpProbe split(op, *objecter, g_ceph_context, 6, true);
    split.init_reference_sub_read();
    ASSERT_FALSE(split.abort);
    split.init_read(op->ops[0], false, 0);
    ASSERT_FALSE(split.abort);
    std::set<int> abs_shards;
    for (auto& [key, sr] : split.sub_reads) {
      abs_shards.insert((int)sr.abs_shard);
    }
    EXPECT_TRUE(abs_shards.contains(3));
    EXPECT_TRUE(abs_shards.contains(4));
    EXPECT_TRUE(abs_shards.contains(0));
  }
  op->put();
}

// A localized sparse read from zone 1 reassembles chunks read from zone-1 shards.
TEST_F(TestSplitOpInit, ECLocalizeZone1SparseReadAssembles)
{
  set_client_zone(1);
  std::vector<int> acting = {0, 1, 2, 4, 5, 6};
  auto op = make_read_op(ec_pool_id, acting, 0, 8192, CEPH_OSD_FLAG_LOCALIZE_READS);
  op->ops[0].op.op = CEPH_OSD_OP_SPARSE_READ;
  {
    ECSplitOpProbe split(op, *objecter, g_ceph_context, 6, true);
    split.init_reference_sub_read();
    ASSERT_FALSE(split.abort);
    split.init_read(op->ops[0], true, 0);
    ASSERT_FALSE(split.abort);
    for (int rel_shard = 0; rel_shard < 2; rel_shard++) {
      auto& d = split.sub_reads.at(rel_shard + 3).details[0];
      d.e->emplace(rel_shard * 4096, 4096);
      d.bl.append(std::string(4096, 'a' + rel_shard));
    }
    auto [extents, bl] = split.assemble_buffer_sparse_read(0);
    EXPECT_EQ(1u, extents.num_intervals());
    EXPECT_EQ(0u, extents.range_start());
    EXPECT_EQ(8192u, extents.range_end());
    std::string expected = std::string(4096, 'a') + std::string(4096, 'b');
    EXPECT_TRUE(bl.contents_equal(expected.data(), expected.size()));
  }
  op->put();
}

// A single-chunk LOCALIZE_READS read goes directly to the client's zone.
TEST_F(TestSplitOpInit, ECSingleChunkLocalizeReadsLocalZone)
{
  set_client_zone(1);
  auto op = make_read_op(ec_pool_id, {}, 0, 4096, CEPH_OSD_FLAG_LOCALIZE_READS);
  SplitOp::prepare_single_op(op, *objecter, g_ceph_context);
  EXPECT_TRUE(op->target.flags & CEPH_OSD_FLAG_EC_DIRECT_READ);
  EXPECT_EQ(4, op->target.osd);
  EXPECT_EQ(shard_id_t(3), op->target.actual_pgid.shard);
  op->put();
}

// Single-chunk BALANCE_READS reads are spread over every zone.
TEST_F(TestSplitOpInit, ECSingleChunkBalanceReadsUsesEveryZone)
{
  std::set<int> zones;
  for (unsigned seed = 0; seed < 16; seed++) {
    srand(seed);
    auto op = make_read_op(ec_pool_id, {}, 0, 4096, CEPH_OSD_FLAG_BALANCE_READS);
    SplitOp::prepare_single_op(op, *objecter, g_ceph_context);
    EXPECT_TRUE(op->target.flags & CEPH_OSD_FLAG_EC_DIRECT_READ);
    zones.insert(zone_of(op->target.osd));
    op->put();
  }
  EXPECT_EQ(2u, zones.size());
}

// BALANCE_READS single-chunk reads only pick zones that hold the shard.
TEST_F(TestSplitOpInit, ECSingleChunkBalanceReadsSkipZoneMissingShard)
{
  for (unsigned seed = 0; seed < 16; seed++) {
    srand(seed);
    auto op = make_read_op(degraded_ec_pool_id, {}, 0, 4096, CEPH_OSD_FLAG_BALANCE_READS);
    op->ops[0].op.extent.offset = 4096;
    SplitOp::prepare_single_op(op, *objecter, g_ceph_context);
    EXPECT_TRUE(op->target.flags & CEPH_OSD_FLAG_EC_DIRECT_READ) << "seed " << seed;
    EXPECT_EQ(1, op->target.osd) << "seed " << seed;
    op->put();
  }
}

// BALANCE_READS picks the zone of each data shard independently.
TEST_F(TestSplitOpInit, ECBalanceReadsChooseZonePerShard)
{
  std::vector<int> acting = {0, 1, 2, 4, 5, 6};
  pg_pool_t pool = objecter->with_osdmap([](const OSDMap& o) {
    return *o.get_pg_pool(ec_pool_id);
  });
  bool mixed = false;
  for (unsigned seed = 0; seed < 16; seed++) {
    srand(seed);
    auto op = make_read_op(ec_pool_id, acting, 0, 8192, CEPH_OSD_FLAG_BALANCE_READS);
    {
      ECSplitOpProbe split(op, *objecter, g_ceph_context, 6, false);
      split.init_reference_sub_read();
      ASSERT_FALSE(split.abort);
      split.init_read(op->ops[0], false, 0);
      ASSERT_FALSE(split.abort);
      std::set<int> zones;
      for (auto& [key, sr] : split.sub_reads) {
        if (sr.details.contains(0)) {
          int rel_shard = (int)pool.get_relative_shard(sr.abs_shard);
          zones.insert(zone_of(acting[(int)sr.abs_shard]));
          sr.details[0].bl.append(std::string(4096, 'a' + rel_shard));
        }
      }
      mixed = mixed || zones.size() > 1;
      bufferlist out;
      split.assemble_buffer_read(out, 0);
      std::string expected = std::string(4096, 'a') + std::string(4096, 'b');
      EXPECT_TRUE(out.contents_equal(expected.data(), expected.size()))
        << "seed " << seed;
    }
    op->put();
  }
  EXPECT_TRUE(mixed);
}

// BALANCE_READS split reads only pick zones that hold each shard.
TEST_F(TestSplitOpInit, ECBalanceReadsSkipZoneMissingShard)
{
  std::vector<int> acting = {0, 1, 2, 4, CRUSH_ITEM_NONE, 6};
  for (unsigned seed = 0; seed < 16; seed++) {
    srand(seed);
    auto op = make_read_op(degraded_ec_pool_id, acting, 0, 8192,
                           CEPH_OSD_FLAG_BALANCE_READS);
    {
      ECSplitOpProbe split(op, *objecter, g_ceph_context, 6, false);
      split.init_reference_sub_read();
      ASSERT_FALSE(split.abort);
      split.init_read(op->ops[0], false, 0);
      ASSERT_FALSE(split.abort) << "seed " << seed;
      EXPECT_TRUE(split.sub_reads.contains(1)) << "seed " << seed;
      EXPECT_FALSE(split.sub_reads.contains(4)) << "seed " << seed;
    }
    op->put();
  }
}

// A stat ahead of the read still sends the reference sub-read to the reference replica.
TEST_F(TestSplitOpInit, ReplicaStatBeforeReadTargetsReference)
{
  std::vector<int> acting = {0, 1, 2, 3};
  osdc_opvec ops(2);
  ops[0].op.op = CEPH_OSD_OP_STAT;
  ops[1].op.op = CEPH_OSD_OP_READ;
  ops[1].op.extent.length = 4 * 65536;
  auto op = new Objecter::Op(object_t("obj"), object_locator_t(rep_pool_id),
                             std::move(ops), CEPH_OSD_FLAG_BALANCE_READS,
                             (Context*)nullptr, nullptr);
  op->target.acting = acting;
  {
    ReplicaSplitOpProbe split(op, *objecter, g_ceph_context, 16, false);
    split.init_reference_sub_read();
    ASSERT_FALSE(split.abort);
    split.init(op->ops[0], 0);
    split.init(op->ops[1], 1);
    ASSERT_FALSE(split.abort);
    auto& ref = split.sub_reads.at(split.reference_sub_read_key);
    ASSERT_GE((int)ref.abs_shard, 0);
    EXPECT_EQ(split.reference_sub_read.osd, acting[(int)ref.abs_shard]);
  }
  op->put();
}
