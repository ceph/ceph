// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include <gtest/gtest.h>

#include "common/ref.h"
#include "global/global_context.h"
#include "osd/osd_types.h"

/*
 * Unit tests for the client-side split-op eligibility checks in
 * src/osdc/SplitOp.cc.
 *
 */
#include "osdc/SplitOp.cc"

namespace {

constexpr uint64_t CHUNK = 4096;
constexpr int K = 4;
constexpr int M = 2;
constexpr uint64_t STRIPE_WIDTH = CHUNK * K;

pg_pool_t make_ec_pool(int k = K, int m = M, uint64_t chunk_size = CHUNK) {
  pg_pool_t pool;
  pool.type = pg_pool_t::TYPE_ERASURE;
  pool.size = k + m;
  pool.min_size = k;
  pool.ec_data_shard_count = k;
  pool.ec_coding_shard_count = m;
  pool.set_stripe_width(chunk_size * k);
  pool.flags = pg_pool_t::FLAG_EC_OVERWRITES |
               pg_pool_t::FLAG_EC_OPTIMIZATIONS |
               pg_pool_t::FLAG_CLIENT_SPLIT_READS;
  return pool;
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
bool is_single_direct_op(osdc_opvec &&ops, const pg_pool_t &pool,
                         bool has_primary_ops = false) {
  auto op = make_op(std::move(ops));
  bool single_direct_op = pool.is_erasure();
  bool suitable_read_found = validate_operations(
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
