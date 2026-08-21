// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "osd/oob_placement.h"

#include <random>

#include "gtest/gtest.h"

using namespace ceph::osd::oob;

TEST(OobPlacement, Linear)
{
  EXPECT_TRUE(linear_plan(100, 0).empty());
  auto plan = linear_plan(100, 4096);
  ASSERT_EQ(1u, plan.size());
  EXPECT_EQ((placement_triple{0, 100, 4096}), plan[0]);
}

TEST(OobPlacement, Sparse)
{
  std::map<uint64_t, uint64_t> extents{{4096, 512}, {16384, 1024}};
  auto plan = sparse_plan(1000, 4096, extents, 512 + 1024);
  ASSERT_EQ(2u, plan.size());
  EXPECT_EQ((placement_triple{0, 1000 + 0, 512}), plan[0]);
  EXPECT_EQ((placement_triple{512, 1000 + (16384 - 4096), 1024}), plan[1]);

  // short data blob clips the plan
  auto clipped = sparse_plan(0, 4096, extents, 512 + 100);
  ASSERT_EQ(2u, clipped.size());
  EXPECT_EQ(100u, clipped[1].len);
}

// Oracle: the client-side stripe walk from osdc/SplitOp.h
// (ECStripeIterator), restated: walk the logical range chunk by
// chunk; each chunk belongs to raw shard (chunk_index % k) and the
// client assembles each shard's reply buffer strictly sequentially.
// The per-shard plans must tile the logical range exactly.
static void check_against_oracle(uint64_t ro_off, uint64_t ro_len,
				 uint64_t chunk_size, uint32_t k)
{
  // per-shard plans with unbounded data
  std::vector<placement_plan> plans;
  for (uint32_t s = 0; s < k; s++) {
    plans.push_back(ec_direct_plan(0, ro_off, ro_len, chunk_size, k, s,
				   UINT64_MAX));
  }

  // oracle walk
  std::vector<uint64_t> consumed(k, 0);   // per-shard reply cursor
  std::vector<size_t> next_triple(k, 0);
  uint64_t ro = ro_off;
  while (ro < ro_off + ro_len) {
    const uint64_t chunk = ro / chunk_size;
    const uint32_t shard = chunk % k;
    const uint64_t chunk_end = std::min((chunk + 1) * chunk_size,
					ro_off + ro_len);
    const uint64_t len = chunk_end - ro;

    ASSERT_LT(next_triple[shard], plans[shard].size())
      << "shard " << shard << " plan too short at ro " << ro;
    const auto& t = plans[shard][next_triple[shard]++];
    // reply bytes are consumed strictly sequentially per shard
    EXPECT_EQ(consumed[shard], t.reply_data_ofs);
    // and land at the chunk's logical position within the range
    EXPECT_EQ(ro - ro_off, t.client_ofs);
    EXPECT_EQ(len, t.len);
    consumed[shard] += len;
    ro += len;
  }
  for (uint32_t s = 0; s < k; s++) {
    EXPECT_EQ(next_triple[s], plans[s].size())
      << "shard " << s << " plan has extra triples";
  }
}

TEST(OobPlacement, EcDirectMatchesStripeWalk)
{
  // aligned full stripes
  check_against_oracle(0, 4 * 16384, 16384, 4);
  // partial first and last chunks
  check_against_oracle(1000, 100000, 16384, 4);
  // sub-chunk read (single chunk, single shard)
  check_against_oracle(16384 + 5, 100, 16384, 4);
  // range spanning a stripe wrap back onto the first shard
  check_against_oracle(16384 * 3, 16384 * 6, 16384, 4);
  // k=2, non-power-of-two chunk size
  check_against_oracle(999, 123456, 24576, 2);
  // randomized sweep
  std::mt19937_64 rng(42);
  for (int i = 0; i < 200; i++) {
    const uint64_t chunk = 4096 << (rng() % 4);
    const uint32_t k = 2 + rng() % 6;
    const uint64_t off = rng() % (chunk * k * 3);
    const uint64_t len = 1 + rng() % (chunk * k * 4);
    check_against_oracle(off, len, chunk, k);
  }
}

TEST(OobPlacement, EcDirectClipsToDataLen)
{
  // shard 0 of k=2, chunk 16384, range covers chunks 0..3 so shard 0
  // owns chunks 0 and 2 (16384 bytes each); a short reply of 20000
  // bytes must clip the second triple
  auto plan = ec_direct_plan(0, 0, 4 * 16384, 16384, 2, 0, 20000);
  ASSERT_EQ(2u, plan.size());
  EXPECT_EQ(16384u, plan[0].len);
  EXPECT_EQ((uint64_t)(20000 - 16384), plan[1].len);
  EXPECT_EQ(2u * 16384u, plan[1].client_ofs);

  // and an empty reply yields an empty plan
  EXPECT_TRUE(ec_direct_plan(0, 0, 4 * 16384, 16384, 2, 0, 0).empty());
}
