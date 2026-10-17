// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "osd/oob_placement.h"

#include <random>
#include <string>
#include <utility>

#include "common/crc64nvme.h"
#include "common/rdma_token.h"
#include "include/buffer.h"
#include "osd/ECUtil.h"
#include "gtest/gtest.h"

using namespace ceph::osd::oob;
using ceph::rdma::oob_result_t;

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
    // local bytes are consumed strictly sequentially per shard
    EXPECT_EQ(consumed[shard], t.local_ofs);
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

TEST(OobPlacement, EcDirectParityShardPlacesNothing)
{
  // a parity shard holds none of the data: a direct read of it is never
  // placed
  EXPECT_TRUE(ec_shard_chunks(0, 4 * 16384, 16384, 4, 4).empty());
  EXPECT_TRUE(ec_direct_plan(0, 0, 4 * 16384, 16384, 4, 5, UINT64_MAX)
		.empty());
  EXPECT_TRUE(ec_shard_chunks(0, 100, 16384, 0, 0).empty());
}

namespace {

/// the shard extent that a shard-direct read of [off, +len) reads, as
/// ECBackend::extent_to_shard_extent() computes it, with the same
/// stripe_info_t calls
std::pair<uint64_t, uint64_t> shard_read_extent(
  const ECUtil::stripe_info_t& sinfo, shard_id_t shard, uint64_t off,
  uint64_t len)
{
  const uint64_t chunk_size = sinfo.get_chunk_size();
  const uint64_t start_chunk = off / chunk_size;
  const uint64_t end_chunk = (off + len - 1) / chunk_size;
  const raw_shard_id_t raw_shard = sinfo.get_raw_shard(shard);
  if (end_chunk == start_chunk) {
    return {sinfo.ro_offset_to_shard_offset(off, raw_shard), len};
  }
  ECUtil::shard_extent_set_t full_read(sinfo.get_k_plus_m());
  sinfo.ro_range_to_shard_extent_set(off, len, full_read);
  const uint64_t shard_offset = full_read[shard].range_start();
  return {shard_offset, full_read[shard].range_end() - shard_offset};
}

/// true when a read of [off, +len) reads data of shard: the client sends
/// a shard-direct read only to such a shard (SplitOp)
bool shard_holds_data(const ECUtil::stripe_info_t& sinfo, shard_id_t shard,
		      uint64_t off, uint64_t len)
{
  ECUtil::shard_extent_set_t full_read(sinfo.get_k_plus_m());
  sinfo.ro_range_to_shard_extent_set(off, len, full_read);
  return full_read.contains(shard);
}

/**
 * Every data shard's plan against what its read returns: the walk's
 * chunks map each shard offset to the logical offset that stripe_info_t
 * gives it, and the plan starts at the first byte of the extent that
 * extent_to_shard_extent() reads, covers it back to back, and puts each
 * of its bytes at that byte's logical offset in the window.
 */
void check_against_read(const ECUtil::stripe_info_t& sinfo, uint64_t off,
			uint64_t len, uint64_t base)
{
  const uint64_t chunk_size = sinfo.get_chunk_size();
  const uint32_t k = sinfo.get_k();
  for (uint32_t r = 0; r < k; r++) {
    const raw_shard_id_t raw_shard(r);
    const shard_id_t shard = sinfo.get_shard(raw_shard);
    SCOPED_TRACE("off " + std::to_string(off) + " len " +
		 std::to_string(len) + " chunk " + std::to_string(chunk_size) +
		 " k " + std::to_string(k) + " raw shard " + std::to_string(r));
    const auto chunks = ec_shard_chunks(off, len, chunk_size, k, r);
    for (const auto& c : chunks) {
      EXPECT_EQ(c.shard_off, sinfo.ro_offset_to_shard_offset(c.ro_off,
							      raw_shard));
      EXPECT_EQ(c.ro_off, sinfo.shard_offset_to_ro_offset(shard,
							   c.shard_off));
    }
    const auto plan = ec_direct_plan(base, off, len, chunk_size, k, r,
				     UINT64_MAX);
    ASSERT_EQ(chunks.size(), plan.size());
    if (!shard_holds_data(sinfo, shard, off, len)) {
      EXPECT_TRUE(plan.empty());
      continue;
    }
    ASSERT_FALSE(plan.empty());
    const auto [shard_off, shard_len] =
      shard_read_extent(sinfo, shard, off, len);
    uint64_t local = 0;
    for (const auto& t : plan) {
      EXPECT_EQ(local, t.local_ofs);
      // the first and the last byte of the triple, as read
      for (const uint64_t b : {uint64_t(0), t.len - 1}) {
	EXPECT_EQ(off + (t.client_ofs + b - base),
		  sinfo.shard_offset_to_ro_offset(shard,
						  shard_off + t.local_ofs + b));
      }
      local += t.len;
    }
    EXPECT_EQ(shard_len, local);
  }
}

} // anonymous namespace

TEST(OobPlacement, EcDirectMatchesShardRead)
{
  {
    ECUtil::stripe_info_t sinfo(4, 2, 4 * 16384);
    // aligned full stripes
    check_against_read(sinfo, 0, 4 * 16384, 0);
    // partial first and last chunks
    check_against_read(sinfo, 1000, 100000, 512);
    // inside one chunk
    check_against_read(sinfo, 16384 + 5, 100, 0);
    // across a stripe, back onto the first shard
    check_against_read(sinfo, 16384 * 3, 16384 * 6, 4096);
  }
  {
    // data shards that are not the first k shard ids
    ECUtil::stripe_info_t sinfo(3, 2, 3 * 4096,
				{shard_id_t(2), shard_id_t(4), shard_id_t(0),
				 shard_id_t(1), shard_id_t(3)});
    check_against_read(sinfo, 100, 5 * 4096, 0);
    check_against_read(sinfo, 4096 * 2 + 1, 4096 * 7 + 3, 64);
  }
  std::mt19937_64 rng(131);
  for (int i = 0; i < 500; i++) {
    const uint64_t chunk = 4096 << (rng() % 4);
    const uint32_t k = 2 + rng() % 7;
    ECUtil::stripe_info_t sinfo(k, 2, chunk * k);
    const uint64_t off = rng() % (chunk * k * 3);
    const uint64_t len = 1 + rng() % (chunk * k * 4);
    check_against_read(sinfo, off, len, rng() % 65536);
  }
}

namespace {

ceph::buffer::list random_payload(size_t len, size_t max_seg,
				  std::mt19937_64& rng)
{
  ceph::buffer::list bl;
  for (size_t pos = 0; pos < len;) {
    const size_t n = std::min<size_t>(1 + rng() % max_seg, len - pos);
    std::string s(n, '\0');
    for (auto& c : s) {
      c = static_cast<char>(rng());
    }
    bl.append(ceph::buffer::copy(s.data(), s.size()));
    pos += n;
  }
  return bl;
}

} // anonymous namespace

TEST(OobPlacement, ChecksumPlanOneRange)
{
  std::mt19937_64 rng(1);
  const auto payload = random_payload(100000, 7000, rng);
  const auto plan = linear_plan(4096, payload.length());
  const uint64_t want64 = ceph::crc64nvme(payload);
  const uint32_t want32c = ceph::rdma::crc32c_canonical(payload);

  oob_result_t both;
  checksum_plan(payload, plan, true, true, both);
  EXPECT_EQ(oob_result_t::FLAG_RANGES | oob_result_t::FLAG_COMBINABLE |
	    oob_result_t::FLAG_CRC64NVME | oob_result_t::FLAG_CRC32C,
	    both.flags);
  EXPECT_EQ(want64, both.crc64);
  EXPECT_EQ(want32c, both.crc32c);
  ASSERT_EQ(1u, both.ranges.size());
  EXPECT_EQ((ceph::rdma::crc_range_t{4096, payload.length(), want64,
				     want32c}), both.ranges[0]);

  oob_result_t only64;
  checksum_plan(payload, plan, true, false, only64);
  EXPECT_EQ(oob_result_t::FLAG_RANGES | oob_result_t::FLAG_COMBINABLE |
	    oob_result_t::FLAG_CRC64NVME, only64.flags);
  EXPECT_EQ(want64, only64.crc64);
  EXPECT_EQ(0u, only64.crc32c);
  ASSERT_EQ(1u, only64.ranges.size());
  EXPECT_EQ(0u, only64.ranges[0].crc32c);

  oob_result_t only32c;
  checksum_plan(payload, plan, false, true, only32c);
  EXPECT_EQ(oob_result_t::FLAG_RANGES | oob_result_t::FLAG_COMBINABLE |
	    oob_result_t::FLAG_CRC32C, only32c.flags);
  EXPECT_EQ(0u, only32c.crc64);
  EXPECT_EQ(want32c, only32c.crc32c);

  // nothing asked, nothing added
  oob_result_t none;
  none.flags = oob_result_t::FLAG_LANDED;
  checksum_plan(payload, plan, false, false, none);
  EXPECT_EQ(oob_result_t::FLAG_LANDED, none.flags);
  EXPECT_TRUE(none.ranges.empty());
}

TEST(OobPlacement, ChecksumPlanRanges)
{
  // an EC-direct plan of shard 1: one range for each chunk, each over
  // its own bytes, and the whole folded from them
  std::mt19937_64 rng(2);
  const uint64_t off = 1000, len = 4 * 4096 * 2 + 3000;
  const auto plan = ec_direct_plan(512, off, len, 4096, 4, 1, UINT64_MAX);
  ASSERT_LT(1u, plan.size());
  uint64_t total = 0;
  for (const auto& t : plan) {
    total += t.len;
  }
  const auto payload = random_payload(total, 3000, rng);
  oob_result_t res;
  checksum_plan(payload, plan, true, true, res);
  EXPECT_EQ(oob_result_t::FLAG_RANGES | oob_result_t::FLAG_CRC64NVME |
	    oob_result_t::FLAG_CRC32C, res.flags);
  ASSERT_EQ(plan.size(), res.ranges.size());
  for (size_t i = 0; i < plan.size(); i++) {
    ceph::buffer::list part;
    part.substr_of(payload, plan[i].local_ofs, plan[i].len);
    EXPECT_EQ((ceph::rdma::crc_range_t{plan[i].client_ofs, plan[i].len,
				       ceph::crc64nvme(part),
				       ceph::rdma::crc32c_canonical(part)}),
	      res.ranges[i]);
  }
  // the plan places the whole payload, in payload order
  EXPECT_EQ(ceph::crc64nvme(payload), res.crc64);
  EXPECT_EQ(ceph::rdma::crc32c_canonical(payload), res.crc32c);
}

TEST(OobPlacement, ChecksumPlanKeepsTheCrc32cCache)
{
  // checksumming a plan leaves the CRC-32C of each of the payload's
  // buffers cached, for a later digest of the same payload
  std::mt19937_64 rng(3);
  const auto payload = random_payload(1 << 20, 100000, rng);
  ASSERT_LT(1u, payload.get_num_buffers());
  oob_result_t res;
  checksum_plan(payload, linear_plan(0, payload.length()), true, true, res);
  ceph::buffer::track_cached_crc(true);
  const int hits = ceph::buffer::get_cached_crc();
  const int misses = ceph::buffer::get_missed_crc();
  EXPECT_EQ(res.crc32c, payload.crc32c(-1) ^ 0xffffffff);
  EXPECT_EQ(hits + static_cast<int>(payload.get_num_buffers()),
	    ceph::buffer::get_cached_crc());
  EXPECT_EQ(misses, ceph::buffer::get_missed_crc());
  ceph::buffer::track_cached_crc(false);
}
