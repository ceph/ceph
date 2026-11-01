// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "osd/oob_placement.h"

#include <algorithm>

#include "common/crc64nvme.h"
#include "common/rdma_token.h"
#include "include/buffer.h"

namespace ceph::osd::oob {

uint64_t plan_bytes(const placement_plan& plan)
{
  uint64_t n = 0;
  for (const auto& t : plan) {
    n += t.len;
  }
  return n;
}

placement_plan linear_plan(uint64_t base_offset, uint64_t data_len)
{
  placement_plan plan;
  if (data_len) {
    plan.push_back({0, base_offset, data_len});
  }
  return plan;
}

std::vector<ec_chunk> ec_shard_chunks(uint64_t ro_off, uint64_t ro_len,
				      uint64_t chunk_size, uint32_t k,
				      uint32_t raw_shard)
{
  std::vector<ec_chunk> chunks;
  if (ro_len == 0 || chunk_size == 0 || raw_shard >= k) {
    return chunks;
  }
  const uint64_t c0 = ro_off / chunk_size;
  const uint64_t cN = (ro_off + ro_len - 1) / chunk_size;
  // first chunk this shard owns at or after c0
  for (uint64_t c = c0 + (raw_shard + k - (c0 % k)) % k; c <= cN; c += k) {
    const uint64_t chunk_ro_start = std::max(c * chunk_size, ro_off);
    const uint64_t chunk_ro_end = std::min((c + 1) * chunk_size,
					   ro_off + ro_len);
    chunks.push_back({chunk_ro_start,
		      (c / k) * chunk_size + (chunk_ro_start - c * chunk_size),
		      chunk_ro_end - chunk_ro_start});
  }
  return chunks;
}

placement_plan ec_direct_plan(uint64_t base_offset,
			      uint64_t ro_off, uint64_t ro_len,
			      uint64_t chunk_size, uint32_t k,
			      uint32_t raw_shard, uint64_t data_len)
{
  placement_plan plan;
  const auto chunks = ec_shard_chunks(ro_off, ro_len, chunk_size, k,
				      raw_shard);
  if (chunks.empty()) {
    return plan;
  }
  // the local buffer starts where the read started on the shard
  const uint64_t read_start = chunks.front().shard_off;
  for (const auto& c : chunks) {
    const uint64_t local_ofs = c.shard_off - read_start;
    if (local_ofs >= data_len) {
      break;  // short shard read: nothing left to place
    }
    plan.push_back({local_ofs, base_offset + (c.ro_off - ro_off),
		    std::min(c.len, data_len - local_ofs)});
  }
  return plan;
}

placement_plan sparse_plan(uint64_t base_offset, uint64_t ro_off,
			   const std::map<uint64_t, uint64_t>& extents,
			   uint64_t data_len)
{
  placement_plan plan;
  uint64_t local_ofs = 0;
  for (const auto& [off, len] : extents) {
    if (len == 0) {
      continue;
    }
    if (off < ro_off || local_ofs >= data_len) {
      // malformed map or short data blob: place what we can account for
      break;
    }
    const uint64_t l = std::min(len, data_len - local_ofs);
    plan.push_back({local_ofs, base_offset + (off - ro_off), l});
    local_ofs += l;
    if (l < len) {
      break;
    }
  }
  return plan;
}

void checksum_plan(const ceph::buffer::list& payload,
		   const placement_plan& plan, bool want64, bool want32c,
		   ceph::rdma::oob_result_t& res)
{
  if (!want64 && !want32c) {
    return;
  }
  // checksum each placed range at the storage node, on its side of the
  // fabric. Every triple is one contiguous logical extent, so a caller
  // holding all of a window's ranges can fold them in offset order
  // however they were interleaved across shards
  res.ranges.reserve(plan.size());
  uint64_t whole64 = 0;
  uint32_t whole32c = 0;
  for (size_t i = 0; i < plan.size(); i++) {
    const auto& t = plan[i];
    ceph::buffer::list part;
    part.substr_of(payload, t.local_ofs, t.len);
    ceph::rdma::crc_range_t c{t.client_ofs, t.len};
    if (want64 && want32c) {
      // one walk for both, the CRC-32C seeded and finished as
      // crc32c_canonical() does
      uint32_t crc32 = 0xffffffff;
      c.crc64 = ceph::crc64nvme_crc32c(part, &crc32);
      c.crc32c = crc32 ^ 0xffffffff;
    } else if (want64) {
      c.crc64 = ceph::crc64nvme(part);
    } else {
      c.crc32c = ceph::rdma::crc32c_canonical(part);
    }
    if (want64) {
      whole64 = i == 0 ? c.crc64 :
	ceph::crc64nvme_combine(whole64, c.crc64, t.len);
    }
    if (want32c) {
      whole32c = i == 0 ? c.crc32c :
	ceph::rdma::crc32c_combine(whole32c, c.crc32c, t.len);
    }
    res.ranges.push_back(c);
  }
  res.crc64 = whole64;
  res.crc32c = whole32c;
  res.flags |= ceph::rdma::oob_result_t::FLAG_RANGES |
    (want64 ? ceph::rdma::oob_result_t::FLAG_CRC64NVME : 0) |
    (want32c ? ceph::rdma::oob_result_t::FLAG_CRC32C : 0);
  if (plan.size() == 1) {
    // one contiguous logical extent: crc64 and crc32c themselves fold
    // with the adjacent ones, so a caller needs no ranges. This is the
    // plain read, the EC-direct read whose range fits in one chunk of
    // this shard, and the pull
    res.flags |= ceph::rdma::oob_result_t::FLAG_COMBINABLE;
  }
}

} // namespace ceph::osd::oob
