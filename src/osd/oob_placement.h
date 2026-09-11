// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstdint>
#include <map>
#include <vector>

/**
 * Placement plans for out-of-band (RDMA) delivery of read replies.
 *
 * A plan maps byte ranges of an op's reply data onto offsets within
 * the client's registered memory window: each triple says "reply
 * bytes [reply_data_ofs, +len) land at client window offset
 * client_ofs". The builders are pure functions so the interleave
 * math can be unit-tested without an OSD or an RDMA stack; the
 * cuObject executor consumes the plan verbatim.
 */
namespace ceph::osd::oob {

struct placement_triple {
  uint64_t reply_data_ofs;
  uint64_t client_ofs;
  uint64_t len;

  bool operator==(const placement_triple&) const = default;
};

using placement_plan = std::vector<placement_triple>;

/// The whole reply extent lands contiguously at base_offset.
placement_plan linear_plan(uint64_t base_offset, uint64_t data_len);

/**
 * EC direct-read interleave: the reply holds this shard's chunks of
 * the logical range [ro_off, ro_off+ro_len) in ascending stripe
 * order (partial first/last chunks included); each chunk lands at
 * base_offset + (chunk_ro_start - ro_off) so the shards' concurrent
 * writes interleave into the client's logical view. data_len clips
 * the plan to the bytes actually read (short shard reads at EOF).
 *
 * chunk_size and k are the pool's stripe geometry; raw_shard is this
 * OSD's raw (data-order) shard index.
 */
placement_plan ec_direct_plan(uint64_t base_offset,
			      uint64_t ro_off, uint64_t ro_len,
			      uint64_t chunk_size, uint32_t k,
			      uint32_t raw_shard, uint64_t data_len);

/**
 * Sparse-read plan: the reply's data blob packs the extents
 * back-to-back in map order; each extent lands at
 * base_offset + (extent_offset - ro_off). Extent offsets are logical
 * object offsets (replicated pools and EC primary reads; EC direct
 * sparse reads carry shard-space maps and must not use this).
 */
placement_plan sparse_plan(uint64_t base_offset, uint64_t ro_off,
			   const std::map<uint64_t, uint64_t>& extents,
			   uint64_t data_len);

} // namespace ceph::osd::oob
