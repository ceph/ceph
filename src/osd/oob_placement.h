// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstdint>
#include <map>
#include <vector>

/**
 * Placement plans for out-of-band (RDMA) transfers between an OSD's
 * memory and a client's registered memory window.
 *
 * A plan is a correspondence, not a direction: each triple says
 * "local bytes [local_ofs, +len) correspond to client window offset
 * client_ofs". Today the only executor is the read path, which
 * RDMA-writes local reply data into the client window, but nothing
 * in a plan presumes that - the same geometry describes an OSD
 * RDMA-reading its share of a write payload out of client memory,
 * so the builders and their tests are reusable in that direction.
 * Direction belongs to the executor, not to the plan.
 *
 * The builders are pure functions so the interleave math can be
 * unit-tested without an OSD or an RDMA stack; the cuObject executor
 * consumes the plan verbatim.
 */
namespace ceph::osd::oob {

struct placement_triple {
  uint64_t local_ofs;   ///< offset into the OSD-side buffer
  uint64_t client_ofs;  ///< offset into the client's memory window
  uint64_t len;

  bool operator==(const placement_triple&) const = default;
};

using placement_plan = std::vector<placement_triple>;

/// The whole local extent corresponds to one contiguous range
/// starting at base_offset.
placement_plan linear_plan(uint64_t base_offset, uint64_t data_len);

/**
 * EC direct interleave: the local buffer holds this shard's chunks
 * of the logical range [ro_off, ro_off+ro_len) in ascending stripe
 * order (partial first/last chunks included); each chunk corresponds
 * to base_offset + (chunk_ro_start - ro_off), so the shards'
 * concurrent transfers interleave into the client's logical view.
 * data_len clips the plan to the bytes actually present (short shard
 * reads at EOF).
 *
 * chunk_size and k are the pool's stripe geometry; raw_shard is this
 * OSD's raw (data-order) shard index.
 */
placement_plan ec_direct_plan(uint64_t base_offset,
			      uint64_t ro_off, uint64_t ro_len,
			      uint64_t chunk_size, uint32_t k,
			      uint32_t raw_shard, uint64_t data_len);

/**
 * Sparse-read plan: the local data blob packs the extents
 * back-to-back in map order; each extent corresponds to
 * base_offset + (extent_offset - ro_off). Extent offsets are logical
 * object offsets (replicated pools and EC primary reads; EC direct
 * sparse reads carry shard-space maps and must not use this).
 */
placement_plan sparse_plan(uint64_t base_offset, uint64_t ro_off,
			   const std::map<uint64_t, uint64_t>& extents,
			   uint64_t data_len);

} // namespace ceph::osd::oob
