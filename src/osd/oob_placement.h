// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstdint>
#include <map>
#include <vector>

#include "include/buffer_fwd.h"

namespace ceph::rdma { struct oob_result_t; }

/**
 * Placement plans for out-of-band (RDMA) transfers between an OSD's
 * memory and a client's registered memory window.
 *
 * A plan is a correspondence, not a direction: each triple says
 * "local bytes [local_ofs, +len) correspond to client window offset
 * client_ofs". The executors consume plans in both directions: a push
 * (execute_plan()) RDMA-writes local reply data into the client
 * window, and a pull (execute_pull()) RDMA-reads a write's payload out
 * of it into local memory. Direction belongs to the executor, not to
 * the plan.
 *
 * The builders are pure functions so the interleave math can be
 * unit-tested without an OSD or an RDMA stack; the executors consume
 * the plan verbatim.
 */
namespace ceph::osd::oob {

struct placement_triple {
  uint64_t local_ofs;   ///< offset into the OSD-side buffer
  uint64_t client_ofs;  ///< offset into the client's memory window
  uint64_t len;

  bool operator==(const placement_triple&) const = default;
};

using placement_plan = std::vector<placement_triple>;

/// the bytes a plan moves: the sum of its triples' lengths. A push or a
/// pull is placed only when this is its payload's length
uint64_t plan_bytes(const placement_plan& plan);

/// The whole local extent corresponds to one contiguous range
/// starting at base_offset.
placement_plan linear_plan(uint64_t base_offset, uint64_t data_len);

/// one chunk of a raw shard's data, or the part of it that a logical
/// range covers: the logical bytes [ro_off, +len) are the shard's
/// bytes [shard_off, +len)
struct ec_chunk {
  uint64_t ro_off;
  uint64_t shard_off;
  uint64_t len;

  bool operator==(const ec_chunk&) const = default;
};

/**
 * The stripe walk of an EC pool, as ECUtil::stripe_info_t lays out the
 * stripes: the chunks of the logical range [ro_off, ro_off+ro_len) that
 * raw shard raw_shard holds, clipped to the range, in ascending order.
 * Chunk c of the object is chunk c / k of raw shard c % k, so a
 * shard's chunks of a range are contiguous on the shard: a shard-direct
 * read (ECBackend::extent_to_shard_extent()) reads them from the first
 * one's shard_off to the end of the last. None for a parity shard
 * (raw_shard >= k).
 *
 * chunk_size and k are the pool's stripe geometry; raw_shard is this
 * OSD's raw (data-order) shard index.
 */
std::vector<ec_chunk> ec_shard_chunks(uint64_t ro_off, uint64_t ro_len,
				      uint64_t chunk_size, uint32_t k,
				      uint32_t raw_shard);

/**
 * EC direct interleave: the local buffer holds what a shard-direct read
 * of the logical range [ro_off, ro_off+ro_len) returned, the shard's
 * bytes from the first of its chunks of the range on (ec_shard_chunks());
 * each chunk corresponds to base_offset + (its ro_off - ro_off), so the
 * shards' concurrent transfers interleave into the client's logical
 * view. data_len clips the plan to the bytes actually present (short
 * shard reads at EOF).
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

/**
 * The checksums of what plan placed, into res, for the CRC-64/NVME
 * (want64) and the CRC-32C (want32c) that a descriptor asked for: one
 * crc_range_t for each triple, of payload's bytes [local_ofs, +len) at
 * client_ofs, and res.crc64 and res.crc32c folded from the ranges in
 * plan order, which the builders above make contiguous and ascending in
 * payload order; with the flags that say so. A plan of one triple is
 * one contiguous logical extent, and its result is FLAG_COMBINABLE too.
 * Nothing when neither is wanted. The read push reports the checksums
 * of what it placed so, and the pull those of what it took.
 */
void checksum_plan(const ceph::buffer::list& payload,
		   const placement_plan& plan, bool want64, bool want32c,
		   ceph::rdma::oob_result_t& res);

} // namespace ceph::osd::oob
