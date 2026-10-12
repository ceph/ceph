// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// SPDX-License-Identifier: LGPL-2.1-or-later
#pragma once

#include "ECWriteJournal.h"
#include "ECTransaction.h"
#include "object_state.h"
#include "common/options.h"

namespace ECWriteJournal {

inline bool supported_geometry(unsigned k, unsigned m, uint64_t chunk_size)
{
  return (k == 4 || k == 8 || k == 12) && m == 3 && chunk_size == 4096;
}

// A PG's journal has slots_per_pg slot objects, one segment each, and the
// budget osd_ec_journal_max_bytes is split evenly among them.
inline constexpr uint64_t slots_per_pg = 16;

// Keep config extraction shared with tests: size and uint are distinct option
// alternatives even though both convert to uint64_t.
template<typename Config>
Limits read_limits(const Config& conf, uint64_t stripe_width)
{
  Limits limits;
  limits.stripe_width = stripe_width;
  limits.max_bytes = conf.template get_val<Option::size_t>(
    "osd_ec_journal_max_bytes");
  limits.max_segments = slots_per_pg;
  limits.segment_bytes = limits.max_bytes / slots_per_pg;
  return limits;
}

// Structural admission check at submission: a head object, exactly the two
// attrs finish_ctx writes, no snapshot state, a single write of whole 1 KiB
// blocks inside one stripe. new_oi/snapset are the OBC projections finish_ctx
// already encoded into the attrs, so nothing is decoded on the write path.
// The size half of the check is overwrite_fits(), evaluated at admission.
bool eligible_overwrite(const hobject_t& object,
  const PGTransaction::ObjectOperation& update, const object_info_t& new_oi,
  const SnapSet& snapset, uint64_t stripe_width);

// Admission in do_op, before the op is given a version: a single plain
// WRITE of whole 1 KiB blocks inside one stripe of an existing head object
// with no snapshot state, leaving its size unchanged. It predicts what
// eligible_overwrite() accepts at submission, so an admitted op is appended
// at once and never waits with a version assigned (a flush, which is a
// logged write, would otherwise take a later version and log ahead of it).
bool candidate_write(const hobject_t& object, const std::vector<OSDOp>& ops,
  const ObjectState& obs, const SnapSet& snapset, snapid_t snap_seq,
  uint64_t stripe_width);

// A journaled record never changes the object size and must sit inside the
// object as projected by every earlier write. old_size must come from the
// same source the write planner uses once all earlier ops have entered the
// pipeline (extent cache projection, else attr cache), not from an OBC that
// a still-queued size change has not reached yet.
bool overwrite_fits(uint64_t offset, uint64_t length, uint64_t new_size,
  uint64_t old_size);

// Plan a flush with the classic write planner, so a flush costs what a
// classic write of the same blocks would: a complete stripe is written whole
// with no reads; an incomplete one writes only the chunks it changes plus
// parity, choosing between a parity-delta write (read the old chunks and
// parity) and a conventional one (read the untouched data chunks) the way
// the planner does for client writes, by reads needed. object_in_cache must
// be the extent cache's view, as for a client write. pdw_write_mode 1 forces
// a conventional plan (0 = the planner's choice, 2 = parity delta).
// object_info.size is never changed by a flush.
ECTransaction::WritePlanObj plan_flush(
  const Stripe& stripe, const ECUtil::stripe_info_t& sinfo,
  const shard_id_set& shards, bool object_in_cache = false,
  unsigned pdw_write_mode = 0);

// The shard data a flush writes, encoded for its plan: parity from the whole
// stripe (the planner's reads plus the journaled blocks, see assemble_flush)
// for a conventional plan, or a parity delta against the old chunks and
// parity it read. Covers at least plan.will_write; the caller writes those
// extents and hands the whole map to the extent cache.
ECUtil::shard_extent_map_t flush_data(
  const Stripe& stripe, const ECUtil::stripe_info_t& sinfo,
  const ECTransaction::WritePlanObj& plan,
  const ECUtil::shard_extent_map_t& read,
  const ceph::ErasureCodeInterfaceRef& ec_impl, DoutPrefixProvider* dpp);

// Overlay journal data onto the planner's read result. Ranges the planner did
// not supply are zeros by the extent cache contract (object holes and regions
// created by a size extension are never read), exactly as
// ECTransaction::Generate zero-pads them; bytes beyond EOF are zeroed too.
ECUtil::shard_extent_map_t assemble_flush(
  const Stripe& stripe, const ECUtil::stripe_info_t& sinfo,
  const ECUtil::shard_extent_map_t& base);

} // namespace ECWriteJournal