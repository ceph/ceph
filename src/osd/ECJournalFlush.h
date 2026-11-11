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
  // Requests remembered for duplicate detection: the PG log's window.
  limits.request_window = conf.template get_val<uint64_t>("osd_pg_log_dups_tracked") +
    conf.template get_val<uint64_t>("osd_min_pg_log_entries");
  return limits;
}

// Admission in do_op, before the op is given a version: a single plain
// WRITE of whole 4 KiB blocks inside one stripe (or across one stripe
// boundary, see max_cross) of an existing head object
// with no snapshot state and no data digest, leaving its size unchanged. It
// is the journal's only test of a write: an admitted op becomes a record
// right there and never waits with a version assigned (a flush, which is a
// logged write, would otherwise take a later version and log ahead of it).
// max_cross: a write that crosses a stripe boundary is a candidate too if it
// is no longer than this and than a stripe, so it lies in two stripes
// (osd_ec_journal_max_write_bytes; 0: none). It is journaled as one
// record per stripe (stripe_fragments).
bool candidate_write(const hobject_t& object, const std::vector<OSDOp>& ops,
  const ObjectState& obs, const SnapSet& snapset, snapid_t snap_seq,
  uint64_t stripe_width, uint64_t max_cross = 0);

// A write the journal does not take that still runs at once, as an ordinary
// EC write beside the object's journaled stripes (ECBackend::journal_admit):
// one or more plain WRITEs inside an existing head object with no snapshot
// state, leaving its size unchanged, at any alignment and over any number of
// stripes. It takes the journaled stripes it overlaps with it
// (Journal::take_range, absorb_runs). Anything else (a truncate, a delete,
// a size change, an op that also reads, snapshot state) waits until the
// object has no journaled data. *offset, *length: the range its writes span.
bool plain_overwrite(const hobject_t& object, const std::vector<OSDOp>& ops,
  const ObjectState& obs, const SnapSet& snapset, snapid_t snap_seq,
  uint64_t* offset, uint64_t* length);

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