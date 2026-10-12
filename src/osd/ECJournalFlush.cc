// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// SPDX-License-Identifier: LGPL-2.1-or-later
#include "ECJournalFlush.h"

namespace ECWriteJournal {

bool eligible_overwrite(const hobject_t& object,
  const PGTransaction::ObjectOperation& update, const object_info_t& new_oi,
  const SnapSet& snapset, uint64_t stripe_width)
{
  // finish_ctx always sets both attrs on head writes. Other attrs and real
  // snapshot state remain outside this experiment. A write that records a
  // whole-object data digest is excluded too: a scrub between ACK and flush
  // would compare that digest against the not yet materialized shards.
  if (object.snap != CEPH_NOSNAP || !update.is_none() || update.truncate ||
      update.updated_snaps || update.alloc_hint || update.clear_omap ||
      update.omap_header || !update.omap_updates.empty() ||
      update.attr_updates.size() != 2 ||
      !update.attr_updates.contains(OI_ATTR) || !update.attr_updates.at(OI_ATTR) ||
      !update.attr_updates.contains(SS_ATTR) || !update.attr_updates.at(SS_ATTR) ||
      update.buffer_updates.empty() || !stripe_width ||
      new_oi.is_data_digest() || snapset.seq != 0 || !snapset.clones.empty()) {
    return false;
  }
  auto i = update.buffer_updates.begin();
  const auto* write = std::get_if<PGTransaction::ObjectOperation::BufferUpdate::Write>(
    &i.get_val());
  const auto off = i.get_off();
  const auto len = i.get_len();
  return ++i == update.buffer_updates.end() && write &&
    len && len % block_size == 0 && write->buffer.length() == len &&
    off % block_size == 0 && len <= stripe_width - off % stripe_width &&
    off <= new_oi.size && len <= new_oi.size - off;
}

bool candidate_write(const hobject_t& object, const std::vector<OSDOp>& ops,
  const ObjectState& obs, const SnapSet& snapset, snapid_t snap_seq,
  uint64_t stripe_width)
{
  if (object.snap != CEPH_NOSNAP || ops.size() != 1 || !stripe_width ||
      !obs.exists || obs.oi.is_whiteout() || obs.oi.has_manifest() ||
      snapset.seq != 0 || !snapset.clones.empty() || snap_seq != 0) {
    return false;
  }
  // A record leaves the object's info alone until the flush. A data digest
  // in it (the object was last written whole) would then describe bytes the
  // object no longer has: a whole-object read verifies its data against it.
  // The ordinary write this becomes instead clears the digest, and the
  // object's later writes are journaled.
  if (obs.oi.is_data_digest()) {
    return false;
  }
  const auto& op = ops.front();
  if (op.op.op != CEPH_OSD_OP_WRITE || op.op.extent.truncate_seq != 0) {
    return false;
  }
  const uint64_t off = op.op.extent.offset;
  const uint64_t len = op.op.extent.length;
  // A write of the whole object records a data digest, which the journal
  // cannot keep true until the flush (eligible_overwrite refuses it).
  return len && len % block_size == 0 && off % block_size == 0 &&
    op.indata.length() == len && len <= stripe_width - off % stripe_width &&
    off <= obs.oi.size && len <= obs.oi.size - off &&
    !(off == 0 && len == obs.oi.size);
}

bool overwrite_fits(uint64_t offset, uint64_t length, uint64_t new_size,
  uint64_t old_size)
{
  return new_size == old_size && offset <= old_size &&
    length <= old_size - offset;
}

ECTransaction::WritePlanObj plan_flush(
  const Stripe& stripe, const ECUtil::stripe_info_t& sinfo,
  const shard_id_set& shards, bool object_in_cache, unsigned pdw_write_mode)
{
  ceph_assert(stripe.width == sinfo.get_k() * sinfo.get_chunk_size());
  ceph_assert(stripe.offset % stripe.width == 0);
  ceph_assert(stripe.object_size >= stripe.offset + stripe.valid_bytes);
  PGTransaction::ObjectOperation updates;
  for (const auto& [off, data] : stripe.blocks) {
    updates.buffer_updates.insert(stripe.offset + off, data.length(),
      PGTransaction::ObjectOperation::BufferUpdate::Write{data, 0});
  }
  object_info_t oi;
  oi.size = stripe.object_size;
  return ECTransaction::WritePlanObj(stripe.object, updates, sinfo, shards,
    shards, object_in_cache, oi.size, oi, std::nullopt, pdw_write_mode);
}

ECUtil::shard_extent_map_t flush_data(
  const Stripe& stripe, const ECUtil::stripe_info_t& sinfo,
  const ECTransaction::WritePlanObj& plan,
  const ECUtil::shard_extent_map_t& read,
  const ceph::ErasureCodeInterfaceRef& ec_impl, DoutPrefixProvider* dpp)
{
  if (!plan.do_parity_delta_write) {
    auto data = assemble_flush(stripe, sinfo, read);
    data.insert_parity_buffers();
    const int encoded = data.encode(ec_impl, dpp);
    ceph_assert(encoded == 0);
    return data;
  }
  // As ECTransaction::Generate does for a client parity-delta write: the new
  // blocks, completed to whole pages from the old chunks, then the parity
  // delta applied to the old parity.
  ECUtil::shard_extent_map_t old = read;
  ECUtil::shard_extent_map_t data(&sinfo);
  for (const auto& [off, bytes] : stripe.blocks) {
    ceph::bufferlist block = bytes;
    sinfo.ro_range_to_shard_extent_map(
      stripe.offset + off, block.length(), block, data);
  }
  old.zero_pad(plan.will_write);
  data.pad_with_other(plan.will_write, old);
  const int encoded = data.encode_parity_delta(ec_impl, old, dpp);
  ceph_assert(encoded == 0);
  data.zero_pad(plan.will_write);
  return data;
}

ECUtil::shard_extent_map_t assemble_flush(
  const Stripe& stripe, const ECUtil::stripe_info_t& sinfo,
  const ECUtil::shard_extent_map_t& base)
{
  ECUtil::shard_extent_map_t data = base;
  for (const auto& [off, bytes] : stripe.blocks) {
    ceph::bufferlist block = bytes;
    sinfo.ro_range_to_shard_extent_map(
      stripe.offset + off, block.length(), block, data);
  }
  if (stripe.valid_bytes < stripe.width) {
    ceph::bufferlist zeros;
    zeros.append_zero(stripe.width - stripe.valid_bytes);
    sinfo.ro_range_to_shard_extent_map(
      stripe.offset + stripe.valid_bytes, zeros.length(), zeros, data);
  }
  // The planner subtracts the cache's do_not_read set from its reads, so a
  // hole below EOF (never written, or created by an earlier size extension)
  // arrives as an absent range. Those bytes are zero, and the parity must be
  // computed over full chunks: pad like ECTransaction::Generate does.
  const uint64_t chunk_off = stripe.offset / sinfo.get_k();
  for (auto shard : sinfo.get_data_shards()) {
    data.zero_pad(shard, chunk_off, sinfo.get_chunk_size());
  }
  return data;
}

} // namespace ECWriteJournal