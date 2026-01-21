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

#include "osdc/Objecter.h"
#include "osdc/SplitOp.h"
#include "osd/osd_types.h"

using namespace std::literals;

#define dout_subsys ceph_subsys_objecter
#define DBG_LVL 20

namespace {
inline boost::system::error_code osdcode(int r) {
  return (r < 0) ? boost::system::error_code(-r, osd_category()) : boost::system::error_code();
}
}

constexpr static uint64_t kReplicaMinShardReads = 2;

#undef dout_prefix
#define dout_prefix *_dout << " ECSplitOp::"

std::pair<SplitOp::extent_set, bufferlist> ECSplitOp::assemble_buffer_sparse_read(int ops_index) const {
  bufferlist bl_out;
  extent_set extents_out;


  auto &orig_osd_op = orig_op->ops[ops_index].op;
  const pg_pool_t *pi = objecter.osdmap->get_pg_pool(orig_op->target.base_oloc.pool);
  ECStripeView stripe_view(orig_osd_op.extent.offset, orig_osd_op.extent.length, pi);
  ldout(cct, DBG_LVL) << __func__ << " start:"
    << orig_osd_op.extent.offset << "~" << orig_osd_op.extent.length << dendl;

  std::vector<uint64_t> buffer_offset(stripe_view.data_chunk_count);
  mini_flat_map<int, extents_map::const_iterator> map_iterators(stripe_view.data_chunk_count);

  for (auto &&chunk_info : stripe_view) {
    shard_id_t shard = pi->get_shard(chunk_info.raw_shard);
    int shard_index = (int)shard;
    ldout(cct, DBG_LVL) << __func__ << " chunk: " << chunk_info
        << " shard: " << shard << dendl;
    auto &details = sub_reads.at(shard_index).details.at(ops_index);

    if (!map_iterators.contains(shard_index)) {
      map_iterators.emplace(shard_index, details.e->begin());
    }

    extents_map::const_iterator &extent_iter = map_iterators.at(shard_index);

    uint64_t bl_len = 0;
    while (extent_iter != details.e->end() && extent_iter->first < chunk_info.ro_offset + stripe_view.chunk_size) {
      auto [off, len] = *extent_iter;
      ldout(cct, DBG_LVL) << __func__ << " extent=" << off << "~" <<  len << dendl;
      extents_out.insert(off, len);
      bl_len += len;
      ++extent_iter;
    }

    // We try to keep the buffers together where possible.
    if (bl_len != 0) {
      bufferlist bl;
      bl.substr_of(details.bl, buffer_offset[(int)chunk_info.raw_shard], bl_len);
      bl_out.append(bl);
      buffer_offset[(int)chunk_info.raw_shard] += bl_len;
    }
  }

  return std::pair(extents_out, bl_out);
}

void ECSplitOp::assemble_buffer_read(bufferlist &bl_out, int ops_index) const {
  auto &orig_osd_op = orig_op->ops[ops_index].op;
  const pg_pool_t *pi = objecter.osdmap->get_pg_pool(orig_op->target.base_oloc.pool);
  ECStripeView stripe_view(orig_osd_op.extent.offset, orig_osd_op.extent.length, pi);

  std::vector<uint64_t> buffer_offset(stripe_view.data_chunk_count);
  ldout(cct, DBG_LVL) << __func__ << " " << orig_osd_op.extent.offset << "~" << orig_osd_op.extent.length << dendl;

  for (auto &&chunk_info : stripe_view) {
    ldout(cct, DBG_LVL) << __func__ << " chunk info " << chunk_info << dendl;
    shard_id_t shard = pi->get_shard(chunk_info.raw_shard);
    int shard_index = (int)shard;
    auto &details = sub_reads.at(shard_index).details.at(ops_index);
    uint64_t src_len = details.bl.length();
    uint64_t buf_off = buffer_offset[(int)chunk_info.raw_shard];
    if (src_len <= buf_off) {
      // Early termination: we've exhausted the available buffer data.
      // This is a normal completion scenario, not an error condition.
      break;
    }
    uint64_t len = std::min(chunk_info.length, src_len - buf_off);
    bufferlist bl;
    bl.substr_of(details.bl, buf_off, len);
    bl_out.append(bl);
    buffer_offset[(int)chunk_info.raw_shard] += len;
  }
}

void ECSplitOp::init_read(OSDOp &op, bool sparse, int ops_index) {
  auto &target = orig_op->target;
  const pg_pool_t *pi = objecter.osdmap->get_pg_pool(target.base_oloc.pool);
  uint64_t offset = op.op.extent.offset;
  uint64_t length = op.op.extent.length;
  uint64_t data_chunk_count = pi->nonprimary_shards.size() + 1;
  uint32_t chunk_size = pi->get_stripe_width() / data_chunk_count;
  uint64_t start_chunk = offset / chunk_size;
  // This calculation is wrong for length = 0, but such IOs should not have
  // reached here! Zero-length reads are rejected earlier in the validate()
  // function (see lines 551-556), which returns {false, false} for any
  // operation with length == 0, preventing it from being split and processed.
  ceph_assert( op.op.extent.length != 0);
  // No overflow: length >= 1 (asserted above), so offset + length - 1 <= offset + length.
  // Since offset and length are uint64_t, their sum cannot exceed UINT64_MAX in practice
  // due to object size limits and memory constraints. Even if offset + length == UINT64_MAX + 1
  // (impossible in uint64_t), subtracting 1 brings it back to UINT64_MAX (valid).
  uint64_t end_chunk = (offset + op.op.extent.length - 1) / chunk_size;

  unsigned count = std::min(data_chunk_count, end_chunk - start_chunk + 1);
  // Primary is required if:
  // 1. Reading from multiple chunks (count > 1)
  // 2. Version information is needed (orig_op->objver)
  // 3. There are non-read operations that need the primary
  bool has_non_read_ops = false;
  for (const auto &op : orig_op->ops) {
    if (op.op.op != CEPH_OSD_OP_READ && op.op.op != CEPH_OSD_OP_SPARSE_READ) {
      has_non_read_ops = true;
      break;
    }
  }
  bool primary_required = count > 1 || orig_op->objver || has_non_read_ops;

  int first_shard = start_chunk % data_chunk_count;
  // Check all shards are online.
  for (unsigned i = first_shard; i < first_shard + count; i++) {
    raw_shard_id_t raw_shard(i >= data_chunk_count ? i - data_chunk_count : i);
    shard_id_t shard(pi->get_shard(raw_shard));
    int shard_index = (int)shard;

    int direct_osd = target.acting[shard_index];
    if (target.actual_pgid.shard == shard) {
      reference_sub_read = shard_index;
    }
    if (!objecter.osdmap->exists(direct_osd)) {
      ldout(cct, DBG_LVL) << __func__ <<" ABORT: Missing OSD" << dendl;
      abort = true;
      return;
    }
    if (!sub_reads.contains(shard_index)) {
      sub_reads.emplace(shard_index, orig_op->ops.size() + 1);
    }
    auto &d = sub_reads.at(shard_index).details[ops_index];
    if (sparse) {
      d.e.emplace();
      sub_reads.at(shard_index).rd.sparse_read(offset, length, &(*d.e), &d.bl, &d.rval);
    } else {
      sub_reads.at(shard_index).rd.read(offset, length, &d.ec, &d.bl);
    }
  }

  if (primary_required && reference_sub_read == -1) {
    // _calc_target will have picked the primary by default on EC. The "primary"
    // on replica is an arbitrary shard.
    reference_sub_read = (int)target.actual_pgid.shard;
    sub_reads.emplace(reference_sub_read, orig_op->ops.size() + 1);
  }
  
  ceph_assert(reference_sub_read != -1);
}

ECSplitOp::ECSplitOp(Objecter::Op *op, Objecter &objecter, CephContext *cct, int count) :
  SplitOp(op, objecter, cct, count) {}

#undef dout_prefix
#define dout_prefix *_dout << " ReplicaSplitOp::"

std::pair<SplitOp::extent_set, bufferlist> ReplicaSplitOp::assemble_buffer_sparse_read(int ops_index) const {
  extent_set extents_out;
  bufferlist bl_out;

  for (auto && [acting_index, sr] : sub_reads) {
    for (auto [off, len] : *sr.details.at(ops_index).e) {
      extents_out.insert(off, len);
    }
    bl_out.append(sr.details.at(ops_index).bl);
  }

  return std::pair(extents_out, bl_out);
}

void ReplicaSplitOp::assemble_buffer_read(bufferlist &bl_out, int ops_index) const {
  for (auto && [acting_index, sr] : sub_reads) {
    bl_out.append(sr.details.at(ops_index).bl);
  }
}

void ReplicaSplitOp::init_read(OSDOp &op, bool sparse, int ops_index) {

  auto &target = orig_op->target;

  std::set<int> osds;
  for (int direct_osd : target.acting) {
    if (objecter.osdmap->exists(direct_osd)) {
      osds.insert(direct_osd);
    }
  }

  if (osds.size() < 2) {
    ldout(cct, DBG_LVL) << __func__ <<" ABORT: No OSDs" << dendl;
    abort = true;
    return;
  }

  uint64_t replica_min_shard_read_size
    = objecter.get_min_split_replica_read_size();

  uint64_t offset = op.op.extent.offset;
  uint64_t length = op.op.extent.length;
  uint64_t slice_count = std::min(length / replica_min_shard_read_size, osds.size());
  uint64_t chunk_size = p2roundup(length / slice_count, (uint64_t)CEPH_PAGE_SIZE);
  unsigned start = 0;

  if (slice_count < osds.size()) {
    start = rand() % osds.size();
  }

  for (unsigned i = start; length > 0; i = (i + 1 == osds.size()) ? 0 : i + 1) {
    int acting_index = i;
    if (!sub_reads.contains(acting_index)) {
      sub_reads.emplace(acting_index, orig_op->ops.size() + 1);
      // Set reference_sub_read to the first index we use
      if (reference_sub_read == -1) {
        reference_sub_read = acting_index;
      }
    }
    auto &sr = sub_reads.at(acting_index);
    auto bl = &sr.details[ops_index].bl;
    auto rval = &sr.details[ops_index].rval;
    uint64_t len = std::min(length, chunk_size);
    if (sparse) {
      sr.details[ops_index].e.emplace();
      sr.rd.sparse_read(offset, len, &(*sr.details[ops_index].e), bl, rval);
    } else {
      sr.rd.read(offset, len, &sr.details[ops_index].ec, bl);
    }
    offset += len;
    length -= len;
  }
}

#undef dout_prefix
#define dout_prefix *_dout << " SplitOp::"

int SplitOp::assemble_rc() const {
  int rc = 0;
  bool eagain = false;

  // Sub-reads which use a single op should re-use original op.
  ceph_assert(sub_reads.size() > 1);

  // Pick the first bad RC, otherwise return 0.
  for (auto & [index, sub_read] : sub_reads) {
    if (sub_read.rc == -EAGAIN) {
      eagain = true;
    } else if (sub_read.rc < 0) {
      return sub_read.rc;
    }

    // The non-reference indices only get reads, which only ever have zero RCs.
    if (index == reference_sub_read) {
      rc = sub_read.rc;
    }
  }

  if (eagain || version_mismatch()) {
    return -EAGAIN;
  }
  return rc;
}

bool ECSplitOp::version_mismatch() const {
  // First we need to decode the version list from the reference.
  ceph_assert(reference_sub_read != -1);
  ceph_assert(sub_reads.at(reference_sub_read).internal_version.has_value());

  std::map<shard_id_t, eversion_t> ref_vers;
  decode(ref_vers, sub_reads.at(reference_sub_read).internal_version->bl);

  for (auto & [shard_index, sub_read] : sub_reads) {
      // Reference shard can't be different to itself.
    if (shard_index == reference_sub_read) {
      continue;
    }

    ceph_assert(sub_read.internal_version.has_value());
    std::map<shard_id_t, eversion_t> shard_vers;
    decode(shard_vers, sub_read.internal_version->bl);

    shard_id_t shard(shard_index);
    if (!ref_vers.contains(shard)) {
      ldout(cct, DBG_LVL) << __func__ << ": "
         << "Reference shard version missing, failing split op." << dendl;
      return true;
    }
    if (!shard_vers.contains(shard)) {
      ldout(cct, DBG_LVL) << __func__ << ": "
         << "Shard version missing, failing split op." << dendl;
      return true;
    }
    if (ref_vers.at(shard) != shard_vers.at(shard)) {
      ldout(cct, DBG_LVL) << __func__ << ": "
               << "Primary version (" << ref_vers.at(shard) << ") != "
               << "shard version (" << shard_vers.at(shard) <<") "
               << "for shard " << shard << dendl;
      return true;
    }
  }
  return false;
}

// On a replica, no shard maintains a list of "reference" versions, so this
// simply checks that all versions are the same.
bool ReplicaSplitOp::version_mismatch() const {
  std::optional<eversion_t> ref_version;

  // OSD does not understand shards, so fills in invalid entry.
  shard_id_t shard(-1);

  for (auto & [acting_index, sub_read] : sub_reads) {
    ceph_assert(sub_read.internal_version.has_value());
    std::map<shard_id_t, eversion_t> shard_vers;
    decode(shard_vers, sub_read.internal_version->bl);

    if (!shard_vers.contains(shard)) {
      ldout(cct, DBG_LVL) << __func__ << ": "
         << "Shard version missing, failing split op." << dendl;
      return true;
    }

    if (!ref_version) {
      ref_version = shard_vers.at(shard);
    } else if (shard_vers.at(shard) != *ref_version) {
      return true;
    }
  }

  return false;
}

void SplitOp::complete() {
  // STAGE 6: complete() only runs for successfully sent operations.
  // If abort was set during creation, the split op was discarded and
  // complete() is never called. This check handles the edge case where
  // abort might be set after creation but before sending (though this
  // should not happen in the current implementation).
  if (abort) {
    return;
  }
  ldout(cct, DBG_LVL) << __func__ << " entry this=" << this << dendl;
  boost::system::error_code handler_error;

  int rc = assemble_rc();
  if (rc >= 0) {

    // In a "normal" completion, out_ops is generated in the MOSDOpReply reply
    // which we do not have here. Here we are going to mimic this behaviour
    // so as to reproduce as much as possible of the IO completion.
    std::vector out_ops(orig_op->ops.begin(), orig_op->ops.end());

    std::optional<bufferlist> read_bl;
    if (orig_op->outbl) {
      read_bl = bufferlist();
    }

    // Copy is necessary: process_op_reply_handlers() reads out_ops while
    // modifying orig_op state. Overhead is acceptable for typical 1-3 ops.
    for (unsigned ops_index=0; ops_index < out_ops.size(); ++ops_index) {
      auto &out_osd_op = out_ops[ops_index];
      switch (out_osd_op.op.op) {
        case CEPH_OSD_OP_SPARSE_READ: {
          auto [extents, bl] = assemble_buffer_sparse_read(ops_index);
          encode(std::move(extents).detach(), out_osd_op.outdata);
          encode_destructively(bl, out_osd_op.outdata);
          if (read_bl) {
            read_bl->append(out_osd_op.outdata);
          }
          break;
        }
        case CEPH_OSD_OP_READ: {
          assemble_buffer_read(out_osd_op.outdata, ops_index);
          if (read_bl) {
            read_bl->append(out_osd_op.outdata);
          }
          break;
        }
        default: {
          out_osd_op.outdata = std::move(sub_reads.at(reference_sub_read).details[ops_index].bl);
          out_osd_op.rval = sub_reads.at(reference_sub_read).details[ops_index].rval;
          break;
        }
      }
    }

    // Copied from Objecter::handle_osd_op_reply() to match correct API behaviour.
    // This is policed in the librados test harness.
    if (orig_op->outbl) {
      ceph_assert(read_bl);
      // Note: Objecter will check for a single buffer here - this will always
      //       be true. Here, there will frequently be multiple buffers due
      //       to the splitting and the original buffer still needs to be
      //       honoured.
      if (orig_op->outbl->length() == read_bl->length()) {
        // this is here to keep previous users to *relied* on getting data
        // read into existing buffers happy.  Notably,
        // libradosstriper::RadosStriperImpl::aio_read().
        ldout(cct,10) << __func__ << " copying resulting " << read_bl->length()
                      << " into existing ceph::buffer of length " << orig_op->outbl->length()
                      << dendl;
        // The following seems a little convoluted, but the assumption is that
        // there is a good reason why Sage Weil wrote it this way in Objecter.
        bufferlist t;
        t = std::move(*orig_op->outbl);
        t.invalidate_crc();  // we're overwriting the raw buffers via c_str()
        read_bl->begin().copy(read_bl->length(), t.c_str());
        orig_op->outbl->substr_of(t, 0, read_bl->length());
      } else {
        // librados insists that if it provided a buffer and the client is
        // going to be returning a buffer, that this buffer must be
        // contiguous.
        if (orig_op->outbl->length() != 0) {
          read_bl->rebuild();
        }
        orig_op->outbl->substr_of(*read_bl, 0, read_bl->length());
      }
      orig_op->outbl = 0;
    }

    handler_error = objecter.process_op_reply_handlers(orig_op, out_ops);
    ldout(cct, DBG_LVL) << __func__ << " success this=" << this << " rc=" << rc << dendl;
  }
  ldout(cct, DBG_LVL) << __func__ << " retry this=" << this << " rc=" << rc << dendl;
  objecter.op_post_split_op_complete(orig_op, handler_error, rc);
}

void SplitOp::protect_torn_reads() {
  // If multiple reads are emitted from objecter, then it is essential that
  // each read reads the same version. It is not possible to efficiently
  // guarantee this, so instead read the version along with the data and if they
  // are different, then repeat the read to the primary. Such version mismatches
  // should be rare enough that this is not a significant performance impact.
  for (auto [index, sr] : sub_reads) {
    auto &internal_version = sr.internal_version;
    internal_version = std::make_optional<InternalVersion>();
    sr.rd.get_internal_versions(&internal_version->ec, &internal_version->bl);
  }
}

void SplitOp::init(OSDOp &op, int ops_index) {
  switch (op.op.op) {
  case CEPH_OSD_OP_SPARSE_READ: {
    init_read(op, true, ops_index);
    break;
  }
  case CEPH_OSD_OP_READ: {
    init_read(op, false, ops_index);
    break;
  }
  default: {
    // Invalid ops should have been rejected in validate.
    Details &d = sub_reads.at(reference_sub_read).details[ops_index];
    orig_op->pass_thru_op(sub_reads.at(reference_sub_read).rd, ops_index, &d.bl, &d.rval);
    break;
  }
  }
}

namespace {
std::pair<bool, bool> is_single_chunk(const pg_pool_t *pi, uint64_t offset, uint64_t len) {
  if (!pi->is_erasure()) {
    return {false, false};
  }

  uint64_t stripe_width = pi->get_stripe_width();

  // Optimization: Use stripe_width / 2 as a threshold to quickly reject requests
  // that cannot fit in a single chunk. Since k (data chunks) is at least 2,
  // chunk_size = stripe_width / k <= stripe_width / 2. This early check avoids
  // the more expensive division operation (stripe_width / data_chunk_count) below.
  if (len > stripe_width / 2) {
    return {false, false};
  }
  uint64_t data_chunk_count = pi->nonprimary_shards.size() + 1;
  uint32_t chunk_size = pi->get_stripe_width() / data_chunk_count;

  // Chunk_size should never be zero, so this is paranoia.
  if (len > chunk_size || chunk_size == 0) {
    return {false, false};
  }

  uint64_t offset_to_end_of_chunk;

  // Chunk size is normally, but not always a power of 2.
  if (std::has_single_bit(chunk_size)) {
    offset_to_end_of_chunk = chunk_size - (offset & (chunk_size - 1));
  } else {
    offset_to_end_of_chunk = chunk_size - (offset % chunk_size);
  }

  if (len > offset_to_end_of_chunk) {
    return {false, false};
  }

  return {true, offset % stripe_width < chunk_size};
}

/**
 * Validate operation flags for split operation eligibility.
 *
 * Checks that the operation has the required BALANCE_READS flag and
 * is not a write operation.
 *
 * @param op The operation to validate
 * @param cct CephContext for logging
 * @return true if flags are valid, false otherwise
 */
bool validate_flags(Objecter::Op *op, CephContext *cct) {
  if ((op->target.flags & CEPH_OSD_FLAG_BALANCE_READS) == 0) {
    ldout(cct, DBG_LVL) << __func__ << " REJECT: Client rejects balanced read" << dendl;
    return false;
  }

  // We really should not have a WRITE flagged as balanced read, but out of
  // paranoia ignore it.
  if (op->target.flags & CEPH_OSD_FLAG_WRITE) {
    ldout(cct, DBG_LVL) << __func__ << " REJECT: Flagged as write!" << dendl;
    return false;
  }

  return true;
}

/**
 * Validate operation types and check read size constraints.
 *
 * Processes all operations in the op list, checking for:
 * - Supported operation types (READ, SPARSE_READ, and various metadata ops)
 * - Valid read sizes (non-zero length, meets minimum size requirements)
 * - Single chunk constraints for erasure coded pools
 *
 * @param op The operation to validate
 * @param pi Pool information
 * @param is_erasure Whether the pool is erasure coded
 * @param replica_min_read_size Minimum read size for replica pools
 * @param cct CephContext for logging
 * @param[out] has_primary_ops Set to true if primary-only ops are found
 * @param[out] single_direct_op Set to true if op can be sent directly to single OSD
 * @return true if a suitable read operation was found, false otherwise
 */
bool validate_operations(Objecter::Op *op, const pg_pool_t *pi, bool is_erasure,
                        uint64_t replica_min_read_size, CephContext *cct,
                        bool &has_primary_ops, bool &single_direct_op) {
  bool is_first_chunk = true;
  bool suitable_read_found = false;

  for (auto &o : op->ops) {
    switch (o.op.op) {
      case CEPH_OSD_OP_READ:
      case CEPH_OSD_OP_SPARSE_READ: {
        uint64_t length = o.op.extent.length;
        if (length == 0) {
          // length of zero actually means "the whole object". This code cannot
          // know the size of the object efficiently, so reject the op.
          ldout(cct, DBG_LVL) << __func__ << " REJECT: Zero length read" << dendl;
          return false;
        }
        if ((is_erasure && length > 0) ||
            (!is_erasure && length >= replica_min_read_size)) {
          suitable_read_found = true;
        }
        if (single_direct_op) {
          auto [single_chunk, first_chunk] = is_single_chunk(pi, o.op.extent.offset, o.op.extent.length);
          is_first_chunk = is_first_chunk && first_chunk;
          single_direct_op = single_direct_op && single_chunk;
        }
        break;
      }
      case CEPH_OSD_OP_GETXATTRS:
      case CEPH_OSD_OP_CHECKSUM:
      case CEPH_OSD_OP_GETXATTR:
      case CEPH_OSD_OP_STAT:
      case CEPH_OSD_OP_CMPXATTR:
      case CEPH_OSD_OP_CALL: {
        has_primary_ops = true;
        break; // Do not block validate.
      }
      default: {
        ldout(cct, DBG_LVL) << __func__ << " REJECT: unsupported op" << dendl;
        return false;
      }
    }
  }

  if (single_direct_op && has_primary_ops) {
    single_direct_op = is_first_chunk;
  }

  return suitable_read_found;
}

/**
 * Validate if an operation is eligible for split operation optimization.
 *
 * This function performs a multi-stage validation to determine if an operation
 * can be split across multiple OSDs for improved read performance:
 * 1. Validates operation flags (BALANCE_READS required, no WRITE flag)
 * 2. Validates operation types and read size constraints
 *
 * @param op The operation to validate
 * @param objecter Objecter instance for configuration access
 * @param pi Pool information
 * @param cct CephContext for logging
 * @return A pair of bools: {suitable_read_found, single_direct_op}
 *         - suitable_read_found: true if operation contains valid read(s)
 *         - single_direct_op: true if operation can be sent to single OSD
 */
std::pair<bool, bool> validate(Objecter::Op *op, Objecter &objecter,
                               const pg_pool_t *pi, CephContext *cct) {
  bool is_erasure = pi->is_erasure();

  // Validate flags
  if (!validate_flags(op, cct)) {
    return {false, false};
  }

  // Initialize state for operation validation
  bool has_primary_ops = nullptr != op->objver;
  bool single_direct_op = is_erasure;

  uint64_t replica_min_shard_read_size = objecter.get_min_split_replica_read_size();
  uint64_t replica_min_read_size = replica_min_shard_read_size * kReplicaMinShardReads;

  // Validate operations and read sizes
  bool suitable_read_found = validate_operations(op, pi, is_erasure,
                                                 replica_min_read_size, cct,
                                                 has_primary_ops, single_direct_op);

  return {suitable_read_found, single_direct_op};
}

void debug_op_summary(const std::string &str, Objecter::Op *op, CephContext *cct) {
  auto &target = op->target;
  ldout(cct, DBG_LVL) << str
    << " pool=" << target.base_oloc.pool
    << " pgid=" << target.actual_pgid
    << " osd=" << target.osd
    << " force_osd=" << ((target.flags & CEPH_OSD_FLAG_FORCE_OSD) != 0)
    << " balance_reads=" << ((target.flags & CEPH_OSD_FLAG_BALANCE_READS) != 0)
    << " ops.size()=" << op->ops.size()
    << " needs_version=" << (op->objver?"true":"false");

  for (auto && o : op->ops) {
    *_dout << " op_code=" << ceph_osd_op_name(o.op.op);
    switch (o.op.op) {
      case CEPH_OSD_OP_READ:
      case CEPH_OSD_OP_SPARSE_READ: {
        *_dout << "(" << o.op.extent.offset << "~" << o.op.extent.length << ")";
        break;
      }
      default:
      break;
    }
  }
  *_dout << dendl;
}
}

void SplitOp::prepare_single_op(Objecter::Op *op, Objecter &objecter, CephContext *cct) {
  auto &target = op->target;
  const pg_pool_t *pi = objecter.osdmap->get_pg_pool(target.base_oloc.pool);

  objecter._calc_target(&op->target, op);
  uint64_t data_chunk_count = pi->nonprimary_shards.size() + 1;
  uint32_t chunk_size = pi->get_stripe_width() / data_chunk_count;

  // Find the first read to work out where the IO goes.
  for (auto o : op->ops) {
    if (o.op.op == CEPH_OSD_OP_SPARSE_READ ||
        o.op.op == CEPH_OSD_OP_READ) {
      raw_shard_id_t raw_shard((o.op.extent.offset) / chunk_size % data_chunk_count);
      shard_id_t shard = pi->get_shard(raw_shard);
      int acting_index = (int)shard;
      if (objecter.osdmap->exists(op->target.acting[acting_index])) {
        op->target.flags |= CEPH_OSD_FLAG_EC_DIRECT_READ;
        op->target.flags |= CEPH_OSD_FLAG_FORCE_OSD;
        target.osd = target.acting[acting_index];
        target.actual_pgid.reset_shard(shard);
      }
      break;
    }
  }
  debug_op_summary("reuse_op: ", op, cct);
}

/**
 * Create and initialize a split operation for parallel reads.
 *
 * This function implements the abort flag pattern to efficiently handle
 * validation and creation failures:
 *
 * STAGE 1: Cheap validation tests (lines 643-666)
 * - Check pool exists
 * - Check split reads are enabled
 * - Validate operation types and parameters
 * - Return false immediately if validation fails (no split op created)
 *
 * STAGE 2: Create split op object (lines 668-674)
 * - Allocate ECSplitOp or ReplicaSplitOp based on pool type
 * - Constructor may detect issues and set abort flag
 *
 * STAGE 3: Check abort after construction (lines 676-679)
 * - If abort is set during construction, discard split op and return false
 * - This catches issues that can only be detected during object creation
 *
 * STAGE 4: Initialize sub-operations (lines 681-690)
 * - Call init() for each operation in the request
 * - init_read() may set abort if OSDs are missing or state is invalid
 * - Break early if abort is detected to avoid unnecessary work
 *
 * STAGE 5: Final abort check (lines 692-695)
 * - If abort was set during initialization, discard split op and return false
 * - Allows fallback to normal (non-split) operation path
 *
 * STAGE 6: Send operations (lines 697-747)
 * - Only reached if abort=false, meaning split op is valid
 * - complete() will only run for these successfully sent operations
 *
 * @return true if split op was created and sent, false to use normal operation
 */
bool SplitOp::create(Objecter::Op *op, Objecter &objecter,
  shunique_lock<ceph::shared_mutex>& sul, CephContext *cct) {

  auto &target = op->target;
  const pg_pool_t *pi = objecter.osdmap->get_pg_pool(target.base_oloc.pool);

  // STAGE 1: Cheap validation tests run first before creating split op
  if (!pi) {
    ldout(cct, DBG_LVL) << __func__ <<" REJECT: No Pool" << dendl;
    return false;
  }

  debug_op_summary("orig_op: ", op, cct);

  // Reject if direct reads not supported by profile.
  if (!pi->has_flag(pg_pool_t::FLAG_CLIENT_SPLIT_READS)) {
    ldout(cct, DBG_LVL) << __func__ <<" REJECT: split reads off" << dendl;
    return false;
  }

  auto [validated, single_op] = validate(op, objecter, pi, cct);

  if (!validated) {
    return false;
  }

  if (single_op) {
    ldout(cct, DBG_LVL) << __func__ <<" reusing original op " << dendl;
    prepare_single_op(op, objecter, cct);
    return false;
  }

  // STAGE 2: Create split op object (may set abort during construction)
  std::shared_ptr<SplitOp> split_read;

  if (pi->is_erasure()) {
    split_read = std::make_shared<ECSplitOp>(op, objecter, cct, pi->size);
  } else {
    split_read = std::make_shared<ReplicaSplitOp>(op, objecter, cct, pi->size);
  }

  // STAGE 3: Check if abort was set during construction
  if (split_read->abort) {
    ldout(cct, DBG_LVL) << __func__ <<" ABORTED 1" << dendl;
    return false;
  }

  // Populate the target, to extract the acting set from it.
  target.flags &= ~CEPH_OSD_FLAG_BALANCE_READS;
  objecter._calc_target(&op->target, op);

  // STAGE 4: Initialize sub-operations (may set abort if problems detected)
  for (unsigned i = 0; i < op->ops.size(); ++i) {
    split_read->init( op->ops[i], i);
    if (split_read->abort) {
      break;
    }
  }

  // STAGE 5: Final abort check - discard split op if problems were detected
  if (split_read->abort) {
    ldout(cct, DBG_LVL) << __func__ <<" ABORTED 2" << dendl;
    return false;
  }

  // Ideally, the validate should detect any single-op read. However, if that
  // fails, then this will catch the cases (albeit less efficiently).
  if (split_read->sub_reads.size() <= 1) {
    ldout(cct, DBG_LVL) << __func__ <<" reusing original op - inefficient" << dendl;
    prepare_single_op(op, objecter, cct);
    split_read->abort = true; // Required for destructor.
    return false;
  }

  split_read->protect_torn_reads();


  op->split_op_tids = std::make_unique<std::vector<ceph_tid_t>>(split_read->sub_reads.size());
  auto &tids = *op->split_op_tids;

  int i=0;

  // We are committed to doing a split read. Any re-attempts should not be either
  // split or balanced.
  for (auto && [index, sub_read] : split_read->sub_reads) {
    auto fin = new Finisher(split_read, sub_read); // Self-destructs when called.

    version_t *objver = nullptr;
    if (index == split_read->reference_sub_read) {
      objver = split_read->orig_op->objver;
    }

    auto sub_op = objecter.prepare_read_op(
      target.base_oid, target.base_oloc, split_read->sub_reads.at(index).rd, op->snapid,
      nullptr, split_read->flags, -1, fin, objver);

    auto &st = sub_op->target;
    st = target; // Target can start off in same state as parent.
    st.flags |= CEPH_OSD_FLAG_FORCE_OSD;
    st.flags |= CEPH_OSD_FLAG_FAIL_ON_EAGAIN;
    if (pi->is_erasure()) {
      st.flags |= CEPH_OSD_FLAG_EC_DIRECT_READ;
      st.actual_pgid.reset_shard(shard_id_t(index));
    } else {
      st.flags |= CEPH_OSD_FLAG_BALANCE_READS;
    }
    st.osd = st.acting[index];

    objecter._op_submit(sub_op, sul, &tids[i++]);

    debug_op_summary("sent_op", sub_op, cct);
  }

  ceph_assert(split_read->sub_reads.size() > 0);

  return true;
}


#undef dout_prefix
#define dout_prefix *_dout << messenger->get_myname() << ".objecter "