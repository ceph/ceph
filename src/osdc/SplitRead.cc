#include "osdc/Objecter.h"
#include "osdc/SplitRead.h"
#include "osd/osd_types.h"

#define dout_subsys ceph_subsys_objecter
#undef dout_prefix
#define dout_prefix *_dout << " SplitRead::"
#define DBG_LVL 0

namespace {
inline boost::system::error_code osdcode(int r) {
  return (r < 0) ? boost::system::error_code(-r, osd_category()) : boost::system::error_code();
}
}

void ECSplitRead::assemble_buffer(bufferlist *bl_out, int ops_index) {

  const pg_pool_t *pi = objecter.osdmap->get_pg_pool(orig_op->target.base_oloc.pool);

  auto osd_op = orig_op->ops[0].op;
  uint64_t offset = osd_op.extent.offset;
  uint64_t length = osd_op.extent.length;
  unsigned int data_chunk_count = pi->nonprimary_shards.size() + 1;
  uint64_t stripe_size = pi->get_stripe_width();
  uint32_t chunk_size = stripe_size / data_chunk_count;

  uint64_t chunk_aligned_off = offset - (offset % chunk_size);
  uint64_t tmp_len = (offset - chunk_aligned_off) + length;
  uint64_t chunk_aligned_len = ((tmp_len % chunk_size)
                    ? (tmp_len - (tmp_len % chunk_size) + chunk_size)
                    : tmp_len);

  shard_id_t shard((offset / chunk_size) % data_chunk_count);

  mini_flat_map<shard_id_t, uint64_t> shard_offset(data_chunk_count);

  for (uint64_t chunk_offset = chunk_aligned_off;
       chunk_offset < chunk_aligned_off + chunk_aligned_len;
       chunk_offset += chunk_size, ++shard) {

    if (unsigned(shard) == data_chunk_count) {
      shard = 0;
    }

    uint64_t sub_chunk_offset = std::max(chunk_offset, offset);
    uint64_t sub_chunk_len = std::min(offset + length, chunk_offset + chunk_size) - sub_chunk_offset;

    bufferlist sub_bl;
    sub_bl.substr_of(sub_reads.at(shard).details[ops_index].bl, shard_offset[shard], sub_chunk_len);
    shard_offset[shard] += sub_chunk_len;
    bl_out->append(sub_bl);
  }
}

void ReplicaSplitRead::assemble_buffer(bufferlist *bl_out, int ops_index)  {
  for (auto && [shard, sr] : sub_reads) {
    bl_out->append(sr.details[ops_index].bl);
  }
}

void SplitRead::assemble_sparse_buffer(OSDOp &out_osd_op, int ops_index) {
  bufferlist tmp_bl;
  extent_map emap;

  for (auto && [shard, sr] : sub_reads) {
    uint64_t bl_offset = 0;
    for (auto [offset, length] : *sr.details[ops_index].e) {
      ldout(cct, DBG_LVL) << __func__
        << " shard=" << shard << " extent=" << offset << "~" << length << dendl;
      bufferlist e_bl;
      e_bl.substr_of(sr.details[ops_index].bl, bl_offset, length);
      emap.insert(offset, length, e_bl);
      bl_offset += length;
    }
  }
  extent_set extents_out;
  for (auto emap_iter = emap.begin(); emap_iter != emap.end(); ++emap_iter ) {
    extents_out.insert(emap_iter.get_off(), emap_iter.get_len());
    tmp_bl.append(emap_iter.get_val());
  }
  encode(std::move(extents_out).detach(), out_osd_op.outdata);
  encode(tmp_bl, out_osd_op.outdata);
}

int SplitRead::assemble_rc() {
  int rc = 0;
  bool rc_zero = false;

  // This should only happen on a single thread.
  for (auto & [_, sub_read] : sub_reads) {
    if (rc >= 0 && sub_read.rc >= 0) {
      rc += sub_read.rc;
      if (sub_read.rc == 0) {
        rc_zero = true;
      }
    } else if (rc >= 0) {
      rc = sub_read.rc;
    } // else ignore subsequent errors.
  }

  if (rc >= 0 && rc_zero) {
    return 0;
  }

  return rc;
}

void SplitRead::complete() {
  if (abort) {
    return;
  }
  ldout(cct, 20) << __func__ << " entry this=" << this << dendl;

  int rc = assemble_rc();
  if (rc >= 0) {

    // In a "normal" completion, out_ops is generated in the MOSDOpReply reply
    // which we do not have here. So we simply copy the out_ops for now.
    // We do not do this if anything failed in the sub-ops, because we are
    // going to re-drive the original op.
    std::vector out_ops(orig_op->ops.begin(), orig_op->ops.end());
    bufferlist *bl_out = orig_op->out_bl[0];

    // FIXME: This is patch-til-works code.  What were we supposed to do?
    if (bl_out == nullptr) {
      bl_out = &orig_op->ops[0].outdata;
    }

    ceph_assert(bl_out != nullptr);
    for (unsigned i=0; i < out_ops.size(); ++i) {
      auto &out_osd_op = out_ops[i];
      switch (out_osd_op.op.op) {
        case CEPH_OSD_OP_SPARSE_READ: {
          assemble_sparse_buffer(out_osd_op, i);
          break;
        }
        case CEPH_OSD_OP_READ: {
          assemble_buffer(&out_osd_op.outdata, i);
          break;
        }
        case CEPH_OSD_OP_GETXATTRS:
        case CEPH_OSD_OP_CHECKSUM:
        case CEPH_OSD_OP_GETXATTR: {
          out_osd_op.outdata = sub_reads.at(*primary_shard).details[i].bl;
          out_osd_op.rval = sub_reads.at(*primary_shard).details[i].rval;
          break;
        }
      default: {
          ceph_abort_msg("Not supported");
          break;
        }
      }
    }

    objecter.handle_osd_op_reply2(orig_op, out_ops);

    ldout(cct, DBG_LVL) << __func__ << " success this=" << this << " rc=" << rc << dendl;
    Objecter::Op::complete(std::move(orig_op->onfinish), osdcode(rc), rc, objecter.service.get_executor());
    objecter._finish_op(orig_op, rc);
  } else {
    ldout(cct, DBG_LVL) << __func__ << " retry this=" << this << " rc=" << rc << dendl;
    objecter.op_post_submit(orig_op);
  }
}

constexpr static uint64_t MIN_SHARD_READ_SIZE = 128 * 1024;
constexpr static uint64_t MIN_SPLIT_SHARDS = 2;
constexpr static uint64_t MIN_SPLIT_OP_SIZE = MIN_SHARD_READ_SIZE * MIN_SPLIT_SHARDS;

ReplicaSplitRead::ReplicaSplitRead(Objecter::Op *op, Objecter &objecter, CephContext *cct, int pool_size) :
  SplitRead(op, objecter, cct, pool_size) {
  ceph_osd_op &osd_op = orig_op->ops[0].op;

  if (osd_op.extent.length < MIN_SPLIT_OP_SIZE) {
    ldout(cct, DBG_LVL) << __func__ <<" ABORT: IO too small" << dendl;
    abort = true;
    return;
  }

  primary_shard = shard_id_t(0);
}

void ReplicaSplitRead::init_read(OSDOp &op, bool sparse, int ops_index) {

  auto &t = orig_op->target;

  std::set<int> osds;
  for (int direct_osd : t.acting) {
    if (objecter.osdmap->exists(direct_osd)) {
      osds.insert(direct_osd);
    }
  }

  if (osds.size() < 2) {
    ldout(cct, DBG_LVL) << __func__ <<" ABORT: No OSDs" << dendl;
    abort = true;
    return;
  }

  uint64_t offset = op.op.extent.offset;
  uint64_t length = op.op.extent.length;
  uint64_t slice_count = std::min(length / MIN_SHARD_READ_SIZE, osds.size());
  uint64_t chunk_size = p2roundup(length / slice_count, (uint64_t)CEPH_PAGE_SIZE);

  for (unsigned i = 0; i < osds.size() && length > 0; i++) {

    shard_id_t shard(i);
    if (!sub_reads.contains(shard)) {
      sub_reads.emplace(shard, orig_op->ops.size());
    }
    auto &sr = sub_reads.at(shard);
    auto bl = &sr.details[i].bl;
    auto rval = &sr.details[i].rval;
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

ECSplitRead::ECSplitRead(Objecter::Op *op, Objecter &objecter, CephContext *cct, int count) :
  SplitRead(op, objecter, cct, count) {
  auto &t = op->target;
  const pg_pool_t *pi = objecter.osdmap->get_pg_pool(t.base_oloc.pool);

  // Reject if direct reads not supported by profile.
  if (!pi->has_flag(pg_pool_t::FLAG_EC_DIRECT_READS)) {
    ldout(cct, DBG_LVL) << __func__ <<" ABORT: direct reads off" << dendl;
    abort = true;
    return;
  }

  ceph_osd_op &osd_op = op->ops[0].op;

  // Ignore zero-length reads.
  if (osd_op.extent.length == 0) {
    ldout(cct, DBG_LVL) << __func__ <<" ABORT: Zero length read" << dendl;
    abort = true;
    return;
  }
}

void ECSplitRead::init_read(OSDOp &op, bool sparse, int ops_index) {
  auto &t = orig_op->target;
  const pg_pool_t *pi = objecter.osdmap->get_pg_pool(t.base_oloc.pool);

  uint64_t offset = op.op.extent.offset;
  uint64_t length = op.op.extent.length;
  uint64_t data_chunk_count = pi->nonprimary_shards.size() + 1;
  uint32_t chunk_size = pi->get_stripe_width() / data_chunk_count;
  uint64_t start_chunk = offset / chunk_size;
  // This calculation is wrong for length = 0, but such IOs should not have
  // reached here!
  ceph_assert( op.op.extent.length != 0);
  uint64_t end_chunk = (offset + op.op.extent.length - 1) / chunk_size;

  unsigned count = std::min(data_chunk_count, end_chunk - start_chunk + 1);
  bool primary_required = count > 1 || orig_op->objver;
  abort = false;
  int first_shard = start_chunk % data_chunk_count;
  // Check all shards are online.
  for (unsigned i = first_shard; i < first_shard + count; i++) {
    shard_id_t shard(i >= data_chunk_count ? i - data_chunk_count : i);
    int direct_osd = t.acting[(int)shard];
    if (t.acting_primary == direct_osd) {
      primary_shard.emplace(shard);
    }
    if (!objecter.osdmap->exists(direct_osd)) {
      ldout(cct, DBG_LVL) << __func__ <<" ABORT: Missing OSD" << dendl;
      abort = true;
      return;
    }
    if (!sub_reads.contains(shard)) {
      sub_reads.emplace(shard, orig_op->ops.size());
    }
    auto &d = sub_reads.at(shard).details[ops_index];
    if (sparse) {
      d.e.emplace();
      sub_reads.at(shard).rd.sparse_read(offset, length, &(*d.e), &d.bl, &d.rval);
    } else {
      sub_reads.at(shard).rd.read(offset, length, &d.ec, &d.bl);
    }
  }

  if (primary_required && !primary_shard) {
    for (unsigned i=0; i < t.acting.size(); ++i) {
      if (t.acting[i] == t.acting_primary) {
        primary_shard.emplace(i);
      }
    }

    // No primary???  Let the normal code paths deal with this.
    if (!primary_shard) {
      ldout(cct, DBG_LVL) << __func__ <<" ABORT: Can't find primary" << dendl;
      abort = true;
      return;
    }
  }
}

void SplitRead::init(OSDOp &op, int ops_index) {
  switch (op.op.op) {
    case CEPH_OSD_OP_SPARSE_READ: {
      init_read(op, true, ops_index);
      break;
    }
    case CEPH_OSD_OP_READ: {
      init_read(op, false, ops_index);
      break;
    }
    case CEPH_OSD_OP_GETXATTRS:
    case CEPH_OSD_OP_CHECKSUM:
    case CEPH_OSD_OP_GETXATTR: {
      shard_id_t shard = *primary_shard;
      Details &d = sub_reads.at(shard).details[ops_index];
      orig_op->copy_op(sub_reads.at(shard).rd, ops_index, &d.bl, &d.rval);
      break;
    }
    default: {
      ldout(cct, DBG_LVL) << __func__ <<" ABORT: unsupported" << dendl;
      abort = true;
      break;
    }
  }
}

namespace {
void debug_op_summary(const std::string &str, Objecter::Op *op, CephContext *cct) {
  auto &t = op->target;
  ldout(cct, DBG_LVL) << str
    << " balance_reads=" << ((t.flags & CEPH_OSD_FLAG_BALANCE_READS) != 0)
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


bool SplitRead::create(Objecter::Op *op, Objecter &objecter,
  shunique_lock<ceph::shared_mutex>& sul, ceph_tid_t *ptid, int *ctx_budget, CephContext *cct) {

  auto &t = op->target;
  const pg_pool_t *pi = objecter.osdmap->get_pg_pool(t.base_oloc.pool);

  debug_op_summary("orig_op: ", op, cct);

  // Ignore non-erasure IO or if balanced reads were not enabled.
  if ((t.flags & CEPH_OSD_FLAG_BALANCE_READS) == 0) {
    ldout(cct, DBG_LVL) << __func__ <<" REJECTED" << dendl;
    return false;
  }

  // In EC, the "original" op never supports balanced reads and indeed setting
  // it will misdirect IO in the OSD. The following logic will determine if a
  // direct read is actually possible.
  if (pi->is_erasure()) {
    t.flags &= ~CEPH_OSD_FLAG_BALANCE_READS;
  }

  std::shared_ptr<SplitRead> split_read;

  if (pi->is_erasure()) {
    split_read = std::make_shared<ECSplitRead>(op, objecter, cct, pi->size);
  } else {
    split_read = std::make_shared<ReplicaSplitRead>(op, objecter, cct, pi->size);
  }

  if (split_read->abort) {
    ldout(cct, DBG_LVL) << __func__ <<" ABORTED 1" << dendl;
    return false;
  }

  // Populate the target, to extract the acting set from it.
  t.flags &= ~CEPH_OSD_FLAG_BALANCE_READS;
  objecter._calc_target(&op->target, op);

  for (unsigned i = 0; i < op->ops.size(); ++i) {
    split_read->init( op->ops[i], i);
  }


  if (split_read->sub_reads.size() == 1 &&
    (!split_read->primary_shard || split_read->sub_reads.contains(*split_read->primary_shard))) {
    ldout(cct, DBG_LVL) << __func__ <<" single-read to primary or replica, ignore. " << dendl;
    split_read->abort = true;
  }

  if (split_read->abort) {
    ldout(cct, DBG_LVL) << __func__ <<" ABORTED 2" << dendl;
    return false;
  }

  // We are committed to doing a split read. Any re-attempts should not be either
  // split or balanced.
  for (auto && [shard, sub_read] : split_read->sub_reads) {
    auto fin = new Finisher(split_read, sub_read); // Self-destructs when called.

    version_t *objver = nullptr;
    if (split_read->primary_shard && shard == *split_read->primary_shard) {
      objver = split_read->orig_op->objver;
    }

    auto sub_op = objecter.prepare_read_op(
      t.base_oid, t.base_oloc, split_read->sub_reads.at(shard).rd, op->snapid,
      nullptr, split_read->flags, -1, fin, objver);
    sub_op->ec_shard.emplace(shard);
    sub_op->target.flags |= CEPH_OSD_FLAG_BALANCE_READS;

    debug_op_summary("sent_op: ", sub_op, cct);
    objecter._op_submit_with_budget(sub_op, sul, ptid, ctx_budget);
  }

  ceph_assert(split_read->sub_reads.size() > 0);

  return true;
}


#undef dout_prefix
#define dout_prefix *_dout << messenger->get_myname() << ".objecter "