#include "osdc/Objecter.h"
#include "osdc/SplitOp.h"
#include "osd/osd_types.h"

#define dout_subsys ceph_subsys_objecter
#define DBG_LVL 20

namespace {
inline boost::system::error_code osdcode(int r) {
  return (r < 0) ? boost::system::error_code(-r, osd_category()) : boost::system::error_code();
}
}

constexpr static uint64_t kReplicaMinShardReadSize = 128 * 1024;
constexpr static uint64_t kReplicaMinShardReads = 2;
constexpr static uint64_t kReplicaMinReadSize = kReplicaMinShardReadSize * kReplicaMinShardReads;

#undef dout_prefix
#define dout_prefix *_dout << " ECSplitOp::"

std::pair<SplitOp::extent_set, bufferlist> ECSplitOp::assemble_buffer_sparse_read(int ops_index) {
  bufferlist bl_out;
  extent_set extents_out;


  auto &orig_osd_op = orig_op->ops[ops_index].op;
  const pg_pool_t *pi = objecter.osdmap->get_pg_pool(orig_op->target.base_oloc.pool);
  ECStripeView stripe_view(orig_osd_op.extent.offset, orig_osd_op.extent.length, pi);
  ldout(cct, DBG_LVL) << __func__ << " start:"
    << orig_osd_op.extent.offset << "~" << orig_osd_op.extent.length << dendl;

  std::vector<uint64_t> buffer_offset(stripe_view.data_chunk_count);
  mini_flat_map<shard_id_t, extents_map::iterator> map_iterators(stripe_view.data_chunk_count);

  for (auto &&chunk_info : stripe_view) {
    ldout(cct, DBG_LVL) << __func__ << " chunk: " << chunk_info << dendl;
    auto &details = sub_reads.at(chunk_info.shard).details[ops_index];

    if (!map_iterators.contains(chunk_info.shard)) {
      map_iterators.emplace(chunk_info.shard, details.e->begin());
    }

    extents_map::iterator &extent_iter = map_iterators.at(chunk_info.shard);

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
      bl.substr_of(details.bl, buffer_offset[(int)chunk_info.shard], bl_len);
      bl_out.append(bl);
      buffer_offset[(int)chunk_info.shard] += bl_len;
    }
  }

  return std::pair(extents_out, bl_out);
}

void ECSplitOp::assemble_buffer_read(bufferlist &bl_out, int ops_index) {
  auto &orig_osd_op = orig_op->ops[ops_index].op;
  const pg_pool_t *pi = objecter.osdmap->get_pg_pool(orig_op->target.base_oloc.pool);
  ECStripeView stripe_view(orig_osd_op.extent.offset, orig_osd_op.extent.length, pi);

  std::vector<uint64_t> buffer_offset(stripe_view.data_chunk_count);
  ldout(cct, DBG_LVL) << __func__ << " " << orig_osd_op.extent.offset << "~" << orig_osd_op.extent.length << dendl;

  for (auto &&chunk_info : stripe_view) {
    ldout(cct, DBG_LVL) << __func__ << " chunk info " << chunk_info << dendl;
    auto &details = sub_reads.at(chunk_info.shard).details[ops_index];
    bufferlist bl;
    bl.substr_of(details.bl, buffer_offset[(int)chunk_info.shard], chunk_info.length);
    bl_out.append(bl);
    buffer_offset[(int)chunk_info.shard] += chunk_info.length;
  }
}

void ECSplitOp::init_read(OSDOp &op, bool sparse, int ops_index) {
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
  //FIXME: This is not quite right - the ops.size() > 1 does not necessarily mean
  // that the primary is required - it could be two reads to the same shard.
  bool primary_required = count > 1 || orig_op->objver || orig_op->ops.size() > 1;

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
        sub_reads.emplace(*primary_shard, orig_op->ops.size());
      }
    }

    // No primary???  Let the normal code paths deal with this.
    if (!primary_shard) {
      ldout(cct, DBG_LVL) << __func__ <<" ABORT: Can't find primary" << dendl;
      abort = true;
    }
  }
}

ECSplitOp::ECSplitOp(Objecter::Op *op, Objecter &objecter, CephContext *cct, int count) :
  SplitOp(op, objecter, cct, count) {}

#undef dout_prefix
#define dout_prefix *_dout << " ReplicaSplitOp::"

std::pair<SplitOp::extent_set, bufferlist> ReplicaSplitOp::assemble_buffer_sparse_read(int ops_index) {
  extent_set extents_out;
  bufferlist bl_out;

  for (auto && [shard, sr] : sub_reads) {
    for (auto [off, len] : *sr.details[ops_index].e) {
      extents_out.insert(off, len);
    }
    bl_out.append(sr.details[ops_index].bl);
  }

  return std::pair(extents_out, bl_out);
}

void ReplicaSplitOp::assemble_buffer_read(bufferlist &bl_out, int ops_index) {
  for (auto && [_, sr] : sub_reads) {
    bl_out.append(sr.details[ops_index].bl);
  }
}

ReplicaSplitOp::ReplicaSplitOp(Objecter::Op *op, Objecter &objecter, CephContext *cct, int pool_size) :
  SplitOp(op, objecter, cct, pool_size) {

  // This may not actually be the primary, but since all shards are kept current
  // in replica, it does not actually matter which we choose here. Choose 0 since
  // there will always be a read to this shard.
  primary_shard = shard_id_t(0);
}

void ReplicaSplitOp::init_read(OSDOp &op, bool sparse, int ops_index) {

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
  uint64_t slice_count = std::min(length / kReplicaMinShardReadSize, osds.size());
  uint64_t chunk_size = p2roundup(length / slice_count, (uint64_t)CEPH_PAGE_SIZE);

  for (unsigned i = 0; i < osds.size() && length > 0; i++) {

    shard_id_t shard(i);
    if (!sub_reads.contains(shard)) {
      sub_reads.emplace(shard, orig_op->ops.size());
    }
    auto &sr = sub_reads.at(shard);
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

int SplitOp::assemble_rc() {
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

void SplitOp::complete() {
  if (abort) {
    return;
  }
  ldout(cct, 20) << __func__ << " entry this=" << this << dendl;

  int rc = assemble_rc();
  if (rc >= 0) {

    // In a "normal" completion, out_ops is generated in the MOSDOpReply reply
    // which we do not have here. Here we are going to mimic this behaviour
    // so as to reproduce as much as possible of the IO completion.
    std::vector out_ops(orig_op->ops.begin(), orig_op->ops.end());

    for (unsigned ops_index=0; ops_index < out_ops.size(); ++ops_index) {
      auto &out_osd_op = out_ops[ops_index];
      switch (out_osd_op.op.op) {
        case CEPH_OSD_OP_SPARSE_READ: {
          auto [extents, bl] = assemble_buffer_sparse_read(ops_index);
          encode(std::move(extents).detach(), out_osd_op.outdata);
          encode_destructively(bl, out_osd_op.outdata);
          break;
        }
        case CEPH_OSD_OP_READ: {
          assemble_buffer_read(out_osd_op.outdata, ops_index);
          break;
        }
        case CEPH_OSD_OP_GETXATTRS:
        case CEPH_OSD_OP_CHECKSUM:
        case CEPH_OSD_OP_GETXATTR: {
          out_osd_op.outdata = sub_reads.at(*primary_shard).details[ops_index].bl;
          out_osd_op.rval = sub_reads.at(*primary_shard).details[ops_index].rval;
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

static bool validate(Objecter::Op *op, bool is_erasure, CephContext *cct) {

  if ((op->target.flags & CEPH_OSD_FLAG_BALANCE_READS) == 0 ) {
    ldout(cct, DBG_LVL) << __func__ <<" REJECT: Client rejects balanced read" << dendl;
    return false;
  }

  uint64_t suitable_read_found = false;
  for (auto & o : op->ops) {
    switch (o.op.op) {
      case CEPH_OSD_OP_READ:
      case CEPH_OSD_OP_SPARSE_READ: {
        uint64_t length = o.op.extent.length;
        if ((is_erasure && length > 0) ||
            (!is_erasure && length >= kReplicaMinReadSize)) {
          suitable_read_found = true;
        }
        break;
      }
      case CEPH_OSD_OP_GETXATTRS:
      case CEPH_OSD_OP_CHECKSUM:
      case CEPH_OSD_OP_GETXATTR: {
        break; // Do not block validate.
      }
      default: {
        ldout(cct, DBG_LVL) << __func__ <<" REJECT: unsupported op" << dendl;
        return false;
      }
    }
  }

  return suitable_read_found;
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
    case CEPH_OSD_OP_GETXATTRS:
    case CEPH_OSD_OP_CHECKSUM:
    case CEPH_OSD_OP_GETXATTR: {
      shard_id_t shard = *primary_shard;
      Details &d = sub_reads.at(shard).details[ops_index];
      orig_op->pass_thru_op(sub_reads.at(shard).rd, ops_index, &d.bl, &d.rval);
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
    << " pool=" << t.base_oloc.pool
    << " pgid=" << t.actual_pgid
    << " osd=" << t.osd
    << " shard=" << (t.force_shard ? *t.force_shard : shard_id_t(-1))
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


bool SplitOp::create(Objecter::Op *op, Objecter &objecter,
  shunique_lock<ceph::shared_mutex>& sul, ceph_tid_t *ptid, int *ctx_budget, CephContext *cct) {

  auto &t = op->target;
  const pg_pool_t *pi = objecter.osdmap->get_pg_pool(t.base_oloc.pool);

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

  bool validated = validate(op, pi->is_erasure(), cct);

  if (!validated) {
    return false;
  }

  std::shared_ptr<SplitOp> split_read;

  if (pi->is_erasure()) {
    split_read = std::make_shared<ECSplitOp>(op, objecter, cct, pi->size);
  } else {
    split_read = std::make_shared<ReplicaSplitOp>(op, objecter, cct, pi->size);
  }

  if (split_read->abort) {
    ldout(cct, DBG_LVL) << __func__ <<" ABORTED 1" << dendl;return false;
    return false;
  }

  // Populate the target, to extract the acting set from it.
  t.flags &= ~CEPH_OSD_FLAG_BALANCE_READS;
  objecter._calc_target(&op->target, op);

  for (unsigned i = 0; i < op->ops.size(); ++i) {
    split_read->init( op->ops[i], i);
    if (split_read->abort) {
      break;
    }
  }

  if (split_read->abort) {
    ldout(cct, DBG_LVL) << __func__ <<" ABORTED 2" << dendl;
    return false;
  }

  ldout(cct, DBG_LVL) << __func__ <<" sub_reads ready. count=" << split_read->sub_reads.size() << dendl;

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
    sub_op->target.force_shard.emplace(shard);
    if (pi->is_erasure()) {
      sub_op->target.flags |= CEPH_OSD_FLAG_EC_DIRECT_READ;
    } else {
      sub_op->target.flags |= CEPH_OSD_FLAG_BALANCE_READS;
    }

    objecter._op_submit_with_budget(sub_op, sul, ptid, ctx_budget);
    debug_op_summary("sent_op", sub_op, cct);
  }

  ceph_assert(split_read->sub_reads.size() > 0);

  return true;
}


#undef dout_prefix
#define dout_prefix *_dout << messenger->get_myname() << ".objecter "