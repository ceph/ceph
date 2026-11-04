// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "osd/oob_read_delivery.h"

#include <algorithm>
#include <utility>

#include "common/ceph_context.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "include/ceph_assert.h"
#include "messages/MOSDECSubOpReadReply.h"
#include "messages/MOSDOpReply.h"
#include "osd/oob_executor.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "oob_read: "

namespace ceph::osd::oob {

bool read_push_possible(int op_code, const ceph::rdma::delivery_t& d,
			bool ec_direct, bool has_ec)
{
  if (d.flags & ~ceph::rdma::delivery_t::KNOWN_FLAGS) {
    // flag bits we do not implement: deliver inline so future
    // semantics degrade safely
    return false;
  }
  // only data-bearing reads go out of band; a descriptor on anything
  // else (guards, version reads, ...) is ignored and the op stays inline
  switch (op_code) {
  case CEPH_OSD_OP_READ:
  case CEPH_OSD_OP_SYNC_READ:
    break;
  case CEPH_OSD_OP_SPARSE_READ:
    if (ec_direct) {
      // the fiemap extent map is in shard-offset space; interleaving
      // sparse data is a follow-up - deliver inline
      return false;
    }
    break;
  default:
    return false;
  }
  // an EC shard-direct read is placed by the stripe's geometry
  return !ec_direct || has_ec;
}

std::optional<read_push> plan_read_push(
  size_t idx, const OSDOp& op, const ceph::rdma::delivery_t& d,
  bool ec_direct, const std::optional<ec_direct_geometry>& ec)
{
  if (!read_push_possible(op.op.op, d, ec_direct, ec.has_value())) {
    return std::nullopt;
  }
  if (op.rval < 0 || !op.outdata.length()) {
    return std::nullopt;
  }

  read_push p;
  p.idx = idx;
  if (op.op.op == CEPH_OSD_OP_SPARSE_READ) {
    bufferlist databl;
    try {
      auto bp = op.outdata.cbegin();
      decode(p.sparse_extents, bp);
      decode(databl, bp);
    } catch (const ceph::buffer::error&) {
      return std::nullopt;
    }
    p.plan = sparse_plan(d.base_offset, op.op.extent.offset,
			 p.sparse_extents, databl.length());
    p.payload = std::move(databl);
    p.sparse = true;
  } else if (ec_direct) {
    // the reply holds this shard's chunks in ascending stripe order;
    // scatter them to their logical positions in the client window
    p.plan = ec_direct_plan(d.base_offset, op.op.extent.offset,
			    op.op.extent.length, ec->chunk_size, ec->k,
			    ec->raw_shard, op.outdata.length());
    p.payload = op.outdata;
  } else {
    p.plan = linear_plan(d.base_offset, op.outdata.length());
    p.payload = op.outdata;
  }
  if (p.plan.empty() || plan_bytes(p.plan) != p.payload.length()) {
    // a placed op's data leaves the reply, all of it: bytes the plan
    // leaves out would reach neither the window nor the client
    return std::nullopt;
  }
  p.want64 = d.flags & ceph::rdma::delivery_t::FLAG_CRC64NVME;
  p.want32c = d.flags & ceph::rdma::delivery_t::FLAG_CRC32C;
  return p;
}

void finish_op(OSDOp& op, ceph::rdma::oob_result_t& res,
	       const read_push& p, const push_result& r)
{
  if (r.r < 0 || static_cast<uint64_t>(r.r) != p.payload.length()) {
    // inline: declined if nothing of it went out. An executor that says
    // it placed other than the whole payload did not place it
    res.flags |= ceph::rdma::attempt_flags(false, r.started, false);
    return;
  }
  // strip the delivered data from the reply; sparse reads keep their
  // extent map inline with an empty data blob
  op.outdata.clear();
  if (p.sparse) {
    encode(p.sparse_extents, op.outdata);
    encode(bufferlist(), op.outdata);
  }
  res.bytes = static_cast<uint64_t>(r.r);
  // with delivery-complete writes every byte is in the window: nothing of
  // this op lands after the reply, which follows
  res.flags |= ceph::rdma::attempt_flags(true, true, r.delivery_complete);
  // the checksums of the placed ranges, after they crossed the fabric
  checksum_plan(p.payload, p.plan, p.want64, p.want32c, res);
}

oob_reply::oob_reply(MOSDOpReply* reply, std::vector<OSDOp>&& rops,
		     std::vector<ceph::rdma::oob_result_t>&& oob,
		     send_fn send)
  : reply(reply), rops(std::move(rops)), oob(std::move(oob)),
    send(std::move(send))
{
  ceph_assert(this->oob.size() == this->rops.size());
}

oob_reply::~oob_reply()
{
  if (reply) {
    // the guard was never dropped: nothing sends it now, and the client
    // resends
    reply->put();
  }
}

void oob_reply::get()
{
  // only while the guard or a push holds it: past the last put() the
  // reply is gone
  const unsigned before = pending.fetch_add(1, std::memory_order_relaxed);
  ceph_assert(before > 0);
}

void oob_reply::finish(const read_push& p, const push_result& r)
{
  ceph_assert(p.idx < rops.size());
  finish_op(rops[p.idx], oob[p.idx], p, r);
  put();
}

void oob_reply::put()
{
  // acq_rel: the last drop sees every push's slot
  const unsigned before = pending.fetch_sub(1, std::memory_order_acq_rel);
  ceph_assert(before > 0);
  if (before == 1) {
    send_reply();
  }
}

void oob_reply::send_reply()
{
  ceph_assert(reply);
  // report what was delivered, and what was declined: a client that
  // learns no transfer started for any op need not fence its window
  if (std::any_of(oob.begin(), oob.end(), [](const auto& r) {
	return r.bytes || r.flags;
      })) {
    reply->set_oob_results(std::move(oob));
  }
  reply->claim_ops(rops);
  send(std::exchange(reply, nullptr));
}

push_job::push_job(CephContext* cct, std::shared_ptr<oob_reply> join,
		   std::string key, read_push push, bool here)
  : cct(cct), join(std::move(join)), key(std::move(key)),
    push(std::move(push)), here(here)
{
  ceph_assert(this->push.exec);
  this->join->get();
}

push_job::~push_job()
{
  if (!finished) {
    // dropped without running: nothing of it started
    finish({-ECANCELED, false, false});
  }
}

void push_job::run()
{
  // an executor that cannot say counts as having started
  bool started = true;
  const ssize_t r = push.exec->execute_plan(key, push.token, push.payload,
					    push.plan, budget_left(push.bounds),
					    &started);
  debug_delay_done();
  if (r < 0) {
    dout(10) << "op " << push.idx << " of " << key
	     << ": plan execution failed (" << r << "), delivering inline"
	     << dendl;
  } else if (static_cast<uint64_t>(r) != push.payload.length()) {
    dout(0) << "op " << push.idx << " of " << key << ": the executor "
	    << "placed " << r << " of " << push.payload.length()
	    << " bytes, delivering inline" << dendl;
  }
  finish({r, started, push.exec->delivery_complete()});
}

void push_job::expire(int r)
{
  dout(10) << "op " << push.idx << " of " << key << ": not started ("
	   << r << "), delivering inline" << dendl;
  finish({r, false, false});
}

void push_job::finish(const push_result& r)
{
  ceph_assert(!finished);
  finished = true;
  join->finish(push, r);
}

void push_job::dump(ceph::Formatter* f) const
{
  f->dump_string("object", key);
  f->dump_unsigned("op", push.idx);
  f->dump_unsigned("bytes", push.payload.length());
  f->dump_unsigned("ranges", push.plan.size());
}

bool submit_pushes(CephContext* cct, Transfers* x,
		   const std::shared_ptr<oob_reply>& join,
		   const std::string& key, std::vector<read_push>&& pushes,
		   const std::function<bool(const OSDOobExecutor*)>& runs_inline)
{
  bool handed_off = false;
  for (auto& p : pushes) {
    const size_t idx = p.idx;
    // with transfer threads the push runs on one of them, unless its
    // executor keeps its transfers on the op threads (cuObject without
    // osd_oob_cuobj_transfer_threads)
    const bool here = runs_inline(p.exec);
    std::unique_ptr<xfer_job> job =
      std::make_unique<push_job>(cct, join, key, std::move(p), here);
    const int r = x ? x->submit(job) : -ESHUTDOWN;
    if (r < 0) {
      // refused, so nothing of it started: declined
      dout(10) << "op " << idx << " of " << key << ": push refused ("
	       << cpp_strerror(r) << "), delivering inline" << dendl;
      job->expire(r);
    } else if (!here && x->async()) {
      handed_off = true;
    }
  }
  return handed_off;
}

void hand_off(const std::shared_ptr<oob_reply>& join,
	      const std::function<void()>& let_go)
{
  let_go();
  // the guard goes last: the reply waits for it, so it cannot leave
  // while the request still holds anything
  join->put();
}

bool may_change_object(const std::vector<OSDOp>& ops, int flags)
{
  if (flags & CEPH_OSD_FLAG_WRITE) {
    return true;
  }
  // a CALL is not a read here, nor is an op of a write mode or of the
  // cache mode
  return std::any_of(ops.begin(), ops.end(), [](const OSDOp& o) {
    return !ceph_osd_op_mode_read(o.op.op) || ceph_osd_op_mode_modify(o.op.op);
  });
}

void pg_perf_stats::set_queries(const std::list<OSDPerfMetricQuery>& queries)
{
  std::lock_guard l(lock);
  stats.set_queries(queries);
}

DynamicPerfStats pg_perf_stats::take(DynamicPerfStats& fresh)
{
  std::lock_guard l(lock);
  DynamicPerfStats out;
  if (added || stats.is_enabled() != fresh.is_enabled()) {
    out = std::exchange(stats, fresh);
    added = false;
  }
  return out;
}

sub_read_push_job::sub_read_push_job(CephContext* cct,
				     MOSDECSubOpReadReply* reply,
				     sub_read_push push, send_fn send,
				     bool here)
  : cct(cct), reply(reply), push(std::move(push)), send(std::move(send)),
    here(here), pgid(reply->pgid.pgid, reply->op.from.shard),
    tid(reply->op.tid),
    bytes(this->push.all.length())
{
  ceph_assert(this->push.exec);
}

sub_read_push_job::~sub_read_push_job()
{
  if (reply) {
    // dropped without running: nothing of it started
    decline();
    send_reply();
  }
}

void sub_read_push_job::run()
{
  // a job runs or expires once: a second push could land after the
  // primary took its window back
  ceph_assert(reply);
  // an executor that cannot say counts as having started
  bool started = true;
  const ssize_t r = push.exec->execute_plan("ec gather", push.token,
					    push.all, push.plan,
					    budget_left(push.bounds),
					    &started);
  debug_delay_done();
  auto& op = reply->op;
  // only the executor's word returns the window clean: until it says
  // it started nothing, a write may have gone out
  op.push_declined = !started;
  if (r != static_cast<ssize_t>(bytes)) {
    // buffers_read stays as it was read, which is what the primary
    // decodes from
    dout(10) << "sub-read " << tid << " of " << pgid
	     << ": push to the primary failed (" << r
	     << (started ? "" : ", nothing sent") << "), replying inline"
	     << dendl;
  } else {
    dout(20) << "sub-read " << tid << " of " << pgid << ": pushed "
	     << bytes << " bytes to the primary" << dendl;
    op.pushed = std::move(push.pushed);
    // what the primary finds in its window has to match this before it
    // passes for our data
    op.pushed_crc = push.all.crc32c(-1);
    op.buffers_read.clear();
  }
  send_reply();
}

void sub_read_push_job::expire(int r)
{
  ceph_assert(reply);
  dout(10) << "sub-read " << tid << " of " << pgid << ": push not started ("
	   << r << "), replying inline" << dendl;
  decline();
  send_reply();
}

void sub_read_push_job::decline()
{
  reply->op.push_declined = true;
}

void sub_read_push_job::send_reply()
{
  ceph_assert(reply);
  send(std::exchange(reply, nullptr));
}

void sub_read_push_job::dump(ceph::Formatter* f) const
{
  f->dump_stream("pgid") << pgid;
  f->dump_unsigned("sub_read", tid);
  f->dump_unsigned("bytes", bytes);
}

} // namespace ceph::osd::oob
