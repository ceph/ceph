// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "osd/oob_pull.h"

#include <optional>
#include <utility>

#include "common/ceph_context.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "include/ceph_assert.h"
#include "messages/MOSDOp.h"
#include "osd/OpRequest.h"
#include "osd/oob_executor.h"
#include "osd/oob_placement.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "oob_pull: "

namespace ceph::osd::oob {

namespace {

/// the payloads of pulls, together
uint64_t total_len(const std::vector<pull_op>& pulls)
{
  uint64_t n = 0;
  for (const auto& p : pulls) {
    n += p.len;
  }
  return n;
}

/**
 * Check the pulls of a request's ops, every one before any starts, and
 * fix each: 0, with the pulls in *pulls (none when no op pulls), or the
 * errno that fails the request with nothing started. The ops are
 * checked in order, each on its own, so the first op that fails decides
 * the errno.
 */
int plan_pulls(CephContext* cct, const std::string& key,
	       const std::vector<OSDOp>& ops,
	       const std::vector<ceph::rdma::delivery_t>& deliveries,
	       const std::function<OSDOobExecutor*(const std::string&)>& puller_for,
	       std::vector<pull_op>* pulls)
{
  ceph_assert(deliveries.size() == ops.size());
  std::vector<pull_op> planned;
  for (size_t i = 0; i < ops.size(); i++) {
    const auto& d = deliveries[i];
    if (d.empty() || !d.is_pull()) {
      continue;
    }
    const OSDOp& osd_op = ops[i];
    const uint64_t len = osd_op.op.extent.length;
    if (osd_op.op.op != CEPH_OSD_OP_WRITE &&
	osd_op.op.op != CEPH_OSD_OP_WRITEFULL) {
      dout(5) << "op " << i << " " << osd_op << " of " << key
	      << " carries a pull descriptor, which only writes take" << dendl;
      return -EINVAL;
    }
    if (d.flags & ~ceph::rdma::delivery_t::PULL_KNOWN_FLAGS) {
      return -EOPNOTSUPP;
    }
    if (osd_op.indata.length() != 0) {
      return -EINVAL;
    }
    OSDOobExecutor* exec = puller_for(d.token);
    if (!exec) {
      // whatever its length: a pulled WRITEFULL of no bytes would run,
      // and empty the object
      dout(10) << "op " << i << " of " << key << ": no transport pulls "
	       << "from this token" << dendl;
      return -EOPNOTSUPP;
    }
    if (len == 0) {
      continue;
    }
    pull_op p;
    p.idx = i;
    p.len = len;
    p.base_offset = d.base_offset;
    p.want64 = d.flags & ceph::rdma::delivery_t::FLAG_CRC64NVME;
    p.want32c = d.flags & ceph::rdma::delivery_t::FLAG_CRC32C;
    p.exec = exec;
    p.token = d.token;
    planned.push_back(std::move(p));
  }
  *pulls = std::move(planned);
  return 0;
}

} // anonymous namespace

pull_set::pull_set(std::vector<size_t> pulled)
  : pulled(std::move(pulled))
{}

bool pull_set::settled() const
{
  std::lock_guard l(lock);
  return state != state_t::in_flight;
}

bool pull_set::consumed() const
{
  std::lock_guard l(lock);
  return state == state_t::consumed;
}

bool pull_set::park(const OpRequest* who, const kick_target& t)
{
  std::lock_guard l(lock);
  if (state != state_t::in_flight) {
    return false;
  }
  for (auto& [w, target] : targets) {
    if (w == who) {
      // the attempt waits elsewhere now: its earlier hold is gone
      target = t;
      return true;
    }
  }
  targets.emplace_back(who, t);
  return true;
}

bool pull_set::abandoned() const
{
  std::lock_guard l(lock);
  return dropped;
}

void pull_set::clear_declined(
  std::vector<ceph::rdma::oob_result_t>& oob) const
{
  std::lock_guard l(lock);
  auto clear = [&oob](size_t idx) {
    if (idx < oob.size()) {
      oob[idx].flags &= ~ceph::rdma::oob_result_t::FLAG_DECLINED;
    }
  };
  if (state == state_t::in_flight) {
    // any pull of the job may be reading its window now
    for (const size_t idx : pulled) {
      clear(idx);
    }
    return;
  }
  for (const auto& r : results) {
    if (r.started) {
      clear(r.idx);
    }
  }
}

std::vector<kick_target> pull_set::settle(std::vector<pull_result>&& r)
{
  std::lock_guard l(lock);
  ceph_assert(state == state_t::in_flight);
  results = std::move(r);
  state = state_t::settled;
  if (dropped) {
    // the request went away while its pulls ran: nothing takes the
    // payloads, whoever still waits for the set
    drop_payloads();
  }
  // a run that parks from now on finds the set settled, and goes on
  std::vector<kick_target> kicks;
  kicks.reserve(targets.size());
  for (auto& [who, target] : targets) {
    kicks.push_back(std::move(target));
  }
  targets.clear();
  return kicks;
}

int pull_set::consume(std::vector<OSDOp>& ops,
		      std::vector<ceph::rdma::oob_result_t>& pulled,
		      uint64_t max_record)
{
  std::lock_guard l(lock);
  ceph_assert(state == state_t::settled);
  for (const auto& r : results) {
    if (r.r < 0) {
      // the request fails, and no write of it runs with its data
      return r.r;
    }
  }
  for (auto& r : results) {
    ceph_assert(r.idx < ops.size() && r.idx < pulled.size());
    ops[r.idx].indata = std::move(r.payload);
    // a write returns no data of its own: the record takes its place,
    // for the PG log to keep. One that does not fit, with RETURNVEC,
    // would fail the request
    ceph::buffer::list record;
    encode_pull_record(r.oob, record);
    if (record.length() <= max_record) {
      // the encoding took a whole append buffer, which the PG log entry
      // and the dups trimmed from it would pin for as long as they last
      record.rebuild();
      record.reassign_to_mempool(mempool::mempool_osd_pglog);
      ops[r.idx].outdata = std::move(record);
    }
    pulled[r.idx] = std::move(r.oob);
  }
  state = state_t::consumed;
  return 0;
}

bool wait_for_earlier(OpRequest& op, const std::shared_ptr<pull_set>& earlier,
		      const kick_target& target)
{
  if (auto before = op.oob_pull_waits.lock(); before && before != earlier) {
    // a set it waited for before an interval change requeued it: this
    // run of it no longer waits there
    before->leave(&op);
    op.oob_pull_waits.reset();
  }
  if (!earlier->park(&op, target)) {
    return false;
  }
  op.oob_pull_waits = earlier;
  return true;
}

void pulls_out::add(const osd_reqid_t& reqid, std::shared_ptr<pull_set> set)
{
  auto& s = shard_of(reqid);
  std::lock_guard l(s.lock);
  s.sets.emplace(reqid, std::move(set));
}

void pulls_out::remove(const osd_reqid_t& reqid, const pull_set* set)
{
  auto& s = shard_of(reqid);
  std::lock_guard l(s.lock);
  auto [b, e] = s.sets.equal_range(reqid);
  for (auto i = b; i != e; ++i) {
    if (i->second.get() == set) {
      s.sets.erase(i);
      return;
    }
  }
}

std::shared_ptr<pull_set> pulls_out::find(const osd_reqid_t& reqid) const
{
  auto& s = shard_of(reqid);
  std::lock_guard l(s.lock);
  auto i = s.sets.find(reqid);
  return i == s.sets.end() ? nullptr : i->second;
}

size_t pulls_out::size() const
{
  size_t n = 0;
  for (const auto& s : shards) {
    std::lock_guard l(s.lock);
    n += s.sets.size();
  }
  return n;
}

pull_job::pull_job(CephContext* cct, std::shared_ptr<pull_set> set,
		   std::string key, std::vector<pull_op> pulls,
		   bounds_t bounds, bool here, kick_fn kick, pulls_out* out,
		   osd_reqid_t reqid)
  : cct(cct), set(std::move(set)), key(std::move(key)),
    ops(std::move(pulls)), bounds(bounds), here(here),
    kick(std::move(kick)), out(out), reqid(reqid), bytes(total_len(ops))
{
  ceph_assert(this->set);
}

pull_job::~pull_job()
{
  if (!settled) {
    // dropped without running: nothing of it started
    fail(-EOPNOTSUPP);
  }
}

void pull_job::run()
{
  std::vector<pull_result> results(ops.size());
  bool failed = false;
  for (size_t k = 0; k < ops.size(); k++) {
    const auto& p = ops[k];
    auto& res = results[k];
    res.idx = p.idx;
    if (failed) {
      // all or nothing: the request fails, and no later pull starts
      continue;
    }
    if (set->abandoned()) {
      // the request went away, and no run will answer it: the client
      // sends it again, or gave it up
      dout(10) << "op " << p.idx << " of " << key << ": the request was "
	       << "dropped, not pulling " << p.len << " bytes" << dendl;
      res.r = -ESHUTDOWN;
      failed = true;
      continue;
    }
    if (ceph::mono_clock::now() >= bounds.post_by) {
      // past the lease since receipt no read starts: a client that gave
      // up on the request may reuse its window once the drain passed too.
      // Nothing of this pull went out, so one that still waits for the
      // reply may send the payload inline
      dout(5) << "op " << p.idx << " of " << key << ": too late to pull "
	      << p.len << " bytes" << dendl;
      res.r = -EOPNOTSUPP;
      failed = true;
      continue;
    }
    pull(p, res);
    failed = res.r < 0;
  }
  debug_delay_done();
  settle(std::move(results));
}

void pull_job::pull(const pull_op& p, pull_result& res)
{
  const auto plan = linear_plan(p.base_offset, p.len);
  ceph::buffer::list bl;
  // an executor that cannot say counts as having started
  bool started = true;
  const ssize_t r = p.exec->execute_pull(key, p.token, plan, p.len, &bl,
					 budget_left(bounds), &started);
  res.started = started;
  if (r < 0) {
    dout(5) << "op " << p.idx << " of " << key << ": pulling " << p.len
	    << " bytes failed: " << cpp_strerror(r)
	    << (started ? "" : ", nothing sent") << dendl;
    // The client is told one of two things, whatever the executor said
    // (delivery_t::FLAG_PULL): the OSD could not read the window, and
    // nothing of the read went out, so the client may send the payload
    // inline; or the read failed, maybe after some of it went out. An
    // executor that refused (a payload larger than it stages, a full
    // arena, a busy channel, a budget too short to start in) is the
    // first; one that started and then failed or was cut off is the
    // second
    res.r = started ? -EIO : -EOPNOTSUPP;
    return;
  }
  ceph_assert(bl.length() == p.len);
  res.oob.bytes = p.len;
  // the checksums of the bytes as they arrived here, before the write
  // encodes them
  checksum_plan(bl, plan, p.want64, p.want32c, res.oob);
  dout(20) << "op " << p.idx << " of " << key << ": pulled " << p.len
	   << " bytes" << dendl;
  res.r = 0;
  res.payload = std::move(bl);
}

void pull_job::expire(int r)
{
  // nothing of it started, whether it waited past its post_by, was
  // refused or was shed: the client may send the payloads inline
  dout(10) << "pulls of " << key << " not started (" << cpp_strerror(r)
	   << ")" << dendl;
  fail(-EOPNOTSUPP);
}

void pull_job::fail(int r)
{
  std::vector<pull_result> results(ops.size());
  for (size_t k = 0; k < ops.size(); k++) {
    results[k].idx = ops[k].idx;
    results[k].r = r;
  }
  settle(std::move(results));
}

void pull_job::settle(std::vector<pull_result>&& r)
{
  // a job runs or expires once
  ceph_assert(!settled);
  settled = true;
  const auto targets = set->settle(std::move(r));
  if (out) {
    // nothing of them reads the window any more: a later attempt of the
    // request may pull it again
    out->remove(reqid, set.get());
  }
  if (targets.empty()) {
    dout(10) << "pulls of " << key << " settled" << dendl;
    return;
  }
  // each attempt of the request that waits for them, in a hold of its
  // PG: run it again
  for (const auto& target : targets) {
    dout(10) << "pulls of " << key << " settled, kicking " << target.pgid
	     << dendl;
    if (kick) {
      kick(target);
    }
  }
}

void pull_job::dump(ceph::Formatter* f) const
{
  f->dump_string("object", key);
  f->dump_unsigned("ops", ops.size());
  f->dump_unsigned("bytes", bytes);
}

int pull_payloads(const pull_env& env, OpRequest& op,
		  std::vector<OSDOp>& ops)
{
  CephContext* cct = env.cct;
  auto m = op.get_req<MOSDOp>();
  const auto& deliveries = m->get_rdma_deliveries();
  if (deliveries.size() != ops.size()) {
    // the descriptor vector must mirror the ops; a write that needed a
    // pull then fails for its missing data
    return 0;
  }
  if (op.oob_pulled.size() != ops.size()) {
    op.oob_pulled.assign(ops.size(), {});
  }
  if (const auto& set = op.oob_pull; set) {
    // a run of the request after its pulls started. What they did is in
    // the set; the ops cannot tell, since a run may have changed them
    // (a write that a truncation overtook is cut short, its data too)
    if (set->consumed()) {
      return 0;
    }
    // a pull settles before its request runs on: a run whose pulls are
    // still out waits for them before it gets here
    ceph_assert(set->settled());
    return set->consume(ops, op.oob_pulled, env.max_reply);
  }
  const std::string& key = m->get_hobj().oid.name;
  // The pulls keep to the bounds of a read's pushes, counted from the
  // receipt of the request on the monotonic clock: none starts after the
  // pool's lease, and each read lands, or is cut off, by the lease plus
  // the drain, which also bounds how long an op thread that runs the
  // pulls waits on them. A client that gave up on the request, as one
  // whose operation timed out, may then reuse its window once they
  // passed since the last attempt of it reached the OSD, however long
  // that attempt waited in its PG (a laggy or degraded PG, a scrub, the
  // object's lock, the hold of another pulled write)
  const auto bounds = make_bounds(op.get_mono_recv_stamp(), env.lease,
				  env.drain, std::nullopt,
				  ceph::signedspan::zero());
  std::vector<pull_op> pulls;
  if (int r = plan_pulls(cct, key, ops, deliveries, env.puller_for, &pulls);
      r < 0) {
    return r;
  }
  if (pulls.empty()) {
    return 0;
  }
  if (lease_expired(bounds)) {
    // nothing read the window: the reply declines the descriptors, and
    // a client that still waits for it may send the payloads inline, as
    // for any pull the OSD could not start
    dout(5) << key << ": received "
	    << std::chrono::duration<double>(
	         ceph::mono_clock::now() - op.get_mono_recv_stamp()).count()
	    << "s ago, past the lease of " << env.lease << "s: pulling "
	    << "nothing" << dendl;
    return -EOPNOTSUPP;
  }
  // do_op fails a request whose payloads and inline data together
  // exceed osd_max_write_size, which is never 0, before it gets here
  // (MOSDOp::get_pulled_len(), the same sum): it does not wrap
  const uint64_t total = total_len(pulls);
  // The payloads are charged to the client message throttle as inline
  // ones are, all of them before any is pulled, so that what this and
  // the requests the messenger has already read pull stays within its
  // limit. With no room now the client sends the payloads inline, which
  // the messenger throttles before it reads them. The request gives the
  // bytes back when it is done, pulled or not.
  if (!op.reserve_pulled(env.client_throttle ? env.client_throttle() :
			 nullptr, total)) {
    dout(5) << key << ": the client message throttle has no room for "
	    << total << " pulled bytes" << dendl;
    return -EOPNOTSUPP;
  }
  // One job runs the request's pulls, on a transfer thread when the
  // service has them: then the request waits for it, and lets go of the
  // object. It runs on this thread instead when the request may not
  // wait (a coroutine runs it), and when an executor's transfers stay on
  // the op threads; the request goes on with its payloads once it
  // settled, holding the PG and the object's lock meanwhile
  bool here = env.here || !env.xfer || !env.xfer->async();
  for (const auto& p : pulls) {
    here = here || (env.runs_inline && env.runs_inline(p.exec));
  }
  std::vector<size_t> pulled;
  for (const auto& p : pulls) {
    pulled.push_back(p.idx);
  }
  auto set = std::make_shared<pull_set>(std::move(pulled));
  op.oob_pull = set;
  // While they run on a transfer thread, a later attempt of the request
  // waits for them, whatever becomes of this one. Pulls that run here
  // hold the PG until they settled, and none of its runs sees them out
  pulls_out* out = here ? nullptr : env.out;
  if (out) {
    out->add(op.get_reqid(), set);
  }
  std::unique_ptr<xfer_job> job = std::make_unique<pull_job>(
    cct, set, key, std::move(pulls), bounds, here, env.kick, out,
    op.get_reqid());
  if (const int r = env.xfer ? env.xfer->submit(job) : -ESHUTDOWN; r < 0) {
    // no synchronous pull instead: the client sends the payloads inline
    dout(10) << "pulls of " << key << " refused (" << cpp_strerror(r)
	     << ")" << dendl;
    job->expire(r);
  }
  // A job that settled already, on this thread or on a transfer thread
  // before the park, is consumed here, and kicks nothing
  if (!here && set->park(&op, env.target)) {
    dout(20) << "pulls of " << key << " handed off, the request waits in "
	     << env.target.pgid << dendl;
    return PULLS_PARKED;
  }
  ceph_assert(set->settled());
  return set->consume(ops, op.oob_pulled, env.max_reply);
}

void encode_pull_record(const ceph::rdma::oob_result_t& r,
			ceph::buffer::list& bl)
{
  ENCODE_START(1, 1, bl);
  ceph::encode(r.bytes, bl);
  ceph::encode(r.flags, bl);
  ceph::encode(r.crc64, bl);
  ceph::encode(r.crc32c, bl);
  ENCODE_FINISH(bl);
}

bool decode_pull_record(const ceph::buffer::list& bl,
			const ceph::rdma::delivery_t& d,
			ceph::rdma::oob_result_t* r)
{
  using R = ceph::rdma::oob_result_t;
  R res;
  try {
    auto p = bl.cbegin();
    DECODE_START(1, p);
    ceph::decode(res.bytes, p);
    ceph::decode(res.flags, p);
    ceph::decode(res.crc64, p);
    ceph::decode(res.crc32c, p);
    DECODE_FINISH(p);
    if (!p.end()) {
      return false;
    }
  } catch (const ceph::buffer::error&) {
    return false;
  }
  // only what pull_job::pull() reports of a payload it pulled
  constexpr uint32_t pull_flags = R::FLAG_COMBINABLE | R::FLAG_RANGES |
    R::FLAG_CRC64NVME | R::FLAG_CRC32C;
  if (res.bytes == 0 || (res.flags & ~pull_flags)) {
    return false;
  }
  if (res.flags & R::FLAG_RANGES) {
    res.ranges.push_back({d.base_offset, res.bytes, res.crc64, res.crc32c});
  }
  *r = std::move(res);
  return true;
}

bool keep_pull_records(
  const std::vector<ceph::rdma::delivery_t>& deliveries,
  const std::vector<OSDOp>& ops,
  std::vector<pg_log_op_return_item_t>& op_returns)
{
  if (deliveries.size() != ops.size()) {
    return false;
  }
  std::vector<pg_log_op_return_item_t> kept(ops.size());
  bool any = false;
  for (size_t i = 0; i < ops.size(); i++) {
    const auto& d = deliveries[i];
    // a write returns no data of its own: what a pulled one has is the
    // record of its pull, when it fit (pull_set::consume())
    if (d.empty() || !d.is_pull() || ops[i].outdata.length() == 0) {
      continue;
    }
    kept[i].bl = ops[i].outdata;
    any = true;
  }
  if (any) {
    op_returns = std::move(kept);
  }
  return any;
}

void dup_pull_results(
  const std::vector<ceph::rdma::delivery_t>& deliveries,
  const std::vector<pg_log_op_return_item_t>& op_returns, int r,
  std::vector<ceph::rdma::oob_result_t>& oob)
{
  // none kept (a record too large for osd_max_write_op_reply_len), or
  // not for these ops: no op has a record
  const bool kept = op_returns.size() == deliveries.size();
  for (size_t i = 0; i < deliveries.size() && i < oob.size(); i++) {
    const auto& d = deliveries[i];
    if (d.empty() || !d.is_pull()) {
      continue;
    }
    ceph::rdma::oob_result_t res;
    if (r >= 0 && kept && decode_pull_record(op_returns[i].bl, d, &res)) {
      oob[i] = std::move(res);
      continue;
    }
    // The attempt that did the write may have pulled, whether or not the
    // write failed after it, and whether or not the log kept what it
    // pulled: as clear_declined() says of a pull that started, the op is
    // not declined, and without the record it reports no bytes
    oob[i] = ceph::rdma::oob_result_t{};
  }
}

void op_error_results(
  const std::vector<ceph::rdma::delivery_t>& deliveries,
  const std::vector<pg_log_op_return_item_t>& op_returns, int r, bool dup,
  const pull_set* pulls, std::vector<ceph::rdma::oob_result_t>& oob)
{
  reply_error_results(deliveries, r, dup, oob);
  if (pulls) {
    // but a pull of the request may have read the client's window,
    // before it failed or before the request did
    pulls->clear_declined(oob);
  }
  if (dup) {
    dup_pull_results(deliveries, op_returns, r, oob);
  }
}

} // namespace ceph::osd::oob
