// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * The read push split into a plan, a job and a join
 * (src/osd/oob_read_delivery.h), through a fake executor: each op's
 * result, the reply's data and the checksums are what the code before
 * the split produced, for every kind of read op, every outcome of the
 * transfer, and every checksum a descriptor asks for; and the reply
 * goes out exactly once, after every push, whichever thread finishes
 * last. Handed off to transfer threads (submit_pushes()), a push that
 * could not start in time, or that the service shed, starts nothing and
 * is declined, and the reply carries what the op thread put in it, and
 * leaves only after the op thread let go of the request. The fence of
 * an object whose read replies are out holds the ops that may change it,
 * and every op behind them, in order, until the last reply left, through
 * an interval change too.
 */

#include "osd/oob_read_delivery.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "common/Clock.h"
#include "common/TrackedOp.h"
#include "common/ceph_context.h"
#include "common/crc64nvme.h"
#include "common/rdma_token.h"
#include "global/global_context.h"
#include "include/scope_guard.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "msg/Message.h"
#include "osd/OpRequest.h"
#include "osd/oob_executor.h"
#include "osd/oob_placement.h"
#include "osd/oob_xfer.h"

#include "gtest/gtest.h"

using namespace ceph::osd::oob;
using namespace std::chrono_literals;
using ceph::rdma::delivery_t;
using ceph::rdma::oob_result_t;

namespace {

using mono_clock = ceph::mono_clock;

/// what the executor does with a plan
enum class outcome {
  placed,          ///< every triple completed
  failed_started,  ///< failed after a write went out
  failed_unstarted ///< failed before anything went out
};

const char* name(outcome o)
{
  switch (o) {
  case outcome::placed: return "placed";
  case outcome::failed_started: return "failed_started";
  case outcome::failed_unstarted: return "failed_unstarted";
  }
  return "?";
}

/// places a plan into a window of its own, or fails as told
struct fake_exec : OSDOobExecutor {
  outcome how = outcome::placed;
  bool complete = false;
  /// sleep up to this long in execute_plan()
  std::chrono::microseconds max_delay{0};

  std::mutex m;
  std::string window = std::string(1 << 20, '\0');
  std::atomic<int> calls{0};
  std::thread::id ran_on;

  bool is_available() const override { return true; }
  bool handles(const std::string&) const override { return true; }
  bool delivery_complete() const override { return complete; }
  void dump_stats(ceph::Formatter*) const override {}

  ssize_t execute_plan(const std::string& key, const std::string& token,
		       const ceph::buffer::list& data,
		       const placement_plan& plan,
		       std::chrono::milliseconds budget,
		       bool* started) override {
    calls++;
    if (max_delay.count() > 0) {
      thread_local std::minstd_rand rng(std::random_device{}());
      std::this_thread::sleep_for(
	std::chrono::microseconds(rng() % max_delay.count()));
    }
    std::lock_guard l(m);
    ran_on = std::this_thread::get_id();
    switch (how) {
    case outcome::failed_unstarted:
      *started = false;
      return -ETIMEDOUT;
    case outcome::failed_started:
      *started = true;
      return -EIO;
    case outcome::placed:
      break;
    }
    *started = true;
    ssize_t total = 0;
    for (const auto& t : plan) {
      ceph_assert(t.client_ofs + t.len <= window.size());
      ceph::buffer::list part;
      part.substr_of(data, t.local_ofs, t.len);
      part.begin().copy(t.len, window.data() + t.client_ofs);
      total += t.len;
    }
    return total;
  }
};

/**
 * A model of the read push: each op delivered in one step on the calling
 * thread, with what it read from the PG passed in, and the declined flags
 * that deliver_oob() sets. The read push must produce what it produces.
 */
bool legacy_deliver_op_oob(bool ec_direct,
			   const std::optional<ec_direct_geometry>& geo,
			   OSDOobExecutor* exec, const std::string& key,
			   OSDOp& op, const delivery_t& d,
			   oob_result_t& res, bool* started)
{
  *started = false;
  if (d.flags & ~ceph::rdma::delivery_t::KNOWN_FLAGS) {
    return false;
  }
  switch (op.op.op) {
  case CEPH_OSD_OP_READ:
  case CEPH_OSD_OP_SYNC_READ:
  case CEPH_OSD_OP_SPARSE_READ:
    break;
  default:
    return false;
  }
  OSDOp* data_op = &op;
  if (data_op->rval < 0 || !data_op->outdata.length()) {
    return false;
  }

  ceph::osd::oob::placement_plan plan;
  bufferlist payload;  // the bytes the plan indexes
  std::map<uint64_t, uint64_t> sparse_extents;
  if (data_op->op.op == CEPH_OSD_OP_SPARSE_READ) {
    if (ec_direct) {
      return false;
    }
    bufferlist databl;
    try {
      auto bp = data_op->outdata.cbegin();
      decode(sparse_extents, bp);
      decode(databl, bp);
    } catch (const ceph::buffer::error&) {
      return false;
    }
    plan = ceph::osd::oob::sparse_plan(d.base_offset,
				       data_op->op.extent.offset,
				       sparse_extents, databl.length());
    payload = std::move(databl);
  } else if (ec_direct) {
    // was: the pool is erasure-coded, allows EC optimizations, and its
    // stripe supports direct reads
    if (!geo) {
      return false;
    }
    plan = ceph::osd::oob::ec_direct_plan(
      d.base_offset, data_op->op.extent.offset, data_op->op.extent.length,
      geo->chunk_size, geo->k, geo->raw_shard, data_op->outdata.length());
    payload = data_op->outdata;
  } else {
    plan = ceph::osd::oob::linear_plan(d.base_offset,
				       data_op->outdata.length());
    payload = data_op->outdata;
  }
  if (plan.empty()) {
    return false;
  }

  if (!exec) {
    return false;
  }
  // was: the pool's lease plus its drain, less the request's age
  const double bound = 10.0;
  bool exec_started = true;
  ssize_t pushed = exec->execute_plan(
    key, d.token, payload, plan,
    std::chrono::milliseconds(static_cast<int64_t>(bound * 1000.0)),
    &exec_started);
  *started = exec_started;
  if (pushed < 0) {
    return false;
  }
  data_op->outdata.clear();
  if (data_op->op.op == CEPH_OSD_OP_SPARSE_READ) {
    encode(sparse_extents, data_op->outdata);
    encode(bufferlist(), data_op->outdata);
  }
  res.bytes = static_cast<uint64_t>(pushed);
  res.flags |= ceph::rdma::attempt_flags(true, true, exec->delivery_complete());
  const bool want64 = d.flags & ceph::rdma::delivery_t::FLAG_CRC64NVME;
  const bool want32c = d.flags & ceph::rdma::delivery_t::FLAG_CRC32C;
  if (want64 || want32c) {
    res.ranges.reserve(plan.size());
    uint64_t whole64 = 0;
    uint32_t whole32c = 0;
    for (const auto& t : plan) {
      bufferlist part;
      part.substr_of(payload, t.local_ofs, t.len);
      ceph::rdma::crc_range_t r{t.client_ofs, t.len};
      if (want64) {
	r.crc64 = ceph::crc64nvme(part);
	whole64 = res.ranges.empty() ? r.crc64
		: ceph::crc64nvme_combine(whole64, r.crc64, t.len);
      }
      if (want32c) {
	r.crc32c = ceph::rdma::crc32c_canonical(part);
	whole32c = res.ranges.empty() ? r.crc32c
		 : ceph::rdma::crc32c_combine(whole32c, r.crc32c, t.len);
      }
      res.ranges.push_back(r);
    }
    res.crc64 = whole64;
    res.crc32c = whole32c;
    res.flags |= ceph::rdma::oob_result_t::FLAG_RANGES |
      (want64 ? ceph::rdma::oob_result_t::FLAG_CRC64NVME : 0) |
      (want32c ? ceph::rdma::oob_result_t::FLAG_CRC32C : 0);
    if (plan.size() == 1) {
      res.flags |= ceph::rdma::oob_result_t::FLAG_COMBINABLE;
    }
  }
  return true;
}

/// the model for one op, with deliver_oob()'s flags for an op it did not
/// place
void legacy_op(bool ec_direct, const std::optional<ec_direct_geometry>& geo,
	       OSDOobExecutor* exec, OSDOp& op, const delivery_t& d,
	       oob_result_t& res)
{
  bool started = false;
  if (!legacy_deliver_op_oob(ec_direct, geo, exec, "obj", op, d, res,
			     &started)) {
    res.flags |= ceph::rdma::attempt_flags(false, started, false);
  }
}

ceph::buffer::list pattern(size_t len, unsigned seed)
{
  std::string s(len, '\0');
  std::minstd_rand rng(seed);
  for (auto& c : s) {
    c = static_cast<char>(rng());
  }
  ceph::buffer::list bl;
  bl.append(s);
  return bl;
}

OSDOp read_op(int type, uint64_t off, uint64_t len, ceph::buffer::list data)
{
  OSDOp op;
  op.op.op = type;
  op.op.extent.offset = off;
  op.op.extent.length = len;
  op.rval = 0;
  op.outdata = std::move(data);
  return op;
}

/// a SPARSE_READ's reply data: its extent map, then the packed blob
OSDOp sparse_op(uint64_t off, uint64_t len,
		const std::map<uint64_t, uint64_t>& extents, unsigned seed)
{
  uint64_t total = 0;
  for (const auto& [o, l] : extents) {
    total += l;
  }
  ceph::buffer::list out;
  encode(extents, out);
  encode(pattern(total, seed), out);
  return read_op(CEPH_OSD_OP_SPARSE_READ, off, len, std::move(out));
}

/// the bytes shard g.raw_shard holds of the logical range
uint64_t shard_bytes(uint64_t off, uint64_t len, const ec_direct_geometry& g)
{
  uint64_t n = 0;
  for (const auto& t : ec_direct_plan(0, off, len, g.chunk_size, g.k,
				      g.raw_shard, ~0ull)) {
    n += t.len;
  }
  return n;
}

/// one kind of op in the matrix
struct op_case {
  std::string what;
  OSDOp op;
  bool ec_direct = false;
  std::optional<ec_direct_geometry> geo;
  /// a plan of exactly one range: its checksums combine
  std::optional<bool> one_range;
};

const ec_direct_geometry geo4k{4096, 4, 1};

std::vector<op_case> op_cases()
{
  std::vector<op_case> v;
  v.push_back({"READ", read_op(CEPH_OSD_OP_READ, 8192, 10000,
			       pattern(10000, 1)), false, std::nullopt, true});
  v.push_back({"SYNC_READ", read_op(CEPH_OSD_OP_SYNC_READ, 0, 4096,
				    pattern(4096, 2)), false, std::nullopt,
	       true});
  // a short read: the reply holds less than the extent asked for
  v.push_back({"READ short", read_op(CEPH_OSD_OP_READ, 0, 1 << 20,
				     pattern(777, 3)), false, std::nullopt,
	       true});
  v.push_back({"SPARSE_READ", sparse_op(4096, 65536,
					{{4096, 100}, {9000, 3000},
					 {40000, 1}}, 4),
	       false, std::nullopt, false});
  v.push_back({"SPARSE_READ one extent", sparse_op(0, 65536,
						   {{1000, 5000}}, 5),
	       false, std::nullopt, true});
  {
    // several chunks of shard 1 across stripes, partial first and last
    const uint64_t off = 1000, len = 4 * 4096 * 2 + 3000;
    v.push_back({"EC-direct", read_op(CEPH_OSD_OP_READ, off, len,
				      pattern(shard_bytes(off, len, geo4k),
					      6)),
		 true, geo4k, false});
  }
  {
    // inside one chunk of shard 1
    const uint64_t off = 4096 + 100, len = 200;
    v.push_back({"EC-direct one chunk", read_op(CEPH_OSD_OP_READ, off, len,
						pattern(shard_bytes(off, len,
								    geo4k),
							7)),
		 true, geo4k, true});
  }
  {
    // the shard's read stops short, at the object's end
    const uint64_t off = 0, len = 4 * 4096 * 3;
    v.push_back({"EC-direct short", read_op(CEPH_OSD_OP_READ, off, len,
					    pattern(4096 + 1000, 8)),
		 true, geo4k, false});
  }
  return v;
}

/// ops that stay inline, whatever the executor would do
std::vector<op_case> inline_cases()
{
  std::vector<op_case> v;
  v.push_back({"STAT", read_op(CEPH_OSD_OP_STAT, 0, 0, pattern(16, 10))});
  v.push_back({"WRITE", read_op(CEPH_OSD_OP_WRITE, 0, 64, pattern(64, 11))});
  v.push_back({"READ no data", read_op(CEPH_OSD_OP_READ, 0, 64, {})});
  {
    auto op = read_op(CEPH_OSD_OP_READ, 0, 64, pattern(64, 12));
    op.rval = -EIO;
    v.push_back({"READ failed", std::move(op)});
  }
  {
    ceph::buffer::list junk;
    junk.append("not an extent map");
    v.push_back({"SPARSE_READ undecodable",
		 read_op(CEPH_OSD_OP_SPARSE_READ, 0, 64, std::move(junk))});
  }
  v.push_back({"SPARSE_READ EC-direct", sparse_op(0, 65536, {{0, 10}}, 13),
	       true, geo4k});
  {
    // the pool does not allow direct reads after all
    auto op = read_op(CEPH_OSD_OP_READ, 0, 4096, pattern(4096, 14));
    v.push_back({"EC-direct no geometry", std::move(op), true,
		 std::nullopt});
  }
  {
    // shard 1 holds nothing of a range inside shard 0's first chunk
    auto op = read_op(CEPH_OSD_OP_READ, 0, 100, pattern(100, 15));
    v.push_back({"EC-direct nothing here", std::move(op), true, geo4k});
  }
  return v;
}

delivery_t descriptor(uint32_t flags, uint64_t base = 512)
{
  delivery_t d;
  d.token = "token";
  d.base_offset = base;
  d.flags = flags;
  return d;
}

const uint32_t wants[] = {
  0,
  delivery_t::FLAG_CRC64NVME,
  delivery_t::FLAG_CRC32C,
  delivery_t::FLAG_CRC64NVME | delivery_t::FLAG_CRC32C,
};

bounds_t far_bounds()
{
  const auto now = mono_clock::now();
  return {now + 10s, now + 20s};
}

/// what the send hook got
struct sent_t {
  std::atomic<int> count{0};
  std::thread::id on;
  std::vector<OSDOp> ops;
  std::vector<oob_result_t> results;
  int result = 0;
  int flags = 0;
  version_t user_version = 0;
  /// called with the reply before it is taken apart
  std::function<void(MOSDOpReply*)> check;
};

/// the join of a reply to rops; reply, when given, is the reply the op
/// thread built
std::shared_ptr<oob_reply> make_join(std::vector<OSDOp> rops, sent_t& sent,
				     ceph::ref_t<MOSDOpReply> reply = {})
{
  std::vector<oob_result_t> oob(rops.size());
  if (!reply) {
    reply = ceph::make_message<MOSDOpReply>();
  }
  return std::make_shared<oob_reply>(
    reply.detach(), std::move(rops), std::move(oob),
    [&sent](MOSDOpReply* r) {
      ceph::ref_t<MOSDOpReply> got(r, false);
      if (sent.check) {
	sent.check(r);
      }
      sent.on = std::this_thread::get_id();
      sent.results = got->get_oob_results();
      sent.result = got->get_result();
      sent.flags = got->get_flags();
      sent.user_version = got->get_user_version();
      got->claim_ops(sent.ops);
      sent.count++;
    });
}

std::unique_ptr<Transfers> service(unsigned threads, unsigned max_jobs = 64)
{
  auto x = std::make_unique<Transfers>(g_ceph_context, threads, max_jobs,
				       1 << 30, 30s, [] {});
  x->start();
  return x;
}

/**
 * The new path for one op, as PrimaryLogPG takes it: plan it, and
 * either decline it there, or push it through the service on this
 * thread and send the reply when the guard drops.
 */
void new_op(Transfers& x, bool ec_direct,
	    const std::optional<ec_direct_geometry>& geo,
	    OSDOobExecutor* exec, OSDOp& op, const delivery_t& d,
	    oob_result_t& res)
{
  auto p = plan_read_push(0, op, d, ec_direct, geo);
  if (!p) {
    // deliver_oob_prepare(): inline, and declined
    res.flags |= ceph::rdma::attempt_flags(false, false, false);
    return;
  }
  p->exec = exec;
  p->token = d.token;
  p->bounds = far_bounds();
  sent_t sent;
  std::vector<OSDOp> rops;
  rops.push_back(op);
  auto join = make_join(std::move(rops), sent);
  std::unique_ptr<xfer_job> job = std::make_unique<push_job>(
    g_ceph_context, join, "obj", std::move(*p), true);
  ASSERT_EQ(0, x.submit(job));
  // it ran on this thread, inside submit(), and the reply waits for the
  // guard
  ASSERT_FALSE(job);
  ASSERT_EQ(0, sent.count);
  join->put();
  ASSERT_EQ(1, sent.count);
  ASSERT_EQ(1u, sent.ops.size());
  op = std::move(sent.ops[0]);
  // no result at all when the op neither moved bytes nor has flags
  ASSERT_LE(sent.results.size(), 1u);
  res = sent.results.empty() ? oob_result_t{} : sent.results[0];
}

void expect_same(const oob_result_t& want, const oob_result_t& got)
{
  EXPECT_EQ(want.bytes, got.bytes);
  EXPECT_EQ(want.flags, got.flags);
  EXPECT_EQ(want.crc64, got.crc64);
  EXPECT_EQ(want.crc32c, got.crc32c);
  EXPECT_EQ(want.ranges, got.ranges);
}

/// a reply with no op that moved bytes or has flags carries no results
/// at all
oob_result_t as_sent(const oob_result_t& r)
{
  return (r.bytes || r.flags) ? r : oob_result_t{};
}

} // anonymous namespace

// every kind of read op, outcome, executor and checksum request: the
// same reply as the model
TEST(OobReadDelivery, MatrixMatchesTheModel)
{
  for (unsigned threads : {0u, 2u}) {
    // with transfer threads too: the push still runs on the thread that
    // submits it
    auto x = service(threads);
    for (const auto& c : op_cases()) {
      for (auto how : {outcome::placed, outcome::failed_started,
		       outcome::failed_unstarted}) {
	for (bool complete : {false, true}) {
	  for (uint32_t w : wants) {
	    SCOPED_TRACE(c.what + " " + name(how) +
			 (complete ? " delivery-complete" : "") +
			 " wants " + std::to_string(w) + " threads " +
			 std::to_string(threads));
	    const auto d = descriptor(w);
	    fake_exec old_exec, new_exec;
	    old_exec.how = new_exec.how = how;
	    old_exec.complete = new_exec.complete = complete;

	    OSDOp old_op = c.op;
	    oob_result_t old_res;
	    legacy_op(c.ec_direct, c.geo, &old_exec, old_op, d, old_res);

	    OSDOp op = c.op;
	    oob_result_t res;
	    new_op(*x, c.ec_direct, c.geo, &new_exec, op, d, res);

	    // identical to the model, the bytes in the window included
	    expect_same(as_sent(old_res), res);
	    EXPECT_TRUE(old_op.outdata.contents_equal(op.outdata));
	    EXPECT_EQ(1, new_exec.calls);
	    EXPECT_EQ(old_exec.window, new_exec.window);
	    EXPECT_EQ(std::this_thread::get_id(), new_exec.ran_on);

	    // and what that is
	    const bool placed = how == outcome::placed;
	    EXPECT_EQ(placed && complete,
		      bool(res.flags & oob_result_t::FLAG_LANDED));
	    EXPECT_EQ(how == outcome::failed_unstarted,
		      bool(res.flags & oob_result_t::FLAG_DECLINED));
	    if (!placed) {
	      // inline, untouched
	      EXPECT_EQ(0u, res.bytes);
	      EXPECT_TRUE(op.outdata.contents_equal(c.op.outdata));
	      EXPECT_FALSE(res.flags & ~(oob_result_t::FLAG_DECLINED));
	      continue;
	    }
	    auto p = plan_read_push(0, c.op, d, c.ec_direct, c.geo);
	    ASSERT_TRUE(p);
	    EXPECT_EQ(p->payload.length(), res.bytes);
	    if (c.op.op.op == CEPH_OSD_OP_SPARSE_READ) {
	      // the extent map stays, with an empty blob
	      std::map<uint64_t, uint64_t> extents;
	      ceph::buffer::list blob;
	      auto bp = op.outdata.cbegin();
	      decode(extents, bp);
	      decode(blob, bp);
	      EXPECT_EQ(p->sparse_extents, extents);
	      EXPECT_EQ(0u, blob.length());
	      EXPECT_TRUE(bp.end());
	    } else {
	      EXPECT_EQ(0u, op.outdata.length());
	    }
	    const bool w64 = w & delivery_t::FLAG_CRC64NVME;
	    const bool w32c = w & delivery_t::FLAG_CRC32C;
	    EXPECT_EQ(w64, bool(res.flags & oob_result_t::FLAG_CRC64NVME));
	    EXPECT_EQ(w32c, bool(res.flags & oob_result_t::FLAG_CRC32C));
	    EXPECT_EQ(w != 0, bool(res.flags & oob_result_t::FLAG_RANGES));
	    EXPECT_EQ(w != 0 && *c.one_range,
		      bool(res.flags & oob_result_t::FLAG_COMBINABLE));
	    if (w) {
	      ASSERT_EQ(p->plan.size(), res.ranges.size());
	      if (*c.one_range) {
		// one range, whose values are the whole op's
		EXPECT_EQ(res.crc64, res.ranges[0].crc64);
		EXPECT_EQ(res.crc32c, res.ranges[0].crc32c);
	      }
	    } else {
	      EXPECT_TRUE(res.ranges.empty());
	    }
	  }
	}
      }
    }
  }
}

// ops a descriptor cannot move stay inline and are declined, and no
// executor is called, as before
TEST(OobReadDelivery, InlineOpsAreDeclined)
{
  auto x = service(0);
  for (const auto& c : inline_cases()) {
    for (uint32_t w : wants) {
      SCOPED_TRACE(c.what + " wants " + std::to_string(w));
      const auto d = descriptor(w);
      fake_exec old_exec, new_exec;
      OSDOp old_op = c.op;
      oob_result_t old_res;
      legacy_op(c.ec_direct, c.geo, &old_exec, old_op, d, old_res);
      OSDOp op = c.op;
      oob_result_t res;
      new_op(*x, c.ec_direct, c.geo, &new_exec, op, d, res);
      expect_same(old_res, res);
      EXPECT_EQ(oob_result_t::FLAG_DECLINED, res.flags);
      EXPECT_TRUE(op.outdata.contents_equal(c.op.outdata));
      EXPECT_EQ(0, old_exec.calls);
      EXPECT_EQ(0, new_exec.calls);
    }
  }
  // a flag this OSD does not know
  const auto d = descriptor(1u << 30);
  auto op = op_cases()[0].op;
  EXPECT_FALSE(plan_read_push(0, op, d, false, std::nullopt));
}

// the check values of the two checksums, over one contiguous range
TEST(OobReadDelivery, CheckValues)
{
  auto x = service(0);
  ceph::buffer::list data;
  data.append("123456789");
  OSDOp op = read_op(CEPH_OSD_OP_READ, 0, 9, data);
  fake_exec exec;
  exec.complete = true;
  oob_result_t res;
  new_op(*x, false, std::nullopt, &exec, op,
	 descriptor(delivery_t::FLAG_CRC64NVME | delivery_t::FLAG_CRC32C,
		    100),
	 res);
  EXPECT_EQ(9u, res.bytes);
  EXPECT_EQ(0xae8b14860a799888ull, res.crc64);
  EXPECT_EQ(0xe3069283u, res.crc32c);
  EXPECT_EQ(oob_result_t::FLAG_LANDED | oob_result_t::FLAG_COMBINABLE |
	    oob_result_t::FLAG_RANGES | oob_result_t::FLAG_CRC64NVME |
	    oob_result_t::FLAG_CRC32C, res.flags);
  ASSERT_EQ(1u, res.ranges.size());
  EXPECT_EQ(100u, res.ranges[0].ofs);
  EXPECT_EQ(9u, res.ranges[0].len);
  EXPECT_EQ(res.crc64, res.ranges[0].crc64);
  EXPECT_EQ(res.crc32c, res.ranges[0].crc32c);
  EXPECT_EQ("123456789", exec.window.substr(100, 9));
}

// several ops in one request, each its own push: one reply with every
// op's result, and only after the last push
TEST(OobReadDelivery, ManyOpsOneReply)
{
  auto x = service(0);
  fake_exec placed, failed;
  placed.complete = true;
  failed.how = outcome::failed_started;
  const auto cases = op_cases();

  std::vector<OSDOp> rops;
  rops.push_back(read_op(CEPH_OSD_OP_STAT, 0, 0, pattern(16, 20)));  // no descriptor
  rops.push_back(cases[0].op);  // READ, placed
  rops.push_back(cases[3].op);  // SPARSE_READ, placed
  rops.push_back(cases[1].op);  // SYNC_READ, fails after a start
  const std::vector<OSDOp> before = rops;

  std::vector<read_push> pushes;
  for (size_t i : {1, 2, 3}) {
    auto p = plan_read_push(i, rops[i],
			    descriptor(delivery_t::FLAG_CRC64NVME,
				       i * 100000), false, std::nullopt);
    ASSERT_TRUE(p);
    p->exec = i == 3 ? static_cast<OSDOobExecutor*>(&failed) : &placed;
    p->token = "token";
    p->bounds = far_bounds();
    pushes.push_back(std::move(*p));
  }
  sent_t sent;
  auto join = make_join(rops, sent);
  for (auto& p : pushes) {
    std::unique_ptr<xfer_job> job = std::make_unique<push_job>(
      g_ceph_context, join, "obj", std::move(p), true);
    ASSERT_EQ(0, x->submit(job));
    EXPECT_EQ(0, sent.count);
  }
  join->put();
  ASSERT_EQ(1, sent.count);
  ASSERT_EQ(4u, sent.ops.size());
  ASSERT_EQ(4u, sent.results.size());
  EXPECT_TRUE(sent.ops[0].outdata.contents_equal(before[0].outdata));
  EXPECT_EQ(0u, sent.results[0].flags);
  EXPECT_EQ(0u, sent.results[0].bytes);
  EXPECT_EQ(0u, sent.ops[1].outdata.length());
  EXPECT_EQ(before[1].outdata.length(), sent.results[1].bytes);
  EXPECT_TRUE(sent.results[1].flags & oob_result_t::FLAG_LANDED);
  EXPECT_TRUE(sent.results[2].flags & oob_result_t::FLAG_LANDED);
  EXPECT_EQ(3101u, sent.results[2].bytes);
  // failed after its writes went out: inline, and neither declined nor
  // landed, so the client fences its window
  EXPECT_TRUE(sent.ops[3].outdata.contents_equal(before[3].outdata));
  EXPECT_EQ(0u, sent.results[3].flags);
  EXPECT_EQ(0u, sent.results[3].bytes);
}

// all of a placed op's data leaves the reply: an op whose plan does not
// place exactly its payload stays inline, declined, and no executor is
// called. The executor would have reported the bytes placed as the
// payload's, and the bytes the plan left out would have been lost.
TEST(OobReadDelivery, APlanThatMissesBytesStaysInline)
{
  std::vector<op_case> v;
  {
    // the extent map covers 100 bytes of a blob of 150
    ceph::buffer::list out;
    encode(std::map<uint64_t, uint64_t>{{4096, 100}}, out);
    encode(pattern(150, 30), out);
    v.push_back({"SPARSE_READ map short of its data",
		 read_op(CEPH_OSD_OP_SPARSE_READ, 4096, 65536,
			 std::move(out))});
  }
  {
    // 200 bytes inside one chunk of shard 1, and 300 bytes of data
    v.push_back({"EC-direct more data than chunks",
		 read_op(CEPH_OSD_OP_READ, 4096 + 100, 200, pattern(300, 31)),
		 true, geo4k});
  }
  auto x = service(0);
  for (const auto& c : v) {
    SCOPED_TRACE(c.what);
    const auto d = descriptor(0);
    EXPECT_FALSE(plan_read_push(0, c.op, d, c.ec_direct, c.geo));
    fake_exec exec;
    OSDOp op = c.op;
    oob_result_t res;
    new_op(*x, c.ec_direct, c.geo, &exec, op, d, res);
    EXPECT_EQ(oob_result_t::FLAG_DECLINED, res.flags);
    EXPECT_EQ(0u, res.bytes);
    EXPECT_TRUE(op.outdata.contents_equal(c.op.outdata));
    EXPECT_EQ(0, exec.calls);
  }
}

// an op is placed only when the executor says it placed all of its
// payload: one that says it placed fewer bytes, or more, leaves the op
// inline. Its writes went out, so it is not declined either.
TEST(OobReadDelivery, AShortPlacementStaysInline)
{
  struct off_by_exec : fake_exec {
    ssize_t delta = 0;
    ssize_t execute_plan(const std::string& key, const std::string& token,
			 const ceph::buffer::list& data,
			 const placement_plan& plan,
			 std::chrono::milliseconds budget,
			 bool* started) override {
      const ssize_t r = fake_exec::execute_plan(key, token, data, plan,
						budget, started);
      return r < 0 ? r : r + delta;
    }
  };
  auto x = service(0);
  for (ssize_t delta : {-1, 1}) {
    SCOPED_TRACE(delta);
    off_by_exec exec;
    exec.complete = true;
    exec.delta = delta;
    const auto c = op_cases()[0];
    OSDOp op = c.op;
    oob_result_t res;
    new_op(*x, false, std::nullopt, &exec, op,
	   descriptor(delivery_t::FLAG_CRC32C), res);
    EXPECT_EQ(1, exec.calls);
    EXPECT_EQ(0u, res.flags);
    EXPECT_EQ(0u, res.bytes);
    EXPECT_TRUE(op.outdata.contents_equal(c.op.outdata));
  }
}

// a reply whose only push failed after a start reports nothing, as
// before: no op moved bytes or has a flag
TEST(OobReadDelivery, NothingToReport)
{
  auto x = service(0);
  fake_exec failed;
  failed.how = outcome::failed_started;
  std::vector<OSDOp> rops{op_cases()[0].op};
  auto p = plan_read_push(0, rops[0], descriptor(0), false, std::nullopt);
  ASSERT_TRUE(p);
  p->exec = &failed;
  p->bounds = far_bounds();
  sent_t sent;
  auto join = make_join(rops, sent);
  std::unique_ptr<xfer_job> job = std::make_unique<push_job>(
    g_ceph_context, join, "obj", std::move(*p), true);
  ASSERT_EQ(0, x->submit(job));
  join->put();
  ASSERT_EQ(1, sent.count);
  EXPECT_TRUE(sent.results.empty());
  EXPECT_TRUE(sent.ops[0].outdata.contents_equal(rops[0].outdata));
}

// a job that does not run starts nothing: refused, taken past its
// post_by, or dropped, its op is declined and the reply still goes out
TEST(OobReadDelivery, JobsThatDoNotRunDecline)
{
  fake_exec exec;
  exec.complete = true;
  std::vector<OSDOp> rops{op_cases()[0].op, op_cases()[1].op,
			  op_cases()[2].op};
  auto x = service(0);
  sent_t sent;
  auto join = make_join(rops, sent);
  auto job_for = [&](size_t i, bounds_t b) {
    auto p = plan_read_push(i, rops[i], descriptor(0), false, std::nullopt);
    ceph_assert(p);
    p->exec = &exec;
    p->bounds = b;
    return std::unique_ptr<xfer_job>(std::make_unique<push_job>(
      g_ceph_context, join, "obj", std::move(*p), true));
  };
  // past its post_by: the service expires it without the executor
  const auto now = mono_clock::now();
  auto late = job_for(0, {now - 1ms, now + 10s});
  ASSERT_EQ(0, x->submit(late));
  // refused: the caller expires it, as deliver_oob() does
  auto refused = job_for(1, far_bounds());
  refused->expire(-EBUSY);
  refused.reset();
  // dropped without either
  job_for(2, far_bounds()).reset();
  EXPECT_EQ(0, sent.count);
  join->put();
  ASSERT_EQ(1, sent.count);
  EXPECT_EQ(0, exec.calls);
  ASSERT_EQ(3u, sent.results.size());
  for (size_t i = 0; i < 3; i++) {
    EXPECT_EQ(oob_result_t::FLAG_DECLINED, sent.results[i].flags) << i;
    EXPECT_TRUE(sent.ops[i].outdata.contents_equal(rops[i].outdata)) << i;
  }
}

// with no transfer threads each push still keeps to the request's
// post_by, checked right before it starts: a push that outlasts it
// leaves the later ones declined, unstarted
TEST(OobReadDelivery, LaterPushesKeepToPostBy)
{
  struct slow_exec : fake_exec {
    ssize_t execute_plan(const std::string& key, const std::string& token,
			 const ceph::buffer::list& data,
			 const placement_plan& plan,
			 std::chrono::milliseconds budget,
			 bool* started) override {
      std::this_thread::sleep_for(100ms);
      return fake_exec::execute_plan(key, token, data, plan, budget,
				     started);
    }
  } exec;
  exec.complete = true;
  std::vector<OSDOp> rops{op_cases()[0].op, op_cases()[1].op};
  sent_t sent;
  auto x = service(0);
  auto join = make_join(rops, sent);
  const auto now = mono_clock::now();
  const bounds_t b{now + 50ms, now + 10s};
  for (size_t i = 0; i < rops.size(); i++) {
    auto p = plan_read_push(i, rops[i], descriptor(0, i * 100000), false,
			    std::nullopt);
    ASSERT_TRUE(p);
    p->exec = &exec;
    p->bounds = b;
    std::unique_ptr<xfer_job> job = std::make_unique<push_job>(
      g_ceph_context, join, "obj", std::move(*p), true);
    ASSERT_EQ(0, x->submit(job));
  }
  join->put();
  ASSERT_EQ(1, sent.count);
  EXPECT_EQ(1, exec.calls);
  EXPECT_EQ(1u, x->get_logger()->get(l_oob_xfer_expired));
  ASSERT_EQ(2u, sent.results.size());
  // the first started in time, and placed its op
  EXPECT_EQ(oob_result_t::FLAG_LANDED, sent.results[0].flags);
  EXPECT_EQ(rops[0].outdata.length(), sent.results[0].bytes);
  EXPECT_EQ(0u, sent.ops[0].outdata.length());
  // the second came after post_by: inline, and declined
  EXPECT_EQ(oob_result_t::FLAG_DECLINED, sent.results[1].flags);
  EXPECT_EQ(0u, sent.results[1].bytes);
  EXPECT_TRUE(sent.ops[1].outdata.contents_equal(rops[1].outdata));
}

// the pushes finish on many threads, in any order, before or after the
// guard drops: the reply goes out exactly once, after every push
TEST(OobReadDelivery, SendsOnceFromManyThreads)
{
  const unsigned seed = std::random_device{}();
  SCOPED_TRACE("seed " + std::to_string(seed));
  std::mt19937 rng(seed);
  for (int iter = 0; iter < 300; iter++) {
    const size_t n = 1 + rng() % 12;
    fake_exec exec;
    exec.complete = true;
    exec.max_delay = 200us;
    std::vector<OSDOp> rops;
    for (size_t i = 0; i < n; i++) {
      rops.push_back(read_op(CEPH_OSD_OP_READ, 0, 64, pattern(64, i)));
    }
    sent_t sent;
    // when it is sent, every push has put its result in
    sent.check = [n](MOSDOpReply* r) {
      ASSERT_EQ(n, r->get_oob_results().size());
      for (const auto& res : r->get_oob_results()) {
	EXPECT_NE(0u, res.flags);
      }
    };
    auto join = make_join(rops, sent);
    // what each job does: 0 runs, 1 expires, 2 is dropped
    std::vector<int> how(n);
    std::vector<std::unique_ptr<push_job>> jobs;
    for (size_t i = 0; i < n; i++) {
      how[i] = rng() % 3;
      auto p = plan_read_push(i, rops[i], descriptor(0, i * 64), false,
			      std::nullopt);
      ASSERT_TRUE(p);
      p->exec = &exec;
      p->bounds = far_bounds();
      jobs.push_back(std::make_unique<push_job>(g_ceph_context, join, "obj",
						std::move(*p), false));
    }
    std::vector<size_t> order(n);
    for (size_t i = 0; i < n; i++) {
      order[i] = i;
    }
    std::shuffle(order.begin(), order.end(), rng);
    const size_t guard_at = rng() % (n + 1);
    std::vector<std::thread> threads;
    for (size_t k = 0; k < n; k++) {
      if (k == guard_at) {
	join->put();
      }
      const size_t i = order[k];
      threads.emplace_back(
	[j = std::move(jobs[i]), h = how[i],
	 delay = std::chrono::microseconds(rng() % 100)]() mutable {
	  std::this_thread::sleep_for(delay);
	  if (h == 0) {
	    j->run();
	  } else if (h == 1) {
	    j->expire(-ETIMEDOUT);
	  }
	  j.reset();
	});
    }
    if (guard_at == n) {
      join->put();
    }
    join.reset();
    for (auto& t : threads) {
      t.join();
    }
    ASSERT_EQ(1, sent.count);
    ASSERT_EQ(n, sent.results.size());
    for (size_t i = 0; i < n; i++) {
      if (how[i] == 0) {
	EXPECT_EQ(64u, sent.results[i].bytes);
	EXPECT_EQ(oob_result_t::FLAG_LANDED, sent.results[i].flags);
	EXPECT_EQ(0u, sent.ops[i].outdata.length());
      } else {
	EXPECT_EQ(0u, sent.results[i].bytes);
	EXPECT_EQ(oob_result_t::FLAG_DECLINED, sent.results[i].flags);
	EXPECT_TRUE(sent.ops[i].outdata.contents_equal(rops[i].outdata));
      }
    }
  }
}

// the same through the service's threads, with a cap that refuses some
TEST(OobReadDelivery, SendsOnceThroughTransferThreads)
{
  int accepted = 0, refused_total = 0;
  for (int iter = 0; iter < 50; iter++) {
    const size_t n = 16;
    fake_exec exec;
    exec.complete = true;
    exec.max_delay = 2000us;
    std::vector<OSDOp> rops;
    for (size_t i = 0; i < n; i++) {
      rops.push_back(read_op(CEPH_OSD_OP_READ, 0, 64, pattern(64, i)));
    }
    sent_t sent;
    // after exec and sent, so that a failed ASSERT stops the service,
    // and with it every job, before they go
    auto x = service(4, 6);
    auto join = make_join(rops, sent);
    std::vector<bool> refused(n);
    for (size_t i = 0; i < n; i++) {
      auto p = plan_read_push(i, rops[i], descriptor(0, i * 64), false,
			      std::nullopt);
      ASSERT_TRUE(p);
      p->exec = &exec;
      p->bounds = far_bounds();
      std::unique_ptr<xfer_job> job = std::make_unique<push_job>(
	g_ceph_context, join, "obj", std::move(*p), false);
      if (const int r = x->submit(job); r < 0) {
	EXPECT_EQ(-EBUSY, r);
	ASSERT_TRUE(job);
	refused[i] = true;
	refused_total++;
	job->expire(r);
      } else {
	accepted++;
      }
    }
    join->put();
    join.reset();
    // stop() would shed what is still queued: wait for the reply first
    for (int i = 0; i < 10000 && !sent.count; i++) {
      std::this_thread::sleep_for(1ms);
    }
    x->stop();
    ASSERT_EQ(1, sent.count);
    ASSERT_EQ(n, sent.results.size());
    for (size_t i = 0; i < n; i++) {
      EXPECT_EQ(refused[i] ? oob_result_t::FLAG_DECLINED :
		oob_result_t::FLAG_LANDED, sent.results[i].flags) << i;
    }
    // on a transfer thread, not this one
    EXPECT_NE(std::this_thread::get_id(), exec.ran_on);
  }
  EXPECT_LT(0, accepted);
  EXPECT_LT(0, refused_total);
}

namespace {

/// holds the transfers of a gated_exec until it opens
struct gate_t {
  std::mutex m;
  std::condition_variable cv;
  bool open = false;
  int waiting = 0;

  /// until the gate opens, for up to 10 s
  void pass() {
    std::unique_lock l(m);
    waiting++;
    cv.notify_all();
    cv.wait_for(l, 10s, [this] { return open; });
  }
  /// until n transfers wait at the gate, for up to 10 s
  bool wait_for(int n) {
    std::unique_lock l(m);
    return cv.wait_for(l, 10s, [this, n] { return waiting >= n; });
  }
  void release() {
    std::lock_guard l(m);
    open = true;
    cv.notify_all();
  }
};

/// opens g when it goes out of scope. Declared after the service whose
/// threads pass g, it opens g before the service stops, so that a test
/// whose assertion fails while transfers wait at g ends, rather than
/// waiting for them forever
auto opens_at_exit(gate_t& g)
{
  return make_scope_guard([&g] { g.release(); });
}

/// a fake_exec whose transfers wait at a gate before they start
struct gated_exec : fake_exec {
  gate_t gate;

  ssize_t execute_plan(const std::string& key, const std::string& token,
		       const ceph::buffer::list& data,
		       const placement_plan& plan,
		       std::chrono::milliseconds budget,
		       bool* started) override {
    gate.pass();
    return fake_exec::execute_plan(key, token, data, plan, budget, started);
  }
};

/// the pushes of every op of rops through exec, with bounds b, as
/// deliver_oob_prepare() plans them
std::vector<read_push> pushes_for(const std::vector<OSDOp>& rops,
				  OSDOobExecutor* exec, const bounds_t& b)
{
  std::vector<read_push> v;
  for (size_t i = 0; i < rops.size(); i++) {
    auto p = plan_read_push(i, rops[i], descriptor(0, i * 100000), false,
			    std::nullopt);
    ceph_assert(p);
    p->exec = exec;
    p->token = "token";
    p->bounds = b;
    v.push_back(std::move(*p));
  }
  return v;
}

std::vector<OSDOp> reads(size_t n, unsigned seed)
{
  std::vector<OSDOp> rops;
  for (size_t i = 0; i < n; i++) {
    rops.push_back(read_op(CEPH_OSD_OP_READ, 0, 4096,
			   pattern(4096, seed + i)));
  }
  return rops;
}

/// up to 10 s for the reply
bool wait_sent(const sent_t& sent)
{
  for (int i = 0; i < 10000 && !sent.count; i++) {
    std::this_thread::sleep_for(1ms);
  }
  return sent.count > 0;
}

/// the executor's transfers run on the transfer threads
bool never_inline(const OSDOobExecutor*)
{
  return false;
}

/// every op of a reply is declined, and its data is inline
void expect_declined(const sent_t& sent, const std::vector<OSDOp>& rops)
{
  ASSERT_EQ(rops.size(), sent.results.size());
  ASSERT_EQ(rops.size(), sent.ops.size());
  for (size_t i = 0; i < rops.size(); i++) {
    EXPECT_EQ(oob_result_t::FLAG_DECLINED, sent.results[i].flags) << i;
    EXPECT_EQ(0u, sent.results[i].bytes) << i;
    EXPECT_TRUE(sent.ops[i].outdata.contents_equal(rops[i].outdata)) << i;
  }
}

/// every op of a reply was placed, and none of its data is inline
void expect_landed(const sent_t& sent, const std::vector<OSDOp>& rops)
{
  ASSERT_EQ(rops.size(), sent.results.size());
  ASSERT_EQ(rops.size(), sent.ops.size());
  for (size_t i = 0; i < rops.size(); i++) {
    EXPECT_EQ(oob_result_t::FLAG_LANDED, sent.results[i].flags) << i;
    EXPECT_EQ(rops[i].outdata.length(), sent.results[i].bytes) << i;
    EXPECT_EQ(0u, sent.ops[i].outdata.length()) << i;
  }
}

} // anonymous namespace

// where submit_pushes() runs each push: on the transfer threads only
// when there are some, and the executor's transfers do not stay on the
// op threads; only then is the reply handed off
TEST(OobReadDelivery, HandsOffOnlyToTransferThreads)
{
  const auto me = std::this_thread::get_id();
  fake_exec inl;
  inl.complete = true;
  auto stays = [&inl](const OSDOobExecutor* e) { return e == &inl; };
  for (unsigned threads : {0u, 2u}) {
    for (bool stay : {false, true}) {
      SCOPED_TRACE("threads " + std::to_string(threads) +
		   (stay ? " executor stays inline" : ""));
      const bool expect_handoff = threads > 0 && !stay;
      // handed off, the pushes wait at the gate until the guard is
      // dropped, so the last one ends, and sends the reply, on its
      // transfer thread
      gated_exec e;
      e.complete = true;
      if (!expect_handoff) {
	e.gate.release();
      }
      const auto rops = reads(2, 30);
      sent_t sent;
      auto x = service(threads);
      auto open_gate = opens_at_exit(e.gate);
      auto join = make_join(rops, sent);
      OSDOobExecutor* ex = stay ? static_cast<OSDOobExecutor*>(&inl) : &e;
      inl.calls = 0;
      const bool handed_off = submit_pushes(
	g_ceph_context, x.get(), join, "obj",
	pushes_for(rops, ex, far_bounds()), stays);
      EXPECT_EQ(expect_handoff, handed_off);
      if (!handed_off) {
	// both ran inside submit_pushes(), and the reply waits for the
	// guard
	EXPECT_EQ(2, stay ? inl.calls.load() : e.calls.load());
	EXPECT_EQ(0, sent.count);
      }
      join->put();
      if (handed_off) {
	ASSERT_TRUE(e.gate.wait_for(2));
	EXPECT_EQ(0, sent.count);
	e.gate.release();
      }
      ASSERT_TRUE(wait_sent(sent));
      x->stop();
      EXPECT_EQ(1, sent.count);
      expect_landed(sent, rops);
      if (handed_off) {
	EXPECT_NE(me, e.ran_on);
	EXPECT_NE(me, sent.on);
      } else {
	EXPECT_EQ(me, stay ? inl.ran_on : e.ran_on);
	EXPECT_EQ(me, sent.on);
      }
    }
  }
  {
    // one push stays on this thread, one goes to a transfer thread: the
    // reply leaves from wherever the last one ends, after both
    const auto rops = reads(2, 40);
    sent_t sent;
    gated_exec g;
    g.complete = true;
    inl.calls = 0;
    auto x = service(2);
    auto open_gate = opens_at_exit(g.gate);
    auto join = make_join(rops, sent);
    auto pushes = pushes_for(rops, &g, far_bounds());
    pushes[0].exec = &inl;
    EXPECT_TRUE(submit_pushes(g_ceph_context, x.get(), join, "obj",
			      std::move(pushes), stays));
    EXPECT_EQ(1, inl.calls);
    EXPECT_EQ(me, inl.ran_on);
    join->put();
    ASSERT_TRUE(g.gate.wait_for(1));
    EXPECT_EQ(0, sent.count);
    g.gate.release();
    ASSERT_TRUE(wait_sent(sent));
    x->stop();
    expect_landed(sent, rops);
    EXPECT_NE(me, g.ran_on);
  }
  {
    // no service: every push is refused, and declined
    const auto rops = reads(2, 50);
    sent_t sent;
    fake_exec e;
    auto join = make_join(rops, sent);
    EXPECT_FALSE(submit_pushes(g_ceph_context, nullptr, join, "obj",
			       pushes_for(rops, &e, far_bounds()),
			       never_inline));
    join->put();
    ASSERT_EQ(1, sent.count);
    EXPECT_EQ(0, e.calls);
    expect_declined(sent, rops);
  }
}

// handed off, a push that a transfer thread takes past its post_by -
// past the request's lease, or past the PG's readable_until as it was
// at the handoff - never calls the executor: its op is declined, and
// its data inline
TEST(OobReadDelivery, PastPostByInTheQueueDeclines)
{
  const ceph::signedspan mnow = 100s;
  for (bool lease : {true, false}) {
    SCOPED_TRACE(lease ? "lease" : "readable_until");
    // received 4.9 s ago, with a lease of 5 s; or just received, with
    // the PG readable for 100 ms more
    const auto received = mono_clock::now() - (lease ? 4900ms : 0ms);
    const auto b = make_bounds(
      received, 5.0, 3.0,
      lease ? std::nullopt : std::optional<ceph::signedspan>(mnow + 100ms),
      mnow);
    const auto now = mono_clock::now();
    ASSERT_LT(now, b.post_by);
    ASSERT_GE(now + 100ms, b.post_by);
    // the drain counts from the lease either way
    ASSERT_LT(now + 2s, b.land_by);

    const auto blocker_rops = reads(1, 60);
    const auto rops = reads(2, 70);
    gated_exec blocker;
    blocker.complete = true;
    fake_exec exec;
    exec.complete = true;
    sent_t blocker_sent, sent;
    auto x = service(1);
    auto open_gate = opens_at_exit(blocker.gate);
    // the only transfer thread is busy
    auto bjoin = make_join(blocker_rops, blocker_sent);
    ASSERT_TRUE(submit_pushes(g_ceph_context, x.get(), bjoin, "busy",
			      pushes_for(blocker_rops, &blocker,
					 far_bounds()),
			      never_inline));
    bjoin->put();
    bjoin.reset();
    ASSERT_TRUE(blocker.gate.wait_for(1));
    // so these wait in the queue past their post_by
    auto join = make_join(rops, sent);
    ASSERT_TRUE(submit_pushes(g_ceph_context, x.get(), join, "obj",
			      pushes_for(rops, &exec, b), never_inline));
    join->put();
    join.reset();
    std::this_thread::sleep_for(b.post_by + 20ms - mono_clock::now());
    EXPECT_EQ(0, sent.count);
    blocker.gate.release();
    ASSERT_TRUE(wait_sent(sent));
    ASSERT_TRUE(wait_sent(blocker_sent));
    x->stop();
    EXPECT_EQ(0, exec.calls);
    EXPECT_EQ(2u, x->get_logger()->get(l_oob_xfer_expired));
    expect_declined(sent, rops);
    expect_landed(blocker_sent, blocker_rops);
  }
}

// the service sheds the pushes still queued when it stops: they start
// nothing, their ops are declined, and their replies go out at once,
// while stop() waits for the running push, whose reply follows it
TEST(OobReadDelivery, ShedPushesDecline)
{
  const auto blocker_rops = reads(1, 80);
  const auto rops = reads(3, 90);
  gated_exec blocker;
  blocker.complete = true;
  fake_exec exec;
  exec.complete = true;
  sent_t blocker_sent, sent;
  auto x = service(1);
  std::thread stopper;
  // a failed assertion leaves the gate open, and the stop over
  auto end_stop = make_scope_guard([&] {
    blocker.gate.release();
    if (stopper.joinable()) {
      stopper.join();
    }
  });
  auto bjoin = make_join(blocker_rops, blocker_sent);
  ASSERT_TRUE(submit_pushes(g_ceph_context, x.get(), bjoin, "busy",
			    pushes_for(blocker_rops, &blocker, far_bounds()),
			    never_inline));
  bjoin->put();
  bjoin.reset();
  ASSERT_TRUE(blocker.gate.wait_for(1));
  auto join = make_join(rops, sent);
  ASSERT_TRUE(submit_pushes(g_ceph_context, x.get(), join, "obj",
			    pushes_for(rops, &exec, far_bounds()),
			    never_inline));
  join->put();
  join.reset();

  stopper = std::thread([&x] { x->stop(); });
  ASSERT_TRUE(wait_sent(sent));
  EXPECT_EQ(0, exec.calls);
  expect_declined(sent, rops);
  EXPECT_EQ(3u, x->get_logger()->get(l_oob_xfer_shed));
  // the running push still runs
  EXPECT_EQ(0, blocker_sent.count);
  blocker.gate.release();
  stopper.join();
  ASSERT_EQ(1, blocker_sent.count);
  expect_landed(blocker_sent, blocker_rops);

  // once stopped, a push is refused, and declined, at once
  sent_t late_sent;
  auto late = make_join(rops, late_sent);
  EXPECT_FALSE(submit_pushes(g_ceph_context, x.get(), late, "obj",
			     pushes_for(rops, &exec, far_bounds()),
			     never_inline));
  late->put();
  ASSERT_EQ(1, late_sent.count);
  EXPECT_EQ(0, exec.calls);
  expect_declined(late_sent, rops);
}

// while the OSD stops, a push that would wait for a transfer thread is
// refused and declined; one whose executor stays on the op threads
// still runs there
TEST(OobReadDelivery, StoppingRefusesWhatWouldWait)
{
  const auto rops = reads(2, 100);
  fake_exec exec, inl;
  exec.complete = inl.complete = true;
  sent_t sent;
  auto x = service(2);
  x->start_stopping();
  auto join = make_join(rops, sent);
  auto pushes = pushes_for(rops, &exec, far_bounds());
  pushes[1].exec = &inl;
  EXPECT_FALSE(submit_pushes(
    g_ceph_context, x.get(), join, "obj", std::move(pushes),
    [&inl](const OSDOobExecutor* e) { return e == &inl; }));
  join->put();
  ASSERT_EQ(1, sent.count);
  EXPECT_EQ(0, exec.calls);
  EXPECT_EQ(1, inl.calls);
  ASSERT_EQ(2u, sent.results.size());
  EXPECT_EQ(oob_result_t::FLAG_DECLINED, sent.results[0].flags);
  EXPECT_TRUE(sent.ops[0].outdata.contents_equal(rops[0].outdata));
  EXPECT_EQ(oob_result_t::FLAG_LANDED, sent.results[1].flags);
  EXPECT_EQ(0u, sent.ops[1].outdata.length());
  x->stop();
}

// the reply a transfer thread sends carries the result, flags and
// versions the op thread put in it under the PG lock, before the
// handoff
TEST(OobReadDelivery, ReplyKeepsWhatTheOpThreadSet)
{
  const auto rops = reads(2, 110);
  for (int result : {0, -ENOENT}) {
    SCOPED_TRACE("result " + std::to_string(result));
    gated_exec exec;
    exec.complete = true;
    sent_t sent;
    auto x = service(2);
    auto open_gate = opens_at_exit(exec.gate);
    // as complete_read_ctx() fills it in
    auto reply = ceph::make_message<MOSDOpReply>();
    reply->set_reply_versions(eversion_t(), 42);
    reply->set_result(result);
    reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
    auto join = make_join(rops, sent, reply);
    reply.reset();
    ASSERT_TRUE(submit_pushes(g_ceph_context, x.get(), join, "obj",
			      pushes_for(rops, &exec, far_bounds()),
			      never_inline));
    join->put();
    join.reset();
    ASSERT_TRUE(exec.gate.wait_for(1));
    EXPECT_EQ(0, sent.count);
    exec.gate.release();
    ASSERT_TRUE(wait_sent(sent));
    x->stop();
    EXPECT_NE(std::this_thread::get_id(), sent.on);
    EXPECT_EQ(result, sent.result);
    EXPECT_EQ(42u, sent.user_version);
    EXPECT_EQ(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK, sent.flags);
    expect_landed(sent, rops);
  }
}

// handed off, the reply leaves only after the op thread let go of
// everything of the request it held - here the op context and the
// object's context, which deliver_oob() closes in hand_off() before the
// guard drop - however fast the pushes are; and once it left, nothing
// of the join is left either. A push holds neither: they cannot reach
// it (I11).
TEST(OobReadDelivery, OpThreadLetsGoBeforeTheReply)
{
  for (bool gated : {false, true}) {
    SCOPED_TRACE(gated ? "pushes wait" : "pushes are fast");
    const auto rops = reads(3, 120);
    gated_exec exec;
    exec.complete = true;
    if (!gated) {
      exec.gate.release();
    }
    sent_t sent;
    auto x = service(4);
    auto open_gate = opens_at_exit(exec.gate);
    // what the op context and the object's context stand for
    auto ctx = std::make_shared<int>(0);
    std::weak_ptr<int> ctx_ref = ctx;
    bool closed_first = false;
    sent.check = [&](MOSDOpReply*) { closed_first = ctx_ref.expired(); };
    auto join = make_join(rops, sent);
    std::weak_ptr<oob_reply> join_ref = join;
    ASSERT_TRUE(submit_pushes(g_ceph_context, x.get(), join, "obj",
			      pushes_for(rops, &exec, far_bounds()),
			      never_inline));
    if (!gated) {
      // let the pushes end before the op thread goes on
      std::this_thread::sleep_for(20ms);
      EXPECT_EQ(0, sent.count);
    }
    // as deliver_oob() lets go of the request
    hand_off(join, [&] {
      EXPECT_EQ(0, sent.count);
      ctx.reset();
    });
    join.reset();
    if (gated) {
      ASSERT_TRUE(exec.gate.wait_for(3));
      EXPECT_EQ(0, sent.count);
      exec.gate.release();
    }
    ASSERT_TRUE(wait_sent(sent));
    x->stop();
    EXPECT_EQ(1, sent.count);
    EXPECT_TRUE(closed_first);
    EXPECT_TRUE(join_ref.expired());
    expect_landed(sent, rops);
  }
}

// the bounds that the read push, the shard push and the gather share: a
// transfer may start until receipt plus the pool's lease, or until the
// PG's read lease runs out, and its writes land by receipt plus the
// lease plus the drain, which is also how long a gather keeps a window
// it lent out of use
TEST(OobReadDelivery, LeaseAndDeliveryWindow)
{
  EXPECT_EQ(8000ms, std::chrono::duration_cast<std::chrono::milliseconds>(
	      delivery_window(5.0, 3.0)));
  const auto now = mono_clock::now();
  for (double age : {0.0, 4.9, 5.1, 9.0}) {
    SCOPED_TRACE(age);
    const auto received = now - ceph::make_timespan(age);
    const auto b = make_bounds(received, 5.0, 3.0, std::nullopt,
			       ceph::signedspan::zero());
    EXPECT_EQ(age > 5.0, lease_expired(b));
    EXPECT_EQ(received + delivery_window(5.0, 3.0), b.land_by);
  }
  const ceph::signedspan mnow = 100s;
  EXPECT_TRUE(lease_expired(make_bounds(
    now, 5.0, 3.0, std::optional<ceph::signedspan>(mnow - 1ms), mnow)));
}

// what plan_read_push() decides before the op reads
TEST(OobReadDelivery, ReadPushPossible)
{
  const auto d = descriptor(0);
  for (int t : {CEPH_OSD_OP_READ, CEPH_OSD_OP_SYNC_READ,
		CEPH_OSD_OP_SPARSE_READ}) {
    EXPECT_TRUE(read_push_possible(t, d, false, false)) << t;
  }
  EXPECT_FALSE(read_push_possible(CEPH_OSD_OP_STAT, d, false, false));
  EXPECT_FALSE(read_push_possible(CEPH_OSD_OP_CHECKSUM, d, false, false));
  // a flag this OSD does not know
  EXPECT_FALSE(read_push_possible(CEPH_OSD_OP_READ, descriptor(1u << 30),
				  false, false));
  // an EC shard-direct read needs the stripe's geometry, and is never
  // sparse
  EXPECT_TRUE(read_push_possible(CEPH_OSD_OP_READ, d, true, true));
  EXPECT_FALSE(read_push_possible(CEPH_OSD_OP_READ, d, true, false));
  EXPECT_FALSE(read_push_possible(CEPH_OSD_OP_SPARSE_READ, d, true, true));
  // and plan_read_push() keeps inline every op that it says no to, even
  // one that read data
  for (int t : {CEPH_OSD_OP_READ, CEPH_OSD_OP_SYNC_READ,
		CEPH_OSD_OP_SPARSE_READ, CEPH_OSD_OP_STAT}) {
    for (uint32_t f : {0u, 1u << 30}) {
      for (bool ec_direct : {false, true}) {
	for (bool has_ec : {false, true}) {
	  SCOPED_TRACE(std::to_string(t) + " flags " + std::to_string(f) +
		       (ec_direct ? " ec_direct" : "") +
		       (has_ec ? " geometry" : ""));
	  // a shard-direct read's reply holds the shard's part only
	  const auto data = pattern(
	    ec_direct ? shard_bytes(0, 16384, geo4k) : 16384, 7);
	  const OSDOp op = t == CEPH_OSD_OP_SPARSE_READ ?
	    sparse_op(0, 16384, {{0, 16384}}, 7) :
	    read_op(t, 0, 16384, data);
	  const std::optional<ec_direct_geometry> geo =
	    has_ec ? std::optional(geo4k) : std::nullopt;
	  const bool possible =
	    read_push_possible(t, descriptor(f), ec_direct, has_ec);
	  EXPECT_EQ(possible,
		    plan_read_push(0, op, descriptor(f), ec_direct, geo)
		    .has_value());
	}
      }
    }
  }
}

namespace {

/// a perf query by client, as `ceph osd perf query add client_id` asks
/// for it, of the ops and their latency
OSDPerfMetricQuery client_query()
{
  OSDPerfMetricSubKeyDescriptor d(OSDPerfMetricSubKeyType::CLIENT_ID,
				  "^(.*)$");
  d.regex = d.regex_str.c_str();
  return OSDPerfMetricQuery(
    OSDPerfMetricKeyDescriptor{d},
    PerformanceCounterDescriptors{PerformanceCounterType::OPS,
				  PerformanceCounterType::LATENCY});
}

/// the counters of query q in stats, by key, as the mgr gets them
std::map<OSDPerfMetricKey, PerformanceCounters> counted(
  DynamicPerfStats& stats, const OSDPerfMetricQuery& q)
{
  std::map<OSDPerfMetricQuery, OSDPerfMetricReport> reports;
  stats.add_to_reports({{q, {}}}, &reports);
  std::map<OSDPerfMetricKey, PerformanceCounters> out;
  for (const auto& [key, bl] : reports[q].group_packed_performance_counters) {
    auto p = bl.cbegin();
    for (const auto& d : q.performance_counter_descriptors) {
      d.unpack_counter(p, &out[key].emplace_back());
    }
  }
  return out;
}

} // anonymous namespace

// A PG's pushed reads reach the perf query results of the OSD with the
// PG's own ops. The OSD sets the queries of each PG it has when they
// change, and hands each PG fresh stats with them at each report, in
// place of its own. A PG that a split made, or that this OSD made as a
// replica and that became primary, gets its queries only the second
// way: its pushed reads count from the first report on, as its other
// ops do
TEST(OobReadDelivery, PerfStatsTakeTheQueriesOfAReport)
{
  OpTracker tracker{g_ceph_context, false, 1};
  auto on_exit = make_scope_guard([&] { tracker.on_shutdown(); });
  spg_t pgid(pg_t(0, 3));
  auto m = new MOSDOp(0, 1, hobject_t(), pgid, 1, CEPH_OSD_FLAG_READ, 0);
  m->set_src(entity_name_t::CLIENT(4123));
  auto op = tracker.create_request<OpRequest, Message*>(m);
  const auto q = client_query();
  const std::list<OSDPerfMetricQuery> queries{q};
  const OSDPerfMetricKey client{{"client.4123"}};

  // the PG: its own stats, and those of its pushed reads
  DynamicPerfStats own;
  pg_perf_stats pushed(pgid);
  // a report, as OSD::get_perf_reports() and
  // PrimaryLogPG::get_dynamic_perf_stats() make it
  auto report = [&] {
    DynamicPerfStats stats(queries);
    auto p = pushed.take(stats);
    std::swap(own, stats);
    stats.merge(p);
    return counted(stats, q);
  };
  const auto read = [&](utime_t latency) {
    pushed.add(0, *op, 0, 4096, latency);
  };

  // no queries yet: a read does not count
  read(utime_t(1, 0));
  EXPECT_TRUE(report().empty());
  // the report gave the PG its queries: a read counts, in the next
  // report, and only there
  read(utime_t(2, 0));
  read(utime_t(3, 0));
  auto c = report();
  ASSERT_EQ(1u, c.size());
  ASSERT_EQ(1u, c.count(client));
  ASSERT_EQ(2u, c[client].size());
  EXPECT_EQ(2u, c[client][0].first);
  EXPECT_EQ(5'000'000'000u, c[client][1].first);
  EXPECT_EQ(2u, c[client][1].second);
  EXPECT_TRUE(report().empty());
  read(utime_t(1, 0));
  c = report();
  ASSERT_EQ(1u, c.count(client));
  EXPECT_EQ(1u, c[client][0].first);

  // the OSD dropped its queries: a read counts no more
  pushed.set_queries({});
  read(utime_t(1, 0));
  {
    DynamicPerfStats none;
    auto p = pushed.take(none);
    EXPECT_TRUE(counted(p, q).empty());
  }
  // and a PG that got the queries from the OSD counts a read at once
  pg_perf_stats set(pgid);
  set.set_queries(queries);
  set.add(0, *op, 0, 4096, utime_t(1, 0));
  DynamicPerfStats fresh(queries);
  auto p = set.take(fresh);
  c = counted(p, q);
  ASSERT_EQ(1u, c.count(client));
  EXPECT_EQ(1u, c[client][0].first);
  // the report's fresh stats are not changed
  EXPECT_TRUE(counted(fresh, q).empty());
}


namespace {

hobject_t fence_obj(const char* name)
{
  return hobject_t(object_t(name), "", CEPH_NOSNAP, 0, 1, "");
}

} // anonymous namespace

// a reply settles; it asks for a kick only when the PG fenced it
TEST(OobReadDelivery, ReadOutKicksOnlyWhenFenced)
{
  read_out plain;
  EXPECT_FALSE(plain.settled());
  EXPECT_FALSE(plain.settle());
  EXPECT_TRUE(plain.settled());

  read_out fenced;
  fenced.fence();
  EXPECT_FALSE(fenced.settled());
  EXPECT_TRUE(fenced.settle());
  EXPECT_TRUE(fenced.settled());
}

// what waits for the replies of the reads of its object
TEST(OobReadDelivery, MayChangeObject)
{
  auto ops = [](std::initializer_list<int> codes) {
    std::vector<OSDOp> v;
    for (int c : codes) {
      OSDOp o;
      o.op.op = c;
      v.push_back(o);
    }
    return v;
  };
  const int rd = CEPH_OSD_FLAG_READ;
  EXPECT_FALSE(may_change_object(ops({CEPH_OSD_OP_READ}), rd));
  EXPECT_FALSE(may_change_object(
    ops({CEPH_OSD_OP_STAT, CEPH_OSD_OP_SPARSE_READ, CEPH_OSD_OP_GETXATTR,
	 CEPH_OSD_OP_OMAPGETVALS, CEPH_OSD_OP_CMPEXT}), rd));
  // the client says so
  EXPECT_TRUE(may_change_object(ops({CEPH_OSD_OP_READ}), CEPH_OSD_FLAG_WRITE));
  // an op says so
  EXPECT_TRUE(may_change_object(ops({CEPH_OSD_OP_WRITE}), 0));
  EXPECT_TRUE(may_change_object(
    ops({CEPH_OSD_OP_READ, CEPH_OSD_OP_SETXATTR}), rd));
  EXPECT_TRUE(may_change_object(ops({CEPH_OSD_OP_DELETE}), 0));
  EXPECT_TRUE(may_change_object(ops({CEPH_OSD_OP_WATCH}), rd));
  // a class method can write
  EXPECT_TRUE(may_change_object(ops({CEPH_OSD_OP_CALL}), rd));
  // so can a cache op
  EXPECT_TRUE(may_change_object(ops({CEPH_OSD_OP_CACHE_FLUSH}), rd));
  EXPECT_TRUE(may_change_object(ops({CEPH_OSD_OP_CACHE_EVICT}), rd));
}

// a read out holds the ops that may change its object, and every op on
// it behind them, in order; others pass
TEST(OobReadDelivery, ReadFenceHoldsWritesAndWhatFollows)
{
  read_fences<int> f;
  const auto a = fence_obj("a");
  const auto b = fence_obj("b");
  const utime_t now(100, 0);
  EXPECT_FALSE(f.wait(a, 1, true, now));

  auto r = std::make_shared<read_out>();
  f.add(a, r);
  EXPECT_FALSE(f.empty());
  // a read passes, and an op on another object
  EXPECT_FALSE(f.wait(a, 2, false, now));
  EXPECT_FALSE(f.wait(b, 3, true, now));
  // a write waits, and so does what comes after it, a read too
  EXPECT_TRUE(f.wait(a, 4, true, now));
  EXPECT_TRUE(f.wait(a, 5, false, utime_t(101, 0)));
  EXPECT_TRUE(f.wait(a, 6, true, utime_t(102, 0)));
  ASSERT_EQ(1u, f.get().count(a));
  EXPECT_EQ(now, f.get().at(a).since);
  EXPECT_EQ((std::list<int>{4, 5, 6}), f.get().at(a).waiters);

  // a kick before the reply left runs nothing
  EXPECT_TRUE(f.kick(a).empty());
  EXPECT_TRUE(f.kick(b).empty());
  EXPECT_TRUE(r->settle());
  EXPECT_EQ((std::list<int>{4, 5, 6}), f.kick(a));
  EXPECT_TRUE(f.empty());
  // a late kick finds nothing, and the object is open again
  EXPECT_TRUE(f.kick(a).empty());
  EXPECT_FALSE(f.wait(a, 7, true, now));
}

// the fence waits for every read of the object that is out
TEST(OobReadDelivery, ReadFenceWaitsForEveryRead)
{
  read_fences<int> f;
  const auto a = fence_obj("a");
  auto r1 = std::make_shared<read_out>();
  auto r2 = std::make_shared<read_out>();
  f.add(a, r1);
  f.add(a, r2);
  EXPECT_TRUE(f.wait(a, 1, true, utime_t()));
  r2->settle();
  EXPECT_TRUE(f.kick(a).empty());
  // a read after the write waits behind it, and adds nothing
  EXPECT_TRUE(f.wait(a, 2, false, utime_t()));
  r1->settle();
  EXPECT_EQ((std::list<int>{1, 2}), f.kick(a));
  EXPECT_TRUE(f.empty());
}

// a write after every reply left goes on, whether or not the kicks came
TEST(OobReadDelivery, ReadFenceOfSettledReadsPasses)
{
  read_fences<int> f;
  const auto a = fence_obj("a");
  auto r = std::make_shared<read_out>();
  f.add(a, r);
  r->settle();
  EXPECT_FALSE(f.wait(a, 1, true, utime_t()));
  EXPECT_TRUE(f.empty());
  EXPECT_TRUE(f.kick(a).empty());
}

// an interval change takes the ops of every fence, and keeps the reads
// that are out: an op that runs again waits again
TEST(OobReadDelivery, ReadFenceOutlivesAnIntervalChange)
{
  read_fences<int> f;
  const auto a = fence_obj("a");
  const auto b = fence_obj("b");
  auto ra = std::make_shared<read_out>();
  auto rb = std::make_shared<read_out>();
  f.add(a, ra);
  f.add(b, rb);
  EXPECT_TRUE(f.wait(a, 1, true, utime_t()));
  EXPECT_TRUE(f.wait(a, 2, false, utime_t()));
  EXPECT_TRUE(f.wait(b, 3, true, utime_t()));
  rb->settle();

  // each fence's ops in order (a sorts before b); the fence of b ends,
  // its read settled
  EXPECT_EQ((std::list<int>{1, 2, 3}), f.take_waiters());
  EXPECT_EQ(1u, f.get().count(a));
  EXPECT_EQ(0u, f.get().count(b));
  EXPECT_TRUE(f.get().at(a).waiters.empty());

  // they run again: the ops on a wait again, the one on b goes on
  EXPECT_TRUE(f.wait(a, 1, true, utime_t()));
  EXPECT_TRUE(f.wait(a, 2, false, utime_t()));
  EXPECT_FALSE(f.wait(b, 3, true, utime_t()));
  ra->settle();
  EXPECT_EQ((std::list<int>{1, 2}), f.kick(a));
  EXPECT_TRUE(f.empty());

  // a PG that is no longer primary drops them all
  auto r = std::make_shared<read_out>();
  f.add(a, r);
  EXPECT_TRUE(f.wait(a, 4, true, utime_t()));
  f.clear();
  EXPECT_TRUE(f.empty());
  EXPECT_TRUE(r->settle());
  EXPECT_TRUE(f.kick(a).empty());
}

// replies that settle on other threads, while the PG adds reads and
// writes wait; every write runs exactly once, and only after the reply
// of each read added before it settled
TEST(OobReadDelivery, ReadFenceUnderConcurrentSettles)
{
  read_fences<int> f;
  const auto a = fence_obj("a");
  std::vector<std::shared_ptr<read_out>> reads;
  std::vector<std::thread> settlers;
  std::atomic<unsigned> kicks{0};
  for (int i = 0; i < 16; i++) {
    auto r = std::make_shared<read_out>();
    f.add(a, r);
    reads.push_back(r);
    settlers.emplace_back([r, &kicks, i] {
      std::this_thread::sleep_for(std::chrono::microseconds(50 * (16 - i)));
      if (r->settle()) {
	kicks++;
      }
    });
  }
  // a write that does not wait found every reply settled
  unsigned waited = 0;
  for (int w = 0; w < 4; w++) {
    if (f.wait(a, w, true, utime_t())) {
      waited++;
    } else {
      EXPECT_TRUE(std::all_of(reads.begin(), reads.end(),
			      [](const auto& r) { return r->settled(); }));
    }
  }
  for (auto& t : settlers) {
    t.join();
  }
  EXPECT_EQ(16u, kicks.load());
  // the kicks run on the PG one after another, and the first finds every
  // reply settled: it runs every write that waited, in order
  std::list<int> ran;
  for (unsigned k = 0; k < kicks; k++) {
    auto ls = f.kick(a);
    if (k > 0) {
      EXPECT_TRUE(ls.empty());
    }
    ran.splice(ran.end(), ls);
  }
  EXPECT_TRUE(f.empty());
  EXPECT_EQ(waited, ran.size());
  EXPECT_TRUE(std::is_sorted(ran.begin(), ran.end()));
}
