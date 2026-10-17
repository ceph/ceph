// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * The writes whose payload the primary pulls out of client memory.
 *
 * The client message throttle (OpRequest::reserve_pulled()): a request
 * reserves the bytes before the pull, only while the throttle has room
 * for them, and gives them back when it is done, with the ops' data
 * that holds them, whatever limit the throttle has by then.
 *
 * The pull step (src/osd/oob_pull.h), through a fake executor: every
 * pull of a request is checked before any starts; the pulls are all or
 * nothing; a run of the request after they started pulls nothing and
 * charges the throttle no more; the checksums are those of the payload
 * as pulled; and a reply that fails the request declines
 * only the ops whose pull started nothing, and none while the pulls
 * run. Each op keeps the record of its pull for the PG log, from which
 * the reply to a resend answered as a dup reports it. The pulls count
 * their bounds from the request's receipt, as a read's pushes do: a
 * request that waited in the PG past the lease pulls nothing, and fails
 * as one whose pulls the OSD cannot start, so that its client can send
 * the payloads inline.
 *
 * With transfer threads the pulls run there, and the request parks: the
 * set's settle kicks the PG where it waits exactly once, and a set that
 * settles before the request parks is consumed by the thread that
 * would have parked it, which then kicks nothing. A coroutine's request,
 * and one whose executor's transfers stay on the op threads, still pull
 * on the thread that starts them. A pull of a request that went away
 * does not start. Pulls on a transfer thread are out for every PG of the
 * OSD until they settled, and a later attempt of their request waits for
 * them, and gets its own kick; pulls that run where they start never
 * are.
 */

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "common/Clock.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/crc64nvme.h"
#include "common/perf_counters.h"
#include "common/rdma_token.h"
#include "common/Throttle.h"
#include "messages/MOSDOp.h"
#include "osd/OpRequest.h"
#include "osd/oob_executor.h"
#include "osd/oob_placement.h"
#include "osd/oob_pull.h"
#include "osd/oob_xfer.h"

#include "gtest/gtest.h"

using namespace ceph::osd::oob;
using namespace std::chrono_literals;
using ceph::rdma::delivery_t;
using ceph::rdma::oob_result_t;

namespace {

/**
 * Pulls out of a client window of its own, or fails as the token says:
 * "ok" pulls, "started" fails after a read went out, "unstarted" fails
 * before, "big" is too large to stage, "fail" fails with fail_r, after
 * a read went out when fail_started, and "none" is a token it does not
 * pull from.
 */
struct fake_puller : OSDOobExecutor {
  std::string window = std::string(1 << 16, '\0');
  /// how long each pull takes
  std::chrono::milliseconds delay{0};
  /// what a pull from "fail" returns, and whether it says a read went out
  int fail_r = -EIO;
  bool fail_started = false;

  /// runs in each pull, while it reads the window
  std::function<void()> during;

  std::mutex m;
  std::vector<std::string> calls;  ///< the tokens pulled from, in order
  std::vector<std::thread::id> ran_on;
  /// the time each pull had left to land, as the job gave it
  std::vector<std::chrono::milliseconds> budgets;

  fake_puller() {
    for (size_t i = 0; i < window.size(); i++) {
      window[i] = static_cast<char>(i * 7 + i / 251);
    }
  }

  bool is_available() const override { return true; }
  bool handles(const std::string&) const override { return false; }
  bool pulls(const std::string& token) const override {
    return token != "none";
  }
  void dump_stats(ceph::Formatter*) const override {}

  ssize_t execute_plan(const std::string&, const std::string&,
		       const ceph::buffer::list&, const placement_plan&,
		       std::chrono::milliseconds, bool* started) override {
    ADD_FAILURE() << "a pull pushed";
    *started = false;
    return -EOPNOTSUPP;
  }

  ssize_t execute_pull(const std::string& key, const std::string& token,
		       const placement_plan& plan, uint64_t total,
		       ceph::buffer::list* out,
		       std::chrono::milliseconds budget,
		       bool* started) override {
    {
      std::lock_guard l(m);
      calls.push_back(token);
      ran_on.push_back(std::this_thread::get_id());
      budgets.push_back(budget);
    }
    EXPECT_EQ("obj", key);
    EXPECT_GT(budget.count(), 0);
    if (delay.count() > 0) {
      std::this_thread::sleep_for(delay);
    }
    if (during) {
      during();
    }
    if (token == "unstarted") {
      *started = false;
      return -ETIMEDOUT;
    }
    if (token == "big") {
      *started = false;
      return -E2BIG;
    }
    if (token == "fail") {
      *started = fail_started;
      return fail_r;
    }
    *started = true;
    if (token == "started") {
      return -EIO;
    }
    std::string buf(total, '\0');
    for (const auto& t : plan) {
      ceph_assert(t.client_ofs + t.len <= window.size());
      ceph_assert(t.local_ofs + t.len <= total);
      memcpy(buf.data() + t.local_ofs, window.data() + t.client_ofs, t.len);
    }
    out->append(buf);
    return total;
  }
};

/// one op of a request
struct op_spec {
  uint16_t type = CEPH_OSD_OP_WRITEFULL;
  uint64_t len = 0;
  std::string token;  ///< empty: the op carries no descriptor
  uint32_t flags = delivery_t::FLAG_PULL;
  uint64_t base = 0;
  std::string data;   ///< inline data
};

/**
 * What the pull of one op whose payload is bl must report: its byte
 * count and, for each checksum its descriptor asks for, the checksum of
 * the payload as pulled, with one range at the descriptor's offset.
 */
oob_result_t pulled_result(const ceph::buffer::list& bl,
			   const delivery_t& d, uint64_t len)
{
  oob_result_t res;
  res.bytes = len;
  const bool want64 = d.flags & ceph::rdma::delivery_t::FLAG_CRC64NVME;
  const bool want32c = d.flags & ceph::rdma::delivery_t::FLAG_CRC32C;
  if (want64 || want32c) {
    if (want64) {
      res.crc64 = ceph::crc64nvme(bl);
    }
    if (want32c) {
      res.crc32c = ceph::rdma::crc32c_canonical(bl);
    }
    res.ranges.push_back({d.base_offset, len, res.crc64, res.crc32c});
    res.flags |= ceph::rdma::oob_result_t::FLAG_COMBINABLE |
      ceph::rdma::oob_result_t::FLAG_RANGES |
      (want64 ? ceph::rdma::oob_result_t::FLAG_CRC64NVME : 0) |
      (want32c ? ceph::rdma::oob_result_t::FLAG_CRC32C : 0);
  }
  return res;
}

void expect_same(const oob_result_t& want, const oob_result_t& got)
{
  EXPECT_EQ(want.bytes, got.bytes);
  EXPECT_EQ(want.flags, got.flags);
  EXPECT_EQ(want.crc64, got.crc64);
  EXPECT_EQ(want.crc32c, got.crc32c);
  EXPECT_EQ(want.ranges, got.ranges);
}

/// the kicks of the PGs where requests wait for their pulls
struct kicks_t {
  std::mutex m;
  std::condition_variable cond;
  std::vector<kick_target> kicked;
  std::vector<std::thread::id> from;

  kick_fn fn() {
    return [this](const kick_target& t) {
      std::lock_guard l(m);
      kicked.push_back(t);
      from.push_back(std::this_thread::get_id());
      cond.notify_all();
    };
  }
  size_t count() {
    std::lock_guard l(m);
    return kicked.size();
  }
  /// wait until there are n kicks, for at most 10 s
  bool wait_for(size_t n) {
    std::unique_lock l(m);
    return cond.wait_for(l, 10s, [&] { return kicked.size() >= n; });
  }
};

/// the PG and object a test request waits at
kick_target test_target(unsigned seed = 1)
{
  kick_target t;
  t.pgid = spg_t(pg_t(seed, 3));
  t.head = hobject_t(object_t("obj"), "", CEPH_NOSNAP, seed, 3, "");
  t.epoch = 10 + seed;
  return t;
}

bool same_target(const kick_target& a, const kick_target& b)
{
  return a.pgid == b.pgid && a.head == b.head && a.epoch == b.epoch;
}

struct OobPull : public ::testing::Test {
  boost::intrusive_ptr<CephContext> cct{
    new CephContext(CEPH_ENTITY_TYPE_OSD), false};
  OpTracker tracker{cct.get(), false, 1};

  ~OobPull() override {
    // join the tracker's history thread, which would outlive the fixture
    tracker.on_shutdown();
  }

  /// a client write, as the OSD's messenger hands it over
  OpRequestRef new_op() {
    spg_t pgid;
    return tracker.create_request<OpRequest, Message*>(
      new MOSDOp(0, 1, hobject_t(), pgid, 1, CEPH_OSD_FLAG_WRITE, 0));
  }

  /// a client write of the ops specs says to object "obj", received
  /// at recv; its attempts share tid
  OpRequestRef new_op(const std::vector<op_spec>& specs,
		      utime_t recv = ceph_clock_now(), ceph_tid_t tid = 1) {
    spg_t pgid;
    const hobject_t oid(object_t("obj"), "", CEPH_NOSNAP, 0, 1, "");
    auto m = new MOSDOp(0, tid, oid, pgid, 1, CEPH_OSD_FLAG_WRITE, 0);
    std::vector<delivery_t> deliveries;
    for (const auto& s : specs) {
      OSDOp op;
      op.op.op = s.type;
      op.op.extent.offset = 0;
      op.op.extent.length = s.len;
      op.indata.append(s.data);
      m->ops.push_back(op);
      delivery_t d;
      d.token = s.token;
      if (!s.token.empty()) {
	d.base_offset = s.base;
	d.flags = s.flags;
      }
      deliveries.push_back(d);
    }
    m->set_rdma_deliveries(std::move(deliveries));
    m->set_recv_stamp(recv);
    return tracker.create_request<OpRequest, Message*>(m);
  }

  /// the request's ops, which the pull step fills and the request runs
  static std::vector<OSDOp>& ops_of(const OpRequestRef& op) {
    return static_cast<MOSDOp*>(op->get_nonconst_req())->ops;
  }

  /// what the PG hands the pull step
  pull_env env(Transfers* x, fake_puller* e, Throttle* t) {
    pull_env env;
    env.cct = cct.get();
    env.xfer = x;
    env.puller_for = [e](const std::string& token) -> OSDOobExecutor* {
      return e->pulls(token) ? e : nullptr;
    };
    env.client_throttle = [t] { return t; };
    env.max_reply = 64;
    env.lease = 6;
    env.drain = 4;
    env.target = test_target();
    env.kick = kicks.fn();
    return env;
  }

  kicks_t kicks;

  /**
   * The pull step of a request, as the PG runs it: when it parks, the
   * PG runs it again once the settle kicked it. Returns what the run
   * that went on returned; *parked says whether one waited.
   */
  int run_request(const pull_env& env, const OpRequestRef& op,
		  bool* parked = nullptr) {
    auto& ops = ops_of(op);
    const size_t before = kicks.count();
    int r = pull_payloads(env, *op, ops);
    if (parked) {
      *parked = r == PULLS_PARKED;
    }
    if (r != PULLS_PARKED) {
      return r;
    }
    EXPECT_TRUE(kicks.wait_for(before + 1));
    EXPECT_TRUE(op->oob_pull->settled());
    r = pull_payloads(env, *op, ops);
    EXPECT_NE(PULLS_PARKED, r);
    return r;
  }

  /// wait until x runs no job, for at most 10 s
  static void wait_until_idle(Transfers& x) {
    for (int i = 0; i < 1000 && (x.get_logger()->get(l_oob_xfer_running) ||
				 x.get_logger()->get(l_oob_xfer_queued)); i++) {
      std::this_thread::sleep_for(10ms);
    }
    EXPECT_EQ(0u, x.get_logger()->get(l_oob_xfer_running));
  }

  /// a transfer service with threads transfer threads
  std::unique_ptr<Transfers> service(unsigned threads) {
    auto x = std::make_unique<Transfers>(cct.get(), threads, 16, 1 << 30,
					 30s, [] {});
    x->start();
    return x;
  }

  /// the oob results of a reply that OSDService::reply_op_error()
  /// sends with result r, and, for a dup, op_returns the return data
  /// the PG log kept for the write it answers. With an error that is no
  /// dup, PrimaryLogPG::complete_read_ctx() builds the same
  static std::vector<oob_result_t> error_results(
    const OpRequestRef& op, int r = -EIO, bool dup = false,
    const std::vector<pg_log_op_return_item_t>& op_returns = {}) {
    const auto& deliveries = op->get_req<MOSDOp>()->get_rdma_deliveries();
    std::vector<oob_result_t> oob(deliveries.size());
    op_error_results(deliveries, op_returns, r, dup, op->oob_pull.get(),
		     oob);
    return oob;
  }
};

} // anonymous namespace

TEST_F(OobPull, HeldUntilTheRequestIsDone)
{
  Throttle t(cct.get(), "osd_client_bytes", 1000, false);
  auto op = new_op();
  ASSERT_TRUE(op->reserve_pulled(&t, 300));
  ASSERT_TRUE(op->reserve_pulled(&t, 200));
  EXPECT_EQ(500, t.get_current());
  op.reset();
  EXPECT_EQ(0, t.get_current());
}

TEST_F(OobPull, NoRoomReservesNothing)
{
  // the requests the messenger has read hold most of the throttle: a
  // pull that does not fit is declined before it takes anything
  Throttle t(cct.get(), "osd_client_bytes", 1000, false);
  auto a = new_op();
  ASSERT_TRUE(a->reserve_pulled(&t, 900));
  auto b = new_op();
  EXPECT_FALSE(b->reserve_pulled(&t, 200));
  EXPECT_EQ(900, t.get_current());
  b.reset();
  EXPECT_EQ(900, t.get_current());

  // the room comes back with the request that held it
  a.reset();
  EXPECT_EQ(0, t.get_current());
  auto c = new_op();
  EXPECT_TRUE(c->reserve_pulled(&t, 200));
  EXPECT_EQ(200, t.get_current());
}

TEST_F(OobPull, NothingToCharge)
{
  // a peer whose policy has no byte throttle, and an empty payload
  Throttle t(cct.get(), "osd_client_bytes", 1000, false);
  auto op = new_op();
  EXPECT_TRUE(op->reserve_pulled(nullptr, 100));
  EXPECT_TRUE(op->reserve_pulled(&t, 0));
  EXPECT_EQ(0, t.get_current());
  op.reset();
  EXPECT_EQ(0, t.get_current());
}

TEST_F(OobPull, LimitChangedWhileHeld)
{
  // osd_client_message_size_cap 0 means no limit; a limit set at run
  // time (OSD::handle_conf_change()) must get back what was taken
  // without one, or the throttle's count goes negative
  Throttle t(cct.get(), "osd_client_bytes", 0, false);
  auto op = new_op();
  ASSERT_TRUE(op->reserve_pulled(&t, 300));
  EXPECT_EQ(300, t.get_current());
  t.reset_max(1000);
  op.reset();
  EXPECT_EQ(0, t.get_current());

  // and the limit taken away
  op = new_op();
  ASSERT_TRUE(op->reserve_pulled(&t, 300));
  t.reset_max(0);
  op.reset();
  EXPECT_EQ(0, t.get_current());
}

TEST_F(OobPull, PullsEveryPayload)
{
  // with transfer threads the pulls run there, the request waits for
  // them, and its next run goes on with its payloads
  for (unsigned threads : {0u, 2u}) {
    SCOPED_TRACE(threads);
    auto x = service(threads);
    fake_puller e;
    Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
    const std::vector<op_spec> specs = {
      {CEPH_OSD_OP_WRITEFULL, 4096, "ok",
       delivery_t::FLAG_PULL | delivery_t::FLAG_CRC64NVME, 0},
      {CEPH_OSD_OP_CREATE, 0, ""},
      {CEPH_OSD_OP_WRITE, 5000, "ok",
       delivery_t::FLAG_PULL | delivery_t::FLAG_CRC64NVME |
       delivery_t::FLAG_CRC32C, 8192},
      {CEPH_OSD_OP_WRITE, 100, "ok", delivery_t::FLAG_PULL, 20000},
      {CEPH_OSD_OP_WRITE, 3000, "ok",
       delivery_t::FLAG_PULL | delivery_t::FLAG_CRC32C, 30001},
      // nothing to pull
      {CEPH_OSD_OP_WRITE, 0, "ok", delivery_t::FLAG_PULL, 0},
    };
    auto op = new_op(specs);
    auto& ops = ops_of(op);
    bool parked = false;
    ASSERT_EQ(0, run_request(env(x.get(), &e, &t), op, &parked));
    EXPECT_EQ(threads > 0, parked);

    EXPECT_EQ(std::vector<std::string>(4, "ok"), e.calls);
    for (const auto& id : e.ran_on) {
      EXPECT_EQ(threads == 0, std::this_thread::get_id() == id);
    }
    EXPECT_EQ(4096 + 5000 + 100 + 3000, t.get_current());
    ASSERT_TRUE(op->oob_pull);
    EXPECT_TRUE(op->oob_pull->consumed());
    ASSERT_EQ(specs.size(), op->oob_pulled.size());
    const auto& deliveries = op->get_req<MOSDOp>()->get_rdma_deliveries();
    for (size_t i = 0; i < specs.size(); i++) {
      SCOPED_TRACE(i);
      const auto& s = specs[i];
      if (s.token.empty() || s.len == 0) {
	EXPECT_EQ(0u, ops[i].indata.length());
	expect_same(oob_result_t{}, op->oob_pulled[i]);
	continue;
      }
      EXPECT_EQ(e.window.substr(s.base, s.len), ops[i].indata.to_str());
      expect_same(pulled_result(ops[i].indata, deliveries[i], s.len),
		  op->oob_pulled[i]);
    }
    // one job, for all the pulls of the request; a transfer thread
    // counts it once the job returned, after the kick
    wait_until_idle(*x);
    EXPECT_EQ(1u, x->get_logger()->get_tavg_ns(l_oob_xfer_pull_lat).second);
    EXPECT_EQ(0u, x->get_logger()->get_tavg_ns(l_oob_xfer_push_lat).second);
    EXPECT_EQ(0u, x->get_logger()->get(l_oob_xfer_running));

    // the bytes go back when the request is done
    op.reset();
    EXPECT_EQ(0, t.get_current());
  }
}

TEST_F(OobPull, AllOrNothing)
{
  auto x = service(0);
  struct case_t {
    std::vector<std::string> tokens;
    int r;
    std::vector<std::string> calls;  ///< the pulls that ran, in order
  };
  const std::vector<case_t> cases = {
    // the second fails after its read went out: the third never starts
    {{"ok", "started", "ok"}, -EIO, {"ok", "started"}},
    // the first fails before anything went out
    // (the executor's -ETIMEDOUT: its budget was too short to start)
    {{"unstarted", "ok"}, -EOPNOTSUPP, {"unstarted"}},
    // a payload the transport cannot stage is one it cannot pull
    {{"ok", "big", "ok"}, -EOPNOTSUPP, {"ok", "big"}},
  };
  for (const auto& c : cases) {
    SCOPED_TRACE(c.calls.back());
    fake_puller e;
    Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
    std::vector<op_spec> specs;
    for (size_t i = 0; i < c.tokens.size(); i++) {
      specs.push_back({CEPH_OSD_OP_WRITE, 4096, c.tokens[i],
		       delivery_t::FLAG_PULL | delivery_t::FLAG_CRC64NVME,
		       i * 4096});
    }
    auto op = new_op(specs);
    auto& ops = ops_of(op);
    EXPECT_EQ(c.r, pull_payloads(env(x.get(), &e, &t), *op, ops));
    EXPECT_EQ(c.calls, e.calls);
    // no op has its payload, and no result is reported: the request
    // fails, and nothing of it is written
    for (size_t i = 0; i < ops.size(); i++) {
      EXPECT_EQ(0u, ops[i].indata.length());
      expect_same(oob_result_t{}, op->oob_pulled[i]);
    }
    ASSERT_TRUE(op->oob_pull);
    EXPECT_TRUE(op->oob_pull->settled());
    EXPECT_FALSE(op->oob_pull->consumed());
    // charged all the same, until the request is done
    EXPECT_EQ(int64_t(4096 * c.tokens.size()), t.get_current());
    op.reset();
    EXPECT_EQ(0, t.get_current());
  }
}

// Whatever an executor returns, the client hears one of the two errors
// of the pull contract (delivery_t::FLAG_PULL): -EOPNOTSUPP when nothing
// of the read went out, on which it sends the payload inline, and -EIO
// when the read failed after some of it may have
TEST_F(OobPull, ExecutorErrorsKeepTheContract)
{
  auto x = service(0);
  Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
  for (const int r : {-ENOMEM, -EBUSY, -ETIMEDOUT, -E2BIG, -EINVAL,
		      -ESHUTDOWN, -EOPNOTSUPP, -ECANCELED, -EACCES,
		      -ENOTRECOVERABLE, -EIO}) {
    for (const bool started : {false, true}) {
      SCOPED_TRACE(std::to_string(r) + (started ? " started" : ""));
      fake_puller e;
      e.fail_r = r;
      e.fail_started = started;
      auto op = new_op({{CEPH_OSD_OP_WRITE, 4096, "fail",
			 delivery_t::FLAG_PULL, 0}});
      EXPECT_EQ(started ? -EIO : -EOPNOTSUPP,
		pull_payloads(env(x.get(), &e, &t), *op, ops_of(op)));
      EXPECT_EQ(1u, e.calls.size());
      EXPECT_EQ(started ? 0u : oob_result_t::FLAG_DECLINED,
		error_results(op)[0].flags);
    }
  }
}

TEST_F(OobPull, ChargedOnceOverTwoRuns)
{
  auto x = service(0);
  fake_puller e;
  Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
  auto op = new_op({{CEPH_OSD_OP_WRITE, 4096, "ok",
		     delivery_t::FLAG_PULL | delivery_t::FLAG_CRC64NVME, 0}});
  auto& ops = ops_of(op);
  const auto env1 = env(x.get(), &e, &t);
  ASSERT_EQ(0, pull_payloads(env1, *op, ops));
  EXPECT_EQ(1u, e.calls.size());
  EXPECT_EQ(4096, t.get_current());
  const auto pulled = op->oob_pulled[0];
  EXPECT_EQ(4096u, pulled.bytes);

  // The request waits and runs again, after its first run changed the
  // op: a write that a truncation overtook is cut short, its data too
  // (PrimaryLogPG::do_osd_ops(), CEPH_OSD_OP_WRITE). The op's data can
  // then no longer tell that the payload was pulled; the set can.
  ops[0].op.extent.length = 1000;
  {
    ceph::buffer::list cut;
    cut.substr_of(ops[0].indata, 0, 1000);
    ops[0].indata = std::move(cut);
  }
  ASSERT_EQ(0, pull_payloads(env1, *op, ops));
  EXPECT_EQ(1u, e.calls.size());
  EXPECT_EQ(4096, t.get_current());
  EXPECT_EQ(e.window.substr(0, 1000), ops[0].indata.to_str());
  expect_same(pulled, op->oob_pulled[0]);

  // and once more, unchanged
  ASSERT_EQ(0, pull_payloads(env1, *op, ops));
  EXPECT_EQ(1u, e.calls.size());
  EXPECT_EQ(4096, t.get_current());

  op.reset();
  EXPECT_EQ(0, t.get_current());
}

TEST_F(OobPull, InvalidSecondPullStartsNothing)
{
  auto x = service(0);
  const op_spec good = {CEPH_OSD_OP_WRITE, 4096, "ok",
			delivery_t::FLAG_PULL, 0};
  struct case_t {
    const char* what;
    op_spec second;
    int r;
  };
  const std::vector<case_t> cases = {
    {"a pull descriptor on a read",
     {CEPH_OSD_OP_READ, 4096, "ok", delivery_t::FLAG_PULL, 4096},
     -EINVAL},
    {"a flag a pull does not know",
     {CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL | (1u << 30),
      4096},
     -EOPNOTSUPP},
    {"a write that carries its data",
     {CEPH_OSD_OP_WRITE, 4, "ok", delivery_t::FLAG_PULL, 4096, "data"},
     -EINVAL},
    {"a token no transport pulls from",
     {CEPH_OSD_OP_WRITE, 4096, "none", delivery_t::FLAG_PULL, 4096},
     -EOPNOTSUPP},
  };
  for (const auto& c : cases) {
    SCOPED_TRACE(c.what);
    fake_puller e;
    Throttle t(cct.get(), "osd_client_bytes", 1 << 30, false);
    auto op = new_op({good, c.second});
    auto& ops = ops_of(op);
    EXPECT_EQ(c.r, pull_payloads(env(x.get(), &e, &t), *op, ops));
    // the first pull, which was fine, did not start either
    EXPECT_TRUE(e.calls.empty());
    EXPECT_EQ(0, t.get_current());
    EXPECT_FALSE(op->oob_pull);
    EXPECT_EQ(0u, ops[0].indata.length());
    // and nothing of the request read the window
    for (const auto& r : error_results(op)) {
      EXPECT_EQ(oob_result_t::FLAG_DECLINED, r.flags);
    }
  }
}

TEST_F(OobPull, ZeroLengthPullNeedsATransport)
{
  // A pulled WRITEFULL of no bytes has nothing to pull, but it writes:
  // it empties the object. An OSD that cannot pull from its token fails
  // it, as it fails a longer one, so that the object stays as it was
  auto x = service(0);
  fake_puller e;
  Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
  for (const uint16_t type : {CEPH_OSD_OP_WRITEFULL, CEPH_OSD_OP_WRITE}) {
    SCOPED_TRACE(type);
    auto op = new_op({{type, 0, "none", delivery_t::FLAG_PULL, 0}});
    EXPECT_EQ(-EOPNOTSUPP, pull_payloads(env(x.get(), &e, &t), *op,
					 ops_of(op)));
    EXPECT_FALSE(op->oob_pull);
    EXPECT_EQ(oob_result_t::FLAG_DECLINED, error_results(op)[0].flags);
    // one that can runs it, with nothing to pull
    auto ok = new_op({{type, 0, "ok", delivery_t::FLAG_PULL, 0}});
    EXPECT_EQ(0, pull_payloads(env(x.get(), &e, &t), *ok, ops_of(ok)));
    EXPECT_FALSE(ok->oob_pull);
    EXPECT_EQ(0u, ops_of(ok)[0].indata.length());
  }
  EXPECT_TRUE(e.calls.empty());
  EXPECT_EQ(0, t.get_current());
}

TEST_F(OobPull, WaitedPastTheLeaseStartsNothing)
{
  // A request that waited in the PG past the pool's lease after receipt
  // (a laggy or degraded PG, a scrub, the object's lock, a hold) pulls
  // nothing: its client may have given up on it, and reused its window,
  // once the lease and the drain passed. The request fails, and nothing
  // read the window: with -EOPNOTSUPP, on which a client that still
  // waits sends the payloads inline
  for (unsigned threads : {0u, 2u}) {
    SCOPED_TRACE(threads);
    auto x = service(threads);
    fake_puller e;
    Throttle t(cct.get(), "osd_client_bytes", 1 << 30, false);
    const op_spec spec = {CEPH_OSD_OP_WRITE, 4096, "ok",
			  delivery_t::FLAG_PULL | delivery_t::FLAG_CRC64NVME, 0};
    // lease 6 plus drain 4; past the lease alone too
    for (const utime_t age : {utime_t(10, 1000), utime_t(6, 1000)}) {
      SCOPED_TRACE(age);
      auto op = new_op({spec, spec}, ceph_clock_now() - age);
      EXPECT_EQ(-EOPNOTSUPP, pull_payloads(env(x.get(), &e, &t), *op,
					   ops_of(op)));
      EXPECT_TRUE(e.calls.empty());
      EXPECT_FALSE(op->oob_pull);
      EXPECT_EQ(0, t.get_current());
      for (const auto& r : error_results(op, -EOPNOTSUPP)) {
	EXPECT_EQ(oob_result_t::FLAG_DECLINED, r.flags);
      }
    }
    // a request with nothing to pull runs, whatever its age
    auto none = new_op({{CEPH_OSD_OP_WRITE, 0, "ok", delivery_t::FLAG_PULL,
			 0}}, ceph_clock_now() - utime_t(10, 0));
    EXPECT_EQ(0, pull_payloads(env(x.get(), &e, &t), *none, ops_of(none)));
  }
}

TEST_F(OobPull, BoundsCountFromReceipt)
{
  // A request that waited in the PG for less than the lease pulls, and
  // its reads must land by its receipt plus the lease plus the drain:
  // the executor gets what is left of that
  for (unsigned threads : {0u, 2u}) {
    SCOPED_TRACE(threads);
    auto x = service(threads);
    fake_puller e;
    Throttle t(cct.get(), "osd_client_bytes", 1 << 30, false);
    const op_spec spec = {CEPH_OSD_OP_WRITE, 4096, "ok",
			  delivery_t::FLAG_PULL | delivery_t::FLAG_CRC64NVME, 0};
    auto op = new_op({spec, spec}, ceph_clock_now() - utime_t(3, 0));
    ASSERT_EQ(0, run_request(env(x.get(), &e, &t), op));
    EXPECT_EQ(2u, e.calls.size());
    // lease 6 plus drain 4 less 3 s, less the moments the test took
    ASSERT_EQ(2u, e.budgets.size());
    EXPECT_GT(e.budgets[0], 6500ms);
    EXPECT_LE(e.budgets[0], 7s);
    EXPECT_EQ(8192, t.get_current());
    EXPECT_EQ(e.window.substr(0, 4096), ops_of(op)[0].indata.to_str());
    EXPECT_EQ(4096u, op->oob_pulled[1].bytes);
  }
  {
    SCOPED_TRACE("the lease ran out in the queue");
    // planned within the lease, which runs out while the job waits for
    // a transfer thread: it starts nothing
    // the service reads the delay as it starts (this context never
    // announces a change; see "the job could no longer start in time")
    cct->_conf.set_val_or_die("osd_debug_oob_delay_post_ms", "1000");
    auto x = service(1);
    cct->_conf.set_val_or_die("osd_debug_oob_delay_post_ms", "0");
    fake_puller e;
    Throttle t(cct.get(), "osd_client_bytes", 1 << 30, false);
    auto op = new_op({{CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL,
		       0}}, ceph_clock_now() - utime_t(5, 500000000));
    bool parked = false;
    EXPECT_EQ(-EOPNOTSUPP, run_request(env(x.get(), &e, &t), op, &parked));
    EXPECT_TRUE(parked);
    EXPECT_TRUE(e.calls.empty());
    EXPECT_EQ(oob_result_t::FLAG_DECLINED,
	      error_results(op, -EOPNOTSUPP)[0].flags);
  }
}

TEST_F(OobPull, NoRoomStartsNothing)
{
  // what the messenger has read holds some of the throttle: each payload
  // would fit in the rest, but not both. The request takes the room for
  // all of them, or none
  auto x = service(0);
  fake_puller e;
  Throttle t(cct.get(), "osd_client_bytes", 10000, false);
  auto held = new_op();
  ASSERT_TRUE(held->reserve_pulled(&t, 3000));
  auto op = new_op({{CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 0},
		    {CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL,
		     4096}});
  EXPECT_EQ(-EOPNOTSUPP, pull_payloads(env(x.get(), &e, &t), *op,
				       ops_of(op)));
  EXPECT_TRUE(e.calls.empty());
  EXPECT_EQ(3000, t.get_current());
  EXPECT_FALSE(op->oob_pull);
  for (const auto& r : error_results(op)) {
    EXPECT_EQ(oob_result_t::FLAG_DECLINED, r.flags);
  }

  // the payloads of one request are charged at once, as an inline
  // request's data is: a throttle at or under its limit takes more than
  // the limit then, as the messenger takes such a message
  Throttle small(cct.get(), "osd_client_bytes", 6000, false);
  auto small_held = new_op();
  ASSERT_TRUE(small_held->reserve_pulled(&small, 1000));
  auto big = new_op({{CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 0},
		     {CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL,
		      4096}});
  ASSERT_EQ(0, pull_payloads(env(x.get(), &e, &small), *big, ops_of(big)));
  EXPECT_EQ(2u, e.calls.size());
  EXPECT_EQ(1000 + 8192, small.get_current());
  big.reset();
  EXPECT_EQ(1000, small.get_current());
  small_held.reset();
  EXPECT_EQ(0, small.get_current());
}

TEST_F(OobPull, DeclinedOnlyWhenNothingStarted)
{
  auto x = service(0);
  Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
  const uint32_t declined = oob_result_t::FLAG_DECLINED;

  {
    SCOPED_TRACE("a pull failed after its read went out");
    fake_puller e;
    // the first pulled, the second failed, the third never started;
    // the read's descriptor started no push
    auto op = new_op(
      {{CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 0},
       {CEPH_OSD_OP_WRITE, 4096, "started", delivery_t::FLAG_PULL, 4096},
       {CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 8192},
       {CEPH_OSD_OP_READ, 4096, "ok", 0, 0}});
    EXPECT_EQ(-EIO, pull_payloads(env(x.get(), &e, &t), *op, ops_of(op)));
    const auto oob = error_results(op);
    EXPECT_EQ(0u, oob[0].flags);
    EXPECT_EQ(0u, oob[1].flags);
    EXPECT_EQ(declined, oob[2].flags);
    EXPECT_EQ(declined, oob[3].flags);
  }
  {
    SCOPED_TRACE("a pull failed before its read went out");
    fake_puller e;
    auto op = new_op(
      {{CEPH_OSD_OP_WRITE, 4096, "unstarted", delivery_t::FLAG_PULL, 0},
       {CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 4096}});
    EXPECT_EQ(-EOPNOTSUPP, pull_payloads(env(x.get(), &e, &t), *op,
					 ops_of(op)));
    for (const auto& r : error_results(op)) {
      EXPECT_EQ(declined, r.flags);
    }
  }
  {
    SCOPED_TRACE("the second pull could no longer land in time");
    // the first pull takes longer than the request has left: received
    // now, it must land within 1.5 s
    fake_puller e;
    e.delay = 1600ms;
    auto op = new_op(
      {{CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 0},
       {CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 4096}});
    auto env1 = env(x.get(), &e, &t);
    env1.lease = 1;
    env1.drain = 0.5;
    // the second started nothing: the client may send the payloads
    // inline, and only the first op is not declined
    EXPECT_EQ(-EOPNOTSUPP, pull_payloads(env1, *op, ops_of(op)));
    EXPECT_EQ(1u, e.calls.size());
    const auto oob = error_results(op);
    EXPECT_EQ(0u, oob[0].flags);
    EXPECT_EQ(declined, oob[1].flags);
  }
  {
    SCOPED_TRACE("the job could no longer start in time");
    fake_puller e;
    auto op = new_op(
      {{CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 0}});
    // a service that waits this long before it runs a job. It reads the
    // option as it starts, and again when apply_changes() says it
    // changed, which this context, whose cluster has no name, never says
    cct->_conf.set_val_or_die("osd_debug_oob_delay_post_ms", "1600");
    auto slow = service(0);
    cct->_conf.set_val_or_die("osd_debug_oob_delay_post_ms", "0");
    auto env1 = env(slow.get(), &e, &t);
    env1.lease = 1;
    env1.drain = 0.5;
    EXPECT_EQ(-EOPNOTSUPP, pull_payloads(env1, *op, ops_of(op)));
    EXPECT_TRUE(e.calls.empty());
    EXPECT_EQ(declined, error_results(op)[0].flags);
    EXPECT_EQ(1u, slow->get_logger()->get(l_oob_xfer_expired));
  }
  {
    SCOPED_TRACE("no transfer service");
    fake_puller e;
    auto op = new_op(
      {{CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 0}});
    EXPECT_EQ(-EOPNOTSUPP, pull_payloads(env(nullptr, &e, &t), *op,
					 ops_of(op)));
    EXPECT_TRUE(e.calls.empty());
    EXPECT_EQ(declined, error_results(op)[0].flags);
  }
  {
    SCOPED_TRACE("a stopped transfer service");
    auto stopped = service(0);
    stopped->stop();
    fake_puller e;
    auto op = new_op(
      {{CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 0}});
    EXPECT_EQ(-EOPNOTSUPP, pull_payloads(env(stopped.get(), &e, &t), *op,
					 ops_of(op)));
    EXPECT_TRUE(e.calls.empty());
    EXPECT_EQ(declined, error_results(op)[0].flags);
  }
  {
    SCOPED_TRACE("the write failed after its pull");
    fake_puller e;
    auto op = new_op(
      {{CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 0}});
    EXPECT_EQ(0, pull_payloads(env(x.get(), &e, &t), *op, ops_of(op)));
    EXPECT_EQ(0u, error_results(op)[0].flags);
  }
  {
    SCOPED_TRACE("a reply while the pull reads the window");
    // the set is in flight: the job has not told what started yet
    fake_puller e;
    auto op = new_op(
      {{CEPH_OSD_OP_WRITE, 4096, "unstarted", delivery_t::FLAG_PULL, 0},
       {CEPH_OSD_OP_READ, 4096, "ok", 0, 0}});
    std::vector<oob_result_t> oob;
    e.during = [&] { oob = error_results(op); };
    EXPECT_EQ(-EOPNOTSUPP, pull_payloads(env(x.get(), &e, &t), *op,
					 ops_of(op)));
    ASSERT_EQ(2u, oob.size());
    EXPECT_EQ(0u, oob[0].flags);
    EXPECT_EQ(declined, oob[1].flags);
    // once it settled, it has: nothing went out
    for (const auto& r : error_results(op)) {
      EXPECT_EQ(declined, r.flags);
    }
  }
}

TEST_F(OobPull, AJobThatNeverRunsStartsNothing)
{
  fake_puller e;
  const auto now = ceph::mono_clock::now();
  const bounds_t b{now + 10s, now + 10s};
  auto pulls = [&] {
    pull_op p;
    p.idx = 1;
    p.len = 4096;
    p.exec = &e;
    p.token = "ok";
    return std::vector<pull_op>{p};
  };
  for (int r : {-ETIMEDOUT, -ESHUTDOWN, -EBUSY, 0}) {
    SCOPED_TRACE(r);
    auto set = std::make_shared<pull_set>(std::vector<size_t>{1});
    {
      pull_job j(cct.get(), set, "obj", pulls(), b, true);
      EXPECT_TRUE(j.pulls());
      EXPECT_TRUE(j.inline_only());
      EXPECT_EQ(4096u, j.pinned_bytes());
      EXPECT_FALSE(set->settled());
      // until the job ends, it may yet start
      std::vector<oob_result_t> oob(2);
      oob[1].flags = oob_result_t::FLAG_DECLINED;
      set->clear_declined(oob);
      EXPECT_EQ(0u, oob[1].flags);
      if (r) {
	// expired in the queue, shed at stop, or refused
	j.expire(r);
      }
      // else dropped without running
    }
    ASSERT_TRUE(set->settled());
    std::vector<OSDOp> ops(2);
    std::vector<oob_result_t> pulled(2);
    // whatever kept it from running, the client may send the payloads
    // inline
    EXPECT_EQ(-EOPNOTSUPP, set->consume(ops, pulled, 64));
    EXPECT_EQ(0u, ops[1].indata.length());
    std::vector<oob_result_t> oob(2);
    oob[1].flags = oob_result_t::FLAG_DECLINED;
    set->clear_declined(oob);
    EXPECT_EQ(oob_result_t::FLAG_DECLINED, oob[1].flags);
  }
  EXPECT_TRUE(e.calls.empty());
}

TEST_F(OobPull, NothingToPull)
{
  auto x = service(0);
  fake_puller e;
  Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
  {
    // a descriptor vector that does not mirror the ops: the write fails
    // later, for its missing data
    auto op = new_op(
      {{CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 0}});
    auto& ops = ops_of(op);
    ops.push_back(OSDOp());
    EXPECT_EQ(0, pull_payloads(env(x.get(), &e, &t), *op, ops));
    EXPECT_FALSE(op->oob_pull);
  }
  {
    // descriptors, but none of them a pull
    auto op = new_op({{CEPH_OSD_OP_WRITE, 4, "", 0, 0, "data"},
		      {CEPH_OSD_OP_READ, 4096, "ok", 0, 0}});
    EXPECT_EQ(0, pull_payloads(env(x.get(), &e, &t), *op, ops_of(op)));
    EXPECT_FALSE(op->oob_pull);
    EXPECT_EQ(2u, op->oob_pulled.size());
  }
  EXPECT_TRUE(e.calls.empty());
  EXPECT_EQ(0, t.get_current());
}

TEST_F(OobPull, ParksWithTransferThreads)
{
  // the job runs on a transfer thread, the request waits for it holding
  // nothing, and the settle kicks the PG where it waits, once
  auto x = service(2);
  fake_puller e;
  e.delay = 200ms;
  Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
  auto op = new_op(
    {{CEPH_OSD_OP_WRITE, 4096, "ok",
      delivery_t::FLAG_PULL | delivery_t::FLAG_CRC64NVME, 0},
     {CEPH_OSD_OP_READ, 4096, "ok", 0, 0}});
  auto& ops = ops_of(op);
  const auto env1 = env(x.get(), &e, &t);
  ASSERT_EQ(PULLS_PARKED, pull_payloads(env1, *op, ops));
  ASSERT_TRUE(op->oob_pull);
  // charged at once, and nothing in the op until a run consumes the set
  EXPECT_EQ(4096, t.get_current());
  EXPECT_EQ(0u, ops[0].indata.length());
  // while the pull runs, a reply that fails the request declines none of
  // its pulled ops: any may be reading its window
  {
    const auto oob = error_results(op);
    EXPECT_EQ(0u, oob[0].flags);
    EXPECT_EQ(oob_result_t::FLAG_DECLINED, oob[1].flags);
  }

  ASSERT_TRUE(kicks.wait_for(1));
  ASSERT_TRUE(op->oob_pull->settled());
  {
    std::lock_guard l(kicks.m);
    EXPECT_TRUE(same_target(env1.target, kicks.kicked[0]));
    // from the transfer thread that settled the set
    ASSERT_EQ(1u, e.ran_on.size());
    EXPECT_EQ(e.ran_on[0], kicks.from[0]);
    EXPECT_NE(std::this_thread::get_id(), kicks.from[0]);
  }

  // the run the kick starts consumes the set; no later run pulls, parks
  // or is kicked
  ASSERT_EQ(0, pull_payloads(env1, *op, ops));
  EXPECT_EQ(e.window.substr(0, 4096), ops[0].indata.to_str());
  EXPECT_EQ(4096u, op->oob_pulled[0].bytes);
  EXPECT_TRUE(op->oob_pulled[0].flags & oob_result_t::FLAG_CRC64NVME);
  ASSERT_EQ(0, pull_payloads(env1, *op, ops));
  wait_until_idle(*x);
  EXPECT_EQ(1u, kicks.count());
  EXPECT_EQ(1u, e.calls.size());
  EXPECT_EQ(4096, t.get_current());
  op.reset();
  EXPECT_EQ(0, t.get_current());
}

TEST_F(OobPull, FailedPullParksToo)
{
  // a pull that fails on the transfer thread fails the request on the
  // run the kick starts, after the set settled; it reports what started
  auto x = service(2);
  Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
  for (const std::string token : {"started", "unstarted"}) {
    SCOPED_TRACE(token);
    fake_puller e;
    auto op = new_op({{CEPH_OSD_OP_WRITE, 4096, token,
		       delivery_t::FLAG_PULL, 0}});
    bool parked = false;
    EXPECT_EQ(token == "started" ? -EIO : -EOPNOTSUPP,
	      run_request(env(x.get(), &e, &t), op, &parked));
    EXPECT_TRUE(parked);
    EXPECT_EQ(token == "started" ? 0u : oob_result_t::FLAG_DECLINED,
	      error_results(op)[0].flags);
  }
  wait_until_idle(*x);
  EXPECT_EQ(2u, kicks.count());
}

TEST_F(OobPull, StaysOnThisThread)
{
  // a coroutine's request does not park, nor one whose executor's
  // transfers stay on the op threads: their pulls run here, and nothing
  // kicks
  auto x = service(2);
  Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
  for (int c = 0; c < 2; c++) {
    SCOPED_TRACE(c);
    fake_puller e;
    auto op = new_op({{CEPH_OSD_OP_WRITE, 4096, "ok",
		       delivery_t::FLAG_PULL, 0}});
    auto env1 = env(x.get(), &e, &t);
    if (c == 0) {
      env1.here = true;
    } else {
      env1.runs_inline = [&e](const OSDOobExecutor* ex) { return ex == &e; };
    }
    ASSERT_EQ(0, pull_payloads(env1, *op, ops_of(op)));
    ASSERT_EQ(1u, e.ran_on.size());
    EXPECT_EQ(std::this_thread::get_id(), e.ran_on[0]);
    EXPECT_TRUE(op->oob_pull->consumed());
    EXPECT_EQ(e.window.substr(0, 4096), ops_of(op)[0].indata.to_str());
  }
  EXPECT_EQ(0u, kicks.count());
}

TEST_F(OobPull, KicksOncePerSettledSet)
{
  pull_result ok;
  ok.idx = 0;
  ok.r = 0;
  ok.started = true;
  ok.payload.append("data");
  const auto t1 = test_target(1);
  const auto t2 = test_target(2);
  auto a = new_op();
  {
    SCOPED_TRACE("parked, then settled");
    pull_set set({0});
    EXPECT_TRUE(set.park(a.get(), t1));
    auto k = set.settle({ok});
    ASSERT_EQ(1u, k.size());
    EXPECT_TRUE(same_target(t1, k[0]));
    // a run after the settle goes on
    EXPECT_FALSE(set.park(a.get(), t2));
  }
  {
    SCOPED_TRACE("parked again after an interval change");
    // the kick goes where the request waits last, once
    pull_set set({0});
    EXPECT_TRUE(set.park(a.get(), t1));
    EXPECT_TRUE(set.park(a.get(), t2));
    auto k = set.settle({ok});
    ASSERT_EQ(1u, k.size());
    EXPECT_TRUE(same_target(t2, k[0]));
  }
  {
    SCOPED_TRACE("settled before the park");
    // the thread that would have parked the request consumes the set,
    // and no kick comes
    pull_set set({0});
    EXPECT_TRUE(set.settle({ok}).empty());
    EXPECT_FALSE(set.park(a.get(), t1));
    std::vector<OSDOp> ops(1);
    std::vector<oob_result_t> pulled(1);
    EXPECT_EQ(0, set.consume(ops, pulled, 64));
    EXPECT_EQ("data", ops[0].indata.to_str());
    EXPECT_TRUE(set.consumed());
  }
  {
    SCOPED_TRACE("abandoned");
    // the request went away: nothing to kick
    pull_set set({0});
    EXPECT_TRUE(set.park(a.get(), t1));
    EXPECT_TRUE(set.abandon(a.get()));
    EXPECT_TRUE(set.abandoned());
    EXPECT_TRUE(set.settle({ok}).empty());
    // once settled, it says so
    EXPECT_FALSE(set.abandon(a.get()));
  }
}

// A later attempt of a request can wait for the pulls of an earlier one
// in another PG of the OSD (pulls_out), while the earlier one waits for
// them too, or after it went away. Each attempt that parks keeps its own
// place: what one attempt does never takes the kick of another
TEST_F(OobPull, KicksEveryAttemptThatWaits)
{
  pull_result ok;
  ok.idx = 0;
  ok.r = 0;
  ok.started = true;
  const auto t1 = test_target(1);
  const auto t2 = test_target(2);
  const auto t3 = test_target(3);
  auto a = new_op();
  auto b = new_op();
  auto kicked = [](const std::vector<kick_target>& k,
		   const kick_target& t) {
    return std::count_if(k.begin(), k.end(), [&t](const auto& x) {
      return same_target(t, x);
    });
  };
  {
    SCOPED_TRACE("both wait");
    pull_set set({0});
    EXPECT_TRUE(set.park(a.get(), t1));
    EXPECT_TRUE(set.park(b.get(), t2));
    const auto k = set.settle({ok});
    ASSERT_EQ(2u, k.size());
    EXPECT_EQ(1, kicked(k, t1));
    EXPECT_EQ(1, kicked(k, t2));
  }
  {
    SCOPED_TRACE("the earlier attempt went away after the later parked");
    pull_set set({0});
    EXPECT_TRUE(set.park(a.get(), t1));
    EXPECT_TRUE(set.park(b.get(), t2));
    EXPECT_TRUE(set.abandon(a.get()));
    const auto k = set.settle({ok});
    ASSERT_EQ(1u, k.size());
    EXPECT_TRUE(same_target(t2, k[0]));
  }
  {
    SCOPED_TRACE("the earlier attempt parked again after the later");
    pull_set set({0});
    EXPECT_TRUE(set.park(a.get(), t1));
    EXPECT_TRUE(set.park(b.get(), t2));
    EXPECT_TRUE(set.park(a.get(), t3));
    const auto k = set.settle({ok});
    ASSERT_EQ(2u, k.size());
    EXPECT_EQ(0, kicked(k, t1));
    EXPECT_EQ(1, kicked(k, t2));
    EXPECT_EQ(1, kicked(k, t3));
  }
}

TEST_F(OobPull, SettleRacesPark)
{
  // a set settles on its transfer thread while an op thread parks the
  // request: either the park comes first, and exactly one kick comes, or
  // the settle does, and the op thread goes on with the set
  const auto t1 = test_target();
  auto a = new_op();
  unsigned parked = 0, consumed = 0;
  for (int i = 0; i < 20000; i++) {
    pull_set set({0});
    std::atomic<int> ready{0};
    std::vector<kick_target> k;
    std::thread settler([&] {
      pull_result ok;
      ok.idx = 0;
      ok.r = 0;
      ok.payload.append("data");
      ready++;
      while (ready.load() < 2) {}
      k = set.settle({ok});
    });
    ready++;
    while (ready.load() < 2) {}
    const bool waits = set.park(a.get(), t1);
    settler.join();
    ASSERT_TRUE(set.settled());
    if (waits) {
      ASSERT_EQ(1u, k.size());
      EXPECT_TRUE(same_target(t1, k[0]));
      parked++;
    } else {
      ASSERT_TRUE(k.empty());
      std::vector<OSDOp> ops(1);
      std::vector<oob_result_t> pulled(1);
      ASSERT_EQ(0, set.consume(ops, pulled, 64));
      consumed++;
    }
  }
  std::cout << "parked first " << parked << ", settled first " << consumed
	    << std::endl;
}

TEST_F(OobPull, SettledBeforeParkIsConsumed)
{
  // A job the service refuses settles before the request can park: the
  // thread that pulls consumes the set, the request fails as a refused
  // pull always did, and nothing kicks
  auto x = std::make_unique<Transfers>(cct.get(), 1, 1, 1 << 30, 30s,
				       [] {});
  x->start();
  fake_puller e;
  std::mutex gate_m;
  std::condition_variable gate_cond;
  bool open = false;
  e.during = [&] {
    std::unique_lock l(gate_m);
    gate_cond.wait(l, [&] { return open; });
  };
  Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
  // the one job the service takes, held
  auto held = new_op({{CEPH_OSD_OP_WRITE, 4096, "ok",
		       delivery_t::FLAG_PULL, 0}});
  ASSERT_EQ(PULLS_PARKED, pull_payloads(env(x.get(), &e, &t), *held,
					ops_of(held)));
  auto op = new_op({{CEPH_OSD_OP_WRITE, 4096, "ok",
		     delivery_t::FLAG_PULL, 4096}});
  EXPECT_EQ(-EOPNOTSUPP, pull_payloads(env(x.get(), &e, &t), *op,
				       ops_of(op)));
  EXPECT_TRUE(op->oob_pull->settled());
  EXPECT_EQ(oob_result_t::FLAG_DECLINED, error_results(op)[0].flags);
  EXPECT_EQ(1u, x->get_logger()->get(l_oob_xfer_refused));
  {
    std::lock_guard l(gate_m);
    open = true;
  }
  gate_cond.notify_all();
  ASSERT_TRUE(kicks.wait_for(1));
  wait_until_idle(*x);
  // only the held request's set kicked
  EXPECT_EQ(1u, kicks.count());
  EXPECT_EQ(0, pull_payloads(env(x.get(), &e, &t), *held, ops_of(held)));

  // And a set that a transfer thread settles before the op thread parks
  // the request, which a fast pull can do: each request either waits
  // and is kicked once, or goes on with its payloads at once
  auto y = service(4);
  unsigned waited = 0, went_on = 0;
  for (int i = 0; i < 2000; i++) {
    fake_puller f;
    auto r = new_op({{CEPH_OSD_OP_WRITE, 512, "ok",
		      delivery_t::FLAG_PULL, 0}});
    const size_t before = kicks.count();
    const int ret = pull_payloads(env(y.get(), &f, &t), *r, ops_of(r));
    if (ret == PULLS_PARKED) {
      ASSERT_TRUE(kicks.wait_for(before + 1));
      ASSERT_EQ(0, pull_payloads(env(y.get(), &f, &t), *r, ops_of(r)));
      waited++;
    } else {
      ASSERT_EQ(0, ret);
      ASSERT_TRUE(r->oob_pull->consumed());
      went_on++;
    }
    EXPECT_EQ(f.window.substr(0, 512), ops_of(r)[0].indata.to_str());
  }
  wait_until_idle(*y);
  EXPECT_EQ(1u + waited, kicks.count());
  std::cout << "waited " << waited << ", went on " << went_on << std::endl;
}

TEST_F(OobPull, AbandonedSetStartsNothing)
{
  // the OSD dropped the request while its job waited for a thread, and
  // the request went away with its last reference: the job posts
  // nothing, and kicks nothing
  auto x = std::make_unique<Transfers>(cct.get(), 1, 16, 1 << 30, 30s,
				       [] {});
  x->start();
  fake_puller e;
  std::mutex gate_m;
  std::condition_variable gate_cond;
  bool open = false;
  e.during = [&] {
    std::unique_lock l(gate_m);
    gate_cond.wait(l, [&] { return open; });
  };
  Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
  auto held = new_op({{CEPH_OSD_OP_WRITE, 4096, "ok",
		       delivery_t::FLAG_PULL, 0}});
  ASSERT_EQ(PULLS_PARKED, pull_payloads(env(x.get(), &e, &t), *held,
					ops_of(held)));
  auto op = new_op({{CEPH_OSD_OP_WRITE, 4096, "ok",
		     delivery_t::FLAG_PULL, 4096},
		    {CEPH_OSD_OP_WRITE, 4096, "ok",
		     delivery_t::FLAG_PULL, 8192}});
  ASSERT_EQ(PULLS_PARKED, pull_payloads(env(x.get(), &e, &t), *op,
					ops_of(op)));
  const auto set = op->oob_pull;
  EXPECT_FALSE(set->abandoned());
  // the bytes go back with the request
  EXPECT_EQ(3 * 4096, t.get_current());
  op.reset();
  EXPECT_TRUE(set->abandoned());
  EXPECT_EQ(4096, t.get_current());
  {
    std::lock_guard l(gate_m);
    open = true;
  }
  gate_cond.notify_all();
  wait_until_idle(*x);
  ASSERT_TRUE(set->settled());
  // only the held request pulled, and only it was kicked
  EXPECT_EQ(1u, e.calls.size());
  EXPECT_EQ(1u, kicks.count());
  std::vector<OSDOp> ops(2);
  std::vector<oob_result_t> pulled(2);
  EXPECT_EQ(-ESHUTDOWN, set->consume(ops, pulled, 64));
  // a request that is answered goes away too, and abandons nothing
  EXPECT_EQ(0, pull_payloads(env(x.get(), &e, &t), *held, ops_of(held)));
  const auto done = held->oob_pull;
  held.reset();
  EXPECT_TRUE(done->consumed());
  EXPECT_EQ(0, t.get_current());
}

// The request that owns a set went away while its pull ran (a split, a
// reset session), and a later attempt of it waits for the set in its
// PG. Nothing takes the payloads any more: they go at the settle, or at
// once when the set settled already, not with the last attempt that
// waits for the set, which no throttle counts them against
TEST_F(OobPull, AbandonedSetFreesItsPayloads)
{
  for (const bool settled_first : {false, true}) {
    SCOPED_TRACE(settled_first ? "settled, then abandoned" :
		 "abandoned, then settled");
    auto owner = new_op();
    auto later = new_op();
    auto set = std::make_shared<pull_set>(std::vector<size_t>{0});
    owner->oob_pull = set;
    const auto t2 = test_target(2);
    EXPECT_TRUE(set->park(later.get(), t2));
    ceph::buffer::ptr payload = ceph::buffer::create(4096);
    auto results = [&] {
      std::vector<pull_result> r(1);
      r[0].idx = 0;
      r[0].r = 0;
      r[0].started = true;
      r[0].payload.append(payload);
      return r;
    };
    std::vector<kick_target> k;
    if (settled_first) {
      k = set->settle(results());
      // the owner's to take, until it goes away
      EXPECT_EQ(2, payload.raw_nref());
      owner.reset();
    } else {
      owner.reset();
      k = set->settle(results());
    }
    EXPECT_TRUE(set->abandoned());
    EXPECT_EQ(1, payload.raw_nref());
    // the later attempt still gets its kick
    ASSERT_EQ(1u, k.size());
    EXPECT_TRUE(same_target(t2, k[0]));
    // and nothing could take what is gone
    std::vector<OSDOp> ops(1);
    std::vector<oob_result_t> pulled(1);
    EXPECT_EQ(-ESHUTDOWN, set->consume(ops, pulled, 64));
    EXPECT_EQ(0u, ops[0].indata.length());
    // its pull started: a reply would not decline it
    std::vector<oob_result_t> oob(1);
    oob[0].flags = oob_result_t::FLAG_DECLINED;
    set->clear_declined(oob);
    EXPECT_EQ(0u, oob[0].flags);
  }
}

TEST_F(OobPull, LaterAttemptWaitsForEarlierPulls)
{
  // The pulls of a request that run on a transfer thread are out, for
  // every PG of the OSD, until they settled, whatever the PG did with
  // the request meanwhile. A later attempt of it finds them, waits for
  // them where it is, and pulls the window itself once they settled
  auto x = service(1);
  fake_puller e;
  std::mutex gate_m;
  std::condition_variable gate_cond;
  bool entered = false, open = false;
  e.during = [&] {
    std::unique_lock l(gate_m);
    entered = true;
    gate_cond.notify_all();
    gate_cond.wait(l, [&] { return open; });
  };
  Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
  pulls_out out;
  auto env1 = env(x.get(), &e, &t);
  env1.out = &out;
  const std::vector<op_spec> specs = {
    {CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 0}};
  auto first = new_op(specs, ceph_clock_now(), 7);
  ASSERT_EQ(PULLS_PARKED, pull_payloads(env1, *first, ops_of(first)));
  EXPECT_EQ(1u, out.size());
  {
    // the PG drops the first attempt while its pull reads the window
    std::unique_lock l(gate_m);
    ASSERT_TRUE(gate_cond.wait_for(l, 10s, [&] { return entered; }));
  }
  const auto first_set = first->oob_pull;
  first.reset();
  EXPECT_TRUE(first_set->abandoned());

  auto second = new_op(specs, ceph_clock_now(), 7);
  EXPECT_FALSE(out.find(new_op(specs, ceph_clock_now(), 8)->get_reqid()));
  auto earlier = out.find(second->get_reqid());
  ASSERT_EQ(first_set.get(), earlier.get());
  // the second waits for it at its own PG: the settle kicks there, once
  const auto t2 = test_target(2);
  EXPECT_TRUE(earlier->park(second.get(), t2));
  {
    std::lock_guard l(gate_m);
    open = true;
  }
  gate_cond.notify_all();
  ASSERT_TRUE(kicks.wait_for(1));
  {
    std::lock_guard l(kicks.m);
    EXPECT_TRUE(same_target(t2, kicks.kicked[0]));
  }
  wait_until_idle(*x);
  EXPECT_FALSE(out.find(second->get_reqid()));
  EXPECT_EQ(0u, out.size());

  // then it pulls the window itself, out while it runs
  e.during = [&] {
    EXPECT_EQ(second->oob_pull.get(), out.find(second->get_reqid()).get());
  };
  bool parked = false;
  EXPECT_EQ(0, run_request(env1, second, &parked));
  EXPECT_TRUE(parked);
  EXPECT_EQ(2u, e.calls.size());
  EXPECT_EQ(e.window.substr(0, 4096), ops_of(second)[0].indata.to_str());
  wait_until_idle(*x);
  EXPECT_EQ(0u, out.size());
  EXPECT_EQ(2u, kicks.count());
}

TEST_F(OobPull, LaterAttemptKickedWhateverTheEarlierDoes)
{
  // A later attempt waits for the pulls of an earlier one, which waits
  // for them too. The earlier one then parks again (an interval change
  // requeued it), or goes away (its PG dropped it): the later one still
  // gets its kick, in its own PG
  for (const bool drop : {false, true}) {
    SCOPED_TRACE(drop ? "dropped" : "parked again");
    auto x = service(1);
    fake_puller e;
    std::mutex gate_m;
    std::condition_variable gate_cond;
    bool entered = false, open = false;
    e.during = [&] {
      std::unique_lock l(gate_m);
      entered = true;
      gate_cond.notify_all();
      gate_cond.wait(l, [&] { return open; });
    };
    Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
    pulls_out out;
    auto env1 = env(x.get(), &e, &t);
    env1.out = &out;
    const std::vector<op_spec> specs = {
      {CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 0}};
    auto first = new_op(specs, ceph_clock_now(), 7);
    ASSERT_EQ(PULLS_PARKED, pull_payloads(env1, *first, ops_of(first)));
    {
      std::unique_lock l(gate_m);
      ASSERT_TRUE(gate_cond.wait_for(l, 10s, [&] { return entered; }));
    }
    auto second = new_op(specs, ceph_clock_now(), 7);
    const auto t2 = test_target(2);
    auto earlier = out.find(second->get_reqid());
    ASSERT_TRUE(earlier);
    EXPECT_TRUE(earlier->park(second.get(), t2));
    const auto t3 = test_target(3);
    if (drop) {
      first.reset();
    } else {
      EXPECT_TRUE(first->oob_pull->park(first.get(), t3));
    }
    {
      std::lock_guard l(gate_m);
      open = true;
    }
    gate_cond.notify_all();
    ASSERT_TRUE(kicks.wait_for(drop ? 1 : 2));
    wait_until_idle(*x);
    std::lock_guard l(kicks.m);
    ASSERT_EQ(drop ? 1u : 2u, kicks.kicked.size());
    EXPECT_EQ(1, std::count_if(kicks.kicked.begin(), kicks.kicked.end(),
			       [&](const auto& k) {
				 return same_target(t2, k);
			       }));
    if (!drop) {
      EXPECT_EQ(1, std::count_if(kicks.kicked.begin(), kicks.kicked.end(),
				 [&](const auto& k) {
				   return same_target(t3, k);
				 }));
    }
    kicks.kicked.clear();
    kicks.from.clear();
  }
}

// A later attempt that waits for the pulls of an earlier one leaves
// them when it goes away (its PG dropped it, a backoff, a discard), or
// when it waits for another set: the settle kicks nothing for it, and
// keeps no place of an attempt that is gone
TEST_F(OobPull, LaterAttemptLeavesWhatItWaitedFor)
{
  auto ok = [] {
    std::vector<pull_result> r(1);
    r[0].idx = 0;
    r[0].r = 0;
    r[0].started = true;
    return r;
  };
  const auto t1 = test_target(1);
  const auto t2 = test_target(2);
  const auto t3 = test_target(3);
  {
    SCOPED_TRACE("gone");
    auto first = new_op();
    auto set = std::make_shared<pull_set>(std::vector<size_t>{0});
    first->oob_pull = set;
    EXPECT_TRUE(set->park(first.get(), t1));
    auto later = new_op();
    ASSERT_TRUE(wait_for_earlier(*later, set, t2));
    EXPECT_EQ(set, later->oob_pull_waits.lock());
    later.reset();
    const auto k = set->settle(ok());
    ASSERT_EQ(1u, k.size());
    EXPECT_TRUE(same_target(t1, k[0]));
  }
  {
    SCOPED_TRACE("waits for another set");
    // an interval change requeued it, and it found another set of its
    // request out
    auto a = std::make_shared<pull_set>(std::vector<size_t>{0});
    auto b = std::make_shared<pull_set>(std::vector<size_t>{0});
    auto later = new_op();
    ASSERT_TRUE(wait_for_earlier(*later, a, t2));
    ASSERT_TRUE(wait_for_earlier(*later, b, t3));
    EXPECT_EQ(b, later->oob_pull_waits.lock());
    EXPECT_TRUE(a->settle(ok()).empty());
    const auto k = b->settle(ok());
    ASSERT_EQ(1u, k.size());
    EXPECT_TRUE(same_target(t3, k[0]));
  }
  {
    SCOPED_TRACE("goes on");
    // requeued, it finds a set that settled already, and waits for
    // nothing: it left the one it waited for
    auto a = std::make_shared<pull_set>(std::vector<size_t>{0});
    auto b = std::make_shared<pull_set>(std::vector<size_t>{0});
    auto later = new_op();
    ASSERT_TRUE(wait_for_earlier(*later, a, t2));
    EXPECT_TRUE(b->settle(ok()).empty());
    EXPECT_FALSE(wait_for_earlier(*later, b, t3));
    EXPECT_FALSE(later->oob_pull_waits.lock());
    EXPECT_TRUE(a->settle(ok()).empty());
  }
  {
    SCOPED_TRACE("waits there again");
    // requeued, it waits for the same set at its PG once more: the
    // settle kicks it there, once
    auto set = std::make_shared<pull_set>(std::vector<size_t>{0});
    auto later = new_op();
    ASSERT_TRUE(wait_for_earlier(*later, set, t2));
    ASSERT_TRUE(wait_for_earlier(*later, set, t3));
    const auto k = set->settle(ok());
    ASSERT_EQ(1u, k.size());
    EXPECT_TRUE(same_target(t3, k[0]));
  }
}

TEST_F(OobPull, OnlyPullsOnTransferThreadsAreOut)
{
  // Pulls that run on the thread that starts them hold its PG until they
  // settled: no attempt of their request can find them out
  Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
  pulls_out out;
  for (unsigned threads : {0u, 2u}) {
    SCOPED_TRACE(threads);
    auto x = service(threads);
    fake_puller e;
    size_t seen = 99;
    e.during = [&] { seen = out.size(); };
    auto env1 = env(x.get(), &e, &t);
    env1.out = &out;
    // with threads, a coroutine's request
    env1.here = threads > 0;
    auto op = new_op({{CEPH_OSD_OP_WRITE, 4096, "ok",
		       delivery_t::FLAG_PULL, 0}});
    ASSERT_EQ(0, pull_payloads(env1, *op, ops_of(op)));
    EXPECT_EQ(0u, seen);
    EXPECT_EQ(0u, out.size());
  }

  // a job the service refuses is out until it settled, before the pull
  // step returns
  auto x = std::make_unique<Transfers>(cct.get(), 1, 1, 1 << 30, 30s,
				       [] {});
  x->start();
  fake_puller e;
  std::mutex gate_m;
  std::condition_variable gate_cond;
  bool open = false;
  e.during = [&] {
    std::unique_lock l(gate_m);
    gate_cond.wait(l, [&] { return open; });
  };
  auto env1 = env(x.get(), &e, &t);
  env1.out = &out;
  auto held = new_op({{CEPH_OSD_OP_WRITE, 4096, "ok",
		       delivery_t::FLAG_PULL, 0}}, ceph_clock_now(), 1);
  ASSERT_EQ(PULLS_PARKED, pull_payloads(env1, *held, ops_of(held)));
  auto op = new_op({{CEPH_OSD_OP_WRITE, 4096, "ok",
		     delivery_t::FLAG_PULL, 4096}}, ceph_clock_now(), 2);
  EXPECT_EQ(-EOPNOTSUPP, pull_payloads(env1, *op, ops_of(op)));
  EXPECT_FALSE(out.find(op->get_reqid()));
  EXPECT_EQ(held->oob_pull.get(), out.find(held->get_reqid()).get());
  EXPECT_EQ(1u, out.size());
  {
    std::lock_guard l(gate_m);
    open = true;
  }
  gate_cond.notify_all();
  ASSERT_TRUE(kicks.wait_for(1));
  wait_until_idle(*x);
  EXPECT_EQ(0u, out.size());
}

// The sets out are spread over shards by their request: each request
// finds its own, two attempts of one request each keep theirs, and the
// count covers every shard
TEST_F(OobPull, PullsOutBySet)
{
  pulls_out out;
  std::vector<OpRequestRef> ops;
  std::vector<std::shared_ptr<pull_set>> sets;
  for (ceph_tid_t tid = 1; tid <= 100; tid++) {
    ops.push_back(new_op({}, ceph_clock_now(), tid));
    sets.push_back(std::make_shared<pull_set>(std::vector<size_t>{0}));
    out.add(ops.back()->get_reqid(), sets.back());
  }
  // a second attempt of the first request
  auto again = std::make_shared<pull_set>(std::vector<size_t>{0});
  out.add(ops[0]->get_reqid(), again);
  EXPECT_EQ(101u, out.size());
  for (size_t i = 0; i < ops.size(); i++) {
    EXPECT_EQ(sets[i], out.find(ops[i]->get_reqid()));
  }
  EXPECT_FALSE(out.find(new_op({}, ceph_clock_now(), 101)->get_reqid()));
  out.remove(ops[0]->get_reqid(), sets[0].get());
  EXPECT_EQ(again, out.find(ops[0]->get_reqid()));
  out.remove(ops[0]->get_reqid(), again.get());
  EXPECT_FALSE(out.find(ops[0]->get_reqid()));
  for (size_t i = 1; i < ops.size(); i++) {
    // a set it did not add stays
    out.remove(ops[i]->get_reqid(), again.get());
    out.remove(ops[i]->get_reqid(), sets[i].get());
  }
  EXPECT_EQ(0u, out.size());
}

// The record of a pull, which the op keeps in its outdata for the PG
// log: its byte count, flags and checksums, and a range at the
// descriptor's offset when the result has one
TEST_F(OobPull, PullRecord)
{
  delivery_t d;
  d.token = "ok";
  d.base_offset = 8192;
  d.flags = delivery_t::FLAG_PULL | delivery_t::FLAG_CRC64NVME |
    delivery_t::FLAG_CRC32C;
  oob_result_t r;
  r.bytes = 5000;
  r.crc64 = 0x1122334455667788ull;
  r.crc32c = 0x99aabbcc;
  r.flags = oob_result_t::FLAG_COMBINABLE | oob_result_t::FLAG_RANGES |
    oob_result_t::FLAG_CRC64NVME | oob_result_t::FLAG_CRC32C;
  r.ranges.push_back({8192, 5000, r.crc64, r.crc32c});
  ceph::buffer::list bl;
  encode_pull_record(r, bl);
  // within osd_max_write_op_reply_len, at its default
  EXPECT_EQ(30u, bl.length());
  oob_result_t got;
  ASSERT_TRUE(decode_pull_record(bl, d, &got));
  expect_same(r, got);

  // no checksum asked for: the byte count, and no range
  oob_result_t plain;
  plain.bytes = 100;
  bl.clear();
  encode_pull_record(plain, bl);
  ASSERT_TRUE(decode_pull_record(bl, d, &got));
  expect_same(plain, got);

  // anything else is no record of a pull
  auto rejects = [&](const ceph::buffer::list& b) {
    oob_result_t untouched;
    untouched.bytes = 7;
    EXPECT_FALSE(decode_pull_record(b, d, &untouched));
    EXPECT_EQ(7u, untouched.bytes);
  };
  rejects(ceph::buffer::list());
  ceph::buffer::list cut;
  cut.substr_of(bl, 0, bl.length() - 1);
  rejects(cut);
  ceph::buffer::list longer = bl;
  longer.append('x');
  rejects(longer);
  for (const uint32_t flags : {oob_result_t::FLAG_DECLINED,
			       oob_result_t::FLAG_LANDED,
			       oob_result_t::FLAG_RESENT}) {
    oob_result_t odd = plain;
    odd.flags = flags;
    ceph::buffer::list b;
    encode_pull_record(odd, b);
    rejects(b);
  }
  oob_result_t none;
  ceph::buffer::list b;
  encode_pull_record(none, b);
  rejects(b);

  // the op keeps it only when it fits in osd_max_write_op_reply_len:
  // one that did not would fail the request
  auto x = service(0);
  fake_puller e;
  Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
  for (const uint64_t max_reply : {64u, 30u, 29u, 0u}) {
    SCOPED_TRACE(max_reply);
    auto op = new_op({{CEPH_OSD_OP_WRITE, 4096, "ok",
		       delivery_t::FLAG_PULL | delivery_t::FLAG_CRC64NVME, 0}});
    auto env1 = env(x.get(), &e, &t);
    env1.max_reply = max_reply;
    ASSERT_EQ(0, pull_payloads(env1, *op, ops_of(op)));
    EXPECT_EQ(4096u, op->oob_pulled[0].bytes);
    const auto& kept = ops_of(op)[0].outdata;
    EXPECT_EQ(max_reply >= 30 ? 30u : 0u, kept.length());
    if (kept.length() > 0) {
      // in a buffer of its own length, counted with the PG log, which
      // keeps it, and the dups trimmed from it, as long as they last
      ASSERT_EQ(1u, kept.get_num_buffers());
      EXPECT_EQ(kept.length(), kept.front().raw_length());
      EXPECT_EQ(mempool::mempool_osd_pglog, kept.front().get_mempool());
    }
  }
}

// A resend of a pulled write that the PG answers as a dup, the write
// being done, reports what the pulls of the attempt that did it
// reported, as the PG log kept them with the write (RETURNVEC, which the
// client library sets on a request that pulls when no other op of it
// returns anything, MOSDOp::wants_pull_records()), and not declined,
// with no bytes: the gateway would take that for a failed OSD-direct PUT
TEST_F(OobPull, DupReportsThePull)
{
  auto x = service(0);
  fake_puller e;
  Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
  const std::vector<op_spec> specs = {
    {CEPH_OSD_OP_CREATE, 0, ""},
    {CEPH_OSD_OP_WRITEFULL, 4096, "ok",
     delivery_t::FLAG_PULL | delivery_t::FLAG_CRC64NVME, 0},
    {CEPH_OSD_OP_WRITE, 5000, "ok",
     delivery_t::FLAG_PULL | delivery_t::FLAG_CRC64NVME |
     delivery_t::FLAG_CRC32C, 8192},
    {CEPH_OSD_OP_WRITE, 100, "ok", delivery_t::FLAG_PULL, 20000},
    {CEPH_OSD_OP_READ, 4096, "ok", 0, 0},
  };
  auto first = new_op(specs, ceph_clock_now(), 7);
  ASSERT_EQ(0, pull_payloads(env(x.get(), &e, &t), *first, ops_of(first)));

  // what the PG log keeps of the write, through its encoding
  pg_log_entry_t entry;
  entry.set_op_returns(ops_of(first));
  {
    ceph::buffer::list bl;
    entry.encode_with_checksum(bl);
    auto p = bl.cbegin();
    entry = pg_log_entry_t();
    entry.decode_with_checksum(p);
  }
  ASSERT_EQ(specs.size(), entry.op_returns.size());
  for (size_t i = 0; i < specs.size(); i++) {
    SCOPED_TRACE(i);
    // a pulled op's record, and nothing for the others
    EXPECT_EQ(i >= 1 && i <= 3, entry.op_returns[i].bl.length() > 0);
  }

  // the resend never ran: OSDService::reply_op_error() takes the
  // records of the attempt that did the write
  auto resend = new_op(specs, ceph_clock_now(), 7);
  auto oob = error_results(resend, 0, true, entry.op_returns);
  ASSERT_EQ(specs.size(), oob.size());
  expect_same(oob_result_t{}, oob[0]);
  for (size_t i = 1; i <= 3; i++) {
    SCOPED_TRACE(i);
    expect_same(first->oob_pulled[i], oob[i]);
  }
  // and declines the read's descriptor: the attempt that did the write
  // pushed nothing, as no logged write reaches complete_read_ctx(), and
  // the resend runs nothing
  EXPECT_EQ(oob_result_t::FLAG_DECLINED, oob[4].flags);
  EXPECT_EQ(0u, oob[4].bytes);

  // the attempt that did it failed after its pulls: as its reply said,
  // the pulled ops are not declined, and report no bytes, and the read's
  // descriptor started nothing
  oob = error_results(resend, -ENOSPC, true, entry.op_returns);
  for (size_t i = 1; i <= 3; i++) {
    SCOPED_TRACE(i);
    expect_same(oob_result_t{}, oob[i]);
  }
  EXPECT_EQ(oob_result_t::FLAG_DECLINED, oob[4].flags);

  // No record kept (one larger than osd_max_write_op_reply_len), or not
  // for these ops. The attempt that did the write pulled, before the
  // write completed or failed: no pulled op is declined, and none
  // reports bytes, so that the client fences its window. The read's
  // descriptor is declined, whatever the result
  const auto& deliveries = resend->get_req<MOSDOp>()->get_rdma_deliveries();
  for (const auto& kept : {std::vector<pg_log_op_return_item_t>{},
			   std::vector<pg_log_op_return_item_t>(2)}) {
    for (const int r : {0, -ENOSPC}) {
      SCOPED_TRACE(r);
      oob = error_results(resend, r, true, kept);
      for (size_t i = 1; i <= 3; i++) {
	SCOPED_TRACE(i);
	expect_same(oob_result_t{}, oob[i]);
      }
      EXPECT_EQ(oob_result_t::FLAG_DECLINED, oob[4].flags);
      // an OSD built without a transport, which keeps no records,
      // answers the dup by the same rule
      std::vector<oob_result_t> no_transport(deliveries.size());
      reply_error_results(deliveries, r, true, no_transport);
      for (size_t i = 0; i < specs.size(); i++) {
	SCOPED_TRACE(i);
	expect_same(oob[i], no_transport[i]);
      }
    }
  }

  // and an error reply that is no dup, which no attempt of the request
  // was answered by before, declines what nothing of it read
  oob = error_results(resend, -ENOSPC, false, entry.op_returns);
  for (size_t i = 1; i < specs.size(); i++) {
    SCOPED_TRACE(i);
    EXPECT_EQ(oob_result_t::FLAG_DECLINED, oob[i].flags);
  }
}

// A request without RETURNVEC, as one with a CALL that returns data,
// keeps no return data in the PG log, but the records of its pulls
// (keep_pull_records()): nothing of what its other ops returned, which
// no reply to it carries, and the reply to a resend that the PG answers
// as a dup reports its pulls from them
TEST_F(OobPull, KeepsOnlyThePullRecords)
{
  auto x = service(0);
  fake_puller e;
  Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
  const std::vector<op_spec> specs = {
    {CEPH_OSD_OP_CALL, 0, ""},
    {CEPH_OSD_OP_WRITEFULL, 4096, "ok",
     delivery_t::FLAG_PULL | delivery_t::FLAG_CRC64NVME, 0},
    {CEPH_OSD_OP_WRITE, 5000, "ok",
     delivery_t::FLAG_PULL | delivery_t::FLAG_CRC32C, 8192},
    {CEPH_OSD_OP_READ, 4096, "ok", 0, 0},
  };
  auto first = new_op(specs, ceph_clock_now(), 7);
  ASSERT_EQ(0, pull_payloads(env(x.get(), &e, &t), *first, ops_of(first)));
  // what the CALL and the read returned
  auto& ops = ops_of(first);
  ops[0].outdata.append("the call's data");
  ops[0].rval = 5;
  ops[3].outdata.append(std::string(4096, 'r'));
  const auto& deliveries = first->get_req<MOSDOp>()->get_rdma_deliveries();

  pg_log_entry_t entry;
  ASSERT_TRUE(keep_pull_records(deliveries, ops, entry.op_returns));
  {
    ceph::buffer::list bl;
    entry.encode_with_checksum(bl);
    auto p = bl.cbegin();
    entry = pg_log_entry_t();
    entry.decode_with_checksum(p);
  }
  ASSERT_EQ(specs.size(), entry.op_returns.size());
  for (const size_t i : {0u, 3u}) {
    SCOPED_TRACE(i);
    EXPECT_EQ(0, entry.op_returns[i].rval);
    EXPECT_EQ(0u, entry.op_returns[i].bl.length());
  }
  for (const size_t i : {1u, 2u}) {
    SCOPED_TRACE(i);
    EXPECT_TRUE(entry.op_returns[i].bl.contents_equal(ops[i].outdata));
  }

  // the resend never ran, and reports the pulls of the attempt that did
  // the write. Its read is declined: a logged write never pushes
  auto resend = new_op(specs, ceph_clock_now(), 7);
  const auto oob = error_results(resend, 0, true, entry.op_returns);
  ASSERT_EQ(specs.size(), oob.size());
  for (const size_t i : {1u, 2u}) {
    SCOPED_TRACE(i);
    expect_same(first->oob_pulled[i], oob[i]);
  }
  expect_same(oob_result_t{}, oob[0]);
  oob_result_t declined;
  declined.flags = oob_result_t::FLAG_DECLINED;
  expect_same(declined, oob[3]);

  // no record kept, as osd_max_write_op_reply_len had no room for one,
  // or descriptors that do not mirror the ops: nothing to keep
  auto none = new_op(specs, ceph_clock_now(), 8);
  auto env1 = env(x.get(), &e, &t);
  env1.max_reply = 0;
  ASSERT_EQ(0, pull_payloads(env1, *none, ops_of(none)));
  std::vector<pg_log_op_return_item_t> kept;
  EXPECT_FALSE(keep_pull_records(
    none->get_req<MOSDOp>()->get_rdma_deliveries(), ops_of(none), kept));
  EXPECT_TRUE(kept.empty());
  std::vector<OSDOp> fewer(ops.begin(), ops.begin() + 2);
  EXPECT_FALSE(keep_pull_records(deliveries, fewer, kept));
  EXPECT_TRUE(kept.empty());
}
