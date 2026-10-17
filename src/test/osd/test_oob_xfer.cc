// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * The OSD's out-of-band transfer service (src/osd/oob_xfer.h): each job
 * it accepts runs or expires exactly once, a job it refuses stays the
 * caller's and untouched, a job taken past its post_by expires without
 * running, stop() sheds what is queued and waits for what runs, and
 * with no threads every job runs on the thread that submits it.
 */

#include "osd/oob_xfer.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include "common/Clock.h"
#include "common/ceph_context.h"
#include "common/ceph_json.h"
#include "common/config.h"
#include "common/HeartbeatMap.h"
#include "common/perf_counters.h"
#include "global/global_context.h"
#include "include/scope_guard.h"
#include "messages/MOSDOp.h"
#include "osd/OpRequest.h"

#include "gtest/gtest.h"

using namespace ceph::osd::oob;
using namespace std::chrono_literals;

namespace {

using mono_clock = ceph::mono_clock;

/// what happened to one job
struct tally_t {
  std::atomic<int> runs{0};
  std::atomic<int> expires{0};
  std::atomic<int> destroyed{0};
  std::atomic<int> expire_r{0};
  std::thread::id ran_on;
};

struct fake_job : xfer_job {
  tally_t* t;
  mono_clock::time_point post;
  uint64_t bytes;
  std::function<void()> body;
  bool pull = false;
  bool here = false;
  /// runs as the job is destroyed, before it is counted
  std::function<void()> on_destroy;

  fake_job(tally_t* t, mono_clock::time_point post, uint64_t bytes = 0,
	   std::function<void()> body = {})
    : t(t), post(post), bytes(bytes), body(std::move(body)) {}
  ~fake_job() override {
    if (on_destroy) {
      on_destroy();
    }
    t->destroyed++;
  }

  uint64_t pinned_bytes() const override { return bytes; }
  mono_clock::time_point post_by() const override { return post; }
  bool pulls() const override { return pull; }
  bool inline_only() const override { return here; }
  void run() override {
    t->ran_on = std::this_thread::get_id();
    t->runs++;
    if (body) {
      body();
    }
    debug_delay_done();
  }
  void expire(int r) override {
    t->expire_r = r;
    t->expires++;
  }
};

std::unique_ptr<xfer_job> job(tally_t* t, ceph::signedspan post_in = 10s,
			      uint64_t bytes = 0,
			      std::function<void()> body = {})
{
  return std::make_unique<fake_job>(t, mono_clock::now() + post_in, bytes,
				    std::move(body));
}

/// a job that runs on the thread that submits it (inline_only())
std::unique_ptr<xfer_job> inline_job(tally_t* t,
				     std::function<void()> body = {},
				     std::function<void()> on_destroy = {})
{
  auto j = std::make_unique<fake_job>(t, mono_clock::now() + 10s, 0,
				      std::move(body));
  j->here = true;
  j->on_destroy = std::move(on_destroy);
  return j;
}

/// wait until the service is done with the job: it ran or expired, and
/// was destroyed
void wait_done(const tally_t& t)
{
  for (int i = 0; i < 10000 && !t.destroyed; i++) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(t.destroyed);
}

/// received s seconds ago, on the monotonic clock
mono_clock::time_point ago(double s)
{
  return mono_clock::now() - ceph::make_timespan(s);
}

/// a job that holds its thread until open()
struct gate_t {
  std::mutex m;
  std::condition_variable c;
  bool is_open = false;
  std::atomic<bool> entered{false};

  std::function<void()> body() {
    return [this] {
      entered = true;
      std::unique_lock l(m);
      c.wait(l, [this] { return is_open; });
    };
  }
  void wait_entered() {
    while (!entered) {
      std::this_thread::sleep_for(1ms);
    }
  }
  void open() {
    std::lock_guard l(m);
    is_open = true;
    c.notify_all();
  }
};

std::unique_ptr<Transfers> service(unsigned threads, unsigned max_jobs = 64,
				   uint64_t max_bytes = 1 << 30,
				   ceph::timespan grace = 30s,
				   std::function<void()> idle = [] {})
{
  auto x = std::make_unique<Transfers>(g_ceph_context, threads, max_jobs,
				       max_bytes, grace, std::move(idle));
  x->start();
  return x;
}

uint64_t counter(const Transfers& x, int idx)
{
  return x.get_logger()->get(idx);
}

/// a top-level number of dump_oob_transfers
uint64_t dumped(const Transfers& x, const std::string& field)
{
  JSONFormatter f;
  f.open_object_section("oob_transfers");
  x.dump(&f);
  f.close_section();
  std::ostringstream os;
  f.flush(os);
  JSONParser p;
  EXPECT_TRUE(p.parse(os.str().c_str(), os.str().size()));
  auto o = p.find_obj(field);
  EXPECT_TRUE(o) << field;
  return o ? std::stoull(o->get_data()) : ~0ull;
}

} // anonymous namespace

// the receipt that the bounds count from is on the monotonic clock
// alone: a time on the wall clock, whose steps would move them, is not
// one (ECBackend's shard push once passed one)
static_assert(!std::is_invocable_v<
	      decltype(&make_bounds), utime_t, double, double,
	      std::optional<ceph::signedspan>, ceph::signedspan>);
static_assert(std::is_invocable_v<
	      decltype(&make_bounds), mono_clock::time_point, double, double,
	      std::optional<ceph::signedspan>, ceph::signedspan>);

// the bounds of a transfer, fixed once on the monotonic clock
TEST(OobXfer, Bounds)
{
  const auto mnow = 100s;  // a span since the OSD started
  // one second old, a lease of 6 and a drain of 4
  auto b = make_bounds(ago(1), 6, 4, std::nullopt,
		       ceph::signedspan(mnow));
  auto now = mono_clock::now();
  EXPECT_NEAR(5.0, std::chrono::duration<double>(b.post_by - now).count(),
	      0.1);
  EXPECT_NEAR(9.0, std::chrono::duration<double>(b.land_by - now).count(),
	      0.1);

  // the PG's read lease ends first: no push starts after it
  b = make_bounds(ago(1), 6, 4,
		  ceph::signedspan(mnow + 2s), ceph::signedspan(mnow));
  now = mono_clock::now();
  EXPECT_NEAR(2.0, std::chrono::duration<double>(b.post_by - now).count(),
	      0.1);
  EXPECT_NEAR(9.0, std::chrono::duration<double>(b.land_by - now).count(),
	      0.1);
  // and one that ends later changes nothing
  b = make_bounds(ago(1), 6, 4,
		  ceph::signedspan(mnow + 20s), ceph::signedspan(mnow));
  now = mono_clock::now();
  EXPECT_NEAR(5.0, std::chrono::duration<double>(b.post_by - now).count(),
	      0.1);

  // past the lease: post_by has gone by, land_by not yet
  b = make_bounds(ago(7), 6, 4, std::nullopt,
		  ceph::signedspan(mnow));
  now = mono_clock::now();
  EXPECT_LT(b.post_by, now);
  EXPECT_NEAR(3.0, std::chrono::duration<double>(b.land_by - now).count(),
	      0.1);

  // received on the monotonic clock (OpRequest::get_mono_recv_stamp()):
  // the bounds are the receipt's, exactly, whatever the wall clock reads
  const auto received = mono_clock::now() - 1s;
  b = make_bounds(received, 6, 4, std::nullopt, ceph::signedspan(mnow));
  EXPECT_EQ(received + 6s, b.post_by);
  EXPECT_EQ(received + 10s, b.land_by);
  b = make_bounds(received, 6, 4, ceph::signedspan(mnow + 2s),
		  ceph::signedspan(mnow));
  now = mono_clock::now();
  EXPECT_NEAR(2.0, std::chrono::duration<double>(b.post_by - now).count(),
	      0.1);
  EXPECT_EQ(received + 10s, b.land_by);

  bounds_t left{mono_clock::now(), mono_clock::now() + 1500ms};
  EXPECT_LE(budget_left(left), 1500ms);
  EXPECT_GT(budget_left(left), 1400ms);
  bounds_t gone{mono_clock::now() - 2s, mono_clock::now() - 1s};
  EXPECT_EQ(0ms, budget_left(gone));
}

// The receipt of a request on the monotonic clock, which the bounds of
// its transfers count from, is fixed when its OpRequest is built, from
// the age the wall clock gives it then: no later step of the wall clock
// moves an age taken from it
TEST(OobXfer, MonoReceiptStamp)
{
  OpTracker tracker{g_ceph_context, false, 1};
  // join the tracker's history thread, after the ops below are gone
  auto shutdown = make_scope_guard([&] { tracker.on_shutdown(); });
  auto new_op = [&](utime_t recv) {
    spg_t pgid;
    auto m = new MOSDOp(0, 1, hobject_t(), pgid, 1, CEPH_OSD_FLAG_READ, 0);
    m->set_recv_stamp(recv);
    return tracker.create_request<OpRequest, Message*>(m);
  };
  auto age = [](const OpRequestRef& op) {
    return std::chrono::duration<double>(
      mono_clock::now() - op->get_mono_recv_stamp()).count();
  };
  auto op = new_op(ceph_clock_now() - utime_t(3, 0));
  EXPECT_NEAR(3.0, age(op), 0.1);
  // a step of the wall clock back by 2 s would read the age as 1 s
  const auto stamp = op->get_mono_recv_stamp();
  op->get_nonconst_req()->set_recv_stamp(ceph_clock_now() - utime_t(1, 0));
  EXPECT_EQ(stamp, op->get_mono_recv_stamp());
  EXPECT_NEAR(3.0, age(op), 0.1);
  // never received, or stamped ahead of the clock: received now
  for (const utime_t recv : {utime_t(), ceph_clock_now() + utime_t(60, 0)}) {
    EXPECT_NEAR(0.0, age(new_op(recv)), 0.1);
  }
}

// the options the OSD reads to build the service, at their defaults
TEST(OobXfer, Options)
{
  auto& conf = g_ceph_context->_conf;
  EXPECT_EQ(0u, conf.get_val<uint64_t>("osd_oob_transfer_threads"));
  EXPECT_EQ(64u, conf.get_val<uint64_t>("osd_oob_transfer_queue_max"));
  EXPECT_EQ(512u << 20, uint64_t(
	      conf.get_val<Option::size_t>("osd_oob_transfer_queue_bytes")));
  EXPECT_EQ(30s, conf.get_val<std::chrono::seconds>(
	      "osd_oob_transfer_thread_timeout"));
  EXPECT_FALSE(conf.get_val<bool>("osd_oob_cuobj_transfer_threads"));
  EXPECT_EQ(0ms, conf.get_val<std::chrono::milliseconds>(
	      "osd_debug_oob_delay_post_ms"));
  EXPECT_EQ(0ms, conf.get_val<std::chrono::milliseconds>(
	      "osd_debug_oob_delay_done_ms"));
}

// many jobs on several threads, some of them already past their post_by,
// and some still queued when the service stops: each ran or expired,
// exactly once, and was destroyed
TEST(OobXfer, ExactlyOnce)
{
  constexpr int N = 400;
  std::vector<tally_t> t(N);
  auto x = service(4, N, 1 << 30);
  int accepted = 0;
  for (int i = 0; i < N; i++) {
    auto j = job(&t[i], i % 3 == 0 ? ceph::signedspan(-1ms) :
		 ceph::signedspan(10s), 1000,
		 [i] {
		   if (i % 7 == 0) {
		     std::this_thread::sleep_for(1ms);
		   }
		 });
    ASSERT_EQ(0, x->submit(j));
    EXPECT_FALSE(j);
    accepted++;
  }
  x->stop();
  int runs = 0, expires = 0;
  for (int i = 0; i < N; i++) {
    EXPECT_EQ(1, t[i].runs + t[i].expires) << "job " << i;
    EXPECT_EQ(1, t[i].destroyed) << "job " << i;
    if (i % 3 == 0) {
      EXPECT_EQ(0, t[i].runs) << "job " << i;
    }
    if (t[i].expires) {
      EXPECT_TRUE(t[i].expire_r == -ETIMEDOUT ||
		  t[i].expire_r == -ESHUTDOWN) << "job " << i;
    }
    runs += t[i].runs;
    expires += t[i].expires;
  }
  EXPECT_EQ(accepted, runs + expires);
  EXPECT_EQ((uint64_t)expires, counter(*x, l_oob_xfer_expired) +
	    counter(*x, l_oob_xfer_shed));
  EXPECT_EQ(0u, counter(*x, l_oob_xfer_queued));
  EXPECT_EQ(0u, counter(*x, l_oob_xfer_running));
  // the threads ran them, not this one
  for (int i = 0; i < N; i++) {
    if (t[i].runs) {
      EXPECT_NE(std::this_thread::get_id(), t[i].ran_on);
    }
  }
}

// over either cap a job is refused: the caller keeps it, and nothing of
// it ran, expired or was destroyed. A job that ended gives its place and
// its bytes back.
TEST(OobXfer, CapsRefuseAndLeaveTheJob)
{
  {
    // at most two jobs queued or running
    gate_t gate;
    tally_t tg, ta, tb;
    auto x = service(1, 2, 1 << 30);
    auto g = job(&tg, 10s, 0, gate.body());
    ASSERT_EQ(0, x->submit(g));
    gate.wait_entered();
    auto a = job(&ta);
    ASSERT_EQ(0, x->submit(a));
    auto b = job(&tb);
    EXPECT_EQ(-EBUSY, x->submit(b));
    ASSERT_TRUE(b);
    EXPECT_EQ(0, tb.runs + tb.expires + tb.destroyed);
    EXPECT_EQ(1u, counter(*x, l_oob_xfer_refused));
    gate.open();
    wait_done(ta);

    // the one thread took g2 only once it was done with a: two jobs fit
    // again, and not a third
    gate_t gate2;
    tally_t tg2, ta2, tb2;
    auto g2 = job(&tg2, 10s, 0, gate2.body());
    ASSERT_EQ(0, x->submit(g2));
    gate2.wait_entered();
    auto a2 = job(&ta2);
    EXPECT_EQ(0, x->submit(a2));
    auto b2 = job(&tb2);
    EXPECT_EQ(-EBUSY, x->submit(b2));
    EXPECT_EQ(2u, counter(*x, l_oob_xfer_refused));
    gate2.open();
    wait_done(ta2);
    x->stop();
    EXPECT_EQ(1, tg.runs);
    EXPECT_EQ(1, ta.runs);
    EXPECT_EQ(1, tg2.runs);
    EXPECT_EQ(1, ta2.runs);
    EXPECT_EQ(0, tb.runs + tb.expires + tb.destroyed);
    // still the caller's to answer
    b.reset();
    EXPECT_EQ(1, tb.destroyed);
  }
  {
    // at most 1000 bytes pinned
    gate_t gate;
    tally_t tg, ta, tb, tc;
    auto x = service(1, 64, 1000);
    auto g = job(&tg, 10s, 600, gate.body());
    ASSERT_EQ(0, x->submit(g));
    gate.wait_entered();
    auto a = job(&ta, 10s, 500);
    EXPECT_EQ(-EBUSY, x->submit(a));
    ASSERT_TRUE(a);
    EXPECT_EQ(0, ta.runs + ta.expires + ta.destroyed);
    auto b = job(&tb, 10s, 400);
    EXPECT_EQ(0, x->submit(b));
    EXPECT_EQ(1000u, dumped(*x, "pinned_bytes"));
    // a single job larger than the cap never fits
    auto c = job(&tc, 10s, 1001);
    EXPECT_EQ(-EBUSY, x->submit(c));
    EXPECT_EQ(2u, counter(*x, l_oob_xfer_refused));
    gate.open();
    wait_done(tb);

    // the one thread took g2 only once it was done with b: 600 and 400
    // bytes fit again, and not a byte more
    gate_t gate2;
    tally_t tg2, ta2, tc2;
    auto g2 = job(&tg2, 10s, 600, gate2.body());
    ASSERT_EQ(0, x->submit(g2));
    gate2.wait_entered();
    auto a2 = job(&ta2, 10s, 400);
    EXPECT_EQ(0, x->submit(a2));
    auto c2 = job(&tc2, 10s, 1);
    EXPECT_EQ(-EBUSY, x->submit(c2));
    EXPECT_EQ(3u, counter(*x, l_oob_xfer_refused));
    gate2.open();
    wait_done(ta2);
    x->stop();
    EXPECT_EQ(1, tb.runs);
    EXPECT_EQ(1, tg2.runs);
    EXPECT_EQ(1, ta2.runs);
    EXPECT_EQ(0, ta.runs + ta.expires + ta.destroyed);
    EXPECT_EQ(0, tc.runs + tc.expires + tc.destroyed);
    EXPECT_EQ(0, tc2.runs + tc2.expires + tc2.destroyed);
    // nothing is left pinned once the jobs are gone
    EXPECT_EQ(0u, dumped(*x, "pinned_bytes"));
    EXPECT_EQ(0u, dumped(*x, "queued"));
  }
}

// a job that waited in the queue past its post_by expires: the
// executor is never called for it
TEST(OobXfer, ExpiresPastPostBy)
{
  gate_t gate;
  tally_t tg, ta, tb;
  auto x = service(1);
  auto g = job(&tg, 10s, 0, gate.body());
  ASSERT_EQ(0, x->submit(g));
  gate.wait_entered();
  auto a = job(&ta, 50ms);
  ASSERT_EQ(0, x->submit(a));
  auto b = job(&tb, 10s);
  ASSERT_EQ(0, x->submit(b));
  std::this_thread::sleep_for(150ms);
  gate.open();
  wait_done(tb);
  x->stop();
  EXPECT_EQ(0, ta.runs);
  EXPECT_EQ(1, ta.expires);
  EXPECT_EQ(-ETIMEDOUT, ta.expire_r);
  EXPECT_EQ(1, tb.runs);
  EXPECT_EQ(1u, counter(*x, l_oob_xfer_expired));
  // the time a job waited for a thread
  EXPECT_EQ(3u, x->get_logger()->get_tavg_ns(l_oob_xfer_queue_lat).second);
}

// stop() sheds the queued jobs, which answer as refused, and returns only
// once the running one ended
TEST(OobXfer, StopShedsAndWaits)
{
  tally_t tg;
  std::vector<tally_t> t(3);
  std::atomic<bool> entered{false}, ended{false};
  auto x = service(1);
  auto g = job(&tg, 10s, 0, [&] {
    entered = true;
    std::this_thread::sleep_for(300ms);
    ended = true;
  });
  ASSERT_EQ(0, x->submit(g));
  while (!entered) {
    std::this_thread::sleep_for(1ms);
  }
  for (auto& ti : t) {
    auto j = job(&ti);
    ASSERT_EQ(0, x->submit(j));
  }
  x->stop();
  EXPECT_TRUE(ended);
  EXPECT_EQ(1, tg.runs);
  for (auto& ti : t) {
    EXPECT_EQ(0, ti.runs);
    EXPECT_EQ(1, ti.expires);
    EXPECT_EQ(-ESHUTDOWN, ti.expire_r);
    EXPECT_EQ(1, ti.destroyed);
  }
  EXPECT_EQ(3u, counter(*x, l_oob_xfer_shed));
  EXPECT_EQ(0u, counter(*x, l_oob_xfer_expired));

  // and refuses what comes later
  tally_t tl;
  auto l = job(&tl);
  EXPECT_EQ(-ESHUTDOWN, x->submit(l));
  EXPECT_TRUE(l);
}

// once stopping, a job that would wait for a thread is refused, and
// stays the caller's; one that runs on the thread that submits it still
// runs there, until stop()
TEST(OobXfer, RefusedWhileStopping)
{
  for (unsigned threads : {0u, 2u}) {
    auto x = service(threads);
    x->start_stopping();
    uint64_t refused = 0;
    if (threads) {
      tally_t t;
      auto j = job(&t);
      EXPECT_EQ(-ESHUTDOWN, x->submit(j));
      ASSERT_TRUE(j);
      EXPECT_EQ(0, t.runs + t.expires + t.destroyed);
      EXPECT_EQ(++refused, counter(*x, l_oob_xfer_refused));
    }
    tally_t ti;
    auto i = threads ? inline_job(&ti) : job(&ti);
    ASSERT_EQ(0, x->submit(i));
    EXPECT_FALSE(i);
    EXPECT_EQ(1, ti.runs);
    EXPECT_EQ(1, ti.destroyed);
    EXPECT_EQ(std::this_thread::get_id(), ti.ran_on);
    EXPECT_EQ(refused, counter(*x, l_oob_xfer_refused));

    // once stopped, every job is refused
    x->stop();
    tally_t ts;
    auto s = threads ? inline_job(&ts) : job(&ts);
    EXPECT_EQ(-ESHUTDOWN, x->submit(s));
    ASSERT_TRUE(s);
    EXPECT_EQ(0, ts.runs + ts.expires + ts.destroyed);
    EXPECT_EQ(++refused, counter(*x, l_oob_xfer_refused));
  }
}

// stop() returns only once a job that runs on the thread that submitted
// it is destroyed: what the job holds goes before the executors do
TEST(OobXfer, StopWaitsForInlineJobs)
{
  for (unsigned threads : {0u, 2u}) {
    gate_t gate;
    tally_t t;
    auto x = service(threads);
    auto j = inline_job(&t, gate.body(),
			[] { std::this_thread::sleep_for(200ms); });
    std::thread op([&] { EXPECT_EQ(0, x->submit(j)); });
    gate.wait_entered();
    std::atomic<int> destroyed_at_stop{-1};
    std::thread stopper([&] {
      x->stop();
      destroyed_at_stop = t.destroyed.load();
    });
    // let stop() start waiting
    std::this_thread::sleep_for(50ms);
    EXPECT_EQ(-1, destroyed_at_stop.load());
    gate.open();
    op.join();
    stopper.join();
    EXPECT_EQ(1, t.runs);
    EXPECT_EQ(1, destroyed_at_stop.load());
  }
}

// with no threads a job runs on the submitting thread, before submit()
// returns; the caps do not apply to it
TEST(OobXfer, InlineWithNoThreads)
{
  auto x = service(0, 0, 0);
  EXPECT_FALSE(x->async());
  tally_t t;
  auto j = job(&t, 10s, 1 << 20);
  ASSERT_EQ(0, x->submit(j));
  EXPECT_FALSE(j);
  EXPECT_EQ(1, t.runs);
  EXPECT_EQ(1, t.destroyed);
  EXPECT_EQ(std::this_thread::get_id(), t.ran_on);

  // and expires there too
  tally_t te;
  auto e = job(&te, -1ms);
  ASSERT_EQ(0, x->submit(e));
  EXPECT_EQ(0, te.runs);
  EXPECT_EQ(1, te.expires);
  EXPECT_EQ(1, te.destroyed);
  x->stop();
}

// a job whose executor has no state on the transfer threads runs on the
// submitting thread, even when there are threads
TEST(OobXfer, InlineOnly)
{
  auto x = service(2);
  EXPECT_TRUE(x->async());
  tally_t t;
  auto j = std::make_unique<fake_job>(&t, mono_clock::now() + 10s);
  j->here = true;
  std::unique_ptr<xfer_job> jj = std::move(j);
  ASSERT_EQ(0, x->submit(jj));
  EXPECT_EQ(1, t.runs);
  EXPECT_EQ(std::this_thread::get_id(), t.ran_on);
  x->stop();
}

// a thread with nothing to run calls idle() about every 100 ms, and only
// on its own thread
TEST(OobXfer, IdleOnIdleThreads)
{
  std::mutex m;
  std::set<std::thread::id> callers;
  std::atomic<int> calls{0};
  auto x = service(2, 64, 1 << 30, 30s, [&] {
    std::lock_guard l(m);
    callers.insert(std::this_thread::get_id());
    calls++;
  });
  std::this_thread::sleep_for(1s);
  x->stop();
  std::lock_guard l(m);
  EXPECT_EQ(2u, callers.size());
  EXPECT_EQ(0u, callers.count(std::this_thread::get_id()));
  // two threads, ten times a second each, give or take
  const int n = calls;
  EXPECT_GE(n, 10);
  EXPECT_LE(n, 30);
}

// the heartbeat grace covers a whole transfer: a long one does not make
// the OSD unhealthy while it is shorter than the grace, even when the
// thread takes it right after another one, one that runs past the grace
// does, and the thread is not killed. (12 s against the default 30 s,
// scaled down.)
TEST(OobXfer, LongJobKeepsTheHeartbeat)
{
  auto hb = g_ceph_context->get_heartbeat_map();
  {
    // two jobs back to back, the second taken without an idle wait
    // between them: it ends 2.4 s after the thread last checked in
    // before the first, past the grace
    std::atomic<bool> healthy1{false}, healthy2{false};
    tally_t t1, t2;
    auto x = service(1, 64, 1 << 30, 2s);
    auto j1 = job(&t1, 10s, 0, [&] {
      std::this_thread::sleep_for(1200ms);
      healthy1 = hb->is_healthy();
    });
    auto j2 = job(&t2, 10s, 0, [&] {
      std::this_thread::sleep_for(1200ms);
      healthy2 = hb->is_healthy();
    });
    ASSERT_EQ(0, x->submit(j1));
    ASSERT_EQ(0, x->submit(j2));
    wait_done(t2);
    x->stop();
    EXPECT_EQ(1, t1.runs);
    EXPECT_EQ(1, t2.runs);
    EXPECT_TRUE(healthy1);
    EXPECT_TRUE(healthy2);
  }
  {
    std::atomic<bool> healthy{true};
    tally_t t;
    auto x = service(1, 64, 1 << 30, 500ms);
    auto j = job(&t, 10s, 0, [&] {
      std::this_thread::sleep_for(800ms);
      healthy = hb->is_healthy();
    });
    ASSERT_EQ(0, x->submit(j));
    wait_done(t);
    // the thread checks in again once it is idle
    std::this_thread::sleep_for(200ms);
    EXPECT_TRUE(hb->is_healthy());
    x->stop();
    EXPECT_EQ(1, t.runs);
    EXPECT_FALSE(healthy);
  }
  EXPECT_TRUE(hb->is_healthy());
}

// osd_debug_oob_delay_post_ms counts against the lease: a delay past a
// job's post_by expires it. osd_debug_oob_delay_done_ms sleeps where a
// job calls debug_delay_done(). The service reads both as they change at
// runtime, and a job that no service runs has no delay.
TEST(OobXfer, DebugDelays)
{
  auto& conf = g_ceph_context->_conf;
  auto set = [&conf](const char* option, const char* value) {
    conf.set_val_or_die(option, value);
    conf.apply_changes(nullptr);
  };
  // the other tests run with no delay, whatever this one asserts
  auto reset = make_scope_guard([&set] {
    set("osd_debug_oob_delay_post_ms", "0");
    set("osd_debug_oob_delay_done_ms", "0");
  });
  auto x = service(0);
  set("osd_debug_oob_delay_post_ms", "200");
  tally_t t;
  auto j = job(&t, 50ms);
  ASSERT_EQ(0, x->submit(j));
  EXPECT_EQ(0, t.runs);
  EXPECT_EQ(1, t.expires);
  set("osd_debug_oob_delay_post_ms", "0");

  // fake_job::run() calls debug_delay_done() last, as a job does
  set("osd_debug_oob_delay_done_ms", "100");
  tally_t slow;
  auto start = mono_clock::now();
  auto s = job(&slow);
  ASSERT_EQ(0, x->submit(s));
  EXPECT_EQ(1, slow.runs);
  EXPECT_GE(mono_clock::now() - start, 100ms);
  // not one that no service runs
  start = mono_clock::now();
  tally_t direct;
  job(&direct)->run();
  EXPECT_EQ(1, direct.runs);
  EXPECT_LT(mono_clock::now() - start, 50ms);
  set("osd_debug_oob_delay_done_ms", "0");
  tally_t quick;
  start = mono_clock::now();
  auto q = job(&quick);
  ASSERT_EQ(0, x->submit(q));
  EXPECT_EQ(1, quick.runs);
  EXPECT_LT(mono_clock::now() - start, 50ms);
  x->stop();
}

// dump_oob_transfers lists the running jobs with their ages; a pull is
// timed apart from a push
TEST(OobXfer, DumpAndLatencies)
{
  gate_t gate;
  tally_t tg, tp;
  auto x = service(1);
  auto g = job(&tg, 10s, 4096, gate.body());
  ASSERT_EQ(0, x->submit(g));
  gate.wait_entered();
  {
    JSONFormatter f;
    f.open_object_section("oob_transfers");
    x->dump(&f);
    f.close_section();
    std::ostringstream os;
    f.flush(os);
    JSONParser p;
    ASSERT_TRUE(p.parse(os.str().c_str(), os.str().size()));
    auto running = p.find_obj("running");
    ASSERT_TRUE(running);
    ASSERT_TRUE(running->is_array());
    auto jobs = running->get_array_elements();
    ASSERT_EQ(1u, jobs.size());
    JSONParser jp;
    ASSERT_TRUE(jp.parse(jobs[0].c_str(), jobs[0].size()));
    ASSERT_TRUE(jp.find_obj("kind"));
    EXPECT_EQ("push", jp.find_obj("kind")->get_data());
    EXPECT_TRUE(jp.find_obj("age"));
    ASSERT_TRUE(jp.find_obj("pinned_bytes"));
    EXPECT_EQ("4096", jp.find_obj("pinned_bytes")->get_data());
  }
  gate.open();
  auto p = std::make_unique<fake_job>(&tp, mono_clock::now() + 10s);
  p->pull = true;
  std::unique_ptr<xfer_job> pj = std::move(p);
  ASSERT_EQ(0, x->submit(pj));
  wait_done(tp);
  x->stop();
  EXPECT_EQ(1u, x->get_logger()->get_tavg_ns(l_oob_xfer_push_lat).second);
  EXPECT_EQ(1u, x->get_logger()->get_tavg_ns(l_oob_xfer_pull_lat).second);
}

namespace {
/// a job of many that share one tally, on any thread
struct count_job : xfer_job {
  tally_t* t;
  mono_clock::time_point post;
  uint64_t bytes;
  bool here;

  count_job(tally_t* t, mono_clock::time_point post, uint64_t bytes,
	    bool here)
    : t(t), post(post), bytes(bytes), here(here) {}
  ~count_job() override { t->destroyed++; }

  uint64_t pinned_bytes() const override { return bytes; }
  mono_clock::time_point post_by() const override { return post; }
  bool inline_only() const override { return here; }
  void run() override {
    t->runs++;
    if (bytes % 3 == 0) {
      std::this_thread::sleep_for(50us);
    }
  }
  void expire(int r) override { t->expires++; }
  void dump(ceph::Formatter* f) const override {
    f->dump_unsigned("bytes", bytes);
  }
};
} // anonymous namespace

// as in the OSD: op threads submit at once, inline and to the threads,
// over the caps and past post_by, the admin socket dumps meanwhile, and
// the service stops under them. Each accepted job ran or expired once,
// and every job was destroyed once, by the service or by its submitter.
TEST(OobXfer, ManySubmittersDumpAndStop)
{
  for (unsigned threads : {0u, 3u}) {
    tally_t t;
    std::atomic<bool> stopped{false}, dumping{true};
    std::atomic<int> created{0}, accepted{0}, refused{0}, shut{0};
    auto x = service(threads, 16, 1 << 20);
    std::vector<std::thread> ops;
    for (int s = 0; s < 6; s++) {
      ops.emplace_back([&, s] {
	// until a submit is refused because the service stopped
	for (int i = 0; ; i++) {
	  std::unique_ptr<xfer_job> j = std::make_unique<count_job>(
	    &t, mono_clock::now() + (i % 5 ? 10s : -1ms), 1000 + i,
	    (s + i) % 4 == 0);
	  created++;
	  const int r = x->submit(j);
	  if (r == 0) {
	    accepted++;
	    EXPECT_FALSE(j);
	  } else {
	    EXPECT_TRUE(j);
	    if (r == -EBUSY) {
	      refused++;
	    } else {
	      EXPECT_EQ(-ESHUTDOWN, r);
	      shut++;
	      if (stopped) {
		break;
	      }
	    }
	  }
	}
      });
    }
    std::thread dumper([&] {
      while (dumping) {
	JSONFormatter f;
	f.open_object_section("oob_transfers");
	x->dump(&f);
	f.close_section();
      }
    });
    std::this_thread::sleep_for(300ms);
    x->start_stopping();
    std::this_thread::sleep_for(20ms);
    x->stop();
    stopped = true;
    for (auto& op : ops) {
      op.join();
    }
    dumping = false;
    dumper.join();
    EXPECT_EQ(accepted.load(), t.runs + t.expires);
    EXPECT_EQ(created.load(), accepted + refused + shut);
    EXPECT_EQ(created.load(), t.destroyed.load());
    EXPECT_GT(shut.load(), 0);
    EXPECT_EQ(0u, dumped(*x, "pinned_bytes"));
    EXPECT_EQ(0u, dumped(*x, "queued"));
    EXPECT_EQ(0u, counter(*x, l_oob_xfer_running));
  }
}
