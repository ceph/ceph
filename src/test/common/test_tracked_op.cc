// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <array>
#include <atomic>
#include <chrono>
#include <map>
#include <sstream>
#include <thread>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "common/JSONFormatter.h"
#include "common/TrackedOp.h"
#include "common/tracer.h"
#include "global/global_context.h"

using tracing::OpPhase;
using tracing::OpTimeline;
using tracing::op_phases;

namespace {

// --- op_phases: turning an op tracker timeline into phases

utime_t at(double secs) {
  // a fixed base, so stamps are far from zero like real ones
  utime_t t(1700000000, 0);
  t += secs;
  return t;
}

utime_t ago(double secs) {
  utime_t t = ceph_clock_now();
  t -= secs;
  return t;
}

OpTimeline timeline(std::vector<std::pair<double, std::string>> events,
                    double end, bool complete = true) {
  OpTimeline t;
  t.name = "osd_op";
  t.start = at(0);
  t.end = at(end);
  t.complete = complete;
  for (auto& [secs, name] : events) {
    t.events.emplace_back(at(secs), name);
  }
  return t;
}

std::vector<std::string> names(const std::vector<OpPhase>& phases) {
  std::vector<std::string> ret;
  for (const auto& p : phases) {
    ret.push_back(p.name);
  }
  return ret;
}

TEST(OpPhases, NamedAfterBothEvents) {
  auto t = timeline({{0, "initiated"}, {0.1, "queued_for_pg"},
                     {0.9, "reached_pg"}, {1.0, "done"}}, 1.0);
  auto phases = op_phases(t, 0.01);
  EXPECT_EQ(names(phases), (std::vector<std::string>{
    "initiated -> queued_for_pg",
    "queued_for_pg -> reached_pg",
    "reached_pg -> done"}));
  EXPECT_EQ(phases[1].start, at(0.1));
  EXPECT_EQ(phases[1].end, at(0.9));
}

TEST(OpPhases, ShortPhasesDropped) {
  auto t = timeline({{0, "initiated"}, {0.001, "queued_for_pg"},
                     {1.0, "done"}}, 1.0);
  // 0.1% of the op is below a 1% share; the long wait stays
  EXPECT_EQ(names(op_phases(t, 0.01)),
            (std::vector<std::string>{"queued_for_pg -> done"}));
  // with no minimum share, every phase with some length is kept
  EXPECT_EQ(op_phases(t, 0).size(), 2u);
}

TEST(OpPhases, ZeroLengthPhasesDropped) {
  // create_request() marks several events with the same receive stamp
  auto t = timeline({{0, "initiated"}, {0, "header_read"}, {0, "all_read"},
                     {1.0, "done"}}, 1.0);
  EXPECT_EQ(names(op_phases(t, 0)),
            (std::vector<std::string>{"all_read -> done"}));
}

TEST(OpPhases, UnsetStampsSkipped) {
  // an unset throttle stamp is zero; it must not become a 50-year phase
  auto t = timeline({{0, "initiated"}, {0.5, "all_read"}, {1.0, "done"}}, 1.0);
  t.events.insert(t.events.begin() + 1, {utime_t(), "throttled"});
  EXPECT_EQ(names(op_phases(t, 0)), (std::vector<std::string>{
    "initiated -> all_read",
    "all_read -> done"}));
}

TEST(OpPhases, InFlightOpEndsInOpenPhase) {
  auto t = timeline({{0, "initiated"}, {0.2, "waiting for subops from 1,2"}},
                    1.0, false);
  auto phases = op_phases(t, 0.01);
  ASSERT_EQ(phases.size(), 2u);
  EXPECT_EQ(phases.back().name, "waiting for subops from 1,2 -> (in flight)");
  EXPECT_EQ(phases.back().end, at(1.0));
  // a completed op gets no open phase
  t.complete = true;
  EXPECT_EQ(op_phases(t, 0.01).size(), 1u);
}

TEST(OpPhases, NoEvents) {
  EXPECT_TRUE(op_phases(timeline({}, 1.0), 0.01).empty());
  EXPECT_TRUE(op_phases(timeline({{0, "initiated"}}, 1.0), 0.01).empty());
}

// --- named_phases: what the op was doing, for the events the OSD records

using tracing::named_phases;

// the events of a replicated write on its primary; osd.2 is the slow replica
OpTimeline primary_write() {
  return timeline({{0, "initiated"}, {0, "header_read"}, {0.001, "throttled"},
                   {0.002, "all_read"}, {0.003, "dispatched"},
                   {0.004, "queued_for_pg"}, {0.020, "reached_pg"},
                   {0.021, "started"}, {0.030, "waiting for subops from 1,2"},
                   {0.040, "op_commit"}, {0.200, "sub_op_commit_rec from osd.1"},
                   {0.600, "sub_op_commit_rec from osd.2"},
                   {0.610, "commit_sent"}, {0.611, "done"}}, 0.611);
}

std::map<std::string, OpPhase> by_name(const std::vector<OpPhase>& phases) {
  std::map<std::string, OpPhase> m;
  for (const auto& p : phases) {
    EXPECT_TRUE(m.emplace(p.name, p).second) << "duplicate phase " << p.name;
  }
  return m;
}

TEST(NamedPhases, PrimaryWrite) {
  auto phases = by_name(named_phases(primary_write(), 0));
  std::vector<std::string> names;
  for (const auto& [name, _] : phases) {
    names.push_back(name);
  }
  EXPECT_EQ(names, (std::vector<std::string>{
    "dispatch", "execute", "finish", "in PG", "local commit", "queued for PG",
    "receive", "replica osd.1", "replica osd.2", "reply"}));
  // the replica phases run in parallel from sending the sub-ops
  EXPECT_EQ(phases["replica osd.1"].start, at(0.030));
  EXPECT_EQ(phases["replica osd.1"].end, at(0.200));
  EXPECT_EQ(phases["replica osd.2"].start, at(0.030));
  EXPECT_EQ(phases["replica osd.2"].end, at(0.600));
  EXPECT_EQ(phases["local commit"].end, at(0.040));
  // the receive events merge into one phase
  EXPECT_EQ(phases["receive"].start, at(0));
  EXPECT_EQ(phases["receive"].end, at(0.003));
  // execution ends when the sub-ops are sent; the reply follows the last ack
  EXPECT_EQ(phases["execute"].end, at(0.030));
  EXPECT_EQ(phases["reply"].start, at(0.600));
  EXPECT_EQ(phases["reply"].end, at(0.610));
}

TEST(NamedPhases, OrderedByStart) {
  auto phases = named_phases(primary_write(), 0);
  for (size_t i = 1; i < phases.size(); ++i) {
    EXPECT_LE(phases[i - 1].start, phases[i].start);
  }
}

TEST(NamedPhases, ShortPhasesDropped) {
  // at a 1% share only the long waits of a 611 ms op are left
  auto phases = by_name(named_phases(primary_write(), 0.01));
  EXPECT_TRUE(phases.count("replica osd.2"));
  EXPECT_TRUE(phases.count("replica osd.1"));
  EXPECT_TRUE(phases.count("reply"));
  EXPECT_FALSE(phases.count("receive"));
  EXPECT_FALSE(phases.count("in PG"));
}

TEST(NamedPhases, DelayReasons) {
  auto phases = by_name(named_phases(
    timeline({{0, "queued_for_pg"}, {0.1, "reached_pg"},
              {0.1, "waiting for rw locks"}, {0.8, "reached_pg"},
              {0.8, "started"}, {1.0, "done"}}, 1.0), 0));
  EXPECT_EQ(phases["waiting for rw locks"].start, at(0.1));
  EXPECT_EQ(phases["waiting for rw locks"].end, at(0.8));
  EXPECT_EQ(phases["queued for PG"].end, at(0.1));
  EXPECT_EQ(phases["execute"].end, at(1.0));
}

TEST(NamedPhases, UnknownEventsKeepGenericNames) {
  auto phases = by_name(named_phases(
    timeline({{0, "initiated"}, {0.5, "something new"}, {1.0, "done"}}, 1.0), 0));
  EXPECT_TRUE(phases.count("receive"));
  EXPECT_TRUE(phases.count("something new -> done"));
}

TEST(NamedPhases, InFlight) {
  auto phases = by_name(named_phases(
    timeline({{0, "initiated"}, {0.2, "waiting for subops from 1,2"}}, 1.0, false), 0));
  EXPECT_EQ(phases["waiting for subops from 1,2 -> (in flight)"].end, at(1.0));
}

// --- exported_phases: the phases that get a child span of their own

TEST(ExportedPhases, OnlyTheLongOnes) {
  // at 5% of 611 ms only the two replica waits are left; the rest stay on
  // the op span as events
  auto phases = by_name(tracing::exported_phases(primary_write()));
  EXPECT_TRUE(phases.count("replica osd.1"));
  EXPECT_TRUE(phases.count("replica osd.2"));
  EXPECT_FALSE(phases.count("receive"));
  EXPECT_FALSE(phases.count("dispatch"));
  EXPECT_FALSE(phases.count("queued for PG"));
  EXPECT_FALSE(phases.count("reply"));
  EXPECT_EQ(phases.size(), 2u);
}

TEST(ExportedPhases, NoneWhenOnePhaseIsTheWholeOp) {
  // a replica's sub-op that spent its time waiting for the message to arrive
  auto t = timeline({{0, "initiated"}, {0.149, "queued_for_pg"},
                     {0.1492, "reached_pg"}, {0.1493, "done"}}, 0.1493);
  EXPECT_TRUE(tracing::exported_phases(t).empty());
}

TEST(ExportedPhases, KeptWhenTheOpSplits) {
  auto t = timeline({{0, "initiated"}, {0.05, "queued_for_pg"},
                     {0.15, "reached_pg"}, {0.16, "done"}}, 0.16);
  auto phases = by_name(tracing::exported_phases(t));
  EXPECT_TRUE(phases.count("receive"));
  EXPECT_TRUE(phases.count("queued for PG"));
}

// --- request_trace: one trace per request, derived from its reqid

TEST(RequestTrace, SameRequestSameIds) {
  auto a = tracing::request_trace(7, 8, 4135, 1, 42);
  auto b = tracing::request_trace(7, 8, 4135, 1, 42);
  EXPECT_EQ(a.trace_id, b.trace_id);
  EXPECT_EQ(a.root_span_id, b.root_span_id);
}

TEST(RequestTrace, EveryFieldMatters) {
  const auto base = tracing::request_trace(7, 8, 4135, 1, 42);
  for (const auto& other : {
         tracing::request_trace(8, 8, 4135, 1, 42),  // another cluster
         tracing::request_trace(7, 4, 4135, 1, 42),  // another entity type
         tracing::request_trace(7, 8, 4136, 1, 42),  // another client
         tracing::request_trace(7, 8, 4135, 2, 42),  // another incarnation
         tracing::request_trace(7, 8, 4135, 1, 43),  // another request
       }) {
    EXPECT_NE(base.trace_id, other.trace_id);
    EXPECT_NE(base.root_span_id, other.root_span_id);
    EXPECT_NE(base.primary_span_id, other.primary_span_id);
  }
  EXPECT_NE(base.root_span_id, base.primary_span_id);
}

TEST(RequestTrace, StableAcrossReleases) {
  // OSDs of different versions must derive the same ids, or a request's
  // spans split across traces during an upgrade: never change these
  auto r = tracing::request_trace(7, 8, 4135, 1, 42);
  auto hex = [](const auto& bytes) {
    std::string s;
    for (uint8_t b : bytes) {
      s += fmt::format("{:02x}", b);
    }
    return s;
  };
  EXPECT_EQ(hex(r.trace_id), "e2f12c19c42f121018e834d1f436e17e");
  EXPECT_EQ(hex(r.root_span_id), "4fef91d5752ea8dc");
  EXPECT_EQ(hex(r.primary_span_id), "e8b68dbd8c0cf9d9");
}

TEST(RequestTrace, IdsAreValid) {
  // OpenTelemetry treats all-zero ids as invalid, and would drop the parent
  const std::array<uint8_t, 16> zero_trace{};
  const std::array<uint8_t, 8> zero_span{};
  for (uint64_t tid = 0; tid < 1000; ++tid) {
    auto r = tracing::request_trace(0, 0, 0, 0, tid);
    EXPECT_NE(r.trace_id, zero_trace);
    EXPECT_NE(r.root_span_id, zero_span);
    EXPECT_NE(r.primary_span_id, zero_span);
  }
}

// --- OpHistory: tracing slow ops after they complete

struct TestOp : public TrackedOp {
  TestOp(OpTracker* tracker, utime_t initiated)
    : TrackedOp(tracker, initiated) {}
  void _dump_op_descriptor(std::ostream& stream) const override {
    stream << "test_op";
  }
};

class SlowOpTracing : public ::testing::Test {
protected:
  OpTracker tracker{g_ceph_context, true, 1};
  std::atomic<int> traced{0};
  std::atomic<int> traced_in_flight{0};

  void SetUp() override {
    tracker.set_history_size_and_duration(100, 600);
  }
  void TearDown() override {
    tracker.on_shutdown();
  }

  void set_hook() {
    tracker.set_slow_op_tracer([this](TrackedOp&, bool in_flight) {
      if (in_flight) {
        ++traced_in_flight;
      }
      return "trace-" + std::to_string(++traced);
    });
  }

  // runs an op that started `secs` ago and completes now
  void run_op(double secs) {
    TrackedOpRef op(new TestOp(&tracker, ago(secs)));
    op->tracking_start();
    op->mark_event("started");
  }

  std::string dump() {
    ceph::JSONFormatter f;
    tracker.dump_historic_ops(&f);
    std::ostringstream ss;
    f.flush(ss);
    return ss.str();
  }

  static size_t count(const std::string& s, const std::string& what) {
    size_t n = 0;
    for (auto pos = s.find(what); pos != std::string::npos; pos = s.find(what, pos + 1)) {
      ++n;
    }
    return n;
  }

  // the history is filled by a service thread; wait until it holds `n` ops
  void wait_for_history(size_t n) {
    for (int i = 0; i < 100; ++i) {
      if (count(dump(), "\"description\"") >= n) {
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    FAIL() << "history never reached " << n << " ops: " << dump();
  }
};

TEST_F(SlowOpTracing, OffByDefault) {
  set_hook();
  run_op(2.0);
  wait_for_history(1);
  EXPECT_EQ(traced, 0);
  EXPECT_EQ(count(dump(), "\"trace_id\""), 0u);
}

TEST_F(SlowOpTracing, OnlySlowOpsTraced) {
  set_hook();
  tracker.set_trace_threshold_and_rate(1.0, 100);
  run_op(0);
  run_op(2.0);
  wait_for_history(2);
  EXPECT_EQ(traced, 1);
  const auto d = dump();
  EXPECT_EQ(count(d, "\"trace_id\""), 1u);
  EXPECT_EQ(count(d, "\"trace-1\""), 1u);
}

TEST_F(SlowOpTracing, RateLimited) {
  set_hook();
  tracker.set_trace_threshold_and_rate(1.0, 1);
  for (int i = 0; i < 5; ++i) {
    run_op(2.0);
  }
  wait_for_history(5);
  // one token to start with, and far less than a second passed since
  EXPECT_EQ(traced, 1);
  EXPECT_EQ(count(dump(), "\"trace_id\""), 1u);
}

TEST_F(SlowOpTracing, NoTracerNoTraceId) {
  tracker.set_trace_threshold_and_rate(1.0, 100);
  run_op(2.0);
  wait_for_history(1);
  EXPECT_EQ(count(dump(), "\"trace_id\""), 0u);
}

TEST_F(SlowOpTracing, ThresholdChangeAppliesToLaterOps) {
  set_hook();
  tracker.set_trace_threshold_and_rate(1.0, 100);
  run_op(2.0);
  wait_for_history(1);
  tracker.set_trace_threshold_and_rate(0, 100);
  run_op(2.0);
  wait_for_history(2);
  EXPECT_EQ(traced, 1);
}

TEST(TrackedOp, GetEventsInOrder) {
  OpTracker tracker{g_ceph_context, true, 1};
  {
    TrackedOpRef op(new TestOp(&tracker, ceph_clock_now()));
    op->tracking_start();
    op->mark_event("a", at(1));
    op->mark_event("b", at(2));
    auto events = op->get_events();
    ASSERT_EQ(events.size(), 3u);  // "initiated", then ours
    EXPECT_EQ(events[0].second, "initiated");
    EXPECT_EQ(events[1], std::make_pair(at(1), std::string("a")));
    EXPECT_EQ(events[2], std::make_pair(at(2), std::string("b")));
  }
  tracker.on_shutdown();
}

} // anonymous namespace

TEST_F(SlowOpTracing, InFlightTracedOnce) {
  set_hook();
  tracker.set_trace_threshold_and_rate(1.0, 100);
  TrackedOpRef op(new TestOp(&tracker, ago(2.0)));
  op->tracking_start();
  op->mark_event("started");
  EXPECT_TRUE(tracker.trace_in_flight(*op));
  EXPECT_FALSE(tracker.trace_in_flight(*op));
  EXPECT_EQ(traced_in_flight, 1);
  EXPECT_EQ(op->get_trace_id(), "trace-1");

  ceph::JSONFormatter f;
  tracker.dump_ops_in_flight(&f);
  std::ostringstream ss;
  f.flush(ss);
  EXPECT_EQ(count(ss.str(), "\"trace-1\""), 1u);
}

TEST_F(SlowOpTracing, InFlightOffWithoutThreshold) {
  set_hook();
  TrackedOpRef op(new TestOp(&tracker, ago(2.0)));
  op->tracking_start();
  op->mark_event("started");
  EXPECT_FALSE(tracker.trace_in_flight(*op));
  // once tracing is on, the op is still traced
  tracker.set_trace_threshold_and_rate(1.0, 100);
  EXPECT_TRUE(tracker.trace_in_flight(*op));
  EXPECT_EQ(traced, 1);
}
