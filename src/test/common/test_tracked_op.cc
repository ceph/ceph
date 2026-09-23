// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <atomic>
#include <chrono>
#include <sstream>
#include <thread>

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

  void SetUp() override {
    tracker.set_history_size_and_duration(100, 600);
  }
  void TearDown() override {
    tracker.on_shutdown();
  }

  void set_hook() {
    tracker.set_slow_op_tracer([this](TrackedOp&) {
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
