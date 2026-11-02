// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "common/TrackedOp.h"
#include "common/Clock.h"
#include "global/global_context.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

namespace {

class TestOp : public TrackedOp {
public:
  using Ref = boost::intrusive_ptr<TestOp>;
  TestOp(OpTracker* tracker, const utime_t& initiated)
    : TrackedOp(tracker, initiated) {}

protected:
  void _dump_op_descriptor(std::ostream& stream) const override {
    stream << "test_op(" << seq << ")";
  }
};

constexpr uint32_t num_shards = 4;
constexpr float complaint_time = 30;

// Ops get seq 1, 2, 3, ... and go to shard seq % num_shards, so the
// op with seq == num_shards lands in shard 0, which is gathered first.
// Make that op young and every other op slow.
class OpTrackerSlowOps : public ::testing::Test {
protected:
  OpTracker tracker{g_ceph_context, true, num_shards};
  std::vector<TestOp::Ref> ops;
  const utime_t now = ceph_clock_now();

  void SetUp() override {
    tracker.set_complaint_and_threshold(complaint_time, 100);
    for (uint32_t i = 1; i <= num_shards; ++i) {
      utime_t initiated = now;
      if (i != num_shards) {
        initiated -= 60 + i;
      }
      ops.emplace_back(new TestOp(&tracker, initiated));
      ops.back()->tracking_start();
    }
  }

  void TearDown() override {
    ops.clear();
    tracker.on_shutdown();
  }
};

} // anonymous namespace

TEST_F(OpTrackerSlowOps, VisitOldestFirst)
{
  utime_t oldest_secs;
  std::vector<utime_t> seen;
  ASSERT_TRUE(tracker.visit_ops_in_flight(&oldest_secs, [&](TrackedOp& op) {
    seen.push_back(op.get_initiated());
    return true;
  }));
  ASSERT_EQ(num_shards, seen.size());
  EXPECT_TRUE(std::is_sorted(seen.begin(), seen.end()));
  EXPECT_GE(oldest_secs, utime_t(60 + num_shards - 1, 0));
}

TEST_F(OpTrackerSlowOps, CountAcrossShards)
{
  utime_t oldest_secs;
  int slow = 0;
  int warned = 0;
  ASSERT_TRUE(tracker.with_slow_ops_in_flight(
    &oldest_secs, &slow, &warned, [](TrackedOp&) {}));
  EXPECT_EQ(int(num_shards - 1), slow);
  EXPECT_EQ(int(num_shards - 1), warned);
}

TEST_F(OpTrackerSlowOps, CheckOpsInFlight)
{
  std::string summary;
  std::vector<std::string> warnings;
  int slow = 0;
  ASSERT_TRUE(tracker.check_ops_in_flight(&summary, warnings, &slow));
  EXPECT_EQ(int(num_shards - 1), slow);
  EXPECT_EQ(size_t(num_shards - 1), warnings.size());
}
