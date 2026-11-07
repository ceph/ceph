// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <regex>
#include <sstream>

#include "common/ceph_context.h"
#include "common/Formatter.h"
#include "common/histogram.h"
#include "common/TrackedOp.h"
#include "include/msgr.h"
#include "gtest/gtest.h"

namespace {

class TestOp : public TrackedOp {
public:
  TestOp(OpTracker *tracker, const utime_t& initiated)
    : TrackedOp(tracker, initiated) {}
  void start() {
    tracking_start();
  }
protected:
  void _dump_op_descriptor(std::ostream& stream) const override {
    stream << "test_op";
  }
};

class TrackedOpAge : public ::testing::Test {
protected:
  boost::intrusive_ptr<CephContext> cct{new CephContext(CEPH_ENTITY_TYPE_CLIENT), false};
  OpTracker tracker{cct.get(), true, 1};

  void TearDown() override {
    tracker.on_shutdown();
  }

  // an op the tracker registers after the caller sampled "now", as
  // OpTracker::dump_ops_in_flight() can meet while it walks the shards
  TrackedOpRef start_op(utime_t initiated) {
    auto op = new TestOp(&tracker, initiated);
    TrackedOpRef ref(op);
    op->start();
    return ref;
  }

  std::vector<double> dumped_ages(bool print_only_blocked) {
    std::unique_ptr<ceph::Formatter> f(ceph::Formatter::create("json"));
    tracker.dump_ops_in_flight(f.get(), print_only_blocked);
    std::ostringstream ss;
    f->flush(ss);
    std::vector<double> ages;
    std::regex age_re("\"age\":\\s*([0-9.eE+-]+)");
    auto s = ss.str();
    for (auto it = std::sregex_iterator(s.begin(), s.end(), age_re);
	 it != std::sregex_iterator(); ++it) {
      ages.push_back(std::stod((*it)[1].str()));
    }
    return ages;
  }
};

TEST_F(TrackedOpAge, GetAge)
{
  utime_t now = ceph_clock_now();
  utime_t past = now;
  past -= 5.0;
  utime_t future = now;
  future += 5.0;

  auto old_op = start_op(past);
  auto new_op = start_op(future);

  EXPECT_NEAR(5.0, (double)old_op->get_age(now), 0.001);
  EXPECT_EQ(utime_t(), new_op->get_age(now));
}

TEST_F(TrackedOpAge, DumpOpsInFlight)
{
  utime_t future = ceph_clock_now();
  future += 5.0;
  auto op = start_op(future);

  auto ages = dumped_ages(false);
  ASSERT_EQ(1u, ages.size());
  EXPECT_GE(ages[0], 0.0);
  EXPECT_LT(ages[0], 1.0);
}

TEST_F(TrackedOpAge, DumpBlockedOps)
{
  tracker.set_complaint_and_threshold(30, 30);
  utime_t future = ceph_clock_now();
  future += 5.0;
  auto op = start_op(future);

  // an op that has just started is not blocked
  EXPECT_TRUE(dumped_ages(true).empty());
}

TEST_F(TrackedOpAge, AgeHistogram)
{
  utime_t future = ceph_clock_now();
  future += 5.0;
  auto op = start_op(future);

  pow2_hist_t h;
  tracker.get_age_ms_histogram(&h);
  // a single op of age 0 lands in the first bin
  ASSERT_EQ(1u, h.h.size());
  EXPECT_EQ(1, h.h[0]);
}

} // namespace
