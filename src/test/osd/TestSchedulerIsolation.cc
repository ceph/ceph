// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Sanity checks for the scheduler_bench.h harness, run against the
 * in-tree op schedulers, plus isolation assertions for the bfq op
 * scheduler.  The harness paces in wall time, which is unfit for
 * shared CI executors, so these tests SKIP unless
 * CEPH_TEST_SCHEDULER_ISOLATION is set in the environment.  Bounds
 * are deliberately loose even then.  The full comparative study lives
 * in ceph_bench_op_scheduler.
 */

#include <cstdlib>

#include "gtest/gtest.h"

#include "global/global_context.h"
#include "global/global_init.h"
#include "common/common_init.h"

#include "scheduler_bench.h"

using namespace scheduler_bench;

int main(int argc, char **argv) {
  std::vector<const char*> args(argv, argv+argc);
  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_OSD,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  // calibrate mclock's capacity model to the simulated device rate
  // used below, as ceph_bench_op_scheduler does
  g_ceph_context->_conf.set_val(
    "osd_mclock_max_sequential_bandwidth_ssd", "100000000");
  g_ceph_context->_conf.set_val(
    "osd_mclock_max_capacity_iops_ssd",
    std::to_string(100e6 / 65536.0));

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

namespace {

constexpr double kRate = 100e6;  // simulated device, bytes/sec

const std::unordered_map<int64_t, bfq_stream_t> kPoolMap = {
  {1, bfq_stream_t::client_block},
  {2, bfq_stream_t::client_object},
};

const std::vector<op_queue_type_t> kTypes = {
  op_queue_type_t::WeightedPriorityQueue,
  op_queue_type_t::mClockScheduler,
  op_queue_type_t::BfqScheduler,
};

}

// wall-clock pacing is unreliable on shared CI executors; opt in
#define REQUIRE_ISOLATION_ENV()						\
  do {									\
    if (!std::getenv("CEPH_TEST_SCHEDULER_ISOLATION")) {		\
      GTEST_SKIP()							\
	<< "set CEPH_TEST_SCHEDULER_ISOLATION=1 to run "		\
	   "wall-clock isolation checks";				\
    }									\
  } while (0)

// A single backlogged stream must be served at ~the simulated device
// rate: the harness pacing is intact and the scheduler under test is
// work conserving.
TEST(SchedulerBenchSmoke, WorkConserving) {
  REQUIRE_ISOLATION_ENV();
  for (auto type : kTypes) {
    std::vector<StreamSpec> specs = {
      {.name = "only", .pool = 1, .first_owner = 1, .num_owners = 1},
    };
    auto r = run_cell(g_ceph_context, type, specs, kPoolMap, kRate, 2.0, 0.5);
    EXPECT_GT(r.total_mbps, 50.0)
      << get_op_queue_type_name(type) << " under-served the device";
    EXPECT_LT(r.total_mbps, 140.0)
      << get_op_queue_type_name(type) << " overran the simulated device";
  }
}

// Two identical backlogged single-session streams must both make
// sustained progress; no scheduler may starve one outright.
TEST(SchedulerBenchSmoke, EqualStreamsBothServed) {
  REQUIRE_ISOLATION_ENV();
  for (auto type : kTypes) {
    std::vector<StreamSpec> specs = {
      {.name = "a", .pool = 1, .first_owner = 1, .num_owners = 1},
      {.name = "b", .pool = 2, .first_owner = 100, .num_owners = 1},
    };
    auto r = run_cell(g_ceph_context, type, specs, kPoolMap, kRate, 2.0, 0.5);
    EXPECT_GT(r.streams.at("a").share, 0.2)
      << get_op_queue_type_name(type) << " starved stream a";
    EXPECT_GT(r.streams.at("b").share, 0.2)
      << get_op_queue_type_name(type) << " starved stream b";
  }
}

// A backlogged rbd-pool victim must hold ~its weight share (50% at
// equal weights) even when the rgw-pool aggressor fans out over 16
// client sessions.  This is the property wpq (per-owner round robin)
// and mclock (single dmclock client for all client ops) structurally
// dilute with session count.
TEST(BfqIsolation, ShareUnderSessionScaling) {
  REQUIRE_ISOLATION_ENV();
  std::vector<StreamSpec> specs = {
    {.name = "victim", .pool = 1, .first_owner = 1, .num_owners = 1},
    {.name = "aggr", .pool = 2, .first_owner = 100, .num_owners = 16},
  };
  auto r = run_cell(g_ceph_context, op_queue_type_t::BfqScheduler,
		    specs, kPoolMap, kRate, 2.0, 0.5);
  const double share = r.streams.at("victim").share;
  EXPECT_GT(share, 0.35) << "victim diluted by aggressor sessions";
  EXPECT_LT(share, 0.65) << "victim over-served";
}

// A paced victim (20% of device rate, under its fair share) must keep
// its offered throughput with queueing delay bounded by budget
// rotation, despite a saturating multi-session aggressor.
TEST(BfqIsolation, PacedVictimServiced) {
  REQUIRE_ISOLATION_ENV();
  std::vector<StreamSpec> specs = {
    {.name = "victim", .pool = 1, .first_owner = 1, .num_owners = 1,
     .offered_ops_per_sec = kRate * 0.2 / 65536.0},
    {.name = "aggr", .pool = 2, .first_owner = 100, .num_owners = 8},
  };
  auto r = run_cell(g_ceph_context, op_queue_type_t::BfqScheduler,
		    specs, kPoolMap, kRate, 2.0, 0.5);
  const auto &victim = r.streams.at("victim");
  // offered 20 MB/s; allow generous scheduling/pacing slack
  EXPECT_GT(victim.mbps, 12.0) << "victim throughput collapsed";
  EXPECT_LT(victim.p99_ms, 500.0) << "victim latency unbounded";
}
