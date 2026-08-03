// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * ceph_bench_op_scheduler
 *
 * Isolation micro benchmarks comparing the OSD op schedulers (wpq,
 * mclock_scheduler) on identical synthetic workloads with a simulated
 * device drain rate.  See scheduler_bench.h for the harness.
 *
 * Usage:
 *   ceph_bench_op_scheduler [--rate-mb N] [--secs S] [--csv FILE]
 *                           [--scenario share|latency|recovery|all]
 */

#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#include "global/global_context.h"
#include "global/global_init.h"
#include "common/common_init.h"

#include "scheduler_bench.h"

using namespace scheduler_bench;

namespace {

struct Options {
  double rate_mb = 100.0;
  double secs = 3.0;
  double warmup = 0.5;
  std::string scenario = "all";
  std::string csv_path;
};

const std::vector<op_queue_type_t> kTypes = {
  op_queue_type_t::WeightedPriorityQueue,
  op_queue_type_t::mClockScheduler,
};

std::ofstream csv;

void report_cell(const std::string &scenario, const std::string &cell,
		 op_queue_type_t type,
		 const std::vector<StreamSpec> &specs,
		 const CellResult &r)
{
  for (const auto &s : specs) {
    const auto &sr = r.streams.at(s.name);
    std::printf("  %-16s %-14s %8.1f %7.3f %8.1f %9.1f\n",
		std::string(get_op_queue_type_name(type)).c_str(),
		s.name.c_str(), sr.mbps, sr.share, sr.p50_ms, sr.p99_ms);
    if (csv.is_open()) {
      csv << scenario << ',' << cell << ','
	  << get_op_queue_type_name(type) << ',' << s.name << ','
	  << sr.ops << ',' << sr.bytes << ',' << sr.share << ','
	  << sr.mbps << ',' << sr.p50_ms << ',' << sr.p99_ms << '\n';
    }
  }
}

void run_matrix(const Options &opt, const std::string &scenario,
		const std::string &cell,
		const std::vector<StreamSpec> &specs)
{
  std::printf("--- %s / %s (device %.0f MB/s, %.1fs)\n",
	      scenario.c_str(), cell.c_str(), opt.rate_mb, opt.secs);
  std::printf("  %-16s %-14s %8s %7s %8s %9s\n",
	      "scheduler", "stream", "MB/s", "share", "p50ms", "p99ms");
  for (auto type : kTypes) {
    auto r = run_cell(g_ceph_context, type, specs,
		      opt.rate_mb * 1e6, opt.secs, opt.warmup);
    report_cell(scenario, cell, type, specs, r);
  }
  std::printf("\n");
}

// A1: both streams saturating; the aggressor fans out over K client
// sessions.  Isolation == the victim's share is insensitive to K.
void scenario_share(const Options &opt)
{
  for (unsigned k : {1u, 4u, 16u}) {
    std::vector<StreamSpec> specs = {
      {.name = "victim_rbd", .pool = 1, .first_owner = 1, .num_owners = 1},
      {.name = "aggr_rgw", .pool = 2, .first_owner = 100, .num_owners = k},
    };
    run_matrix(opt, "share", "sessions=" + std::to_string(k), specs);
  }
}

// A2: paced victim (20% of device rate) vs a saturating 8-session
// aggressor.  Isolation == the victim keeps its offered throughput
// with bounded queueing delay.
void scenario_latency(const Options &opt)
{
  const double victim_ops = opt.rate_mb * 1e6 * 0.2 / 65536.0;
  std::vector<StreamSpec> specs = {
    {.name = "victim_rbd", .pool = 1, .first_owner = 1, .num_owners = 1,
     .offered_ops_per_sec = victim_ops},
    {.name = "aggr_rgw", .pool = 2, .first_owner = 100, .num_owners = 8},
  };
  run_matrix(opt, "latency", "victim=20%,sessions=8", specs);
}

// B: saturating client stream vs saturating recovery stream (1M
// chunks, DEGRADED priority encoding).
void scenario_recovery(const Options &opt)
{
  std::vector<StreamSpec> specs = {
    {.name = "client_rbd", .pool = 1, .first_owner = 1, .num_owners = 2},
    {.name = "recovery",
     .klass = SchedulerClass::background_recovery,
     .pool = 9, .first_owner = 500, .num_owners = 1,
     .op_size = 1048576, .priority = 10, .backlog_per_owner = 16},
  };
  run_matrix(opt, "recovery", "client-vs-recovery", specs);
}

} // anonymous namespace

int main(int argc, char **argv)
{
  std::vector<const char*> args(argv, argv + argc);
  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_OSD,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  Options opt;
  for (size_t i = 1; i < args.size(); ++i) {
    auto need_val = [&](const char *flag) -> const char * {
      if (i + 1 >= args.size()) {
	std::fprintf(stderr, "%s requires a value\n", flag);
	exit(1);
      }
      return args[++i];
    };
    if (!std::strcmp(args[i], "--rate-mb")) {
      opt.rate_mb = std::stod(need_val("--rate-mb"));
    } else if (!std::strcmp(args[i], "--secs")) {
      opt.secs = std::stod(need_val("--secs"));
    } else if (!std::strcmp(args[i], "--csv")) {
      opt.csv_path = need_val("--csv");
    } else if (!std::strcmp(args[i], "--scenario")) {
      opt.scenario = need_val("--scenario");
    }
  }
  opt.warmup = std::min(opt.warmup, opt.secs / 4);

  // calibrate mclock's capacity model to the simulated device so we
  // benchmark mclock, not a misconfigured mclock
  g_ceph_context->_conf.set_val(
    "osd_mclock_max_sequential_bandwidth_ssd",
    std::to_string(static_cast<uint64_t>(opt.rate_mb * 1e6)));
  g_ceph_context->_conf.set_val(
    "osd_mclock_max_capacity_iops_ssd",
    std::to_string(opt.rate_mb * 1e6 / 65536.0));

  if (!opt.csv_path.empty()) {
    csv.open(opt.csv_path);
    csv << "scenario,cell,scheduler,stream,ops,bytes,share,mbps,"
	   "p50_ms,p99_ms\n";
  }

  if (opt.scenario == "all" || opt.scenario == "share") {
    scenario_share(opt);
  }
  if (opt.scenario == "all" || opt.scenario == "latency") {
    scenario_latency(opt);
  }
  if (opt.scenario == "all" || opt.scenario == "recovery") {
    scenario_recovery(opt);
  }
  return 0;
}
