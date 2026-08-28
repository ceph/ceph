// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Shared harness for op-scheduler isolation micro benchmarks.
 *
 * Drives synthetic per-stream workloads through any OpScheduler
 * implementation in-process, pacing dequeues at a simulated device
 * rate so that saturation dynamics (and therefore isolation) are
 * meaningful.  Runs in real time because mclock's dmclock tags are
 * wall-clock based; bfq and wpq are indifferent.
 */

#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/Clock.h"
#include "common/ceph_context.h"
#include "osd/scheduler/BfqScheduler.h"
#include "osd/scheduler/OpScheduler.h"
#include "osd/scheduler/OpSchedulerItem.h"

namespace scheduler_bench {

using namespace ceph::osd::scheduler;

struct MockItem : public PGOpQueueable {
  SchedulerClass klass;

  MockItem(spg_t pgid, SchedulerClass k)
    : PGOpQueueable(pgid), klass(k) {}

  std::ostream &print(std::ostream &rhs) const final { return rhs; }
  std::string print() const final { return std::string(); }
  std::optional<OpRequestRef> maybe_get_op() const final {
    return std::nullopt;
  }
  SchedulerClass get_scheduler_class() const final { return klass; }
  void run(OSD *osd, OSDShard *sdata, PGRef &pg,
	   ThreadPool::TPHandle &handle) final {}
};

struct StreamSpec {
  std::string name;
  SchedulerClass klass = SchedulerClass::client;
  int64_t pool = 1;
  uint64_t first_owner = 1;      ///< owners model client sessions
  unsigned num_owners = 1;
  uint64_t op_size = 65536;      ///< item cost in bytes
  unsigned priority = 63;        ///< osd_client_op_priority default
  double offered_ops_per_sec = 0;  ///< 0 => saturating
  unsigned backlog_per_owner = 64; ///< target queue depth when saturating
};

struct StreamResult {
  uint64_t ops = 0;
  uint64_t bytes = 0;
  double share = 0;    ///< of post-warmup dequeued bytes
  double mbps = 0;
  double p50_ms = 0;
  double p99_ms = 0;
};

struct CellResult {
  std::map<std::string, StreamResult> streams;
  double total_mbps = 0;
};

inline double percentile(std::vector<double> &v, double p)
{
  if (v.empty()) {
    return 0;
  }
  std::sort(v.begin(), v.end());
  return v[std::min(v.size() - 1, static_cast<size_t>(p * v.size()))];
}

/**
 * Run one benchmark cell: a fresh scheduler of the given type, the
 * given streams, a simulated device draining rate_bytes_per_sec, for
 * duration_sec of wall time.  Samples from the first warmup_sec are
 * discarded.
 */
inline CellResult run_cell(
  CephContext *cct,
  op_queue_type_t type,
  const std::vector<StreamSpec> &specs,
  const std::unordered_map<int64_t, bfq_stream_t> &pool_map,
  double rate_bytes_per_sec,
  double duration_sec,
  double warmup_sec)
{
  using steady = std::chrono::steady_clock;

  auto sched = make_scheduler(cct, 0, 1, 0, false /*is_rotational*/,
			      "bluestore", type, 196 /*cutoff*/);
  if (type == op_queue_type_t::BfqScheduler) {
    auto pm = pool_map;
    static_cast<BfqScheduler*>(sched.get())->set_pool_streams(std::move(pm));
  }

  std::unordered_map<uint64_t, size_t> owner_stream;
  std::unordered_map<uint64_t, unsigned> owner_queued;
  for (size_t i = 0; i < specs.size(); ++i) {
    for (unsigned o = 0; o < specs[i].num_owners; ++o) {
      owner_stream[specs[i].first_owner + o] = i;
      owner_queued[specs[i].first_owner + o] = 0;
    }
  }

  std::vector<uint64_t> bytes(specs.size(), 0), ops(specs.size(), 0);
  std::vector<std::vector<double>> lat(specs.size());
  std::vector<double> credit(specs.size(), 0.0);
  std::vector<unsigned> paced_rr(specs.size(), 0);
  epoch_t epoch = 1;

  auto enqueue_one = [&](size_t si, uint64_t owner) {
    const auto &s = specs[si];
    sched->enqueue(OpSchedulerItem(
      std::make_unique<MockItem>(spg_t(pg_t(0, s.pool)), s.klass),
      static_cast<int>(s.op_size), s.priority, ceph_clock_now(), owner,
      epoch++));
    ++owner_queued[owner];
  };

  const auto t0 = steady::now();
  const auto t_warm =
    t0 + std::chrono::duration_cast<steady::duration>(
      std::chrono::duration<double>(warmup_sec));
  const auto t_end =
    t0 + std::chrono::duration_cast<steady::duration>(
      std::chrono::duration<double>(duration_sec));
  auto last = t0;
  double tokens = 0;
  const double burst_cap = rate_bytes_per_sec / 4;

  while (true) {
    const auto now = steady::now();
    if (now >= t_end) {
      break;
    }
    const double dt = std::chrono::duration<double>(now - last).count();
    last = now;
    tokens = std::min(tokens + rate_bytes_per_sec * dt, burst_cap);

    // load generators
    for (size_t i = 0; i < specs.size(); ++i) {
      const auto &s = specs[i];
      if (s.offered_ops_per_sec > 0) {
	credit[i] = std::min(credit[i] + s.offered_ops_per_sec * dt, 1000.0);
	while (credit[i] >= 1.0) {
	  enqueue_one(i, s.first_owner + (paced_rr[i]++ % s.num_owners));
	  credit[i] -= 1.0;
	}
      } else {
	for (unsigned o = 0; o < s.num_owners; ++o) {
	  const uint64_t owner = s.first_owner + o;
	  while (owner_queued[owner] < s.backlog_per_owner) {
	    enqueue_one(i, owner);
	  }
	}
      }
    }

    // simulated device drain
    bool progressed = false;
    while (tokens > 0 && !sched->empty()) {
      WorkItem wi = sched->dequeue();
      if (std::holds_alternative<double>(wi)) {
	// mclock says "not ready until later"; let wall time advance
	break;
      }
      auto &item = std::get<OpSchedulerItem>(wi);
      const uint64_t owner = item.get_owner();
      const size_t si = owner_stream.at(owner);
      if (owner_queued[owner] > 0) {
	--owner_queued[owner];
      }
      tokens -= static_cast<double>(item.get_cost());
      progressed = true;
      if (now >= t_warm) {
	bytes[si] += item.get_cost();
	++ops[si];
	const utime_t sojourn = ceph_clock_now() - item.get_start_time();
	lat[si].push_back(sojourn.to_nsec() / 1e6);
      }
    }
    if (!progressed) {
      std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
  }

  CellResult result;
  const double measured_sec = duration_sec - warmup_sec;
  uint64_t total_bytes = 0;
  for (auto b : bytes) {
    total_bytes += b;
  }
  for (size_t i = 0; i < specs.size(); ++i) {
    StreamResult r;
    r.ops = ops[i];
    r.bytes = bytes[i];
    r.share = total_bytes ? static_cast<double>(bytes[i]) / total_bytes : 0;
    r.mbps = bytes[i] / 1e6 / measured_sec;
    r.p50_ms = percentile(lat[i], 0.50);
    r.p99_ms = percentile(lat[i], 0.99);
    result.streams[specs[i].name] = r;
  }
  result.total_mbps = total_bytes / 1e6 / measured_sec;
  return result;
}

} // namespace scheduler_bench
