// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Clyso Technologies Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "MDSPhaseTracker.h"

#include <algorithm>
#include <chrono>
#include <functional>
#include <iterator>
#include <tuple>
#include <utility>
#include <vector>

#include "common/Formatter.h"
#include "common/ceph_context.h"
#include "common/debug.h"
#include "common/perf_counters.h"
#include "common/perf_counters_collection.h"
#include "include/ceph_assert.h"
#include "include/ceph_fs.h" // for CEPH_MSG_CLIENT_*
#include "mdstypes.h"        // for MDS_PORT_*
#include "msg/Message.h"     // for MSG_MDS_*

#define dout_context cct
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds.phase_tracker "

thread_local MDSPhaseTracker::Timer *MDSPhaseTracker::Timer::tl_current = nullptr;

namespace {

/* Indexed by (counter id - l_mdsp_phase_first); kept in the same order as the
 * enum so that dump() can name a phase without a second switch. */
const char *phase_names[] = {
  "client_request",
  "client_caps",
  "client_session",
  "peer_request",
  "cache_message",
  "migrator_message",
  "locker_message",
  "heartbeat_message",
  "table_message",
  "quiesce_message",
  "scrub_message",
  "other_message",
  "io_completion",
  "finished_contexts",
  "tick",
  "locker_tick",
  "balancer_tick",
  "cache_memory",
  "cache_trim",
  "client_leases",
  "client_recall",
  "heap_release",
  "log_trim",
};
static_assert(static_cast<int>(std::size(phase_names)) ==
                MDSPhaseTracker::num_phases,
              "phase_names must cover every l_mdsp_* phase counter");

double to_seconds(uint64_t ns)
{
  return static_cast<double>(ns) / 1e9;
}

uint64_t now_ns()
{
  return static_cast<uint64_t>(
    ceph::mono_clock::now().time_since_epoch().count());
}

} // anonymous namespace

MDSPhaseTracker::MDSPhaseTracker(CephContext *cct, ceph::fair_mutex& mds_lock)
  : cct(cct), mds_lock(mds_lock)
{
  PerfCountersBuilder plb(cct, "mds_phase", l_mdsp_first, l_mdsp_last);

  plb.set_prio_default(PerfCountersBuilder::PRIO_USEFUL);
  plb.add_time(l_mdsp_lock_wait, "lock_wait",
               "Time spent waiting to acquire mds_lock");
  plb.add_time(l_mdsp_lock_held, "lock_held",
               "Time mds_lock was held; over wall time, rank utilization");
  plb.add_u64_counter(l_mdsp_lock_acquisitions, "lock_acquisitions",
                      "mds_lock acquisitions");

  /* Every phase is a time_avg, so that "sum" is the total time charged to the
   * phase and "avgcount" is the number of times it was entered. */
  plb.add_time_avg(l_mdsp_client_request, "client_request",
                   "Time handling client requests");
  plb.add_time_avg(l_mdsp_client_caps, "client_caps",
                   "Time handling client cap messages");
  plb.add_time_avg(l_mdsp_client_session, "client_session",
                   "Time handling client session messages");
  plb.add_time_avg(l_mdsp_peer_request, "peer_request",
                   "Time handling peer (slave) requests");
  plb.add_time_avg(l_mdsp_cache_message, "cache_message",
                   "Time handling MDS cache messages (discover, resolve, ...)");
  plb.add_time_avg(l_mdsp_migrator_message, "migrator_message",
                   "Time handling subtree import/export messages");
  plb.add_time_avg(l_mdsp_locker_message, "locker_message",
                   "Time handling inter-MDS lock messages");
  plb.add_time_avg(l_mdsp_heartbeat_message, "heartbeat_message",
                   "Time handling MDS load heartbeats");
  plb.add_time_avg(l_mdsp_table_message, "table_message",
                   "Time handling MDS table messages");
  plb.add_time_avg(l_mdsp_quiesce_message, "quiesce_message",
                   "Time handling quiesce db messages");
  plb.add_time_avg(l_mdsp_scrub_message, "scrub_message",
                   "Time handling scrub messages");
  plb.add_time_avg(l_mdsp_other_message, "other_message",
                   "Time handling other messages");
  plb.add_time_avg(l_mdsp_io_completion, "io_completion",
                   "Time completing journal and object IO under mds_lock");
  plb.add_time_avg(l_mdsp_finished_contexts, "finished_contexts",
                   "Time completing queued contexts (waiters unblocked by an "
                   "IO completion)");
  plb.add_time_avg(l_mdsp_tick, "tick",
                   "Time in the periodic tick, excluding the phases below");
  plb.add_time_avg(l_mdsp_locker_tick, "locker_tick",
                   "Time in periodic locker upkeep (cap revocation, idle "
                   "sessions)");
  plb.add_time_avg(l_mdsp_balancer_tick, "balancer_tick",
                   "Time in periodic balancer upkeep");
  plb.add_time_avg(l_mdsp_cache_memory, "cache_memory",
                   "Time sampling memory usage");
  plb.add_time_avg(l_mdsp_cache_trim, "cache_trim",
                   "Time trimming the metadata cache");
  plb.add_time_avg(l_mdsp_client_leases, "client_leases",
                   "Time trimming client leases");
  plb.add_time_avg(l_mdsp_client_recall, "client_recall",
                   "Time asking clients to release caps");
  plb.add_time_avg(l_mdsp_heap_release, "heap_release",
                   "Time releasing free heap memory (not under mds_lock)");
  plb.add_time_avg(l_mdsp_log_trim, "log_trim",
                   "Time trimming the MDS log");

  logger = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);
}

MDSPhaseTracker::~MDSPhaseTracker()
{
  if (logger) {
    cct->get_perfcounters_collection()->remove(logger);
    delete logger;
    logger = nullptr;
  }
}

void MDSPhaseTracker::set_enabled(bool enable)
{
  if (enable == is_enabled()) {
    return;
  }
  dout(5) << (enable ? "enabling" : "disabling") << " mds phase tracking"
          << dendl;
  if (enable) {
    reset();
  }
  /* Enable the mutex accounting first and disable it last, so that the
   * published lock stats never cover a period the phase counters do not. */
  if (enable) {
    mds_lock.set_track_stats(true);
    enabled.store(true, std::memory_order_relaxed);
  } else {
    enabled.store(false, std::memory_order_relaxed);
    mds_lock.set_track_stats(false);
  }
}

void MDSPhaseTracker::reset()
{
  /* Only ever called with tracking disabled, so no Timer is concurrently
   * incrementing the counters we are about to zero -- except an in-flight
   * l_mdsp_heap_release, the one phase that does not run under mds_lock.
   * The counters are atomic, so the worst case is one lost sample. */
  logger->reset();

  const auto stats = mds_lock.get_stats();
  last_lock_stats = stats;
  baseline_acquisitions.store(stats.acquisitions, std::memory_order_relaxed);
  baseline_wait_ns.store(stats.wait_ns, std::memory_order_relaxed);
  baseline_held_ns.store(stats.held_ns, std::memory_order_relaxed);
  since_ns.store(now_ns(), std::memory_order_relaxed);
}

int MDSPhaseTracker::phase_for_message(int message_type)
{
  switch (message_type & 0xff00) {
  case MDS_PORT_CACHE:
    return l_mdsp_cache_message;
  case MDS_PORT_MIGRATOR:
    return l_mdsp_migrator_message;
  default:
    break;
  }

  switch (message_type) {
  case CEPH_MSG_CLIENT_REQUEST:
  case CEPH_MSG_CLIENT_REPLY:
    return l_mdsp_client_request;
  case CEPH_MSG_CLIENT_CAPS:
  case CEPH_MSG_CLIENT_CAPRELEASE:
  case CEPH_MSG_CLIENT_LEASE:
    return l_mdsp_client_caps;
  case CEPH_MSG_CLIENT_SESSION:
  case CEPH_MSG_CLIENT_RECONNECT:
  case CEPH_MSG_CLIENT_RECLAIM:
    return l_mdsp_client_session;
  case MSG_MDS_PEER_REQUEST:
    return l_mdsp_peer_request;
  case MSG_MDS_LOCK:
  case MSG_MDS_INODEFILECAPS:
    return l_mdsp_locker_message;
  case MSG_MDS_HEARTBEAT:
    return l_mdsp_heartbeat_message;
  case MSG_MDS_TABLE_REQUEST:
    return l_mdsp_table_message;
  case MSG_MDS_QUIESCE_DB_LISTING:
  case MSG_MDS_QUIESCE_DB_ACK:
    return l_mdsp_quiesce_message;
  case MSG_MDS_SCRUB:
  case MSG_MDS_SCRUB_STATS:
    return l_mdsp_scrub_message;
  default:
    return l_mdsp_other_message;
  }
}

void MDSPhaseTracker::account(int phase, ceph::timespan exclusive)
{
  ceph_assert(phase >= l_mdsp_phase_first && phase < l_mdsp_phase_last);

  /* The counter is the only bookkeeping: a time_avg accumulates both the sum
   * and the entry count, and get_tavg_ns() reads them back for dump().  This
   * is the hottest path in the tracker -- it runs once per dispatched message
   * -- so it does no work the perf counter does not already do. */
  logger->tinc(phase, exclusive);
}

void MDSPhaseTracker::update_lock_stats()
{
  if (!is_enabled()) {
    return;
  }

  /* fair_mutex keeps running totals; publish the delta since the last call so
   * that the perf counters behave like every other Ceph counter. */
  const auto now = mds_lock.get_stats();
  logger->tinc(l_mdsp_lock_wait, std::chrono::nanoseconds(
                 static_cast<int64_t>(now.wait_ns - last_lock_stats.wait_ns)));
  logger->tinc(l_mdsp_lock_held, std::chrono::nanoseconds(
                 static_cast<int64_t>(now.held_ns - last_lock_stats.held_ns)));
  logger->inc(l_mdsp_lock_acquisitions,
              now.acquisitions - last_lock_stats.acquisitions);
  last_lock_stats = now;
}

void MDSPhaseTracker::dump(ceph::Formatter *f) const
{
  f->open_object_section("phase_times");
  f->dump_bool("enabled", is_enabled());

  if (!is_enabled()) {
    f->dump_string("note", "set mds_enable_phase_tracker=true to enable");
    f->close_section();
    return;
  }

  /* Everything below is reported for the period since tracking was enabled,
   * which is also the period the perf counters cover. */
  const uint64_t elapsed_ns =
    now_ns() - since_ns.load(std::memory_order_relaxed);
  const double elapsed = to_seconds(elapsed_ns);
  f->dump_float("elapsed_sec", elapsed);

  const auto stats = mds_lock.get_stats();
  const uint64_t lock_held_ns =
    stats.held_ns - baseline_held_ns.load(std::memory_order_relaxed);
  f->open_object_section("mds_lock");
  f->dump_unsigned("acquisitions", stats.acquisitions -
                   baseline_acquisitions.load(std::memory_order_relaxed));
  f->dump_float("wait_sec", to_seconds(
                  stats.wait_ns -
                  baseline_wait_ns.load(std::memory_order_relaxed)));
  f->dump_float("held_sec", to_seconds(lock_held_ns));
  /* The single most actionable number here: a rank whose lock is busy for
   * most of the wall clock cannot go any faster without being split up. */
  f->dump_float("utilization",
                elapsed_ns ? double(lock_held_ns) / elapsed_ns : 0);
  f->close_section();

  /* Report the busiest phase first: the answer to "what is this rank doing?"
   * should be the first line of the output. */
  std::vector<std::tuple<uint64_t, uint64_t, int>> by_time;
  by_time.reserve(num_phases);
  uint64_t total_ns = 0;
  for (int i = 0; i < num_phases; ++i) {
    const auto [ns, count] = logger->get_tavg_ns(l_mdsp_phase_first + i);
    total_ns += ns;
    by_time.emplace_back(ns, count, i);
  }
  std::sort(by_time.begin(), by_time.end(), std::greater<>());

  f->dump_float("accounted_sec", to_seconds(total_ns));
  f->open_array_section("phases");
  for (const auto& [ns, count, i] : by_time) {
    f->open_object_section("phase");
    f->dump_string("phase", phase_names[i]);
    f->dump_float("total_sec", to_seconds(ns));
    f->dump_unsigned("count", count);
    f->dump_float("mean_ms", count ? to_seconds(ns) * 1000 / count : 0);
    f->dump_float("pct_of_elapsed", elapsed_ns ? 100.0 * ns / elapsed_ns : 0);
    f->dump_float("pct_of_accounted", total_ns ? 100.0 * ns / total_ns : 0);
    f->close_section();
  }
  f->close_section();

  f->close_section();
}
