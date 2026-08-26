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

#ifndef CEPH_MDS_PHASE_TRACKER_H
#define CEPH_MDS_PHASE_TRACKER_H

#include <atomic>
#include <cstdint>

#include "common/ceph_time.h"
#include "common/fair_mutex.h"

#include "include/common_fwd.h"

namespace ceph { class Formatter; }

/*
 * Where does an MDS rank's time go?
 *
 * Nearly everything a rank does is serialized by mds_lock: messages
 * dispatched by the messenger threads, contexts completed by the finisher,
 * the periodic MDSRankDispatcher::tick(), MDCache's cache-trim thread and
 * MDLog's log-trim thread all take it.  The lock, rather than any one
 * thread, is therefore the resource that saturates first, and "where did
 * mds_lock time go?" is the question that has to be answered before
 * deciding whether to scale a rank up (more cache memory) or out (more
 * ranks).
 *
 * MDSPhaseTracker answers it by charging *exclusive* wall time to a phase.
 * Each Timer is an RAII scope; timers nest, and time spent in a nested
 * timer is subtracted from its parent.  So mds_phase.cache_trim is the time
 * actually spent trimming and not the time spent trimming plus everything
 * the trim happened to dispatch into.
 *
 * Divided by wall time, the phase totals give the share of a rank spent on
 * each kind of work; summed and divided by wall time they approximate
 * mds_lock utilization, which is also reported directly (lock_held) from
 * the mutex itself.
 */

enum {
  l_mdsp_first = 2700,

  /* mds_lock itself; see ceph::fair_mutex::stats */
  l_mdsp_lock_wait,
  l_mdsp_lock_held,
  l_mdsp_lock_acquisitions,

  /* message dispatch, by message class */
  l_mdsp_phase_first,
  l_mdsp_client_request = l_mdsp_phase_first,
  l_mdsp_client_caps,
  l_mdsp_client_session,
  l_mdsp_peer_request,
  l_mdsp_cache_message,
  l_mdsp_migrator_message,
  l_mdsp_locker_message,
  l_mdsp_heartbeat_message,
  l_mdsp_table_message,
  l_mdsp_quiesce_message,
  l_mdsp_scrub_message,
  l_mdsp_other_message,

  /* completions run under mds_lock outside message dispatch */
  l_mdsp_io_completion,
  l_mdsp_finished_contexts,

  /* the periodic tick, and the more expensive things it calls */
  l_mdsp_tick,
  l_mdsp_locker_tick,
  l_mdsp_balancer_tick,

  /* the cache-trim upkeep thread */
  l_mdsp_cache_memory,
  l_mdsp_cache_trim,
  l_mdsp_client_leases,
  l_mdsp_client_recall,
  l_mdsp_heap_release,

  /* the log-trim upkeep thread */
  l_mdsp_log_trim,

  l_mdsp_phase_last,
  l_mdsp_last = l_mdsp_phase_last,
};

class MDSPhaseTracker {
public:
  static constexpr int num_phases = l_mdsp_phase_last - l_mdsp_phase_first;

  MDSPhaseTracker(CephContext *cct, ceph::fair_mutex& mds_lock);
  ~MDSPhaseTracker();

  MDSPhaseTracker(const MDSPhaseTracker&) = delete;
  MDSPhaseTracker& operator=(const MDSPhaseTracker&) = delete;

  /**
   * Start or stop accounting.  Enabling resets the counters and the wall
   * clock they are measured against, so that a dump taken afterwards
   * describes only the period since it was enabled.
   */
  void set_enabled(bool enable);
  bool is_enabled() const {
    return enabled.load(std::memory_order_relaxed);
  }

  /// map a message type to the phase its handling should be charged to
  static int phase_for_message(int message_type);

  /// publish mds_lock wait/hold time; called periodically from tick()
  void update_lock_stats();

  /// human readable breakdown, for the `dump phase times` asok command
  void dump(ceph::Formatter *f) const;

  /*
   * An RAII scope charging exclusive wall time to `phase`.  Nesting is
   * tracked per thread, so a Timer may be used from any thread; every
   * instrumented scope but l_mdsp_heap_release holds mds_lock.
   */
  class Timer {
  public:
    Timer(MDSPhaseTracker *tracker, int phase)
      : tracker(tracker && tracker->is_enabled() ? tracker : nullptr),
        phase(phase)
    {
      if (!this->tracker) {
        return;
      }
      parent = tl_current;
      tl_current = this;
      start = ceph::mono_clock::now();
    }

    ~Timer() {
      if (!tracker) {
        return;
      }
      const auto total = ceph::mono_clock::now() - start;
      tracker->account(phase, total > children ? total - children
                                               : ceph::timespan::zero());
      if (parent) {
        parent->children += total;
      }
      tl_current = parent;
    }

    Timer(const Timer&) = delete;
    Timer& operator=(const Timer&) = delete;

  private:
    static thread_local Timer *tl_current;

    MDSPhaseTracker *tracker;
    int phase;
    Timer *parent = nullptr;
    ceph::mono_time start;
    ceph::timespan children = ceph::timespan::zero();
  };

private:
  void account(int phase, ceph::timespan exclusive);
  void reset();

  CephContext *cct;
  ceph::fair_mutex& mds_lock;
  PerfCounters *logger = nullptr;

  std::atomic<bool> enabled = false;

  /* When tracking started; dump() reports everything relative to it.  Both
   * this and the baselines below are written only by reset() but read by
   * dump(), which runs without mds_lock, hence the atomics. */
  std::atomic<uint64_t> since_ns = 0;

  /* mds_lock totals as of the last reset() */
  std::atomic<uint64_t> baseline_acquisitions = 0;
  std::atomic<uint64_t> baseline_wait_ns = 0;
  std::atomic<uint64_t> baseline_held_ns = 0;

  /* the last mds_lock totals published, so that deltas can be tinc()'d;
   * only touched by update_lock_stats(), which runs under mds_lock */
  ceph::fair_mutex::stats last_lock_stats;
};

#endif // CEPH_MDS_PHASE_TRACKER_H
