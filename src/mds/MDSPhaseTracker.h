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
#include <mutex>

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
 * mds_lock utilization.  Direct wait/hold accounting for mds_lock itself
 * comes from lockstat rather than an in-mutex counter.
 */

enum {
  l_mdsp_first = 2700,

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

  /// formerly published fair_mutex wait/hold deltas; lockstat supersedes this
  void update_lock_stats() {}

  /// human readable breakdown, for the `dump phase times` asok command
  void dump(ceph::Formatter *f) const;

  /*
   * An RAII scope charging exclusive wall time to `phase`.  Nesting is
   * tracked per thread, so a Timer may be used from any thread; every
   * instrumented scope but l_mdsp_heap_release holds mds_lock.
   *
   * A scope may straddle a set_enabled() cycle, which resets the counters
   * from another thread; each Timer therefore remembers the generation it
   * started in and its destructor drops the sample if a reset intervened,
   * rather than charging time from before the reset into counters that are
   * supposed to cover only the period since it.
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
      /* Read the generation before the clock, so that a reset() racing with
       * this constructor either has already published the generation we
       * observe -- and therefore zeroed the counters before we started
       * timing -- or publishes a newer one, and the destructor drops us. */
      generation = this->tracker->generation.load(std::memory_order_acquire);
      parent = tl_current;
      tl_current = this;
      start = ceph::mono_clock::now();
    }

    ~Timer() {
      if (!tracker) {
        return;
      }
      const auto total = ceph::mono_clock::now() - start;
      /* Unconditionally, and before the generation check: a parent that does
       * survive must not be charged for a child that was dropped. */
      if (parent) {
        parent->children += total;
      }
      tl_current = parent;
      if (generation != tracker->generation.load(std::memory_order_acquire)) {
        return;
      }
      tracker->account(phase, total > children ? total - children
                                               : ceph::timespan::zero());
    }

    Timer(const Timer&) = delete;
    Timer& operator=(const Timer&) = delete;

  private:
    static thread_local Timer *tl_current;

    MDSPhaseTracker *tracker;
    int phase;
    uint64_t generation = 0;
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

  /* Bumped by every reset(); see MDSPhaseTracker::Timer. */
  std::atomic<uint64_t> generation = 0;

  /* Protects since_ns and the logger snapshot taken by dump()/reset(). */
  mutable std::mutex stats_mutex;

  /// when tracking started; dump() reports everything relative to it
  uint64_t since_ns = 0;
};

#endif // CEPH_MDS_PHASE_TRACKER_H
