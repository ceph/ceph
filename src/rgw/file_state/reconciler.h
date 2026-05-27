// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

// Storage-neutral reconciler for the s3files control plane.
//
// Composition:
//   Store       — uniform read of FS / AP / MT records
//   ChangeFeed  — wakeup signal when state mutates
//   GaneshaSink — applies the desired export set
//
// The reconciler subscribes to the change feed and runs a
// worker thread. Whenever the feed signals OR a safety-net
// timer fires, the worker:
//
//   1. Drains any pending wake events into a single iteration.
//   2. Calls compose_exports() to build the desired set.
//   3. Calls sink.apply(desired).
//
// Multiple wakes that arrive while a reconcile is in flight
// coalesce into a single follow-up reconcile (a "dirty" flag);
// no work is dropped. The sink sees idempotent apply() calls
// and is responsible for emitting only deltas if surgical
// mutations matter (e.g. DbusGaneshaSink) — RecordingGaneshaSink
// just logs them.
//
// Reliability comes from the safety-net timer, NOT from the
// feed. Both RADOS watch/notify and FDB watches can drop events
// (network blips, watcher reconnect, expired session); the timer
// re-runs the cycle every `safety_net_interval` regardless of
// feed activity, so a missed notify causes at-most-N-seconds of
// staleness rather than permanent divergence.

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

#include "change_feed.h"
#include "ganesha_sink.h"
#include "store.h"

namespace rgw::file_state {

struct ReconcilerConfig {
  // Safety-net cadence — the reconciler runs at least this
  // often regardless of feed activity. Tunes the worst-case
  // staleness window. 30s is the typical default; tests can
  // crank this down for fast assertions.
  std::chrono::milliseconds safety_net_interval{30000};
};

class Reconciler {
 public:
  // The reconciler does not take ownership of `store`, `feed`,
  // `sink`, or `bootstrap` — the caller must guarantee they
  // outlive the Reconciler instance.  `bootstrap` resolves an
  // FS owner account-id into the credentials FSAL_RGW will use
  // to call sts:AssumeRole at create_export time.
  Reconciler(Store& store, ChangeFeed& feed, GaneshaSink& sink,
             BootstrapResolver& bootstrap,
             ReconcilerConfig cfg = {});
  ~Reconciler();

  Reconciler(const Reconciler&) = delete;
  Reconciler& operator=(const Reconciler&) = delete;

  // Start the worker thread. The reconciler runs an immediate
  // initial reconcile to converge against any pre-existing
  // state, then enters its wake loop.
  //
  // Safe to call only once. Calling again before stop() is a
  // logic error (asserted in debug builds).
  void start();

  // Stop the worker thread, blocking until it exits. Idempotent
  // — calling stop() on an already-stopped or never-started
  // reconciler is a no-op.
  void stop();

  // Force one synchronous reconcile pass. Bypasses the worker
  // thread; runs on the caller's thread. Intended for unit
  // tests where deterministic, time-independent assertions are
  // preferable to event-driven ones.
  void reconcile_once();

 private:
  void worker_loop();

  Store& store_;
  ChangeFeed& feed_;
  GaneshaSink& sink_;
  BootstrapResolver& bootstrap_;
  ReconcilerConfig cfg_;

  ChangeFeed::Handle sub_handle_{0};

  // Synchronization between the producer (feed callback) and
  // the worker thread. `dirty_` is the wakeup flag; `cv_` is
  // how the worker sleeps. `stop_` signals shutdown.
  std::mutex mu_;
  std::condition_variable cv_;
  bool dirty_{false};
  bool stop_{false};
  bool running_{false};

  std::thread worker_;
};

}  // namespace rgw::file_state
