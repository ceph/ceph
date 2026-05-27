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

#include "reconciler.h"

#include <cassert>

#include "desired_export.h"

namespace rgw::file_state {

Reconciler::Reconciler(Store& store, ChangeFeed& feed, GaneshaSink& sink,
                        BootstrapResolver& bootstrap, ReconcilerConfig cfg)
    : store_(store), feed_(feed), sink_(sink), bootstrap_(bootstrap),
      cfg_(cfg) {}

Reconciler::~Reconciler() {
  stop();
}

void Reconciler::start() {
  {
    std::lock_guard lock(mu_);
    assert(!running_ && "Reconciler::start() called twice");
    running_ = true;
    stop_ = false;
    // Mark dirty so the initial loop iteration runs immediately
    // — the reconciler should always converge against any
    // pre-existing state on startup, not wait for the first
    // change.
    dirty_ = true;
  }

  // Subscribe BEFORE the worker starts so we never miss a fire
  // that happens during startup. The callback itself is cheap
  // — just toggles `dirty_` and notifies the cv.
  sub_handle_ = feed_.subscribe([this]{
    {
      std::lock_guard lock(mu_);
      dirty_ = true;
    }
    cv_.notify_one();
  });

  worker_ = std::thread([this]{ worker_loop(); });
}

void Reconciler::stop() {
  {
    std::lock_guard lock(mu_);
    if (!running_) return;
    stop_ = true;
    running_ = false;
  }
  cv_.notify_all();
  if (worker_.joinable()) {
    worker_.join();
  }
  // Unsubscribe AFTER the worker exits so a late callback can't
  // race with destruction of the wake state.
  feed_.unsubscribe(sub_handle_);
  sub_handle_ = 0;
}

void Reconciler::reconcile_once() {
  // Pure synchronous: read store, hand to sink. Used by tests
  // and from the worker loop.
  sink_.apply(compose_exports(store_, bootstrap_));
}

void Reconciler::worker_loop() {
  while (true) {
    {
      std::unique_lock lock(mu_);
      // Sleep until either: someone sets dirty_, stop_ fires,
      // or the safety-net timeout elapses.
      cv_.wait_for(lock, cfg_.safety_net_interval,
                   [this]{ return dirty_ || stop_; });
      if (stop_) return;
      // Consume the dirty flag before we run, so any wake that
      // happens during this reconcile sets it true again and
      // we'll loop once more.
      dirty_ = false;
    }
    reconcile_once();
  }
}

}  // namespace rgw::file_state
