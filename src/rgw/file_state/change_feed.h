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

// Pluggable "tell me when control-plane state has changed"
// abstraction for the s3files reconciler.
//
// The reconciler runs a worker loop that re-evaluates the desired
// Ganesha export set whenever (a) the change feed signals a
// mutation, or (b) a safety-net timer fires. Both paths converge
// on the same idempotent "read all state, render, reload" cycle,
// so the feed is a *latency optimization* — correctness is
// recovered by the timer regardless of feed reliability.
//
// Implementations:
//   NoopChangeFeed       — never signals; reconciler relies
//                          entirely on the safety-net timer. Used
//                          in unit tests against MemoryStore where
//                          timed re-evaluation is sufficient.
//   InProcessChangeFeed  — same-process callback bus. MemoryStore
//                          (which lives in the same RGW process)
//                          fires post-mutation; the feed forwards
//                          to the reconciler immediately. Used
//                          for end-to-end demos that want
//                          sub-second latency without a separate
//                          reconciler service or a real backend
//                          watch primitive.
//   RadosChangeFeed      — librados watch/notify on the per-realm
//                          resource omap objects. Future commit.
//   FdbChangeFeed        — FDB watches on tuple-prefix keys.
//                          Future commit, deferred until #65535
//                          (chardan's FDB plumbing) lands.
//
// The interface is deliberately coarse: a callback fires meaning
// "something changed, re-evaluate." Granular per-resource
// notifications could be added later if reconcile-everything
// becomes too expensive, but for tens-to-hundreds of FileSystems
// per zone the full re-read is bounded and predictable.

#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace rgw::file_state {

class ChangeFeed {
 public:
  using Handle = std::size_t;
  using Callback = std::function<void()>;

  virtual ~ChangeFeed() = default;

  // Register `cb` to fire whenever the feed observes a change.
  // The implementation may invoke `cb` from any thread; the
  // callback must be thread-safe and should marshal work onto
  // the reconciler's own thread (typically by enqueueing an
  // evaluation event).
  //
  // Returns a handle usable for unsubscribe(). Multiple
  // subscribers are supported; each is called per change event.
  virtual Handle subscribe(Callback cb) = 0;

  // Cancel a prior subscribe(). Idempotent; unknown handles are
  // silently ignored.
  virtual void unsubscribe(Handle h) = 0;
};

// Stub feed that never signals. The reconciler still functions
// correctly when paired with a NoopChangeFeed because the
// safety-net timer drives reconciliation regardless of feed
// activity — just at coarser latency than a live feed would
// provide. Useful in unit tests where deterministic, time-driven
// progression is preferable to event-driven.
class NoopChangeFeed final : public ChangeFeed {
 public:
  Handle subscribe(Callback) override { return 0; }
  void unsubscribe(Handle) override {}
};

// Same-process broadcaster. Producers (MemoryStore) call
// `fire()` post-mutation; subscribers (reconciler) get invoked
// synchronously on the producer's thread.
//
// Synchronization: subscriber registration is mutex-guarded so
// concurrent subscribe/unsubscribe/fire from RGW handler threads
// + a reconciler thread is safe. Subscribers run inline on the
// fire() caller's thread; they should not block on I/O.
class InProcessChangeFeed final : public ChangeFeed {
 public:
  Handle subscribe(Callback cb) override;
  void unsubscribe(Handle h) override;

  // Producer side: notify all current subscribers.
  void fire();

 private:
  mutable std::mutex mu_;
  std::unordered_map<Handle, Callback> subs_;
  Handle next_{1};
};

}  // namespace rgw::file_state
