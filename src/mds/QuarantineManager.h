/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM, Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#pragma once

#include <atomic>
#include <mutex>

#include "include/ceph_assert.h"
#include "include/cephfs/types.h"
#include "include/Context.h"

/**
 * QuarantineTracker - Tracks async cap revocation work for a quarantine operation.
 *
 * Usage:
 *   1. register_callback(cb)     - set completion callback
 *   2. add_work()                - call before queueing each inode's async work
 *   3. seal()                    - call when all inodes discovered
 *   4. work_done()               - call when each inode's work completes
 *   5. When sealed AND pending==0, callback fires
 *
 * The inode's optmetadata (is_quarantined flag) is the source of truth for
 * enforcement. This class only tracks async work completion.
 */
class QuarantineTracker {
public:
  QuarantineTracker() = default;

  void register_callback(Context* cb) {
    std::lock_guard<ceph::mutex> lck(m);
    on_complete = cb;
  }

  void add_work() {
    std::lock_guard<ceph::mutex> lck(m);
    ceph_assert(!sealed);
    ++pending;
    ++started;
  }

  void work_done() {
    Context* cb = nullptr;
    {
      std::lock_guard<ceph::mutex> lck(m);
      ceph_assert(pending > 0);
      --pending;
      if (sealed && pending == 0 && on_complete) {
        cb = on_complete;
        on_complete = nullptr;
      }
    }
    if (cb) cb->complete(0);
  }

  void seal() {
    Context* cb = nullptr;
    {
      std::lock_guard<ceph::mutex> lck(m);
      if (sealed) return;  // idempotent
      sealed = true;
      if (pending == 0 && on_complete) {
        cb = on_complete;
        on_complete = nullptr;
      }
    }
    if (cb) cb->complete(0);
  }

  void complete(int r) {
    Context* cb = nullptr;
    {
      std::lock_guard<ceph::mutex> lck(m);
      cb = on_complete;
      on_complete = nullptr;
    }
    if (cb) cb->complete(r);
  }

  bool is_complete() const {
    std::lock_guard<ceph::mutex> lck(m);
    return sealed && pending == 0;
  }

  // Inode tracking for deferred unregistration
  void set_qtine_ino(inodeno_t ino) {
    std::lock_guard<ceph::mutex> lck(m);
    qtine_ino = ino;
  }
  inodeno_t get_qtine_ino() const {
    std::lock_guard<ceph::mutex> lck(m);
    return qtine_ino;
  }

  // Reference counting
  void get() { ++ref; }
  void put() { --ref; }
  int get_ref() const { return ref; }

  // Stats
  uint64_t get_started() const {
    std::lock_guard<ceph::mutex> lck(m);
    return started;
  }
  uint64_t get_finished() const {
    std::lock_guard<ceph::mutex> lck(m);
    return started - pending;
  }
  uint64_t get_pending() const {
    std::lock_guard<ceph::mutex> lck(m);
    return pending;
  }

private:
  mutable ceph::mutex m = ceph::make_mutex("QuarantineTracker::m");
  uint64_t pending = 0;
  uint64_t started = 0;
  bool sealed = false;
  Context* on_complete = nullptr;
  std::atomic_int ref{0};
  inodeno_t qtine_ino{0};
};

using QtineMgrRef = std::shared_ptr<QuarantineTracker>;
