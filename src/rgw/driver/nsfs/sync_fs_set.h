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

#pragma once

#include <chrono>
#include <condition_variable>
#include <map>
#include <mutex>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

namespace rgw::sal::nsfs {

/* Every filesystem that holds a bucket, synced on an interval:  the
 * relaxed sync policy's half of durability.
 *
 * Under that policy a write is not fsync'd, and is durable once syncfs()
 * has run on its filesystem.  syncfs() flushes the one filesystem its
 * descriptor is on, and nsfs buckets are wherever their records place
 * them, so one descriptor on the base path covers only the buckets that
 * happen to be on its filesystem.  The set is learned as buckets are
 * opened -- a write reaches a bucket only after it is loaded -- and one
 * descriptor is kept per filesystem.
 *
 * posix keeps its own SyncFsThread over its one root, which is right
 * for posix. */
class SyncFsSet {
  const std::chrono::milliseconds interval;

  mutable std::mutex mtx;
  std::condition_variable cv;
  bool stopping{false};
  /* device -> a descriptor on that filesystem, ours to close */
  std::map<dev_t, int> fds;
  std::thread thread;

public:
  explicit SyncFsSet(uint64_t interval_ms) : interval(interval_ms) {}
  ~SyncFsSet() { shutdown(); }

  SyncFsSet(const SyncFsSet&) = delete;
  SyncFsSet& operator=(const SyncFsSet&) = delete;

  /* Cover the filesystem `fd` is on, known by `dev`.  `fd` stays the
   * caller's;  a duplicate is kept the first time a device is seen.
   * Returns whether the device is new. */
  bool add(dev_t dev, int fd) {
    std::lock_guard l{mtx};
    if (fds.contains(dev)) {
      return false;
    }
    const int dup = ::fcntl(fd, F_DUPFD_CLOEXEC, 0);
    if (dup < 0) {
      return false;
    }
    fds.emplace(dev, dup);
    return true;
  }

  std::vector<dev_t> devices() const {
    std::lock_guard l{mtx};
    std::vector<dev_t> out;
    for (const auto& [dev, _] : fds) {
      out.push_back(dev);
    }
    return out;
  }

  /* Waits on a condition rather than sleeping, so that shutdown does not
   * wait out an interval. */
  void start() {
    thread = std::thread([this] {
      std::unique_lock l{mtx};
      for (;;) {
	if (cv.wait_for(l, interval, [this] { return stopping; })) {
	  return;
	}
	/* the descriptors are closed only after this thread is joined,
	 * so a snapshot of them stays valid with the lock dropped, and a
	 * bucket load never waits on a sync */
	std::vector<int> snapshot;
	for (const auto& [_, fd] : fds) {
	  snapshot.push_back(fd);
	}
	l.unlock();
	for (int fd : snapshot) {
	  ::syncfs(fd);
	}
	l.lock();
      }
    });
  }

  void shutdown() {
    {
      std::lock_guard l{mtx};
      stopping = true;
    }
    cv.notify_all();
    if (thread.joinable()) {
      thread.join();
    }
    std::lock_guard l{mtx};
    for (const auto& [_, fd] : fds) {
      ::close(fd);
    }
    fds.clear();
  }
};

} // namespace rgw::sal::nsfs
