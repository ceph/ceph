// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-

#pragma once

#include "common/ceph_mutex.h"
#include "common/ceph_time.h"

#ifdef CEPH_DEBUG_MUTEX
#include <thread> // for std::this_thread::get_id()
#endif

#include <atomic>
#include <cstdint>
#include <string>

namespace ceph {
/// a FIFO mutex
class fair_mutex {
public:
  /// cumulative wait/hold accounting; only advances while tracking is enabled
  struct stats {
    uint64_t acquisitions = 0;
    /// total time spent blocked in lock(), in nanoseconds
    uint64_t wait_ns = 0;
    /// total time the mutex was held, in nanoseconds
    uint64_t held_ns = 0;
  };

  fair_mutex(const std::string& name)
    : mutex{ceph::make_mutex(name)}
  {}
  ~fair_mutex() = default;
  fair_mutex(const fair_mutex&) = delete;
  fair_mutex& operator=(const fair_mutex&) = delete;

  void lock()
  {
    const bool track = track_stats.load(std::memory_order_relaxed);
    const auto start = track ? mono_clock::now() : mono_clock::zero();
    std::unique_lock lock(mutex);
    const unsigned my_id = next_id++;
    cond.wait(lock, [&] {
      return my_id == unblock_id;
    });
    _set_locked_by();
    _account_acquired(track, start);
  }

  bool try_lock()
  {
    const bool track = track_stats.load(std::memory_order_relaxed);
    std::lock_guard lock(mutex);
    if (is_locked()) {
      return false;
    }
    ++next_id;
    _set_locked_by();
    /* an uncontended acquisition, so there is no wait to account for */
    _account_acquired(track, mono_clock::zero());
    return true;
  }

  void unlock()
  {
    std::lock_guard lock(mutex);
    if (!mono_clock::is_zero(locked_at)) {
      held_ns.fetch_add((mono_clock::now() - locked_at).count(),
                        std::memory_order_relaxed);
      locked_at = mono_clock::zero();
    }
    ++unblock_id;
    _reset_locked_by();
    cond.notify_all();
  }

  bool is_locked() const
  {
    return next_id != unblock_id;
  }

  /**
   * Enable or disable wait/hold time accounting.
   *
   * Disabled by default: when disabled lock() does not read the clock at
   * all, so the only cost is a relaxed load of a bool. Enabling it costs
   * three clock reads and three relaxed increments per acquisition, which is
   * why it is opt-in -- see mds_enable_phase_tracker, the one caller that
   * turns it on.
   */
  void set_track_stats(bool b) {
    track_stats.store(b, std::memory_order_relaxed);
  }
  bool get_track_stats() const {
    return track_stats.load(std::memory_order_relaxed);
  }

  /// a snapshot of the counters; safe to call without holding the mutex
  stats get_stats() const {
    stats s;
    s.acquisitions = acquisitions.load(std::memory_order_relaxed);
    s.wait_ns = wait_ns.load(std::memory_order_relaxed);
    s.held_ns = held_ns.load(std::memory_order_relaxed);
    return s;
  }

#ifdef CEPH_DEBUG_MUTEX
  bool is_locked_by_me() const {
    return is_locked() && locked_by == std::this_thread::get_id();
  }
private:
  void _set_locked_by() {
    locked_by = std::this_thread::get_id();
  }
  void _reset_locked_by() {
    locked_by = {};
  }
#else
  void _set_locked_by() {}
  void _reset_locked_by() {}
#endif

private:
  /* called with `mutex` held, right after the mutex was acquired. `start` is
   * when the caller began waiting, or zero if it never did. */
  void _account_acquired(bool track, const mono_time& start) {
    if (!track) {
      return;
    }
    const auto now = mono_clock::now();
    if (!mono_clock::is_zero(start)) {
      wait_ns.fetch_add((now - start).count(), std::memory_order_relaxed);
    }
    acquisitions.fetch_add(1, std::memory_order_relaxed);
    locked_at = now;
  }

  unsigned next_id = 0;
  unsigned unblock_id = 0;
  ceph::condition_variable cond;
  ceph::mutex mutex;
  std::atomic<bool> track_stats = false;
  /* `locked_at` is only ever read or written with `mutex` held; it is left at
   * zero when tracking is off, which is also how unlock() knows whether the
   * current acquisition was accounted for. */
  mono_time locked_at = mono_clock::zero();
  std::atomic<uint64_t> acquisitions = 0;
  std::atomic<uint64_t> wait_ns = 0;
  std::atomic<uint64_t> held_ns = 0;
#ifdef CEPH_DEBUG_MUTEX
  std::thread::id locked_by = {};
#endif
};
} // namespace ceph
