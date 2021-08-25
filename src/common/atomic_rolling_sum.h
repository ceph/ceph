// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <algorithm> // std::min()
#include <atomic>
#include <chrono>

namespace ceph {

/// a rolling sum of atomic counters spread over a number of time intervals
template <std::size_t IntervalCount,
          typename Rep, typename Ratio,
          Rep IntervalValue>
class AtomicRollingSum {
 public:
  /// representation of logical position (time / interval)
  using size_type = Rep;
  /// duration type for the interval
  using duration_type = std::chrono::duration<size_type, Ratio>;
  /// compile-time interval value
  static constexpr duration_type interval{IntervalValue};
  /// number of counters to track
  static constexpr size_type interval_count = IntervalCount;
  /// value type used for the atomic counters
  using value_type = size_type; // TODO: allow value_type != size_type

 private:
  /// counters for each interval
  std::atomic<value_type> counters_[interval_count] = {0};
  /// starting position of the sliding window [start_,start_+interval_count)
  std::atomic<size_type> start_ = 0;

  /// return a reference to the counter at the given logical position
  auto& at(size_type pos) { return counters_[pos % interval_count]; }
  /// return a const reference to the counter at the given logical position
  auto& at(size_type pos) const { return counters_[pos % interval_count]; }

  /// return a sum of intervals in the cyclical range [start,end)
  value_type sum(size_type start, size_type end) const
  {
    auto count = 0;
    for (auto i = start; i < end; ++i) {
      count += at(i).load(std::memory_order_relaxed);
    }
    return count;
  }

  /// recycle expired entries and advance the starting position. returns the
  /// new starting position >= new_start
  size_type advance(size_type start, size_type new_start)
  {
    const auto count = std::min(new_start - start, interval_count);
    // by clearing these counters before we advance 'start_', we guarantee that
    // no thread will count old values. however, this also means that several
    // threads may see the old 'start_' and race to clear the counters - and
    // those writes may overwrite the result of the counter's increment below
    for (value_type i = 0; i < count; ++i) {
      at(start + i).store(0, std::memory_order_relaxed);
    }

    // advance 'start_' if it hasn't changed since we read it, retrying the
    // write until we succeed or find a value >= new_start
    do {
      // store with 'release' ordering to guarantee that the counter
      // resets happen first
      if (start_.compare_exchange_weak(start, new_start,
                                       std::memory_order_release,
                                       std::memory_order_relaxed)) {
        return new_start;
      }
    } while (start < new_start);

    return start;
  }

 public:
  /// return the sum of currently valid intervals
  value_type sum(duration_type now) const
  {
    const auto pos = now / interval;
    auto start = start_.load(std::memory_order_acquire);
    const auto end = start + interval_count;
    if (pos >= end) { // skip any expired intervals
      start = pos - (interval_count - 1);
    }
    return sum(start, end);
  }

  /// add to the current counter
  void add(value_type amount, duration_type now)
  {
    const auto pos = now / interval;
    auto start = start_.load(std::memory_order_acquire);
    const auto end = start + interval_count;

    if (pos < start) {
      // we've already advanced past 'pos', so we can't register 'amount'
      return;
    }
    if (pos >= end) {
      // out of space, advance the window so 'pos' is the last element
      const auto new_start = pos - (interval_count - 1);
      start = advance(start, new_start);
    }
    at(pos).fetch_add(amount, std::memory_order_relaxed);
  }

  /// add to the current counter and return the sum over all intervals
  value_type add_sum(value_type amount, duration_type now)
  {
    const auto pos = now / interval;
    auto start = start_.load(std::memory_order_acquire);
    auto end = start + interval_count;

    if (pos < start) {
      // we've already advanced past 'pos', so we can't register 'amount'.
      // just sum our latest counters
      return sum(start, end);
    }
    if (pos >= end) {
      // out of space, advance the window so 'pos' is the last element
      const auto new_start = 1 + pos - interval_count;
      start = advance(start, new_start);
      end = start + interval_count;
    }

    auto count = at(pos).fetch_add(amount, std::memory_order_relaxed);
    count += amount; // fetch_add() returns the original value
    count += sum(start, pos); // sum any counters before pos
    count += sum(pos + 1, end); // sum any counters after pos
    return count;
  }
};

} // namespace ceph
