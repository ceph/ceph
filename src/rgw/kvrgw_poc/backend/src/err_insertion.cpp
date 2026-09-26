// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "err_insertion.hpp"

#include <chrono>

namespace kvrgw {

void ErrInsertion::set_fixed(FaultType type)
{
  auto &p = payload_[static_cast<int>(type)];
  p.call_count.store(0, std::memory_order_relaxed);
  p.period = 0;
  p.remaining_burst.store(0, std::memory_order_relaxed);
  p.burst_size = 0;
  flags_[static_cast<int>(type)].store(kModeFixed, std::memory_order_release);
}

void ErrInsertion::set_counter(FaultType type, int32_t period,
                               uint8_t burst_size)
{
  auto &p = payload_[static_cast<int>(type)];
  p.call_count.store(0, std::memory_order_relaxed);
  p.period = period;
  p.remaining_burst.store(0, std::memory_order_relaxed);
  p.burst_size = burst_size;
  flags_[static_cast<int>(type)].store(kModeCounter, std::memory_order_release);
}

void ErrInsertion::set_time(FaultType type, int64_t interval_us,
                            uint8_t burst_size)
{
  auto &p = payload_[static_cast<int>(type)];
  p.interval_us = interval_us;
  p.next_trigger_ns.store(now_ns() + interval_us * 1000,
                          std::memory_order_relaxed);
  p.remaining_burst.store(0, std::memory_order_relaxed);
  p.burst_size = burst_size;
  flags_[static_cast<int>(type)].store(kModeTime, std::memory_order_release);
}

void ErrInsertion::clear(FaultType type)
{
  flags_[static_cast<int>(type)].store(kModeDisabled,
                                       std::memory_order_release);
  auto &p = payload_[static_cast<int>(type)];
  p.call_count.store(0, std::memory_order_relaxed);
  p.remaining_burst.store(0, std::memory_order_relaxed);
}

bool ErrInsertion::check_and_update(FaultType type) const
{
  auto &p = payload_[static_cast<int>(type)];
  const uint8_t mode =
      flags_[static_cast<int>(type)].load(std::memory_order_acquire);

  uint8_t burst = p.remaining_burst.load(std::memory_order_relaxed);
  if (burst > 0) {
    p.remaining_burst.store(burst - 1, std::memory_order_relaxed);
    return true;
  }

  bool triggered = false;

  switch (mode) {
  case kModeFixed:
    triggered = true;
    break;

  case kModeCounter: {
    int32_t count = p.call_count.fetch_add(1, std::memory_order_relaxed) + 1;
    triggered = (count % p.period == 0);
    break;
  }

  case kModeTime: {
    int64_t now = now_ns();
    int64_t next = p.next_trigger_ns.load(std::memory_order_relaxed);
    if (now >= next) {
      triggered = true;
      if (p.burst_size > 0) {
        // Next trigger starts after burst completes (set by caller on last
        // burst op) Don't advance next_trigger_ns here; it's set when burst
        // finishes
      }
      else {
        p.next_trigger_ns.store(now + p.interval_us * 1000,
                                std::memory_order_relaxed);
      }
    }
    break;
  }

  default:
    break;
  }

  if (triggered && p.burst_size > 0) {
    p.remaining_burst.store(p.burst_size - 1, std::memory_order_relaxed);
  }

  return triggered;
}

int64_t ErrInsertion::now_ns() const
{
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

} // namespace kvrgw
