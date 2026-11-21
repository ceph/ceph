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

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>

namespace kvrgw {

enum class FaultType : uint8_t {
  kAbortAfterBatchPhase2 = 0,
  kAbortAfterSinglePhase2,
  kAbortSweeperAfterPutGo,
  kAbortGcWorkerMidGroup,
  kCount
};

inline constexpr size_t kFaultTypeCount = static_cast<size_t>(FaultType::kCount);

struct FaultPayload {
  std::atomic<int32_t> call_count{0};
  int32_t period{0};
  std::atomic<int64_t> next_trigger_ns{0};
  int64_t interval_us{0};
  std::atomic<uint8_t> remaining_burst{0};
  uint8_t burst_size{0};
};

class ErrInsertion {
 public:
  static constexpr uint8_t kModeDisabled = 0x00;
  static constexpr uint8_t kModeFixed    = 0x01;
  static constexpr uint8_t kModeCounter  = 0x02;
  static constexpr uint8_t kModeTime     = 0x03;

  void set_fixed(FaultType type);
  void set_counter(FaultType type, int32_t period, uint8_t burst_size = 0);
  void set_time(FaultType type, int64_t interval_us, uint8_t burst_size = 0);
  void clear(FaultType type);

  inline bool is_error_active(FaultType type) const {
    if (flags_[static_cast<int>(type)].load(std::memory_order_relaxed) == 0) [[likely]]
      return false;
    return check_and_update(type);
  }

 private:
  bool check_and_update(FaultType type) const;
  int64_t now_ns() const;

  std::atomic<uint8_t> flags_[kFaultTypeCount]{};
  mutable FaultPayload payload_[kFaultTypeCount]{};
};

}  // namespace kvrgw
