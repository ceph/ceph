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

#include "fdb_latency.hpp"
#include "err_insertion.hpp"
#include "error_codes.hpp"
#include "gc_config_state.hpp"
#include "tier_config_state.hpp"

#include <atomic>
#include <chrono>
#include <string>

namespace kvrgw {

struct LatencyStats;
struct OpsStats;
struct BatchStats;

struct ErrorStats {
  std::atomic<int64_t> counts[::kvrgw::v1::KvrgwErrorCode_ARRAYSIZE]{};

  void record(KvrgwErrorCode code) {
    counts[static_cast<int>(code)].fetch_add(1, std::memory_order_relaxed);
  }

  int64_t get(KvrgwErrorCode code) const {
    return counts[static_cast<int>(code)].load(std::memory_order_relaxed);
  }

  void reset() {
    for (auto& c : counts) c.store(0, std::memory_order_relaxed);
  }
};

class AdminServer {
 public:
  AdminServer(GcConfigState& gc_config, TierConfigState& tier_config,
              std::string socket_path, std::atomic<bool>& stop_flag,
              LatencyStats* latency_stats = nullptr,
              ErrorStats* error_stats = nullptr,
              BatchStats* batch_stats = nullptr,
              ErrInsertion* err_insertion = nullptr,
              OpsStats* ops_stats = nullptr);

  bool run();

 private:
  std::string handle_line(const std::string& line);

  GcConfigState& config_;
  TierConfigState& tier_config_;
  std::string socket_path_;
  std::atomic<bool>& stop_flag_;
  LatencyStats* latency_stats_{};
  ErrorStats* error_stats_{};
  BatchStats* batch_stats_{};
  ErrInsertion* err_insertion_{};
  OpsStats* ops_stats_{};

  std::chrono::steady_clock::time_point bs_last_time_{};
  int64_t bs_prev_commits_{};
  int64_t bs_prev_entries_{};
  int64_t bs_prev_wait_{};
  int64_t bs_prev_queue_{};

  std::chrono::steady_clock::time_point lat_last_time_{};
  int64_t lat_prev_count_[static_cast<int>(OpType::kCount)]{};
  int64_t lat_prev_total_us_[static_cast<int>(OpType::kCount)]{};

  std::chrono::steady_clock::time_point ops_last_time_{};
  uint64_t ops_prev_get_{};
};

}  // namespace kvrgw
