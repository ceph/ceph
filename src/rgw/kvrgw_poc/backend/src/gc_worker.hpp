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

#include "data_store.hpp"
#include "gc_config_state.hpp"
#include "kv_store.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>

namespace kvrgw {

class GcWorker {
 public:
  GcWorker(
      KvStore& store,
      DataStore& data_store,
      GcConfigState& config,
      std::atomic<bool>& stop_flag);

  void run();

 private:
  struct RateWindow {
    std::chrono::steady_clock::time_point window_start{};
    int objects = 0;
    uint64_t bytes = 0;
  };

  void gc_once(const GcPolicy& policy, RateWindow* rate);
  bool rate_allow(RateWindow* rate, const GcPolicy& policy, uint64_t blob_bytes);
  void sleep_ms(int ms);

  KvStore& store_;
  DataStore& data_store_;
  GcConfigState& config_;
  std::atomic<bool>& stop_flag_;
};

}  // namespace kvrgw
