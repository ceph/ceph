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

#include <array>
#include <atomic>
#include <cstdint>

namespace kvrgw {

struct OpsStats {
  static constexpr std::size_t kN = static_cast<std::size_t>(OpType::kCount);

  void inc(OpType op) {
    counts_[static_cast<std::size_t>(op)].fetch_add(1, std::memory_order_relaxed);
  }

  void snapshot(std::array<uint64_t, kN>& out) const {
    for (std::size_t i = 0; i < kN; ++i) {
      out[i] = counts_[i].load(std::memory_order_relaxed);
    }
  }

 private:
  std::array<std::atomic<uint64_t>, kN> counts_{};
};

}  // namespace kvrgw
