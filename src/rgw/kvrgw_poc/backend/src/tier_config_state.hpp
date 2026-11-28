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

#include "service_impl.hpp"

#include <atomic>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>

namespace kvrgw {

struct TierConfigSnapshot {
  uint32_t active_age{};
  uint32_t pending_age{};
  TierConfig active;
};

class TierConfigState {
 public:
  explicit TierConfigState(TierConfig initial);

  std::optional<uint32_t> try_stage(const TierConfig& config);
  uint32_t active_age() const;
  TierConfigSnapshot snapshot() const;
  TierConfig active_copy() const;
  void maybe_apply_pending();

  static TierConfig load_from_yaml(const std::string& path);
  static TierConfig apply_env_overrides(TierConfig base);

 private:
  mutable std::mutex mu_;
  TierConfig active_;
  TierConfig pending_;
  std::atomic<uint32_t> active_age_{0};
  std::atomic<uint32_t> pending_age_{0};
};

}  // namespace kvrgw
