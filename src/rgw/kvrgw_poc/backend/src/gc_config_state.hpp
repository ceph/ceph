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

#include "gc_policy.hpp"

#include <atomic>
#include <cstdint>
#include <mutex>
#include <optional>

namespace kvrgw {

class GcConfigState {
 public:
  explicit GcConfigState(GcPolicy initial);

  std::optional<uint32_t> try_stage(const GcPolicy& policy);
  uint32_t active_age() const;
  GcConfigSnapshot snapshot() const;
  GcPolicy active_policy_copy() const;
  void maybe_apply_pending();

 private:
  mutable std::mutex mu_;
  GcPolicy active_policy_;
  GcPolicy pending_policy_;
  std::atomic<uint32_t> active_age_{0};
  std::atomic<uint32_t> pending_age_{0};
};

}  // namespace kvrgw
