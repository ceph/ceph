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

#include "gc_config_state.hpp"

namespace kvrgw {

GcConfigState::GcConfigState(GcPolicy initial)
    : active_policy_(initial), pending_policy_(initial)
{
}

std::optional<uint32_t> GcConfigState::try_stage(const GcPolicy &policy)
{
  std::lock_guard lock(mu_);
  if (active_age_.load(std::memory_order_acquire) !=
      pending_age_.load(std::memory_order_relaxed)) {
    return std::nullopt;
  }
  pending_policy_ = policy;
  const uint32_t handle =
      pending_age_.fetch_add(1, std::memory_order_acq_rel) + 1;
  return handle;
}

uint32_t GcConfigState::active_age() const
{
  return active_age_.load(std::memory_order_acquire);
}

GcConfigSnapshot GcConfigState::snapshot() const
{
  std::lock_guard lock(mu_);
  GcConfigSnapshot out;
  out.active_age = active_age_.load(std::memory_order_relaxed);
  out.pending_age = pending_age_.load(std::memory_order_relaxed);
  out.active = active_policy_;
  return out;
}

GcPolicy GcConfigState::active_policy_copy() const
{
  std::lock_guard lock(mu_);
  return active_policy_;
}

void GcConfigState::maybe_apply_pending()
{
  if (active_age_.load(std::memory_order_acquire) ==
      pending_age_.load(std::memory_order_acquire)) {
    return;
  }
  std::lock_guard lock(mu_);
  if (active_age_ == pending_age_) {
    return;
  }
  active_policy_ = pending_policy_;
  active_age_.store(pending_age_.load(std::memory_order_relaxed),
                    std::memory_order_release);
}

} // namespace kvrgw
