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

#include "tier_config_state.hpp"

#include "constants.hpp"

#include <cstdlib>
#include <filesystem>
#include <yaml-cpp/yaml.h>

namespace kvrgw {

TierConfigState::TierConfigState(TierConfig initial)
    : active_(initial), pending_(initial)
{
}

std::optional<uint32_t> TierConfigState::try_stage(const TierConfig &config)
{
  std::lock_guard lock(mu_);
  if (active_age_.load(std::memory_order_acquire) !=
      pending_age_.load(std::memory_order_relaxed)) {
    return std::nullopt;
  }
  pending_ = config;
  const uint32_t handle =
      pending_age_.fetch_add(1, std::memory_order_acq_rel) + 1;
  return handle;
}

uint32_t TierConfigState::active_age() const
{
  return active_age_.load(std::memory_order_acquire);
}

TierConfigSnapshot TierConfigState::snapshot() const
{
  std::lock_guard lock(mu_);
  TierConfigSnapshot out;
  out.active_age = active_age_.load(std::memory_order_relaxed);
  out.pending_age = pending_age_.load(std::memory_order_relaxed);
  out.active = active_;
  return out;
}

TierConfig TierConfigState::active_copy() const
{
  std::lock_guard lock(mu_);
  return active_;
}

void TierConfigState::maybe_apply_pending()
{
  if (active_age_.load(std::memory_order_acquire) ==
      pending_age_.load(std::memory_order_acquire)) {
    return;
  }
  std::lock_guard lock(mu_);
  if (active_age_ == pending_age_) {
    return;
  }
  active_ = pending_;
  active_age_.store(pending_age_.load(std::memory_order_relaxed),
                    std::memory_order_release);
}

TierConfig TierConfigState::load_from_yaml(const std::string &path)
{
  TierConfig config;
  if (!std::filesystem::exists(path)) {
    return config;
  }
  YAML::Node root = YAML::LoadFile(path);
  if (!root["tier"]) {
    return config;
  }
  const auto tier = root["tier"];
  if (tier["max_inline"]) {
    config.max_inline = tier["max_inline"].as<uint32_t>();
  }
  if (tier["max_kv_store"]) {
    config.max_kv_store = tier["max_kv_store"].as<uint32_t>();
  }
  if (tier["kv_store_coalescing"]) {
    config.kv_store_coalescing = tier["kv_store_coalescing"].as<bool>();
  }
  return config;
}

TierConfig TierConfigState::apply_env_overrides(TierConfig base)
{
  if (const char *v = std::getenv("KVRGW_MAX_INLINE")) {
    base.max_inline = static_cast<uint32_t>(std::atoi(v));
  }
  if (const char *v = std::getenv("KVRGW_MAX_KV_STORE")) {
    base.max_kv_store = static_cast<uint32_t>(std::atoi(v));
  }
  if (const char *v = std::getenv("KVRGW_KV_STORE_COALESCING")) {
    base.kv_store_coalescing = std::atoi(v) != 0;
  }
  if (const char *v = std::getenv("KVRGW_BATCH_SIZE")) {
    base.batch_size = std::atoi(v);
  }
  if (const char *v = std::getenv("KVRGW_BATCH_TIMEOUT_US")) {
    base.batch_timeout_us = std::atoi(v);
  }
  if (const char *v = std::getenv("KVRGW_BATCH_THREADS")) {
    base.batch_threads = std::atoi(v);
  }
  return base;
}

} // namespace kvrgw
