// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <atomic>
#include <unordered_map>
#include <vector>
#include <string>
#include "rgw_common.h"
#include "rgw_sal.h"

namespace rgw::restore {

// Represents a single GET request waiting for restore completion
struct RestoreWaiter {
  std::mutex mtx;
  std::condition_variable cv;
  std::atomic<bool> completed{false};
  std::atomic<bool> failed{false};
  std::atomic<int16_t> result{0};  // Error codes fit in int16_t
  std::string cached_key;  // Cached registry key to avoid recomputation in unregister
  ceph::coarse_real_time last_used;  // Timestamp for pool eviction
};

// Object pool for RestoreWaiter to avoid repeated allocations
class RestoreWaiterPool {
private:
  std::mutex pool_mtx;
  std::vector<std::unique_ptr<RestoreWaiter>> free_list;
  static constexpr size_t MAX_POOL_SIZE = 256;
  static constexpr std::chrono::seconds EVICTION_TIME{300};  // 5 minutes

public:
  std::shared_ptr<RestoreWaiter> acquire();

private:
  void release(RestoreWaiter* waiter);
  void evict_old_waiters();
};

// Global registry mapping object keys to waiting GET requests
class RestoreWaiterRegistry {
private:
  mutable std::shared_mutex registry_mtx;
  std::unordered_map<std::string, std::vector<std::shared_ptr<RestoreWaiter>>> waiters;
  RestoreWaiterPool waiter_pool;

  static std::string make_key(const rgw_bucket& bucket, const rgw_obj_key& obj_key);

public:
  // Register a waiter for an object restore
  std::shared_ptr<RestoreWaiter> register_waiter(const rgw_bucket& bucket,
                                                   const rgw_obj_key& obj_key);

  // Unregister a waiter (called on timeout or completion)
  void unregister_waiter(std::shared_ptr<RestoreWaiter> waiter);

  // Signal all waiters for an object (called by restore worker)
  void notify_completion(const rgw_bucket& bucket,
                         const rgw_obj_key& obj_key,
                         bool success,
                         int result);
};

// RAII guard for automatic waiter unregistration
struct WaiterGuard {
  RestoreWaiterRegistry* registry;
  std::shared_ptr<RestoreWaiter> waiter;

  WaiterGuard(RestoreWaiterRegistry* reg, std::shared_ptr<RestoreWaiter> w)
    : registry(reg), waiter(std::move(w)) {}

  ~WaiterGuard() {
    if (registry && waiter) {
      registry->unregister_waiter(waiter);
    }
  }

  WaiterGuard(const WaiterGuard&) = delete;
  WaiterGuard& operator=(const WaiterGuard&) = delete;
};

// Global singleton
extern RestoreWaiterRegistry* g_restore_waiter_registry;

} // namespace rgw::restore
