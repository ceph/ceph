// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <chrono>
#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/error.hpp>
#include <boost/system/error_code.hpp>
#include <condition_variable>
#include <atomic>
#include <unordered_map>
#include <vector>
#include <string>
#include "rgw_common.h"
#include "rgw_sal.h"

namespace rgw::restore {

class RestoreWaiterRegistry;

// Represents a single GET request waiting for restore completion
struct RestoreWaiter {
  // the blocking wait uses std::condition_variable::wait_for(), which uses the
  // std::chrono::steady_clock. use that clock for async waits as well
  using Clock = std::chrono::steady_clock;
  using Timer = boost::asio::basic_waitable_timer<Clock>;

  std::mutex mtx;
  std::condition_variable cv;
  std::mutex timer_mtx;
  std::weak_ptr<Timer> active_timer;
  std::atomic<bool> completed{false};
  std::atomic<bool> failed{false};
  std::atomic<int16_t> result{0};  // Error codes fit in int16_t
  std::string cached_key;  // Cached registry key to avoid recomputation in unregister
  ceph::coarse_real_time last_used;  // Timestamp for pool eviction

  // Wait for completion for up to 'timeout'. Uses cv for blocking callers and
  // a timer for coroutine callers so we don't block the frontend coroutine.
  bool wait_for(std::chrono::milliseconds timeout, optional_yield y);
  // Mark completion and wake any waiting callers.
  void complete(bool success, int result_code);
  // Reset state before reuse.
  void reset();
};

// Object pool for RestoreWaiter to avoid repeated allocations
class RestoreWaiterPool {
private:
  friend class RestoreWaiterRegistry;
  std::mutex pool_mtx;
  std::vector<std::unique_ptr<RestoreWaiter>> free_list;
  static constexpr size_t MAX_POOL_SIZE = 256;
  static constexpr std::chrono::seconds EVICTION_TIME{300};  // 5 minutes

public:
  std::shared_ptr<RestoreWaiter> acquire(std::weak_ptr<RestoreWaiterRegistry> owner);

private:
  void release(RestoreWaiter* waiter);
  void evict_old_waiters();
};

// Registry mapping object keys to waiting GET requests
class RestoreWaiterRegistry : public std::enable_shared_from_this<RestoreWaiterRegistry> {
private:
  mutable std::shared_mutex registry_mtx;
  std::unordered_map<std::string, std::vector<std::shared_ptr<RestoreWaiter>>> waiters;
  RestoreWaiterPool waiter_pool;
  std::atomic<bool> shutting_down{false};
  friend class RestoreWaiterPool;

  static std::string make_key(const rgw_bucket& bucket, const rgw_obj_key& obj_key);
  void release_waiter(RestoreWaiter* waiter);

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

  // Cancel all waiters and prevent new registrations
  void shutdown();
};

// RAII guard for automatic waiter unregistration
struct WaiterGuard {
  std::shared_ptr<RestoreWaiterRegistry> registry;
  std::shared_ptr<RestoreWaiter> waiter;

  WaiterGuard(std::shared_ptr<RestoreWaiterRegistry> reg, std::shared_ptr<RestoreWaiter> w)
    : registry(std::move(reg)), waiter(std::move(w)) {}

  ~WaiterGuard() {
    if (registry && waiter) {
      registry->unregister_waiter(waiter);
    }
  }

  WaiterGuard(const WaiterGuard&) = delete;
  WaiterGuard& operator=(const WaiterGuard&) = delete;
};

} // namespace rgw::restore
