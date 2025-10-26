// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_restore_waiter.h"
#include <algorithm>

namespace rgw::restore {

RestoreWaiterRegistry* g_restore_waiter_registry = nullptr;

// RestoreWaiterPool implementation
std::shared_ptr<RestoreWaiter> RestoreWaiterPool::acquire() {
  std::unique_lock lock(pool_mtx);

  // Periodic eviction of old waiters
  evict_old_waiters();

  if (!free_list.empty()) {
    auto waiter = std::move(free_list.back());
    free_list.pop_back();
    lock.unlock();

    // Reset state
    waiter->completed.store(false, std::memory_order_relaxed);
    waiter->failed.store(false, std::memory_order_relaxed);
    waiter->result.store(0, std::memory_order_relaxed);
    waiter->cached_key.clear();

    return std::shared_ptr<RestoreWaiter>(waiter.release(),
      [this](RestoreWaiter* w) { this->release(w); });
  }

  lock.unlock();
  auto waiter = std::make_unique<RestoreWaiter>();
  return std::shared_ptr<RestoreWaiter>(waiter.release(),
    [this](RestoreWaiter* w) { this->release(w); });
}

void RestoreWaiterPool::release(RestoreWaiter* waiter) {
  std::lock_guard lock(pool_mtx);
  if (free_list.size() < MAX_POOL_SIZE) {
    waiter->last_used = ceph::coarse_real_clock::now();
    free_list.emplace_back(waiter);
  } else {
    delete waiter;
  }
}

void RestoreWaiterPool::evict_old_waiters() {
  // Assumes pool_mtx is already held
  if (free_list.empty()) {
    return;
  }

  const auto now = ceph::coarse_real_clock::now();
  const auto eviction_threshold = now - EVICTION_TIME;

  free_list.erase(
    std::remove_if(free_list.begin(), free_list.end(),
      [eviction_threshold](const std::unique_ptr<RestoreWaiter>& w) {
        return w->last_used < eviction_threshold;
      }),
    free_list.end()
  );
}

// RestoreWaiterRegistry implementation
std::string RestoreWaiterRegistry::make_key(const rgw_bucket& bucket, const rgw_obj_key& obj_key) {
  std::string key;
  key.reserve(bucket.name.size() + obj_key.name.size() + obj_key.instance.size() + 10);
  key = bucket.get_key();
  key += ':';
  key += obj_key.name;
  if (!obj_key.instance.empty()) {
    key += ':';
    key += obj_key.instance;
  }
  return key;
}

std::shared_ptr<RestoreWaiter> RestoreWaiterRegistry::register_waiter(const rgw_bucket& bucket,
                                                                        const rgw_obj_key& obj_key) {
  auto waiter = waiter_pool.acquire();
  waiter->cached_key = make_key(bucket, obj_key);

  std::unique_lock lock(registry_mtx);
  auto& vec = waiters[waiter->cached_key];
  vec.push_back(waiter);

  return waiter;
}

void RestoreWaiterRegistry::unregister_waiter(std::shared_ptr<RestoreWaiter> waiter) {
  std::unique_lock lock(registry_mtx);

  auto it = waiters.find(waiter->cached_key);
  if (it != waiters.end()) {
    auto& vec = it->second;
    vec.erase(std::remove(vec.begin(), vec.end(), waiter), vec.end());
    if (vec.empty()) {
      waiters.erase(it);
    }
  }
}

void RestoreWaiterRegistry::notify_completion(const rgw_bucket& bucket,
                                               const rgw_obj_key& obj_key,
                                               bool success,
                                               int result) {
  std::string key = make_key(bucket, obj_key);
  std::vector<std::shared_ptr<RestoreWaiter>> to_notify;

  {
    std::unique_lock lock(registry_mtx);
    auto it = waiters.find(key);
    if (it != waiters.end()) {
      to_notify = std::move(it->second);
      waiters.erase(it);
    }
  }

  // Notify outside the lock - lock-free atomic updates
  for (auto& waiter : to_notify) {
    waiter->failed.store(!success, std::memory_order_release);
    waiter->result.store(result, std::memory_order_release);
    waiter->completed.store(true, std::memory_order_release);

    std::lock_guard waiter_lock(waiter->mtx);
    waiter->cv.notify_all();
  }
}

} // namespace rgw::restore
