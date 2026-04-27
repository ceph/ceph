// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_restore_waiter.h"
#include <algorithm>
#include <errno.h>

namespace rgw::restore {

void RestoreWaiter::reset() {
  completed.store(false, std::memory_order_relaxed);
  failed.store(false, std::memory_order_relaxed);
  result.store(0, std::memory_order_relaxed);
  cached_key.clear();
}

bool RestoreWaiter::wait_for(std::chrono::milliseconds timeout, optional_yield y) {
  if (completed.load(std::memory_order_acquire)) {
    return true;
  }

  if (y) {
    auto& yield = y.get_yield_context();
    auto timer = std::make_shared<Timer>(yield.get_executor());

    {
      std::lock_guard lock(timer_mtx);
      active_timer = timer;
    }

    timer->expires_after(timeout);

    boost::system::error_code ec;
    timer->async_wait(yield[ec]);

    {
      std::lock_guard lock(timer_mtx);
      active_timer.reset();
    }

    // timer cancellation is used to wake async waiters on completion/shutdown
    if (ec && ec != boost::asio::error::operation_aborted) {
      return false;
    }

    return completed.load(std::memory_order_acquire);
  }

  std::unique_lock lock(mtx);
  return cv.wait_for(lock, timeout,
                     [this] { return completed.load(std::memory_order_acquire); });
}

void RestoreWaiter::complete(bool success, int result_code) {
  failed.store(!success, std::memory_order_release);
  result.store(result_code, std::memory_order_release);
  completed.store(true, std::memory_order_release);

  cv.notify_all();

  std::shared_ptr<Timer> timer;
  {
    std::lock_guard lock(timer_mtx);
    timer = active_timer.lock();
  }
  if (timer) {
    timer->cancel();
  }
}

// RestoreWaiterPool implementation
std::shared_ptr<RestoreWaiter> RestoreWaiterPool::acquire(std::weak_ptr<RestoreWaiterRegistry> owner) {
  std::unique_lock lock(pool_mtx);

  // Periodic eviction of old waiters
  evict_old_waiters();

  if (!free_list.empty()) {
    auto waiter = std::move(free_list.back());
    free_list.pop_back();
    lock.unlock();

    // Reset state
    waiter->reset();

    return std::shared_ptr<RestoreWaiter>(waiter.release(),
      [owner](RestoreWaiter* w) {
        if (auto reg = owner.lock()) {
          reg->release_waiter(w);
        } else {
          delete w;
        }
      });
  }

  lock.unlock();
  auto waiter = std::make_unique<RestoreWaiter>();
  return std::shared_ptr<RestoreWaiter>(waiter.release(),
    [owner](RestoreWaiter* w) {
      if (auto reg = owner.lock()) {
        reg->release_waiter(w);
      } else {
        delete w;
      }
    });
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

void RestoreWaiterRegistry::release_waiter(RestoreWaiter* waiter) {
  waiter_pool.release(waiter);
}

std::shared_ptr<RestoreWaiter> RestoreWaiterRegistry::register_waiter(const rgw_bucket& bucket,
                                                                        const rgw_obj_key& obj_key) {
  if (shutting_down.load(std::memory_order_acquire)) {
    return nullptr;
  }

  auto self = shared_from_this();
  auto waiter = waiter_pool.acquire(self);
  waiter->cached_key = make_key(bucket, obj_key);

  std::unique_lock lock(registry_mtx);
  if (shutting_down.load(std::memory_order_relaxed)) {
    return nullptr;
  }
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
    waiter->complete(success, result);
  }
}

void RestoreWaiterRegistry::shutdown() {
  shutting_down.store(true, std::memory_order_release);
  std::vector<std::shared_ptr<RestoreWaiter>> to_notify;

  {
    std::unique_lock lock(registry_mtx);
    for (auto& [_, vec] : waiters) {
      to_notify.insert(to_notify.end(), vec.begin(), vec.end());
    }
    waiters.clear();
  }

  for (auto& waiter : to_notify) {
    waiter->complete(false, -ECANCELED);
  }
}

} // namespace rgw::restore
