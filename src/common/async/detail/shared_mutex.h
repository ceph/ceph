// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <condition_variable>
#include <mutex>
#include <optional>
#include <shared_mutex> // for std::shared_lock

#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/intrusive/list.hpp>

#include "include/ceph_assert.h"

#include "common/async/completion.h"

namespace ceph::async::detail {

struct LockRequest : public boost::intrusive::list_base_hook<> {
  virtual ~LockRequest() {}
  virtual void complete(boost::system::error_code ec) = 0;
  virtual void destroy() = 0;
};

class SharedMutexImpl : public boost::intrusive_ref_counter<SharedMutexImpl> {
 public:
  ~SharedMutexImpl();

  template <typename Mutex, typename CompletionToken>
  auto async_lock(Mutex& mtx, CompletionToken&& token);
  void lock();
  void lock(boost::system::error_code& ec);
  bool try_lock();
  void unlock();
  template <typename Mutex, typename CompletionToken>
  auto async_lock_shared(Mutex& mtx, CompletionToken&& token);
  void lock_shared();
  void lock_shared(boost::system::error_code& ec);
  bool try_lock_shared();
  void unlock_shared();
  void cancel();

 private:
  using RequestList = boost::intrusive::list<LockRequest>;

  RequestList shared_queue; //< requests waiting on a shared lock
  RequestList exclusive_queue; //< requests waiting on an exclusive lock

  /// lock state encodes the number of shared lockers, or 'max' for exclusive
  using LockState = uint16_t;
  static constexpr LockState Unlocked = 0;
  static constexpr LockState Exclusive = std::numeric_limits<LockState>::max();
  static constexpr LockState MaxShared = Exclusive - 1;
  LockState state = Unlocked; //< current lock state

  std::mutex mutex; //< protects lock state and wait queues

  void complete(RequestList&& requests, boost::system::error_code ec);
};

// sync requests live on the stack and wait on a condition variable
class SyncRequest : public LockRequest {
  std::condition_variable cond;
  std::optional<boost::system::error_code> ec;
 public:
  boost::system::error_code wait(std::unique_lock<std::mutex>& lock) {
    // return the error code once its been set
    cond.wait(lock, [this] { return ec; });
    return *ec;
  }
  void complete(boost::system::error_code ec) override {
    this->ec = ec;
    cond.notify_one();
  }
  void destroy() override {
    // nothing, SyncRequests live on the stack
  }
};

// async requests use async::Completion to invoke a handler on its executor
template <typename Mutex, template <typename> typename Lock>
class AsyncRequest : public LockRequest {
  Mutex& mutex; //< mutex argument for lock guard
 public:
  explicit AsyncRequest(Mutex& mutex) : mutex(mutex) {}

  using Signature = void(boost::system::error_code, Lock<Mutex>);
  using LockCompletion = Completion<Signature, AsBase<AsyncRequest>>;

  void complete(boost::system::error_code ec) override {
    auto r = static_cast<LockCompletion*>(this);
    // pass ownership of ourselves to post(). on error, pass an empty lock
    post(std::unique_ptr<LockCompletion>{r}, ec,
         ec ? Lock{mutex, std::defer_lock} : Lock{mutex, std::adopt_lock});
  }
  void destroy() override {
    delete static_cast<LockCompletion*>(this);
  }
};

inline SharedMutexImpl::~SharedMutexImpl()
{
  ceph_assert(state == Unlocked);
  ceph_assert(shared_queue.empty());
  ceph_assert(exclusive_queue.empty());
}

template <typename Mutex, typename CompletionToken>
auto SharedMutexImpl::async_lock(Mutex& mtx, CompletionToken&& token)
{
  using Request = AsyncRequest<Mutex, std::unique_lock>;
  using Signature = typename Request::Signature;
  boost::asio::async_completion<CompletionToken, Signature> init(token);
  auto& handler = init.completion_handler;
  auto ex1 = mtx.get_executor();
  {
    std::lock_guard lock{mutex};

    boost::system::error_code ec;
    if (state == Unlocked) {
      state = Exclusive;

      // post a successful completion
      auto ex2 = boost::asio::get_associated_executor(handler, ex1);
      auto h = boost::asio::bind_executor(ex2, std::move(handler));
      boost::asio::post(bind_handler(std::move(h), ec,
                                     std::unique_lock{mtx, std::adopt_lock}));
    } else {
      // create a request and add it to the exclusive list
      using LockCompletion = typename Request::LockCompletion;
      auto request = LockCompletion::create(ex1, std::move(handler), mtx);
      exclusive_queue.push_back(*request.release());
    }
  }
  return init.result.get();
}

inline void SharedMutexImpl::lock()
{
  boost::system::error_code ec;
  lock(ec);
  if (ec) {
    throw boost::system::system_error(ec);
  }
}

void SharedMutexImpl::lock(boost::system::error_code& ec)
{
  std::unique_lock lock{mutex};

  if (state == Unlocked) {
    state = Exclusive;
    ec.clear();
  } else {
    SyncRequest request;
    exclusive_queue.push_back(request);
    ec = request.wait(lock);
  }
}

inline bool SharedMutexImpl::try_lock()
{
  std::lock_guard lock{mutex};

  if (state == Unlocked) {
    state = Exclusive;
    return true;
  }
  return false;
}

void SharedMutexImpl::unlock()
{
  RequestList granted;
  {
    std::lock_guard lock{mutex};
    ceph_assert(state == Exclusive);

    if (!exclusive_queue.empty()) {
      // grant next exclusive lock
      auto& request = exclusive_queue.front();
      exclusive_queue.pop_front();
      granted.push_back(request);
    } else {
      // grant shared locks, if any
      state = shared_queue.size();
      if (state > MaxShared) {
        state = MaxShared;
        auto end = std::next(shared_queue.begin(), MaxShared);
        granted.splice(granted.end(), shared_queue,
                       shared_queue.begin(), end, MaxShared);
      } else {
        granted.splice(granted.end(), shared_queue);
      }
    }
  }
  complete(std::move(granted), boost::system::error_code{});
}

template <typename Mutex, typename CompletionToken>
auto SharedMutexImpl::async_lock_shared(Mutex& mtx, CompletionToken&& token)
{
  using Request = AsyncRequest<Mutex, std::shared_lock>;
  using Signature = typename Request::Signature;
  boost::asio::async_completion<CompletionToken, Signature> init(token);
  auto& handler = init.completion_handler;
  auto ex1 = mtx.get_executor();
  {
    std::lock_guard lock{mutex};

    boost::system::error_code ec;
    if (exclusive_queue.empty() && state < MaxShared) {
      state++;

      auto ex2 = boost::asio::get_associated_executor(handler, ex1);
      auto h = boost::asio::bind_executor(ex2, std::move(handler));
      boost::asio::post(bind_handler(std::move(h), ec,
                                     std::shared_lock{mtx, std::adopt_lock}));
    } else {
      using LockCompletion = typename Request::LockCompletion;
      auto request = LockCompletion::create(ex1, std::move(handler), mtx);
      shared_queue.push_back(*request.release());
    }
  }
  return init.result.get();
}

inline void SharedMutexImpl::lock_shared()
{
  boost::system::error_code ec;
  lock_shared(ec);
  if (ec) {
    throw boost::system::system_error(ec);
  }
}

void SharedMutexImpl::lock_shared(boost::system::error_code& ec)
{
  std::unique_lock lock{mutex};

  if (exclusive_queue.empty() && state < MaxShared) {
    state++;
    ec.clear();
  } else {
    SyncRequest request;
    shared_queue.push_back(request);
    ec = request.wait(lock);
  }
}

inline bool SharedMutexImpl::try_lock_shared()
{
  std::lock_guard lock{mutex};

  if (exclusive_queue.empty() && state < MaxShared) {
    state++;
    return true;
  }
  return false;
}

inline void SharedMutexImpl::unlock_shared()
{
  std::lock_guard lock{mutex};
  ceph_assert(state != Unlocked && state <= MaxShared);

  if (state == 1 && !exclusive_queue.empty()) {
    // grant next exclusive lock
    state = Exclusive;
    auto& request = exclusive_queue.front();
    exclusive_queue.pop_front();
    request.complete(boost::system::error_code{});
  } else if (state == MaxShared && !shared_queue.empty() &&
             exclusive_queue.empty()) {
    // grant next shared lock
    auto& request = shared_queue.front();
    shared_queue.pop_front();
    request.complete(boost::system::error_code{});
  } else {
    state--;
  }
}

inline void SharedMutexImpl::cancel()
{
  RequestList canceled;
  {
    std::lock_guard lock{mutex};
    canceled.splice(canceled.end(), shared_queue);
    canceled.splice(canceled.end(), exclusive_queue);
  }
  complete(std::move(canceled), boost::asio::error::operation_aborted);
}

void SharedMutexImpl::complete(RequestList&& requests,
                               boost::system::error_code ec)
{
  while (!requests.empty()) {
    auto& request = requests.front();
    requests.pop_front();
    try {
      request.complete(ec);
    } catch (...) {
      // clean up any remaining completions and rethrow
      requests.clear_and_dispose([] (LockRequest *r) { r->destroy(); });
      throw;
    }
  }
}

} // namespace ceph::async::detail
