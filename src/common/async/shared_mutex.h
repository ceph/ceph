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

#ifndef CEPH_ASYNC_SHARED_MUTEX_H
#define CEPH_ASYNC_SHARED_MUTEX_H

#include <condition_variable>
#include <mutex>
#include <shared_mutex> // for std::shared_lock

#include <boost/intrusive/list.hpp>

#include "common/async/completion.h"

namespace ceph::async {

/**
 * An asynchronous shared mutex for use with boost::asio.
 *
 * A shared mutex class with asynchronous lock operations that complete on a
 * boost::asio executor. The class also has synchronous interfaces that meet
 * most of the standard library's requirements for the SharedMutex concept,
 * which makes it compatible with lock_guard, unique_lock, and shared_lock.
 *
 * All lock requests can fail with operation_aborted on cancel() or destruction.
 * The non-error_code overloads of lock() and lock_shared() will throw this
 * error as an exception of type boost::system::system_error.
 *
 * Exclusive locks are prioritized over shared locks. Locks of the same type
 * are granted in fifo order. The implementation defines a limit on the number
 * of shared locks to 65534 at a time.
 *
 * Example use:
 *
 *   boost::asio::io_context context;
 *   SharedMutex mutex{context.get_executor()};
 *
 *   mutex.async_lock([&] (boost::system::error_code ec, auto lock) {
 *       if (!ec) {
 *         // mutate shared state ...
 *       }
 *     });
 *   mutex.async_lock_shared([&] (boost::system::error_code ec, auto lock) {
 *       if (!ec) {
 *         // read shared state ...
 *       }
 *     });
 *
 *   context.run();
 */
template <typename Executor>
class SharedMutex {
 public:
  SharedMutex(const Executor& ex1);

  /// on destruction, all pending lock requests are canceled
  ~SharedMutex();

  using executor_type = Executor;
  executor_type get_executor() const noexcept { return ex1; }

  /// initiate an asynchronous request for an exclusive lock. when the lock is
  /// granted, the completion handler is invoked with a successful error code
  /// and a std::unique_lock that owns this mutex.
  /// Signature = void(boost::system::error_code, std::unique_lock)
  template <typename CompletionToken>
  auto async_lock(CompletionToken&& token);

  /// wait synchronously for an exclusive lock. if an error occurs before the
  /// lock is granted, that error is thrown as an exception
  void lock();

  /// wait synchronously for an exclusive lock. if an error occurs before the
  /// lock is granted, that error is assigned to 'ec'
  void lock(boost::system::error_code& ec);

  /// try to acquire an exclusive lock. if the lock is not immediately
  /// available, returns false
  bool try_lock();

  /// releases an exclusive lock. not required to be called from the same thread
  /// that initiated the lock
  void unlock();

  /// initiate an asynchronous request for a shared lock. when the lock is
  /// granted, the completion handler is invoked with a successful error code
  /// and a std::shared_lock that owns this mutex.
  /// Signature = void(boost::system::error_code, std::shared_lock)
  template <typename CompletionToken>
  auto async_lock_shared(CompletionToken&& token);

  /// wait synchronously for a shared lock. if an error occurs before the
  /// lock is granted, that error is thrown as an exception
  void lock_shared();

  /// wait synchronously for a shared lock. if an error occurs before the lock
  /// is granted, that error is assigned to 'ec'
  void lock_shared(boost::system::error_code& ec);

  /// try to acquire a shared lock. if the lock is not immediately available,
  /// returns false
  bool try_lock_shared();

  /// releases a shared lock. not required to be called from the same thread
  /// that initiated the lock
  void unlock_shared();

  /// cancel any pending requests for exclusive or shared locks with an
  /// operation_aborted error
  void cancel();

 private:
  Executor ex1; //< default callback executor

  struct LockRequest : public boost::intrusive::list_base_hook<> {
    virtual ~LockRequest() {}
    virtual void complete(boost::system::error_code ec) = 0;
    virtual void destroy() = 0;
  };
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

  // sync requests live on the stack and wait on a condition variable
  class SyncRequest;

  // async requests use async::Completion to invoke a handler on its executor
  template <template <typename Mutex> typename Lock>
  class AsyncRequest;

  using AsyncExclusiveRequest = AsyncRequest<std::unique_lock>;
  using AsyncSharedRequest = AsyncRequest<std::shared_lock>;

  void complete(RequestList&& requests, boost::system::error_code ec);
};

template <typename Executor>
class SharedMutex<Executor>::SyncRequest : public LockRequest {
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

template <typename Executor>
template <template <typename Mutex> typename Lock>
class SharedMutex<Executor>::AsyncRequest : public LockRequest {
  SharedMutex& mutex; //< mutex argument for lock guard
 public:
  AsyncRequest(SharedMutex& mutex) : mutex(mutex) {}

  using Signature = void(boost::system::error_code, Lock<SharedMutex>);
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


template <typename Executor>
inline SharedMutex<Executor>::SharedMutex(const Executor& ex1)
  : ex1(ex1)
{
}

template <typename Executor>
inline SharedMutex<Executor>::~SharedMutex()
{
  try {
    cancel();
  } catch (const std::exception&) {
    // swallow any exceptions, the destructor can't throw
  }
}

template <typename Executor>
template <typename CompletionToken>
auto SharedMutex<Executor>::async_lock(CompletionToken&& token)
{
  using Signature = typename AsyncExclusiveRequest::Signature;
  boost::asio::async_completion<CompletionToken, Signature> init(token);
  auto& handler = init.completion_handler;
  {
    std::lock_guard lock{mutex};

    if (state == Unlocked) {
      state = Exclusive;

      // post the completion
      auto ex2 = boost::asio::get_associated_executor(handler, ex1);
      auto alloc2 = boost::asio::get_associated_allocator(handler);
      auto b = bind_handler(std::move(handler), boost::system::error_code{},
                            std::unique_lock{*this, std::adopt_lock});
      ex2.post(forward_handler(std::move(b)), alloc2);
    } else {
      // create a request and add it to the exclusive list
      using LockCompletion = typename AsyncExclusiveRequest::LockCompletion;
      auto request = LockCompletion::create(ex1, std::move(handler), *this);
      exclusive_queue.push_back(*request.release());
    }
  }
  return init.result.get();
}

template <typename Executor>
inline void SharedMutex<Executor>::lock()
{
  boost::system::error_code ec;
  lock(ec);
  if (ec) {
    throw boost::system::system_error(ec);
  }
}

template <typename Executor>
void SharedMutex<Executor>::lock(boost::system::error_code& ec)
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

template <typename Executor>
inline bool SharedMutex<Executor>::try_lock()
{
  std::lock_guard lock{mutex};

  if (state == Unlocked) {
    state = Exclusive;
    return true;
  }
  return false;
}

template <typename Executor>
void SharedMutex<Executor>::unlock()
{
  RequestList granted;
  {
    std::lock_guard lock{mutex};
    assert(state == Exclusive);

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

template <typename Executor>
template <typename CompletionToken>
auto SharedMutex<Executor>::async_lock_shared(CompletionToken&& token)
{
  using Signature = typename AsyncSharedRequest::Signature;
  boost::asio::async_completion<CompletionToken, Signature> init(token);
  auto& handler = init.completion_handler;
  {
    std::lock_guard lock{mutex};

    if (exclusive_queue.empty() && state < MaxShared) {
      state++;

      auto ex2 = boost::asio::get_associated_executor(handler, ex1);
      auto alloc2 = boost::asio::get_associated_allocator(handler);
      auto b = bind_handler(std::move(handler), boost::system::error_code{},
                            std::shared_lock{*this, std::adopt_lock});
      ex2.post(forward_handler(std::move(b)), alloc2);
    } else {
      using LockCompletion = typename AsyncSharedRequest::LockCompletion;
      auto request = LockCompletion::create(ex1, std::move(handler), *this);
      shared_queue.push_back(*request.release());
    }
  }
  return init.result.get();
}

template <typename Executor>
inline void SharedMutex<Executor>::lock_shared()
{
  boost::system::error_code ec;
  lock_shared(ec);
  if (ec) {
    throw boost::system::system_error(ec);
  }
}

template <typename Executor>
void SharedMutex<Executor>::lock_shared(boost::system::error_code& ec)
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

template <typename Executor>
inline bool SharedMutex<Executor>::try_lock_shared()
{
  std::lock_guard lock{mutex};

  if (exclusive_queue.empty() && state < MaxShared) {
    state++;
    return true;
  }
  return false;
}

template <typename Executor>
inline void SharedMutex<Executor>::unlock_shared()
{
  std::lock_guard lock{mutex};
  assert(state != Unlocked && state <= MaxShared);

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

template <typename Executor>
inline void SharedMutex<Executor>::cancel()
{
  RequestList canceled;
  {
    std::lock_guard lock{mutex};
    canceled.splice(canceled.end(), shared_queue);
    canceled.splice(canceled.end(), exclusive_queue);
  }
  complete(std::move(canceled), boost::asio::error::operation_aborted);
}

template <typename Executor>
void SharedMutex<Executor>::complete(RequestList&& requests,
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

} // namespace ceph::async

#endif // CEPH_ASYNC_SHARED_MUTEX_H
