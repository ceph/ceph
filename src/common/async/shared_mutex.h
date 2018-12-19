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

#include "common/async/detail/shared_mutex.h"

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
  explicit SharedMutex(const Executor& ex);

  /// on destruction, all pending lock requests are canceled
  ~SharedMutex();

  using executor_type = Executor;
  executor_type get_executor() const noexcept { return ex; }

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
  Executor ex; //< default callback executor
  boost::intrusive_ptr<detail::SharedMutexImpl> impl;

  // allow lock guards to access impl
  friend class std::unique_lock<SharedMutex>;
  friend class std::shared_lock<SharedMutex>;
};


template <typename Executor>
SharedMutex<Executor>::SharedMutex(const Executor& ex)
  : ex(ex), impl(new detail::SharedMutexImpl)
{
}

template <typename Executor>
SharedMutex<Executor>::~SharedMutex()
{
  try {
    impl->cancel();
  } catch (const std::exception&) {
    // swallow any exceptions, the destructor can't throw
  }
}

template <typename Executor>
template <typename CompletionToken>
auto SharedMutex<Executor>::async_lock(CompletionToken&& token)
{
  return impl->async_lock(*this, std::forward<CompletionToken>(token));
}

template <typename Executor>
void SharedMutex<Executor>::lock()
{
  impl->lock();
}

template <typename Executor>
void SharedMutex<Executor>::lock(boost::system::error_code& ec)
{
  impl->lock(ec);
}

template <typename Executor>
bool SharedMutex<Executor>::try_lock()
{
  return impl->try_lock();
}

template <typename Executor>
void SharedMutex<Executor>::unlock()
{
  impl->unlock();
}

template <typename Executor>
template <typename CompletionToken>
auto SharedMutex<Executor>::async_lock_shared(CompletionToken&& token)
{
  return impl->async_lock_shared(*this, std::forward<CompletionToken>(token));
}

template <typename Executor>
void SharedMutex<Executor>::lock_shared()
{
  impl->lock_shared();
}

template <typename Executor>
void SharedMutex<Executor>::lock_shared(boost::system::error_code& ec)
{
  impl->lock_shared(ec);
}

template <typename Executor>
bool SharedMutex<Executor>::try_lock_shared()
{
  return impl->try_lock_shared();
}

template <typename Executor>
void SharedMutex<Executor>::unlock_shared()
{
  impl->unlock_shared();
}

template <typename Executor>
void SharedMutex<Executor>::cancel()
{
  impl->cancel();
}

} // namespace ceph::async

#include "common/async/detail/shared_lock.h"
