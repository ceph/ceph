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

namespace std {

// specialize unique_lock and shared_lock for SharedMutex to operate on
// SharedMutexImpl instead, because the locks may outlive the SharedMutex itself

template <typename Executor>
class unique_lock<ceph::async::SharedMutex<Executor>> {
 public:
  using mutex_type = boost::intrusive_ptr<ceph::async::detail::SharedMutexImpl>;

  unique_lock() = default;
  explicit unique_lock(ceph::async::SharedMutex<Executor>& m)
    : impl(m.impl), locked(true)
  {
    impl->lock();
  }
  unique_lock(ceph::async::SharedMutex<Executor>& m, defer_lock_t t) noexcept
    : impl(m.impl)
  {}
  unique_lock(ceph::async::SharedMutex<Executor>& m, try_to_lock_t t)
    : impl(m.impl), locked(impl->try_lock())
  {}
  unique_lock(ceph::async::SharedMutex<Executor>& m, adopt_lock_t t) noexcept
    : impl(m.impl), locked(true)
  {}
  ~unique_lock() {
    if (impl && locked)
      impl->unlock();
  }

  unique_lock(unique_lock&& other) noexcept
    : impl(std::move(other.impl)),
      locked(other.locked) {
    other.locked = false;
  }
  unique_lock& operator=(unique_lock&& other) noexcept {
    if (impl && locked) {
      impl->unlock();
    }
    impl = std::move(other.impl);
    locked = other.locked;
    other.locked = false;
    return *this;
  }
  void swap(unique_lock& other) noexcept {
    using std::swap;
    swap(impl, other.impl);
    swap(locked, other.locked);
  }

  mutex_type mutex() const noexcept { return impl; }
  bool owns_lock() const noexcept { return impl && locked; }
  explicit operator bool() const noexcept { return impl && locked; }

  mutex_type release() {
    auto result = std::move(impl);
    locked = false;
    return result;
  }

  void lock() {
    if (!impl)
      throw system_error(make_error_code(errc::operation_not_permitted));
    if (locked)
      throw system_error(make_error_code(errc::resource_deadlock_would_occur));
    impl->lock();
    locked = true;
  }
  bool try_lock() {
    if (!impl)
      throw system_error(make_error_code(errc::operation_not_permitted));
    if (locked)
      throw system_error(make_error_code(errc::resource_deadlock_would_occur));
    return locked = impl->try_lock();
  }
  void unlock() {
    if (!impl || !locked)
      throw system_error(make_error_code(errc::operation_not_permitted));
    impl->unlock();
    locked = false;
  }
 private:
  mutex_type impl;
  bool locked{false};
};

template <typename Executor>
class shared_lock<ceph::async::SharedMutex<Executor>> {
 public:
  using mutex_type = boost::intrusive_ptr<ceph::async::detail::SharedMutexImpl>;

  shared_lock() = default;
  explicit shared_lock(ceph::async::SharedMutex<Executor>& m)
    : impl(m.impl), locked(true)
  {
    impl->lock_shared();
  }
  shared_lock(ceph::async::SharedMutex<Executor>& m, defer_lock_t t) noexcept
    : impl(m.impl)
  {}
  shared_lock(ceph::async::SharedMutex<Executor>& m, try_to_lock_t t)
    : impl(m.impl), locked(impl->try_lock_shared())
  {}
  shared_lock(ceph::async::SharedMutex<Executor>& m, adopt_lock_t t) noexcept
    : impl(m.impl), locked(true)
  {}

  ~shared_lock() {
    if (impl && locked)
      impl->unlock_shared();
  }

  shared_lock(shared_lock&& other) noexcept
    : impl(std::move(other.impl)),
      locked(other.locked) {
    other.locked = false;
  }
  shared_lock& operator=(shared_lock&& other) noexcept {
    if (impl && locked) {
      impl->unlock_shared();
    }
    impl = std::move(other.impl);
    locked = other.locked;
    other.locked = false;
    return *this;
  }
  void swap(shared_lock& other) noexcept {
    using std::swap;
    swap(impl, other.impl);
    swap(locked, other.locked);
  }

  mutex_type mutex() const noexcept { return impl; }
  bool owns_lock() const noexcept { return impl && locked; }
  explicit operator bool() const noexcept { return impl && locked; }

  mutex_type release() {
    auto result = std::move(impl);
    locked = false;
    return result;
  }

  void lock() {
    if (!impl)
      throw system_error(make_error_code(errc::operation_not_permitted));
    if (locked)
      throw system_error(make_error_code(errc::resource_deadlock_would_occur));
    impl->lock_shared();
    locked = true;
  }
  bool try_lock() {
    if (!impl)
      throw system_error(make_error_code(errc::operation_not_permitted));
    if (locked)
      throw system_error(make_error_code(errc::resource_deadlock_would_occur));
    return locked = impl->try_lock_shared();
  }
  void unlock() {
    if (!impl || !locked)
      throw system_error(make_error_code(errc::operation_not_permitted));
    impl->unlock_shared();
    locked = false;
  }
 private:
  mutex_type impl;
  bool locked{false};
};

} // namespace std
