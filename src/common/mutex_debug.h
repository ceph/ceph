// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_COMMON_MUTEX_DEBUG_H
#define CEPH_COMMON_MUTEX_DEBUG_H

#include <system_error>
#include <thread>

#include <pthread.h>

#include "include/ceph_assert.h"

#include "ceph_time.h"
#include "likely.h"
#include "lockdep.h"

class CephContext;
class PerfCounters;

namespace ceph {
namespace mutex_debug_detail {

class mutex_debugging_base
{
protected:
  std::string name;
  int id;
  bool backtrace; // gather backtrace on lock acquisition

  int nlock;
  std::thread::id locked_by;


  void _register();
  void _will_lock(bool recursive=false); // about to lock
  void _locked(); // just locked
  void _will_unlock(); // about to unlock

  mutex_debugging_base(const std::string &n, bool bt = false);
  mutex_debugging_base(const char *n, bool bt = false);
  ~mutex_debugging_base();

public:
  bool is_locked() const {
    return (nlock > 0);
  }
  bool is_locked_by_me() const {
    return nlock > 0 && locked_by == std::this_thread::get_id();
  }
  operator bool() const {
    return nlock > 0 && locked_by == std::this_thread::get_id();
  }
};

// Since this is a /debugging/ mutex just define it in terms of the
// pthread error check mutex.
template<bool Recursive>
class mutex_debug_impl : public mutex_debugging_base
{
private:
  pthread_mutex_t m;

  void _init() {
    pthread_mutexattr_t a;
    pthread_mutexattr_init(&a);
    int r;
    if (recursive)
      r = pthread_mutexattr_settype(&a, PTHREAD_MUTEX_RECURSIVE);
    else
      r = pthread_mutexattr_settype(&a, PTHREAD_MUTEX_ERRORCHECK);
    ceph_assert(r == 0);
    r = pthread_mutex_init(&m, &a);
    ceph_assert(r == 0);
  }

  bool enable_lockdep(bool no_lockdep) const {
    if (recursive) {
      return false;
    } else if (no_lockdep) {
      return false;
    } else {
      return g_lockdep;
    }
  }

public:
  static constexpr bool recursive = Recursive;

  // Mutex concept is DefaultConstructible
  mutex_debug_impl(const std::string &n, bool bt = false)
    : mutex_debugging_base(n, bt) {
    _init();
  }
  mutex_debug_impl(const char *n, bool bt = false)
    : mutex_debugging_base(n, bt) {
    _init();
  }

  // Mutex is Destructible
  ~mutex_debug_impl() {
    int r = pthread_mutex_destroy(&m);
    ceph_assert(r == 0);
  }

  // Mutex concept is non-Copyable
  mutex_debug_impl(const mutex_debug_impl&) = delete;
  mutex_debug_impl& operator =(const mutex_debug_impl&) = delete;

  // Mutex concept is non-Movable
  mutex_debug_impl(mutex_debug_impl&&) = delete;
  mutex_debug_impl& operator =(mutex_debug_impl&&) = delete;

  void lock_impl() {
    int r = pthread_mutex_lock(&m);
    // Allowed error codes for Mutex concept
    if (unlikely(r == EPERM ||
		 r == EDEADLK ||
		 r == EBUSY)) {
      throw std::system_error(r, std::generic_category());
    }
    ceph_assert(r == 0);
  }

  void unlock_impl() noexcept {
    int r = pthread_mutex_unlock(&m);
    ceph_assert(r == 0);
  }

  bool try_lock_impl() {
    int r = pthread_mutex_trylock(&m);
    switch (r) {
    case 0:
      return true;
    case EBUSY:
      return false;
    default:
      throw std::system_error(r, std::generic_category());
    }
  }
  pthread_mutex_t* native_handle() {
    return &m;
  }

  void _post_lock() {
    if (!recursive)
      ceph_assert(nlock == 0);
    locked_by = std::this_thread::get_id();
    nlock++;
  }

  void _pre_unlock() {
    ceph_assert(nlock > 0);
    --nlock;
    ceph_assert(locked_by == std::this_thread::get_id());
    if (!recursive)
      ceph_assert(nlock == 0);
    if (nlock == 0)
      locked_by = std::thread::id();
  }

  bool try_lock(bool no_lockdep = false) {
    bool locked = try_lock_impl();
    if (locked) {
      if (enable_lockdep(no_lockdep))
	_locked();
      _post_lock();
    }
    return locked;
  }

  void lock(bool no_lockdep = false) {
    if (enable_lockdep(no_lockdep))
      _will_lock(recursive);

    if (try_lock())
      return;

    lock_impl();
    if (enable_lockdep(no_lockdep))
      _locked();
    _post_lock();
  }

  void unlock(bool no_lockdep = false) {
    _pre_unlock();
    if (enable_lockdep(no_lockdep))
      _will_unlock();
    unlock_impl();
  }

};


} // namespace mutex_debug_detail
typedef mutex_debug_detail::mutex_debug_impl<false> mutex_debug;
typedef mutex_debug_detail::mutex_debug_impl<true> mutex_recursive_debug;
} // namespace ceph

#endif
