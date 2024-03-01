// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <utility>
#include "common/containers.h"

// What and why
// ============
//
// For general code making use of mutexes, use these ceph:: types.
// The key requirement is that you make use of the ceph::make_mutex()
// and make_recursive_mutex() factory methods, which take a string
// naming the mutex for the purposes of the lockdep debug variant.

#if defined(WITH_SEASTAR) && !defined(WITH_ALIEN)
#include <seastar/core/condition-variable.hh>

#include "crimson/common/log.h"
#include "include/ceph_assert.h"

#ifndef NDEBUG
#define FUT_DEBUG(FMT_MSG, ...) crimson::get_logger(ceph_subsys_).trace(FMT_MSG, ##__VA_ARGS__)
#else
#define FUT_DEBUG(FMT_MSG, ...)
#endif

namespace ceph {
  // an empty class satisfying the mutex concept
  struct dummy_mutex {
    void lock() {}
    bool try_lock() {
      return true;
    }
    void unlock() {}
    void lock_shared() {}
    void unlock_shared() {}
  };

  struct dummy_shared_mutex : dummy_mutex {
    void lock_shared() {}
    void unlock_shared() {}
  };

  // this implementation assumes running within a seastar::thread
  struct green_condition_variable : private seastar::condition_variable {
    template <class LockT>
    void wait(LockT&&) {
      FUT_DEBUG("green_condition_variable::{}: before blocking", __func__);
      seastar::condition_variable::wait().get();
      FUT_DEBUG("green_condition_variable::{}: after blocking", __func__);
    }

    void notify_one() noexcept {
      FUT_DEBUG("green_condition_variable::{}", __func__);
      signal();
    }

    void notify_all() noexcept {
      FUT_DEBUG("green_condition_variable::{}", __func__);
      broadcast();
    }
  };

  using mutex = dummy_mutex;
  using recursive_mutex = dummy_mutex;
  using shared_mutex = dummy_shared_mutex;
  using condition_variable = green_condition_variable;

  template <typename ...Args>
  dummy_mutex make_mutex(Args&& ...args) {
    return {};
  }

  template <typename ...Args>
  recursive_mutex make_recursive_mutex(Args&& ...args) {
    return {};
  }

  template <typename ...Args>
  shared_mutex make_shared_mutex(Args&& ...args) {
    return {};
  }

  static constexpr bool mutex_debugging = false;
  #define ceph_mutex_is_locked(m) true
  #define ceph_mutex_is_locked_by_me(m) true
}

#else  // defined (WITH_SEASTAR) && !defined(WITH_ALIEN)
//
// For legacy Mutex users that passed recursive=true, use
// ceph::make_recursive_mutex.  For legacy Mutex users that passed
// lockdep=false, use std::mutex directly.

#ifdef CEPH_DEBUG_MUTEX

// ============================================================================
// debug (lockdep-capable, various sanity checks and asserts)
// ============================================================================
//
// Note: this is known to cause deadlocks on Windows because
// of the winpthreads shared mutex implementation.

#include "common/condition_variable_debug.h"
#include "common/mutex_debug.h"
#include "common/shared_mutex_debug.h"

namespace ceph {
  typedef ceph::mutex_debug mutex;
  typedef ceph::mutex_recursive_debug recursive_mutex;
  typedef ceph::condition_variable_debug condition_variable;
  typedef ceph::shared_mutex_debug shared_mutex;

  // pass arguments to mutex_debug ctor
  template <typename ...Args>
  mutex make_mutex(Args&& ...args) {
    return {std::forward<Args>(args)...};
  }

  // pass arguments to recursive_mutex_debug ctor
  template <typename ...Args>
  recursive_mutex make_recursive_mutex(Args&& ...args) {
    return {std::forward<Args>(args)...};
  }

  // pass arguments to shared_mutex_debug ctor
  template <typename ...Args>
  shared_mutex make_shared_mutex(Args&& ...args) {
    return {std::forward<Args>(args)...};
  }

  static constexpr bool mutex_debugging = true;

  // debug methods
  #define ceph_mutex_is_locked(m) ((m).is_locked())
  #define ceph_mutex_is_not_locked(m) (!(m).is_locked())
  #define ceph_mutex_is_rlocked(m) ((m).is_rlocked())
  #define ceph_mutex_is_wlocked(m) ((m).is_wlocked())
  #define ceph_mutex_is_locked_by_me(m) ((m).is_locked_by_me())
  #define ceph_mutex_is_not_locked_by_me(m) (!(m).is_locked_by_me())
}

#else

// ============================================================================
// release (fast and minimal)
// ============================================================================

#include <condition_variable>
#include <mutex>

// The winpthreads shared mutex implementation is broken.
// We'll use boost::shared_mutex instead.
// https://github.com/msys2/MINGW-packages/issues/3319
#if defined(__MINGW32__) && !defined(__clang__)
#include <boost/thread/shared_mutex.hpp>
#else
#include <shared_mutex>
#endif

namespace ceph {

  typedef std::mutex mutex;
  typedef std::recursive_mutex recursive_mutex;
  typedef std::condition_variable condition_variable;

#if defined(__MINGW32__) && !defined(__clang__)
  typedef boost::shared_mutex shared_mutex;
#else
  typedef std::shared_mutex shared_mutex;
#endif

  // discard arguments to make_mutex (they are for debugging only)
  template <typename ...Args>
  mutex make_mutex(Args&& ...args) {
    return {};
  }
  template <typename ...Args>
  recursive_mutex make_recursive_mutex(Args&& ...args) {
    return {};
  }
  template <typename ...Args>
  shared_mutex make_shared_mutex(Args&& ...args) {
    return {};
  }

  static constexpr bool mutex_debugging = false;

  // debug methods.  Note that these can blindly return true
  // because any code that does anything other than assert these
  // are true is broken.
  #define ceph_mutex_is_locked(m) true
  #define ceph_mutex_is_not_locked(m) true
  #define ceph_mutex_is_rlocked(m) true
  #define ceph_mutex_is_wlocked(m) true
  #define ceph_mutex_is_locked_by_me(m) true
  #define ceph_mutex_is_not_locked_by_me(m) true

}

#endif	// CEPH_DEBUG_MUTEX

#endif	// WITH_SEASTAR

namespace ceph {

template <class LockT,
          class LockFactoryT>
ceph::containers::tiny_vector<LockT> make_lock_container(
  const std::size_t num_instances,
  LockFactoryT&& lock_factory)
{
  return {
    num_instances, [&](const std::size_t i, auto emplacer) {
      // this will be called `num_instances` times
      new (emplacer.data()) LockT {lock_factory(i)};
    }
  };
}
} // namespace ceph

