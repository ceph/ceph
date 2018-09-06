// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "lock_policy.h"
#ifdef NDEBUG
#include <shared_mutex>
#else
#include "shared_mutex_debug.h"
#endif

namespace ceph {

// empty helper class except when the template argument is LockPolicy::MUTEX
template<LockPolicy lp>
class SharedMutex {
  struct Dummy {
    // exclusive lock
    void lock() {}
    bool try_lock() {
      return true;
    }
    void unlock() {}
    // shared lock
    void lock_shared() {}
    bool try_lock_shared() {
      return true;
    }
    void unlock_shared() {}
  };
public:
  using type = Dummy;
  template<typename... Args>
  static Dummy create(Args&& ...) {
    return Dummy{};
  }
};

#ifdef NDEBUG
template<>
class SharedMutex<LockPolicy::MUTEX>
{
public:
  using type = std::shared_mutex;
  // discard the constructor params
  template<typename... Args>
  static std::shared_mutex create(Args&&... args) {
    return std::shared_mutex{};
  }
};
#else
template<>
class SharedMutex<LockPolicy::MUTEX> {
public:
  using type = ceph::shared_mutex_debug;
  template<typename... Args>
  static ceph::shared_mutex_debug create(Args&&... args) {
    return ceph::shared_mutex_debug{std::forward<Args>(args)...};
  }
};
#endif	// NDEBUG

template<LockPolicy lp> using SharedMutexT = typename SharedMutex<lp>::type;

} // namespace ceph

