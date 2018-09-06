// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#pragma once

#include "lock_policy.h"
#ifdef NDEBUG
#include <mutex>
#else
#include "mutex_debug.h"
#endif

namespace ceph {

// empty helper class except when the template argument is LockPolicy::MUTEX
template<LockPolicy lp>
class LockMutex {
  struct Dummy {
    void lock() {}
    bool try_lock() {
      return true;
    }
    void unlock() {}
    bool is_locked() const {
      return true;
    }
  };
public:
  using type = Dummy;
  // discard the constructor params
  template<typename... Args>
  static Dummy create(Args&& ...) {
    return Dummy{};
  }
};

#ifdef NDEBUG
template<>
class LockMutex<LockPolicy::MUTEX> {
public:
  using type = std::mutex;
  // discard the constructor params
  template<typename... Args>
  static std::mutex create(Args&& ...) {
    return std::mutex{};
  }
};
#else
template<>
class LockMutex<LockPolicy::MUTEX> {
public:
  using type = ceph::mutex_debug;
  template<typename... Args>
  static ceph::mutex_debug create(Args&& ...args) {
    return ceph::mutex_debug{std::forward<Args>(args)...};
  }
};
#endif	// NDEBUG

template<LockPolicy lp> using LockMutexT = typename LockMutex<lp>::type;

} // namespace ceph
