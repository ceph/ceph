// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#pragma once

#include "lock_policy.h"
#include "Mutex.h"

class SharedLRUTest;

namespace ceph {

template<LockPolicy lp> class LockCond;

// empty helper class except when the template argument is LockPolicy::MUTEX
template<LockPolicy lp>
class LockMutex {
public:
  template<typename... Args>
  LockMutex(Args&& ...) {}
  auto operator()() const {
    struct Locker {};
    return Locker{};
  }
  bool is_locked() const {
    return true;
  }
};

template<>
class LockMutex<LockPolicy::MUTEX> {
public:
  template<typename... Args>
  LockMutex(Args&& ...args)
    : mutex{std::forward<Args>(args)...}
  {}
  auto operator()() const {
    return Mutex::Locker{mutex};
  }
  bool is_locked() const {
    return mutex.is_locked();
  }
private:
  Mutex& get() {
    return mutex;
  }
  mutable Mutex mutex;
  friend class LockCond<LockPolicy::MUTEX>;
  friend class ::SharedLRUTest;
};

} // namespace ceph
