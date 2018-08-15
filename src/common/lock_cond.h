// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#pragma once

#include "lock_policy.h"
#include "lock_mutex.h"
#include "Cond.h"

class SharedLRUTest;

namespace ceph {

// empty helper class except when the template argument is LockPolicy::MUTEX
template<LockPolicy lock_policy>
class LockCond {
public:
  int Wait(LockMutex<lock_policy>&) {
    return 0;
  }
  int Signal() {
    return 0;
  }
};

template<>
class LockCond<LockPolicy::MUTEX> {
public:
  int Wait(LockMutex<LockPolicy::MUTEX>& mutex) {
    return cond.Wait(mutex.get());
  }
  int Signal() {
    return cond.Signal();
  }
private:
  Cond& get() {
    return cond;
  }
  Cond cond;
  friend class ::SharedLRUTest;
};

} // namespace ceph

