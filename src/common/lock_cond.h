// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#pragma once

#include "lock_policy.h"
#include "lock_mutex.h"
#ifdef NDEBUG
#include <condition_variable>
#else
#include "common/condition_variable_debug.h"
#endif

class SharedLRUTest;

namespace ceph {

// empty helper class except when the template argument is LockPolicy::MUTEX
template<LockPolicy lock_policy>
class LockCond {
  // lockless condition_variable cannot be represented using
  // std::condition_variables interfaces.
};

#ifdef NDEBUG
template<>
class LockCond<LockPolicy::MUTEX>
{
public:
  using type = std::condition_variable;
};
#else
template<>
class LockCond<LockPolicy::MUTEX> {
public:
  using type = ceph::condition_variable_debug;
};

#endif	// NDEBUG

template<LockPolicy lp> using LockCondT = typename LockCond<lp>::type;

} // namespace ceph
