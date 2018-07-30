// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#pragma once

#include "lock_policy.h"
#include <memory>
#include <boost/smart_ptr/local_shared_ptr.hpp>

namespace ceph::internal {

template<LockPolicy lock_policy>
struct SharedPtrTrait {
  template<class T> using shared_ptr = boost::local_shared_ptr<T>;
  template<class T> using weak_ptr = boost::weak_ptr<T>;
};

template<>
struct SharedPtrTrait<LockPolicy::MUTEX> {
  template<class T> using shared_ptr = std::shared_ptr<T>;
  template<class T> using weak_ptr = std::weak_ptr<T>;
};

}
