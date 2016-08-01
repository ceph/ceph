// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 &smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <future>
#include <mutex>
#include <thread>

#include <boost/thread/shared_mutex.hpp>

#include "common/ceph_time.h"
#include "common/shunique_lock.h"

#include "gtest/gtest.h"

template<typename SharedMutex>
static bool test_try_lock(SharedMutex* sm) {
  if (!sm->try_lock())
    return false;
  sm->unlock();
  return true;
}

template<typename SharedMutex>
static bool test_try_lock_shared(SharedMutex* sm) {
  if (!sm->try_lock_shared())
    return false;
  sm->unlock_shared();
  return true;
}

template<typename SharedMutex, typename AcquireType>
static void check_conflicts(SharedMutex sm, AcquireType) {
}

template<typename SharedMutex>
static void ensure_conflicts(SharedMutex& sm, ceph::acquire_unique_t) {
  auto ttl = &test_try_lock<boost::shared_mutex>;
  auto ttls = &test_try_lock_shared<boost::shared_mutex>;
  ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
  ASSERT_FALSE(std::async(std::launch::async, ttls, &sm).get());
}

template<typename SharedMutex>
static void ensure_conflicts(SharedMutex& sm, ceph::acquire_shared_t) {
  auto ttl = &test_try_lock<boost::shared_mutex>;
  auto ttls = &test_try_lock_shared<boost::shared_mutex>;
  ASSERT_FALSE(std::async(std::launch::async, ttl, &sm).get());
  ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
}

template<typename SharedMutex>
static void ensure_free(SharedMutex& sm) {
  auto ttl = &test_try_lock<boost::shared_mutex>;
  auto ttls = &test_try_lock_shared<boost::shared_mutex>;
  ASSERT_TRUE(std::async(std::launch::async, ttl, &sm).get());
  ASSERT_TRUE(std::async(std::launch::async, ttls, &sm).get());
}

template<typename SharedMutex, typename AcquireType>
static void check_owns_lock(const SharedMutex& sm,
			    const ceph::shunique_lock<SharedMutex>& sul,
			    AcquireType) {
}

template<typename SharedMutex>
static void check_owns_lock(const SharedMutex& sm,
			    const ceph::shunique_lock<SharedMutex>& sul,
			    ceph::acquire_unique_t) {
  ASSERT_TRUE(sul.mutex() == &sm);
  ASSERT_TRUE(sul.owns_lock());
  ASSERT_TRUE(!!sul);
}

template<typename SharedMutex>
static void check_owns_lock(const SharedMutex& sm,
			    const ceph::shunique_lock<SharedMutex>& sul,
			    ceph::acquire_shared_t) {
  ASSERT_TRUE(sul.owns_lock_shared());
  ASSERT_TRUE(!!sul);
}

template<typename SharedMutex>
static void check_abjures_lock(const SharedMutex& sm,
			       const ceph::shunique_lock<SharedMutex>& sul) {
  ASSERT_EQ(sul.mutex(), &sm);
  ASSERT_FALSE(sul.owns_lock());
  ASSERT_FALSE(sul.owns_lock_shared());
  ASSERT_FALSE(!!sul);
}

template<typename SharedMutex>
static void check_abjures_lock(const ceph::shunique_lock<SharedMutex>& sul) {
  ASSERT_EQ(sul.mutex(), nullptr);
  ASSERT_FALSE(sul.owns_lock());
  ASSERT_FALSE(sul.owns_lock_shared());
  ASSERT_FALSE(!!sul);
}

TEST(ShuniqueLock, DefaultConstructor) {
  typedef ceph::shunique_lock<boost::shared_mutex> shunique_lock;

  shunique_lock l;

  ASSERT_EQ(l.mutex(), nullptr);
  ASSERT_FALSE(l.owns_lock());
  ASSERT_FALSE(!!l);

  ASSERT_THROW(l.lock(), std::system_error);
  ASSERT_THROW(l.try_lock(), std::system_error);

  ASSERT_THROW(l.lock_shared(), std::system_error);
  ASSERT_THROW(l.try_lock_shared(), std::system_error);

  ASSERT_THROW(l.unlock(), std::system_error);

  ASSERT_EQ(l.mutex(), nullptr);
  ASSERT_FALSE(l.owns_lock());
  ASSERT_FALSE(!!l);

  ASSERT_EQ(l.release(), nullptr);

  ASSERT_EQ(l.mutex(), nullptr);
  ASSERT_FALSE(l.owns_lock());
  ASSERT_FALSE(l.owns_lock_shared());
  ASSERT_FALSE(!!l);
}

template<typename AcquireType>
void lock_unlock(AcquireType at) {
  boost::shared_mutex sm;
  typedef ceph::shunique_lock<boost::shared_mutex> shunique_lock;

  shunique_lock l(sm, at);

  check_owns_lock(sm, l, at);
  ensure_conflicts(sm, at);

  l.unlock();

  check_abjures_lock(sm, l);
  ensure_free(sm);

  l.lock(at);

  check_owns_lock(sm, l, at);
  ensure_conflicts(sm, at);
}

TEST(ShuniqueLock, LockUnlock) {
  lock_unlock(ceph::acquire_unique);
  lock_unlock(ceph::acquire_shared);
}

template<typename AcquireType>
void lock_destruct(AcquireType at) {
  boost::shared_mutex sm;
  typedef ceph::shunique_lock<boost::shared_mutex> shunique_lock;

  {
    shunique_lock l(sm, at);

    check_owns_lock(sm, l, at);
    ensure_conflicts(sm, at);
  }

  ensure_free(sm);
}

TEST(ShuniqueLock, LockDestruct) {
  lock_destruct(ceph::acquire_unique);
  lock_destruct(ceph::acquire_shared);
}

template<typename AcquireType>
void move_construct(AcquireType at) {
  boost::shared_mutex sm;

  typedef ceph::shunique_lock<boost::shared_mutex> shunique_lock;

  {
    shunique_lock l(sm, at);

    check_owns_lock(sm, l, at);
    ensure_conflicts(sm, at);

    shunique_lock o(std::move(l));

    check_abjures_lock(l);

    check_owns_lock(sm, o, at);
    ensure_conflicts(sm, at);

    o.unlock();

    shunique_lock c(std::move(o));


    ASSERT_EQ(o.mutex(), nullptr);
    ASSERT_FALSE(!!o);

    check_abjures_lock(sm, c);

    ensure_free(sm);
  }
}

TEST(ShuniqueLock, MoveConstruct) {
  move_construct(ceph::acquire_unique);
  move_construct(ceph::acquire_shared);

  boost::shared_mutex sm;
  {
    std::unique_lock<boost::shared_mutex> ul(sm);
    ensure_conflicts(sm, ceph::acquire_unique);
    ceph::shunique_lock<boost::shared_mutex> l(std::move(ul));
    check_owns_lock(sm, l, ceph::acquire_unique);
    ensure_conflicts(sm, ceph::acquire_unique);
  }
  {
    std::unique_lock<boost::shared_mutex> ul(sm, std::defer_lock);
    ensure_free(sm);
    ceph::shunique_lock<boost::shared_mutex> l(std::move(ul));
    check_abjures_lock(sm, l);
    ensure_free(sm);
  }
  {
    std::unique_lock<boost::shared_mutex> ul;
    ceph::shunique_lock<boost::shared_mutex> l(std::move(ul));
    check_abjures_lock(l);
  }
  {
    boost::shared_lock<boost::shared_mutex> sl(sm);
    ensure_conflicts(sm, ceph::acquire_shared);
    ceph::shunique_lock<boost::shared_mutex> l(std::move(sl));
    check_owns_lock(sm, l, ceph::acquire_shared);
    ensure_conflicts(sm, ceph::acquire_shared);
  }
  {
    boost::shared_lock<boost::shared_mutex> sl;
    ceph::shunique_lock<boost::shared_mutex> l(std::move(sl));
    check_abjures_lock(l);
  }
}

template<typename AcquireType>
void move_assign(AcquireType at) {
  boost::shared_mutex sm;

  typedef ceph::shunique_lock<boost::shared_mutex> shunique_lock;

  {
    shunique_lock l(sm, at);

    check_owns_lock(sm, l, at);
    ensure_conflicts(sm, at);

    shunique_lock o;

    o = std::move(l);

    check_abjures_lock(l);

    check_owns_lock(sm, o, at);
    ensure_conflicts(sm, at);

    o.unlock();

    shunique_lock c(std::move(o));

    check_abjures_lock(o);
    check_abjures_lock(sm, c);

    ensure_free(sm);

    shunique_lock k;

    c = std::move(k);

    check_abjures_lock(k);
    check_abjures_lock(c);

    ensure_free(sm);
  }
}

TEST(ShuniqueLock, MoveAssign) {
  move_assign(ceph::acquire_unique);
  move_assign(ceph::acquire_shared);

  boost::shared_mutex sm;
  {
    std::unique_lock<boost::shared_mutex> ul(sm);
    ensure_conflicts(sm, ceph::acquire_unique);
    ceph::shunique_lock<boost::shared_mutex> l;
    l = std::move(ul);
    check_owns_lock(sm, l, ceph::acquire_unique);
    ensure_conflicts(sm, ceph::acquire_unique);
  }
  {
    std::unique_lock<boost::shared_mutex> ul(sm, std::defer_lock);
    ensure_free(sm);
    ceph::shunique_lock<boost::shared_mutex> l;
    l = std::move(ul);
    check_abjures_lock(sm, l);
    ensure_free(sm);
  }
  {
    std::unique_lock<boost::shared_mutex> ul;
    ceph::shunique_lock<boost::shared_mutex> l;
    l = std::move(ul);
    check_abjures_lock(l);
  }
  {
    boost::shared_lock<boost::shared_mutex> sl(sm);
    ensure_conflicts(sm, ceph::acquire_shared);
    ceph::shunique_lock<boost::shared_mutex> l;
    l = std::move(sl);
    check_owns_lock(sm, l, ceph::acquire_shared);
    ensure_conflicts(sm, ceph::acquire_shared);
  }
  {
    boost::shared_lock<boost::shared_mutex> sl;
    ceph::shunique_lock<boost::shared_mutex> l;
    l = std::move(sl);
    check_abjures_lock(l);
  }

}

template<typename AcquireType>
void construct_deferred(AcquireType at) {
  boost::shared_mutex sm;

  typedef ceph::shunique_lock<boost::shared_mutex> shunique_lock;

  {
    shunique_lock l(sm, std::defer_lock);
    check_abjures_lock(sm, l);
    ensure_free(sm);

    ASSERT_THROW(l.unlock(), std::system_error);

    check_abjures_lock(sm, l);
    ensure_free(sm);

    l.lock(at);
    check_owns_lock(sm, l, at);
    ensure_conflicts(sm, at);
  }

  {
    shunique_lock l(sm, std::defer_lock);
    check_abjures_lock(sm, l);
    ensure_free(sm);

    ASSERT_THROW(l.unlock(), std::system_error);

    check_abjures_lock(sm, l);
    ensure_free(sm);
  }
  ensure_free(sm);
}

TEST(ShuniqueLock, ConstructDeferred) {
  construct_deferred(ceph::acquire_unique);
  construct_deferred(ceph::acquire_shared);
}

template<typename AcquireType>
void construct_try(AcquireType at) {
  boost::shared_mutex sm;
  typedef ceph::shunique_lock<boost::shared_mutex> shunique_lock;

  {
    shunique_lock l(sm, at, std::try_to_lock);
    check_owns_lock(sm, l, at);
    ensure_conflicts(sm, at);
  }

  {
    std::unique_lock<boost::shared_mutex> l(sm);
    ensure_conflicts(sm, ceph::acquire_unique);

    std::async(std::launch::async, [&sm, at]() {
	shunique_lock l(sm, at, std::try_to_lock);
	check_abjures_lock(sm, l);
	ensure_conflicts(sm, ceph::acquire_unique);
      }).get();

    l.unlock();

    std::async(std::launch::async, [&sm, at]() {
	shunique_lock l(sm, at, std::try_to_lock);
	check_owns_lock(sm, l, at);
	ensure_conflicts(sm, at);
      }).get();
  }
}

TEST(ShuniqueLock, ConstructTry) {
  construct_try(ceph::acquire_unique);
  construct_try(ceph::acquire_shared);
}

template<typename AcquireType>
void construct_adopt(AcquireType at) {
  boost::shared_mutex sm;

  typedef ceph::shunique_lock<boost::shared_mutex> shunique_lock;

  {
    shunique_lock d(sm, at);
    d.release();
  }

  ensure_conflicts(sm, at);

  {
    shunique_lock l(sm, at, std::adopt_lock);
    check_owns_lock(sm, l, at);
    ensure_conflicts(sm, at);
  }

  ensure_free(sm);
}

TEST(ShuniqueLock, ConstructAdopt) {
  construct_adopt(ceph::acquire_unique);
  construct_adopt(ceph::acquire_shared);
}

template<typename AcquireType>
void try_lock(AcquireType at) {
  boost::shared_mutex sm;

  typedef ceph::shunique_lock<boost::shared_mutex> shunique_lock;

  {
    shunique_lock l(sm, std::defer_lock);
    l.try_lock(at);

    check_owns_lock(sm, l, at);
    ensure_conflicts(sm, at);
  }

  {
    std::unique_lock<boost::shared_mutex> l(sm);

    std::async(std::launch::async, [&sm, at]() {
	shunique_lock l(sm, std::defer_lock);
	l.try_lock(at);

	check_abjures_lock(sm, l);
	ensure_conflicts(sm, ceph::acquire_unique);
      }).get();


    l.unlock();
    std::async(std::launch::async, [&sm, at]() {
	shunique_lock l(sm, std::defer_lock);
	l.try_lock(at);

	check_owns_lock(sm, l, at);
	ensure_conflicts(sm, at);
      }).get();
  }
}

TEST(ShuniqueLock, TryLock) {
  try_lock(ceph::acquire_unique);
  try_lock(ceph::acquire_shared);
}

TEST(ShuniqueLock, Release) {
  boost::shared_mutex sm;
  typedef ceph::shunique_lock<boost::shared_mutex> shunique_lock;

  {
    shunique_lock l(sm, ceph::acquire_unique);
    check_owns_lock(sm, l, ceph::acquire_unique);
    ensure_conflicts(sm, ceph::acquire_unique);

    l.release();
    check_abjures_lock(l);
    ensure_conflicts(sm, ceph::acquire_unique);
  }
  ensure_conflicts(sm, ceph::acquire_unique);
  sm.unlock();
  ensure_free(sm);

  {
    shunique_lock l(sm, ceph::acquire_shared);
    check_owns_lock(sm, l, ceph::acquire_shared);
    ensure_conflicts(sm, ceph::acquire_shared);

    l.release();
    check_abjures_lock(l);
    ensure_conflicts(sm, ceph::acquire_shared);
  }
  ensure_conflicts(sm, ceph::acquire_shared);
  sm.unlock_shared();
  ensure_free(sm);

  sm.lock();
  {
    shunique_lock l(sm, std::defer_lock);
    check_abjures_lock(sm, l);
    ensure_conflicts(sm, ceph::acquire_unique);

    l.release();
    check_abjures_lock(l);
    ensure_conflicts(sm, ceph::acquire_unique);
  }
  ensure_conflicts(sm, ceph::acquire_unique);
  sm.unlock();

  ensure_free(sm);

  {
    std::unique_lock<boost::shared_mutex> ul;
    shunique_lock l(sm, std::defer_lock);
    check_abjures_lock(sm, l);
    ensure_free(sm);

    ASSERT_NO_THROW(ul = l.release_to_unique());
    check_abjures_lock(l);
    ASSERT_EQ(ul.mutex(), &sm);
    ASSERT_FALSE(ul.owns_lock());
    ensure_free(sm);
  }
  ensure_free(sm);

  {
    std::unique_lock<boost::shared_mutex> ul;
    shunique_lock l;
    check_abjures_lock(l);

    ASSERT_NO_THROW(ul = l.release_to_unique());
    check_abjures_lock(l);
    ASSERT_EQ(ul.mutex(), nullptr);
    ASSERT_FALSE(ul.owns_lock());
  }
}

TEST(ShuniqueLock, NoRecursion) {
  boost::shared_mutex sm;

  typedef ceph::shunique_lock<boost::shared_mutex> shunique_lock;

  {
    shunique_lock l(sm, ceph::acquire_unique);
    ASSERT_THROW(l.lock(), std::system_error);
    ASSERT_THROW(l.try_lock(), std::system_error);
    ASSERT_THROW(l.lock_shared(), std::system_error);
    ASSERT_THROW(l.try_lock_shared(), std::system_error);
  }

  {
    shunique_lock l(sm, ceph::acquire_shared);
    ASSERT_THROW(l.lock(), std::system_error);
    ASSERT_THROW(l.try_lock(), std::system_error);
    ASSERT_THROW(l.lock_shared(), std::system_error);
    ASSERT_THROW(l.try_lock_shared(), std::system_error);
  }
}
