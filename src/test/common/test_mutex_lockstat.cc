// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM Corp
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

#include "common/ceph_mutex_lockstat.h"
#include "gtest/gtest.h"

template <typename Mutex>
static bool
test_try_lockstat(Mutex* m)
{
  if (!m->try_lock())
    return false;
  m->unlock();
  return true;
}

template <typename Mutex>
static void
test_lockstat()
{
  Mutex m(LOCKSTAT("mutex"));
  auto ttl = &test_try_lockstat<Mutex>;

  m.lock();
  auto f1 = std::async(std::launch::async, ttl, &m);
  ASSERT_FALSE(f1.get());

  m.unlock();

  auto f3 = std::async(std::launch::async, ttl, &m);
  ASSERT_TRUE(f3.get());
}

TEST(MutexLockStat, Lock) { test_lockstat<ceph::mutex_lockstat>(); }

/*
TEST(MutexLockStatDeathTest, NotRecursive)
{
  ceph::mutex_lockstat m(LOCKSTAT("foo"));
  // avoid an assert during test cleanup where the mutex is locked and cannot be
  // pthread_mutex_destroy'd
  std::unique_lock locker{m};
  ASSERT_DEATH(m.lock(), "FAILED ceph_assert(recursive || !is_locked_by_me())");
}
*/
TEST(MutexRecursiveLockStat, Lock)
{
  test_lockstat<ceph::mutex_recursive_lockstat>();
}

TEST(MutexRecursiveLockStat, Recursive)
{
  ceph::mutex_recursive_lockstat m(LOCKSTAT("m"));
  auto ttl = &test_try_lockstat<mutex_recursive_lockstat>;

  ASSERT_NO_THROW(m.lock());
  ASSERT_FALSE(std::async(std::launch::async, ttl, &m).get());

  ASSERT_NO_THROW(m.lock());
  ASSERT_FALSE(std::async(std::launch::async, ttl, &m).get());

  ASSERT_NO_THROW(m.unlock());
  ASSERT_FALSE(std::async(std::launch::async, ttl, &m).get());

  ASSERT_NO_THROW(m.unlock());
  ASSERT_TRUE(std::async(std::launch::async, ttl, &m).get());
}
