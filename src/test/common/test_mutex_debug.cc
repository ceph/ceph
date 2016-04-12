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

#include "common/mutex_debug.h"

#include "gtest/gtest.h"


template<typename Mutex>
static bool test_try_lock(Mutex* m) {
  if (!m->try_lock())
    return false;
  m->unlock();
  return true;
}

template<typename Mutex>
static void test_lock() {
  Mutex m;
  auto ttl = &test_try_lock<Mutex>;

  m.lock();
  ASSERT_TRUE(m.is_locked());
  auto f1 = std::async(std::launch::async, ttl, &m);
  ASSERT_FALSE(f1.get());

  ASSERT_TRUE(m.is_locked());
  ASSERT_TRUE(!!m);

  m.unlock();
  ASSERT_FALSE(m.is_locked());
  ASSERT_FALSE(!!m);

  auto f3 = std::async(std::launch::async, ttl, &m);
  ASSERT_TRUE(f3.get());

  ASSERT_FALSE(m.is_locked());
  ASSERT_FALSE(!!m);
}

TEST(MutexDebug, Lock) {
  test_lock<ceph::mutex_debug>();
}

TEST(MutexDebug, NotRecursive) {
  ceph::mutex_debug m;
  auto ttl = &test_try_lock<mutex_debug>;

  ASSERT_NO_THROW(m.lock());
  ASSERT_TRUE(m.is_locked());
  ASSERT_FALSE(std::async(std::launch::async, ttl, &m).get());

  ASSERT_THROW(m.lock(), std::system_error);
  ASSERT_TRUE(m.is_locked());
  ASSERT_FALSE(std::async(std::launch::async, ttl, &m).get());

  ASSERT_NO_THROW(m.unlock());
  ASSERT_FALSE(m.is_locked());
  ASSERT_TRUE(std::async(std::launch::async, ttl, &m).get());
}

TEST(MutexRecursiveDebug, Lock) {
  test_lock<ceph::mutex_recursive_debug>();
}


TEST(MutexRecursiveDebug, Recursive) {
  ceph::mutex_recursive_debug m;
  auto ttl = &test_try_lock<mutex_recursive_debug>;

  ASSERT_NO_THROW(m.lock());
  ASSERT_TRUE(m.is_locked());
  ASSERT_FALSE(std::async(std::launch::async, ttl, &m).get());

  ASSERT_NO_THROW(m.lock());
  ASSERT_TRUE(m.is_locked());
  ASSERT_FALSE(std::async(std::launch::async, ttl, &m).get());

  ASSERT_NO_THROW(m.unlock());
  ASSERT_TRUE(m.is_locked());
  ASSERT_FALSE(std::async(std::launch::async, ttl, &m).get());

  ASSERT_NO_THROW(m.unlock());
  ASSERT_FALSE(m.is_locked());
  ASSERT_TRUE(std::async(std::launch::async, ttl, &m).get());
}
