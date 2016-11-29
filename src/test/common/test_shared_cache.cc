// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *         Cheng Cheng <ccheng.leo@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library Public License for more details.
 *
 */

#include <stdio.h>
#include <signal.h>
#include "common/Thread.h"
#include "common/shared_cache.hpp"
#include "common/ceph_argparse.h"
#include "test/unit.h"

class SharedLRUTest : public SharedLRU<unsigned int, int> {
public:
  Mutex &get_lock() { return lock; }
  Cond &get_cond() { return cond; }
  map<unsigned int, pair< ceph::weak_ptr<int>, int* > > &get_weak_refs() {
    return weak_refs;
  }
};

class SharedLRU_all : public ::testing::Test {
public:

  class Thread_wait : public Thread {
  public:
    SharedLRUTest &cache;
    unsigned int key;
    int value;
    ceph::shared_ptr<int> ptr;
    enum in_method_t { LOOKUP, LOWER_BOUND } in_method;

    Thread_wait(SharedLRUTest& _cache, unsigned int _key, 
                int _value, in_method_t _in_method) :
      cache(_cache),
      key(_key),
      value(_value),
      in_method(_in_method) { }

    virtual void * entry() {
      switch (in_method) {
      case LOWER_BOUND:
        ptr = cache.lower_bound(key);
        break;
      case LOOKUP:
        ptr = ceph::shared_ptr<int>(new int);
        *ptr = value;
        ptr = cache.lookup(key);
        break;
      }
      return NULL;
    }
  };

  static const useconds_t DELAY_MAX = 20 * 1000 * 1000;
  static useconds_t delay;

  bool wait_for(SharedLRUTest &cache, int waitting) {
    do {
      //
      // the delay variable is supposed to be initialized to zero. It would be fine
      // to usleep(0) but we take this opportunity to test the loop. It will try 
      // again and therefore show that the logic ( increasing the delay ) actually
      // works. 
      //
      if (delay > 0)
        usleep(delay);
      {
        Mutex::Locker l(cache.get_lock());
        if (cache.waiting == waitting) {
          break;
        }
      }
      if (delay > 0) {
        cout << "delay " << delay << "us, is not long enough, try again\n";
      }
    } while ((delay = delay * 2 + 1) < DELAY_MAX); 
    return delay < DELAY_MAX;
  }
};

useconds_t SharedLRU_all::delay = 0;

TEST_F(SharedLRU_all, add) {
  SharedLRUTest cache;
  unsigned int key = 1;
  int value1 = 2;
  bool existed = false;
  {
    ceph::shared_ptr<int> ptr = cache.add(key, new int(value1), &existed);
    ASSERT_EQ(value1, *ptr);
    ASSERT_FALSE(existed);
  }
  {
    int value2 = 3;
    ceph::shared_ptr<int> ptr = cache.add(key, new int(value2), &existed);
    ASSERT_EQ(value1, *ptr);
    ASSERT_TRUE(existed);
  }
}
TEST_F(SharedLRU_all, empty) {
  SharedLRUTest cache;
  unsigned int key = 1;
  bool existed = false;

  ASSERT_TRUE(cache.empty());
  {
    int value1 = 2;
    ceph::shared_ptr<int> ptr = cache.add(key, new int(value1), &existed);
    ASSERT_EQ(value1, *ptr);
    ASSERT_FALSE(existed);
  }
  ASSERT_FALSE(cache.empty());

  cache.clear(key);
  ASSERT_TRUE(cache.empty());
}

TEST_F(SharedLRU_all, lookup) {
  SharedLRUTest cache;
  unsigned int key = 1;
  {
    int value = 2;
    ASSERT_TRUE(cache.add(key, new int(value)).get());
    ASSERT_TRUE(cache.lookup(key).get());
    ASSERT_EQ(value, *cache.lookup(key));
  }
  ASSERT_TRUE(cache.lookup(key).get());
}
TEST_F(SharedLRU_all, lookup_or_create) {
  SharedLRUTest cache;
  {
    int value = 2;
    unsigned int key = 1;
    ASSERT_TRUE(cache.add(key, new int(value)).get());
    ASSERT_TRUE(cache.lookup_or_create(key).get());
    ASSERT_EQ(value, *cache.lookup(key));
  }
  {
    unsigned int key = 2;
    ASSERT_TRUE(cache.lookup_or_create(key).get());
    ASSERT_EQ(0, *cache.lookup(key));
  }
  ASSERT_TRUE(cache.lookup(1).get());
  ASSERT_TRUE(cache.lookup(2).get());
}

TEST_F(SharedLRU_all, wait_lookup) {
  SharedLRUTest cache;
  unsigned int key = 1;
  int value = 2;

  {
    ceph::shared_ptr<int> ptr(new int);
    cache.get_weak_refs()[key] = make_pair(ptr, &*ptr);
  }
  EXPECT_FALSE(cache.get_weak_refs()[key].first.lock());

  Thread_wait t(cache, key, value, Thread_wait::LOOKUP);
  t.create("wait_lookup_1");
  ASSERT_TRUE(wait_for(cache, 1));
  EXPECT_EQ(value, *t.ptr);
  // waiting on a key does not block lookups on other keys
  EXPECT_FALSE(cache.lookup(key + 12345));
  {
    Mutex::Locker l(cache.get_lock());
    cache.get_weak_refs().erase(key);
    cache.get_cond().Signal();
  }
  ASSERT_TRUE(wait_for(cache, 0));
  t.join();
  EXPECT_FALSE(t.ptr);
}
TEST_F(SharedLRU_all, wait_lookup_or_create) {
  SharedLRUTest cache;
  unsigned int key = 1;
  int value = 2;

  {
    ceph::shared_ptr<int> ptr(new int);
    cache.get_weak_refs()[key] = make_pair(ptr, &*ptr);
  }
  EXPECT_FALSE(cache.get_weak_refs()[key].first.lock());

  Thread_wait t(cache, key, value, Thread_wait::LOOKUP);
  t.create("wait_lookup_2");
  ASSERT_TRUE(wait_for(cache, 1));
  EXPECT_EQ(value, *t.ptr);
  // waiting on a key does not block lookups on other keys
  EXPECT_TRUE(cache.lookup_or_create(key + 12345).get());
  {
    Mutex::Locker l(cache.get_lock());
    cache.get_weak_refs().erase(key);
    cache.get_cond().Signal();
  }
  ASSERT_TRUE(wait_for(cache, 0));
  t.join();
  EXPECT_FALSE(t.ptr);
}

TEST_F(SharedLRU_all, lower_bound) {
  SharedLRUTest cache;

  {
    unsigned int key = 1;
    ASSERT_FALSE(cache.lower_bound(key));
    int value = 2;

    ASSERT_TRUE(cache.add(key, new int(value)).get());
    ASSERT_TRUE(cache.lower_bound(key).get());
    EXPECT_EQ(value, *cache.lower_bound(key));
  }
}

TEST_F(SharedLRU_all, wait_lower_bound) {
  SharedLRUTest cache;
  unsigned int key = 1;
  int value = 2;
  unsigned int other_key = key + 1;
  int other_value = value + 1;

  ASSERT_TRUE(cache.add(other_key, new int(other_value)).get());

  {
    ceph::shared_ptr<int> ptr(new int);
    cache.get_weak_refs()[key] = make_pair(ptr, &*ptr);
  }
  EXPECT_FALSE(cache.get_weak_refs()[key].first.lock());

  Thread_wait t(cache, key, value, Thread_wait::LOWER_BOUND);
  t.create("wait_lower_bnd");
  ASSERT_TRUE(wait_for(cache, 1));
  EXPECT_FALSE(t.ptr);
  // waiting on a key does not block getting lower_bound on other keys
  EXPECT_TRUE(cache.lower_bound(other_key).get());
  {
    Mutex::Locker l(cache.get_lock());
    cache.get_weak_refs().erase(key);
    cache.get_cond().Signal();
  }
  ASSERT_TRUE(wait_for(cache, 0));
  t.join();
  EXPECT_TRUE(t.ptr.get());
}
TEST_F(SharedLRU_all, get_next) {

  {
    SharedLRUTest cache;
    const unsigned int key = 0;
    pair<unsigned int, int> i;
    EXPECT_FALSE(cache.get_next(key, &i));
  }
  {
    SharedLRUTest cache;

    const unsigned int key2 = 333;
    ceph::shared_ptr<int> ptr2 = cache.lookup_or_create(key2);
    const int value2 = *ptr2 = 400;

    // entries with expired pointers are silently ignored
    const unsigned int key_gone = 222;
    cache.get_weak_refs()[key_gone] = make_pair(ceph::shared_ptr<int>(), (int*)0);

    const unsigned int key1 = 111;
    ceph::shared_ptr<int> ptr1 = cache.lookup_or_create(key1);
    const int value1 = *ptr1 = 800;

    pair<unsigned int, int> i;
    EXPECT_TRUE(cache.get_next(0, &i));
    EXPECT_EQ(key1, i.first);
    EXPECT_EQ(value1, i.second);

    EXPECT_TRUE(cache.get_next(i.first, &i));
    EXPECT_EQ(key2, i.first);
    EXPECT_EQ(value2, i.second);

    EXPECT_FALSE(cache.get_next(i.first, &i));

    cache.get_weak_refs().clear();
  }
  {
    SharedLRUTest cache;
    const unsigned int key1 = 111;
    ceph::shared_ptr<int> *ptr1 = new shared_ptr<int>(cache.lookup_or_create(key1));
    const unsigned int key2 = 222;
    ceph::shared_ptr<int> ptr2 = cache.lookup_or_create(key2);

    pair<unsigned int, ceph::shared_ptr<int> > i;
    EXPECT_TRUE(cache.get_next(i.first, &i));
    EXPECT_EQ(key1, i.first);
    delete ptr1;
    EXPECT_TRUE(cache.get_next(i.first, &i));
    EXPECT_EQ(key2, i.first);
  }
}

TEST_F(SharedLRU_all, clear) {
  SharedLRUTest cache;
  unsigned int key = 1;
  int value = 2;
  {
    ceph::shared_ptr<int> ptr = cache.add(key, new int(value));
    ASSERT_EQ(value, *cache.lookup(key));
  }
  ASSERT_TRUE(cache.lookup(key).get());
  cache.clear(key);
  ASSERT_FALSE(cache.lookup(key));

  {
    ceph::shared_ptr<int> ptr = cache.add(key, new int(value));
  }
  ASSERT_TRUE(cache.lookup(key).get());
  cache.clear(key);
  ASSERT_FALSE(cache.lookup(key));
}
TEST_F(SharedLRU_all, clear_all) {
  SharedLRUTest cache;
  unsigned int key = 1;
  int value = 2;
  {
    ceph::shared_ptr<int> ptr = cache.add(key, new int(value));
    ASSERT_EQ(value, *cache.lookup(key));
  }
  ASSERT_TRUE(cache.lookup(key).get());
  cache.clear();
  ASSERT_FALSE(cache.lookup(key));

  ceph::shared_ptr<int> ptr2 = cache.add(key, new int(value));
  ASSERT_TRUE(cache.lookup(key).get());
  cache.clear();
  ASSERT_TRUE(cache.lookup(key).get());
  ASSERT_FALSE(cache.empty());
}

TEST(SharedCache_all, add) {
  SharedLRU<int, int> cache;
  unsigned int key = 1;
  int value = 2;
  ceph::shared_ptr<int> ptr = cache.add(key, new int(value));
  ASSERT_EQ(ptr, cache.lookup(key));
  ASSERT_EQ(value, *cache.lookup(key));
}

TEST(SharedCache_all, lru) {
  const size_t SIZE = 5;
  SharedLRU<int, int> cache(NULL, SIZE);

  bool existed = false;
  ceph::shared_ptr<int> ptr = cache.add(0, new int(0), &existed);
  ASSERT_FALSE(existed);
  {
    int *tmpint = new int(0);
    ceph::shared_ptr<int> ptr2 = cache.add(0, tmpint, &existed);
    ASSERT_TRUE(existed);
    delete tmpint;
  }
  for (size_t i = 1; i < 2*SIZE; ++i) {
    cache.add(i, new int(i), &existed);
    ASSERT_FALSE(existed);
  }

  ASSERT_TRUE(cache.lookup(0).get());
  ASSERT_EQ(0, *cache.lookup(0));

  ASSERT_FALSE(cache.lookup(SIZE-1));
  ASSERT_FALSE(cache.lookup(SIZE));
  ASSERT_TRUE(cache.lookup(SIZE+1).get());
  ASSERT_EQ((int)SIZE+1, *cache.lookup(SIZE+1));

  cache.purge(0);
  ASSERT_FALSE(cache.lookup(0));
  ceph::shared_ptr<int> ptr2 = cache.add(0, new int(0), &existed);
  ASSERT_FALSE(ptr == ptr2);
  ptr = ceph::shared_ptr<int>();
  ASSERT_TRUE(cache.lookup(0).get());
}

// Local Variables:
// compile-command: "cd ../.. ; make unittest_shared_cache && ./unittest_shared_cache # --gtest_filter=*.* --log-to-stderr=true"
// End:
