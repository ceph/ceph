// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
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
#include "common/sharedptr_registry.hpp"
#include "common/ceph_argparse.h"
#include "test/unit.h"

class SharedPtrRegistryTest : public SharedPtrRegistry<unsigned int, int> {
public:
  Mutex &get_lock() { return lock; }
  map<unsigned int, pair<ceph::weak_ptr<int>, int*> > &get_contents() {
    return contents;
  }
};

class SharedPtrRegistry_all : public ::testing::Test {
public:

  class Thread_wait : public Thread {
  public:
    SharedPtrRegistryTest &registry;
    unsigned int key;
    int value;
    ceph::shared_ptr<int> ptr;
    enum in_method_t { LOOKUP, LOOKUP_OR_CREATE } in_method;

    Thread_wait(SharedPtrRegistryTest& _registry, unsigned int _key, int _value, in_method_t _in_method) : 
      registry(_registry),
      key(_key),
      value(_value),
      in_method(_in_method)
    {
    }
    
    virtual void *entry() {
      switch(in_method) {
      case LOOKUP_OR_CREATE:
	if (value) 
	  ptr = registry.lookup_or_create<int>(key, value);
	else
	  ptr = registry.lookup_or_create(key);
	break;
      case LOOKUP:
	ptr = ceph::shared_ptr<int>(new int);
	*ptr = value;
	ptr = registry.lookup(key);
	break;
      }
      return NULL;
    }
  };

  static const useconds_t DELAY_MAX = 20 * 1000 * 1000;
  static useconds_t delay;

  bool wait_for(SharedPtrRegistryTest &registry, int waiting) {
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
	Mutex::Locker l(registry.get_lock());
	if (registry.waiting == waiting) 
	  break;
      }
      if (delay > 0)
	cout << "delay " << delay << "us, is not long enough, try again\n";
    } while (( delay = delay * 2 + 1) < DELAY_MAX);
    return delay < DELAY_MAX;
  }
};

useconds_t SharedPtrRegistry_all::delay = 0;

TEST_F(SharedPtrRegistry_all, lookup_or_create) {
  SharedPtrRegistryTest registry;
  unsigned int key = 1;
  int value = 2;
  ceph::shared_ptr<int> ptr = registry.lookup_or_create(key);
  *ptr = value;
  ASSERT_EQ(value, *registry.lookup_or_create(key));
}

TEST_F(SharedPtrRegistry_all, wait_lookup_or_create) {
  SharedPtrRegistryTest registry;

  //
  // simulate the following: The last reference to a shared_ptr goes
  // out of scope and the shared_ptr object is about to be removed and
  // marked as such. The weak_ptr stored in the registry will show
  // that it has expired(). However, the SharedPtrRegistry::OnRemoval
  // object has not yet been called and did not get a chance to
  // acquire the lock. The lookup_or_create and lookup methods must
  // detect that situation and wait until the weak_ptr is removed from
  // the registry.
  //
  {
    unsigned int key = 1;
    {
      ceph::shared_ptr<int> ptr(new int);
      registry.get_contents()[key] = make_pair(ptr, ptr.get());
    }
    EXPECT_FALSE(registry.get_contents()[key].first.lock());

    Thread_wait t(registry, key, 0, Thread_wait::LOOKUP_OR_CREATE);
    t.create("wait_lookcreate");
    ASSERT_TRUE(wait_for(registry, 1));
    EXPECT_FALSE(t.ptr);
    // waiting on a key does not block lookups on other keys
    EXPECT_TRUE(registry.lookup_or_create(key + 12345).get());
    registry.remove(key);
    ASSERT_TRUE(wait_for(registry, 0));
    t.join();
    EXPECT_TRUE(t.ptr.get());
  }
  {
    unsigned int key = 2;
    int value = 3;
    {
      ceph::shared_ptr<int> ptr(new int);
      registry.get_contents()[key] = make_pair(ptr, ptr.get());
    }
    EXPECT_FALSE(registry.get_contents()[key].first.lock());

    Thread_wait t(registry, key, value, Thread_wait::LOOKUP_OR_CREATE);
    t.create("wait_lookcreate");
    ASSERT_TRUE(wait_for(registry, 1));
    EXPECT_FALSE(t.ptr);
    // waiting on a key does not block lookups on other keys
    {
      int other_value = value + 1;
      unsigned int other_key = key + 1;
      ceph::shared_ptr<int> ptr = registry.lookup_or_create<int>(other_key, other_value);
      EXPECT_TRUE(ptr.get());
      EXPECT_EQ(other_value, *ptr);
    }
    registry.remove(key);
    ASSERT_TRUE(wait_for(registry, 0));
    t.join();
    EXPECT_TRUE(t.ptr.get());
    EXPECT_EQ(value, *t.ptr);
  }
}

TEST_F(SharedPtrRegistry_all, lookup) {
  SharedPtrRegistryTest registry;
  unsigned int key = 1;
  {
    ceph::shared_ptr<int> ptr = registry.lookup_or_create(key);
    int value = 2;
    *ptr = value;
    ASSERT_EQ(value, *registry.lookup(key));
  }
  ASSERT_FALSE(registry.lookup(key));
}

TEST_F(SharedPtrRegistry_all, wait_lookup) {
  SharedPtrRegistryTest registry;

  unsigned int key = 1;
  int value = 2;
  {
    ceph::shared_ptr<int> ptr(new int);
    registry.get_contents()[key] = make_pair(ptr, ptr.get());
  }
  EXPECT_FALSE(registry.get_contents()[key].first.lock());

  Thread_wait t(registry, key, value, Thread_wait::LOOKUP);
  t.create("wait_lookup");
  ASSERT_TRUE(wait_for(registry, 1));
  EXPECT_EQ(value, *t.ptr);
  // waiting on a key does not block lookups on other keys
  EXPECT_FALSE(registry.lookup(key + 12345));
  registry.remove(key);
  ASSERT_TRUE(wait_for(registry, 0));
  t.join();
  EXPECT_FALSE(t.ptr);
}

TEST_F(SharedPtrRegistry_all, get_next) {

  {
    SharedPtrRegistry<unsigned int,int> registry;
    const unsigned int key = 0;
    pair<unsigned int, int> i;
    EXPECT_FALSE(registry.get_next(key, &i));
  }
  {
    SharedPtrRegistryTest registry;

    const unsigned int key2 = 333;
    ceph::shared_ptr<int> ptr2 = registry.lookup_or_create(key2);
    const int value2 = *ptr2 = 400;

    // entries with expired pointers are silentely ignored
    const unsigned int key_gone = 222;
    registry.get_contents()[key_gone] = make_pair(ceph::shared_ptr<int>(), (int*)0);

    const unsigned int key1 = 111;
    ceph::shared_ptr<int> ptr1 = registry.lookup_or_create(key1);
    const int value1 = *ptr1 = 800;

    pair<unsigned int, int> i;
    EXPECT_TRUE(registry.get_next(i.first, &i));
    EXPECT_EQ(key1, i.first);
    EXPECT_EQ(value1, i.second);

    EXPECT_TRUE(registry.get_next(i.first, &i));
    EXPECT_EQ(key2, i.first);
    EXPECT_EQ(value2, i.second);

    EXPECT_FALSE(registry.get_next(i.first, &i));
  }
  {
    //
    // http://tracker.ceph.com/issues/6117
    // reproduce the issue.
    //
    SharedPtrRegistryTest registry;
    const unsigned int key1 = 111;
    ceph::shared_ptr<int> *ptr1 = new ceph::shared_ptr<int>(registry.lookup_or_create(key1));
    const unsigned int key2 = 222;
    ceph::shared_ptr<int> ptr2 = registry.lookup_or_create(key2);
    
    pair<unsigned int, ceph::shared_ptr<int> > i;
    EXPECT_TRUE(registry.get_next(i.first, &i));
    EXPECT_EQ(key1, i.first);
    delete ptr1;
    EXPECT_TRUE(registry.get_next(i.first, &i));    
    EXPECT_EQ(key2, i.first);
  }
}

TEST_F(SharedPtrRegistry_all, remove) {
  {
    SharedPtrRegistryTest registry;
    const unsigned int key1 = 1;
    ceph::shared_ptr<int> ptr1 = registry.lookup_or_create(key1);
    *ptr1 = 400;
    registry.remove(key1);

    ceph::shared_ptr<int> ptr2 = registry.lookup_or_create(key1);
    *ptr2 = 500;

    ptr1 = ceph::shared_ptr<int>();
    ceph::shared_ptr<int> res = registry.lookup(key1);
    assert(res);
    assert(res == ptr2);
    assert(*res == 500);
  }
  {
    SharedPtrRegistryTest registry;
    const unsigned int key1 = 1;
    ceph::shared_ptr<int> ptr1 = registry.lookup_or_create(key1, 400);
    registry.remove(key1);

    ceph::shared_ptr<int> ptr2 = registry.lookup_or_create(key1, 500);

    ptr1 = ceph::shared_ptr<int>();
    ceph::shared_ptr<int> res = registry.lookup(key1);
    assert(res);
    assert(res == ptr2);
    assert(*res == 500);
  }
}

class SharedPtrRegistry_destructor : public ::testing::Test {
public:

  typedef enum { UNDEFINED, YES, NO } DieEnum;
  static DieEnum died;

  struct TellDie {
    TellDie() { died = NO; }
    ~TellDie() { died = YES; }
    
    int value;
  };

  virtual void SetUp() {
    died = UNDEFINED;
  }
};

SharedPtrRegistry_destructor::DieEnum SharedPtrRegistry_destructor::died = SharedPtrRegistry_destructor::UNDEFINED;

TEST_F(SharedPtrRegistry_destructor, destructor) {
  SharedPtrRegistry<int,TellDie> registry;
  EXPECT_EQ(UNDEFINED, died);
  int key = 101;
  {
    ceph::shared_ptr<TellDie> a = registry.lookup_or_create(key);
    EXPECT_EQ(NO, died);
    EXPECT_TRUE(a.get());
  }
  EXPECT_EQ(YES, died);
  EXPECT_FALSE(registry.lookup(key));
}

// Local Variables:
// compile-command: "cd ../.. ; make unittest_sharedptr_registry && ./unittest_sharedptr_registry # --gtest_filter=*.* --log-to-stderr=true"
// End:
