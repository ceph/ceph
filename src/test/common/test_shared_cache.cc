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
 #include "global/global_init.h"
 #include <gtest/gtest.h>

class SharedLRUTest : public SharedLRU<unsigned int, int> {
public:
  Mutex &get_lock() { return lock; }
};

class SharedLRU_all : public ::testing::Test {
public:

  class Thread_wait : public Thread {
  public:
    SharedLRUTest &cache;
    unsigned int key;
    int value;
    shared_ptr<int> ptr;
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
        ptr = shared_ptr<int>(new int);
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
        if (cache.waiting == waitting)
          break;
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
  int value2 = 3;
  bool existed = false;
  {
    shared_ptr<int> ptr = cache.add(key, new int(value1), &existed);
    ASSERT_EQ(value1, *ptr);
    ASSERT_FALSE(existed);
  }
  {
    shared_ptr<int> ptr = cache.add(key, new int(value2), &existed);
    ASSERT_EQ(value1, *ptr);
    ASSERT_TRUE(existed);
  }
}

TEST_F(SharedLRU_all, lookup) {
  SharedLRUTest cache;
  unsigned int key = 1;
  int value = 2;
  {
    shared_ptr<int> ptr = cache.add(key, new int(value));
    ASSERT_TRUE(cache.lookup(key));
    ASSERT_EQ(value, *cache.lookup(key));
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
  ASSERT_TRUE(cache.lookup(key));
  cache.clear(key);
  ASSERT_FALSE(cache.lookup(key));

  {
    ceph::shared_ptr<int> ptr = cache.add(key, new int(value));
  }
  ASSERT_TRUE(cache.lookup(key));
  cache.clear(key);
  ASSERT_FALSE(cache.lookup(key));
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

// Local Variables:
// compile-command: "cd ../.. ; make unittest_shared_cache && ./unittest_shared_cache # --gtest_filter=*.* --log-to-stderr=true"
// End:
