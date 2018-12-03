// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
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
#include "gtest/gtest.h"
#include "common/shared_cache.hpp"

class LRUTest : public SharedLRU<unsigned int, int> {
};

class LRU_all : public ::testing::Test {
};

TEST_F(LRU_all, add) {
  LRUTest cache;
  unsigned int key = 1;
  int value1 = 2;
  bool existed = false;
  {
    boost::local_shared_ptr<int> ptr = cache.add(key, new int(value1), &existed);
    ASSERT_EQ(value1, *ptr);
    ASSERT_FALSE(existed);
  }
  {
    auto p = new int(3);
    boost::local_shared_ptr<int> ptr = cache.add(key, p, &existed);
    ASSERT_EQ(value1, *ptr);
    ASSERT_TRUE(existed);
    delete p;
  }
}

TEST_F(LRU_all, empty) {
  LRUTest cache;
  unsigned int key = 1;
  bool existed = false;

  ASSERT_TRUE(cache.empty());
  {
    int value1 = 2;
    boost::local_shared_ptr<int> ptr = cache.add(key, new int(value1), &existed);
    ASSERT_EQ(value1, *ptr);
    ASSERT_FALSE(existed);
  }
  ASSERT_FALSE(cache.empty());

  cache.clear(key);
  ASSERT_TRUE(cache.empty());
}

TEST_F(LRU_all, lookup) {
  LRUTest cache;
  unsigned int key = 1;
  {
    int value = 2;
    ASSERT_TRUE(cache.add(key, new int(value)).get());
    ASSERT_TRUE(cache.lookup(key).get());
    ASSERT_EQ(value, *cache.lookup(key));
  }
  ASSERT_TRUE(cache.lookup(key).get());
}

TEST_F(LRU_all, lookup_or_create) {
  LRUTest cache;
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

TEST_F(LRU_all, lower_bound) {
  LRUTest cache;

  {
    unsigned int key = 1;
    ASSERT_FALSE(cache.lower_bound(key));
    int value = 2;

    ASSERT_TRUE(cache.add(key, new int(value)).get());
    ASSERT_TRUE(cache.lower_bound(key).get());
    EXPECT_EQ(value, *cache.lower_bound(key));
  }
}

TEST_F(LRU_all, get_next) {

  {
    LRUTest cache;
    const unsigned int key = 0;
    pair<unsigned int, int> i;
    EXPECT_FALSE(cache.get_next(key, &i));
  }
  {
    LRUTest cache;
    const unsigned int key1 = 111;
    boost::local_shared_ptr<int> *ptr1 = new boost::local_shared_ptr<int>(cache.lookup_or_create(key1));
    const unsigned int key2 = 222;
    boost::local_shared_ptr<int> ptr2 = cache.lookup_or_create(key2);

    pair<unsigned int, boost::local_shared_ptr<int> > i;
    EXPECT_TRUE(cache.get_next(i.first, &i));
    EXPECT_EQ(key1, i.first);
    delete ptr1;
    EXPECT_TRUE(cache.get_next(i.first, &i));
    EXPECT_EQ(key2, i.first);
  }
}

TEST_F(LRU_all, clear) {
  LRUTest cache;
  unsigned int key = 1;
  int value = 2;
  {
    boost::local_shared_ptr<int> ptr = cache.add(key, new int(value));
    ASSERT_EQ(value, *cache.lookup(key));
  }
  ASSERT_TRUE(cache.lookup(key).get());
  cache.clear(key);
  ASSERT_FALSE(cache.lookup(key));

  {
    boost::local_shared_ptr<int> ptr = cache.add(key, new int(value));
  }
  ASSERT_TRUE(cache.lookup(key).get());
  cache.clear(key);
  ASSERT_FALSE(cache.lookup(key));
}
TEST_F(LRU_all, clear_all) {
  LRUTest cache;
  unsigned int key = 1;
  int value = 2;
  {
    boost::local_shared_ptr<int> ptr = cache.add(key, new int(value));
    ASSERT_EQ(value, *cache.lookup(key));
  }
  ASSERT_TRUE(cache.lookup(key).get());
  cache.clear();
  ASSERT_FALSE(cache.lookup(key));

  boost::local_shared_ptr<int> ptr2 = cache.add(key, new int(value));
  ASSERT_TRUE(cache.lookup(key).get());
  cache.clear();
  ASSERT_TRUE(cache.lookup(key).get());
  ASSERT_FALSE(cache.empty());
}

TEST(Cache_all, add) {
  SharedLRU<int, int> cache;
  unsigned int key = 1;
  int value = 2;
  boost::local_shared_ptr<int> ptr = cache.add(key, new int(value));
  ASSERT_EQ(ptr, cache.lookup(key));
  ASSERT_EQ(value, *cache.lookup(key));
}

TEST(Cache_all, lru) {
  const size_t SIZE = 5;
  SharedLRU<int, int> cache(nullptr, SIZE);

  bool existed = false;
  boost::local_shared_ptr<int> ptr = cache.add(0, new int(0), &existed);
  ASSERT_FALSE(existed);
  {
    int *tmpint = new int(0);
    boost::local_shared_ptr<int> ptr2 = cache.add(0, tmpint, &existed);
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
  boost::local_shared_ptr<int> ptr2 = cache.add(0, new int(0), &existed);
  ASSERT_FALSE(ptr == ptr2);
  ptr = boost::local_shared_ptr<int>();
  ASSERT_TRUE(cache.lookup(0).get());
}

// Local Variables:
// compile-command: "cd ../.. ; make unittest_seastar_sharedlru && ./unittest_seastar_sharedlru # --gtest_filter=*.* --log-to-stderr=true"
// End:
