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
#include "gtest/gtest.h"
#include "crimson/common/shared_lru.h"

class LRUTest : public SharedLRU<unsigned int, int> {
public:
  auto add(unsigned int key, int value, bool* existed = nullptr) {
    auto pv = new int{value};
    auto ptr = insert(key, std::unique_ptr<int>{pv});
    if (existed) {
      *existed = (ptr.get() != pv);
    }
    return ptr;
  }
};

TEST(LRU, add) {
  LRUTest cache;
  unsigned int key = 1;
  int value1 = 2;
  bool existed = false;
  {
    auto ptr = cache.add(key, value1, &existed);
    ASSERT_TRUE(ptr);
    ASSERT_TRUE(ptr.get());
    ASSERT_EQ(value1, *ptr);
    ASSERT_FALSE(existed);
  }
  {
    auto ptr = cache.add(key, 3, &existed);
    ASSERT_EQ(value1, *ptr);
    ASSERT_TRUE(existed);
  }
}

TEST(LRU, empty) {
  LRUTest cache;
  unsigned int key = 1;
  bool existed = false;

  ASSERT_TRUE(cache.empty());
  {
    int value1 = 2;
    auto ptr = cache.add(key, value1, &existed);
    ASSERT_EQ(value1, *ptr);
    ASSERT_FALSE(existed);
  }
  ASSERT_FALSE(cache.empty());

  cache.clear();
  ASSERT_TRUE(cache.empty());
}

TEST(LRU, lookup) {
  LRUTest cache;
  unsigned int key = 1;
  {
    int value = 2;
    auto ptr = cache.add(key, value);
    ASSERT_TRUE(ptr);
    ASSERT_TRUE(ptr.get());
    ASSERT_TRUE(cache.find(key).get());
    ASSERT_EQ(value, *cache.find(key));
  }
  ASSERT_TRUE(cache.find(key).get());
}

TEST(LRU, lookup_or_create) {
  LRUTest cache;
  {
    int value = 2;
    unsigned int key = 1;
    ASSERT_TRUE(cache.add(key, value).get());
    ASSERT_TRUE(cache[key].get());
    ASSERT_EQ(value, *cache.find(key));
  }
  {
    unsigned int key = 2;
    ASSERT_TRUE(cache[key].get());
    ASSERT_EQ(0, *cache.find(key));
  }
  ASSERT_TRUE(cache.find(1).get());
  ASSERT_TRUE(cache.find(2).get());
}

TEST(LRU, lower_bound) {
  LRUTest cache;

  {
    unsigned int key = 1;
    ASSERT_FALSE(cache.lower_bound(key));
    int value = 2;

    ASSERT_TRUE(cache.add(key, value).get());
    ASSERT_TRUE(cache.lower_bound(key).get());
    EXPECT_EQ(value, *cache.lower_bound(key));
  }
}

TEST(LRU, get_next) {

  {
    LRUTest cache;
    const unsigned int key = 0;
    EXPECT_FALSE(cache.upper_bound(key));
  }
  {
    LRUTest cache;
    const unsigned int key1 = 111;
    auto ptr1 = cache[key1];
    const unsigned int key2 = 222;
    auto ptr2 = cache[key2];

    auto i = cache.upper_bound(0);
    ASSERT_TRUE(i);
    EXPECT_EQ(i->first, key1);
    auto j = cache.upper_bound(i->first);
    ASSERT_TRUE(j);
    EXPECT_EQ(j->first, key2);
  }
}

TEST(LRU, clear) {
  LRUTest cache;
  unsigned int key = 1;
  int value = 2;
  cache.add(key, value);
  {
    auto found = cache.find(key);
    ASSERT_TRUE(found);
    ASSERT_EQ(value, *found);
  }
  ASSERT_TRUE(cache.find(key).get());
  cache.clear();
  ASSERT_FALSE(cache.find(key));
  ASSERT_TRUE(cache.empty());
}

TEST(LRU, eviction) {
  LRUTest cache{5};
  bool existed;
  // add a bunch of elements, some of them will be evicted
  for (size_t i = 0; i < 2 * cache.capacity(); ++i) {
    cache.add(i, i, &existed);
    ASSERT_FALSE(existed);
  }
  size_t i = 0;
  for (; i < cache.capacity(); ++i) {
    ASSERT_FALSE(cache.find(i));
  }
  for (; i < 2 * cache.capacity(); ++i) {
    ASSERT_TRUE(cache.find(i));
  }
}

TEST(LRU, track_weak) {
  constexpr int SIZE = 5;
  LRUTest cache{SIZE};

  bool existed = false;
  // strong reference to keep 0 alive
  auto ptr = cache.add(0, 0, &existed);
  ASSERT_FALSE(existed);

  // add a bunch of elements to get 0 evicted
  for (size_t i = 1; i < 2 * cache.capacity(); ++i) {
    cache.add(i, i, &existed);
    ASSERT_FALSE(existed);
  }
  // 0 is still reachable via the cache
  ASSERT_TRUE(cache.find(0));
  ASSERT_TRUE(cache.find(0).get());
  ASSERT_EQ(0, *cache.find(0));

  // [0..SIZE) are evicted when adding [SIZE..2*SIZE)
  // [SIZE..SIZE * 2) were still in the cache before accessing 0,
  // but SIZE got evicted when accessing 0
  ASSERT_FALSE(cache.find(SIZE-1));
  ASSERT_FALSE(cache.find(SIZE));
  ASSERT_TRUE(cache.find(SIZE+1));
  ASSERT_TRUE(cache.find(SIZE+1).get());
  ASSERT_EQ((int)SIZE+1, *cache.find(SIZE+1));

  ptr.reset();
  // 0 is still reachable, as it is now put back into LRU cache
  ASSERT_TRUE(cache.find(0));
}

// Local Variables:
// compile-command: "cmake --build ../../../build -j 8 --target unittest_seastar_lru  && ctest -R unittest_seastar_lru # --gtest_filter=*.* --log-to-stderr=true"
// End:
