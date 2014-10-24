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
#include "common/shared_cache.hpp"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include <gtest/gtest.h>

using namespace std::tr1;

TEST(SharedCache_all, add) {
  SharedLRU<int, int> cache;
  unsigned int key = 1;
  int value = 2;
  shared_ptr<int> ptr = cache.add(key, new int(value));
  ASSERT_EQ(ptr, cache.lookup(key));
  ASSERT_EQ(value, *cache.lookup(key));
}

TEST(SharedCache_all, lru) {
  const size_t SIZE = 5;
  SharedLRU<int, int> cache(NULL, SIZE);

  bool existed = false;
  shared_ptr<int> ptr = cache.add(0, new int(0), &existed);
  ASSERT_FALSE(existed);
  {
    int *tmpint = new int(0);
    shared_ptr<int> ptr2 = cache.add(0, tmpint, &existed);
    ASSERT_TRUE(existed);
    delete tmpint;
  }
  for (size_t i = 1; i < 2*SIZE; ++i) {
    cache.add(i, new int(i), &existed);
    ASSERT_FALSE(existed);
  }

  ASSERT_TRUE(cache.lookup(0));
  ASSERT_EQ(0, *cache.lookup(0));

  ASSERT_FALSE(cache.lookup(SIZE-1));
  ASSERT_FALSE(cache.lookup(SIZE));
  ASSERT_TRUE(cache.lookup(SIZE+1));
  ASSERT_EQ(SIZE+1, *cache.lookup(SIZE+1));

  cache.purge(0);
  ASSERT_FALSE(cache.lookup(0));
  shared_ptr<int> ptr2 = cache.add(0, new int(0), &existed);
  ASSERT_FALSE(ptr == ptr2);
  ptr = shared_ptr<int>();
  ASSERT_TRUE(cache.lookup(0));
}

// Local Variables:
// compile-command: "cd ../.. ; make unittest_sharedptr_registry && ./unittest_sharedptr_registry # --gtest_filter=*.* --log-to-stderr=true"
// End:
