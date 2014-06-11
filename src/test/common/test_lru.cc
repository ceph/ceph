// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Sahid Orentino Ferdjaoui <sahid.ferdjaoui@cloudwatt.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include <errno.h>
#include <gtest/gtest.h>

#include "include/lru.h"


class Item : public LRUObject {
public:
  int id;
  Item(int v) : id(v) {}
};


TEST(lru, InsertTop) {
  LRU lru = LRU(10);

  lru.lru_set_midpoint(.5); // 50% of 10 elements.
  for (int i=0; i<100; i++) {
    lru.lru_insert_top(new Item(i));
  }
  ASSERT_EQ(5, lru.lru_get_top());
  ASSERT_EQ(95, lru.lru_get_bot());
  ASSERT_EQ(100, lru.lru_get_size());

  ASSERT_EQ(0, (static_cast<Item*>(lru.lru_expire()))->id);
}

TEST(lru, InsertMid) {
  LRU lru = LRU(10);

  for (int i=0; i<100; i++) {
    lru.lru_insert_mid(new Item(i));
  }
  ASSERT_EQ(0, lru.lru_get_top());
  ASSERT_EQ(100, lru.lru_get_bot());
  ASSERT_EQ(100, lru.lru_get_size());

  ASSERT_EQ(0, (static_cast<Item*>(lru.lru_expire()))->id);
}

TEST(lru, InsertBot) {
  LRU lru = LRU(10);

  for (int i=0; i<100; i++) {
    lru.lru_insert_bot(new Item(i));
  }
  ASSERT_EQ(0, lru.lru_get_top());
  ASSERT_EQ(100, lru.lru_get_bot());
  ASSERT_EQ(100, lru.lru_get_size());

  ASSERT_EQ(99, (static_cast<Item*>(lru.lru_expire()))->id);
}


/*
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 &&
 *   make unittest_lru &&
 *   valgrind --tool=memcheck --leak-check=full \
 *      ./unittest_lru
 *   "
 * End:
 */
