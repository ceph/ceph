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
  explicit Item(int v) : id(v) {}
};


TEST(lru, InsertTop) {
  LRU lru = LRU(10);

  lru.lru_set_midpoint(.5); // 50% of 10 elements.
  for (int i=0; i<100; i++) {
    lru.lru_insert_top(new Item(i));
  }
  ASSERT_EQ(5U, lru.lru_get_top());
  ASSERT_EQ(95U, lru.lru_get_bot());
  ASSERT_EQ(100U, lru.lru_get_size());

  ASSERT_EQ(0, (static_cast<Item*>(lru.lru_expire()))->id);
}

TEST(lru, InsertMid) {
  LRU lru = LRU(10);

  for (int i=0; i<100; i++) {
    lru.lru_insert_mid(new Item(i));
  }
  ASSERT_EQ(0U, lru.lru_get_top());
  ASSERT_EQ(100U, lru.lru_get_bot());
  ASSERT_EQ(100U, lru.lru_get_size());

  ASSERT_EQ(0, (static_cast<Item*>(lru.lru_expire()))->id);
}

TEST(lru, InsertBot) {
  LRU lru = LRU(10);

  for (int i=0; i<100; i++) {
    lru.lru_insert_bot(new Item(i));
  }
  ASSERT_EQ(0U, lru.lru_get_top());
  ASSERT_EQ(100U, lru.lru_get_bot());
  ASSERT_EQ(100U, lru.lru_get_size());

  ASSERT_EQ(99, (static_cast<Item*>(lru.lru_expire()))->id);
}

TEST(lru, Adjust) {
  LRU lru = LRU(10);

  lru.lru_set_midpoint(.6); // 60% of 10 elements.
  for (int i=0; i<100; i++) {
    lru.lru_touch(new Item(i));
  }
  ASSERT_EQ(6U, lru.lru_get_top());
  ASSERT_EQ(94U, lru.lru_get_bot());
  ASSERT_EQ(100U, lru.lru_get_size());

  lru.lru_clear();

  lru.lru_set_midpoint(1.2); // 120% of 10 elements.
  for (int i=0; i<100; i++) {
    lru.lru_touch(new Item(i));
  }
  ASSERT_EQ(12U, lru.lru_get_top());
  ASSERT_EQ(88U, lru.lru_get_bot());
  ASSERT_EQ(100U, lru.lru_get_size());
}

TEST(lru, Pinning) {
  LRU lru = LRU();

  Item *ob0 = new Item(0);
  Item *ob1 = new Item(1);

  // test before ob1 are in a LRU
  ob1->lru_pin();
  ASSERT_FALSE(ob1->lru_is_expireable());

  ob1->lru_unpin();
  ASSERT_TRUE(ob1->lru_is_expireable());

  // test when ob1 are in a LRU
  lru.lru_touch(ob0);
  lru.lru_touch(ob1);

  ob1->lru_pin();
  ob1->lru_pin(); // Verify that, one incr.
  ASSERT_EQ(1U, lru.lru_get_num_pinned());
  ASSERT_FALSE(ob1->lru_is_expireable());

  ob1->lru_unpin();
  ob1->lru_unpin(); // Verify that, one decr.
  ASSERT_EQ(0U, lru.lru_get_num_pinned());
  ASSERT_TRUE(ob1->lru_is_expireable());

  ASSERT_EQ(0, (static_cast<Item*>(lru.lru_expire()))->id);
  ob0->lru_pin();
  ASSERT_EQ(1, (static_cast<Item*>(lru.lru_expire()))->id);
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
