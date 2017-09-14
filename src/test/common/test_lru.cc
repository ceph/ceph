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
  Item() : id(0) {}
  Item(int i) : id(i) {}
  void set(int i) {id = i;}
};


TEST(lru, InsertTop) {
  LRU lru;
  static const int n = 100;
  Item items[n];

  lru.lru_set_midpoint(.5); // 50% of elements.
  for (int i=0; i<n; i++) {
    items[i].set(i);
    lru.lru_insert_top(&items[i]);
  }
  ASSERT_EQ(50U, lru.lru_get_top());
  ASSERT_EQ(50U, lru.lru_get_bot());
  ASSERT_EQ(100U, lru.lru_get_size());

  ASSERT_EQ(0, (static_cast<Item*>(lru.lru_expire()))->id);
}

TEST(lru, InsertMid) {
  LRU lru;
  static const int n = 102;
  Item items[n];

  lru.lru_set_midpoint(.7); // 70% of elements.
  for (int i=0; i<n; i++) {
    items[i].set(i);
    lru.lru_insert_mid(&items[i]);
  }
  ASSERT_EQ(71U, lru.lru_get_top());
  ASSERT_EQ(31U, lru.lru_get_bot());
  ASSERT_EQ(102U, lru.lru_get_size());

  ASSERT_EQ(0, (static_cast<Item*>(lru.lru_expire()))->id);
}

TEST(lru, InsertBot) {
  LRU lru;
  static const int n = 100;
  Item items[n];

  lru.lru_set_midpoint(.7); // 70% of elements.
  for (int i=0; i<n; i++) {
    items[i].set(i);
    lru.lru_insert_bot(&items[i]);
  }
  ASSERT_EQ(70U, lru.lru_get_top());
  ASSERT_EQ(30U, lru.lru_get_bot());
  ASSERT_EQ(100U, lru.lru_get_size());

  ASSERT_EQ(99, (static_cast<Item*>(lru.lru_expire()))->id);
}

TEST(lru, Adjust) {
  LRU lru;
  static const int n = 100;
  Item items[n];

  lru.lru_set_midpoint(.6); // 60% of elements.
  for (int i=0; i<n; i++) {
    items[i].set(i);
    lru.lru_insert_top(&items[i]);
    if (i % 5 == 0)
      items[i].lru_pin();
  }
  ASSERT_EQ(48U, lru.lru_get_top()); /* 60% of unpinned */
  ASSERT_EQ(52U, lru.lru_get_bot());
  ASSERT_EQ(100U, lru.lru_get_size());

  ASSERT_EQ(1, (static_cast<Item*>(lru.lru_expire()))->id);
  ASSERT_EQ(1U, lru.lru_get_pintail());
  ASSERT_EQ(47U, lru.lru_get_top()); /* 60% of unpinned */
  ASSERT_EQ(51U, lru.lru_get_bot());
  ASSERT_EQ(99U, lru.lru_get_size());
  ASSERT_EQ(2, (static_cast<Item*>(lru.lru_expire()))->id);
  ASSERT_EQ(1U, lru.lru_get_pintail());
  ASSERT_EQ(46U, lru.lru_get_top()); /* 60% of unpinned */
  ASSERT_EQ(51U, lru.lru_get_bot());
  ASSERT_EQ(98U, lru.lru_get_size());
  ASSERT_EQ(3, (static_cast<Item*>(lru.lru_expire()))->id);
  ASSERT_EQ(4, (static_cast<Item*>(lru.lru_expire()))->id);
  ASSERT_EQ(6, (static_cast<Item*>(lru.lru_expire()))->id);
  ASSERT_EQ(2U, lru.lru_get_pintail());
  ASSERT_EQ(45U, lru.lru_get_top()); /* 60% of unpinned */
  ASSERT_EQ(48U, lru.lru_get_bot());
  ASSERT_EQ(95U, lru.lru_get_size());
}

TEST(lru, Pinning) {
  LRU lru;

  Item ob0(0), ob1(1);

  // test before ob1 are in a LRU
  ob1.lru_pin();
  ASSERT_FALSE(ob1.lru_is_expireable());

  ob1.lru_unpin();
  ASSERT_TRUE(ob1.lru_is_expireable());

  // test when ob1 are in a LRU
  lru.lru_insert_top(&ob0);
  lru.lru_insert_top(&ob1);

  ob1.lru_pin();
  ob1.lru_pin(); // Verify that, one incr.
  ASSERT_EQ(1U, lru.lru_get_num_pinned());
  ASSERT_FALSE(ob1.lru_is_expireable());

  ob1.lru_unpin();
  ob1.lru_unpin(); // Verify that, one decr.
  ASSERT_EQ(0U, lru.lru_get_num_pinned());
  ASSERT_TRUE(ob1.lru_is_expireable());

  ASSERT_EQ(0, (static_cast<Item*>(lru.lru_expire()))->id);
  ob0.lru_pin();
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
