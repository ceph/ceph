// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank <info@inktank.com>
 *
 * LGPL-2.1 (see COPYING-LGPL2.1) or later
 */

#include <iostream>
#include <gtest/gtest.h>

#include "common/LRUSet.h"

struct thing {
  int a;
  thing(int i) : a(i) {}
  friend bool operator==(const thing &a, const thing &b) {
    return a.a == b.a;
  }
  friend std::size_t hash_value(const thing &value) {
    return value.a;
  }
};

namespace std {
  template<> struct hash<thing> {
    size_t operator()(const thing& r) const {
      return r.a;
    }
  };
}


TEST(LRUSet, insert_complex) {
  LRUSet<thing> s;
  s.insert(thing(1));
  s.insert(thing(2));

  ASSERT_TRUE(s.contains(thing(1)));
  ASSERT_TRUE(s.contains(thing(2)));
  ASSERT_FALSE(s.contains(thing(3)));
}

TEST(LRUSet, insert) {
  LRUSet<int> s;
  s.insert(1);
  s.insert(2);

  ASSERT_TRUE(s.contains(1));
  ASSERT_TRUE(s.contains(2));
  ASSERT_FALSE(s.contains(3));
}

TEST(LRUSet, erase) {
  LRUSet<int> s;
  s.insert(1);
  s.insert(2);
  s.insert(3);

  s.erase(2);
  ASSERT_TRUE(s.contains(1));
  ASSERT_FALSE(s.contains(2));
  ASSERT_TRUE(s.contains(3));
  s.prune(1);
  ASSERT_TRUE(s.contains(3));
  ASSERT_FALSE(s.contains(1));
}

TEST(LRUSet, prune) {
  LRUSet<int> s;
  int max = 1000;
  for (int i=0; i<max; ++i) {
    s.insert(i);
    s.prune(max / 10);
  }
  s.prune(0);
  ASSERT_TRUE(s.empty());
}

TEST(LRUSet, lru) {
  LRUSet<int> s;
  s.insert(1);
  s.insert(2);
  s.insert(3);
  s.prune(2);
  ASSERT_FALSE(s.contains(1));
  ASSERT_TRUE(s.contains(2));
  ASSERT_TRUE(s.contains(3));

  s.insert(2);
  s.insert(4);
  s.prune(2);
  ASSERT_FALSE(s.contains(3));
  ASSERT_TRUE(s.contains(2));
  ASSERT_TRUE(s.contains(4));
}

TEST(LRUSet, copy) {
  LRUSet<int> a, b;
  a.insert(1);
  b.insert(2);
  b.insert(3);
  a = b;
  ASSERT_FALSE(a.contains(1));
  ASSERT_TRUE(a.contains(2));
  ASSERT_TRUE(a.contains(3));
}
