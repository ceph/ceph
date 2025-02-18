// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Mirantis, Inc.
 *
 * Author: Igor Fedotov <ifedotov@mirantis.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <gtest/gtest.h>
#include <common/mini_flat_map.h>

struct Value {
  int value;

  explicit Value () : value(0) {}
  explicit Value (int value) : value(value) {}

  friend std::ostream& operator<<(std::ostream& lhs, const Value& rhs)
  {
    return lhs << rhs.value;
  }

  bool operator==(const Value &other) const { return value == other.value; }
};

struct Key {
  int8_t k;

  Key(int8_t k) : k(k) {}

  explicit constexpr operator int8_t() const { return k; }
  Key &operator++() {k++; return *this;}

  friend std::ostream& operator<<(std::ostream& lhs, const Key& rhs)
  {
    return lhs << (uint32_t)rhs.k;
  }
  friend bool operator==(const Key &lhs, const Key &rhs) { return lhs.k == rhs.k; }
};

TEST(mini_flat_map, copy_operator_and_element_access)
{
  mini_flat_map<Key, Value> m(4);
  m[0] = Value(1);
  ASSERT_EQ(1, m[0].value);
  Key non_const_key(0);
  ASSERT_EQ(1, m[non_const_key].value);
  ASSERT_TRUE(m.contains(0));
  mini_flat_map<Key, Value> m2 = m;
  ASSERT_EQ(m, m2);
  mini_flat_map<Key, Value> m3(m);
  ASSERT_TRUE(m3.contains(0));
  ASSERT_TRUE(m.contains(0));
}

TEST(mini_flat_map, iterators)
{
  mini_flat_map<Key, Value> m(4);
  m[0] = Value(1);
  m[2] = Value(2);
  Value values[] = {Value(1), Value(2)};
  Key keys[] = {Key(0), Key(2)};

  int  i=0;
  for (auto &&[k,v] : m) {
    ASSERT_EQ(keys[i], k);
    ASSERT_EQ(values[i], v);
    i++;
  }
  ASSERT_EQ(2, i);

  const mini_flat_map<Key, Value> m2 = m;
  i=0;
  // This loop tests const iterator.
  for (auto &&[k,v] : m2) {
    ASSERT_EQ(keys[i], k);
    ASSERT_EQ(values[i], v);
    i++;
  }
  ASSERT_EQ(2, i);
}

TEST(mini_flat_map, capacity)
{
  mini_flat_map<Key, Value> m(4);
  ASSERT_FALSE(m.contains(Key(0)));
  Key k(1);
  ASSERT_FALSE(m.contains(k));
  ASSERT_TRUE(m.empty());
  ASSERT_EQ(0, m.size());
  ASSERT_EQ(4, m.max_size());

  m[k] = Value(2);
  ASSERT_TRUE(m.contains(k));
  ASSERT_FALSE(m.empty());
  ASSERT_EQ(1, m.size());
  ASSERT_EQ(4, m.max_size());
}

TEST(mini_flat_map, clear)
{
  mini_flat_map<Key, Value> m(4);
  m[1] = Value(2);
  ASSERT_TRUE(m.contains(1));
  m.clear();
  ASSERT_FALSE(m.contains(1));
  ASSERT_TRUE(m.empty());
}
// No insert, insert_range, insert_or_assign, emplace_hint, try_emplace,

TEST(mini_flat_map, emplace_erase)
{
  mini_flat_map<Key, Value> m(4);
  m.emplace(1, 2);
  ASSERT_TRUE(m.contains(1));
  m.erase(Key(1));
  ASSERT_FALSE(m.contains(1));
  m.emplace(1, 2);
  ASSERT_TRUE(m.contains(1));
  auto it = m.begin();
  m.erase(it);
  ASSERT_EQ(m.end(), it);
  ASSERT_FALSE(m.contains(1));
  m.emplace(1, 2);
  ASSERT_TRUE(m.contains(1));
  auto cit = m.cbegin();
  m.erase(cit);
  ASSERT_EQ(m.cend(), cit);
  ASSERT_FALSE(m.contains(1));
}
// no erase(range)

TEST(mini_flat_map, swap)
{
  mini_flat_map<Key, Value> m(4);
  m[1] = Value(2);
  mini_flat_map<Key, Value> m2(4);
  m2.swap(m);
  ASSERT_TRUE(m.empty());
  ASSERT_FALSE(m2.empty());
}
// No extract, merge

TEST(mini_flat_map, lookup)
{
  mini_flat_map<Key, Value> m(4);
  ASSERT_EQ(0, m.count(Key(0)));
  ASSERT_EQ(0, m.count(Key(1)));
  m[1] = Value(2);
  ASSERT_EQ(0, m.count(Key(0)));
  ASSERT_EQ(1, m.count(Key(1)));
}
// NO equal_range, lower_bound, upper_bound

