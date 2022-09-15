// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <stdio.h>
#include "gtest/gtest.h"
#include "common/intrusive_lru.h"

template <typename TestLRUItem>
struct item_to_unsigned {
  using type = unsigned;
  const type &operator()(const TestLRUItem &item) {
    return item.key;
  }
};

struct TestLRUItem : public ceph::common::intrusive_lru_base<
  ceph::common::intrusive_lru_config<
    unsigned, TestLRUItem, item_to_unsigned<TestLRUItem>>> {
  unsigned key = 0;
  int value = 0;

  TestLRUItem(unsigned key) : key(key) {}
};

class LRUTest : public TestLRUItem::lru_t {
public:
  auto add(unsigned int key, int value) {
    auto [ref, key_existed] = get_or_create(key);
    if (!key_existed) {
      ref->value = value;
    }
    return std::pair(ref, key_existed);
  }
};

TEST(LRU, add_immediate_evict) {
  LRUTest cache;
  unsigned int key = 1;
  int value1 = 2;
  int value2 = 3;
  {
    auto [ref, existed] = cache.add(key, value1);
    ASSERT_TRUE(ref);
    ASSERT_EQ(value1, ref->value);
    ASSERT_FALSE(existed);
  }
  {
    auto [ref2, existed] = cache.add(key, value2);
    ASSERT_EQ(value2, ref2->value);
    ASSERT_FALSE(existed);
  }
}

TEST(LRU, lookup_lru_size) {
  LRUTest cache;
  int key = 1;
  int value = 1;
  cache.set_target_size(1);
  {
    auto [ref, existed] = cache.add(key, value);
    ASSERT_TRUE(ref);
    ASSERT_FALSE(existed);
  }
  {
    auto [ref, existed] = cache.add(key, value);
    ASSERT_TRUE(ref);
    ASSERT_TRUE(existed);
  }
  cache.set_target_size(0);
  auto [ref2, existed2] = cache.add(key, value);
  ASSERT_TRUE(ref2);
  ASSERT_FALSE(existed2);
  {
    auto [ref, existed] = cache.add(key, value);
    ASSERT_TRUE(ref);
    ASSERT_TRUE(existed);
  }
}

TEST(LRU, eviction) {
  const unsigned SIZE = 3;
  LRUTest cache;
  cache.set_target_size(SIZE);

  for (unsigned i = 0; i < SIZE; ++i) {
    auto [ref, existed] = cache.add(i, i);
    ASSERT_TRUE(ref && !existed);
  }

  {
    auto [ref, existed] = cache.add(0, 0);
    ASSERT_TRUE(ref && existed);
  }

  for (unsigned i = SIZE; i < (2*SIZE) - 1; ++i) {
    auto [ref, existed]  = cache.add(i, i);
    ASSERT_TRUE(ref && !existed);
  }

  {
    auto [ref, existed] = cache.add(0, 0);
    ASSERT_TRUE(ref && existed);
  }

  for (unsigned i = 1; i < SIZE; ++i) {
    auto [ref, existed] = cache.add(i, i);
    ASSERT_TRUE(ref && !existed);
  }
}

TEST(LRU, eviction_live_ref) {
  const unsigned SIZE = 3;
  LRUTest cache;
  cache.set_target_size(SIZE);

  auto [live_ref, existed2] = cache.add(1, 1);
  ASSERT_TRUE(live_ref && !existed2);

  for (unsigned i = 0; i < SIZE; ++i) {
    auto [ref, existed] = cache.add(i, i);
    ASSERT_TRUE(ref);
    if (i == 1) {
      ASSERT_TRUE(existed);
    } else {
      ASSERT_FALSE(existed);
    }
  }

  {
    auto [ref, existed] = cache.add(0, 0);
    ASSERT_TRUE(ref && existed);
  }

  for (unsigned i = SIZE; i < (2*SIZE) - 1; ++i) {
    auto [ref, existed]  = cache.add(i, i);
    ASSERT_TRUE(ref && !existed);
  }

  for (unsigned i = 0; i < SIZE; ++i) {
    auto [ref, existed] = cache.add(i, i);
    ASSERT_TRUE(ref);
    if (i == 1) {
      ASSERT_TRUE(existed);
    } else {
      ASSERT_FALSE(existed);
    }
  }
}
