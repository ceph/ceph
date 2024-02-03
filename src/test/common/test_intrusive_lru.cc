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


static int LIVE_TEST_LRU_ITEMS = 0;
struct TestLRUItem : public ceph::common::intrusive_lru_base<
  ceph::common::intrusive_lru_config<
    unsigned, TestLRUItem, item_to_unsigned<TestLRUItem>>> {
  unsigned key = 0;
  int value = 0;
  bool invalidated = false;

  TestLRUItem(unsigned key) : key(key) {
    ++LIVE_TEST_LRU_ITEMS;
  }
  ~TestLRUItem() { --LIVE_TEST_LRU_ITEMS; }
};
using TestLRUItemRef = boost::intrusive_ptr<TestLRUItem>;

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

TEST(LRU, clear_range) {
  LRUTest cache;
  const unsigned SIZE = 10;
  cache.set_target_size(SIZE);
  {
    auto [ref, existed] = cache.add(1, 4);
    ASSERT_FALSE(existed);
  }
  {
    auto [ref, existed] = cache.add(2, 4);
    ASSERT_FALSE(existed);
  }
  {
    auto [ref, existed] = cache.add(3, 4);
    ASSERT_FALSE(existed);
  }
  // Unlike above, the reference is not being destroyed
  auto [live_ref1, existed1] = cache.add(4, 4);
  ASSERT_FALSE(existed1);

  auto [live_ref2, existed2] = cache.add(5, 4);
  ASSERT_FALSE(existed2);

  cache.clear_range(0,4);

  // Should not exists (Unreferenced):
  {
    auto [ref, existed] = cache.add(1, 4);
    ASSERT_FALSE(existed);
  }
  {
    auto [ref, existed] = cache.add(2, 4);
    ASSERT_FALSE(existed);
  }
  {
    auto [ref, existed] = cache.add(3, 4);
    ASSERT_FALSE(existed);
  }
  // Should exist (Still being referenced):
  {
    auto [ref, existed] = cache.add(4, 4);
    ASSERT_TRUE(existed);
  }
  // Should exists (Still being referenced and wasn't removed)
  {
    auto [ref, existed] = cache.add(5, 4);
    ASSERT_TRUE(existed);
  }
  // Test out of bound deletion:
  {
    cache.clear_range(3,8);
    auto [ref, existed] = cache.add(4, 4);
    ASSERT_TRUE(existed);
  }
  {
    auto [ref, existed] = cache.add(3, 4);
    ASSERT_FALSE(existed);
  }
}

TEST(LRU, clear) {
  LRUTest cache;
  const unsigned SIZE = 10;
  cache.set_target_size(SIZE);
  
  std::vector<TestLRUItemRef> refs;
  for (unsigned i = 0; i < 100; ++i) {
    auto [ref, existed] = cache.add(i, i);
    ASSERT_FALSE(existed);
    if ((i % 2) == 0) {
      refs.push_back(ref);
    }
  }

  for (unsigned i = 0; i < 100; i += 2) {
    auto [ref, existed] = cache.add(i, i);
    ASSERT_TRUE(existed);
  }

  cache.clear([](auto &i) { i.invalidated = true; });
  ASSERT_EQ(refs.size(), LIVE_TEST_LRU_ITEMS);

  for (auto &i: refs) {
    ASSERT_TRUE(i->invalidated);
  }

  std::vector<TestLRUItemRef> refs_new;
  for (unsigned i = 0; i < 100; ++i) {
    auto [ref, existed] = cache.add(i, i);
    ASSERT_FALSE(existed);
    ASSERT_FALSE(ref->invalidated);
    if ((i % 2) == 0) {
      refs_new.push_back(ref);
    }
  }

  for (unsigned i = 0; i < 100; i += 2) {
    auto [ref, existed] = cache.add(i, i);
    ASSERT_TRUE(existed);
    ASSERT_FALSE(ref->invalidated);
  }

  refs.clear();
  cache.set_target_size(0);
  ASSERT_EQ(refs_new.size(), LIVE_TEST_LRU_ITEMS);
  cache.set_target_size(SIZE);

  for (unsigned i = 100; i < 200; ++i) {
    auto [ref, existed] = cache.add(i, i);
    ASSERT_FALSE(existed);
    ASSERT_FALSE(ref->invalidated);
    if ((i % 2) == 0) {
      refs_new.push_back(ref);
    }
  }

  for (unsigned i = 0; i < 200; i += 2) {
    auto [ref, existed] = cache.add(i, i);
    ASSERT_TRUE(existed);
    ASSERT_FALSE(ref->invalidated);
  }

  ASSERT_EQ(refs_new.size(), LIVE_TEST_LRU_ITEMS);
  refs_new.clear();
  ASSERT_EQ(SIZE, LIVE_TEST_LRU_ITEMS);
  cache.set_target_size(0);
  ASSERT_EQ(0, LIVE_TEST_LRU_ITEMS);
}
