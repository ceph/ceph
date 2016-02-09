// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"
#include "common/PrioritizedQueue.h"

#include <numeric>
#include <vector>
#include <algorithm>

using std::vector;

class PrioritizedQueueTest : public testing::Test
{
protected:
  typedef int Klass;
  typedef unsigned Item;
  typedef PrioritizedQueue<Item, Klass> PQ;
  enum { item_size  = 100, };
  vector<Item> items;

  virtual void SetUp() {
    for (int i = 0; i < item_size; i++) {
      items.push_back(Item(i));
    }
    std::random_shuffle(items.begin(), items.end());
  }
  virtual void TearDown() {
    items.clear();
  }
};

TEST_F(PrioritizedQueueTest, capacity) {
  const unsigned min_cost  = 10;
  const unsigned max_tokens_per_subqueue = 50;
  PQ pq(max_tokens_per_subqueue, min_cost);
  EXPECT_TRUE(pq.empty());
  EXPECT_EQ(0u, pq.length());

  pq.enqueue_strict(Klass(1), 0, Item(0));
  EXPECT_FALSE(pq.empty());
  EXPECT_EQ(1u, pq.length());

  for (int i = 0; i < 3; i++) {
    pq.enqueue(Klass(1), 0, 10, Item(0));
  }
  for (unsigned i = 4; i > 0; i--) {
    EXPECT_FALSE(pq.empty());
    EXPECT_EQ(i, pq.length());
    pq.dequeue();
  }
  EXPECT_TRUE(pq.empty());
  EXPECT_EQ(0u, pq.length());
}

TEST_F(PrioritizedQueueTest, strict_pq) {
  const unsigned min_cost = 1;
  const unsigned max_tokens_per_subqueue = 50;
  PQ pq(max_tokens_per_subqueue, min_cost);
  // 0 .. item_size-1
  for (unsigned i = 0; i < item_size; i++) {
    unsigned priority = items[i];
    pq.enqueue_strict(Klass(0), priority, items[i]);
  }
  // item_size-1 .. 0
  for (unsigned i = item_size; i > 0; i--) {
    Item item = pq.dequeue();
    unsigned priority = item;
    EXPECT_EQ(i - 1, priority);
  }
}

TEST_F(PrioritizedQueueTest, lowest_among_eligible_otherwise_highest) {
  // to minimize the effect of `distribute_tokens()`
  // all eligible items will be assigned with cost of min_cost
  const unsigned min_cost = 0;
  const unsigned max_tokens_per_subqueue = 100;
  PQ pq(max_tokens_per_subqueue, min_cost);

#define ITEM_TO_COST(item_) (item_ % 5 ? min_cost : max_tokens_per_subqueue)
  unsigned num_low_cost = 0, num_high_cost = 0;
  for (int i = 0; i < item_size; i++) {
    const Item& item = items[i];
    unsigned cost = ITEM_TO_COST(item);
    unsigned priority = item;
    if (cost == min_cost) {
      num_low_cost++;
    } else {
      num_high_cost++;
    }
    pq.enqueue(Klass(0), priority, cost, item);
  }
  // the token in all buckets is 0 at the beginning, so dequeue() should pick
  // the first one with the highest priority.
  unsigned highest_priority;
  {
    Item item = pq.dequeue();
    unsigned cost = ITEM_TO_COST(item);
    unsigned priority = item;
    if (cost == min_cost) {
      num_low_cost--;
    } else {
      num_high_cost--;
    }
    EXPECT_EQ(item_size - 1u, priority);
    highest_priority = priority;
  }
  unsigned lowest_priority = 0;
  for (unsigned i = 0; i < num_low_cost; i++) {
    Item item = pq.dequeue();
    unsigned cost = ITEM_TO_COST(item);
    unsigned priority = item;
    EXPECT_EQ(min_cost, cost);
    EXPECT_GT(priority, lowest_priority);
    lowest_priority = priority;
  }
  for (unsigned i = 0; i < num_high_cost; i++) {
    Item item = pq.dequeue();
    unsigned cost = ITEM_TO_COST(item);
    unsigned priority = item;
    EXPECT_EQ(max_tokens_per_subqueue, cost);
    EXPECT_LT(priority, highest_priority);
    highest_priority = priority;
  }
#undef ITEM_TO_COST
}

static const unsigned num_classes = 4;
// just a determinitic number
#define ITEM_TO_CLASS(item_) Klass((item_ + 43) % num_classes)

TEST_F(PrioritizedQueueTest, fairness_by_class) {
  // dequeue should be fair to all classes in a certain bucket
  const unsigned min_cost = 1;
  const unsigned max_tokens_per_subqueue = 50;
  PQ pq(max_tokens_per_subqueue, min_cost);

  for (int i = 0; i < item_size; i++) {
    const Item& item = items[i];
    Klass k = ITEM_TO_CLASS(item);
    unsigned priority = 0;
    unsigned cost = 1;
    pq.enqueue(k, priority, cost, item);
  }
  // just sample first 1/2 of the items
  // if i pick too small a dataset, the result won't be statisitcally
  // significant. if the sample dataset is too large, it will converge to the
  // distribution of the full set.
  vector<unsigned> num_picked_in_class(num_classes, 0u);
  for (int i = 0; i < item_size / 2; i++) {
    Item item = pq.dequeue();
    Klass k = ITEM_TO_CLASS(item);
    num_picked_in_class[k]++;
  }
  unsigned total = std::accumulate(num_picked_in_class.begin(),
				   num_picked_in_class.end(),
				   0);
  float avg = float(total) / num_classes;
  for (unsigned i = 0; i < num_classes; i++) {
    EXPECT_NEAR(avg, num_picked_in_class[i], 0.5);
  }
}

template <typename T>
struct Greater {
  const T rhs;
  explicit Greater(const T& v) : rhs(v)
  {}
  bool operator()(const T& lhs) const {
    return lhs > rhs;
  }
};

TEST_F(PrioritizedQueueTest, remove_by_filter) {
  const unsigned min_cost = 1;
  const unsigned max_tokens_per_subqueue = 50;
  PQ pq(max_tokens_per_subqueue, min_cost);

  const Greater<Item> pred(item_size/2);
  unsigned num_to_remove = 0;
  for (unsigned i = 0; i < item_size; i++) {
    const Item& item = items[i];
    pq.enqueue(Klass(1), 0, 10, item);
    if (pred(item)) {
      num_to_remove++;
    }
  }
  std::list<Item> removed;
  pq.remove_by_filter(pred, &removed);

  // see if the removed items are expected ones.
  for (std::list<Item>::iterator it = removed.begin();
       it != removed.end();
       ++it) {
    const Item& item = *it;
    EXPECT_TRUE(pred(item));
    items.erase(remove(items.begin(), items.end(), item), items.end());
  }
  EXPECT_EQ(num_to_remove, removed.size());
  EXPECT_EQ(item_size - num_to_remove, pq.length());
  EXPECT_EQ(item_size - num_to_remove, items.size());
  // see if the remainder are expeceted also.
  while (!pq.empty()) {
    const Item item = pq.dequeue();
    EXPECT_FALSE(pred(item));
    items.erase(remove(items.begin(), items.end(), item), items.end());
  }
  EXPECT_TRUE(items.empty());
}

TEST_F(PrioritizedQueueTest, remove_by_class) {
  const unsigned min_cost = 1;
  const unsigned max_tokens_per_subqueue = 50;
  PQ pq(max_tokens_per_subqueue, min_cost);
  const Klass class_to_remove(2);
  unsigned num_to_remove = 0;
  for (int i = 0; i < item_size; i++) {
    const Item& item = items[i];
    Klass k = ITEM_TO_CLASS(item);
    pq.enqueue(k, 0, 0, item);
    if (k == class_to_remove) {
      num_to_remove++;
    }
  }
  std::list<Item> removed;
  pq.remove_by_class(class_to_remove, &removed);

  // see if the removed items are expected ones.
  for (std::list<Item>::iterator it = removed.begin();
       it != removed.end();
       ++it) {
    const Item& item = *it;
    Klass k = ITEM_TO_CLASS(item);
    EXPECT_EQ(class_to_remove, k);
    items.erase(remove(items.begin(), items.end(), item), items.end());
  }
  EXPECT_EQ(num_to_remove, removed.size());
  EXPECT_EQ(item_size - num_to_remove, pq.length());
  EXPECT_EQ(item_size - num_to_remove, items.size());
  // see if the remainder are expeceted also.
  while (!pq.empty()) {
    const Item item = pq.dequeue();
    Klass k = ITEM_TO_CLASS(item);
    EXPECT_NE(class_to_remove, k);
    items.erase(remove(items.begin(), items.end(), item), items.end());
  }
  EXPECT_TRUE(items.empty());
}
