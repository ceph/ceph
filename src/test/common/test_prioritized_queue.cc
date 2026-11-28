// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "gtest/gtest.h"
#include "common/PrioritizedQueue.h"

#include <numeric>
#include <vector>
#include <algorithm>
#include <random>

using std::vector;

class PrioritizedQueueTest : public testing::Test
{
protected:
  typedef int Klass;
  typedef unsigned Item;
  typedef PrioritizedQueue<Item, Klass> PQ;
  enum { item_size  = 100, };
  vector<Item> items;

  void SetUp() override {
    for (int i = 0; i < item_size; i++) {
      items.push_back(Item(i));
    }
    std::random_device rd;
    std::default_random_engine rng(rd());
    std::shuffle(items.begin(), items.end(), rng);
  }
  void TearDown() override {
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
    const Item& item = items[i];
    pq.enqueue_strict(Klass(0), priority, Item(item));
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
    pq.enqueue(Klass(0), priority, cost, Item(item));
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
    pq.enqueue(k, priority, cost, Item(item));
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


TEST_F(PrioritizedQueueTest, remove_by_class) {
  const unsigned min_cost = 1;
  const unsigned max_tokens_per_subqueue = 50;
  PQ pq(max_tokens_per_subqueue, min_cost);
  const Klass class_to_remove(2);
  unsigned num_to_remove = 0;
  for (int i = 0; i < item_size; i++) {
    const Item& item = items[i];
    Klass k = ITEM_TO_CLASS(item);
    pq.enqueue(k, 0, 0, Item(item));
    if (k == class_to_remove) {
      num_to_remove++;
    }
  }
  auto removed = pq.remove_by_class(class_to_remove);

  // see if the removed items are expected ones.
  for (const auto item : removed) {
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

TEST_F(PrioritizedQueueTest, remove_by_class_preserves_order) {
  PQ pq(50, 1);
  constexpr Klass k = 7;

  pq.enqueue(k, 1, 1, 10);
  pq.enqueue(k, 1, 1, 11);
  pq.enqueue(k, 3, 1, 30);
  pq.enqueue(k, 3, 1, 31);
  pq.enqueue_strict(k, 2, 20);
  pq.enqueue_strict(k, 2, 21);
  pq.enqueue_strict(k, 4, 40);

  const std::vector<Item> expected {40, 20, 21, 30, 31, 10, 11};
  EXPECT_EQ(expected, pq.remove_by_class(k));
  EXPECT_TRUE(pq.empty());
}
