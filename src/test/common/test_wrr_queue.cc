// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"
#include "common/WrrQueue.h"

#include <numeric>
#include <vector>
#include <map>
#include <list>

class WrrQueueTest : public testing::Test
{
protected:
  typedef int Klass;
  typedef unsigned Item;
  typedef unsigned Prio;
  typedef unsigned Kost;
  typedef WrrQueue<Item, Klass> WQ;
  // Simulate queue structure
  typedef std::list<Kost, Item> ItemList;
  typedef std::map<Klass, ItemList> KlassItem;
  typedef std::map<Prio, KlassItem> LQ;
  enum { item_size  = (rand() % 500) + 500, };

  virtual void SetUp() {
  }
  virtual void TearDown() {
    items.clear();
  }
};

TEST_F(WrrQueueTest, wrr_size){
  WQ wq(0, 0);
  EXPECT_TRUE(wq.empty());
  EXPECT_EQ(0u, wq.length());

  for (int i = 1; i < 5; ++i) {
    wq.enqueue(Klass(i), Item(i), 0, 0,
      CEPH_OP_QUEUE_BACK, CEPH_OP_CLASS_STRICT);
    EXPECT_FALSE(wq.empty());
    EXPECT_EQ(i, wq.length());
  }
  for (int i = 5; i < 10; ++i) {
    wq.enqueue(Klass(i), Item(i));
    EXPECT_FALSE(wq.empty());
    EXPECT_EQ(i, wq.length());
  }
  for (int i = 9; i >0; --i) {
    wq.dequeue();
    EXPECT_FALSE(wq.empty());
    EXPECT_EQ(i, wq.length());
  }
  wq.dequeue();
  EXPECT_TRUE(wq.empty());
  EXPECT_EQ(i, wq.length());
}

TEST_F(WrrQueueTest, wrr_test){
}
