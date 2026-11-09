// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "gtest/gtest.h"
#include "common/Formatter.h"
#include "common/WeightedPriorityQueue.h"

#include <numeric>
#include <vector>
#include <map>
#include <list>
#include <tuple>

#define CEPH_OP_CLASS_STRICT	0
#define CEPH_OP_CLASS_NORMAL	0
#define CEPH_OP_QUEUE_BACK	0
#define CEPH_OP_QUEUE_FRONT	0

class WeightedPriorityQueueTest : public testing::Test
{
protected:
  typedef unsigned Klass;
  // tuple<Prio, Klass, OpID> so that we can verfiy the op
  typedef std::tuple<unsigned, unsigned, unsigned> Item;
  typedef unsigned Prio;
  typedef unsigned Kost;
  typedef WeightedPriorityQueue<Item, Klass> WQ;
  // Simulate queue structure
  typedef std::list<std::pair<Kost, Item> > ItemList;
  typedef std::map<Klass, ItemList> KlassItem;
  typedef std::map<Prio, KlassItem> LQ;
  typedef std::list<Item> Removed;
  const unsigned max_prios = 5; // (0-4) * 64
  const unsigned klasses = 37;  // Make prime to help get good coverage

  void fill_queue(WQ &wq, LQ &strictq, LQ &normq,
      unsigned item_size, bool randomize = false) {
    unsigned p, k, c, o, op_queue, fob;
    for (unsigned i = 1; i <= item_size; ++i) {
      // Choose priority, class, cost and 'op' for this op.
      if (randomize) {
        p = (rand() % max_prios) * 64;
        k = rand() % klasses;
        c = rand() % (1<<22);  // 4M cost
        // Make some of the costs 0, but make sure small costs
        // still work ok.
        if (c > (1<<19) && c < (1<<20)) {
          c = 0;
	}
        op_queue = rand() % 10;
        fob = rand() % 10;
      } else {
        p = (i % max_prios) * 64;
        k = i % klasses;
        c = (i % 8 == 0 || i % 16 == 0) ? 0 : 1 << (i % 23);
        op_queue = i % 7; // Use prime numbers to
        fob = i % 11;     // get better coverage
      }
      o = rand() % (1<<16);
      // Choose how to enqueue this op.
      switch (op_queue) {
      case 6 :
        // Strict Queue
        if (fob == 4) {
	  // Queue to the front.
	  strictq[p][k].push_front(std::make_pair(
  	    c, std::make_tuple(p, k, o)));
	  wq.enqueue_strict_front(Klass(k), p, std::make_tuple(p, k, o));
        } else {
	  //Queue to the back.
	  strictq[p][k].push_back(std::make_pair(
	    c, std::make_tuple(p, k, o)));
	  wq.enqueue_strict(Klass(k), p, std::make_tuple(p, k, o));
        }
        break;
      default:
        // Normal queue
        if (fob == 4) {
	  // Queue to the front.
	  normq[p][k].push_front(std::make_pair(
	    c, std::make_tuple(p, k, o)));
	  wq.enqueue_front(Klass(k), p, c, std::make_tuple(p, k, o));
        } else {
	  //Queue to the back.
	  normq[p][k].push_back(std::make_pair(
	    c, std::make_tuple(p, k, o)));
	  wq.enqueue(Klass(k), p, c, std::make_tuple(p, k, o));
        }
        break;
      }
    }
  }
  void test_queue(unsigned item_size, bool randomize = false,
                  bool eligible = false) {
    // Due to the WRR queue having a lot of probabilistic logic
    // we can't determine the exact order OPs will be dequeued.
    // However, the queue should not dequeue a priority out of
    // order. It should also dequeue the strict priority queue
    // first and in order. In both the strict and normal queues
    // push front and back should be respected. Here we keep
    // track of the ops queued and make sure they dequeue
    // correctly.
  
    // Set up local tracking queues
    WQ wq(0, 0);
    LQ strictq, normq;
    fill_queue(wq, strictq, normq, item_size, randomize);
    // Test that the queue is dequeuing properly.
    typedef std::map<unsigned, unsigned> LastKlass;
    LastKlass last_strict, last_norm;
    while (!(wq.empty())) {
      auto cost = [&](const Item &item) -> std::optional<unsigned> {
        auto &queue = strictq.empty() ? normq : strictq;
        return queue.at(std::get<0>(item)).at(std::get<1>(item)).front().first;
      };
      Item r = eligible ? *wq.dequeue_eligible(cost) : wq.dequeue();
      if (!(strictq.empty())) {
        // Check that there are no higher priorities
        // in the strict queue.
        LQ::reverse_iterator ri = strictq.rbegin();
        EXPECT_EQ(std::get<0>(r), ri->first);
        // Check that if there are multiple classes in a priority
        // that it is not dequeueing the same class each time.
        LastKlass::iterator si = last_strict.find(std::get<0>(r));
        if (strictq[std::get<0>(r)].size() > 1 && si != last_strict.end()) {
	  EXPECT_NE(std::get<1>(r), si->second);
	}
        last_strict[std::get<0>(r)] = std::get<1>(r);

	Item t = strictq[std::get<0>(r)][std::get<1>(r)].front().second;
        EXPECT_EQ(std::get<2>(r), std::get<2>(t));
        strictq[std::get<0>(r)][std::get<1>(r)].pop_front();
        if (strictq[std::get<0>(r)][std::get<1>(r)].empty()) {
	  strictq[std::get<0>(r)].erase(std::get<1>(r));
	}
        if (strictq[std::get<0>(r)].empty()) {
	  strictq.erase(std::get<0>(r));
	}
      } else {
        // Check that if there are multiple classes in a priority
        // that it is not dequeueing the same class each time.
        LastKlass::iterator si = last_norm.find(std::get<0>(r));
        if (normq[std::get<0>(r)].size() > 1 && si != last_norm.end()) {
	  EXPECT_NE(std::get<1>(r), si->second);
	}
        last_norm[std::get<0>(r)] = std::get<1>(r);

	Item t = normq[std::get<0>(r)][std::get<1>(r)].front().second;
        EXPECT_EQ(std::get<2>(r), std::get<2>(t));
        normq[std::get<0>(r)][std::get<1>(r)].pop_front();
        if (normq[std::get<0>(r)][std::get<1>(r)].empty()) {
	  normq[std::get<0>(r)].erase(std::get<1>(r));
	}
        if (normq[std::get<0>(r)].empty()) {
	  normq.erase(std::get<0>(r));
	}
      }
    }
  }

  void SetUp() override {
    srand(time(0));
  }
  void TearDown() override {
  }
};

TEST_F(WeightedPriorityQueueTest, wpq_size){
  WQ wq(0, 0);
  EXPECT_TRUE(wq.empty());
  EXPECT_EQ(0u, wq.get_size_slow());

  // Test the strict queue size.
  for (unsigned i = 1; i < 5; ++i) {
    wq.enqueue_strict(Klass(i),i, std::make_tuple(i, i, i));
    EXPECT_FALSE(wq.empty());
    EXPECT_EQ(i, wq.get_size_slow());
  }
  // Test the normal queue size.
  for (unsigned i = 5; i < 10; ++i) {
    wq.enqueue(Klass(i), i, i, std::make_tuple(i, i, i));
    EXPECT_FALSE(wq.empty());
    EXPECT_EQ(i, wq.get_size_slow());
  }
  // Test that as both queues are emptied
  // the size is correct.
  for (unsigned i = 8; i >0; --i) {
    wq.dequeue();
    EXPECT_FALSE(wq.empty());
    EXPECT_EQ(i, wq.get_size_slow());
  }
  wq.dequeue();
  EXPECT_TRUE(wq.empty());
  EXPECT_EQ(0u, wq.get_size_slow());
}

TEST_F(WeightedPriorityQueueTest, wpq_test_static) {
  test_queue(1000);
} 

TEST_F(WeightedPriorityQueueTest, wpq_test_random) {
  test_queue(rand() % 500 + 500, true);
} 

TEST_F(WeightedPriorityQueueTest, wpq_test_remove_by_class_null) {
  WQ wq(0, 0);
  LQ strictq, normq;
  unsigned num_items = 10;
  fill_queue(wq, strictq, normq, num_items);
  Removed wq_removed;
  // Pick a klass that was not enqueued
  wq.remove_by_class(klasses + 1, &wq_removed);
  EXPECT_EQ(0u, wq_removed.size());
}

TEST_F(WeightedPriorityQueueTest, eligible_queue_preserves_priority_and_owner_order)
{
  test_queue(1000, true, true);
}

TEST_F(WeightedPriorityQueueTest, blocked_owners_retain_their_round_robin_position)
{
  WQ queue(0, 0);
  for (unsigned i = 0; i < 2; ++i) {
    queue.enqueue(1, 5, 1, Item{5, 1, i});
    queue.enqueue(2, 5, 1, Item{5, 2, i});
  }
  bool blocked = false;
  auto cost = [&blocked](const Item &item) -> std::optional<unsigned> {
    if (blocked && std::get<1>(item) != 3) {
      return std::nullopt;
    }
    return 1;
  };
  EXPECT_EQ(std::get<1>(*queue.dequeue_eligible(cost)), 1);
  blocked = true;
  queue.enqueue_strict(3, 100, Item{100, 3, 0});
  EXPECT_EQ(std::get<1>(*queue.dequeue_eligible(cost)), 3);
  EXPECT_FALSE(queue.dequeue_eligible(cost));
  blocked = false;
  for (unsigned owner : {2, 1, 2}) {
    EXPECT_EQ(std::get<1>(*queue.dequeue_eligible(cost)), owner);
  }
  EXPECT_TRUE(queue.empty());
}

TEST_F(WeightedPriorityQueueTest, blocked_strict_work_does_not_hide_priority_zero)
{
  WQ queue(0, 0);
  queue.enqueue_strict(1, 100, Item{100, 1, 0});
  queue.enqueue_strict(2, 0, Item{0, 2, 0});
  queue.enqueue(3, 50, 1, Item{50, 3, 0});
  auto cost = [](const Item &item) -> std::optional<unsigned> {
    if (std::get<1>(item) == 1) {
      return std::nullopt;
    }
    return 0;
  };
  EXPECT_EQ(std::get<1>(*queue.dequeue_eligible(cost)), 2);
  EXPECT_EQ(std::get<1>(*queue.dequeue_eligible(cost)), 3);
  EXPECT_FALSE(queue.dequeue_eligible(cost));
  EXPECT_EQ(queue.get_size_slow(), 1);
  EXPECT_EQ(std::get<1>(queue.dequeue()), 1);
}

TEST_F(WeightedPriorityQueueTest, wpq_test_remove_by_class) {
  WQ wq(0, 0);
  LQ strictq, normq;
  unsigned num_items = 1000;
  fill_queue(wq, strictq, normq, num_items);
  unsigned num_to_remove = 0;
  const Klass k = 5;
  // Find how many ops are in the class
  for (LQ::iterator it = strictq.begin();
       it != strictq.end(); ++it) {
    num_to_remove += it->second[k].size();
  }
  for (LQ::iterator it = normq.begin();
       it != normq.end(); ++it) {
    num_to_remove += it->second[k].size();
  }
  Removed wq_removed;
  wq.remove_by_class(k, &wq_removed);
  // Check that the right ops were removed.
  EXPECT_EQ(num_to_remove, wq_removed.size());
  EXPECT_EQ(num_items - num_to_remove, wq.get_size_slow());
  for (Removed::iterator it = wq_removed.begin();
       it != wq_removed.end(); ++it) {
    EXPECT_EQ(k, std::get<1>(*it));
  }
  // Check that none were missed
  while (!(wq.empty())) {
    EXPECT_NE(k, std::get<1>(wq.dequeue()));
  }
}
