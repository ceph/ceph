// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */

#include <iostream>
#include <memory>
#include <set>

#include "gtest/gtest.h"

#include "indirect_intrusive_heap.h"


struct Elem {
  int data;

  crimson::IndIntruHeapData heap_data;
  crimson::IndIntruHeapData heap_data_alt;

  Elem(int _data) : data(_data) { }

  friend std::ostream& operator<<(std::ostream& out, const Elem& d) {
    out << d.data;
    return out;
  }
};


// sorted low to high
struct ElemCompare {
  bool operator()(const Elem& d1, const Elem& d2) {
    return d1.data < d2.data;
  }
};


// first all evens precede all odds, then they're sorted high to low
struct ElemCompareAlt {
  bool operator()(const Elem& d1, const Elem& d2) {
    if (0 == d1.data % 2) {
      if (0 == d2.data % 2) {
	return d1.data > d2.data;
      } else {
	return true;
      }
    } else if (0 == d2.data % 2) {
      return false;
    } else {
      return d1.data > d2.data;
    }
  }
};


class HeapFixture1: public ::testing::Test { 

public:

  crimson::IndIntruHeap<std::shared_ptr<Elem>,
			Elem,
			&Elem::heap_data,
			ElemCompare> heap;

  std::shared_ptr<Elem> data1, data2, data3, data4, data5, data6, data7;

  void SetUp() {
    data1 = std::make_shared<Elem>(2);
    data2 = std::make_shared<Elem>(99);
    data3 = std::make_shared<Elem>(1);
    data4 = std::make_shared<Elem>(-5);
    data5 = std::make_shared<Elem>(12);
    data6 = std::make_shared<Elem>(-12);
    data7 = std::make_shared<Elem>(-7);

    heap.push(data1);
    heap.push(data2);
    heap.push(data3);
    heap.push(data4);
    heap.push(data5);
    heap.push(data6);
    heap.push(data7);
  }

  void TearDown() { 
    // nothing to do
  }
}; // class HeapFixture1

TEST(IndIntruHeap, shared_ptr) {
  crimson::IndIntruHeap<std::shared_ptr<Elem>,
			Elem,
			&Elem::heap_data,
			ElemCompare> heap;

  EXPECT_TRUE(heap.empty());

  heap.push(std::make_shared<Elem>(2));

  EXPECT_FALSE(heap.empty());

  heap.push(std::make_shared<Elem>(99));
  heap.push(std::make_shared<Elem>(1));
  heap.push(std::make_shared<Elem>(-5));
  heap.push(std::make_shared<Elem>(12));
  heap.push(std::make_shared<Elem>(-12));
  heap.push(std::make_shared<Elem>(-7));

  // std::cout << heap << std::endl;

  EXPECT_FALSE(heap.empty());

  EXPECT_EQ(-12, heap.top().data);
  heap.pop();
  EXPECT_EQ(-7, heap.top().data);
  heap.pop();
  EXPECT_EQ(-5, heap.top().data);
  heap.pop();
  EXPECT_EQ(1, heap.top().data);
  heap.pop();
  EXPECT_EQ(2, heap.top().data);
  heap.pop();
  EXPECT_EQ(12, heap.top().data);
  heap.pop();
  EXPECT_EQ(99, heap.top().data);

  EXPECT_FALSE(heap.empty());
  heap.pop();
  EXPECT_TRUE(heap.empty());
}


TEST(IndIntruHeap, unique_ptr) {
  crimson::IndIntruHeap<std::unique_ptr<Elem>,
			Elem,
			&Elem::heap_data,
			ElemCompare> heap;

  EXPECT_TRUE(heap.empty());

  heap.push(std::unique_ptr<Elem>(new Elem(2)));

  EXPECT_FALSE(heap.empty());

  heap.push(std::unique_ptr<Elem>(new Elem(99)));
  heap.push(std::unique_ptr<Elem>(new Elem(1)));
  heap.push(std::unique_ptr<Elem>(new Elem(-5)));
  heap.push(std::unique_ptr<Elem>(new Elem(12)));
  heap.push(std::unique_ptr<Elem>(new Elem(-12)));
  heap.push(std::unique_ptr<Elem>(new Elem(-7)));

  EXPECT_FALSE(heap.empty());

  EXPECT_EQ(-12, heap.top().data);
  heap.pop();
  EXPECT_EQ(-7, heap.top().data);
  heap.pop();
  EXPECT_EQ(-5, heap.top().data);
  heap.pop();
  EXPECT_EQ(1, heap.top().data);
  heap.pop();
  EXPECT_EQ(2, heap.top().data);
  heap.pop();
  EXPECT_EQ(12, heap.top().data);
  heap.pop();
  EXPECT_EQ(99, heap.top().data);

  EXPECT_FALSE(heap.empty());
  heap.pop();
  EXPECT_TRUE(heap.empty());
}


TEST(IndIntruHeap, regular_ptr) {
  crimson::IndIntruHeap<Elem*, Elem, &Elem::heap_data, ElemCompare> heap;

  EXPECT_TRUE(heap.empty());

  heap.push(new Elem(2));

  EXPECT_FALSE(heap.empty());

  heap.push(new Elem(99));
  heap.push(new Elem(1));
  heap.push(new Elem(-5));
  heap.push(new Elem(12));
  heap.push(new Elem(-12));
  heap.push(new Elem(-7));

  EXPECT_FALSE(heap.empty());

  EXPECT_EQ(-12, heap.top().data);
  delete &heap.top();
  heap.pop();
  EXPECT_EQ(-7, heap.top().data);
  delete &heap.top();
  heap.pop();
  EXPECT_EQ(-5, heap.top().data);
  delete &heap.top();
  heap.pop();
  EXPECT_EQ(1, heap.top().data);
  delete &heap.top();
  heap.pop();
  EXPECT_EQ(2, heap.top().data);
  delete &heap.top();
  heap.pop();
  EXPECT_EQ(12, heap.top().data);
  delete &heap.top();
  heap.pop();
  EXPECT_EQ(99, heap.top().data);

  delete &heap.top();

  EXPECT_FALSE(heap.empty());
  heap.pop();
  EXPECT_TRUE(heap.empty());
}


TEST(IndIntruHeap, K_3) {
  crimson::IndIntruHeap<std::shared_ptr<Elem>,
			Elem,
			&Elem::heap_data,
			ElemCompare,
			3> heap;

  EXPECT_TRUE(heap.empty());

  heap.push(std::make_shared<Elem>(2));

  EXPECT_FALSE(heap.empty());

  heap.push(std::make_shared<Elem>(99));
  heap.push(std::make_shared<Elem>(1));
  heap.push(std::make_shared<Elem>(-5));
  heap.push(std::make_shared<Elem>(12));
  heap.push(std::make_shared<Elem>(-12));
  heap.push(std::make_shared<Elem>(-7));

  // std::cout << heap << std::endl;

  EXPECT_FALSE(heap.empty());

  EXPECT_EQ(-12, heap.top().data);
  heap.pop();
  EXPECT_EQ(-7, heap.top().data);
  heap.pop();
  EXPECT_EQ(-5, heap.top().data);
  heap.pop();
  EXPECT_EQ(1, heap.top().data);
  heap.pop();
  EXPECT_EQ(2, heap.top().data);
  heap.pop();
  EXPECT_EQ(12, heap.top().data);
  heap.pop();
  EXPECT_EQ(99, heap.top().data);

  EXPECT_FALSE(heap.empty());
  heap.pop();
  EXPECT_TRUE(heap.empty());
}


TEST(IndIntruHeap, K_4) {
  crimson::IndIntruHeap<std::shared_ptr<Elem>,
			Elem,
			&Elem::heap_data,
			ElemCompare,
			4> heap;

  EXPECT_TRUE(heap.empty());

  heap.push(std::make_shared<Elem>(2));

  EXPECT_FALSE(heap.empty());

  heap.push(std::make_shared<Elem>(99));
  heap.push(std::make_shared<Elem>(1));
  heap.push(std::make_shared<Elem>(-5));
  heap.push(std::make_shared<Elem>(12));
  heap.push(std::make_shared<Elem>(-12));
  heap.push(std::make_shared<Elem>(-7));

  // std::cout << heap << std::endl;

  EXPECT_FALSE(heap.empty());

  EXPECT_EQ(-12, heap.top().data);
  heap.pop();
  EXPECT_EQ(-7, heap.top().data);
  heap.pop();
  EXPECT_EQ(-5, heap.top().data);
  heap.pop();
  EXPECT_EQ(1, heap.top().data);
  heap.pop();
  EXPECT_EQ(2, heap.top().data);
  heap.pop();
  EXPECT_EQ(12, heap.top().data);
  heap.pop();
  EXPECT_EQ(99, heap.top().data);

  EXPECT_FALSE(heap.empty());
  heap.pop();
  EXPECT_TRUE(heap.empty());
}


TEST(IndIntruHeap, K_10) {
  crimson::IndIntruHeap<std::shared_ptr<Elem>,
			Elem,
			&Elem::heap_data,
			ElemCompare,
			10> heap;

  EXPECT_TRUE(heap.empty());

  heap.push(std::make_shared<Elem>(2));

  EXPECT_FALSE(heap.empty());

  heap.push(std::make_shared<Elem>(99));
  heap.push(std::make_shared<Elem>(1));
  heap.push(std::make_shared<Elem>(-5));
  heap.push(std::make_shared<Elem>(12));
  heap.push(std::make_shared<Elem>(-12));
  heap.push(std::make_shared<Elem>(-7));

  // std::cout << heap << std::endl;

  EXPECT_FALSE(heap.empty());

  EXPECT_EQ(-12, heap.top().data);
  heap.pop();
  EXPECT_EQ(-7, heap.top().data);
  heap.pop();
  EXPECT_EQ(-5, heap.top().data);
  heap.pop();
  EXPECT_EQ(1, heap.top().data);
  heap.pop();
  EXPECT_EQ(2, heap.top().data);
  heap.pop();
  EXPECT_EQ(12, heap.top().data);
  heap.pop();
  EXPECT_EQ(99, heap.top().data);

  EXPECT_FALSE(heap.empty());
  heap.pop();
  EXPECT_TRUE(heap.empty());
}


TEST(IndIntruHeap, multi_K) {
  crimson::IndIntruHeap<std::shared_ptr<Elem>,
			Elem,
			&Elem::heap_data,
			ElemCompare,
			2> heap2;

  crimson::IndIntruHeap<std::shared_ptr<Elem>,
			Elem,
			&Elem::heap_data,
			ElemCompare,
			3> heap3;

  crimson::IndIntruHeap<std::shared_ptr<Elem>,
			Elem,
			&Elem::heap_data,
			ElemCompare,
			4> heap4;

  crimson::IndIntruHeap<std::shared_ptr<Elem>,
			Elem,
			&Elem::heap_data,
			ElemCompare,
			10> heap10;

  // 250 should give us at least 4 levels on all heaps
  constexpr size_t count = 250;

  std::srand(std::time(0)); // use current time as seed for random generator

  // insert same set of random values into the four heaps
  for (size_t i = 0; i < count; ++i) {
    int value = std::rand() % 201 - 100; // -100...+100
    auto data = std::make_shared<Elem>(value);
    heap2.push(data);
    heap3.push(data);
    heap4.push(data);
    heap10.push(data);
  }

  auto bound = std::numeric_limits<decltype(Elem::data)>::min();

  for (size_t i = 0; i < count; ++i) {
    auto current = heap2.top().data;

    EXPECT_GE(current, bound) <<
      "we should never go down, only increase or remain the same";
    EXPECT_EQ(current, heap3.top().data) <<
      "heap1's data and heap3's data should match";
    EXPECT_EQ(current, heap4.top().data) <<
      "heap1's data and heap4's data should match";
    EXPECT_EQ(current, heap10.top().data) <<
      "heap1's data and heap10's data should match";

    heap2.pop();
    heap3.pop();
    heap4.pop();
    heap10.pop();

    bound = current;
  }
  
  EXPECT_TRUE(heap2.empty()) << "should be empty after all elements popped";
  EXPECT_TRUE(heap3.empty()) << "should be empty after all elements popped";
  EXPECT_TRUE(heap4.empty()) << "should be empty after all elements popped";
  EXPECT_TRUE(heap10.empty()) << "should be empty after all elements popped";
}


TEST(IndIntruHeap, demote) {
  crimson::IndIntruHeap<std::unique_ptr<Elem>,
			Elem,
			&Elem::heap_data,
			ElemCompare> heap;

  heap.push(std::unique_ptr<Elem>(new Elem(2)));
  heap.push(std::unique_ptr<Elem>(new Elem(99)));
  heap.push(std::unique_ptr<Elem>(new Elem(1)));
  heap.push(std::unique_ptr<Elem>(new Elem(-5)));
  heap.push(std::unique_ptr<Elem>(new Elem(12)));
  heap.push(std::unique_ptr<Elem>(new Elem(-12)));
  heap.push(std::unique_ptr<Elem>(new Elem(-7)));

  heap.top().data = 24;

  heap.demote(heap.top());

  EXPECT_EQ(-7, heap.top().data);

  heap.pop();
  heap.pop();
  heap.pop();
  heap.pop();
  heap.pop();

  EXPECT_EQ(24, heap.top().data);
}


TEST(IndIntruHeap, demote_not) {
  crimson::IndIntruHeap<std::unique_ptr<Elem>,
			Elem,
			&Elem::heap_data,
			ElemCompare> heap;

  heap.push(std::unique_ptr<Elem>(new Elem(2)));
  heap.push(std::unique_ptr<Elem>(new Elem(99)));
  heap.push(std::unique_ptr<Elem>(new Elem(1)));
  heap.push(std::unique_ptr<Elem>(new Elem(-5)));
  heap.push(std::unique_ptr<Elem>(new Elem(12)));
  heap.push(std::unique_ptr<Elem>(new Elem(-12)));
  heap.push(std::unique_ptr<Elem>(new Elem(-7)));

  heap.top().data = -99;

  heap.demote(heap.top());

  EXPECT_EQ(-99, heap.top().data);

  heap.pop();

  EXPECT_EQ(-7, heap.top().data);
}


TEST(IndIntruHeap, promote_and_demote) {
  crimson::IndIntruHeap<std::shared_ptr<Elem>,
			Elem,
			&Elem::heap_data,
			ElemCompare> heap;

  auto data1 = std::make_shared<Elem>(1);

  heap.push(std::make_shared<Elem>(2));
  heap.push(std::make_shared<Elem>(99));
  heap.push(data1);
  heap.push(std::make_shared<Elem>(-5));
  heap.push(std::make_shared<Elem>(12));
  heap.push(std::make_shared<Elem>(-12));
  heap.push(std::make_shared<Elem>(-7));

  EXPECT_EQ(-12, heap.top().data);

  data1->data = -99;
  heap.promote(*data1);

  EXPECT_EQ(-99, heap.top().data);

  data1->data = 999;
  heap.demote(*data1);

  EXPECT_EQ(-12, heap.top().data);

  data1->data = 9;
  heap.promote(*data1);

  heap.pop(); // remove -12
  heap.pop(); // remove -7
  heap.pop(); // remove -5
  heap.pop(); // remove 2

  EXPECT_EQ(9, heap.top().data);
}


TEST(IndIntruHeap, adjust) {
  crimson::IndIntruHeap<std::shared_ptr<Elem>,
			Elem,
			&Elem::heap_data,
			ElemCompare> heap;

  auto data1 = std::make_shared<Elem>(1);

  heap.push(std::make_shared<Elem>(2));
  heap.push(std::make_shared<Elem>(99));
  heap.push(data1);
  heap.push(std::make_shared<Elem>(-5));
  heap.push(std::make_shared<Elem>(12));
  heap.push(std::make_shared<Elem>(-12));
  heap.push(std::make_shared<Elem>(-7));

  // heap.display_sorted(std::cout);

  EXPECT_EQ(-12, heap.top().data);

  data1->data = 999;
  heap.adjust(*data1);

  EXPECT_EQ(-12, heap.top().data);

  data1->data = -99;
  heap.adjust(*data1);

  EXPECT_EQ(-99, heap.top().data);

  data1->data = 9;
  heap.adjust(*data1);

  EXPECT_EQ(-12, heap.top().data);

  heap.pop(); // remove -12
  heap.pop(); // remove -7
  heap.pop(); // remove -5
  heap.pop(); // remove 2

  EXPECT_EQ(9, heap.top().data);
}


TEST_F(HeapFixture1, shared_data) {

  crimson::IndIntruHeap<std::shared_ptr<Elem>,Elem,&Elem::heap_data_alt,ElemCompareAlt> heap2;

  heap2.push(data1);
  heap2.push(data2);
  heap2.push(data3);
  heap2.push(data4);
  heap2.push(data5);
  heap2.push(data6);
  heap2.push(data7);

  data3->data = 32;
  heap.adjust(*data3);
  heap2.adjust(*data3);

  EXPECT_EQ(-12, heap.top().data);
  heap.pop();
  EXPECT_EQ(-7, heap.top().data);
  heap.pop();
  EXPECT_EQ(-5, heap.top().data);
  heap.pop();
  EXPECT_EQ(2, heap.top().data);
  heap.pop();
  EXPECT_EQ(12, heap.top().data);
  heap.pop();
  EXPECT_EQ(32, heap.top().data);
  heap.pop();
  EXPECT_EQ(99, heap.top().data);

  EXPECT_EQ(32, heap2.top().data);
  heap2.pop();
  EXPECT_EQ(12, heap2.top().data);
  heap2.pop();
  EXPECT_EQ(2, heap2.top().data);
  heap2.pop();
  EXPECT_EQ(-12, heap2.top().data);
  heap2.pop();
  EXPECT_EQ(99, heap2.top().data);
  heap2.pop();
  EXPECT_EQ(-5, heap2.top().data);
  heap2.pop();
  EXPECT_EQ(-7, heap2.top().data);
}


TEST_F(HeapFixture1, iterator_basics) {
  {
    uint count = 0;
    for(auto i = heap.begin(); i != heap.end(); ++i) {
      ++count;
    }

    EXPECT_EQ(7, count) << "count should be 7";
  }

  auto i1 = heap.begin();
  
  EXPECT_EQ(-12, i1->data) <<
    "first member with * operator must be smallest";

  EXPECT_EQ(-12, (*i1).data) <<
    "first member with -> operator must be smallest";

  Elem& e1 = *i1;
  EXPECT_EQ(-12, e1.data) <<
    "first member with -> operator must be smallest";

  {
    std::set<int> values;
    values.insert(2);
    values.insert(99);
    values.insert(1);
    values.insert(-5);
    values.insert(12);
    values.insert(-12);
    values.insert(-7);

    for(auto i = heap.begin(); i != heap.end(); ++i) {
      auto v = *i;
      EXPECT_NE(values.end(), values.find(v.data)) <<
	"value in heap must be part of original set";
      values.erase(v.data);
    }
    EXPECT_EQ(0, values.size()) << "all values must have been seen";
  }
}


TEST_F(HeapFixture1, const_iterator_basics) {
  const auto& cheap = heap;

  {
    uint count = 0;
    for(auto i = cheap.cbegin(); i != cheap.cend(); ++i) {
      ++count;
    }

    EXPECT_EQ(7, count) << "count should be 7";
  }

  auto i1 = heap.cbegin();
  
  EXPECT_EQ(-12, i1->data) <<
    "first member with * operator must be smallest";

  EXPECT_EQ(-12, (*i1).data) <<
    "first member with -> operator must be smallest";

  const Elem& e1 = *i1;
  EXPECT_EQ(-12, e1.data) <<
    "first member with -> operator must be smallest";

  {
    std::set<int> values;
    values.insert(2);
    values.insert(99);
    values.insert(1);
    values.insert(-5);
    values.insert(12);
    values.insert(-12);
    values.insert(-7);

    for(auto i = heap.cbegin(); i != heap.cend(); ++i) {
      auto v = *i;
      EXPECT_NE(values.end(), values.find(v.data)) <<
	"value in heap must be part of original set";
      values.erase(v.data);
    }
    EXPECT_EQ(0, values.size()) << "all values must have been seen";
  }
}


TEST_F(HeapFixture1, iterator_find_rfind) {
  {
    auto it1 = heap.find(data7);
    EXPECT_NE(heap.end(), it1) << "find for included element should succeed";
    EXPECT_EQ(-7, it1->data) <<
      "find for included element should result in right value";

    auto fake_data = std::make_shared<Elem>(-7);
    auto it2 = heap.find(fake_data);
    EXPECT_EQ(heap.end(), it2) << "find for not included element should fail";
  }

  {
    auto it1 = heap.rfind(data7);
    EXPECT_NE(heap.end(), it1) <<
      "reverse find for included element should succeed";
    EXPECT_EQ(-7, it1->data) <<
      "reverse find for included element should result in right value";

    auto fake_data = std::make_shared<Elem>(-7);
    auto it2 = heap.rfind(fake_data);
    EXPECT_EQ(heap.end(), it2) <<
      "reverse find for not included element should fail";
  }
}


TEST_F(HeapFixture1, iterator_remove) {
  auto it1 = heap.find(data7);
  EXPECT_NE(heap.end(), it1) << "find for included element should succeed";

  heap.remove(it1);

  auto it2 = heap.find(data7);
  EXPECT_EQ(heap.end(), it2) << "find for removed element should fail";

  for (auto it3 = heap.begin(); it3 != heap.end(); ++it3) {
    EXPECT_NE(-7, it3->data) <<
      "iterating through heap should not find removed value";
  }

  // move through heap without -7
  EXPECT_EQ(-12, heap.top().data);
  heap.pop();
  EXPECT_EQ(-5, heap.top().data);
  heap.pop();
  EXPECT_EQ(1, heap.top().data);
  heap.pop();
  EXPECT_EQ(2, heap.top().data);
  heap.pop();
  EXPECT_EQ(12, heap.top().data);
  heap.pop();
  EXPECT_EQ(99, heap.top().data);
  heap.pop();
}


TEST_F(HeapFixture1, four_tops) {
  Elem& top1 = heap.top();
  EXPECT_EQ(-12, top1.data);

  const Elem& top2 = heap.top();
  EXPECT_EQ(-12, top2.data);

  std::shared_ptr<Elem> top3 = heap.top_ind();
  EXPECT_EQ(-12, top3->data);

  const std::shared_ptr<Elem> top4 = heap.top_ind();
  EXPECT_EQ(-12, top4->data);

  const auto& c_heap = heap;

  const Elem& top5 = c_heap.top();
  EXPECT_EQ(-12, top5.data);

  const std::shared_ptr<Elem> top6 = c_heap.top_ind();
  EXPECT_EQ(-12, top6->data);
}
