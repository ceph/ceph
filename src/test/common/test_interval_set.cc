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
#include "include/interval_set.h"
#include "include/btree_interval_set.h"

using namespace ceph;

typedef uint64_t IntervalValueType;

template<typename T>  // tuple<type to test on, test array size>
class IntervalSetTest : public ::testing::Test {

 public:
  typedef T ISet;
};

typedef ::testing::Types< interval_set<IntervalValueType> ,  btree_interval_set<IntervalValueType> > IntervalSetTypes;

TYPED_TEST_CASE(IntervalSetTest, IntervalSetTypes);

TYPED_TEST(IntervalSetTest, compare) {
  typedef typename TestFixture::ISet ISet;
  ISet iset1, iset2;
  ASSERT_TRUE(iset1 == iset1);
  ASSERT_TRUE(iset1 == iset2);

  iset1.insert(1);
  ASSERT_FALSE(iset1 == iset2);

  iset2.insert(1);
  ASSERT_TRUE(iset1 == iset2);

  iset1.insert(2, 3);
  iset2.insert(2, 4);
  ASSERT_FALSE(iset1 == iset2);

  iset2.erase(2, 4);
  iset2.erase(1);
  iset2.insert(2, 3);
  iset2.insert(1);
  ASSERT_TRUE(iset1 == iset2);

  iset1.insert(100, 10);
  iset2.insert(100, 5);
  ASSERT_FALSE(iset1 == iset2);
  iset2.insert(105, 5);
  ASSERT_TRUE(iset1 == iset2);

  iset1.insert(200, 10);
  iset2.insert(205, 5);
  ASSERT_FALSE(iset1 == iset2);
  iset2.insert(200, 1);
  iset2.insert(202, 3);
  ASSERT_FALSE(iset1 == iset2);
  iset2.insert(201, 1);
  ASSERT_TRUE(iset1 == iset2);

  iset1.clear();
  ASSERT_FALSE(iset1 == iset2);
  iset2.clear();
  ASSERT_TRUE(iset1 == iset2);
}

TYPED_TEST(IntervalSetTest, contains) {
  typedef typename TestFixture::ISet ISet;
  ISet iset1;
  ASSERT_FALSE(iset1.contains( 1 ));
  ASSERT_FALSE(iset1.contains( 0, 1 ));

  iset1.insert(1);
  ASSERT_TRUE(iset1.contains( 1 ));
  ASSERT_FALSE(iset1.contains( 0 ));
  ASSERT_FALSE(iset1.contains( 2 ));
  ASSERT_FALSE(iset1.contains( 0, 1 ));
  ASSERT_FALSE(iset1.contains( 0, 2 ));
  ASSERT_TRUE(iset1.contains( 1, 1 ));
  ASSERT_FALSE(iset1.contains( 1, 2 ));

  iset1.insert(2, 3);
  ASSERT_TRUE(iset1.contains( 1 ));
  ASSERT_FALSE(iset1.contains( 0 ));
  ASSERT_TRUE(iset1.contains( 2 ));
  ASSERT_FALSE(iset1.contains( 0, 1 ));
  ASSERT_FALSE(iset1.contains( 0, 2 ));
  ASSERT_TRUE(iset1.contains( 1, 1 ));
  ASSERT_TRUE(iset1.contains( 1, 2 ));
  ASSERT_TRUE(iset1.contains( 1, 3 ));
  ASSERT_TRUE(iset1.contains( 1, 4 ));
  ASSERT_FALSE(iset1.contains( 1, 5 ));
  ASSERT_TRUE(iset1.contains( 2, 1 ));
  ASSERT_TRUE(iset1.contains( 2, 2 ));
  ASSERT_TRUE(iset1.contains( 2, 3 ));
  ASSERT_FALSE(iset1.contains( 2, 4 ));
  ASSERT_TRUE(iset1.contains( 3, 2 ));
  ASSERT_TRUE(iset1.contains( 4, 1 ));
  ASSERT_FALSE(iset1.contains( 4, 2 ));

  iset1.insert(10, 10);
  ASSERT_TRUE(iset1.contains( 1, 4 ));
  ASSERT_FALSE(iset1.contains( 1, 5 ));
  ASSERT_TRUE(iset1.contains( 2, 2 ));
  ASSERT_FALSE(iset1.contains( 2, 4 ));

  ASSERT_FALSE(iset1.contains( 1, 10 ));
  ASSERT_FALSE(iset1.contains( 9, 1 ));
  ASSERT_FALSE(iset1.contains( 9 ));
  ASSERT_FALSE(iset1.contains( 9, 11 ));
  ASSERT_TRUE(iset1.contains( 10, 1 ));
  ASSERT_TRUE(iset1.contains( 11, 9 ));
  ASSERT_TRUE(iset1.contains( 11, 2 ));
  ASSERT_TRUE(iset1.contains( 18, 2 ));
  ASSERT_TRUE(iset1.contains( 18, 2 ));
  ASSERT_TRUE(iset1.contains( 10 ));
  ASSERT_TRUE(iset1.contains( 19 ));
  ASSERT_FALSE(iset1.contains( 20 ));
  ASSERT_FALSE(iset1.contains( 21 ));

  ASSERT_FALSE(iset1.contains( 11, 11 ));
  ASSERT_FALSE(iset1.contains( 18, 9 ));

  iset1.clear();
  ASSERT_FALSE(iset1.contains( 1 ));
  ASSERT_FALSE(iset1.contains( 0 ));
  ASSERT_FALSE(iset1.contains( 2 ));
  ASSERT_FALSE(iset1.contains( 0, 1 ));
  ASSERT_FALSE(iset1.contains( 0, 2 ));
  ASSERT_FALSE(iset1.contains( 1, 1 ));
  ASSERT_FALSE(iset1.contains( 10, 2 ));
}

TYPED_TEST(IntervalSetTest, intersects) {
  typedef typename TestFixture::ISet ISet;
  ISet iset1;
  ASSERT_FALSE(iset1.intersects( 1, 1 ));
  ASSERT_FALSE(iset1.intersects( 0, 1 ));
  ASSERT_FALSE(iset1.intersects( 0, 10 ));

  iset1.insert(1);
  ASSERT_TRUE(iset1.intersects( 1, 1 ));
  ASSERT_FALSE(iset1.intersects( 0, 1 ));
  ASSERT_FALSE(iset1.intersects( 2, 1 ));
  ASSERT_TRUE(iset1.intersects( 0, 2 ));
  ASSERT_TRUE(iset1.intersects( 0, 20 ));
  ASSERT_TRUE(iset1.intersects( 1, 2 ));
  ASSERT_TRUE(iset1.intersects( 1, 20 ));

  iset1.insert(2, 3);
  ASSERT_FALSE(iset1.intersects( 0, 1 ));
  ASSERT_TRUE(iset1.intersects( 0, 2 ));
  ASSERT_TRUE(iset1.intersects( 0, 200 ));
  ASSERT_TRUE(iset1.intersects( 1, 1 ));
  ASSERT_TRUE(iset1.intersects( 1, 4 ));
  ASSERT_TRUE(iset1.intersects( 1, 5 ));
  ASSERT_TRUE(iset1.intersects( 2, 1 ));
  ASSERT_TRUE(iset1.intersects( 2, 2 ));
  ASSERT_TRUE(iset1.intersects( 2, 3 ));
  ASSERT_TRUE(iset1.intersects( 2, 4 ));
  ASSERT_TRUE(iset1.intersects( 3, 2 ));
  ASSERT_TRUE(iset1.intersects( 4, 1 ));
  ASSERT_TRUE(iset1.intersects( 4, 2 ));
  ASSERT_FALSE(iset1.intersects( 5, 2 ));

  iset1.insert(10, 10);
  ASSERT_TRUE(iset1.intersects( 1, 4 ));
  ASSERT_TRUE(iset1.intersects( 1, 5 ));
  ASSERT_TRUE(iset1.intersects( 1, 10 ));
  ASSERT_TRUE(iset1.intersects( 2, 2 ));
  ASSERT_TRUE(iset1.intersects( 2, 4 ));
  ASSERT_FALSE(iset1.intersects( 5, 1 ));
  ASSERT_FALSE(iset1.intersects( 5, 2 ));
  ASSERT_FALSE(iset1.intersects( 5, 5 ));
  ASSERT_TRUE(iset1.intersects( 5, 12 ));
  ASSERT_TRUE(iset1.intersects( 5, 20 ));

  ASSERT_FALSE(iset1.intersects( 9, 1 ));
  ASSERT_TRUE(iset1.intersects( 9, 2 ));

  ASSERT_TRUE(iset1.intersects( 9, 11 ));
  ASSERT_TRUE(iset1.intersects( 10, 1 ));
  ASSERT_TRUE(iset1.intersects( 11, 9 ));
  ASSERT_TRUE(iset1.intersects( 11, 2 ));
  ASSERT_TRUE(iset1.intersects( 11, 11 ));
  ASSERT_TRUE(iset1.intersects( 18, 2 ));
  ASSERT_TRUE(iset1.intersects( 18, 9 ));
  ASSERT_FALSE(iset1.intersects( 20, 1 ));
  ASSERT_FALSE(iset1.intersects( 21, 12 ));

  iset1.clear();
  ASSERT_FALSE(iset1.intersects( 0, 1 ));
  ASSERT_FALSE(iset1.intersects( 0, 2 ));
  ASSERT_FALSE(iset1.intersects( 1, 1 ));
  ASSERT_FALSE(iset1.intersects( 5, 2 ));
  ASSERT_FALSE(iset1.intersects( 10, 2 ));
}

TYPED_TEST(IntervalSetTest, insert_erase) {
  typedef typename TestFixture::ISet ISet;
  ISet iset1, iset2;
  IntervalValueType start, len;
  
  iset1.insert(3, 5, &start, &len);
  ASSERT_TRUE( start == 3 );
  ASSERT_TRUE( len == 5 );
  ASSERT_TRUE( iset1.num_intervals() == 1 );
  ASSERT_TRUE( iset1.size() == 5 );

  //adding standalone interval
  iset1.insert(15, 10, &start, &len);
  ASSERT_TRUE( start == 15 );
  ASSERT_TRUE( len == 10 );
  ASSERT_TRUE( iset1.num_intervals() == 2 );
  ASSERT_EQ( iset1.size(), 15 );

  //adding leftmost standalone interval
  iset1.insert(1, 1, &start, &len);
  ASSERT_TRUE( start == 1 );
  ASSERT_TRUE( len == 1 );
  ASSERT_TRUE( iset1.num_intervals() == 3 );
  ASSERT_EQ( iset1.size(), 16 );

  //adding leftmost adjusent interval
  iset1.insert(0, 1, &start, &len);
  ASSERT_TRUE( start == 0 );
  ASSERT_TRUE( len == 2 );
  ASSERT_TRUE( iset1.num_intervals() == 3 );
  ASSERT_EQ( iset1.size(), 17 );

  //adding interim interval that merges leftmost and subseqent intervals
  iset1.insert(2, 1, &start, &len);
  ASSERT_TRUE( start == 0 );
  ASSERT_TRUE( len == 8 );
  ASSERT_TRUE( iset1.num_intervals() == 2);
  ASSERT_EQ( iset1.size(), 18);

  //adding rigtmost standalone interval 
  iset1.insert(30, 5, &start, &len);
  ASSERT_TRUE( start == 30 );
  ASSERT_TRUE( len == 5 );
  ASSERT_TRUE( iset1.num_intervals() == 3);
  ASSERT_EQ( iset1.size(), 23 );

  //adding rigtmost adjusent interval 
  iset1.insert(35, 10, &start, &len);
  ASSERT_TRUE( start == 30 );
  ASSERT_TRUE( len == 15 );
  ASSERT_TRUE( iset1.num_intervals() == 3);
  ASSERT_EQ( iset1.size(), 33 );

  //adding interim interval that merges with the interval preceeding the rightmost
  iset1.insert(25, 1, &start, &len);
  ASSERT_TRUE( start == 15 );
  ASSERT_TRUE( len == 11 );
  ASSERT_TRUE( iset1.num_intervals() == 3);
  ASSERT_EQ( iset1.size(), 34);

  //adding interim interval that merges with the rightmost and preceeding intervals
  iset1.insert(26, 4, &start, &len);
  ASSERT_TRUE( start == 15 );
  ASSERT_TRUE( len == 30 );
  ASSERT_TRUE( iset1.num_intervals() == 2);
  ASSERT_EQ( iset1.size(), 38);

  //and finally build single interval filling the gap at  8-15 using different interval set
  iset2.insert( 8, 1 );
  iset2.insert( 14, 1 );
  iset2.insert( 9, 4 );
  iset1.insert( iset2 );
  iset1.insert(13, 1, &start, &len);
  ASSERT_TRUE( start == 0 );
  ASSERT_TRUE( len == 45 );
  ASSERT_TRUE( iset1.num_intervals() == 1);
  ASSERT_EQ( iset1.size(), 45);

  //now reverses the process using subtract & erase
  iset1.subtract( iset2 );
  iset1.erase(13, 1);
  ASSERT_TRUE( iset1.num_intervals() == 2);
  ASSERT_EQ( iset1.size(), 38);
  ASSERT_TRUE( iset1.contains( 7, 1 ));
  ASSERT_FALSE( iset1.contains( 8, 7 ));
  ASSERT_TRUE( iset1.contains( 15, 1 ));
  ASSERT_TRUE( iset1.contains( 26, 4 ));

  iset1.erase(26, 4);
  ASSERT_TRUE( iset1.num_intervals() == 3);
  ASSERT_EQ( iset1.size(), 34);
  ASSERT_TRUE( iset1.contains( 7, 1 ));
  ASSERT_FALSE( iset1.intersects( 8, 7 ));
  ASSERT_TRUE( iset1.contains( 15, 1 ));
  ASSERT_TRUE( iset1.contains( 25, 1 ));
  ASSERT_FALSE( iset1.contains( 26, 4 ));
  ASSERT_TRUE( iset1.contains( 30, 1 ));

  iset1.erase(25, 1);
  ASSERT_TRUE( iset1.num_intervals() == 3);
  ASSERT_EQ( iset1.size(), 33 );
  ASSERT_TRUE( iset1.contains( 24, 1 ));
  ASSERT_FALSE( iset1.contains( 25, 1 ));
  ASSERT_FALSE( iset1.intersects( 26, 4 ));
  ASSERT_TRUE( iset1.contains( 30, 1 ));
  ASSERT_TRUE( iset1.contains( 35, 10 ));

  iset1.erase(35, 10);
  ASSERT_TRUE( iset1.num_intervals() == 3);
  ASSERT_EQ( iset1.size(), 23 );
  ASSERT_TRUE( iset1.contains( 30, 5 ));
  ASSERT_TRUE( iset1.contains( 34, 1 ));
  ASSERT_FALSE( iset1.contains( 35, 10 ));
  ASSERT_FALSE(iset1.contains( 45, 1 ));

  iset1.erase(30, 5);
  ASSERT_TRUE( iset1.num_intervals() == 2);
  ASSERT_EQ( iset1.size(), 18);
  ASSERT_TRUE( iset1.contains( 2, 1 ));
  ASSERT_TRUE( iset1.contains( 24, 1 ));
  ASSERT_FALSE( iset1.contains( 25, 1 ));
  ASSERT_FALSE( iset1.contains( 29, 1 ));
  ASSERT_FALSE( iset1.contains( 30, 5 ));
  ASSERT_FALSE( iset1.contains( 35, 1 ));

  iset1.erase(2, 1);
  ASSERT_TRUE( iset1.num_intervals() == 3 );
  ASSERT_EQ( iset1.size(), 17 );
  ASSERT_TRUE( iset1.contains( 0, 1 ));
  ASSERT_TRUE( iset1.contains( 1, 1 ));
  ASSERT_FALSE( iset1.contains( 2, 1 ));
  ASSERT_TRUE( iset1.contains( 3, 1 ));
  ASSERT_TRUE( iset1.contains( 15, 1 ));
  ASSERT_FALSE( iset1.contains( 25, 1 ));

  iset1.erase( 0, 1);
  ASSERT_TRUE( iset1.num_intervals() == 3 );
  ASSERT_EQ( iset1.size(), 16 );
  ASSERT_FALSE( iset1.contains( 0, 1 ));
  ASSERT_TRUE( iset1.contains( 1, 1 ));
  ASSERT_FALSE( iset1.contains( 2, 1 ));
  ASSERT_TRUE( iset1.contains( 3, 1 ));
  ASSERT_TRUE( iset1.contains( 15, 1 ));

  iset1.erase(1, 1);
  ASSERT_TRUE( iset1.num_intervals() == 2 );
  ASSERT_EQ( iset1.size(), 15 );
  ASSERT_FALSE( iset1.contains( 1, 1 ));
  ASSERT_TRUE( iset1.contains( 15, 10 ));
  ASSERT_TRUE( iset1.contains( 3, 5 ));

  iset1.erase(15, 10);
  ASSERT_TRUE( iset1.num_intervals() == 1 );
  ASSERT_TRUE( iset1.size() == 5 );
  ASSERT_FALSE( iset1.contains( 1, 1 ));
  ASSERT_FALSE( iset1.contains( 15, 10 ));
  ASSERT_FALSE( iset1.contains( 25, 1 ));
  ASSERT_TRUE( iset1.contains( 3, 5 ));

  iset1.erase( 3, 1);
  ASSERT_TRUE( iset1.num_intervals() == 1 );
  ASSERT_TRUE( iset1.size() == 4 );
  ASSERT_FALSE( iset1.contains( 1, 1 ));
  ASSERT_FALSE( iset1.contains( 15, 10 ));
  ASSERT_FALSE( iset1.contains( 25, 1 ));
  ASSERT_TRUE( iset1.contains( 4, 4 ));
  ASSERT_FALSE( iset1.contains( 3, 5 ));

  iset1.erase( 4, 4);
  ASSERT_TRUE( iset1.num_intervals() == 0);
  ASSERT_TRUE( iset1.size() == 0);
  ASSERT_FALSE( iset1.contains( 1, 1 ));
  ASSERT_FALSE( iset1.contains( 15, 10 ));
  ASSERT_FALSE( iset1.contains( 25, 1 ));
  ASSERT_FALSE( iset1.contains( 3, 4 ));
  ASSERT_FALSE( iset1.contains( 3, 5 ));
  ASSERT_FALSE( iset1.contains( 4, 4 ));


}

TYPED_TEST(IntervalSetTest, intersect_of) {
  typedef typename TestFixture::ISet ISet;
  ISet iset1, iset2, iset3;

  iset1.intersection_of( iset2, iset3 );
  ASSERT_TRUE( iset1.num_intervals() == 0);
  ASSERT_TRUE( iset1.size() == 0);

  iset2.insert( 0, 1 );
  iset2.insert( 5, 10 );
  iset2.insert( 30, 10 );

  iset3.insert( 0, 2 );
  iset3.insert( 15, 1 );
  iset3.insert( 20, 5 );
  iset3.insert( 29, 3 );
  iset3.insert( 35, 3 );
  iset3.insert( 39, 3 );

  iset1.intersection_of( iset2, iset3 );
  ASSERT_TRUE( iset1.num_intervals() == 4);
  ASSERT_TRUE( iset1.size() == 7);

  ASSERT_TRUE( iset1.contains( 0, 1 ));
  ASSERT_FALSE( iset1.contains( 0, 2 ));

  ASSERT_FALSE( iset1.contains( 5, 11 ));
  ASSERT_FALSE( iset1.contains( 4, 1 ));
  ASSERT_FALSE( iset1.contains( 16, 1 ));
  
  ASSERT_FALSE( iset1.contains( 20, 5 ));

  ASSERT_FALSE( iset1.contains( 29, 1 ));
  ASSERT_FALSE( iset1.contains( 30, 10 ));

  ASSERT_TRUE( iset1.contains( 30, 2 ));
  ASSERT_TRUE( iset1.contains( 35, 3 ));
  ASSERT_FALSE( iset1.contains( 35, 4 ));

  ASSERT_TRUE( iset1.contains( 39, 1 ));
  ASSERT_FALSE( iset1.contains( 38, 2 ));
  ASSERT_FALSE( iset1.contains( 39, 2 ));

  iset3=iset1;
  iset1.intersection_of(iset2);
  ASSERT_TRUE( iset1 == iset3);

  iset2.clear();
  iset2.insert(0,1);
  iset1.intersection_of(iset2);
  ASSERT_TRUE( iset1.num_intervals() == 1);
  ASSERT_TRUE( iset1.size() == 1);

  iset1 = iset3;
  iset2.clear();
  iset1.intersection_of(iset2);
  ASSERT_TRUE( iset1.num_intervals() == 0);
  ASSERT_TRUE( iset1.size() == 0);

}

TYPED_TEST(IntervalSetTest, union_of) {
  typedef typename TestFixture::ISet ISet;
  ISet iset1, iset2, iset3;

  iset1.union_of( iset2, iset3 );
  ASSERT_TRUE( iset1.num_intervals() == 0);
  ASSERT_TRUE( iset1.size() == 0);

  iset2.insert( 0, 1 );
  iset2.insert( 5, 10 );
  iset2.insert( 30, 10 );

  iset3.insert( 0, 2 );
  iset3.insert( 15, 1 );
  iset3.insert( 20, 5 );
  iset3.insert( 29, 3 );
  iset3.insert( 39, 3 );

  iset1.union_of( iset2, iset3 );
  ASSERT_TRUE( iset1.num_intervals() == 4);
  ASSERT_EQ( iset1.size(), 31);
  ASSERT_TRUE( iset1.contains( 0, 2 ));
  ASSERT_FALSE( iset1.contains( 0, 3 ));

  ASSERT_TRUE( iset1.contains( 5, 11 ));
  ASSERT_FALSE( iset1.contains( 4, 1 ));
  ASSERT_FALSE( iset1.contains( 16, 1 ));
  
  ASSERT_TRUE( iset1.contains( 20, 5 ));

  ASSERT_TRUE( iset1.contains( 30, 10 ));
  ASSERT_TRUE( iset1.contains( 29, 13 ));
  ASSERT_FALSE( iset1.contains( 29, 14 ));
  ASSERT_FALSE( iset1.contains( 42, 1 ));

  iset2.clear();
  iset1.union_of(iset2);
  ASSERT_TRUE( iset1.num_intervals() == 4);
  ASSERT_EQ( iset1.size(), 31);

  iset3.clear();
  iset3.insert( 29, 3 );
  iset3.insert( 39, 2 );
  iset1.union_of(iset3);

  ASSERT_TRUE( iset1.num_intervals() == 4);
  ASSERT_EQ( iset1.size(), 31); //actually we added nothing
  ASSERT_TRUE( iset1.contains( 29, 13 ));
  ASSERT_FALSE( iset1.contains( 29, 14 ));
  ASSERT_FALSE( iset1.contains( 42, 1 ));

}

TYPED_TEST(IntervalSetTest, subset_of) {
  typedef typename TestFixture::ISet ISet;
  ISet iset1, iset2;

  ASSERT_TRUE(iset1.subset_of(iset2));

  iset1.insert(5,10);
  ASSERT_FALSE(iset1.subset_of(iset2));

  iset2.insert(6,8);
  ASSERT_FALSE(iset1.subset_of(iset2));

  iset2.insert(5,1);
  ASSERT_FALSE(iset1.subset_of(iset2));

  iset2.insert(14,10);
  ASSERT_TRUE(iset1.subset_of(iset2));

  iset1.insert( 20, 4);
  ASSERT_TRUE(iset1.subset_of(iset2));

  iset1.insert( 24, 1);
  ASSERT_FALSE(iset1.subset_of(iset2));
}

TYPED_TEST(IntervalSetTest, span_of) {
  typedef typename TestFixture::ISet ISet;
  ISet iset1, iset2;

  iset2.insert(5,5);
  iset2.insert(20,5);

  iset1.span_of( iset2, 8, 5 );
  ASSERT_EQ( iset1.num_intervals(), 2);
  ASSERT_EQ( iset1.size(), 5);
  ASSERT_TRUE( iset1.contains( 8, 2 ));
  ASSERT_TRUE( iset1.contains( 20, 3 ));
  
  iset1.span_of( iset2, 3, 5 );
  ASSERT_EQ( iset1.num_intervals(), 1);
  ASSERT_EQ( iset1.size(), 5);
  ASSERT_TRUE( iset1.contains( 5, 5 ));

  iset1.span_of( iset2, 10, 7 );
  ASSERT_EQ( iset1.num_intervals(), 1);
  ASSERT_EQ( iset1.size(), 5);
  ASSERT_TRUE( iset1.contains( 20, 5 ));
  ASSERT_FALSE( iset1.contains( 20, 6 ));

  iset1.span_of( iset2, 5, 10);
  ASSERT_EQ( iset1.num_intervals(), 2);
  ASSERT_EQ( iset1.size(), 10);
  ASSERT_TRUE( iset1.contains( 5, 5 ));
  ASSERT_TRUE( iset1.contains( 20, 5 ));

  iset1.span_of( iset2, 100, 5 );
  ASSERT_EQ( iset1.num_intervals(), 0);
  ASSERT_EQ( iset1.size(), 0);
}