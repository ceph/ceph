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
#include <boost/container/flat_map.hpp>

#include "include/interval_set.h"
#include "include/btree_map.h"

using namespace ceph;

/* This define implements the following logic:
 * if (interval set has strict=true) expect that ceph will panic.
 * else expect that ceph will not panic and execute the second clause.
 */
#define ASSERT_STRICT_DEATH(s, e) if constexpr (ISet::test_strict) ASSERT_DEATH(s, ""); else { s; e; }

typedef uint64_t IntervalValueType;

template<typename T>  // tuple<type to test on, test array size>
class IntervalSetTest : public ::testing::Test {
 public:
  typedef T ISet;
};

typedef ::testing::Types<
  interval_set<IntervalValueType>,
  interval_set<IntervalValueType, btree::btree_map>,
  interval_set<IntervalValueType, boost::container::flat_map>,
  interval_set<IntervalValueType, std::map, false>,
  interval_set<IntervalValueType, btree::btree_map, false>,
  interval_set<IntervalValueType, boost::container::flat_map, false>
  > IntervalSetTypes;

TYPED_TEST_SUITE(IntervalSetTest, IntervalSetTypes);

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
  if constexpr (ISet::test_strict) {
    ISet iset1, iset2;
    IntervalValueType start, len;

    iset1.insert(3, 5, &start, &len);
    ASSERT_EQ(3, start);
    ASSERT_EQ(5, len);
    ASSERT_EQ(1, iset1.num_intervals());
    ASSERT_EQ(5, iset1.size());

    //adding standalone interval
    iset1.insert(15, 10, &start, &len);
    ASSERT_EQ(15, start);
    ASSERT_EQ(10, len);
    ASSERT_EQ(2, iset1.num_intervals());
    ASSERT_EQ(15, iset1.size());

    //adding leftmost standalone interval
    iset1.insert(1, 1, &start, &len);
    ASSERT_EQ(1, start);
    ASSERT_EQ(1, len);
    ASSERT_EQ(3, iset1.num_intervals());
    ASSERT_EQ(16, iset1.size());

    //adding leftmost adjucent interval
    iset1.insert(0, 1, &start, &len);
    ASSERT_EQ(0, start);
    ASSERT_EQ(2, len);
    ASSERT_EQ(3, iset1.num_intervals());
    ASSERT_EQ(17, iset1.size());

    //adding interim interval that merges leftmost and subseqent intervals
    iset1.insert(2, 1, &start, &len);
    ASSERT_EQ(0, start);
    ASSERT_EQ(8, len);
    ASSERT_EQ(2, iset1.num_intervals());
    ASSERT_EQ(18, iset1.size());

    //adding rigtmost standalone interval
    iset1.insert(30, 5, &start, &len);
    ASSERT_EQ(30, start);
    ASSERT_EQ(5, len);
    ASSERT_EQ(3, iset1.num_intervals());
    ASSERT_EQ(23, iset1.size());

    //adding rigtmost adjusent interval
    iset1.insert(35, 10, &start, &len);
    ASSERT_EQ(30, start);
    ASSERT_EQ(15, len );
    ASSERT_EQ(3, iset1.num_intervals());
    ASSERT_EQ(33, iset1.size());

    //adding interim interval that merges with the interval preceeding the rightmost
    iset1.insert(25, 1, &start, &len);
    ASSERT_EQ(15, start);
    ASSERT_EQ(11, len);
    ASSERT_EQ(3, iset1.num_intervals());
    ASSERT_EQ(34, iset1.size());

    //adding interim interval that merges with the rightmost and preceeding intervals
    iset1.insert(26, 4, &start, &len);
    ASSERT_EQ(15, start);
    ASSERT_EQ(30, len);
    ASSERT_EQ(2, iset1.num_intervals());
    ASSERT_EQ(38, iset1.size());

    //and finally build single interval filling the gap at  8-15 using different interval set
    iset2.insert( 8, 1 );
    iset2.insert( 14, 1 );
    iset2.insert( 9, 4 );
    iset1.insert( iset2 );
    iset1.insert(13, 1, &start, &len);
    ASSERT_EQ(0, start);
    ASSERT_EQ(45, len);
    ASSERT_EQ(1, iset1.num_intervals());
    ASSERT_EQ(45, iset1.size());

    //now reverses the process using subtract & erase
    iset1.subtract( iset2 );
    iset1.erase(13, 1);
    ASSERT_EQ( 2, iset1.num_intervals() );
    ASSERT_EQ(38, iset1.size());
    ASSERT_TRUE( iset1.contains( 7, 1 ));
    ASSERT_FALSE( iset1.contains( 8, 7 ));
    ASSERT_TRUE( iset1.contains( 15, 1 ));
    ASSERT_TRUE( iset1.contains( 26, 4 ));

    iset1.erase(26, 4);
    ASSERT_EQ(3, iset1.num_intervals());
    ASSERT_EQ(34, iset1.size());
    ASSERT_TRUE( iset1.contains( 7, 1 ));
    ASSERT_FALSE( iset1.intersects( 8, 7 ));
    ASSERT_TRUE( iset1.contains( 15, 1 ));
    ASSERT_TRUE( iset1.contains( 25, 1 ));
    ASSERT_FALSE( iset1.contains( 26, 4 ));
    ASSERT_TRUE( iset1.contains( 30, 1 ));

    iset1.erase(25, 1);
    ASSERT_EQ(3, iset1.num_intervals());
    ASSERT_EQ(33, iset1.size());
    ASSERT_TRUE( iset1.contains( 24, 1 ));
    ASSERT_FALSE( iset1.contains( 25, 1 ));
    ASSERT_FALSE( iset1.intersects( 26, 4 ));
    ASSERT_TRUE( iset1.contains( 30, 1 ));
    ASSERT_TRUE( iset1.contains( 35, 10 ));

    iset1.erase(35, 10);
    ASSERT_EQ(3, iset1.num_intervals());
    ASSERT_EQ(23, iset1.size());
    ASSERT_TRUE( iset1.contains( 30, 5 ));
    ASSERT_TRUE( iset1.contains( 34, 1 ));
    ASSERT_FALSE( iset1.contains( 35, 10 ));
    ASSERT_FALSE(iset1.contains( 45, 1 ));

    iset1.erase(30, 5);
    ASSERT_EQ(2, iset1.num_intervals());
    ASSERT_EQ(18, iset1.size());
    ASSERT_TRUE( iset1.contains( 2, 1 ));
    ASSERT_TRUE( iset1.contains( 24, 1 ));
    ASSERT_FALSE( iset1.contains( 25, 1 ));
    ASSERT_FALSE( iset1.contains( 29, 1 ));
    ASSERT_FALSE( iset1.contains( 30, 5 ));
    ASSERT_FALSE( iset1.contains( 35, 1 ));

    iset1.erase(2, 1);
    ASSERT_EQ(3, iset1.num_intervals());
    ASSERT_EQ( iset1.size(), 17 );
    ASSERT_TRUE( iset1.contains( 0, 1 ));
    ASSERT_TRUE( iset1.contains( 1, 1 ));
    ASSERT_FALSE( iset1.contains( 2, 1 ));
    ASSERT_TRUE( iset1.contains( 3, 1 ));
    ASSERT_TRUE( iset1.contains( 15, 1 ));
    ASSERT_FALSE( iset1.contains( 25, 1 ));

    iset1.erase( 0, 1);
    ASSERT_EQ(3, iset1.num_intervals());
    ASSERT_EQ(16, iset1.size());
    ASSERT_FALSE( iset1.contains( 0, 1 ));
    ASSERT_TRUE( iset1.contains( 1, 1 ));
    ASSERT_FALSE( iset1.contains( 2, 1 ));
    ASSERT_TRUE( iset1.contains( 3, 1 ));
    ASSERT_TRUE( iset1.contains( 15, 1 ));

    iset1.erase(1, 1);
    ASSERT_EQ(2, iset1.num_intervals());
    ASSERT_EQ(15, iset1.size());
    ASSERT_FALSE( iset1.contains( 1, 1 ));
    ASSERT_TRUE( iset1.contains( 15, 10 ));
    ASSERT_TRUE( iset1.contains( 3, 5 ));

    iset1.erase(15, 10);
    ASSERT_EQ(1, iset1.num_intervals());
    ASSERT_EQ(5, iset1.size());
    ASSERT_FALSE( iset1.contains( 1, 1 ));
    ASSERT_FALSE( iset1.contains( 15, 10 ));
    ASSERT_FALSE( iset1.contains( 25, 1 ));
    ASSERT_TRUE( iset1.contains( 3, 5 ));

    iset1.erase( 3, 1);
    ASSERT_EQ(1, iset1.num_intervals());
    ASSERT_EQ(4, iset1.size());
    ASSERT_FALSE( iset1.contains( 1, 1 ));
    ASSERT_FALSE( iset1.contains( 15, 10 ));
    ASSERT_FALSE( iset1.contains( 25, 1 ));
    ASSERT_TRUE( iset1.contains( 4, 4 ));
    ASSERT_FALSE( iset1.contains( 3, 5 ));

    iset1.erase( 4, 4);
    ASSERT_EQ(0, iset1.num_intervals());
    ASSERT_EQ(0, iset1.size());
    ASSERT_FALSE( iset1.contains( 1, 1 ));
    ASSERT_FALSE( iset1.contains( 15, 10 ));
    ASSERT_FALSE( iset1.contains( 25, 1 ));
    ASSERT_FALSE( iset1.contains( 3, 4 ));
    ASSERT_FALSE( iset1.contains( 3, 5 ));
    ASSERT_FALSE( iset1.contains( 4, 4 ));
  }
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

  iset2.insert( 24, 1);
  ASSERT_TRUE(iset1.subset_of(iset2));

  iset1.insert( 30, 5);
  ASSERT_FALSE(iset1.subset_of(iset2));

  iset2.insert( 30, 5);
  ASSERT_TRUE(iset1.subset_of(iset2));

  iset2.erase( 30, 1);
  ASSERT_FALSE(iset1.subset_of(iset2));

  iset1.erase( 30, 1);
  ASSERT_TRUE(iset1.subset_of(iset2));

  iset2.erase( 34, 1);
  ASSERT_FALSE(iset1.subset_of(iset2));

  iset1.erase( 34, 1);
  ASSERT_TRUE(iset1.subset_of(iset2));

  iset1.insert( 40, 5);
  ASSERT_FALSE(iset1.subset_of(iset2));

  iset2.insert( 39, 7);
  ASSERT_TRUE(iset1.subset_of(iset2));

  iset1.insert( 50, 5);
  iset2.insert( 55, 2);
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

TYPED_TEST(IntervalSetTest, compare_union) {
  typedef typename TestFixture::ISet ISet;
  ISet iset1, iset2;
  ASSERT_TRUE(iset1 == iset1);
  ASSERT_TRUE(iset1 == iset2);

  iset1.insert(1);
  ASSERT_FALSE(iset1 == iset2);

  iset2.insert(1);
  ASSERT_TRUE(iset1 == iset2);

  iset1.union_insert(2, 3);
  iset2.union_insert(2, 4);
  ASSERT_FALSE(iset1 == iset2);

  iset2.erase(2, 4);
  iset2.erase(1);
  iset2.union_insert(2, 3);
  iset2.insert(1);
  ASSERT_TRUE(iset1 == iset2);

  iset1.union_insert(100, 10);
  iset2.union_insert(100, 5);
  ASSERT_FALSE(iset1 == iset2);
  iset2.union_insert(105, 5);
  ASSERT_TRUE(iset1 == iset2);

  iset1.union_insert(200, 10);
  iset2.union_insert(205, 5);
  ASSERT_FALSE(iset1 == iset2);
  iset2.union_insert(200, 1);
  iset2.union_insert(202, 3);
  ASSERT_FALSE(iset1 == iset2);
  iset2.union_insert(201, 1);
  ASSERT_TRUE(iset1 == iset2);

  iset1.clear();
  ASSERT_FALSE(iset1 == iset2);
  iset2.clear();
  ASSERT_TRUE(iset1 == iset2);
}

TYPED_TEST(IntervalSetTest, contains_union) {
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

  iset1.union_insert(2, 3);
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

  iset1.union_insert(10, 10);
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

TYPED_TEST(IntervalSetTest, intersects_union) {
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

  iset1.union_insert(2, 3);
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

  iset1.union_insert(10, 10);
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

TYPED_TEST(IntervalSetTest, insert_erase_union) {
  typedef typename TestFixture::ISet ISet;
  ISet iset1, iset2;

  iset1.union_insert(3, 5);
  ASSERT_EQ(1, iset1.num_intervals());
  ASSERT_EQ(5, iset1.size());

  //adding standalone interval
  iset1.union_insert(15, 10);
  ASSERT_EQ(2, iset1.num_intervals());
  ASSERT_EQ(15, iset1.size());

  //adding leftmost standalone interval
  iset1.union_insert(1, 1);
  ASSERT_EQ(3, iset1.num_intervals());
  ASSERT_EQ(16, iset1.size());

  //adding leftmost adjucent interval
  iset1.union_insert(0, 1);
  ASSERT_EQ(3, iset1.num_intervals());
  ASSERT_EQ(17, iset1.size());

  //adding interim interval that merges leftmost and subseqent intervals
  iset1.union_insert(2, 1);
  ASSERT_EQ(2, iset1.num_intervals());
  ASSERT_EQ(18, iset1.size());

  //adding rigtmost standalone interval 
  iset1.union_insert(30, 5);
  ASSERT_EQ(3, iset1.num_intervals());
  ASSERT_EQ(23, iset1.size());

  //adding rigtmost adjusent interval 
  iset1.union_insert(35, 10);
  ASSERT_EQ(3, iset1.num_intervals());
  ASSERT_EQ(33, iset1.size());

  //adding interim interval that merges with the interval preceeding the rightmost
  iset1.union_insert(25, 1);
  ASSERT_EQ(3, iset1.num_intervals());
  ASSERT_EQ(34, iset1.size());

  //adding interim interval that merges with the rightmost and preceeding intervals
  iset1.union_insert(26, 4);
  ASSERT_EQ(2, iset1.num_intervals());
  ASSERT_EQ(38, iset1.size());

  //and finally build single interval filling the gap at  8-15 using different interval set
  iset2.union_insert( 8, 1 );
  iset2.union_insert( 14, 1 );
  iset2.union_insert( 9, 4 );
  iset1.union_of( iset2 );
  iset1.union_insert(13, 1);
  ASSERT_EQ(1, iset1.num_intervals());
  ASSERT_EQ(45, iset1.size());

  //now reverses the process using subtract & erase
  iset1.subtract( iset2 );
  iset1.erase(13, 1);
  ASSERT_EQ( 2, iset1.num_intervals() );
  ASSERT_EQ(38, iset1.size());
  ASSERT_TRUE( iset1.contains( 7, 1 ));
  ASSERT_FALSE( iset1.contains( 8, 7 ));
  ASSERT_TRUE( iset1.contains( 15, 1 ));
  ASSERT_TRUE( iset1.contains( 26, 4 ));

  iset1.erase(26, 4);
  ASSERT_EQ(3, iset1.num_intervals());
  ASSERT_EQ(34, iset1.size());
  ASSERT_TRUE( iset1.contains( 7, 1 ));
  ASSERT_FALSE( iset1.intersects( 8, 7 ));
  ASSERT_TRUE( iset1.contains( 15, 1 ));
  ASSERT_TRUE( iset1.contains( 25, 1 ));
  ASSERT_FALSE( iset1.contains( 26, 4 ));
  ASSERT_TRUE( iset1.contains( 30, 1 ));

  iset1.erase(25, 1);
  ASSERT_EQ(3, iset1.num_intervals());
  ASSERT_EQ(33, iset1.size());
  ASSERT_TRUE( iset1.contains( 24, 1 ));
  ASSERT_FALSE( iset1.contains( 25, 1 ));
  ASSERT_FALSE( iset1.intersects( 26, 4 ));
  ASSERT_TRUE( iset1.contains( 30, 1 ));
  ASSERT_TRUE( iset1.contains( 35, 10 ));

  iset1.erase(35, 10);
  ASSERT_EQ(3, iset1.num_intervals());
  ASSERT_EQ(23, iset1.size());
  ASSERT_TRUE( iset1.contains( 30, 5 ));
  ASSERT_TRUE( iset1.contains( 34, 1 ));
  ASSERT_FALSE( iset1.contains( 35, 10 ));
  ASSERT_FALSE(iset1.contains( 45, 1 ));

  iset1.erase(30, 5);
  ASSERT_EQ(2, iset1.num_intervals());
  ASSERT_EQ(18, iset1.size());
  ASSERT_TRUE( iset1.contains( 2, 1 ));
  ASSERT_TRUE( iset1.contains( 24, 1 ));
  ASSERT_FALSE( iset1.contains( 25, 1 ));
  ASSERT_FALSE( iset1.contains( 29, 1 ));
  ASSERT_FALSE( iset1.contains( 30, 5 ));
  ASSERT_FALSE( iset1.contains( 35, 1 ));

  iset1.erase(2, 1);
  ASSERT_EQ(3, iset1.num_intervals());
  ASSERT_EQ( iset1.size(), 17 );
  ASSERT_TRUE( iset1.contains( 0, 1 ));
  ASSERT_TRUE( iset1.contains( 1, 1 ));
  ASSERT_FALSE( iset1.contains( 2, 1 ));
  ASSERT_TRUE( iset1.contains( 3, 1 ));
  ASSERT_TRUE( iset1.contains( 15, 1 ));
  ASSERT_FALSE( iset1.contains( 25, 1 ));

  iset1.erase( 0, 1);
  ASSERT_EQ(3, iset1.num_intervals());
  ASSERT_EQ(16, iset1.size());
  ASSERT_FALSE( iset1.contains( 0, 1 ));
  ASSERT_TRUE( iset1.contains( 1, 1 ));
  ASSERT_FALSE( iset1.contains( 2, 1 ));
  ASSERT_TRUE( iset1.contains( 3, 1 ));
  ASSERT_TRUE( iset1.contains( 15, 1 ));

  iset1.erase(1, 1);
  ASSERT_EQ(2, iset1.num_intervals());
  ASSERT_EQ(15, iset1.size());
  ASSERT_FALSE( iset1.contains( 1, 1 ));
  ASSERT_TRUE( iset1.contains( 15, 10 ));
  ASSERT_TRUE( iset1.contains( 3, 5 ));

  iset1.erase(15, 10);
  ASSERT_EQ(1, iset1.num_intervals());
  ASSERT_EQ(5, iset1.size());
  ASSERT_FALSE( iset1.contains( 1, 1 ));
  ASSERT_FALSE( iset1.contains( 15, 10 ));
  ASSERT_FALSE( iset1.contains( 25, 1 ));
  ASSERT_TRUE( iset1.contains( 3, 5 ));

  iset1.erase( 3, 1);
  ASSERT_EQ(1, iset1.num_intervals());
  ASSERT_EQ(4, iset1.size());
  ASSERT_FALSE( iset1.contains( 1, 1 ));
  ASSERT_FALSE( iset1.contains( 15, 10 ));
  ASSERT_FALSE( iset1.contains( 25, 1 ));
  ASSERT_TRUE( iset1.contains( 4, 4 ));
  ASSERT_FALSE( iset1.contains( 3, 5 ));

  iset1.erase( 4, 4);
  ASSERT_EQ(0, iset1.num_intervals());
  ASSERT_EQ(0, iset1.size());
  ASSERT_FALSE( iset1.contains( 1, 1 ));
  ASSERT_FALSE( iset1.contains( 15, 10 ));
  ASSERT_FALSE( iset1.contains( 25, 1 ));
  ASSERT_FALSE( iset1.contains( 3, 4 ));
  ASSERT_FALSE( iset1.contains( 3, 5 ));
  ASSERT_FALSE( iset1.contains( 4, 4 ));


}

TYPED_TEST(IntervalSetTest, intersect_of_union) {
  typedef typename TestFixture::ISet ISet;
  ISet iset1, iset2, iset3;

  iset1.intersection_of( iset2, iset3 );
  ASSERT_TRUE( iset1.num_intervals() == 0);
  ASSERT_TRUE( iset1.size() == 0);

  iset2.union_insert( 0, 1 );
  iset2.union_insert( 5, 10 );
  iset2.union_insert( 30, 10 );

  iset3.union_insert( 0, 2 );
  iset3.union_insert( 15, 1 );
  iset3.union_insert( 20, 5 );
  iset3.union_insert( 29, 3 );
  iset3.union_insert( 35, 3 );
  iset3.union_insert( 39, 3 );

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
  iset2.union_insert(0,1);
  iset1.intersection_of(iset2);
  ASSERT_TRUE( iset1.num_intervals() == 1);
  ASSERT_TRUE( iset1.size() == 1);

  iset1 = iset3;
  iset2.clear();
  iset1.intersection_of(iset2);
  ASSERT_TRUE( iset1.num_intervals() == 0);
  ASSERT_TRUE( iset1.size() == 0);

}

TYPED_TEST(IntervalSetTest, union_of_union) {
  typedef typename TestFixture::ISet ISet;
  ISet iset1, iset2, iset3;

  iset1.union_of( iset2, iset3 );
  ASSERT_TRUE( iset1.num_intervals() == 0);
  ASSERT_TRUE( iset1.size() == 0);

  iset2.union_insert( 0, 1 );
  iset2.union_insert( 5, 10 );
  iset2.union_insert( 30, 10 );

  iset3.union_insert( 0, 2 );
  iset3.union_insert( 15, 1 );
  iset3.union_insert( 20, 5 );
  iset3.union_insert( 29, 3 );
  iset3.union_insert( 39, 3 );

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
  iset3.union_insert( 29, 3 );
  iset3.union_insert( 39, 2 );
  iset1.union_of(iset3);

  ASSERT_TRUE( iset1.num_intervals() == 4);
  ASSERT_EQ( iset1.size(), 31); //actually we added nothing
  ASSERT_TRUE( iset1.contains( 29, 13 ));
  ASSERT_FALSE( iset1.contains( 29, 14 ));
  ASSERT_FALSE( iset1.contains( 42, 1 ));

}

TYPED_TEST(IntervalSetTest, subset_of_union) {
  typedef typename TestFixture::ISet ISet;
  ISet iset1, iset2;

  ASSERT_TRUE(iset1.subset_of(iset2));

  iset1.union_insert(5,10);
  ASSERT_FALSE(iset1.subset_of(iset2));

  iset2.union_insert(6,8);
  ASSERT_FALSE(iset1.subset_of(iset2));

  iset2.union_insert(5,1);
  ASSERT_FALSE(iset1.subset_of(iset2));

  iset2.union_insert(14,10);
  ASSERT_TRUE(iset1.subset_of(iset2));

  iset1.union_insert( 20, 4);
  ASSERT_TRUE(iset1.subset_of(iset2));

  iset1.union_insert( 24, 1);
  ASSERT_FALSE(iset1.subset_of(iset2));

  iset2.union_insert( 24, 1);
  ASSERT_TRUE(iset1.subset_of(iset2));

  iset1.union_insert( 30, 5);
  ASSERT_FALSE(iset1.subset_of(iset2));

  iset2.union_insert( 30, 5);
  ASSERT_TRUE(iset1.subset_of(iset2));

  iset2.erase( 30, 1);
  ASSERT_FALSE(iset1.subset_of(iset2));

  iset1.erase( 30, 1);
  ASSERT_TRUE(iset1.subset_of(iset2));

  iset2.erase( 34, 1);
  ASSERT_FALSE(iset1.subset_of(iset2));

  iset1.erase( 34, 1);
  ASSERT_TRUE(iset1.subset_of(iset2));

  iset1.union_insert( 40, 5);
  ASSERT_FALSE(iset1.subset_of(iset2));

  iset2.union_insert( 39, 7);
  ASSERT_TRUE(iset1.subset_of(iset2));

  iset1.union_insert( 50, 5);
  iset2.union_insert( 55, 2);
  ASSERT_FALSE(iset1.subset_of(iset2));
}

TYPED_TEST(IntervalSetTest, span_of_union) {
  typedef typename TestFixture::ISet ISet;
  ISet iset1, iset2;

  iset2.union_insert(5,5);
  iset2.union_insert(20,5);

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


TYPED_TEST(IntervalSetTest, align) {
  typedef typename TestFixture::ISet ISet;
  {
    ISet iset1, iset2;

    iset1.union_insert(1, 2);
    iset1.align(8);
    iset2.union_insert(0,8);
    ASSERT_TRUE(iset1 == iset2);
  }

  {
    ISet iset1, iset2;

    iset1.union_insert(1, 2);
    iset1.union_insert(4, 2);

    iset1.align(8);
    iset2.union_insert(0,8);
    ASSERT_TRUE(iset1 == iset2);
  }

  {
    ISet iset1, iset2;

    iset1.union_insert(7, 2);
    iset1.union_insert(15, 2);

    iset1.align(8);
    iset2.union_insert(0,24);
    ASSERT_TRUE(iset1 == iset2);
  }

  {
    ISet iset1, iset2;

    iset1.union_insert(8, 2);
    iset1.union_insert(12, 2);

    iset1.align(8);
    iset2.union_insert(8,8);
    ASSERT_TRUE(iset1 == iset2);
  }

  {
    ISet iset1, iset2;

    iset1.union_insert(8, 2);
    iset1.union_insert(12, 2);

    iset1.align(8);
    iset2.union_insert(8,8);
    ASSERT_TRUE(iset1 == iset2);
  }

  {
    ISet iset1, iset2;

    iset1.union_insert(0, 1);
    iset1.union_insert(23, 1);
    iset1.union_insert(40, 1);

    iset1.align(8);
    iset2.union_insert(0, 8);
    iset2.union_insert(16, 8);
    iset2.union_insert(40, 8);

    ASSERT_TRUE(iset1 == iset2);
  }

  {
    ISet iset1, iset2;

    iset1.union_insert(4, 2);
    iset1.union_insert(12, 2);

    iset1.align(8);
    iset2.union_insert(0,16);
    ASSERT_TRUE(iset1 == iset2);
  }

  // Example given in review comment.
  // https://github.com/ceph/ceph/pull/59729#discussion_r1755591959
  {
    ISet iset1, iset2;

    iset1.union_insert(4, 1);
    iset1.union_insert(8, 10);

    iset1.align(10);
    iset2.union_insert(0,20);
    ASSERT_TRUE(iset1 == iset2);
  }
}

TYPED_TEST(IntervalSetTest, not_strict_insert) {
  // Tests targetted at refactor allowing over-lapping inserts.
  typedef typename TestFixture::ISet ISet;

  // Exact overlap
  {
    ISet iset1, iset2;

    iset1.insert(1, 4);
    iset2.insert(1, 4);

    ASSERT_STRICT_DEATH(iset1.insert(1, 4), ASSERT_TRUE(iset1 == iset2));
  }

  // Adjacent before
  {
    ISet iset1, iset2;

    iset1.insert(3, 4);
    iset2.insert(1, 6);
    iset1.insert(1, 2);
    ASSERT_TRUE(iset1 == iset2);
  }

  // Overlap before - single unit.
  {
    ISet iset1, iset2;

    iset1.insert(3, 4);
    iset2.insert(2, 5);
    ASSERT_STRICT_DEATH(iset1.insert(2, 2), ASSERT_TRUE(iset1 == iset2));

  }

  // Overlap before - two units.
  {
    ISet iset1, iset2;

    iset1.insert(3, 4);
    iset2.insert(2, 5);
    ASSERT_STRICT_DEATH(iset1.insert(2, 3), ASSERT_TRUE(iset1 == iset2));
  }

  // Adjacent after
  {
    ISet iset1, iset2;

    iset1.insert(3, 4);
    iset2.insert(3, 6);
    iset1.insert(7, 2);
    ASSERT_TRUE(iset1 == iset2);
  }

  // Overlap after - single unit.
  {
    ISet iset1, iset2;

    iset1.insert(3, 4);
    iset2.insert(3, 5);
    ASSERT_STRICT_DEATH(iset1.insert(6, 2), ASSERT_TRUE(iset1 == iset2));
  }

  // Overlap after - two units.
  {
    ISet iset1, iset2;

    iset1.insert(3, 4);
    iset2.insert(3, 5);

    ASSERT_STRICT_DEATH(iset1.insert(5, 3), ASSERT_TRUE(iset1 == iset2));
  }

  // insert entirely contains existing.
  {
    ISet iset1, iset2;

    iset1.insert(3, 4);
    iset2.insert(2, 6);

    ASSERT_STRICT_DEATH(iset1.insert(2, 6), ASSERT_TRUE(iset1 == iset2));
  }

  // insert entirely contained within existing
  {
    ISet iset1, iset2;

    iset1.insert(3, 4);
    iset2.insert(3, 4);

    ASSERT_STRICT_DEATH(iset1.insert(4, 2), ASSERT_TRUE(iset1 == iset2));
  }

  // insert joins exactly
  {
    ISet iset1, iset2;

    iset1.insert(2, 4);
    iset1.insert(10, 4);
    iset2.insert(2, 12);

    iset1.insert(6, 4);
    ASSERT_TRUE(iset1 == iset2);
  }

  // insert join - overlaps before
  {
    ISet iset1, iset2;

    iset1.insert(2, 4);
    iset1.insert(10, 4);
    iset2.insert(2, 12);

    ASSERT_STRICT_DEATH(iset1.insert(5, 5), ASSERT_TRUE(iset1 == iset2));
  }

  // insert join - overlaps after
  {
    ISet iset1, iset2;

    iset1.insert(2, 4);
    iset1.insert(10, 4);
    iset2.insert(2, 12);

    ASSERT_STRICT_DEATH(iset1.insert(6, 5), ASSERT_TRUE(iset1 == iset2));
  }

  // insert join - overlaps before and after
  {
    ISet iset1, iset2;

    iset1.insert(2, 4);
    iset1.insert(10, 4);
    iset2.insert(2, 12);

    ASSERT_STRICT_DEATH(iset1.insert(5, 7), ASSERT_TRUE(iset1 == iset2));
  }

  // insert join multiple - start/start.
  {
    ISet iset1, iset2;

    iset1.insert(2, 4);
    iset1.insert(8, 4);
    iset1.insert(16, 4);
    iset1.insert(24, 4);

    iset2.insert(2, 26);

    ASSERT_STRICT_DEATH(iset1.insert(2, 22), ASSERT_TRUE(iset1 == iset2));
  }

  // insert join multiple - start/middle.
  {
    ISet iset1, iset2;

    iset1.insert(2, 4);
    iset1.insert(8, 4);
    iset1.insert(16, 4);
    iset1.insert(24, 4);

    iset2.insert(2, 26);

    ASSERT_STRICT_DEATH(iset1.insert(2, 23), ASSERT_TRUE(iset1 == iset2));
  }
  // insert join multiple - start/end.
  {
    ISet iset1, iset2;

    iset1.insert(2, 4);
    iset1.insert(8, 4);
    iset1.insert(16, 4);
    iset1.insert(24, 4);

    iset2.insert(2, 26);

    ASSERT_STRICT_DEATH(iset1.insert(2, 26), ASSERT_TRUE(iset1 == iset2));
  }

  // insert join multiple - middle/start.
  {
    ISet iset1, iset2;

    iset1.insert(2, 4);
    iset1.insert(8, 4);
    iset1.insert(16, 4);
    iset1.insert(24, 4);

    iset2.insert(2, 26);

    ASSERT_STRICT_DEATH(iset1.insert(3, 21), ASSERT_TRUE(iset1 == iset2));
  }

  // insert join multiple - middle/middle.
  {
    ISet iset1, iset2;

    iset1.insert(2, 4);
    iset1.insert(8, 4);
    iset1.insert(16, 4);
    iset1.insert(24, 4);

    iset2.insert(2, 26);

    ASSERT_STRICT_DEATH(iset1.insert(3, 22), ASSERT_TRUE(iset1 == iset2));
  }
  // insert join multiple - middle/end.
  {
    ISet iset1, iset2;

    iset1.insert(2, 4);
    iset1.insert(8, 4);
    iset1.insert(16, 4);
    iset1.insert(24, 4);

    iset2.insert(2, 26);

    ASSERT_STRICT_DEATH(iset1.insert(3, 25), ASSERT_TRUE(iset1 == iset2));
  }

  // insert join multiple - end/start.
  {
    ISet iset1, iset2;

    iset1.insert(2, 4);
    iset1.insert(8, 4);
    iset1.insert(16, 4);
    iset1.insert(24, 4);

    iset2.insert(2, 26);

    ASSERT_STRICT_DEATH(iset1.insert(6, 18), ASSERT_TRUE(iset1 == iset2));
  }

  // insert join multiple - start/middle.
  {
    ISet iset1, iset2;

    iset1.insert(2, 4);
    iset1.insert(8, 4);
    iset1.insert(16, 4);
    iset1.insert(24, 4);

    iset2.insert(2, 26);

    ASSERT_STRICT_DEATH(iset1.insert(6, 19), ASSERT_TRUE(iset1 == iset2));
  }
  // insert join multiple - start/end.
  {
    ISet iset1, iset2;

    iset1.insert(2, 4);
    iset1.insert(8, 4);
    iset1.insert(16, 4);
    iset1.insert(24, 4);

    iset2.insert(2, 26);

    ASSERT_STRICT_DEATH(iset1.insert(6, 22), ASSERT_TRUE(iset1 == iset2));
  }

  // insert entirely contained within existing
  if constexpr (!ISet::test_strict)
  {
    ISet iset1, iset2;

    iset1.insert(0x3000,  0xd000);
    iset1.insert(0x11000, 0xf000);
    iset1.insert(0x20000, 0x9000);
    iset1.insert(0x9000,  0x1000);
    iset1.insert(0xa000,  0x1000);
    iset1.insert(0xb000,  0x1000);
    iset1.insert(0x18000, 0x1000);
    iset1.insert(0x19000, 0x1000);
    iset1.insert(0xc000,  0x4000);
    iset1.insert(0x10000, 0x8000);
    iset1.insert(0x10000, 0x1000);
    iset2.insert(0x2c000, 0x10000);

    iset2.intersection_of(iset1);

    ASSERT_TRUE(iset2.empty());
  }
}

TYPED_TEST(IntervalSetTest, erase) {
  typedef typename TestFixture::ISet ISet;
  // erase before miss by 1
  {
    ISet iset1, iset2;

    iset1.union_insert(4, 4);
    iset2.union_insert(4, 4);
    ASSERT_STRICT_DEATH(iset1.erase(1, 2), ASSERT_EQ(iset1, iset2));
  }

  // erase before miss by 0
  {
    ISet iset1, iset2;

    iset1.union_insert(3, 4);
    iset2.union_insert(3, 4);
    ASSERT_STRICT_DEATH(iset1.erase(1, 2), ASSERT_EQ(iset1, iset2));
  }

  // erase overlapping start by 1
  {
    ISet iset1, iset2;

    iset1.union_insert(3, 4);
    iset2.union_insert(4, 3);
    ASSERT_STRICT_DEATH(iset1.erase(1, 3), ASSERT_EQ(iset1, iset2));
  }

  // erase overlapping start by 2
  {
    ISet iset1, iset2;

    iset1.union_insert(3, 4);
    iset2.union_insert(5, 2);

    ASSERT_STRICT_DEATH(iset1.erase(1, 4), ASSERT_EQ(iset1, iset2));
  }

  // erase overlapping to end
  {
    ISet iset1, iset2;

    iset1.union_insert(3, 4);
    ASSERT_STRICT_DEATH(iset1.erase(1, 6), ASSERT_EQ(iset1, iset2));
  }

  // erase overlapping past end
  {
    ISet iset1, iset2;

    iset1.union_insert(3, 4);
    ASSERT_STRICT_DEATH(iset1.erase(1, 7), ASSERT_EQ(iset1, iset2));
  }

  // erase middle (split)
  {
    ISet iset1, iset2;

    iset1.union_insert(3, 4);
    iset2.union_insert(3, 1);
    iset2.union_insert(6, 1);
    iset1.erase(4, 2);
    ASSERT_EQ(iset1, iset2);
  }

  // erase overlap end
  {
    ISet iset1, iset2;

    iset1.union_insert(3, 4);
    iset2.union_insert(3, 1);
    iset1.erase(4, 3);
    ASSERT_EQ(iset1, iset2);
  }

  // erase overlap end + 1
  {
    ISet iset1, iset2;

    iset1.union_insert(3, 4);
    iset2.union_insert(3, 1);
    ASSERT_STRICT_DEATH(iset1.erase(4, 4), ASSERT_EQ(iset1, iset2));
  }

  // erase after
  {
    ISet iset1, iset2;

    iset1.union_insert(3, 4);
    iset2.union_insert(3, 4);
    ASSERT_STRICT_DEATH(iset1.erase(7, 1), ASSERT_EQ(iset1, iset2));
  }

  // erase after + 1
  {
    ISet iset1, iset2;

    iset1.union_insert(3, 4);
    iset2.union_insert(3, 4);
    ASSERT_STRICT_DEATH(iset1.erase(8, 1), ASSERT_EQ(iset1, iset2));
  }

  // erase between
  {
    ISet iset1, iset2;

    iset1.union_insert(3, 4);
    iset1.union_insert(10, 4);
    iset2.union_insert(3, 4);
    iset2.union_insert(10, 4);
    ASSERT_STRICT_DEATH(iset1.erase(8, 1), ASSERT_EQ(iset1, iset2));
  }

  // erase multiple
  {
    ISet iset1, iset2;

    iset1.union_insert(3, 4);
    iset1.union_insert(10, 4);
    ASSERT_STRICT_DEATH(iset1.erase(3, 11), ASSERT_EQ(iset1, iset2));
  }

  // erase many
  {
    ISet iset1, iset2;

    iset1.union_insert(3, 1);
    iset1.union_insert(5, 1);
    iset1.union_insert(7, 1);
    iset1.union_insert(9, 1);
    ASSERT_STRICT_DEATH(iset1.erase(2, 9), ASSERT_EQ(iset1, iset2));
  }

  // erase many with border
  {
    ISet iset1, iset2;

    iset1.union_insert(1, 1);
    iset1.union_insert(3, 1);
    iset1.union_insert(5, 1);
    iset1.union_insert(7, 1);
    iset1.union_insert(9, 1);
    iset2.union_insert(1, 1);
    iset2.union_insert(9, 1);
    ASSERT_STRICT_DEATH(iset1.erase(2, 6), ASSERT_EQ(iset1, iset2));
  }

  // erase many splitting begin and end
  {
    ISet iset1, iset2;

    iset1.union_insert(1, 1);
    iset1.union_insert(3, 4);
    iset1.union_insert(8, 1);
    iset1.union_insert(10, 1);
    iset1.union_insert(12, 4);
    iset2.union_insert(1, 1);
    iset2.union_insert(3, 1);
    iset2.union_insert(15, 1);
    ASSERT_STRICT_DEATH(iset1.erase(4, 11), ASSERT_EQ(iset1, iset2));
  }
}

TYPED_TEST(IntervalSetTest, erase_after)
{
  typedef typename TestFixture::ISet ISet;
  if constexpr (!ISet::test_strict) {
    // Overlap whole thing.
    {
      ISet iset1, iset2;

      iset1.union_insert(4, 4);
      iset1.erase_after(1);

      ASSERT_EQ(iset1, iset2);
    }

    // erase overlapping to end
    {
      ISet iset1, iset2;

      iset1.union_insert(4, 4);
      iset1.erase_after(4);

      ASSERT_EQ(iset1, iset2);
    }

    // erase overlap end
    {
      ISet iset1, iset2;

      iset1.union_insert(3, 4);
      iset1.erase_after(4);
      iset2.union_insert(3, 1);

      ASSERT_EQ(iset1, iset2);
    }

    // erase after
    {
      ISet iset1, iset2;

      iset1.union_insert(3, 4);
      iset1.erase_after(7);
      iset2.union_insert(3, 4);

      ASSERT_EQ(iset1, iset2);
    }

    // erase between
    {
      ISet iset1, iset2;

      iset1.union_insert(3, 4);
      iset1.union_insert(10, 4);
      iset1.erase_after(8);
      iset2.union_insert(3, 4);

      ASSERT_EQ(iset1, iset2);
    }

    // erase multiple
    {
      ISet iset1, iset2;

      iset1.union_insert(3, 4);
      iset1.union_insert(10, 4);
      iset1.erase_after(3);

      ASSERT_EQ(iset1, iset2);
    }

    // erase many
    {
      ISet iset1, iset2;

      iset1.union_insert(3, 1);
      iset1.union_insert(5, 1);
      iset1.union_insert(7, 1);
      iset1.union_insert(9, 1);
      iset1.erase_after(2);

      ASSERT_EQ(iset1, iset2);
    }

    // erase many with border
    {
      ISet iset1, iset2;

      iset1.union_insert(1, 1);
      iset1.union_insert(3, 1);
      iset1.union_insert(5, 1);
      iset1.union_insert(7, 1);
      iset1.union_insert(9, 1);
      iset1.erase_after(2);
      iset2.union_insert(1, 1);

      ASSERT_EQ(iset1, iset2);
    }
  }
}

TYPED_TEST(IntervalSetTest, subtract) {
  typedef typename TestFixture::ISet ISet;

  //Subtract from empty
  // erase many with border
  {
    ISet iset1, iset2, iset3;
    iset1.subtract(iset2);

    ASSERT_EQ(iset1, iset3);
  }

  //Subtract from empty
  // erase many with border
  {
    ISet iset1, iset2, iset3;
    iset1.union_insert(1, 1);
    iset1.subtract(iset2);

    iset3.union_insert(1, 1);
    ASSERT_EQ(iset1, iset3);
  }

  //Subtract from empty
  // erase many with border
  {
    ISet iset1, iset2, iset3;
    iset2.union_insert(1, 1);
    ASSERT_STRICT_DEATH(iset1.subtract(iset2), ASSERT_EQ(iset1, iset3));
  }

  // Subtract many span.
  {
    ISet iset1, iset2, iset3;

    iset1.union_insert(1, 1);
    iset1.union_insert(3, 1);
    iset1.union_insert(5, 1);
    iset1.union_insert(7, 1);
    iset1.union_insert(9, 1);

    iset2.union_insert(1, 4);
    iset2.union_insert(7, 3);

    iset3.union_insert(5, 1);

    ASSERT_STRICT_DEATH(iset1.subtract(iset2), ASSERT_EQ(iset1, iset3));
  }

  // Subtract with larger sizes and exact overlaps
  {
    ISet iset1, iset2, iset3;

    iset1.union_insert(0, 5);
    iset1.union_insert(10, 5);
    iset1.union_insert(20, 5);
    iset1.union_insert(30, 5);
    iset1.union_insert(40, 4);

    iset2.union_insert(20, 25);


    iset3.union_insert(0, 5);
    iset3.union_insert(10, 5);

    ASSERT_STRICT_DEATH(iset1.subtract(iset2), ASSERT_EQ(iset1, iset3));
  }
}
