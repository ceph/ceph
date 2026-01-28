// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest.h>
#include "include/hybrid_interval_set.h"
#include <boost/container/flat_map.hpp>

// Type alias for testing
using test_set = hybrid_interval_set<uint64_t, boost::container::flat_map, false>;

TEST(HybridExtentSet, EmptySet) {
  test_set s;
  ASSERT_TRUE(s.empty());
  ASSERT_EQ(0u, s.num_intervals());
  ASSERT_TRUE(s.is_single()) << "Empty set should be in single mode";
}

TEST(HybridExtentSet, SingleInsert) {
  test_set s;
  s.insert(100, 50);
  
  ASSERT_FALSE(s.empty());
  ASSERT_EQ(1u, s.num_intervals());
  ASSERT_TRUE(s.is_single()) << "Single interval should stay in single mode";
  ASSERT_TRUE(s.contains(100, 50));
  ASSERT_EQ(100u, s.range_start());
  ASSERT_EQ(150u, s.range_end());
}

TEST(HybridExtentSet, SingleInsertMergeAppend) {
  test_set s;
  s.insert(100, 50);
  s.insert(150, 50);  // Adjacent - should merge
  
  ASSERT_FALSE(s.empty());
  ASSERT_EQ(1u, s.num_intervals());
  ASSERT_TRUE(s.is_single()) << "Merged single interval should stay in single mode";
  ASSERT_TRUE(s.contains(100, 100));
  ASSERT_EQ(100u, s.range_start());
  ASSERT_EQ(200u, s.range_end());
}

TEST(HybridExtentSet, SingleInsertMergePrepend) {
  test_set s;
  s.insert(100, 50);
  s.insert(50, 50);  // Adjacent before - should merge
  
  ASSERT_FALSE(s.empty());
  ASSERT_EQ(1u, s.num_intervals());
  ASSERT_TRUE(s.is_single()) << "Merged single interval should stay in single mode";
  ASSERT_TRUE(s.contains(50, 100));
  ASSERT_EQ(50u, s.range_start());
  ASSERT_EQ(150u, s.range_end());
}

TEST(HybridExtentSet, MultipleNonAdjacent) {
  test_set s;
  s.insert(100, 50);
  s.insert(200, 50);  // Not adjacent - should upgrade to multi
  
  ASSERT_FALSE(s.empty());
  ASSERT_EQ(2u, s.num_intervals());
  ASSERT_FALSE(s.is_single()) << "Two intervals should be in multi mode";
  ASSERT_TRUE(s.contains(100, 50));
  ASSERT_TRUE(s.contains(200, 50));
  ASSERT_FALSE(s.contains(100, 150));
}

TEST(HybridExtentSet, OverlappingInsertShouldStaySingle) {
  test_set s;
  s.insert(100, 50);  // [100, 150)
  ASSERT_TRUE(s.is_single()) << "Single interval should be in single mode";
  
  // Insert overlapping interval - should merge and stay single
  s.insert(120, 40);  // [120, 160) overlaps with [100, 150)
  
  ASSERT_FALSE(s.empty());
  ASSERT_EQ(1u, s.num_intervals()) << "Overlapping intervals should merge to single";
  ASSERT_TRUE(s.is_single()) << "Merged overlapping interval should stay in single mode";
  ASSERT_TRUE(s.contains(100, 60)) << "Should contain merged range [100, 160)";
  ASSERT_EQ(100u, s.range_start());
  ASSERT_EQ(160u, s.range_end());
}

TEST(HybridExtentSet, ContainedInsertShouldStaySingle) {
  test_set s;
  s.insert(100, 100);  // [100, 200)
  ASSERT_TRUE(s.is_single()) << "Single interval should be in single mode";
  
  // Insert contained interval - should stay single
  s.insert(120, 40);  // [120, 160) is contained in [100, 200)
  
  ASSERT_FALSE(s.empty());
  ASSERT_EQ(1u, s.num_intervals()) << "Contained interval should not create new interval";
  ASSERT_TRUE(s.is_single()) << "Should stay in single mode";
  ASSERT_TRUE(s.contains(100, 100)) << "Should still contain original range";
  ASSERT_EQ(100u, s.range_start());
  ASSERT_EQ(200u, s.range_end());
}

TEST(HybridExtentSet, ContainingInsertShouldStaySingle) {
  test_set s;
  s.insert(120, 40);  // [120, 160)
  ASSERT_TRUE(s.is_single()) << "Single interval should be in single mode";
  
  // Insert containing interval - should replace and stay single
  s.insert(100, 100);  // [100, 200) contains [120, 160)
  
  ASSERT_FALSE(s.empty());
  ASSERT_EQ(1u, s.num_intervals()) << "Containing interval should merge";
  ASSERT_TRUE(s.is_single()) << "Should stay in single mode";
  ASSERT_TRUE(s.contains(100, 100)) << "Should contain larger range";
  ASSERT_EQ(100u, s.range_start());
  ASSERT_EQ(200u, s.range_end());
}

TEST(HybridExtentSet, PartialOverlapLeftShouldStaySingle) {
  test_set s;
  s.insert(100, 50);  // [100, 150)
  ASSERT_TRUE(s.is_single()) << "Single interval should be in single mode";
  
  // Insert with left overlap
  s.insert(80, 40);  // [80, 120) overlaps left side
  
  ASSERT_FALSE(s.empty());
  ASSERT_EQ(1u, s.num_intervals()) << "Overlapping intervals should merge";
  ASSERT_TRUE(s.is_single()) << "Should stay in single mode";
  ASSERT_TRUE(s.contains(80, 70)) << "Should contain merged range [80, 150)";
  ASSERT_EQ(80u, s.range_start());
  ASSERT_EQ(150u, s.range_end());
}

TEST(HybridExtentSet, PartialOverlapRightShouldStaySingle) {
  test_set s;
  s.insert(100, 50);  // [100, 150)
  ASSERT_TRUE(s.is_single()) << "Single interval should be in single mode";
  
  // Insert with right overlap
  s.insert(130, 40);  // [130, 170) overlaps right side
  
  ASSERT_FALSE(s.empty());
  ASSERT_EQ(1u, s.num_intervals()) << "Overlapping intervals should merge";
  ASSERT_TRUE(s.is_single()) << "Should stay in single mode";
  ASSERT_TRUE(s.contains(100, 70)) << "Should contain merged range [100, 170)";
  ASSERT_EQ(100u, s.range_start());
  ASSERT_EQ(170u, s.range_end());
}

TEST(HybridExtentSet, EraseComplete) {
  test_set s;
  s.insert(100, 50);
  s.erase(100, 50);
  
  ASSERT_TRUE(s.empty());
  ASSERT_EQ(0u, s.num_intervals());
  ASSERT_TRUE(s.is_single()) << "Empty set should be in single mode";
}

TEST(HybridExtentSet, EraseNoOverlap) {
  test_set s;
  s.insert(100, 50);
  s.erase(200, 50);  // No overlap
  
  ASSERT_FALSE(s.empty());
  ASSERT_EQ(1u, s.num_intervals());
  ASSERT_TRUE(s.is_single()) << "Single interval should stay in single mode";
  ASSERT_TRUE(s.contains(100, 50));
}

TEST(HybridExtentSet, Intersects) {
  test_set s;
  s.insert(100, 50);  // Interval [100, 150) - offsets 100-149 inclusive
  ASSERT_TRUE(s.is_single()) << "Single interval should be in single mode";
  
  // Full overlap
  ASSERT_TRUE(s.intersects(100, 50)) << "Exact match [100,150) should intersect";
  
  // Partial overlaps
  ASSERT_TRUE(s.intersects(120, 10)) << "Middle [120,130) should intersect";
  ASSERT_TRUE(s.intersects(90, 20)) << "Left overlap [90,110) should intersect";
  ASSERT_TRUE(s.intersects(140, 20)) << "Right overlap [140,160) should intersect";
  
  // Boundary conditions - exact edges (off-by-one tests)
  ASSERT_TRUE(s.intersects(100, 1)) << "Start at left edge [100,101) should intersect";
  ASSERT_TRUE(s.intersects(149, 1)) << "End at right edge-1 [149,150) should intersect";
  ASSERT_FALSE(s.intersects(150, 1)) << "Start at right edge [150,151) should NOT intersect";
  ASSERT_FALSE(s.intersects(99, 1)) << "End at left edge [99,100) should NOT intersect";
  ASSERT_FALSE(s.intersects(100, 0)) << "Zero length should NOT intersect";
  
  // One byte before and after
  ASSERT_TRUE(s.intersects(99, 2)) << "[99,101) overlaps at 100";
  ASSERT_TRUE(s.intersects(149, 2)) << "[149,151) overlaps at 149";
  
  // Adjacent but not overlapping
  ASSERT_FALSE(s.intersects(50, 50)) << "Adjacent left [50,100) should NOT intersect";
  ASSERT_FALSE(s.intersects(150, 50)) << "Adjacent right [150,200) should NOT intersect";
  
  // Completely outside
  ASSERT_FALSE(s.intersects(50, 40)) << "Before [50,90) should NOT intersect";
  ASSERT_FALSE(s.intersects(160, 40)) << "After [160,200) should NOT intersect";
  
  // Containing interval
  ASSERT_TRUE(s.intersects(50, 200)) << "Containing [50,250) should intersect";
}

TEST(HybridExtentSet, Iterator) {
  test_set s;
  s.insert(100, 50);
  ASSERT_TRUE(s.is_single()) << "Single interval should be in single mode";
  
  auto it = s.begin();
  ASSERT_NE(it, s.end());
  ASSERT_EQ(100u, it.get_start());
  ASSERT_EQ(50u, it.get_len());
  ASSERT_EQ(150u, it.get_end());
  
  ++it;
  ASSERT_EQ(it, s.end());
}

TEST(HybridExtentSet, IteratorMultiple) {
  test_set s;
  s.insert(100, 50);
  s.insert(200, 50);
  s.insert(300, 50);
  ASSERT_FALSE(s.is_single()) << "Three intervals should be in multi mode";
  
  std::vector<std::pair<uint64_t, uint64_t>> extents;
  for (auto it = s.begin(); it != s.end(); ++it) {
    extents.push_back({it.get_start(), it.get_len()});
  }
  
  ASSERT_EQ(3u, extents.size());
  ASSERT_EQ(100u, extents[0].first);
  ASSERT_EQ(50u, extents[0].second);
  ASSERT_EQ(200u, extents[1].first);
  ASSERT_EQ(50u, extents[1].second);
  ASSERT_EQ(300u, extents[2].first);
  ASSERT_EQ(50u, extents[2].second);
}

TEST(HybridExtentSet, CopyConstructor) {
  test_set s1;
  s1.insert(100, 50);
  ASSERT_TRUE(s1.is_single());
  
  test_set s2(s1);
  ASSERT_FALSE(s2.empty());
  ASSERT_TRUE(s2.is_single()) << "Copy of single should be single";
  ASSERT_TRUE(s2.contains(100, 50));
}

TEST(HybridExtentSet, MoveConstructor) {
  test_set s1;
  s1.insert(100, 50);
  ASSERT_TRUE(s1.is_single());
  
  test_set s2(std::move(s1));
  ASSERT_FALSE(s2.empty());
  ASSERT_TRUE(s2.is_single()) << "Move of single should be single";
  ASSERT_TRUE(s2.contains(100, 50));
}

TEST(HybridExtentSet, Clear) {
  test_set s;
  s.insert(100, 50);
  s.insert(200, 50);
  ASSERT_FALSE(s.is_single());
  
  s.clear();
  ASSERT_TRUE(s.empty());
  ASSERT_EQ(0u, s.num_intervals());
  ASSERT_FALSE(s.is_single()) << "After clear, stays in multi mode (no downgrade)";
}

TEST(HybridExtentSet, Contains) {
  test_set s;
  ASSERT_TRUE(s.is_single()) << "Empty should be single";
  ASSERT_FALSE(s.contains(1, 1));
  ASSERT_FALSE(s.contains(0, 1));
  
  s.insert(1, 1);
  ASSERT_TRUE(s.is_single()) << "Single interval should be single";
  ASSERT_TRUE(s.contains(1, 1));
  ASSERT_FALSE(s.contains(0, 1));
  ASSERT_FALSE(s.contains(2, 1));
  ASSERT_FALSE(s.contains(0, 2));
  ASSERT_FALSE(s.contains(1, 2));
  
  s.insert(2, 3);  // Adjacent, merges to [1~4]
  ASSERT_TRUE(s.is_single()) << "Merged interval should be single";
  ASSERT_TRUE(s.contains(1, 1));
  ASSERT_FALSE(s.contains(0, 1));
  ASSERT_TRUE(s.contains(2, 1));
  ASSERT_FALSE(s.contains(0, 2));
  ASSERT_TRUE(s.contains(1, 2));
  ASSERT_TRUE(s.contains(1, 3));
  ASSERT_TRUE(s.contains(1, 4));
  ASSERT_FALSE(s.contains(1, 5));
  ASSERT_TRUE(s.contains(2, 1));
  ASSERT_TRUE(s.contains(2, 2));
  ASSERT_TRUE(s.contains(2, 3));
  ASSERT_FALSE(s.contains(2, 4));
  ASSERT_TRUE(s.contains(3, 2));
  ASSERT_TRUE(s.contains(4, 1));
  ASSERT_FALSE(s.contains(4, 2));
  
  s.insert(10, 10);  // Non-adjacent, creates [1~4, 10~10]
  ASSERT_FALSE(s.is_single()) << "Two intervals should be multi";
  ASSERT_TRUE(s.contains(1, 4));
  ASSERT_FALSE(s.contains(1, 5));
  ASSERT_TRUE(s.contains(2, 2));
  ASSERT_FALSE(s.contains(2, 4));
  ASSERT_FALSE(s.contains(1, 10));
  ASSERT_FALSE(s.contains(9, 1));
  ASSERT_FALSE(s.contains(9, 11));
  ASSERT_TRUE(s.contains(10, 1));
  ASSERT_TRUE(s.contains(11, 9));
  ASSERT_TRUE(s.contains(11, 2));
  ASSERT_TRUE(s.contains(18, 2));
  ASSERT_TRUE(s.contains(19, 1));
  ASSERT_FALSE(s.contains(20, 1));
  ASSERT_FALSE(s.contains(21, 1));
  ASSERT_FALSE(s.contains(11, 11));
  ASSERT_FALSE(s.contains(18, 9));
  
  s.clear();
  // Note: stays in multi mode after clear (no downgrade)
  ASSERT_FALSE(s.contains(1, 1));
  ASSERT_FALSE(s.contains(0, 1));
  ASSERT_FALSE(s.contains(2, 1));
  ASSERT_FALSE(s.contains(10, 2));
}

TEST(HybridExtentSet, IntersectionOf) {
  test_set s1, s2, s3;
  
  s1.intersection_of(s2, s3);
  ASSERT_EQ(0u, s1.num_intervals());
  ASSERT_TRUE(s1.is_single()) << "Empty result should be single";
  ASSERT_EQ(0u, s1.size());
  
  s2.insert(0, 1);
  s2.insert(5, 10);
  s2.insert(30, 10);
  ASSERT_FALSE(s2.is_single()) << "Three intervals should be multi";
  
  s3.insert(0, 2);
  s3.insert(15, 1);
  s3.insert(20, 5);
  s3.insert(29, 3);
  s3.insert(35, 3);
  s3.insert(39, 3);
  ASSERT_FALSE(s3.is_single()) << "Six intervals should be multi";
  
  s1.intersection_of(s2, s3);
  ASSERT_EQ(4u, s1.num_intervals());
  ASSERT_FALSE(s1.is_single()) << "Four intervals should be multi";
  ASSERT_EQ(7u, s1.size());
  
  ASSERT_TRUE(s1.contains(0, 1));
  ASSERT_FALSE(s1.contains(0, 2));
  ASSERT_FALSE(s1.contains(5, 11));
  ASSERT_FALSE(s1.contains(4, 1));
  ASSERT_FALSE(s1.contains(16, 1));
  ASSERT_FALSE(s1.contains(20, 5));
  ASSERT_FALSE(s1.contains(29, 1));
  ASSERT_FALSE(s1.contains(30, 10));
  ASSERT_TRUE(s1.contains(30, 2));
  ASSERT_TRUE(s1.contains(35, 3));
  ASSERT_FALSE(s1.contains(35, 4));
  ASSERT_TRUE(s1.contains(39, 1));
  ASSERT_FALSE(s1.contains(38, 2));
  ASSERT_FALSE(s1.contains(39, 2));
  
  s3 = s1;
  s1.intersection_of(s2);
  ASSERT_TRUE(s1 == s3);
  
  // s2 was previously upgraded to multi-mode, so it stays multi even after clear
  s2.clear();
  s2.insert(0, 1);
  ASSERT_FALSE(s2.is_single()) << "s2 stays in multi-mode after previous upgrade";
  s1.intersection_of(s2);
  ASSERT_EQ(1u, s1.num_intervals());
  // Note: s1 was multi, stays multi after intersection
  ASSERT_EQ(1u, s1.size());
  
  s1 = s3;
  s2.clear();
  s1.intersection_of(s2);
  ASSERT_EQ(0u, s1.num_intervals());
  // Note: stays in multi mode
  ASSERT_EQ(0u, s1.size());
}

TEST(HybridExtentSet, UnionOf) {
  test_set s1, s2, s3;
  
  s1.union_of(s2, s3);
  ASSERT_EQ(0u, s1.num_intervals());
  ASSERT_TRUE(s1.is_single()) << "Empty result should be single";
  ASSERT_EQ(0u, s1.size());
  
  s2.insert(0, 1);
  s2.insert(5, 10);
  s2.insert(30, 10);
  ASSERT_FALSE(s2.is_single());
  
  s3.insert(0, 2);
  s3.insert(15, 1);
  s3.insert(20, 5);
  s3.insert(29, 3);
  s3.insert(39, 3);
  ASSERT_FALSE(s3.is_single());
  
  s1.union_of(s2, s3);
  ASSERT_EQ(4u, s1.num_intervals());
  ASSERT_FALSE(s1.is_single()) << "Four intervals should be multi";
  ASSERT_EQ(31u, s1.size());
  ASSERT_TRUE(s1.contains(0, 2));
  ASSERT_FALSE(s1.contains(0, 3));
  ASSERT_TRUE(s1.contains(5, 11));
  ASSERT_FALSE(s1.contains(4, 1));
  ASSERT_FALSE(s1.contains(16, 1));
  ASSERT_TRUE(s1.contains(20, 5));
  ASSERT_TRUE(s1.contains(30, 10));
  ASSERT_TRUE(s1.contains(29, 13));
  ASSERT_FALSE(s1.contains(29, 14));
  ASSERT_FALSE(s1.contains(42, 1));
  
  s2.clear();
  s1.union_of(s2);
  ASSERT_EQ(4u, s1.num_intervals());
  ASSERT_FALSE(s1.is_single());
  ASSERT_EQ(31u, s1.size());
  
  s3.clear();
  s3.insert(29, 3);
  s3.insert(39, 2);
  ASSERT_FALSE(s3.is_single());
  s1.union_of(s3);
  ASSERT_EQ(4u, s1.num_intervals());
  ASSERT_FALSE(s1.is_single());
  ASSERT_EQ(31u, s1.size());
  ASSERT_TRUE(s1.contains(29, 13));
  ASSERT_FALSE(s1.contains(29, 14));
  ASSERT_FALSE(s1.contains(42, 1));
}

TEST(HybridExtentSet, SubsetOf) {
  test_set s1, s2;
  
  ASSERT_TRUE(s1.is_single());
  ASSERT_TRUE(s2.is_single());
  ASSERT_TRUE(s1.subset_of(s2));
  
  s1.insert(5, 10);
  ASSERT_TRUE(s1.is_single());
  ASSERT_FALSE(s1.subset_of(s2));
  
  s2.insert(6, 8);
  ASSERT_TRUE(s2.is_single());
  ASSERT_FALSE(s1.subset_of(s2));
  
  s2.insert(5, 1);
  ASSERT_TRUE(s2.is_single()) << "Should merge to single interval";
  ASSERT_FALSE(s1.subset_of(s2));
  
  s2.insert(14, 10);
  ASSERT_TRUE(s2.is_single()) << "Should merge to single interval";
  ASSERT_TRUE(s1.subset_of(s2));
  
  s1.insert(20, 4);
  ASSERT_FALSE(s1.is_single()) << "Two intervals should be multi";
  ASSERT_TRUE(s1.subset_of(s2));
  
  s1.insert(25, 1);
  ASSERT_FALSE(s1.subset_of(s2));
}

TEST(HybridExtentSet, SpanOf) {
  test_set s1, s2, s3;
  
  s1.span_of(s2, s3);
  ASSERT_TRUE(s1.empty());
  ASSERT_TRUE(s1.is_single()) << "Empty result should be single";
  
  s2.insert(5, 10);
  s1.span_of(s2, s3);
  ASSERT_EQ(1u, s1.num_intervals());
  ASSERT_TRUE(s1.is_single()) << "span_of always produces single interval";
  ASSERT_EQ(10u, s1.size());
  ASSERT_TRUE(s1.contains(5, 10));
  
  s3.insert(20, 5);
  s1.span_of(s2, s3);
  ASSERT_EQ(1u, s1.num_intervals());
  ASSERT_TRUE(s1.is_single()) << "span_of always produces single interval";
  ASSERT_EQ(20u, s1.size());
  ASSERT_TRUE(s1.contains(5, 20));
  
  s3.insert(2, 2);
  s1.span_of(s2, s3);
  ASSERT_EQ(1u, s1.num_intervals());
  ASSERT_TRUE(s1.is_single()) << "span_of always produces single interval";
  ASSERT_EQ(23u, s1.size());
  ASSERT_TRUE(s1.contains(2, 23));
  
  // Test span_of with one empty set and multi-interval set
  test_set s4, s5;
  s5.insert(10, 5);
  s5.insert(20, 5);  // s5 now has 2 intervals: [10~5, 20~5]
  s4.span_of(test_set(), s5);  // span_of(empty, multi)
  ASSERT_EQ(1u, s4.num_intervals()) << "span_of should produce single interval";
  ASSERT_TRUE(s4.is_single()) << "span_of always produces single interval";
  ASSERT_EQ(15u, s4.size());
  ASSERT_TRUE(s4.contains(10, 15)) << "Should span from 10 to 25";
}

TEST(HybridExtentSet, Align) {
  test_set s;
  
  // Test from interval_set: (4,2) and (12,2) aligned to 8 becomes (0,16)
  s.insert(4, 2);
  s.insert(12, 2);
  ASSERT_FALSE(s.is_single()) << "Two intervals should be multi";
  s.align(8);
  ASSERT_EQ(1u, s.num_intervals());
  // Note: stays in multi mode after align
  ASSERT_EQ(16u, s.size());
  ASSERT_TRUE(s.contains(0, 16));
  
  // Test from interval_set: (4,1) and (8,10) aligned to 10 becomes (0,20)
  s.clear();
  s.insert(4, 1);
  s.insert(8, 10);
  ASSERT_FALSE(s.is_single()) << "Two intervals should be multi";
  s.align(10);
  ASSERT_EQ(1u, s.num_intervals());
  // Note: stays in multi mode after align
  ASSERT_EQ(20u, s.size());
  ASSERT_TRUE(s.contains(0, 20));
  // Test single-mode alignment with power-of-2 (uses bit manipulation)
  test_set s_single;
  s_single.insert(100, 50);
  ASSERT_TRUE(s_single.is_single());
  s_single.align(64);  // Power of 2
  ASSERT_TRUE(s_single.is_single()) << "Single interval should stay single after align";
  ASSERT_EQ(1u, s_single.num_intervals());
  ASSERT_EQ(128u, s_single.size());
  ASSERT_TRUE(s_single.contains(64, 128));
  
  // Test single-mode alignment with non-power-of-2 (uses division)
  test_set s_single2;
  s_single2.insert(100, 50);
  ASSERT_TRUE(s_single2.is_single());
  s_single2.align(30);  // Not power of 2
  ASSERT_TRUE(s_single2.is_single()) << "Single interval should stay single after align";
  ASSERT_EQ(1u, s_single2.num_intervals());
  ASSERT_EQ(60u, s_single2.size());
  ASSERT_TRUE(s_single2.contains(90, 60));
}

TEST(HybridExtentSet, Subtract) {
  test_set s1, s2;
  
  s1.insert(10, 20);
  ASSERT_TRUE(s1.is_single());
  s2.insert(15, 5);
  s1.subtract(s2);
  ASSERT_EQ(2u, s1.num_intervals());
  ASSERT_FALSE(s1.is_single()) << "Subtract creating 2 intervals should be multi";
  ASSERT_EQ(15u, s1.size());
  ASSERT_TRUE(s1.contains(10, 5));
  ASSERT_TRUE(s1.contains(20, 10));
  ASSERT_FALSE(s1.contains(15, 5));
  
  // s1 was previously upgraded to multi-mode, so it stays multi even after clear
  s1.clear();
  s2.clear();
  s1.insert(10, 20);
  ASSERT_FALSE(s1.is_single()) << "s1 stays in multi-mode after previous upgrade";
  s2.insert(5, 10);
  s1.subtract(s2);
  ASSERT_EQ(1u, s1.num_intervals());
  ASSERT_FALSE(s1.is_single()) << "s1 stays in multi-mode (no downgrade policy)";
  ASSERT_EQ(15u, s1.size());
  ASSERT_TRUE(s1.contains(15, 15));
  
  // s1 was previously upgraded to multi-mode, so it stays multi even after clear
  s1.clear();
  s2.clear();
  s1.insert(10, 20);
  ASSERT_FALSE(s1.is_single()) << "s1 stays in multi-mode after previous upgrade";
  s2.insert(25, 10);
  s1.subtract(s2);
  ASSERT_EQ(1u, s1.num_intervals());
  ASSERT_FALSE(s1.is_single()) << "s1 stays in multi-mode (no downgrade policy)";
  ASSERT_EQ(15u, s1.size());
  ASSERT_TRUE(s1.contains(10, 15));
}

TEST(HybridExtentSet, EraseAfter) {
  test_set s;
  
  s.insert(10, 20);
  s.insert(40, 20);
  ASSERT_FALSE(s.is_single());
  s.erase_after(25);
  ASSERT_EQ(1u, s.num_intervals());
  // Note: stays in multi mode
  ASSERT_EQ(15u, s.size());
  ASSERT_TRUE(s.contains(10, 15));
  ASSERT_FALSE(s.contains(25, 1));
  
  s.clear();
  s.insert(10, 20);
  s.insert(40, 20);
  ASSERT_FALSE(s.is_single());
  s.erase_after(50);
  ASSERT_EQ(2u, s.num_intervals());
  ASSERT_FALSE(s.is_single());
  ASSERT_EQ(30u, s.size());
  ASSERT_TRUE(s.contains(10, 20));
  ASSERT_TRUE(s.contains(40, 10));
  ASSERT_FALSE(s.contains(50, 1));
}



TEST(HybridExtentSet, NoDowngradeAfterUpgrade) {
  test_set s;

  // Start in single mode
  s.insert(100, 50);
  ASSERT_EQ(1u, s.num_intervals());
  ASSERT_TRUE(s.is_single()) << "Should start in single mode";

  // Force upgrade to multi mode by inserting non-adjacent interval
  s.insert(200, 50);
  ASSERT_EQ(2u, s.num_intervals());
  ASSERT_FALSE(s.is_single()) << "Should upgrade to multi mode";

  // Now erase one interval, leaving only one
  s.erase(200, 50);
  ASSERT_EQ(1u, s.num_intervals());
  ASSERT_FALSE(s.is_single()) << "Should stay in multi mode (no downgrade)";
  ASSERT_TRUE(s.contains(100, 50));

  // Verify it still works correctly in multi mode
  s.insert(150, 50);  // Adjacent - should merge
  ASSERT_EQ(1u, s.num_intervals());
  ASSERT_FALSE(s.is_single()) << "Should stay in multi mode";
  ASSERT_TRUE(s.contains(100, 100));

  // Erase back down to single interval again
  s.erase(150, 50);
  ASSERT_EQ(1u, s.num_intervals());
  ASSERT_FALSE(s.is_single()) << "Should stay in multi mode";
  ASSERT_TRUE(s.contains(100, 50));

  // Clear and re-insert - should still be in multi mode
  s.clear();
  ASSERT_TRUE(s.empty());
  ASSERT_FALSE(s.is_single()) << "Should stay in multi mode after clear";
  s.insert(300, 50);
  ASSERT_EQ(1u, s.num_intervals());
  ASSERT_FALSE(s.is_single()) << "Should stay in multi mode";
  ASSERT_TRUE(s.contains(300, 50));
}

TEST(HybridExtentSet, IteratorOnEmptyMultiMode) {
  test_set s;
  
  // Start in single mode, upgrade to multi, then clear
  s.insert(100, 50);
  ASSERT_TRUE(s.is_single());
  
  // Upgrade to multi mode
  s.insert(200, 50);
  ASSERT_FALSE(s.is_single()) << "Should be in multi mode";
  ASSERT_EQ(2u, s.num_intervals());
  
  // Clear - stays in multi mode but becomes empty
  s.clear();
  ASSERT_TRUE(s.empty());
  ASSERT_FALSE(s.is_single()) << "Should stay in multi mode after clear";
  
  // Test iterator behavior on empty multi-mode set
  auto begin_it = s.begin();
  auto end_it = s.end();
  
  // begin() should equal end() for empty set
  ASSERT_EQ(begin_it, end_it) << "begin() should equal end() for empty multi-mode set";
  
  // Should not be able to iterate
  int count = 0;
  for (auto it = s.begin(); it != s.end(); ++it) {
    count++;
  }
  ASSERT_EQ(0, count) << "Should not iterate over empty multi-mode set";
}


TEST(HybridExtentSet, SingleContainsMulti) {
  test_set single_large;
  test_set multi_small;

  // Single large extent from 0 to 1000
  single_large.insert(0, 1000);
  ASSERT_TRUE(single_large.is_single()) << "Should be in single mode";

  // Multiple small extents all within the large one
  multi_small.insert(10, 50);
  multi_small.insert(100, 50);
  multi_small.insert(200, 50);
  multi_small.insert(300, 50);
  ASSERT_FALSE(multi_small.is_single()) << "Four intervals should be multi";

  // The large single extent should contain all the small extents
  ASSERT_TRUE(single_large.contains(multi_small));

  // Add an extent outside the range
  multi_small.insert(1100, 50);
  ASSERT_FALSE(multi_small.is_single()) << "Five intervals should be multi";

  // Now it should NOT contain all
  ASSERT_FALSE(single_large.contains(multi_small));
}


TEST(HybridExtentSet, MultiModeWithSingleExtentFullCoverage) {
  test_set s;

  // Start in single mode
  s.insert(100, 50);
  ASSERT_EQ(1u, s.num_intervals());

  // Upgrade to multi mode
  s.insert(200, 50);
  ASSERT_EQ(2u, s.num_intervals());

  // Reduce back to single extent (but still in multi mode)
  s.erase(200, 50);
  ASSERT_EQ(1u, s.num_intervals());

  // Now test all operations while in multi mode with single extent

  // Test: empty() and size()
  ASSERT_FALSE(s.empty());
  ASSERT_EQ(50u, s.size());

  // Test: contains(offset, length)
  ASSERT_TRUE(s.contains(100, 50));
  ASSERT_TRUE(s.contains(110, 30));
  ASSERT_FALSE(s.contains(90, 20));
  ASSERT_FALSE(s.contains(140, 20));

  // Test: intersects()
  ASSERT_TRUE(s.intersects(90, 20));
  ASSERT_TRUE(s.intersects(140, 20));
  ASSERT_FALSE(s.intersects(50, 40));
  ASSERT_FALSE(s.intersects(160, 40));

  // Test: range_start() and range_end()
  ASSERT_EQ(100u, s.range_start());
  ASSERT_EQ(150u, s.range_end());

  // Test: insert with merge
  s.insert(150, 50);
  ASSERT_EQ(1u, s.num_intervals());
  ASSERT_EQ(100u, s.size());
  ASSERT_TRUE(s.contains(100, 100));

  // Test: erase partial
  s.erase(120, 60);
  ASSERT_EQ(2u, s.num_intervals());
  ASSERT_TRUE(s.contains(100, 20));
  ASSERT_TRUE(s.contains(180, 20));

  // Reset to single extent in multi mode
  s.clear();
  s.insert(100, 50);
  s.insert(200, 50);
  s.erase(200, 50);

  // Test: union_of with another set
  test_set other;
  other.insert(50, 30);
  other.insert(160, 30);
  s.union_of(other);
  ASSERT_EQ(3u, s.num_intervals());
  ASSERT_TRUE(s.contains(50, 30));
  ASSERT_TRUE(s.contains(100, 50));
  ASSERT_TRUE(s.contains(160, 30));

  // Reset
  s.clear();
  s.insert(100, 100);
  s.insert(300, 50);
  s.erase(300, 50);

  // Test: intersection_of
  test_set other2;
  other2.insert(120, 60);
  s.intersection_of(other2);
  ASSERT_EQ(1u, s.num_intervals());
  ASSERT_TRUE(s.contains(120, 60));

  // Reset
  s.clear();
  s.insert(100, 100);
  s.insert(300, 50);
  s.erase(300, 50);

  // Test: subtract
  test_set other3;
  other3.insert(120, 30);
  s.subtract(other3);
  ASSERT_EQ(2u, s.num_intervals());
  ASSERT_TRUE(s.contains(100, 20));
  ASSERT_TRUE(s.contains(150, 50));

  // Reset
  s.clear();
  s.insert(100, 100);
  s.insert(300, 50);
  s.erase(300, 50);

  // Test: subset_of
  test_set superset;
  superset.insert(50, 200);
  ASSERT_TRUE(s.subset_of(superset));
  test_set not_superset;
  not_superset.insert(120, 50);
  ASSERT_FALSE(s.subset_of(not_superset));

  // Test: contains(other set)
  test_set subset;
  subset.insert(110, 30);
  subset.insert(150, 20);
  ASSERT_TRUE(s.contains(subset));

  // Test: iterator
  s.clear();
  s.insert(100, 50);
  s.insert(200, 50);
  s.erase(200, 50);
  int count = 0;
  for (const auto& [offset, length] : s) {
    ASSERT_EQ(100u, offset);
    ASSERT_EQ(50u, length);
    count++;
  }
  ASSERT_EQ(1, count);

  // Test: erase_after
  s.clear();
  s.insert(100, 100);
  s.insert(300, 50);
  s.erase(300, 50);
  s.erase_after(150);
  ASSERT_EQ(1u, s.num_intervals());
  ASSERT_TRUE(s.contains(100, 50));

  // Test: align
  s.clear();
  s.insert(105, 90);
  s.insert(300, 50);
  s.erase(300, 50);
  s.align(10);
  ASSERT_EQ(1u, s.num_intervals());
  ASSERT_EQ(100u, s.range_start());  // 105 rounds down to 100
  ASSERT_EQ(200u, s.range_end());    // 195 rounds up to 200

  // Test: equality operators
  test_set s2;
  s2.insert(100, 100);  // Same extent as s after align
  ASSERT_TRUE(s == s2);
  ASSERT_FALSE(s != s2);
  s2.insert(200, 10);  // Add another extent
  ASSERT_FALSE(s == s2);
  ASSERT_TRUE(s != s2);
}



TEST(HybridExtentSet, IntersectionSingleSingleStaysSingle) {
  test_set s1, s2;
  
  // Test 1: Overlapping single intervals - should stay single
  s1.insert(100, 50);
  s2.insert(120, 40);
  ASSERT_TRUE(s1.is_single());
  ASSERT_TRUE(s2.is_single());
  s1.intersection_of(s2);
  ASSERT_FALSE(s1.empty());
  ASSERT_EQ(1u, s1.num_intervals());
  ASSERT_TRUE(s1.is_single()) << "Single-single intersection should stay single";
  ASSERT_TRUE(s1.contains(120, 30));
  
  // Test 2: Non-overlapping single intervals - should be empty but stay single
  s1.clear();
  s2.clear();
  s1.insert(100, 50);
  s2.insert(200, 50);
  ASSERT_TRUE(s1.is_single());
  ASSERT_TRUE(s2.is_single());
  s1.intersection_of(s2);
  ASSERT_TRUE(s1.empty());
  ASSERT_TRUE(s1.is_single()) << "Empty result should stay single mode";
  
  // Test 3: Partial overlap at start
  s1.clear();
  s2.clear();
  s1.insert(100, 50);
  s2.insert(90, 20);
  s1.intersection_of(s2);
  ASSERT_FALSE(s1.empty());
  ASSERT_EQ(1u, s1.num_intervals());
  ASSERT_TRUE(s1.is_single()) << "Single-single intersection should stay single";
  ASSERT_TRUE(s1.contains(100, 10));
  
  // Test 4: Complete containment
  s1.clear();
  s2.clear();
  s1.insert(100, 50);
  s2.insert(110, 20);
  s1.intersection_of(s2);
  ASSERT_FALSE(s1.empty());
  ASSERT_EQ(1u, s1.num_intervals());
  ASSERT_TRUE(s1.is_single()) << "Single-single intersection should stay single";
  ASSERT_TRUE(s1.contains(110, 20));
}


TEST(HybridExtentSet, UnionOfWithEmpty) {
  test_set result, a, b;

  // Case 1: Both empty
  result.union_of(a, b);
  ASSERT_TRUE(result.empty());
  ASSERT_TRUE(result.is_single()) << "Empty result should be single";

  // Case 2: a empty (single mode), b has data
  b.insert(100, 50);
  result.union_of(a, b);
  ASSERT_FALSE(result.empty());
  ASSERT_EQ(1u, result.num_intervals());
  ASSERT_TRUE(result.is_single()) << "Single interval result should be single";
  ASSERT_TRUE(result.contains(100, 50));

  // Case 3: a has data, b empty (single mode)
  a.insert(200, 50);
  b.clear();
  result.union_of(a, b);
  ASSERT_FALSE(result.empty());
  ASSERT_EQ(1u, result.num_intervals());
  ASSERT_TRUE(result.is_single()) << "Single interval result should be single";
  ASSERT_TRUE(result.contains(200, 50));

  // Case 4: Both have data
  a.clear();
  b.clear();
  a.insert(100, 50);
  b.insert(200, 50);
  result.union_of(a, b);
  ASSERT_EQ(2u, result.num_intervals());
  ASSERT_FALSE(result.is_single()) << "Two intervals should be multi";
  ASSERT_TRUE(result.contains(100, 50));
  ASSERT_TRUE(result.contains(200, 50));
}
