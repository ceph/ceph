// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest.h>
#include "common/hybrid_interval_map.h"
#include "common/interval_map.h"
#include "include/buffer.h"
#include <boost/container/flat_map.hpp>

// Split/merge policy for bufferlist
struct bl_split_merge {
  bufferlist split(uint64_t offset, uint64_t len, bufferlist& bl) const
  {
    bufferlist result;
    result.substr_of(bl, offset, len);
    return result;
  }

  bool can_merge(const bufferlist& left, const bufferlist& right) const
  {
    return true;
  }

  bufferlist merge(bufferlist&& left, bufferlist&& right) const
  {
    left.claim_append(right);
    return std::move(left);
  }

  uint64_t length(const bufferlist& bl) const
  {
    return bl.length();
  }
};

// Type aliases for testing
using test_hybrid_map = hybrid_interval_map<uint64_t, bufferlist, bl_split_merge>;
using test_interval_map = interval_map<uint64_t, bufferlist, bl_split_merge>;

// Helper to create a bufferlist with specific content
static bufferlist make_bl(const std::string& content) {
  bufferlist bl;
  bl.append(content);
  return bl;
}

// Helper to extract string from bufferlist
static std::string bl_to_string(const bufferlist& bl) {
  std::string result;
  for (auto& p : bl.buffers()) {
    result.append(p.c_str(), p.length());
  }
  return result;
}

// Helper to count intervals
static size_t count_intervals(const test_hybrid_map& map) {
  size_t count = 0;
  for (auto it = map.begin(); it != map.end(); ++it) {
    ++count;
  }
  return count;
}

// Helper to count intervals in interval_map
static size_t count_intervals(const test_interval_map& map) {
  size_t count = 0;
  for (auto it = map.begin(); it != map.end(); ++it) {
    ++count;
  }
  return count;
}

// Helper to verify map contents
static void verify_map_content(const test_hybrid_map& map, uint64_t offset, uint64_t len, const std::string& expected) {
  bool found = false;
  for (auto it = map.begin(); it != map.end(); ++it) {
    if (it.get_off() == offset && it.get_len() == len) {
      EXPECT_EQ(expected, bl_to_string(it.get_val()))
        << "At offset " << offset << ", length " << len;
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found) << "Expected interval at offset " << offset << ", length " << len << " not found";
}

TEST(HybridIntervalMap, EmptyMap) {
  test_hybrid_map map;
  ASSERT_TRUE(map.empty());
  ASSERT_EQ(0u, count_intervals(map));
  ASSERT_TRUE(map.is_single()) << "Empty map should be in single mode";
}

TEST(HybridIntervalMap, SingleInsert) {
  test_hybrid_map map;
  bufferlist bl = make_bl("AAAA");
  map.insert(100, 4, bl);
  
  ASSERT_FALSE(map.empty());
  ASSERT_EQ(1u, count_intervals(map));
  ASSERT_TRUE(map.is_single()) << "Single interval should stay in single mode";
  
  verify_map_content(map, 100, 4, "AAAA");
}

TEST(HybridIntervalMap, AdjacentMergeAppend) {
  test_hybrid_map map;
  map.insert(100, 4, make_bl("AAAA"));
  map.insert(104, 4, make_bl("BBBB"));  // Adjacent - should merge
  
  ASSERT_FALSE(map.empty());
  ASSERT_EQ(1u, count_intervals(map));
  ASSERT_TRUE(map.is_single()) << "Merged single interval should stay in single mode";
  
  verify_map_content(map, 100, 8, "AAAABBBB");
}

TEST(HybridIntervalMap, AdjacentMergePrepend) {
  test_hybrid_map map;
  map.insert(100, 4, make_bl("BBBB"));
  map.insert(96, 4, make_bl("AAAA"));  // Adjacent before - should merge
  
  ASSERT_FALSE(map.empty());
  ASSERT_EQ(1u, count_intervals(map));
  ASSERT_TRUE(map.is_single()) << "Merged single interval should stay in single mode";
  
  verify_map_content(map, 96, 8, "AAAABBBB");
}

// Test overlapping inserts - the most recent insert should win
TEST(HybridIntervalMap, OverlapPartialEnd) {
  test_hybrid_map hybrid_map;
  test_interval_map interval_map;
  
  // Insert initial data: [100, 108) = "AAAAAAAA"
  hybrid_map.insert(100, 8, make_bl("AAAAAAAA"));
  interval_map.insert(100, 8, make_bl("AAAAAAAA"));
  
  // Overlap at end: [104, 112) = "BBBBBBBB"
  // Expected result: [100, 104) = "AAAA", [104, 112) = "BBBBBBBB"
  hybrid_map.insert(104, 8, make_bl("BBBBBBBB"));
  interval_map.insert(104, 8, make_bl("BBBBBBBB"));
  
  // Both should have same number of intervals
  ASSERT_EQ(count_intervals(interval_map), count_intervals(hybrid_map))
    << "Hybrid and interval map should have same size";
  
  // Verify hybrid_map content matches interval_map
  auto h_it = hybrid_map.begin();
  auto i_it = interval_map.begin();
  while (h_it != hybrid_map.end() && i_it != interval_map.end()) {
    EXPECT_EQ(i_it.get_off(), h_it.get_off()) << "Offsets should match";
    EXPECT_EQ(i_it.get_len(), h_it.get_len()) << "Lengths should match";
    EXPECT_EQ(bl_to_string(i_it.get_val()), bl_to_string(h_it.get_val()))
      << "Content should match at offset " << h_it.get_off();
    ++h_it;
    ++i_it;
  }
}

TEST(HybridIntervalMap, OverlapPartialStart) {
  test_hybrid_map hybrid_map;
  test_interval_map interval_map;
  
  // Insert initial data: [100, 108) = "AAAAAAAA"
  hybrid_map.insert(100, 8, make_bl("AAAAAAAA"));
  interval_map.insert(100, 8, make_bl("AAAAAAAA"));
  
  // Overlap at start: [96, 104) = "BBBBBBBB"
  // Expected result: [96, 104) = "BBBBBBBB", [104, 108) = "AAAA"
  hybrid_map.insert(96, 8, make_bl("BBBBBBBB"));
  interval_map.insert(96, 8, make_bl("BBBBBBBB"));
  
  ASSERT_EQ(count_intervals(interval_map), count_intervals(hybrid_map))
    << "Hybrid and interval map should have same size";
  
  auto h_it = hybrid_map.begin();
  auto i_it = interval_map.begin();
  while (h_it != hybrid_map.end() && i_it != interval_map.end()) {
    EXPECT_EQ(i_it.get_off(), h_it.get_off()) << "Offsets should match";
    EXPECT_EQ(i_it.get_len(), h_it.get_len()) << "Lengths should match";
    EXPECT_EQ(bl_to_string(i_it.get_val()), bl_to_string(h_it.get_val()))
      << "Content should match at offset " << h_it.get_off();
    ++h_it;
    ++i_it;
  }
}

TEST(HybridIntervalMap, OverlapContained) {
  test_hybrid_map hybrid_map;
  test_interval_map interval_map;
  
  // Insert initial data: [100, 120) = "AAAAAAAAAAAAAAAAAAAA"
  hybrid_map.insert(100, 20, make_bl("AAAAAAAAAAAAAAAAAAAA"));
  interval_map.insert(100, 20, make_bl("AAAAAAAAAAAAAAAAAAAA"));
  
  // Insert contained: [105, 115) = "BBBBBBBBBB"
  // Expected result: [100, 105) = "AAAAA", [105, 115) = "BBBBBBBBBB", [115, 120) = "AAAAA"
  hybrid_map.insert(105, 10, make_bl("BBBBBBBBBB"));
  interval_map.insert(105, 10, make_bl("BBBBBBBBBB"));
  
  ASSERT_EQ(count_intervals(interval_map), count_intervals(hybrid_map))
    << "Hybrid and interval map should have same size";
  
  auto h_it = hybrid_map.begin();
  auto i_it = interval_map.begin();
  while (h_it != hybrid_map.end() && i_it != interval_map.end()) {
    EXPECT_EQ(i_it.get_off(), h_it.get_off()) << "Offsets should match";
    EXPECT_EQ(i_it.get_len(), h_it.get_len()) << "Lengths should match";
    EXPECT_EQ(bl_to_string(i_it.get_val()), bl_to_string(h_it.get_val()))
      << "Content should match at offset " << h_it.get_off();
    ++h_it;
    ++i_it;
  }
}

TEST(HybridIntervalMap, OverlapContaining) {
  test_hybrid_map hybrid_map;
  test_interval_map interval_map;
  
  // Insert initial data: [100, 110) = "AAAAAAAAAA"
  hybrid_map.insert(100, 10, make_bl("AAAAAAAAAA"));
  interval_map.insert(100, 10, make_bl("AAAAAAAAAA"));
  
  // Insert containing: [95, 115) = "BBBBBBBBBBBBBBBBBBBB"
  // Expected result: [95, 115) = "BBBBBBBBBBBBBBBBBBBB" (completely replaces)
  hybrid_map.insert(95, 20, make_bl("BBBBBBBBBBBBBBBBBBBB"));
  interval_map.insert(95, 20, make_bl("BBBBBBBBBBBBBBBBBBBB"));
  
  ASSERT_EQ(count_intervals(interval_map), count_intervals(hybrid_map))
    << "Hybrid and interval map should have same size";
  
  auto h_it = hybrid_map.begin();
  auto i_it = interval_map.begin();
  while (h_it != hybrid_map.end() && i_it != interval_map.end()) {
    EXPECT_EQ(i_it.get_off(), h_it.get_off()) << "Offsets should match";
    EXPECT_EQ(i_it.get_len(), h_it.get_len()) << "Lengths should match";
    EXPECT_EQ(bl_to_string(i_it.get_val()), bl_to_string(h_it.get_val()))
      << "Content should match at offset " << h_it.get_off();
    ++h_it;
    ++i_it;
  }
}

TEST(HybridIntervalMap, OverlapExact) {
  test_hybrid_map hybrid_map;
  test_interval_map interval_map;
  
  // Insert initial data: [100, 110) = "AAAAAAAAAA"
  hybrid_map.insert(100, 10, make_bl("AAAAAAAAAA"));
  interval_map.insert(100, 10, make_bl("AAAAAAAAAA"));
  
  // Insert exact same range: [100, 110) = "BBBBBBBBBB"
  // Expected result: [100, 110) = "BBBBBBBBBB" (replaces)
  hybrid_map.insert(100, 10, make_bl("BBBBBBBBBB"));
  interval_map.insert(100, 10, make_bl("BBBBBBBBBB"));
  
  ASSERT_EQ(count_intervals(interval_map), count_intervals(hybrid_map))
    << "Hybrid and interval map should have same size";
  
  auto h_it = hybrid_map.begin();
  auto i_it = interval_map.begin();
  while (h_it != hybrid_map.end() && i_it != interval_map.end()) {
    EXPECT_EQ(i_it.get_off(), h_it.get_off()) << "Offsets should match";
    EXPECT_EQ(i_it.get_len(), h_it.get_len()) << "Lengths should match";
    EXPECT_EQ(bl_to_string(i_it.get_val()), bl_to_string(h_it.get_val()))
      << "Content should match at offset " << h_it.get_off();
    ++h_it;
    ++i_it;
  }
}

// Test sequence of overlapping operations
TEST(HybridIntervalMap, SequenceOfOverlaps) {
  test_hybrid_map hybrid_map;
  test_interval_map interval_map;
  
  // Build up a complex pattern with multiple overlaps
  // Each insert should match between hybrid and interval map
  
  // Step 1: [100, 110) = "AAAAAAAAAA"
  hybrid_map.insert(100, 10, make_bl("AAAAAAAAAA"));
  interval_map.insert(100, 10, make_bl("AAAAAAAAAA"));
  
  // Step 2: [105, 115) = "BBBBBBBBBB" (overlaps end)
  hybrid_map.insert(105, 10, make_bl("BBBBBBBBBB"));
  interval_map.insert(105, 10, make_bl("BBBBBBBBBB"));
  
  // Step 3: [95, 105) = "CCCCCCCCCC" (overlaps start of first)
  hybrid_map.insert(95, 10, make_bl("CCCCCCCCCC"));
  interval_map.insert(95, 10, make_bl("CCCCCCCCCC"));
  
  // Step 4: [108, 118) = "DDDDDDDDDD" (overlaps middle)
  hybrid_map.insert(108, 10, make_bl("DDDDDDDDDD"));
  interval_map.insert(108, 10, make_bl("DDDDDDDDDD"));
  
  ASSERT_EQ(count_intervals(interval_map), count_intervals(hybrid_map))
    << "Hybrid and interval map should have same size after sequence";
  
  auto h_it = hybrid_map.begin();
  auto i_it = interval_map.begin();
  int idx = 0;
  while (h_it != hybrid_map.end() && i_it != interval_map.end()) {
    EXPECT_EQ(i_it.get_off(), h_it.get_off()) << "Offsets should match at index " << idx;
    EXPECT_EQ(i_it.get_len(), h_it.get_len()) << "Lengths should match at index " << idx;
    EXPECT_EQ(bl_to_string(i_it.get_val()), bl_to_string(h_it.get_val()))
      << "Content should match at index " << idx << ", offset " << h_it.get_off();
    ++h_it;
    ++i_it;
    ++idx;
  }
}

// Test that most recent data wins in overlaps
TEST(HybridIntervalMap, MostRecentWins) {
  test_hybrid_map map;
  
  // Insert: [100, 110) = "1111111111"
  map.insert(100, 10, make_bl("1111111111"));
  verify_map_content(map, 100, 10, "1111111111");
  
  // Overlap: [105, 115) = "2222222222"
  // Erase will split [100, 110) into [100, 105), then insert [105, 115)
  // Since can_merge returns true, try_merge will merge them into one interval
  map.insert(105, 10, make_bl("2222222222"));
  
  // After merge, should have one interval [100, 115) with merged content
  ASSERT_EQ(1u, count_intervals(map));
  
  // Verify the merged result
  auto it = map.begin();
  ASSERT_NE(it, map.end());
  EXPECT_EQ(100u, it.get_off());
  EXPECT_EQ(15u, it.get_len());
  // Content should be "11111" + "2222222222" = "111112222222222"
  EXPECT_EQ("111112222222222", bl_to_string(it.get_val()));
}

// Made with Bob
