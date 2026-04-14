// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <gtest/gtest.h>
#include "common/bitset_set.h"

struct Key {
  int8_t k;

  constexpr Key(int8_t k) : k(k) {
  }

  explicit constexpr operator int8_t() const {
    return k;
  }
  explicit constexpr operator int() const {
    return k;
  }

  friend std::ostream &operator<<(std::ostream &lhs, const Key &rhs) {
    return lhs << (uint32_t) rhs.k;
  }
  friend bool operator==(const Key &lhs, const Key &rhs) = default;
};

namespace fmt {
template <>
struct formatter<Key> : private formatter<int> {
  using formatter<int>::parse;
  template <typename FormatContext>
  auto format(const Key& k, FormatContext& ctx) const {
    return formatter<int>::format(k.k, ctx);
  }
};
} // namespace fmt


TEST(bitset_set, constructors) {
  bitset_set<128, Key> bs;
  ASSERT_TRUE(bs.empty());
  bs.insert(2);
  bs.insert(4);
  bitset_set<128, Key> bs2(bs);
  ASSERT_EQ(bs, bs2);
  int array[] = {2, 4};
  bitset_set<128, Key> bs3(array, array + 2);
  ASSERT_EQ(bs, bs3);
  bitset_set<128, Key> bs4{2, 4};
  ASSERT_EQ(bs, bs4);
}

TEST(bitset_set, insert_emplace) {
  bitset_set<128, Key> bitset;
  bitset.insert(1);
  bitset.emplace(3);

  Key keys[] = {1, 3};
  int i = 0;
  for (const Key &k : bitset) {
    ASSERT_EQ(k, keys[i++]);
  }
  ASSERT_EQ(2, i);

  bitset_set<128, Key> bitset2;
  bitset2.insert(5);
  bitset2.insert(bitset);
  ASSERT_EQ(3, bitset2.size());
}

TEST(bitset_set, erase) {
  bitset_set<128, Key> bitset;
  bitset.insert(1);
  bitset.insert(3);
  bitset.insert(5);
  bitset.erase(1);

  Key keys[] = {3, 5};

  // i is used here to count the number of iterations and check it against the
  // reference array.
  int i = 0;
  for (const Key &k : bitset) {
    ASSERT_EQ(keys[i++], k);
  }
  ASSERT_EQ(2, i);
}

void test_insert_range(int start, int length)
{
  bitset_set<128, Key> bitset;
  bitset_set<128, Key> bitset_ref;

  bitset.insert_range(start, length);
  for (int i = start; i < length + start; ++i) {
    bitset_ref.insert(i);
  }
  ASSERT_EQ(bitset_ref, bitset) << "start=" << start << " length=" << length;
}

TEST(bitset_set, insert_range) {
  for (int i=0; i < 128; i++) {
    for (int j=0; j <= 128 - i; j++) {
      test_insert_range(i, j);
    }
  }
}

void test_erase_range(int start, int length)
{
  bitset_set<128, Key> bitset;
  bitset_set<128, Key> bitset_ref;

  bitset.insert_range(0, 128);
  bitset_ref.insert_range(0, 128);
  bitset.erase_range(start, length);
  for (int i = start; i < length + start; ++i) {
    bitset_ref.erase(i);
  }
  ASSERT_EQ(bitset_ref, bitset);
}

TEST(bitset_set, erase_range) {
  for (int i=0; i < 128; i++) {
    for (int j=0; j <= 128 - i; j++) {
      test_erase_range(i, j);
    }
  }
}

TEST(bitset_set, clear_empty) {
  bitset_set<128, Key> bitset;
  bitset.insert_range(1, 4);
  bitset.clear();
  ASSERT_TRUE(bitset.empty());
}

TEST(bitset_set, count_contains_find) {
  bitset_set<128, Key> bitset;
  ASSERT_FALSE(bitset.contains(1));
  ASSERT_EQ(0, bitset.count(1));
  ASSERT_EQ(bitset.end(), bitset.find(1));

  bitset.insert(1);
  ASSERT_TRUE(bitset.contains(1));
  ASSERT_EQ(1, bitset.count(1));
  ASSERT_EQ(bitset.begin(), bitset.find(1));
}

TEST(bitset_set, swap) {
  bitset_set<128, Key> bitset;
  bitset_set<128, Key> bitset2;

  ASSERT_FALSE(bitset.contains(1));
  ASSERT_EQ(0, bitset.count(1));
  ASSERT_EQ(bitset.end(), bitset.find(1));

  bitset.insert(1);
  ASSERT_FALSE(bitset.empty());
  ASSERT_TRUE(bitset2.empty());

  bitset2.swap(bitset);
  ASSERT_TRUE(bitset.empty());
  ASSERT_FALSE(bitset2.empty());
}

TEST(bitset_set, assign) {
  bitset_set<128, Key> bitset;
  bitset_set<128, Key> bitset2;

  bitset.insert(1);
  ASSERT_FALSE(bitset.empty());
  ASSERT_TRUE(bitset2.empty());

  bitset2 = bitset;
  ASSERT_FALSE(bitset.empty());
  ASSERT_FALSE(bitset2.empty());

  bitset_set<128, Key> bitset3;
  bitset3 = std::move(bitset);
  ASSERT_FALSE(bitset3.empty());
}

TEST(bitset_set, equality) {
  bitset_set<128, Key> bitset;
  bitset_set<128, Key> bitset2;

  ASSERT_EQ(bitset, bitset2);
  bitset.insert(1);
  ASSERT_NE(bitset, bitset2);
  bitset2.insert(1);
  ASSERT_EQ(bitset, bitset2);
  bitset.insert(64);
  ASSERT_NE(bitset, bitset2);
  bitset2.insert(64);
  ASSERT_EQ(bitset, bitset2);
}

TEST(bitset_set, fmt_formatting) {
  bitset_set<128, Key> bitset;
  // when empty:
  auto using_fmt = fmt::format("{}", bitset);
  ASSERT_EQ("{}", using_fmt);

  bitset.insert(30);
  bitset.insert(20);
  bitset.insert(10);
  using_fmt = fmt::format("{}", bitset);
  ASSERT_EQ("{10,20,30}", using_fmt);
  // compare to operator<<
  std::ostringstream oss;
  oss << bitset;
  EXPECT_EQ(using_fmt, oss.str());
}

TEST(bitset_set, find_nth) {
  constexpr size_t range = 128;
  bitset_set<range, Key> bitset;

  ASSERT_EQ(bitset.end(), bitset.find_nth(0) );
  ASSERT_EQ(bitset.end(), bitset.find_nth(1) );
  ASSERT_EQ(bitset.end(), bitset.find_nth(range) );

  bitset.insert(0);
  ASSERT_EQ(Key(0), *bitset.find_nth(0) );
  ASSERT_EQ(bitset.end(), bitset.find_nth(1) );
  ASSERT_EQ(bitset.end(), bitset.find_nth(range) );

  // Single bit set
  for (unsigned int i = 0; i < range; i++) {
    bitset.clear();
    bitset.insert(i);
    ASSERT_EQ(Key(i), *bitset.find_nth(0) );
    ASSERT_EQ(bitset.end(), bitset.find_nth(1) );
    ASSERT_EQ(bitset.end(), bitset.find_nth(range) );
  }

  /* Alt bits set */
  bitset.clear();
  for (unsigned int i = 0; i < range; i += 2) {
    bitset.insert(i);
  }
  for (unsigned int i = 0; i < range / 2; i++) {
    ASSERT_EQ(Key(i * 2), *bitset.find_nth(i) );
  }
  ASSERT_EQ(bitset.end(), bitset.find_nth(range / 2) );

  /* Other alt bits set */
  bitset.clear();
  for (unsigned int i = 1; i < range; i += 2) {
    bitset.insert(i);
  }
  for (unsigned int i = 0; i < range / 2; i++) {
    ASSERT_EQ(Key(i * 2 + 1), *bitset.find_nth(i) );
  }
  ASSERT_EQ(bitset.end(), bitset.find_nth(range / 2) );

  /* All bits set */
  bitset.clear();
  bitset.insert_range(Key(0), range);
  for (unsigned int i = 0; i < range; i++) {
    ASSERT_EQ(Key(i), *bitset.find_nth(i) );
  }
  ASSERT_EQ(bitset.end(), bitset.find_nth(range) );
}

TEST(bitset_set, left_shift) {
  bitset_set<128, Key> bitset;
  
  // Test shift by 0 (no change)
  bitset.insert(0);
  bitset.insert(5);
  bitset.insert(10);
  auto result = bitset << 0;
  ASSERT_EQ(bitset, result);
  
  // Test simple left shift
  bitset.clear();
  bitset.insert(0);
  bitset.insert(1);
  bitset.insert(2);
  result = bitset << 3;
  ASSERT_TRUE(result.contains(3));
  ASSERT_TRUE(result.contains(4));
  ASSERT_TRUE(result.contains(5));
  ASSERT_FALSE(result.contains(0));
  ASSERT_FALSE(result.contains(1));
  ASSERT_FALSE(result.contains(2));
  
  // Test shift across word boundary (64 bits)
  bitset.clear();
  bitset.insert(0);
  bitset.insert(63);
  result = bitset << 1;
  ASSERT_TRUE(result.contains(1));
  ASSERT_TRUE(result.contains(64));
  ASSERT_FALSE(result.contains(0));
  ASSERT_FALSE(result.contains(63));
  
  // Test word-aligned shift
  bitset.clear();
  bitset.insert(0);
  bitset.insert(1);
  result = bitset << 64;
  ASSERT_TRUE(result.contains(64));
  ASSERT_TRUE(result.contains(65));
  ASSERT_FALSE(result.contains(0));
  ASSERT_FALSE(result.contains(1));
  
  // Test shift beyond max_bits
  bitset.clear();
  bitset.insert(0);
  result = bitset << 128;
  ASSERT_TRUE(result.empty());
  
  // Test shift that loses some bits
  bitset.clear();
  bitset.insert(120);
  bitset.insert(127);
  result = bitset << 5;
  ASSERT_TRUE(result.contains(125));
  ASSERT_FALSE(result.contains(132)); // Would be out of range
  ASSERT_EQ(1, result.size());
}

TEST(bitset_set, right_shift) {
  bitset_set<128, Key> bitset;
  
  // Test shift by 0 (no change)
  bitset.insert(5);
  bitset.insert(10);
  bitset.insert(15);
  auto result = bitset >> 0;
  ASSERT_EQ(bitset, result);
  
  // Test simple right shift
  bitset.clear();
  bitset.insert(3);
  bitset.insert(4);
  bitset.insert(5);
  result = bitset >> 3;
  ASSERT_TRUE(result.contains(0));
  ASSERT_TRUE(result.contains(1));
  ASSERT_TRUE(result.contains(2));
  ASSERT_FALSE(result.contains(3));
  ASSERT_FALSE(result.contains(4));
  ASSERT_FALSE(result.contains(5));
  
  // Test shift across word boundary (64 bits)
  bitset.clear();
  bitset.insert(1);
  bitset.insert(64);
  result = bitset >> 1;
  ASSERT_TRUE(result.contains(0));
  ASSERT_TRUE(result.contains(63));
  ASSERT_FALSE(result.contains(1));
  ASSERT_FALSE(result.contains(64));
  
  // Test word-aligned shift
  bitset.clear();
  bitset.insert(64);
  bitset.insert(65);
  result = bitset >> 64;
  ASSERT_TRUE(result.contains(0));
  ASSERT_TRUE(result.contains(1));
  ASSERT_FALSE(result.contains(64));
  ASSERT_FALSE(result.contains(65));
  
  // Test shift beyond max_bits
  bitset.clear();
  bitset.insert(127);
  result = bitset >> 128;
  ASSERT_TRUE(result.empty());
  
  // Test shift that loses some bits
  bitset.clear();
  bitset.insert(0);
  bitset.insert(7);
  result = bitset >> 5;
  ASSERT_TRUE(result.contains(2));
  ASSERT_FALSE(result.contains(0)); // Lost
  ASSERT_EQ(1, result.size());
}

TEST(bitset_set, shift_assignment) {
  bitset_set<128, Key> bitset;
  
  // Test left shift assignment
  bitset.insert(0);
  bitset.insert(1);
  bitset <<= 3;
  ASSERT_TRUE(bitset.contains(3));
  ASSERT_TRUE(bitset.contains(4));
  ASSERT_FALSE(bitset.contains(0));
  ASSERT_FALSE(bitset.contains(1));
  
  // Test right shift assignment
  bitset.clear();
  bitset.insert(5);
  bitset.insert(6);
  bitset >>= 2;
  ASSERT_TRUE(bitset.contains(3));
  ASSERT_TRUE(bitset.contains(4));
  ASSERT_FALSE(bitset.contains(5));
  ASSERT_FALSE(bitset.contains(6));
}

TEST(bitset_set, shift_multiple_words) {
  bitset_set<128, Key> bitset;
  
  // Test pattern across multiple words
  for (int i = 0; i < 128; i += 8) {
    bitset.insert(i);
  }
  
  auto result = bitset << 4;
  for (int i = 0; i < 128; i += 8) {
    if (i + 4 < 128) {
      ASSERT_TRUE(result.contains(i + 4));
    }
    ASSERT_FALSE(result.contains(i));
  }
  
  result = bitset >> 4;
  for (int i = 0; i < 128; i += 8) {
    if (i >= 4) {
      ASSERT_TRUE(result.contains(i - 4));
    }
  }
}

TEST(bitset_set, shift_edge_cases) {
  bitset_set<128, Key> bitset;
  
  // Empty set
  auto result = bitset << 5;
  ASSERT_TRUE(result.empty());
  result = bitset >> 5;
  ASSERT_TRUE(result.empty());
  
  // Full set left shift
  bitset.insert_range(0, 128);
  result = bitset << 10;
  ASSERT_EQ(118, result.size()); // 128 - 10 bits remain
  for (int i = 10; i < 128; ++i) {
    ASSERT_TRUE(result.contains(i));
  }
  
  // Full set right shift
  result = bitset >> 10;
  ASSERT_EQ(118, result.size()); // 128 - 10 bits remain
  for (int i = 0; i < 118; ++i) {
    ASSERT_TRUE(result.contains(i));
  }
}

TEST(bitset_set, shift_boundary_behavior) {
  bitset_set<128, Key> bitset;
  
  // Test that shift-left followed by shift-right zeros bits that went out of range
  bitset.insert(0);
  bitset.insert(64);
  bitset.insert(120);
  bitset.insert(127);
  
  // Shift left by 10, then right by 10 - bits at 120 and 127 should be lost
  auto result = (bitset << 10) >> 10;
  ASSERT_TRUE(result.contains(0));
  ASSERT_TRUE(result.contains(64));
  ASSERT_FALSE(result.contains(120)); // Lost when shifted left
  ASSERT_FALSE(result.contains(127)); // Lost when shifted left
  ASSERT_EQ(2, result.size());
  
  // Test that shift-right followed by shift-left zeros bits that went out of range
  bitset.clear();
  bitset.insert(0);
  bitset.insert(5);
  bitset.insert(64);
  bitset.insert(127);
  
  result = (bitset >> 10) << 10;
  ASSERT_FALSE(result.contains(0));  // Lost when shifted right
  ASSERT_FALSE(result.contains(5));  // Lost when shifted right
  ASSERT_TRUE(result.contains(64));
  ASSERT_TRUE(result.contains(127));
  ASSERT_EQ(2, result.size());
  
  // Test large shift that loses everything
  bitset.clear();
  bitset.insert_range(0, 128);
  result = (bitset << 64) >> 64;
  ASSERT_EQ(64, result.size()); // Only lower 64 bits remain
  for (int i = 0; i < 64; ++i) {
    ASSERT_TRUE(result.contains(i));
  }
  for (int i = 64; i < 128; ++i) {
    ASSERT_FALSE(result.contains(i));
  }
  
  // Test shift beyond range
  bitset.clear();
  bitset.insert_range(0, 128);
  result = (bitset << 200) >> 200;
  ASSERT_TRUE(result.empty()); // Everything lost
}


TEST(bitset_set, bitwise_and) {
  bitset_set<128, Key> bitset1;
  bitset_set<128, Key> bitset2;
  
  // Test empty sets
  auto result = bitset1 & bitset2;
  ASSERT_TRUE(result.empty());
  
  // Test one empty, one non-empty
  bitset1.insert(5);
  bitset1.insert(10);
  result = bitset1 & bitset2;
  ASSERT_TRUE(result.empty());
  
  // Test no overlap
  bitset2.insert(15);
  bitset2.insert(20);
  result = bitset1 & bitset2;
  ASSERT_TRUE(result.empty());
  
  // Test partial overlap
  bitset2.insert(10);
  result = bitset1 & bitset2;
  ASSERT_EQ(1, result.size());
  ASSERT_TRUE(result.contains(10));
  ASSERT_FALSE(result.contains(5));
  ASSERT_FALSE(result.contains(15));
  ASSERT_FALSE(result.contains(20));
  
  // Test complete overlap
  bitset1.clear();
  bitset2.clear();
  bitset1.insert(1);
  bitset1.insert(2);
  bitset1.insert(3);
  bitset2.insert(1);
  bitset2.insert(2);
  bitset2.insert(3);
  result = bitset1 & bitset2;
  ASSERT_EQ(3, result.size());
  ASSERT_TRUE(result.contains(1));
  ASSERT_TRUE(result.contains(2));
  ASSERT_TRUE(result.contains(3));
  
  // Test across word boundaries
  bitset1.clear();
  bitset2.clear();
  bitset1.insert(0);
  bitset1.insert(63);
  bitset1.insert(64);
  bitset1.insert(127);
  bitset2.insert(63);
  bitset2.insert(64);
  result = bitset1 & bitset2;
  ASSERT_EQ(2, result.size());
  ASSERT_TRUE(result.contains(63));
  ASSERT_TRUE(result.contains(64));
  ASSERT_FALSE(result.contains(0));
  ASSERT_FALSE(result.contains(127));
}

TEST(bitset_set, bitwise_or) {
  bitset_set<128, Key> bitset1;
  bitset_set<128, Key> bitset2;
  
  // Test empty sets
  auto result = bitset1 | bitset2;
  ASSERT_TRUE(result.empty());
  
  // Test one empty, one non-empty
  bitset1.insert(5);
  bitset1.insert(10);
  result = bitset1 | bitset2;
  ASSERT_EQ(2, result.size());
  ASSERT_TRUE(result.contains(5));
  ASSERT_TRUE(result.contains(10));
  
  // Test no overlap
  bitset2.insert(15);
  bitset2.insert(20);
  result = bitset1 | bitset2;
  ASSERT_EQ(4, result.size());
  ASSERT_TRUE(result.contains(5));
  ASSERT_TRUE(result.contains(10));
  ASSERT_TRUE(result.contains(15));
  ASSERT_TRUE(result.contains(20));
  
  // Test partial overlap
  bitset2.insert(10);
  result = bitset1 | bitset2;
  ASSERT_EQ(4, result.size());
  ASSERT_TRUE(result.contains(5));
  ASSERT_TRUE(result.contains(10));
  ASSERT_TRUE(result.contains(15));
  ASSERT_TRUE(result.contains(20));
  
  // Test complete overlap
  bitset1.clear();
  bitset2.clear();
  bitset1.insert(1);
  bitset1.insert(2);
  bitset1.insert(3);
  bitset2.insert(1);
  bitset2.insert(2);
  bitset2.insert(3);
  result = bitset1 | bitset2;
  ASSERT_EQ(3, result.size());
  ASSERT_TRUE(result.contains(1));
  ASSERT_TRUE(result.contains(2));
  ASSERT_TRUE(result.contains(3));
  
  // Test across word boundaries
  bitset1.clear();
  bitset2.clear();
  bitset1.insert(0);
  bitset1.insert(63);
  bitset2.insert(64);
  bitset2.insert(127);
  result = bitset1 | bitset2;
  ASSERT_EQ(4, result.size());
  ASSERT_TRUE(result.contains(0));
  ASSERT_TRUE(result.contains(63));
  ASSERT_TRUE(result.contains(64));
  ASSERT_TRUE(result.contains(127));
}

TEST(bitset_set, bitwise_and_assignment) {
  bitset_set<128, Key> bitset1;
  bitset_set<128, Key> bitset2;
  
  // Test empty sets
  bitset1 &= bitset2;
  ASSERT_TRUE(bitset1.empty());
  
  // Test one empty, one non-empty
  bitset1.insert(5);
  bitset1.insert(10);
  bitset1 &= bitset2;
  ASSERT_TRUE(bitset1.empty());
  
  // Test no overlap
  bitset1.insert(5);
  bitset1.insert(10);
  bitset2.insert(15);
  bitset2.insert(20);
  bitset1 &= bitset2;
  ASSERT_TRUE(bitset1.empty());
  
  // Test partial overlap
  bitset1.clear();
  bitset2.clear();
  bitset1.insert(5);
  bitset1.insert(10);
  bitset1.insert(15);
  bitset2.insert(10);
  bitset2.insert(15);
  bitset2.insert(20);
  bitset1 &= bitset2;
  ASSERT_EQ(2, bitset1.size());
  ASSERT_TRUE(bitset1.contains(10));
  ASSERT_TRUE(bitset1.contains(15));
  ASSERT_FALSE(bitset1.contains(5));
  ASSERT_FALSE(bitset1.contains(20));
  
  // Test complete overlap
  bitset1.clear();
  bitset2.clear();
  bitset1.insert(1);
  bitset1.insert(2);
  bitset1.insert(3);
  bitset2.insert(1);
  bitset2.insert(2);
  bitset2.insert(3);
  bitset1 &= bitset2;
  ASSERT_EQ(3, bitset1.size());
  ASSERT_TRUE(bitset1.contains(1));
  ASSERT_TRUE(bitset1.contains(2));
  ASSERT_TRUE(bitset1.contains(3));
  
  // Test across word boundaries
  bitset1.clear();
  bitset2.clear();
  bitset1.insert(0);
  bitset1.insert(63);
  bitset1.insert(64);
  bitset1.insert(127);
  bitset2.insert(63);
  bitset2.insert(64);
  bitset1 &= bitset2;
  ASSERT_EQ(2, bitset1.size());
  ASSERT_TRUE(bitset1.contains(63));
  ASSERT_TRUE(bitset1.contains(64));
  ASSERT_FALSE(bitset1.contains(0));
  ASSERT_FALSE(bitset1.contains(127));
}

TEST(bitset_set, bitwise_or_assignment) {
  bitset_set<128, Key> bitset1;
  bitset_set<128, Key> bitset2;
  
  // Test empty sets
  bitset1 |= bitset2;
  ASSERT_TRUE(bitset1.empty());
  
  // Test one empty, one non-empty
  bitset2.insert(5);
  bitset2.insert(10);
  bitset1 |= bitset2;
  ASSERT_EQ(2, bitset1.size());
  ASSERT_TRUE(bitset1.contains(5));
  ASSERT_TRUE(bitset1.contains(10));
  
  // Test no overlap
  bitset1.clear();
  bitset2.clear();
  bitset1.insert(5);
  bitset1.insert(10);
  bitset2.insert(15);
  bitset2.insert(20);
  bitset1 |= bitset2;
  ASSERT_EQ(4, bitset1.size());
  ASSERT_TRUE(bitset1.contains(5));
  ASSERT_TRUE(bitset1.contains(10));
  ASSERT_TRUE(bitset1.contains(15));
  ASSERT_TRUE(bitset1.contains(20));
  
  // Test partial overlap
  bitset1.clear();
  bitset2.clear();
  bitset1.insert(5);
  bitset1.insert(10);
  bitset2.insert(10);
  bitset2.insert(15);
  bitset1 |= bitset2;
  ASSERT_EQ(3, bitset1.size());
  ASSERT_TRUE(bitset1.contains(5));
  ASSERT_TRUE(bitset1.contains(10));
  ASSERT_TRUE(bitset1.contains(15));
  
  // Test complete overlap
  bitset1.clear();
  bitset2.clear();
  bitset1.insert(1);
  bitset1.insert(2);
  bitset1.insert(3);
  bitset2.insert(1);
  bitset2.insert(2);
  bitset2.insert(3);
  bitset1 |= bitset2;
  ASSERT_EQ(3, bitset1.size());
  ASSERT_TRUE(bitset1.contains(1));
  ASSERT_TRUE(bitset1.contains(2));
  ASSERT_TRUE(bitset1.contains(3));
  
  // Test across word boundaries
  bitset1.clear();
  bitset2.clear();
  bitset1.insert(0);
  bitset1.insert(63);
  bitset2.insert(64);
  bitset2.insert(127);
  bitset1 |= bitset2;
  ASSERT_EQ(4, bitset1.size());
  ASSERT_TRUE(bitset1.contains(0));
  ASSERT_TRUE(bitset1.contains(63));
  ASSERT_TRUE(bitset1.contains(64));
  ASSERT_TRUE(bitset1.contains(127));
}

TEST(bitset_set, bitwise_operators_combined) {
  bitset_set<128, Key> bitset1;
  bitset_set<128, Key> bitset2;
  bitset_set<128, Key> bitset3;
  
  // Test combination: (A | B) & C
  bitset1.insert(1);
  bitset1.insert(2);
  bitset2.insert(2);
  bitset2.insert(3);
  bitset3.insert(2);
  bitset3.insert(4);
  
  auto result = (bitset1 | bitset2) & bitset3;
  ASSERT_EQ(1, result.size());
  ASSERT_TRUE(result.contains(2));
  
  // Test combination: (A & B) | C
  bitset1.clear();
  bitset2.clear();
  bitset3.clear();
  bitset1.insert(1);
  bitset1.insert(2);
  bitset2.insert(2);
  bitset2.insert(3);
  bitset3.insert(4);
  bitset3.insert(5);
  
  result = (bitset1 & bitset2) | bitset3;
  ASSERT_EQ(3, result.size());
  ASSERT_TRUE(result.contains(2));
  ASSERT_TRUE(result.contains(4));
  ASSERT_TRUE(result.contains(5));
  
  // Test chained assignment operators
  bitset1.clear();
  bitset2.clear();
  bitset3.clear();
  bitset1.insert(1);
  bitset1.insert(2);
  bitset1.insert(3);
  bitset2.insert(2);
  bitset2.insert(3);
  bitset2.insert(4);
  bitset3.insert(3);
  bitset3.insert(4);
  bitset3.insert(5);
  
  bitset1 &= bitset2;  // bitset1 now has {2, 3}
  bitset1 |= bitset3;  // bitset1 now has {2, 3, 4, 5}
  ASSERT_EQ(4, bitset1.size());
  ASSERT_TRUE(bitset1.contains(2));
  ASSERT_TRUE(bitset1.contains(3));
  ASSERT_TRUE(bitset1.contains(4));
  ASSERT_TRUE(bitset1.contains(5));
}

TEST(bitset_set, bitwise_operators_consistency_with_static_methods) {
  bitset_set<128, Key> bitset1;
  bitset_set<128, Key> bitset2;
  
  // Populate sets
  bitset1.insert(1);
  bitset1.insert(2);
  bitset1.insert(3);
  bitset1.insert(64);
  bitset2.insert(2);
  bitset2.insert(3);
  bitset2.insert(4);
  bitset2.insert(64);
  
  // Test that & operator matches intersection static method
  auto and_result = bitset1 & bitset2;
  auto intersection_result = bitset_set<128, Key>::intersection(bitset1, bitset2);
  ASSERT_EQ(and_result, intersection_result);
  
  // Test that | operator produces union (no static method exists, but verify behavior)
  auto or_result = bitset1 | bitset2;
  ASSERT_EQ(5, or_result.size());
  ASSERT_TRUE(or_result.contains(1));
  ASSERT_TRUE(or_result.contains(2));
  ASSERT_TRUE(or_result.contains(3));
  ASSERT_TRUE(or_result.contains(4));
  ASSERT_TRUE(or_result.contains(64));
}
