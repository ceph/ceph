// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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
