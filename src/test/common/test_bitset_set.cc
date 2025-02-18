//
// Created by root on 1/31/25.
//
#include <gtest/gtest.h>
#include <../../common/bitset_set.h>

struct Key {
  int8_t k;

  Key(int8_t k) : k(k) {}

  explicit constexpr operator int8_t() const { return k; }
  explicit constexpr operator int() const { return k; }
  Key &operator++() {k++; return *this;}

  friend std::ostream& operator<<(std::ostream& lhs, const Key& rhs)
  {
    return lhs << (uint32_t)rhs.k;
  }
  friend bool operator==(const Key &lhs, const Key &rhs) { return lhs.k == rhs.k; }
};

TEST(bitset_set, constructors)
{
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

TEST(bitset_set, insert_emplace)
{
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
  int i = 0;
  for (const Key &k : bitset) {
    ASSERT_EQ(keys[i++], k);
  }
  ASSERT_EQ(2, i);
}

TEST(bitset_set, insert_range) {
  bitset_set<128, Key> bitset;
  bitset.insert_range(1, 4);
  {
    Key keys[] = {1, 2, 3, 4};
    int i = 0;
    for (const Key &k : bitset) {
      ASSERT_EQ(keys[i++], k);
    }
  }
  bitset.erase(2);
  {
    Key keys[] = {1, 3, 4};
    int i = 0;
    for (const Key &k : bitset) {
      ASSERT_EQ(keys[i++], k);
    }
  }
  bitset.insert_range(2, 4);
  {
    Key keys[] = {1, 2, 3, 4, 5};
    int i = 0;
    for (const Key &k : bitset) {
      ASSERT_EQ(keys[i++], k);
    }
  }
}

TEST(bitset_set, erase_range) {
  bitset_set<128, Key> bitset;
  bitset.erase_range(1, 2);
  ASSERT_TRUE(bitset.empty());

  bitset.insert_range(1, 5);
  bitset.erase_range(2, 2);
  {
    Key keys[] = {1, 4, 5};
    int i = 0;
    for (const Key &k : bitset) {
      ASSERT_EQ(keys[i++], k);
    }
  }
  bitset.erase_range(5, 2);
  {
    Key keys[] = {1, 4};
    int i = 0;
    for (const Key &k : bitset) {
      ASSERT_EQ(keys[i++], k);
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

  bitset_set<128, Key>bitset3;
  bitset3 = std::move(bitset);
  ASSERT_FALSE(bitset2.empty());
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