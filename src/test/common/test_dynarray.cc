// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/dynarray.h"
#include <string>
#include <vector>
#include <gtest/gtest.h>

using ceph::dynarray;

struct immovable
{
  int a, b;

  immovable(int a, int b)
    : a(a), b(b) {}

  immovable(const immovable&) = delete;
  immovable& operator =(const immovable&) = delete;

  immovable(immovable&&) = delete;
  immovable& operator =(immovable&&) = delete;
};

TEST(Dynarray, Value)
{
  dynarray a(3, 1);
  for (auto i = 0; i < 3; ++i) {
    ASSERT_EQ(1, a[i]);
  }
}

TEST(Dynarray, Single)
{
  dynarray a(3, [](std::size_t i) { return int(i); } );
  for (auto i = 0; i < 3; ++i) {
    ASSERT_EQ(i, a[i]);
  }
}

TEST(Dynarray, Tuple)
{
  dynarray<immovable> a(3, [](std::size_t i) {
    return std::tuple(int(i), int(i * 2));
  });
  for (auto i = 0; i < 3; ++i) {
    ASSERT_EQ(i, a[i].a);
    ASSERT_EQ(i * 2, a[i].b);
  }
}

TEST(Dynarray, Iterator)
{
  std::vector v({1, 1, 2, 3, 5, 8});
  dynarray a(v.begin(), v.end());

  auto i = v.begin();
  for (const auto& e : a) {
    ASSERT_EQ(*i, e);
    ++i;
  }
}

TEST(Dynarray, Access) {
  dynarray a{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

  for (std::size_t i = 0; i < a.size(); ++i) {
    ASSERT_EQ(i, a.at(i));
  }
  EXPECT_THROW(a.at(11), std::out_of_range);

  for (std::size_t i = 0; i < a.size(); ++i) {
    ASSERT_EQ(i, a[i]);
  }

  ASSERT_EQ(0, a.front());
  ASSERT_EQ(10, a.back());

  auto d = a.data();
  for (std::size_t i = 0; i < a.size(); ++i) {
    ASSERT_EQ(i, d[i]);
  }
}

TEST(Dynarray, Move)
{
  {
    dynarray a(3, 1);
    for (const auto& e : a) {
      ASSERT_EQ(1, e);
    }
    dynarray b(std::move(a));
    ASSERT_TRUE(a.empty());
    for (const auto& e : b) {
      ASSERT_EQ(1, e);
    }
  }

  {
    dynarray a(3, 1);
    for (const auto& e : a) {
      ASSERT_EQ(1, e);
    }
    dynarray<int> b;
    ASSERT_TRUE(b.empty());

    b = std::move(a);
    ASSERT_TRUE(a.empty());

    for (const auto& e : b) {
      ASSERT_EQ(1, e);
    }
  }
}

TEST(Dynarray, Compare) {
  ASSERT_EQ(dynarray({0, 1}), dynarray({0, 1}));
  ASSERT_LE(dynarray({0, 1}), dynarray({0, 1}));
  ASSERT_GE(dynarray({0, 1}), dynarray({0, 1}));

  ASSERT_LE(dynarray({0, 0}), dynarray({0, 1}));
  ASSERT_GE(dynarray({1, 1}), dynarray({0, 1}));

  ASSERT_NE(dynarray({0}), dynarray({0, 1}));
  ASSERT_NE(dynarray({1, 0}), dynarray({0, 1}));

  ASSERT_LT(dynarray({0}), dynarray({0, 1}));
  ASSERT_LT(dynarray({0, 0}), dynarray({0, 1}));
  ASSERT_GT(dynarray({1, 1}), dynarray({0, 1}));
  ASSERT_GT(dynarray({1, 1}), dynarray({1}));
}

TEST(Dynarray, Swap) {
  dynarray a{'a'};
  dynarray b{'b'};

  ASSERT_EQ('a', a.front());
  ASSERT_EQ('b', b.front());

  swap(a, b);
  ASSERT_EQ('b', a.front());
  ASSERT_EQ('a', b.front());
}

TEST(Dynarray, Empty) {
  {
    dynarray<int> a;
    ASSERT_TRUE(a.empty());
    int i = 0;
    for ([[maybe_unused]] const auto& e : a) ++i;
    ASSERT_EQ(0, i);
  }

  {
    dynarray a(0, 1);
    ASSERT_TRUE(a.empty());
    int i = 0;
    for ([[maybe_unused]] const auto& e : a) ++i;
    ASSERT_EQ(0, i);
  }

  {
    dynarray<int> a({});
    ASSERT_TRUE(a.empty());
    int i = 0;
    for ([[maybe_unused]] const auto& e : a) ++i;
    ASSERT_EQ(0, i);
  }

  {
    dynarray a(0, [](std::size_t i) { return i; });
    ASSERT_TRUE(a.empty());
    int i = 0;
    for ([[maybe_unused]] const auto& e : a) ++i;
    ASSERT_EQ(0, i);
  }
}
