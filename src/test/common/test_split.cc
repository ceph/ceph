// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "include/str_lib.h"
#include "common/container_concepts.h"

#include <algorithm>
#include <cstddef>
#include <ranges>
#include <gtest/gtest.h>

namespace ceph {

using string_list = std::initializer_list<std::string_view>;

static_assert(std::forward_iterator<spliterator>);
static_assert(std::ranges::forward_range<split>);
static_assert(ceph::concepts::container_compatible_range<split, std::string_view>);

constexpr std::size_t count_parts(std::string_view input,
                                  std::string_view delims = ";,= \t\n")
{
  return static_cast<std::size_t>(
    std::ranges::distance(split { input, delims }));
}

static_assert(0 == count_parts(""));
static_assert(0 == count_parts(",,", ","));
static_assert(3 == count_parts(",alpha,beta,,gamma,", ","));
static_assert(1 == count_parts("alpha,beta", ""));

bool operator==(const split& lhs, const string_list& rhs) {
  return std::ranges::equal(lhs, rhs);
}
bool operator==(const string_list& lhs, const split& rhs) {
  return std::ranges::equal(lhs, rhs);
}

TEST(split, split)
{
  EXPECT_EQ(string_list({}), split(""));
  EXPECT_EQ(string_list({}), split(","));
  EXPECT_EQ(string_list({}), split(",;"));

  EXPECT_EQ(string_list({"a"}), split("a,;"));
  EXPECT_EQ(string_list({"a"}), split(",a;"));
  EXPECT_EQ(string_list({"a"}), split(",;a"));

  EXPECT_EQ(string_list({"a", "b"}), split("a,b;"));
  EXPECT_EQ(string_list({"a", "b"}), split("a,;b"));
  EXPECT_EQ(string_list({"a", "b"}), split(",a;b"));

  EXPECT_EQ(string_list({"a,b"}), split("a,b", ""));
}

TEST(split, iterator_indirection)
{
  const auto parts = split("a,b");
  auto i = std::begin(parts);
  ASSERT_NE(i, std::end(parts));
  EXPECT_EQ("a", *i); // test operator*
}

TEST(split, iterator_dereference)
{
  const auto parts = split("a,b");
  auto i = std::begin(parts);
  ASSERT_NE(i, std::end(parts));
  EXPECT_EQ(1, i->size()); // test operator->
}

TEST(split, iterator_pre_increment)
{
  const auto parts = split("a,b");
  auto i = std::begin(parts);
  ASSERT_NE(i, std::end(parts));

  ASSERT_EQ("a", *i);
  EXPECT_EQ("b", *++i); // test operator++()
  EXPECT_EQ("b", *i);
}

TEST(split, iterator_post_increment)
{
  const auto parts = split("a,b");
  auto i = std::begin(parts);
  ASSERT_NE(i, std::end(parts));

  ASSERT_EQ("a", *i);
  EXPECT_EQ("a", *i++); // test operator++(int)
  ASSERT_NE(std::end(parts), i);
  EXPECT_EQ("b", *i);
}

TEST(split, iterator_singular)
{
  const auto parts = split("a,b");
  auto i = std::begin(parts);

  // test comparions against default-constructed 'singular' iterators
  split::iterator j;
  split::iterator k;
  EXPECT_EQ(j, std::end(parts)); // singular == end
  EXPECT_EQ(j, k);           // singular == singular
  EXPECT_NE(j, i);           // singular != valid
}

TEST(split, iterator_multipass)
{
  const auto parts = split("a,b");
  auto i = std::begin(parts);
  ASSERT_NE(i, std::end(parts));

  // copy the iterator to test LegacyForwardIterator's multipass guarantee
  auto j = i;
  ASSERT_EQ(i, j);

  ASSERT_EQ("a", *i);
  ASSERT_NE(std::end(parts), ++i);
  EXPECT_EQ("b", *i);

  ASSERT_EQ("a", *j); // test that ++i left j unmodified
  ASSERT_NE(std::end(parts), ++j);
  EXPECT_EQ("b", *j);

  EXPECT_EQ(i, j);
}

} // namespace ceph
