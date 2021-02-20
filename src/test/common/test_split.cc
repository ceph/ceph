// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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

#include "common/split.h"
#include <algorithm>
#include <gtest/gtest.h>

namespace ceph {

using string_list = std::initializer_list<std::string_view>;

bool operator==(const split& lhs, const string_list& rhs) {
  return std::equal(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
}
bool operator==(const string_list& lhs, const split& rhs) {
  return std::equal(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
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
}

TEST(split, iterator_indirection)
{
  const auto parts = split("a,b");
  auto i = parts.begin();
  ASSERT_NE(i, parts.end());
  EXPECT_EQ("a", *i); // test operator*
}

TEST(split, iterator_dereference)
{
  const auto parts = split("a,b");
  auto i = parts.begin();
  ASSERT_NE(i, parts.end());
  EXPECT_EQ(1, i->size()); // test operator->
}

TEST(split, iterator_pre_increment)
{
  const auto parts = split("a,b");
  auto i = parts.begin();
  ASSERT_NE(i, parts.end());

  ASSERT_EQ("a", *i);
  EXPECT_EQ("b", *++i); // test operator++()
  EXPECT_EQ("b", *i);
}

TEST(split, iterator_post_increment)
{
  const auto parts = split("a,b");
  auto i = parts.begin();
  ASSERT_NE(i, parts.end());

  ASSERT_EQ("a", *i);
  EXPECT_EQ("a", *i++); // test operator++(int)
  ASSERT_NE(parts.end(), i);
  EXPECT_EQ("b", *i);
}

TEST(split, iterator_singular)
{
  const auto parts = split("a,b");
  auto i = parts.begin();

  // test comparions against default-constructed 'singular' iterators
  split::iterator j;
  split::iterator k;
  EXPECT_EQ(j, parts.end()); // singular == end
  EXPECT_EQ(j, k);           // singular == singular
  EXPECT_NE(j, i);           // singular != valid
}

TEST(split, iterator_multipass)
{
  const auto parts = split("a,b");
  auto i = parts.begin();
  ASSERT_NE(i, parts.end());

  // copy the iterator to test LegacyForwardIterator's multipass guarantee
  auto j = i;
  ASSERT_EQ(i, j);

  ASSERT_EQ("a", *i);
  ASSERT_NE(parts.end(), ++i);
  EXPECT_EQ("b", *i);

  ASSERT_EQ("a", *j); // test that ++i left j unmodified
  ASSERT_NE(parts.end(), ++j);
  EXPECT_EQ("b", *j);

  EXPECT_EQ(i, j);
}

} // namespace ceph
