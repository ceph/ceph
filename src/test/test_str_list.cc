// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/str_lib.h"

#include <array>
#include <deque>
#include <list>
#include <vector>

#include "gtest/gtest.h"

namespace {

struct constexpr_tokens {
  using value_type = std::string_view;

  std::array<std::string_view, 8> tokens {};
  std::size_t count = 0;

  constexpr void clear()
  {
    tokens = {};
    count = 0;
  }

  constexpr void emplace_back(std::string_view token)
  {
    tokens[count] = token;
    ++count;
  }

  constexpr std::size_t size() const
  {
    return count;
  }

  constexpr std::string_view operator[](std::size_t index) const
  {
    return tokens[index];
  }
};

consteval bool for_each_substr_works_at_compile_time()
{
  constexpr_tokens out;

  ceph::for_each_substr("alpha,,beta;gamma", ",;", [&out](auto token) {
    out.emplace_back(token);
  });

  return 3 == std::size(out) &&
    "alpha" == out[0] &&
    "beta" == out[1] &&
    "gamma" == out[2];
}

consteval bool
split_works_at_compile_time()
{
  constexpr_tokens out;

  for (const auto token : ceph::split("alpha,beta gamma", ", ")) {
    out.emplace_back(token);
  }

  return 3 == std::size(out) && "alpha" == out[0] && "beta" == out[1] &&
         "gamma" == out[2];
}

consteval bool split_char_works_at_compile_time()
{
  constexpr_tokens out;

  for (const auto token : ceph::split("alpha,,beta,gamma", ',')) {
    out.emplace_back(token);
  }

  return 3 == std::size(out) && "alpha" == out[0] && "beta" == out[1] &&
         "gamma" == out[2];
}

consteval bool split_strings_works_at_compile_time()
{
  const auto tokens = ceph::split_strings("alpha,,beta;gamma", ",;");

  return 3 == std::size(tokens) && "alpha" == tokens[0] &&
         "beta" == tokens[1] && "gamma" == tokens[2];
}

consteval bool str_join_works_at_compile_time()
{
  // Clang cannot constexpr-evaluate an allocated libstdc++ 12 string.
  constexpr std::array tokens {std::string_view {"a"},
                               std::string_view {"b"},
                               std::string_view {"c"}};

  return "a,b,c" == ceph::str_join(tokens, ",") &&
         "a b c" == ceph::str_join({"a", "b", "c"}, " ");
}

static_assert(for_each_substr_works_at_compile_time());
static_assert(split_works_at_compile_time());
static_assert(split_char_works_at_compile_time());
static_assert(split_strings_works_at_compile_time());
static_assert(str_join_works_at_compile_time());
} // namespace

TEST(StrLib, SplitStrings)
{
  EXPECT_EQ((std::vector<std::string>{}), ceph::split_strings("", " "));
  EXPECT_EQ((std::vector<std::string>{}), ceph::split_strings(" ", " "));
  EXPECT_EQ((std::vector<std::string>{"foo"}),
            ceph::split_strings("foo", " "));
  EXPECT_EQ((std::vector<std::string>{"foo", "bar"}),
            ceph::split_strings(" foo  bar ", " "));

  // default delimiter
  EXPECT_EQ((std::vector<std::string>{}),
            ceph::split_strings(" ; , = \t "));
  EXPECT_EQ((std::vector<std::string>{"foo"}),
            ceph::split_strings(" ; foo = \t "));
  EXPECT_EQ((std::vector<std::string>{"a", "b", "c", "d", "e"}),
            ceph::split_strings("a=b c,d\te"));
  EXPECT_EQ((std::vector<std::string>{"a\nb"}),
            ceph::split_strings("a\nb"));
}

TEST(StrLib, SplitDefaultDelimitersExcludeNewline)
{
  EXPECT_TRUE(std::ranges::equal(
    ceph::split("alpha\nbeta"),
    std::array {std::string_view {"alpha\nbeta"}}));
  EXPECT_TRUE(std::ranges::equal(
    ceph::split("alpha\nbeta", ";,= \t\n"),
    std::array {std::string_view {"alpha"}, std::string_view {"beta"}}));
}

TEST(StrLib, ForEachSubstrYieldsStringViews)
{
  std::vector<std::string> out;

  ::ceph::for_each_substr("alpha,,beta;gamma", ",;", [&out](auto token) {
    out.emplace_back(token);
  });

  EXPECT_EQ((std::vector<std::string>{"alpha", "beta", "gamma"}), out);
}

TEST(StrLib, ForEachSubstrCanUseDefaultDelimiters)
{
  std::vector<std::string> out;

  ::ceph::for_each_substr("alpha,,beta;gamma", [&out](auto token) {
    out.emplace_back(token);
  });

  EXPECT_EQ((std::vector<std::string>{"alpha", "beta", "gamma"}), out);
}

TEST(StrLib, SplitStringsOwnsItsValues)
{
  std::string input = "alpha,beta,gamma";
  const auto out = ceph::split_strings(input, ",");

  input.clear();

  EXPECT_EQ((std::vector<std::string>{"alpha", "beta", "gamma"}), out);
}

TEST(StrLib, StrJoin)
{
  EXPECT_EQ("", ceph::str_join({}, " "));
  EXPECT_EQ("a", ceph::str_join({"a"}, " "));
  EXPECT_EQ("a b c", ceph::str_join({"a", "b", "c"}, " "));
  EXPECT_EQ("a, b, c", ceph::str_join({"a", "b", "c"}, ", "));
}

TEST(StrLib, StrJoinWorksWithStringSequences)
{
  std::list<std::string> list{"a", "b", "c"};
  std::deque<std::string_view> deque{"one", "two", "three"};
  std::vector<const char *> vec{"alpha", "beta", "gamma"};

  EXPECT_EQ("a/b/c", ceph::str_join(list, "/"));
  EXPECT_EQ("one, two, three", ceph::str_join(deque, ", "));
  EXPECT_EQ("alpha:beta:gamma", ceph::str_join(vec, ":"));
}
