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
#include <forward_list>
#include <list>
#include <set>
#include <unordered_set>

#include "gtest/gtest.h"

namespace {

const char *default_delims = ";,= \t";

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

constexpr bool for_each_substr_works_at_compile_time()
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

constexpr bool split_str_works_at_compile_time()
{
  const auto out = ceph::split_str<constexpr_tokens>("alpha,beta gamma");

  return 3 == std::size(out) &&
    "alpha" == out[0] &&
    "beta" == out[1] &&
    "gamma" == out[2];
}

constexpr bool comma_split_works_at_compile_time()
{
  const auto out = ceph::comma_split<constexpr_tokens>(" alpha, beta ,, gamma ");

  return 3 == std::size(out) &&
    "alpha" == out[0] &&
    "beta" == out[1] &&
    "gamma" == out[2];
}

static_assert(for_each_substr_works_at_compile_time());
static_assert(split_str_works_at_compile_time());
static_assert(comma_split_works_at_compile_time());

} // namespace

// SplitTest is parameterized for standard containers supported by split_str()
using Types = ::testing::Types<std::list<std::string>,
                               std::vector<std::string>,
                               std::deque<std::string>,
                               std::forward_list<std::string>,
                               std::set<std::string>,
                               std::unordered_set<std::string>>;

template <typename T>
struct SplitTest : ::testing::Test {
  void test(const char* input, const char *delim, const T& expected) {
    EXPECT_EQ(expected, ceph::split_str<T>(input, delim));
  }
};

TYPED_TEST_SUITE(SplitTest, Types);

TYPED_TEST(SplitTest, Get)
{
  this->test("", " ", TypeParam{});
  this->test(" ", " ", TypeParam{});
  this->test("foo", " ", TypeParam{"foo"});
  this->test("foo bar", " ", TypeParam{"foo","bar"});
  this->test(" foo bar", " ", TypeParam{"foo","bar"});
  this->test("foo bar ", " ", TypeParam{"foo","bar"});
  this->test("foo bar ", " ", TypeParam{"foo","bar"});

  // default delimiter
  this->test(" ; , = \t ", default_delims, TypeParam{});
  this->test(" ; foo = \t ", default_delims, TypeParam{"foo"});
  this->test("a,b,c", default_delims, TypeParam{"a","b","c"});
  this->test("a\tb\tc\t", default_delims, TypeParam{"a","b","c"});
  this->test("a, b, c", default_delims, TypeParam{"a","b","c"});
  this->test("a b c", default_delims, TypeParam{"a","b","c"});
  this->test("a=b=c", default_delims, TypeParam{"a","b","c"});
  this->test("a,,b;;;c", default_delims, TypeParam{"a","b","c"});
  this->test("a=b c,d\te", default_delims, TypeParam{"a","b","c","d","e"});
}

TEST(StrLib, SplitStrCanReturnListsWithDefaultDelimiters)
{
  EXPECT_EQ((std::list<std::string>{"a", "b", "c", "d"}),
            ceph::split_str<std::list>("a,b c=d"));
}

TEST(StrLib, SplitStrReturnsVectorsByDefault)
{
  EXPECT_EQ((std::vector<std::string>{"a", "b", "c", "d"}),
            ceph::split_str("a,b c=d"));
}

TEST(StrLib, SplitStrClearsOutputListsBeforeSplitting)
{
  std::list<std::string> out{"old"};

  ceph::split_str("new,values", ",", out);

  EXPECT_EQ((std::list<std::string>{"new", "values"}), out);
}

TEST(StrLib, SplitStrInfersOutputContainerWithDefaultDelimiters)
{
  std::list<std::string> out{"old"};

  ceph::split_str("a,b c=d", out);

  EXPECT_EQ((std::list<std::string>{"a", "b", "c", "d"}), out);
}

TEST(StrLib, SplitStrClearsOutputVectorsBeforeSplitting)
{
  std::vector<std::string> out{"old"};

  ceph::split_str("new,values", ",", out);

  EXPECT_EQ((std::vector<std::string>{"new", "values"}), out);
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

TEST(StrLib, SplitStrWorksWithAppendableStringSequences)
{
  std::deque<std::string> out{"old"};

  ceph::split_str("alpha,beta,gamma", ",", out);

  EXPECT_EQ((std::deque<std::string>{"alpha", "beta", "gamma"}), out);
}

TEST(StrLib, SplitStrCanReturnStringSequences)
{
  auto out = ceph::split_str<std::deque<std::string>>("alpha,beta,gamma", ",");

  EXPECT_EQ((std::deque<std::string>{"alpha", "beta", "gamma"}), out);
}

TEST(StrLib, SplitStrCanUseOrderedSets)
{
  std::set<std::string> out{"old"};

  ceph::split_str("gamma,alpha,beta", ",", out);

  EXPECT_EQ((std::set<std::string>{"alpha", "beta", "gamma"}), out);
}

TEST(StrLib, CommaSplitTrimsTokens)
{
  std::vector<std::string> out{"old"};

  ceph::comma_split(" alpha, beta ,\tgamma\n", out);

  EXPECT_EQ((std::vector<std::string>{"alpha", "beta", "gamma"}), out);
}

TEST(StrLib, CommaSplitCanReturnStringSequences)
{
  auto out = ceph::comma_split<std::list>(" alpha, beta ,\tgamma\n");

  EXPECT_EQ((std::list<std::string>{"alpha", "beta", "gamma"}), out);
}

TEST(StrLib, CommaSplitPreservesInternalTokenText)
{
  std::vector<std::string> out;

  ceph::comma_split("beast port=8080, civetweb port=8081", out);

  EXPECT_EQ((std::vector<std::string>{"beast port=8080", "civetweb port=8081"}), out);
}

TEST(StrLib, CommaSplitSkipsEmptyTrimmedTokens)
{
  std::list<std::string> out;

  ceph::comma_split("alpha, ,,, beta", out);

  EXPECT_EQ((std::list<std::string>{"alpha", "beta"}), out);
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
