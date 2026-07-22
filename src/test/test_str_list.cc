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

#include "include/str_list.h"

#include <deque>
#include <forward_list>
#include <set>
#include <unordered_set>

#include "gtest/gtest.h"

namespace {

const char *default_delims = ";,= \t";

} // namespace

// SplitTest is parameterized for standard containers supported by get_str_seq()
using Types = ::testing::Types<std::list<std::string>,
                               std::vector<std::string>,
                               std::deque<std::string>,
                               std::forward_list<std::string>,
                               std::set<std::string>,
                               std::unordered_set<std::string>>;

template <typename T>
struct SplitTest : ::testing::Test {
  void test(const char* input, const char *delim, const T& expected) {
    EXPECT_EQ(expected, get_str_seq<T>(input, delim));
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

TEST(StrList, GetListUsesDefaultDelimiters)
{
  EXPECT_EQ((std::list<std::string>{"a", "b", "c", "d"}),
            get_str_list("a,b c=d"));
}

TEST(StrList, GetVecUsesDefaultDelimiters)
{
  EXPECT_EQ((std::vector<std::string>{"a", "b", "c", "d"}),
            get_str_vec("a,b c=d"));
}

TEST(StrList, OutputListsAreClearedBeforeSplitting)
{
  std::list<std::string> out{"old"};

  get_str_list("new,values", ",", out);

  EXPECT_EQ((std::list<std::string>{"new", "values"}), out);
}

TEST(StrList, OutputVectorsAreClearedBeforeSplitting)
{
  std::vector<std::string> out{"old"};

  get_str_vec("new,values", ",", out);

  EXPECT_EQ((std::vector<std::string>{"new", "values"}), out);
}

TEST(StrList, ForEachSubstrYieldsStringViews)
{
  std::vector<std::string> out;

  ::ceph::for_each_substr("alpha,,beta;gamma", ",;", [&out](auto token) {
      out.emplace_back(token);
    });

  EXPECT_EQ((std::vector<std::string>{"alpha", "beta", "gamma"}), out);
}

TEST(StrList, GetStrSeqWorksWithAppendableStringSequences)
{
  std::deque<std::string> out{"old"};

  get_str_seq("alpha,beta,gamma", ",", out);

  EXPECT_EQ((std::deque<std::string>{"alpha", "beta", "gamma"}), out);
}

TEST(StrList, GetStrSeqCanReturnStringSequences)
{
  auto out = get_str_seq<std::deque<std::string>>("alpha,beta,gamma", ",");

  EXPECT_EQ((std::deque<std::string>{"alpha", "beta", "gamma"}), out);
}

TEST(StrList, GetStrSeqCanUseOrderedSets)
{
  std::set<std::string> out{"old"};

  get_str_seq("gamma,alpha,beta", ",", out);

  EXPECT_EQ((std::set<std::string>{"alpha", "beta", "gamma"}), out);
}

TEST(StrList, SplitAtCommasTrimsTokens)
{
  std::vector<std::string> out{"old"};

  split_at_commas(" alpha, beta ,\tgamma\n", out);

  EXPECT_EQ((std::vector<std::string>{"alpha", "beta", "gamma"}), out);
}

TEST(StrList, SplitAtCommasCanReturnStringSequences)
{
  auto out = split_at_commas<std::list<std::string>>(" alpha, beta ,\tgamma\n");

  EXPECT_EQ((std::list<std::string>{"alpha", "beta", "gamma"}), out);
}

TEST(StrList, SplitAtCommasPreservesInternalTokenText)
{
  std::vector<std::string> out;

  split_at_commas("beast port=8080, civetweb port=8081", out);

  EXPECT_EQ((std::vector<std::string>{"beast port=8080", "civetweb port=8081"}), out);
}

TEST(StrList, SplitAtCommasSkipsEmptyTrimmedTokens)
{
  std::list<std::string> out;

  split_at_commas("alpha, ,,, beta", out);

  EXPECT_EQ((std::list<std::string>{"alpha", "beta"}), out);
}

TEST(StrList, Join)
{
  EXPECT_EQ("", str_join({}, " "));
  EXPECT_EQ("a", str_join({"a"}, " "));
  EXPECT_EQ("a b c", str_join({"a", "b", "c"}, " "));
  EXPECT_EQ("a, b, c", str_join({"a", "b", "c"}, ", "));
}

TEST(StrList, JoinWorksWithStringSequences)
{
  std::list<std::string> list{"a", "b", "c"};
  std::deque<std::string_view> deque{"one", "two", "three"};
  std::vector<const char *> vec{"alpha", "beta", "gamma"};

  EXPECT_EQ("a/b/c", str_join(list, "/"));
  EXPECT_EQ("one, two, three", str_join(deque, ", "));
  EXPECT_EQ("alpha:beta:gamma", str_join(vec, ":"));
}
