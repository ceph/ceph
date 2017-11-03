// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/str_list.h"

#include "gtest/gtest.h"

// SplitTest is parameterized for list/vector/set
using Types = ::testing::Types<std::list<std::string>,
                               std::vector<std::string>,
                               std::set<std::string>>;

template <typename T>
struct SplitTest : ::testing::Test {
  void test(const char* input, const char *delim,
            const std::list<std::string>& expected) {
    EXPECT_EQ(expected, get_str_list(input, delim));
  }
  void test(const char* input, const char *delim,
            const std::vector<std::string>& expected) {
    EXPECT_EQ(expected, get_str_vec(input, delim));
  }
  void test(const char* input, const char *delim,
            const std::set<std::string>& expected) {
    EXPECT_EQ(expected, get_str_set(input, delim));
  }
};

TYPED_TEST_CASE(SplitTest, Types);

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
  const char *delims = ";,= \t";
  this->test(" ; , = \t ", delims, TypeParam{});
  this->test(" ; foo = \t ", delims, TypeParam{"foo"});
  this->test("a,b,c", delims, TypeParam{"a","b","c"});
  this->test("a\tb\tc\t", delims, TypeParam{"a","b","c"});
  this->test("a, b, c", delims, TypeParam{"a","b","c"});
  this->test("a b c", delims, TypeParam{"a","b","c"});
  this->test("a=b=c", delims, TypeParam{"a","b","c"});
}
