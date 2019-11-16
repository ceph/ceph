// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson <aemerson@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <array>
#include <cerrno>
#include <string_view>

#include <gtest/gtest.h>

#include "common/str_util.h"

using namespace std::literals;

using ceph::cf;
using ceph::nul_terminated_copy;
using ceph::substr_do;
using ceph::substr_insert;

static constexpr auto foo = "foo"sv;
static constexpr auto fred = "fred"sv;
static constexpr auto blarg = "blarg"sv;

TEST(NulCopy, CharPtr) {
  char storage[4];
  char* dest = storage;

  EXPECT_TRUE(nul_terminated_copy<4>(foo, dest));
  EXPECT_EQ(foo.compare(dest), 0);
  EXPECT_EQ(dest[3], '\0');

  EXPECT_FALSE(nul_terminated_copy<4>(fred, dest));
  // Ensure dest is unmodified if false
  EXPECT_EQ(foo.compare(dest), 0);
  EXPECT_FALSE(nul_terminated_copy<4>(blarg, dest));
  EXPECT_EQ(foo.compare(dest), 0);

  // Zero length array
  EXPECT_FALSE(nul_terminated_copy<0>(std::string_view(), dest));
}

TEST(NulCopy, CArray) {
  char dest[4];

  EXPECT_TRUE(nul_terminated_copy(foo, dest));
  EXPECT_EQ(foo.compare(dest), 0);
  EXPECT_EQ(dest[3], '\0');

  EXPECT_FALSE(nul_terminated_copy(fred, dest));
  // Ensure dest is unmodified if false
  EXPECT_EQ(foo.compare(dest), 0);
  EXPECT_FALSE(nul_terminated_copy(blarg, dest));
  EXPECT_EQ(foo.compare(dest), 0);
}

TEST(NulCopy, StdArray) {
  std::array<char, 0> zero;
  std::array<char, 4> dest;

  nul_terminated_copy(foo, dest);
  ASSERT_TRUE(nul_terminated_copy(foo, dest));
  EXPECT_EQ(foo.compare(dest.data()), 0);
  EXPECT_EQ(dest[3], '\0');


  EXPECT_FALSE(nul_terminated_copy(fred, dest));
  // Ensure dest is unmodified if false
  EXPECT_EQ(foo.compare(dest.data()), 0);
  EXPECT_FALSE(nul_terminated_copy(blarg, dest));
  EXPECT_EQ(foo.compare(dest.data()), 0);

  // Zero length array
  nul_terminated_copy(std::string_view(), zero);
}

TEST(NulCopy, Optional) {
  {
    auto a = nul_terminated_copy<4>(foo);
    ASSERT_TRUE(a);
    EXPECT_EQ(foo.compare(a->data()), 0);
    EXPECT_EQ((*a)[3], '\0');
  }
  {
    auto a = nul_terminated_copy<4>(fred);
    EXPECT_FALSE(a);
  }
  {
    auto a = nul_terminated_copy<4>(blarg);
    EXPECT_FALSE(a);
  }
}

inline constexpr auto testlist = "a,b,c,d"sv;

TEST(Do, Unbreaking) {
  std::string d;
  substr_do(testlist,
	    [&](std::string_view s) {
	      d.append(s);
	    });
  EXPECT_EQ(d, "abcd"s);
}

TEST(Do, Breaking) {
  std::string d;
  substr_do(testlist,
	    [&](std::string_view s) {
	      if (s == "c")
		return cf::stop;
	      d.append(s);
	      return cf::go;
	    });
  EXPECT_EQ(d, "ab"s);
}

TEST(Insert, View) {
  std::vector<std::string_view> d;
  substr_insert(testlist, std::back_inserter(d));
  EXPECT_EQ(d, std::vector({"a"sv, "b"sv, "c"sv, "d"sv}));
}

TEST(Insert, String) {
  std::vector<std::string> d;
  substr_insert(testlist, std::back_inserter(d));
  EXPECT_EQ(d, std::vector({"a"s, "b"s, "c"s, "d"s}));
}
