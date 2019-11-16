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
#include <cstdint>
#include <string_view>
#include <type_traits>

#include <gtest/gtest.h>

#include "common/str_util.h"

using namespace std::literals;

using ceph::cf;
using ceph::consume;
using ceph::nul_terminated_copy;
using ceph::parse;
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

template<typename T>
inline void test_parse() {
  auto r = parse<T>("23"sv);
  ASSERT_TRUE(r);
  EXPECT_EQ(*r, 23);

  r = parse<T>("    23"sv);
  EXPECT_FALSE(r);

  r = parse<T>("-5"sv);
  if constexpr (std::is_signed_v<T>) {
    ASSERT_TRUE(r);
    EXPECT_EQ(*r, -5);
  } else {
    EXPECT_FALSE(r);
  }

  r = parse<T>("meow"sv);
  EXPECT_FALSE(r);

  r = parse<T>("9yards"sv);
  EXPECT_FALSE(r);
}

TEST(Parse, Char) {
  test_parse<char>();
}

TEST(Parse, UChar) {
  test_parse<unsigned char>();
}

TEST(Parse, SChar) {
  test_parse<signed char>();
}

TEST(Parse, UInt8) {
  test_parse<std::uint8_t>();
}

TEST(Parse, Int8) {
  test_parse<std::int8_t>();
}

TEST(Parse, UInt16) {
  test_parse<std::uint16_t>();
}

TEST(Parse, Int16) {
  test_parse<std::int16_t>();
}

TEST(Parse, UInt32) {
  test_parse<std::uint32_t>();
}

TEST(Parse, Int32) {
  test_parse<std::int32_t>();
}

TEST(Parse, UInt64) {
  test_parse<std::uint64_t>();
}

TEST(Parse, Int64) {
  test_parse<std::int64_t>();
}

TEST(Parse, UIntMax) {
  test_parse<std::uintmax_t>();
}

TEST(Parse, IntMax) {
  test_parse<std::intmax_t>();
}

TEST(Parse, UIntPtr) {
  test_parse<std::uintptr_t>();
}

TEST(Parse, IntPtr) {
  test_parse<std::intptr_t>();
}

TEST(Parse, UShort) {
  test_parse<unsigned short>();
}

TEST(Parse, Short) {
  test_parse<short>();
}

TEST(Parse, ULong) {
  test_parse<unsigned long>();
}

TEST(Parse, Long) {
  test_parse<long>();
}

TEST(Parse, ULongLong) {
  test_parse<unsigned long long>();
}

TEST(Parse, LongLong) {
  test_parse<long long>();
}

template<typename T>
inline void test_consume() {
  auto pos = "23"sv;
  auto spacepos = "    23"sv;
  auto neg = "-5"sv;
  auto meow = "meow"sv;
  auto trail = "9yards"sv;

  auto v = pos;
  auto r = consume<T>(v);
  ASSERT_TRUE(r);
  EXPECT_EQ(*r, 23);
  EXPECT_TRUE(v.empty());

  v = spacepos;
  r = consume<T>(v);
  EXPECT_FALSE(r);
  EXPECT_EQ(v, spacepos);

  v = neg;
  r = consume<T>(v);
  if constexpr (std::is_signed_v<T>) {
    ASSERT_TRUE(r);
    EXPECT_EQ(*r, -5);
    EXPECT_TRUE(v.empty());
  } else {
    EXPECT_FALSE(r);
    EXPECT_EQ(v, neg);
  }

  v = meow;
  r = consume<T>(v);
  EXPECT_FALSE(r);
  EXPECT_EQ(v, meow);

  v = trail;
  r = consume<T>(v);
  ASSERT_TRUE(r);
  EXPECT_EQ(*r, 9);
  auto w = trail;
  w.remove_prefix(1);
  EXPECT_EQ(v, w);
}

TEST(Consume, Char) {
  test_consume<char>();
}

TEST(Consume, UChar) {
  test_consume<unsigned char>();
}

TEST(Consume, SChar) {
  test_consume<signed char>();
}

TEST(Consume, UInt8) {
  test_consume<std::uint8_t>();
}

TEST(Consume, Int8) {
  test_consume<std::int8_t>();
}

TEST(Consume, UInt16) {
  test_consume<std::uint16_t>();
}

TEST(Consume, Int16) {
  test_consume<std::int16_t>();
}

TEST(Consume, UInt32) {
  test_consume<std::uint32_t>();
}

TEST(Consume, Int32) {
  test_consume<std::int32_t>();
}

TEST(Consume, UInt64) {
  test_consume<std::uint64_t>();
}

TEST(Consume, Int64) {
  test_consume<std::int64_t>();
}

TEST(Consume, UIntMax) {
  test_consume<std::uintmax_t>();
}

TEST(Consume, IntMax) {
  test_consume<std::intmax_t>();
}

TEST(Consume, UIntPtr) {
  test_consume<std::uintptr_t>();
}

TEST(Consume, IntPtr) {
  test_consume<std::intptr_t>();
}

TEST(Consume, UShort) {
  test_consume<unsigned short>();
}

TEST(Consume, Short) {
  test_consume<short>();
}

TEST(Consume, ULong) {
  test_consume<unsigned long>();
}

TEST(Consume, Long) {
  test_consume<long>();
}

TEST(Consume, ULongLong) {
  test_consume<unsigned long long>();
}

TEST(Consume, LongLong) {
  test_consume<long long>();
}
