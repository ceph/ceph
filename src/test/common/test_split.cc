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
#include <array>
#include <cstddef>
#include <memory>
#include <ranges>
#include <gtest/gtest.h>

namespace ceph {

using string_list = std::initializer_list<std::string_view>;

static_assert(std::forward_iterator<spliterator<>>);
static_assert(std::forward_iterator<spliterator<char>>);
static_assert(std::ranges::forward_range<split_view<>>);
static_assert(std::ranges::forward_range<split_view<char>>);
static_assert(std::ranges::view<split_view<>>);
static_assert(std::ranges::borrowed_range<split_view<>>);
static_assert(std::ranges::borrowed_range<split_view<char>>);
static_assert(ceph::concepts::container_compatible_range<split_view<>,
                                                         std::string_view>);
static_assert(std::same_as<decltype(split("a,b", ',')), split_view<char>>);

consteval std::size_t count_parts(std::string_view input)
{
  return static_cast<std::size_t>(std::ranges::distance(split(input)));
}

consteval std::size_t count_parts(std::string_view input,
                                  std::string_view delims)
{
  return static_cast<std::size_t>(
    std::ranges::distance(split(input, delims)));
}

static_assert(0 == count_parts(""));
static_assert(0 == count_parts(",,", ","));
static_assert(3 == count_parts(",alpha,beta,,gamma,", ","));
static_assert(1 == count_parts("alpha,beta", ""));

consteval bool split_returns_expected_parts()
{
  return std::ranges::equal(split(""), string_list {}) &&
         std::ranges::equal(split(","), string_list {}) &&
         std::ranges::equal(split(",;"), string_list {}) &&
         std::ranges::equal(split("a,;"), string_list {"a"}) &&
         std::ranges::equal(split(",a;"), string_list {"a"}) &&
         std::ranges::equal(split(",;a"), string_list {"a"}) &&
         std::ranges::equal(split("a,b;"), string_list {"a", "b"}) &&
         std::ranges::equal(split("a,;b"), string_list {"a", "b"}) &&
         std::ranges::equal(split(",a;b"), string_list {"a", "b"}) &&
         std::ranges::equal(split("a\nb"), string_list {"a\nb"}) &&
         std::ranges::equal(split("a\nb", ";,= \t\n"),
                            string_list {"a", "b"}) &&
         std::ranges::equal(split("a,b", ""), string_list {"a,b"});
}

static_assert(split_returns_expected_parts());

template <typename DelimsT>
consteval bool postfix_preserves_previous_part(DelimsT delims)
{
  const auto parts = split_view { "alpha,beta", delims };
  auto cursor = std::begin(parts);
  const auto previous = cursor++;

  return "alpha" == *previous && "beta" == *cursor &&
         "beta" == *cursor++ && cursor == std::end(parts);
}

static_assert(postfix_preserves_previous_part(','));
static_assert(postfix_preserves_previous_part(std::string_view { "," }));

consteval bool char_split_handles_binary_input()
{
  return std::ranges::equal(split("", ','), string_list {}) &&
         std::ranges::equal(split(",,,", ','), string_list {}) &&
         std::ranges::equal(split("a,abcdefghijklmnopq,,z", ','),
                            string_list {"a", "abcdefghijklmnopq", "z"}) &&
         std::ranges::equal(split(std::string_view {"\0a\0\0b\0", 6}, '\0'),
                            string_list {"a", "b"});
}

static_assert(char_split_handles_binary_input());

TEST(split, char_delimiter_bounded_binary_input)
{
  const std::array delimiters { '\0', '\1', '\x7f', '\x80', '\xff', ',' };
  for (std::size_t length = 0; length <= 65; ++length) {
    for (std::size_t offset = 0; offset < 8; ++offset) {
      // End the view at the allocation boundary to catch overreads under ASan.
      const auto storage = std::make_unique<char[]>(offset + length + 2);
      for (const char delimiter : delimiters) {
        const auto other = static_cast<char>(static_cast<unsigned char>(delimiter) ^ 1U);
        storage[offset] = other;
        storage[offset + 1] = delimiter;
        for (std::size_t position = 0; position <= length; ++position) {
          std::fill_n(storage.get() + offset + 2, length, other);
          if (position < length) {
            storage[offset + 2 + position] = delimiter;
          }
          const std::string_view input { storage.get() + offset, length + 2 };
          ASSERT_TRUE(std::ranges::equal(
            split(input, delimiter), split(input, std::string_view { &delimiter, 1 })))
            << "length=" << length << " offset=" << offset
            << " position=" << position
            << " delimiter=" << static_cast<unsigned>(static_cast<unsigned char>(delimiter));
        }
      }
    }
  }
}

TEST(split, char_iterator_multipass_after_long_token)
{
  constexpr std::string_view long_token = "abcdefghijklmnopq";
  const auto parts = split("a,abcdefghijklmnopq,,b,c", ',');
  auto first = parts.begin();
  auto second = first;

  EXPECT_EQ("a", *first++);
  EXPECT_EQ(long_token, *first++);
  EXPECT_EQ("b", *first);
  EXPECT_EQ("a", *second++);
  EXPECT_EQ(long_token, *second);
  const auto previous = second++;
  EXPECT_EQ(long_token, *previous);
  EXPECT_EQ(first, second);
  EXPECT_EQ("b", *first++);
  EXPECT_EQ("b", *second++);
  EXPECT_EQ("c", *first++);
  EXPECT_EQ("c", *second++);
  EXPECT_EQ(parts.end(), first);
  EXPECT_EQ(parts.end(), second);
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
  split_view<>::iterator j;
  split_view<>::iterator k;
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
